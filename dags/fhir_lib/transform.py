"""Pure functions mapping FHIR R4 JSON to flat rows. No I/O, easy to unit test."""
from __future__ import annotations

import re
from datetime import date, datetime

_PATIENT_REF = re.compile(r"(?:^|/)Patient/([^/]+)$")


def parse_instant(value: str | None) -> datetime | None:
    """Parse a FHIR instant/dateTime with time and offset; None if partial/invalid."""
    if not value or len(value) < 19:  # needs at least YYYY-MM-DDThh:mm:ss
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else None  # FHIR requires an offset when time is present


def parse_full_date(value: str | None) -> date | None:
    """Return a date only for complete YYYY-MM-DD values (FHIR allows YYYY, YYYY-MM)."""
    if value and len(value) == 10:
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _to_number(value):
    """Accept int/float only (bool is an int subclass, so exclude it)."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    return None


def _first_coding(concept: dict | None) -> dict:
    codings = (concept or {}).get("coding") or []
    return codings[0] if codings else {}


def _pick_name(names: list | None) -> dict:
    if not names:
        return {}
    return next((n for n in names if n.get("use") == "official"), names[0])


def patient_row(res: dict) -> dict:
    name = _pick_name(res.get("name"))
    given = [g for g in (name.get("given") or []) if isinstance(g, str)]
    meta = res.get("meta") or {}
    return {
        "patient_id": res["id"],
        "family_name": name.get("family"),
        "given_names": " ".join(given) or None,
        "gender": res.get("gender"),
        "birth_date_raw": res.get("birthDate"),
        "birth_date": parse_full_date(res.get("birthDate")),
        "active": res.get("active"),
        "version_id": meta.get("versionId"),
        "last_updated": parse_instant(meta.get("lastUpdated")),
    }


def observation_row(res: dict) -> dict:
    ref = (res.get("subject") or {}).get("reference")
    match = _PATIENT_REF.search(ref) if ref else None
    code = res.get("code") or {}
    coding = _first_coding(code)
    categories = res.get("category") or []
    effective = (
        res.get("effectiveDateTime")
        or res.get("effectiveInstant")
        or (res.get("effectivePeriod") or {}).get("start")
    )
    vq = res.get("valueQuantity") or {}
    value_numeric = _to_number(vq.get("value"))
    if value_numeric is None:
        value_numeric = _to_number(res.get("valueInteger")) or _to_number(res.get("valueDecimal"))
    value_text = res.get("valueString")
    if value_text is None and res.get("valueCodeableConcept"):
        vcc = res["valueCodeableConcept"]
        value_text = vcc.get("text") or _first_coding(vcc).get("display")
    meta = res.get("meta") or {}
    return {
        "observation_id": res["id"],
        "patient_ref": ref,
        "patient_id": match.group(1) if match else None,
        "status": res.get("status"),
        "category_code": _first_coding(categories[0]).get("code") if categories else None,
        "code_system": coding.get("system"),
        "code": coding.get("code"),
        "code_display": coding.get("display") or code.get("text"),
        "effective_at": parse_instant(effective),
        "value_numeric": value_numeric,
        "value_unit": vq.get("unit"),
        "value_ucum_code": vq.get("code"),
        "value_text": value_text,
        "components": res.get("component"),
        "version_id": meta.get("versionId"),
        "last_updated": parse_instant(meta.get("lastUpdated")),
    }
