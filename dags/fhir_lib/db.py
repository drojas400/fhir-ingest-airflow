"""Idempotent upserts into raw.fhir_resource and core.* (psycopg2)."""
from __future__ import annotations

import logging

from psycopg2.extras import Json, execute_values

from fhir_lib.transform import observation_row, parse_instant, patient_row

log = logging.getLogger(__name__)

RAW_UPSERT = """
INSERT INTO raw.fhir_resource AS t
    (resource_type, resource_id, version_id, last_updated, payload, run_id)
VALUES %s
ON CONFLICT (resource_type, resource_id) DO UPDATE SET
    version_id   = EXCLUDED.version_id,
    last_updated = EXCLUDED.last_updated,
    payload      = EXCLUDED.payload,
    run_id       = EXCLUDED.run_id,
    ingested_at  = now()
WHERE EXCLUDED.last_updated > t.last_updated
RETURNING (xmax = 0) AS inserted
"""

PATIENT_COLS = [
    "patient_id", "family_name", "given_names", "gender", "birth_date_raw",
    "birth_date", "active", "version_id", "last_updated",
]
OBSERVATION_COLS = [
    "observation_id", "patient_ref", "patient_id", "status", "category_code",
    "code_system", "code", "code_display", "effective_at", "value_numeric",
    "value_unit", "value_ucum_code", "value_text", "components", "version_id",
    "last_updated",
]
JSON_COLS = {"components"}

# Fixed mapping (never user input), so f-string identifiers are safe here.
CORE = {
    "Patient": ("core.patient", "patient_id", PATIENT_COLS, patient_row),
    "Observation": ("core.observation", "observation_id", OBSERVATION_COLS, observation_row),
}

# Raw rows that are missing in core or newer than core: makes the load self-healing.
PENDING_SQL = """
SELECT r.payload
FROM raw.fhir_resource r
LEFT JOIN {table} c ON c.{pk} = r.resource_id
WHERE r.resource_type = %s
  AND (c.{pk} IS NULL OR r.last_updated > c.last_updated)
"""


def _count(returned: list, total: int) -> tuple[int, int, int]:
    inserted = sum(1 for (ins,) in returned if ins)
    updated = len(returned) - inserted
    return inserted, updated, total - len(returned)


def upsert_raw(cur, resource_type: str, resources: list[dict], run_id: str) -> dict:
    """Upsert one page into raw. Dedupes by id (keeps newest) to avoid
    'ON CONFLICT cannot affect row a second time'."""
    rows: dict[str, tuple] = {}
    skipped = 0
    for r in resources:
        meta = r.get("meta") or {}
        lu = parse_instant(meta.get("lastUpdated"))
        if not r.get("id") or lu is None:
            skipped += 1
            continue
        prev = rows.get(r["id"])
        if prev is None or lu > prev[3]:
            rows[r["id"]] = (resource_type, r["id"], meta.get("versionId"), lu, Json(r), run_id)
    if not rows:
        return {"inserted": 0, "updated": 0, "unchanged": 0, "skipped": skipped, "max_last_updated": None}
    returned = execute_values(cur, RAW_UPSERT, list(rows.values()), fetch=True)
    inserted, updated, unchanged = _count(returned, len(rows))
    return {"inserted": inserted, "updated": updated, "unchanged": unchanged, "skipped": skipped,
            "max_last_updated": max(v[3] for v in rows.values())}


def _upsert_core(cur, table: str, pk: str, cols: list[str], rows: list[dict]) -> tuple[int, int, int]:
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != pk)
    sql = (
        f"INSERT INTO {table} AS t ({', '.join(cols)}) VALUES %s "
        f"ON CONFLICT ({pk}) DO UPDATE SET {updates}, loaded_at = now() "
        f"WHERE EXCLUDED.last_updated > t.last_updated "
        f"RETURNING (xmax = 0) AS inserted"
    )
    values = [
        tuple(Json(r[c]) if c in JSON_COLS and r[c] is not None else r[c] for c in cols)
        for r in rows
    ]
    returned = execute_values(cur, sql, values, fetch=True)
    return _count(returned, len(rows))


def load_pending_to_core(conn, resource_type: str, batch_size: int = 500) -> dict:
    """Transform pending raw rows and upsert them into core (single transaction)."""
    table, pk, cols, to_row = CORE[resource_type]
    stats = {"resource_type": resource_type, "pending": 0, "inserted": 0,
             "updated": 0, "unchanged": 0, "rejected": 0}
    with conn.cursor() as read_cur, conn.cursor() as write_cur:
        read_cur.execute(PENDING_SQL.format(table=table, pk=pk), (resource_type,))
        while batch := read_cur.fetchmany(batch_size):
            rows = []
            for (payload,) in batch:
                stats["pending"] += 1
                try:
                    rows.append(to_row(payload))
                except Exception:  # one bad record must not fail the batch
                    stats["rejected"] += 1
                    log.exception("Rejected %s/%s", resource_type, payload.get("id"))
            if rows:
                ins, upd, unch = _upsert_core(write_cur, table, pk, cols, rows)
                stats["inserted"] += ins
                stats["updated"] += upd
                stats["unchanged"] += unch
    return stats


WATERMARK_UPSERT = """
INSERT INTO audit.watermark AS w (resource_type, high_watermark, updated_by_run)
VALUES (%s, %s, %s)
ON CONFLICT (resource_type) DO UPDATE SET
    high_watermark = EXCLUDED.high_watermark,
    updated_at     = now(),
    updated_by_run = EXCLUDED.updated_by_run
WHERE EXCLUDED.high_watermark > w.high_watermark
"""


def get_watermark(cur, resource_type: str):
    """Highest source lastUpdated already landed in raw, or None on first run."""
    cur.execute("SELECT high_watermark FROM audit.watermark WHERE resource_type = %s",
                (resource_type,))
    row = cur.fetchone()
    return row[0] if row else None


def advance_watermark(cur, resource_type: str, value, run_id: str) -> None:
    """Move the watermark forward only (monotonic); never moves it back."""
    cur.execute(WATERMARK_UPSERT, (resource_type, value, run_id))
