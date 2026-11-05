"""Data-quality checks. Every result is stored; only 'critical' failures fail the run."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from fhir_lib.db import CORE, PENDING_SQL

log = logging.getLogger(__name__)

FRESHNESS_MAX_HOURS = 3

INSERT_CHECK = """
INSERT INTO audit.quality_check
    (run_id, check_name, resource_type, severity, metric_value, threshold, passed, details)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# (check_name, resource_type, numerator SQL, denominator SQL, threshold %)
PCT_CHECKS = [
    ("observation_missing_subject_pct", "Observation",
     "SELECT count(*) FROM core.observation WHERE patient_ref IS NULL",
     "SELECT count(*) FROM core.observation", 40),
    ("observation_orphan_patient_pct", "Observation",
     """SELECT count(*) FROM core.observation o
        WHERE o.patient_id IS NOT NULL
          AND NOT EXISTS (SELECT 1 FROM core.patient p WHERE p.patient_id = o.patient_id)""",
     "SELECT count(*) FROM core.observation WHERE patient_id IS NOT NULL", 50),
    ("patient_missing_birth_date_pct", "Patient",
     "SELECT count(*) FROM core.patient WHERE birth_date_raw IS NULL",
     "SELECT count(*) FROM core.patient", 50),
    ("patient_test_data_pct", "Patient",
     """SELECT count(*) FROM core.patient
        WHERE family_name ILIKE 'testfamily%' OR given_names ILIKE 'testgiven%'""",
     "SELECT count(*) FROM core.patient", 50),
]


def _scalar(cur, sql: str, params=None):
    cur.execute(sql, params)
    return cur.fetchone()[0]


def run_checks(conn, run_id: str, load_results: list[dict]) -> list[dict]:
    """Run all checks, persist every result, return them."""
    by_rt = {r["resource_type"]: r for r in load_results}
    results: list[dict] = []

    def add(name, rt, severity, value, threshold, passed, details=None):
        results.append({"check_name": name, "resource_type": rt, "severity": severity,
                        "metric_value": value, "threshold": threshold,
                        "passed": bool(passed), "details": details or {}})

    with conn.cursor() as cur:
        for rt, (table, pk, _cols, _fn) in CORE.items():
            info = by_rt.get(rt, {})

            # Critical: everything landed in raw is reflected in core.
            pending = _scalar(cur, f"SELECT count(*) FROM ({PENDING_SQL.format(table=table, pk=pk)}) s", (rt,))
            rejected = info.get("rejected", 0)
            add("reconciliation_pending_rows", rt, "critical", pending, rejected,
                pending <= rejected, {"rejected_this_run": rejected})

            # Critical: watermark is recent, unless we are still catching up.
            wm = _scalar(cur, "SELECT high_watermark FROM audit.watermark WHERE resource_type = %s", (rt,))
            if wm is None:
                add("freshness_lag_hours", rt, "critical", None, FRESHNESS_MAX_HOURS, False,
                    {"state": "no_watermark"})
            else:
                lag = round((datetime.now(timezone.utc) - wm).total_seconds() / 3600, 2)
                if info.get("extract_truncated"):
                    add("freshness_lag_hours", rt, "critical", lag, FRESHNESS_MAX_HOURS, True,
                        {"state": "catching_up"})
                else:
                    add("freshness_lag_hours", rt, "critical", lag, FRESHNESS_MAX_HOURS,
                        lag <= FRESHNESS_MAX_HOURS, {"state": "steady"})

        # Warnings: completeness / integrity of the source data.
        for name, rt, num_sql, den_sql, threshold in PCT_CHECKS:
            num, den = _scalar(cur, num_sql), _scalar(cur, den_sql)
            pct = round(100.0 * num / den, 2) if den else 0.0
            add(name, rt, "warning", pct, threshold, pct <= threshold,
                {"numerator": num, "denominator": den})

        for r in results:
            cur.execute(INSERT_CHECK, (run_id, r["check_name"], r["resource_type"], r["severity"],
                                       r["metric_value"], r["threshold"], r["passed"],
                                       json.dumps(r["details"])))
    conn.commit()
    for r in results:
        log.info("check %-32s %-12s %-8s value=%s threshold=%s passed=%s",
                 r["check_name"], r["resource_type"], r["severity"],
                 r["metric_value"], r["threshold"], r["passed"])
    return results
