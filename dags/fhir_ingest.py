"""FHIR R4 -> Postgres incremental ingestion from the public HAPI server.

extract_<resource>:   _lastUpdated >= (watermark - overlap), oldest first, paged
                      -> upsert raw.fhir_resource + advance audit.watermark
                         (same transaction per page: progress is never lost)
load_core_<resource>: raw rows missing/stale in core -> typed upsert into core.*

Idempotent: re-running never duplicates rows and never overwrites newer data.
The window is driven by the source watermark, not by the scheduler clock, so
failed or paused runs are caught up automatically on the next run.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

RESOURCES = ["Patient", "Observation"]
CONN_ID = "fhir_db"
log = logging.getLogger(__name__)


def _pg_conn():
    """Open an explicit psycopg2 connection using the Airflow connection's credentials.

    PostgresHook.get_conn() may return a psycopg 3 connection depending on the
    provider version; fhir_lib.db relies on psycopg2 (execute_values, Json).
    """
    import psycopg2
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    c = PostgresHook.get_connection(CONN_ID)
    return psycopg2.connect(
        host=c.host, port=c.port or 5432, dbname=c.schema,
        user=c.login, password=c.password,
        application_name="fhir_ingest",
    )


def _fhir_instant(dt: datetime) -> str:
    # Truncated to seconds: only widens the window slightly (safe with 'ge').
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dag(
    dag_id="fhir_ingest",
    description="Incremental FHIR Patient/Observation ingestion from HAPI R4 into Postgres",
    schedule="@hourly",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # small shared server: one task at a time
    dagrun_timeout=timedelta(minutes=50),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=15),
        "execution_timeout": timedelta(minutes=20),
    },
    params={
        "initial_lookback_days": Param(30, type="integer", minimum=1, maximum=30),
        "overlap_minutes": Param(5, type="integer", minimum=0, maximum=60),
        "max_pages": Param(20, type="integer", minimum=1, maximum=200),
        "page_size": Param(100, type="integer", minimum=10, maximum=200),
    },
    tags=["fhir", "portfolio"],
)
def fhir_ingest():
    @task
    def extract(resource_type: str) -> dict:
        from fhir_lib.client import build_session, iter_search_pages
        from fhir_lib.db import advance_watermark, get_watermark, upsert_raw

        ctx = get_current_context()
        p, run_id = ctx["params"], ctx["run_id"]
        conn = _pg_conn()
        try:
            with conn.cursor() as cur:
                wm_before = get_watermark(cur, resource_type)
                conn.commit()
                if wm_before is None:
                    mode = "initial"
                    since = datetime.now(timezone.utc) - timedelta(days=p["initial_lookback_days"])
                else:
                    mode = "incremental"
                    since = wm_before - timedelta(minutes=p["overlap_minutes"])
                query = {
                    "_lastUpdated": f"ge{_fhir_instant(since)}",
                    "_count": p["page_size"],
                    "_sort": "_lastUpdated",  # oldest first: the watermark only moves forward
                }
                stats = {"resource_type": resource_type, "mode": mode,
                         "since": _fhir_instant(since),
                         "watermark_before": wm_before.isoformat() if wm_before else None,
                         "pages": 0, "fetched": 0, "inserted": 0, "updated": 0,
                         "unchanged": 0, "skipped": 0, "truncated": False}
                wm = wm_before
                for page, resources, has_more in iter_search_pages(
                    resource_type, query, max_pages=p["max_pages"], session=build_session()
                ):
                    s = upsert_raw(cur, resource_type, resources, run_id)
                    page_max = s["max_last_updated"]
                    if page_max is not None and (wm is None or page_max > wm):
                        advance_watermark(cur, resource_type, page_max, run_id)
                        wm = page_max
                    conn.commit()  # page rows + watermark land atomically
                    stats["pages"] = page
                    stats["fetched"] += len(resources)
                    for k in ("inserted", "updated", "unchanged", "skipped"):
                        stats[k] += s[k]
                    stats["truncated"] = has_more
        finally:
            conn.close()

        stats["watermark_after"] = wm.isoformat() if wm else None
        log.info("extract stats: %s", stats)

        stalled = stats["truncated"] and wm_before is not None and wm == wm_before
        if stalled:
            raise AirflowFailException(
                f"{resource_type}: watermark did not advance although more pages remain "
                f"(> max_pages*page_size records inside the overlap window). "
                f"Trigger a run with overlap_minutes=0 or a larger max_pages."
            )
        return stats

    @task
    def load_core(resource_type: str, extract_stats: dict) -> dict:
        from fhir_lib.db import load_pending_to_core

        conn = _pg_conn()
        try:
            stats = load_pending_to_core(conn, resource_type)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        log.info("load_core stats: %s (extract: %s)", stats, extract_stats)
        return stats

    for rt in RESOURCES:
        slug = rt.lower()
        stats = extract.override(task_id=f"extract_{slug}")(rt)
        load_core.override(task_id=f"load_core_{slug}")(rt, stats)


fhir_ingest()
