"""FHIR R4 -> Postgres ingestion from the public HAPI server.

extract_<resource>:   paged search (_lastUpdated window) -> upsert raw.fhir_resource
load_core_<resource>: raw rows missing/stale in core -> typed upsert into core.*

Idempotent: re-running never duplicates rows and never overwrites newer data.
Only small stats travel through XCom; data stays in Postgres.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

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


@dag(
    dag_id="fhir_ingest",
    description="Ingest FHIR Patient/Observation from HAPI R4 into Postgres",
    schedule=None,  # manual for now; scheduled + watermark in the next step
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,  # small shared server: one task at a time
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    params={
        "lookback_days": Param(7, type="integer", minimum=1, maximum=30),
        "max_pages": Param(20, type="integer", minimum=1, maximum=200),
        "page_size": Param(100, type="integer", minimum=10, maximum=200),
    },
    tags=["fhir", "portfolio"],
)
def fhir_ingest():
    @task
    def extract(resource_type: str) -> dict:
        from fhir_lib.client import iter_search_pages
        from fhir_lib.db import upsert_raw

        ctx = get_current_context()
        p = ctx["params"]
        since = datetime.now(timezone.utc) - timedelta(days=p["lookback_days"])
        query = {
            "_lastUpdated": f"gt{since.strftime('%Y-%m-%dT%H:%M:%SZ')}",
            "_count": p["page_size"],
            "_sort": "_lastUpdated",
        }
        stats = {"resource_type": resource_type, "since": since.isoformat(), "pages": 0,
                 "fetched": 0, "inserted": 0, "updated": 0, "unchanged": 0,
                 "skipped": 0, "truncated": False}
        conn = _pg_conn()
        try:
            with conn.cursor() as cur:
                for page, resources, has_more in iter_search_pages(
                    resource_type, query, max_pages=p["max_pages"]
                ):
                    s = upsert_raw(cur, resource_type, resources, ctx["run_id"])
                    conn.commit()  # per page: progress survives a later failure
                    stats["pages"] = page
                    stats["fetched"] += len(resources)
                    for k in ("inserted", "updated", "unchanged", "skipped"):
                        stats[k] += s[k]
                    stats["truncated"] = has_more
        finally:
            conn.close()
        log.info("extract stats: %s", stats)
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
