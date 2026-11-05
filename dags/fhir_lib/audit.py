"""Audit trail: one row per task attempt in audit.pipeline_run."""
from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone

log = logging.getLogger(__name__)

INSERT_RUN = """
INSERT INTO audit.pipeline_run
    (run_id, task_id, resource_type, try_number, status, started_at, error, stats)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""


@contextmanager
def audited(connect, *, run_id: str, task_id: str, resource_type: str | None, try_number: int):
    """Record a task attempt (success or failure) in audit.pipeline_run.

    Usage: `with audited(...) as rec: ...; rec["stats"] = stats`.
    Writes on its own connection so a broken task transaction can never
    prevent the audit row; an audit write error never masks the task error.
    """
    started_at = datetime.now(timezone.utc)
    rec: dict = {"stats": None}
    try:
        yield rec
    except BaseException as exc:
        _write(connect, run_id, task_id, resource_type, try_number, "failed", started_at,
               f"{type(exc).__name__}: {exc}"[:2000], rec["stats"])
        raise
    else:
        _write(connect, run_id, task_id, resource_type, try_number, "success", started_at,
               None, rec["stats"])


def _write(connect, run_id, task_id, resource_type, try_number, status, started_at, error, stats):
    try:
        conn = connect()
        try:
            with conn.cursor() as cur:
                cur.execute(INSERT_RUN, (
                    run_id, task_id, resource_type, try_number, status, started_at, error,
                    json.dumps(stats, default=str) if stats is not None else None,
                ))
            conn.commit()
        finally:
            conn.close()
    except Exception:
        log.exception("Could not write audit row for %s/%s", run_id, task_id)
