-- Audit trail: one row per task attempt, plus data-quality check results.

CREATE TABLE IF NOT EXISTS audit.pipeline_run (
    id             bigserial   PRIMARY KEY,
    run_id         text        NOT NULL,
    task_id        text        NOT NULL,
    resource_type  text,
    try_number     integer,
    status         text        NOT NULL CHECK (status IN ('success', 'failed')),
    started_at     timestamptz NOT NULL,
    finished_at    timestamptz NOT NULL DEFAULT now(),
    error          text,
    stats          jsonb
);
CREATE INDEX IF NOT EXISTS ix_pipeline_run_started ON audit.pipeline_run (started_at DESC);
CREATE INDEX IF NOT EXISTS ix_pipeline_run_run_id  ON audit.pipeline_run (run_id);

CREATE OR REPLACE VIEW audit.v_pipeline_run AS
SELECT
    id, run_id, task_id, resource_type, try_number, status,
    started_at, finished_at,
    round(extract(epoch FROM finished_at - started_at)::numeric, 1) AS duration_s,
    stats ->> 'mode'                           AS mode,
    (stats ->> 'pages')::int                   AS pages,
    (stats ->> 'fetched')::int                 AS fetched,
    (stats ->> 'inserted')::int                AS inserted,
    (stats ->> 'updated')::int                 AS updated,
    (stats ->> 'unchanged')::int               AS unchanged,
    (stats ->> 'pending')::int                 AS pending,
    (stats ->> 'rejected')::int                AS rejected,
    (stats ->> 'truncated')::boolean           AS truncated,
    (stats ->> 'watermark_before')::timestamptz AS watermark_before,
    (stats ->> 'watermark_after')::timestamptz  AS watermark_after,
    error
FROM audit.pipeline_run;

CREATE TABLE IF NOT EXISTS audit.quality_check (
    id             bigserial   PRIMARY KEY,
    run_id         text        NOT NULL,
    check_name     text        NOT NULL,
    resource_type  text,
    severity       text        NOT NULL CHECK (severity IN ('critical', 'warning')),
    metric_value   numeric,
    threshold      numeric,
    passed         boolean     NOT NULL,
    details        jsonb,
    checked_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_quality_check_name_time ON audit.quality_check (check_name, checked_at DESC);

CREATE OR REPLACE VIEW audit.v_quality_latest AS
SELECT DISTINCT ON (check_name, resource_type)
    check_name, resource_type, severity, metric_value, threshold, passed, details, checked_at, run_id
FROM audit.quality_check
ORDER BY check_name, resource_type, checked_at DESC;
