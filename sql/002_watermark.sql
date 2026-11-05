-- Incremental-load state: highest source lastUpdated already landed in raw.
CREATE TABLE IF NOT EXISTS audit.watermark (
    resource_type   text        PRIMARY KEY,
    high_watermark  timestamptz NOT NULL,
    updated_at      timestamptz NOT NULL DEFAULT now(),
    updated_by_run  text
);
