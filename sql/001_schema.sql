-- FHIR ingestion schema. Idempotent: safe to re-run.
-- Layers: raw (landing, full JSON) -> core (typed, query-friendly).
-- No FK / CHECK constraints on purpose: source data from the public HAPI
-- server is inconsistent; quality is measured by checks, not enforced on load.

-- ---------- raw ----------
CREATE TABLE IF NOT EXISTS raw.fhir_resource (
    resource_type  text        NOT NULL,
    resource_id    text        NOT NULL,
    version_id     text,
    last_updated   timestamptz NOT NULL,
    payload        jsonb       NOT NULL,
    ingested_at    timestamptz NOT NULL DEFAULT now(),
    run_id         text,
    PRIMARY KEY (resource_type, resource_id)
);
CREATE INDEX IF NOT EXISTS ix_raw_fhir_resource_type_updated
    ON raw.fhir_resource (resource_type, last_updated);

-- ---------- core ----------
CREATE TABLE IF NOT EXISTS core.patient (
    patient_id      text        PRIMARY KEY,
    family_name     text,
    given_names     text,
    gender          text,
    birth_date_raw  text,       -- FHIR allows partial dates (YYYY, YYYY-MM)
    birth_date      date,       -- populated only for full YYYY-MM-DD values
    active          boolean,
    version_id      text,
    last_updated    timestamptz NOT NULL,
    loaded_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.observation (
    observation_id  text        PRIMARY KEY,
    patient_ref     text,       -- original reference, e.g. 'Patient/123'
    patient_id      text,       -- extracted id when the reference is a Patient
    status          text,
    category_code   text,
    code_system     text,
    code            text,
    code_display    text,
    effective_at    timestamptz,
    value_numeric   numeric,
    value_unit      text,
    value_ucum_code text,
    value_text      text,       -- valueString / valueCodeableConcept text
    components      jsonb,      -- e.g. systolic/diastolic blood pressure
    version_id      text,
    last_updated    timestamptz NOT NULL,
    loaded_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_observation_patient_id   ON core.observation (patient_id);
CREATE INDEX IF NOT EXISTS ix_observation_code         ON core.observation (code);
CREATE INDEX IF NOT EXISTS ix_observation_effective_at ON core.observation (effective_at);
