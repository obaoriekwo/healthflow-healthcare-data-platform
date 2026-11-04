-- Raw de-identified patient records, one row per source row per file.
-- Append-only: an update (for example a recorded death) arrives as a new row.

CREATE TABLE IF NOT EXISTS raw_staging.patients (
    id                   text,
    birthdate            text,
    deathdate            text,
    marital              text,
    race                 text,
    ethnicity            text,
    gender               text,
    county               text,
    state                text,
    healthcare_expenses  text,
    healthcare_coverage  text,
    income               text,
    source_updated_at    text,

    _file_id             uuid        NOT NULL REFERENCES audit.ingestion_file_audit (file_id),
    _row_number          integer     NOT NULL,
    _event_date          date        NOT NULL,  -- registration or update date
    _available_date      date        NOT NULL,
    _ingested_at         timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (_file_id, _row_number)
);

CREATE INDEX IF NOT EXISTS ix_raw_patients_id ON raw_staging.patients (id);
