-- Raw encounters exactly as received from the source, one row per source row per file.
-- Append-only: a corrected record arrives as a new row from a later file.
-- dbt (staging) casts types and keeps the latest version per encounter id.

CREATE TABLE IF NOT EXISTS raw_staging.encounters (
    -- Synthea encounter columns, kept as text (no interpretation at this layer)
    id                   text,
    start                text,
    stop                 text,
    patient              text,
    organization         text,
    provider             text,
    payer                text,
    encounterclass       text,
    code                 text,
    description          text,
    base_encounter_cost  text,
    total_claim_cost     text,
    payer_coverage       text,
    reasoncode           text,
    reasondescription    text,
    source_updated_at    text,

    -- Ingestion metadata
    _file_id             uuid        NOT NULL REFERENCES audit.ingestion_file_audit (file_id),
    _row_number          integer     NOT NULL,  -- position in the source file (1 = first data row)
    _event_date          date        NOT NULL,  -- when the encounter happened (from START)
    _available_date      date        NOT NULL,  -- when the source delivered it
    _ingested_at         timestamptz NOT NULL DEFAULT now(),

    PRIMARY KEY (_file_id, _row_number)
);

CREATE INDEX IF NOT EXISTS ix_raw_encounters_id ON raw_staging.encounters (id);
