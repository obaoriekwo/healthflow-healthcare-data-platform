-- HealthFlow warehouse: schemas and audit tables.
-- Idempotent: safe to run on every deploy (scripts/init_warehouse.py).

CREATE SCHEMA IF NOT EXISTS raw_staging;   -- loaded by Python, append-only, all source columns as text
CREATE SCHEMA IF NOT EXISTS staging;       -- dbt: cleaned, typed, deduplicated
CREATE SCHEMA IF NOT EXISTS intermediate;  -- dbt: reusable business logic
CREATE SCHEMA IF NOT EXISTS marts;         -- dbt: facts, dimensions, KPI marts
CREATE SCHEMA IF NOT EXISTS audit;         -- pipeline observability

-- One row per pipeline run (one source, one processing date).
CREATE TABLE IF NOT EXISTS audit.pipeline_run_audit (
    run_id             text PRIMARY KEY,
    dag_id             text,
    source_name        text        NOT NULL,
    process_date       date        NOT NULL,
    start_time         timestamptz NOT NULL DEFAULT now(),
    end_time           timestamptz,
    status             text        NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    files_seen         integer     NOT NULL DEFAULT 0,
    files_loaded       integer     NOT NULL DEFAULT 0,
    files_skipped      integer     NOT NULL DEFAULT 0,  -- already ingested (same checksum)
    files_quarantined  integer     NOT NULL DEFAULT 0,
    records_loaded     integer     NOT NULL DEFAULT 0,
    records_rejected   integer     NOT NULL DEFAULT 0,
    error_message      text
);

-- One row per distinct delivery ever received: (source, delivery date, file checksum).
-- The UNIQUE constraint is what makes re-delivery of the same file for the same day,
-- or a rerun of that day, a no-op.
CREATE TABLE IF NOT EXISTS audit.ingestion_file_audit (
    file_id             uuid PRIMARY KEY,            -- uuid5(source|date|checksum): stable across reruns
    source_name         text        NOT NULL,
    original_file_name  text        NOT NULL,
    available_date      date        NOT NULL,        -- the day the source delivered it
    file_checksum       char(64)    NOT NULL,        -- sha256 of the file bytes
    storage_path        text        NOT NULL,        -- Bronze path, or quarantine path if rejected
    run_id              text        NOT NULL,
    record_count        integer     NOT NULL,
    records_loaded      integer     NOT NULL,
    records_rejected    integer     NOT NULL,
    ingestion_status    text        NOT NULL CHECK (ingestion_status IN ('loaded', 'quarantined')),
    rejection_reason    text,
    ingested_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (source_name, available_date, file_checksum)
);

CREATE INDEX IF NOT EXISTS ix_file_audit_source_date
    ON audit.ingestion_file_audit (source_name, available_date);

-- Freshness: last successful delivery per source (Power BI / alerting can read this).
CREATE OR REPLACE VIEW audit.source_freshness AS
SELECT source_name,
       max(available_date) AS latest_available_date,
       max(ingested_at)    AS latest_ingested_at,
       count(*)            AS files_loaded
FROM audit.ingestion_file_audit
WHERE ingestion_status = 'loaded'
GROUP BY source_name;
