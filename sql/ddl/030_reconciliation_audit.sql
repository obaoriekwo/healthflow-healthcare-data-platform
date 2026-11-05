-- Results of the end-of-run checks the Airflow DAG performs after dbt (ingestion/checks.py).
-- One row per check per run. A failed check fails the DAG run.

CREATE TABLE IF NOT EXISTS audit.reconciliation_audit (
    run_id        text        NOT NULL,
    process_date  date        NOT NULL,
    check_name    text        NOT NULL,
    expected      bigint,
    actual        bigint,
    passed        boolean     NOT NULL,
    detail        text,
    checked_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, check_name)
);
