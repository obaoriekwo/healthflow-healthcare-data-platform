"""HealthFlow daily pipeline.

Today: for each source, wait for that day's delivery, then ingest it
(landing -> Bronze -> raw_staging). Phase 4 adds dbt (Cosmos), data quality and
reconciliation after the ingestion groups.

The day processed is the run's data interval start, so a scheduled run, a retry and a
backfill all resolve to the same delivery date and therefore the same files. Rerunning
any of them is safe: already-ingested files are skipped by checksum.

Backfill (Airflow 3):
    airflow backfill create --dag-id healthflow_daily --from-date 2025-11-01 --to-date 2025-11-30
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.sdk import TaskGroup, dag, get_current_context, task

SOURCES = ("patients", "encounters")


@task
def resolve_process_date() -> str:
    start = get_current_context().get("data_interval_start")
    if start is None:  # manual trigger without a logical date
        raise ValueError("Trigger with a logical date so the run knows which day to ingest")
    return start.date().isoformat()


@task(retries=6, retry_delay=timedelta(minutes=10))
def check_source_delivered(source: str, process_date: str) -> None:
    """Fail (and retry) until the source has delivered that day's file."""
    from ingestion.config import load_settings
    from ingestion.pipeline import landing_files

    if not landing_files(load_settings().landing_dir, source, date.fromisoformat(process_date)):
        raise FileNotFoundError(f"No {source} file for {process_date} yet")


@task
def ingest(source: str, process_date: str) -> dict:
    from ingestion.config import load_settings
    from ingestion.db import connect
    from ingestion.pipeline import run_ingestion
    from ingestion.storage import get_storage

    context = get_current_context()
    settings = load_settings()
    with connect(settings.warehouse_dsn) as conn:
        summary = run_ingestion(
            conn,
            get_storage(settings),
            source=source,
            process_date=date.fromisoformat(process_date),
            landing_dir=settings.landing_dir,
            run_id=f"{context['run_id']}::{source}",  # one audit row per source per run
            dag_id=context["dag"].dag_id,
        )
    return {
        "files_loaded": summary.count("loaded"),
        "files_skipped": summary.count("skipped"),
        "files_quarantined": summary.count("quarantined"),
        "records_loaded": summary.records_loaded,
        "records_rejected": summary.records_rejected,
    }


@dag(
    dag_id="healthflow_daily",
    schedule="@daily",
    start_date=datetime(2025, 11, 1),
    catchup=False,  # backfills are explicit (airflow backfill create), never accidental
    max_active_runs=1,  # one day at a time keeps the audit trail readable
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["healthflow"],
)
def healthflow_daily():
    process_date = resolve_process_date()
    for source in SOURCES:
        with TaskGroup(group_id=f"ingest_{source}"):
            check = check_source_delivered.override(task_id="check_delivered")(source, process_date)
            load = ingest.override(task_id="ingest")(source, process_date)
            check >> load


healthflow_daily()
