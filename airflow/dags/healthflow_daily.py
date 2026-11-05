"""HealthFlow daily pipeline.

    resolve_process_date
      ├─ ingest_patients:   check_delivered → ingest      (landing → Bronze → raw_staging)
      └─ ingest_encounters: check_delivered → ingest
    transform (Astronomer Cosmos: one Airflow task per dbt seed, model and test)
    reconcile            (cross-layer checks, recorded in audit.reconciliation_audit)

The day processed is the run's data interval start, so a scheduled run, a retry and a
backfill all resolve to the same delivery date and therefore the same files. Rerunning
any of them is safe: already-ingested files are skipped by checksum, and dbt rebuilds.

Backfill (Airflow 3):
    airflow backfill create --dag-id healthflow_daily --from-date 2025-11-01 --to-date 2025-11-30

dbt runs from its own virtual environment (DBT_EXECUTABLE), because dbt's dependencies
conflict with Airflow's. Paths come from environment variables so the same DAG runs in
Docker Compose, in GitHub Actions (the live demo) and on a VM.
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow.sdk import TaskGroup, dag, get_current_context, task
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import InvocationMode, LoadMode, SourceRenderingBehavior, TestBehavior

SOURCES = ("patients", "encounters")
DBT_DIR = Path(os.environ.get("HEALTHFLOW_DBT_DIR", "/opt/healthflow/dbt"))
DBT_EXECUTABLE = os.environ.get("DBT_EXECUTABLE", "/opt/dbt-venv/bin/dbt")
DBT_ARTIFACTS_DIR = os.environ.get("HEALTHFLOW_DBT_ARTIFACTS")  # optional: keep each task's run_results.json


def keep_run_results(project_dir: str, context=None, **_) -> None:
    """Cosmos callback: copy the task's dbt run_results.json, so a run's results can be reported."""
    if not DBT_ARTIFACTS_DIR:
        return
    source = Path(project_dir) / "target" / "run_results.json"
    if source.exists():
        target = Path(DBT_ARTIFACTS_DIR)
        target.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, target / f"{context['ti'].task_id}.json")


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


@task
def reconcile(process_date: str) -> list[dict]:
    from ingestion.checks import record_and_raise, run_checks
    from ingestion.config import load_settings
    from ingestion.db import connect

    context = get_current_context()
    settings = load_settings()
    day = date.fromisoformat(process_date)
    with connect(settings.warehouse_dsn) as conn:
        results = run_checks(conn, process_date=day, landing_dir=settings.landing_dir)
        summary = [{"check": r.name, "expected": r.expected, "actual": r.actual, "passed": r.passed} for r in results]
        print(json.dumps(summary, indent=2))
        record_and_raise(conn, results, run_id=context["run_id"], process_date=day)
    return summary


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

    ingestion_groups = []
    for source in SOURCES:
        with TaskGroup(group_id=f"ingest_{source}") as group:
            check = check_source_delivered.override(task_id="check_delivered")(source, process_date)
            load = ingest.override(task_id="ingest")(source, process_date)
            check >> load
        ingestion_groups.append(group)

    transform = DbtTaskGroup(
        group_id="transform",
        project_config=ProjectConfig(dbt_project_path=DBT_DIR),
        profile_config=ProfileConfig(
            profile_name="healthflow",
            target_name="dev",
            profiles_yml_filepath=DBT_DIR / "profiles.yml",
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
        ),
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            dbt_executable_path=DBT_EXECUTABLE,
            invocation_mode=InvocationMode.SUBPROCESS,
            test_behavior=TestBehavior.AFTER_EACH,  # each model's tests run right after it
            should_detach_multiple_parents_tests=True,  # cross-model business rules get their own tasks
            source_rendering_behavior=SourceRenderingBehavior.WITH_TESTS_OR_FRESHNESS,  # raw-table tests + freshness
        ),
        operator_args={"callback": keep_run_results, "install_deps": False},
        default_args={"retries": 1},
    )

    ingestion_groups >> transform >> reconcile(process_date)


healthflow_daily()
