"""Run the ingestion half of the live demo, end to end, and record what happened.

    WAREHOUSE_DSN=postgresql://... python scripts/demo_pipeline.py \\
        --synthea-csv-dir data/synthea/output/csv --reset

1. Works out the simulation window: from the first day of the month a year before,
   to the day before the last encounter Synthea generated (Synthea simulates up to
   the day it runs). That gives 12 complete months for the dashboard.
2. Simulates a year of daily deliveries, with faults injected (a schema-drifted
   re-send and a few corrupt rows) so the quarantine has something to catch.
3. Ingests every day in order, patients then encounters, as the Airflow DAG does.
   With --leave-last-day the final day is left for the Airflow DAG to process.
4. Replays every day a second time and checks the warehouse did not change.
5. Writes a summary to dbt/target/demo_pipeline.json for scripts/build_site.py.

Run dbt build afterwards. Used by .github/workflows/live-demo.yml.

WARNING: --reset drops and recreates the HealthFlow schemas in the target database.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ingestion.config import load_settings  # noqa: E402
from ingestion.db import apply_ddl, connect  # noqa: E402
from ingestion.pipeline import run_ingestion  # noqa: E402
from ingestion.storage import get_storage  # noqa: E402
from ingestion.storage.local_storage import LocalStorageClient  # noqa: E402
from simulator.hospital_feed import generate  # noqa: E402

SOURCES = ("patients", "encounters")
SCHEMAS = ("raw_staging", "staging", "intermediate", "marts", "reference", "audit")


def last_complete_day(encounters_csv: Path) -> date:
    with open(encounters_csv, newline="", encoding="utf-8") as fh:
        latest = max(row["START"][:10] for row in csv.DictReader(fh))
    return date.fromisoformat(latest) - timedelta(days=1)


def raw_counts(conn) -> dict[str, int]:
    return {s: conn.execute(f"SELECT count(*) FROM raw_staging.{s}").fetchone()[0] for s in SOURCES}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--synthea-csv-dir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, default=ROOT / "data" / "demo")
    parser.add_argument("--months", type=int, default=12, help="Complete months to simulate")
    parser.add_argument("--reset", action="store_true", help="Drop and recreate the HealthFlow schemas first")
    parser.add_argument(
        "--leave-last-day",
        action="store_true",
        help="Do not ingest the final day, so the Airflow DAG can process it (the live demo does this)",
    )
    parser.add_argument("--summary", type=Path, default=ROOT / "dbt" / "target" / "demo_pipeline.json")
    args = parser.parse_args()

    dsn = os.environ.get("WAREHOUSE_DSN") or sys.exit("Set WAREHOUSE_DSN")
    to_date = last_complete_day(args.synthea_csv_dir / "encounters.csv")
    month_start = to_date.replace(day=1)
    if (to_date + timedelta(days=1)).day == 1:  # to_date is a month end: that month is complete
        month_start = to_date + timedelta(days=1)
    year, month = divmod(month_start.year * 12 + month_start.month - 1 - args.months, 12)
    from_date = date(year, month + 1, 1)
    print(f"simulation window: {from_date} to {to_date}")

    landing = args.work_dir / "landing"
    lake = args.work_dir / "lake"
    for path in (landing, lake):
        shutil.rmtree(path, ignore_errors=True)

    generated = generate(args.synthea_csv_dir, landing, from_date=from_date, to_date=to_date, faults=True)
    for entity, (records, files) in generated.items():
        print(f"simulated {entity}: {records} records in {files} daily files")
    last_day = to_date  # the simulator never delivers after the window ends
    ingest_until = last_day - timedelta(days=1) if args.leave_last_day else last_day

    # Bronze goes to a local folder by default; with STORAGE_BACKEND=azure it goes to Azure
    # Blob / ADLS Gen2 (Azurite locally, a real storage account in Azure).
    settings = load_settings()
    storage = get_storage(settings) if settings.storage_backend == "azure" else LocalStorageClient(lake)
    print(f"Bronze storage: {type(storage).__name__}")
    with connect(dsn) as conn:
        if args.reset:
            for schema in SCHEMAS:
                conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        apply_ddl(conn)

        def ingest_all(label: str) -> dict[str, int]:
            totals = {"runs": 0, "files_loaded": 0, "files_skipped": 0, "files_quarantined": 0, "rows_loaded": 0}
            day = from_date
            while day <= ingest_until:
                for source in SOURCES:
                    summary = run_ingestion(
                        conn,
                        storage,
                        source=source,
                        process_date=day,
                        landing_dir=landing,
                        run_id=f"demo-{label}__{source}__{day.isoformat()}",
                        dag_id="live_demo",
                    )
                    totals["runs"] += 1
                    totals["files_loaded"] += summary.count("loaded")
                    totals["files_skipped"] += summary.count("skipped")
                    totals["files_quarantined"] += summary.count("quarantined")
                    totals["rows_loaded"] += summary.records_loaded
                day += timedelta(days=1)
            return totals

        started = time.monotonic()
        first = ingest_all("load")
        load_seconds = round(time.monotonic() - started, 1)
        print(f"first pass: {first} in {load_seconds}s")

        before = raw_counts(conn)
        replay = ingest_all("replay")
        after = raw_counts(conn)
        print(f"replay pass: {replay}")
        if before != after or replay["rows_loaded"] != 0:
            sys.exit(f"Replay changed the warehouse: {before} -> {after}")

    args.summary.parent.mkdir(parents=True, exist_ok=True)
    args.summary.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
                "window": {
                    "from": from_date.isoformat(),
                    "to": to_date.isoformat(),
                    "last_delivery": last_day.isoformat(),
                    "ingested_by_script_until": ingest_until.isoformat(),
                },
                "simulated": {k: {"records": v[0], "files": v[1]} for k, v in generated.items()},
                "first_pass": {**first, "seconds": load_seconds},
                "replay": {**replay, "raw_rows_before": before, "raw_rows_after": after},
            },
            indent=2,
        )
    )
    print(f"OK: replay of {replay['runs']} runs added 0 rows. Summary written to {args.summary}")


if __name__ == "__main__":
    main()
