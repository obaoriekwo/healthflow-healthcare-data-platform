"""End-to-end check: ingestion + dbt, run incrementally, must equal a full rebuild.

    HEALTHFLOW_TEST_DSN=postgresql://user:pass@localhost:5433/healthflow_test \\
        python scripts/check_incremental.py [--synthea-csv-dir data/synthea/output/csv]

1. Simulates daily deliveries (from real Synthea output, or a synthetic sample if no
   folder is given).
2. Ingests the first half of the period and runs `dbt build`.
3. Ingests the second half (which includes corrections to records loaded in the first
   half) and runs `dbt build` again, so incremental models take the incremental path.
4. Saves the incremental fact_encounters, runs `dbt build --full-refresh`, and compares.

Exits non-zero if any dbt step fails or the two results differ.

WARNING: drops and recreates the HealthFlow schemas in the target database.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

from psycopg.conninfo import conninfo_to_dict

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from ingestion.db import apply_ddl, connect  # noqa: E402
from ingestion.pipeline import run_ingestion  # noqa: E402
from ingestion.storage.local_storage import LocalStorageClient  # noqa: E402
from simulator.hospital_feed import generate  # noqa: E402
from tests.synthetic import make_encounters, make_patients, write_synthea_csv  # noqa: E402

FROM = date(2026, 1, 1)
DAYS = 120


def dbt(*args: str, env: dict[str, str]) -> None:
    cmd = [os.environ.get("DBT", "dbt"), *args, "--project-dir", str(ROOT / "dbt"), "--profiles-dir", str(ROOT / "dbt")]
    print("$", " ".join(cmd[:3]), flush=True)
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    summary = [line for line in result.stdout.splitlines() if "Done." in line or "ERROR" in line or "FAIL" in line]
    print("\n".join(summary[-15:]))
    if result.returncode != 0:
        sys.exit(f"dbt {' '.join(args)} failed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--synthea-csv-dir", type=Path, help="Real Synthea CSV export (default: synthetic sample)")
    parser.add_argument("--from-date", type=date.fromisoformat, default=FROM)
    parser.add_argument("--days", type=int, default=DAYS)
    args = parser.parse_args()

    dsn = os.environ.get("HEALTHFLOW_TEST_DSN") or sys.exit("Set HEALTHFLOW_TEST_DSN (a throwaway database)")
    info = conninfo_to_dict(dsn)
    env = {
        **os.environ,
        "WAREHOUSE_HOST": str(info.get("host", "localhost")),
        "WAREHOUSE_PORT": str(info.get("port", 5432)),
        "WAREHOUSE_USER": str(info.get("user", "")),
        "WAREHOUSE_PASSWORD": str(info.get("password", "")),
        "WAREHOUSE_DB": str(info.get("dbname", "")),
    }
    to_date = args.from_date + timedelta(days=args.days - 1)
    midpoint = args.from_date + timedelta(days=args.days // 2)

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        synthea_dir = args.synthea_csv_dir
        if synthea_dir is None:
            synthea_dir = tmp_path / "synthea"
            encounters = make_encounters(3000, args.from_date, args.days, seed=11)
            write_synthea_csv(encounters, synthea_dir, "encounters")
            write_synthea_csv(
                make_patients(sorted({e["PATIENT"] for e in encounters}), death_window=(args.from_date, args.days)),
                synthea_dir,
                "patients",
            )
        landing = tmp_path / "landing"
        for entity, (records, files) in generate(
            synthea_dir, landing, from_date=args.from_date, to_date=to_date, correction_rate=0.05
        ).items():
            print(f"simulated {entity}: {records} records in {files} files")
        last_day = max(date.fromisoformat(p.name.split("=")[1]) for p in (landing / "encounters").iterdir())

        storage = LocalStorageClient(tmp_path / "lake")
        with connect(dsn) as conn:
            for schema in ("raw_staging", "staging", "intermediate", "marts", "reference", "audit"):
                conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            conn.execute("DROP TABLE IF EXISTS public.check_incremental_fact")
            apply_ddl(conn)

            def ingest(first: date, last: date) -> None:
                day = first
                while day <= last:
                    for source in ("patients", "encounters"):
                        run_ingestion(conn, storage, source=source, process_date=day, landing_dir=landing)
                    day += timedelta(days=1)

            ingest(args.from_date, midpoint)
            dbt("build", env=env)
            ingest(midpoint + timedelta(days=1), last_day)
            dbt("build", env=env)

            conn.execute("CREATE TABLE public.check_incremental_fact AS SELECT * FROM marts.fact_encounters")
            dbt("build", "--full-refresh", env=env)

            rows, corrected = conn.execute(
                "SELECT count(*), count(*) FILTER (WHERE was_corrected) FROM marts.fact_encounters"
            ).fetchone()
            diff = conn.execute(
                """
                SELECT
                  (SELECT count(*) FROM (SELECT * FROM public.check_incremental_fact
                                         EXCEPT SELECT * FROM marts.fact_encounters) a),
                  (SELECT count(*) FROM (SELECT * FROM marts.fact_encounters
                                         EXCEPT SELECT * FROM public.check_incremental_fact) b)
                """
            ).fetchone()
            conn.execute("DROP TABLE public.check_incremental_fact")

    print(f"fact_encounters: {rows} rows, {corrected} corrected after first delivery")
    if diff != (0, 0):
        sys.exit(f"Incremental and full-refresh results differ: {diff[0]} / {diff[1]} rows")
    print("OK: incremental build matches full refresh")


if __name__ == "__main__":
    main()
