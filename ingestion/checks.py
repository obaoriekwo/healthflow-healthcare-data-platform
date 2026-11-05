"""End-of-run reconciliation: does what arrived match what the warehouse holds?

Run by the Airflow DAG after dbt. dbt tests check the models are internally consistent;
these checks compare across layers, from the landing files to the gold layer:

    delivery_<source>         every file in landing for the day has an audit row
    rows_<source>             rows in loaded files = rows in raw_staging (nothing lost or doubled)
    freshness_<source>        the source has delivered up to the processing date
    encounters_source_to_gold every encounter id ever received is in fact_encounters exactly once

Results are written to audit.reconciliation_audit; any failure raises.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ingestion.pipeline import landing_files

SOURCES = ("patients", "encounters")


@dataclass
class CheckResult:
    name: str
    expected: int
    actual: int
    detail: str = ""

    @property
    def passed(self) -> bool:
        return self.expected == self.actual


class ReconciliationError(Exception):
    pass


def _scalar(conn, sql: str, params=()) -> int:
    return int(conn.execute(sql, params).fetchone()[0] or 0)


def run_checks(conn, *, process_date: date, landing_dir: Path) -> list[CheckResult]:
    results: list[CheckResult] = []
    for source in SOURCES:
        files = landing_files(landing_dir, source, process_date)
        recorded = _scalar(
            conn,
            "SELECT count(*) FROM audit.ingestion_file_audit WHERE source_name = %s AND available_date = %s",
            (source, process_date),
        )
        results.append(CheckResult(f"delivery_{source}", len(files), recorded, "files in landing vs audit rows"))

        expected_rows = _scalar(
            conn,
            """SELECT sum(records_loaded) FROM audit.ingestion_file_audit
               WHERE source_name = %s AND ingestion_status = 'loaded'""",
            (source,),
        )
        actual_rows = _scalar(conn, f"SELECT count(*) FROM raw_staging.{source}")  # source is from SOURCES only
        results.append(CheckResult(f"rows_{source}", expected_rows, actual_rows, "audited loaded rows vs raw rows"))

        latest = conn.execute(
            """SELECT max(available_date) FROM audit.ingestion_file_audit
               WHERE source_name = %s AND ingestion_status = 'loaded'""",
            (source,),
        ).fetchone()[0]
        results.append(
            CheckResult(
                f"freshness_{source}",
                1,
                int(latest is not None and latest >= process_date),
                f"latest loaded delivery {latest}, processing {process_date}",
            )
        )

    received = _scalar(conn, "SELECT count(DISTINCT id) FROM raw_staging.encounters")
    in_gold = _scalar(conn, "SELECT count(*) FROM marts.fact_encounters")
    results.append(CheckResult("encounters_source_to_gold", received, in_gold, "distinct ids received vs fact rows"))
    return results


def record_and_raise(conn, results: list[CheckResult], *, run_id: str, process_date: date) -> None:
    with conn.transaction(), conn.cursor() as cur:
        for r in results:
            cur.execute(
                """
                INSERT INTO audit.reconciliation_audit
                    (run_id, process_date, check_name, expected, actual, passed, detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id, check_name) DO UPDATE SET
                    expected = EXCLUDED.expected, actual = EXCLUDED.actual, passed = EXCLUDED.passed,
                    detail = EXCLUDED.detail, checked_at = now()
                """,
                (run_id, process_date, r.name, r.expected, r.actual, r.passed, r.detail),
            )
    failed = [r for r in results if not r.passed]
    if failed:
        raise ReconciliationError(
            "; ".join(f"{r.name}: expected {r.expected}, got {r.actual} ({r.detail})" for r in failed)
        )
