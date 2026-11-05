"""Export the latest healthflow_daily DAG run, and merge the dbt results its Cosmos tasks kept.

    python scripts/export_airflow_run.py --airflow-db $AIRFLOW_HOME/airflow.db \\
        --artifacts dbt/target/cosmos --out dbt/target

Writes dbt/target/airflow_run.json (task states) and dbt/target/run_results.json (every
dbt result from every Cosmos task, merged), for scripts/build_site.py. Exits non-zero if
the DAG run or any task did not succeed, so a failed run is never published.

Reads Airflow's SQLite metadata database directly (the live demo uses `airflow dags test`,
which needs no scheduler or API server).
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path


def task_kind(task_id: str) -> str:
    if task_id.startswith("ingest_") or task_id == "resolve_process_date":
        return "ingestion"
    if task_id == "reconcile":
        return "reconciliation"
    if task_id.startswith("transform."):
        if task_id.endswith("_seed") or ".seed" in task_id:
            return "dbt seed"
        if task_id.endswith(".run"):
            return "dbt model"
        return "dbt test"
    return "other"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--airflow-db", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True, help="Folder of per-task run_results.json files")
    parser.add_argument("--out", type=Path, default=Path("dbt/target"))
    parser.add_argument("--dag-id", default="healthflow_daily")
    args = parser.parse_args()

    db = sqlite3.connect(args.airflow_db)
    db.row_factory = sqlite3.Row
    run = db.execute(
        "SELECT run_id, state, logical_date, start_date, end_date FROM dag_run WHERE dag_id = ? "
        "ORDER BY start_date DESC LIMIT 1",
        (args.dag_id,),
    ).fetchone()
    if run is None:
        sys.exit(f"No run of {args.dag_id} found")
    tasks = [
        dict(r)
        for r in db.execute(
            "SELECT task_id, state, try_number, start_date, end_date FROM task_instance "
            "WHERE dag_id = ? AND run_id = ? ORDER BY start_date",
            (args.dag_id, run["run_id"]),
        )
    ]
    for t in tasks:
        t["kind"] = task_kind(t["task_id"])

    merged, elapsed, generated_at = {}, 0.0, None
    for path in sorted(args.artifacts.glob("*.json")):
        data = json.loads(path.read_text())
        elapsed += data.get("elapsed_time", 0.0)
        generated_at = max(filter(None, [generated_at, data.get("metadata", {}).get("generated_at")]), default=None)
        for result in data["results"]:
            merged[result["unique_id"]] = result  # a retried task's later result wins
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "run_results.json").write_text(
        json.dumps(
            {
                "elapsed_time": round(elapsed, 1),
                "results": list(merged.values()),
                "metadata": {"generated_at": generated_at, "merged_from": "cosmos task artifacts"},
            }
        )
    )

    summary = {
        "dag_id": args.dag_id,
        "run_id": run["run_id"],
        "state": run["state"],
        "logical_date": run["logical_date"],
        "start_date": run["start_date"],
        "end_date": run["end_date"],
        "tasks": tasks,
        "by_kind": dict(Counter(t["kind"] for t in tasks)),
        "by_state": dict(Counter(t["state"] for t in tasks)),
    }
    (args.out / "airflow_run.json").write_text(json.dumps(summary, indent=2, default=str))

    print(f"{args.dag_id} {run['state']}: {summary['by_state']} by kind {summary['by_kind']}")
    print(f"merged {len(merged)} dbt results from {len(list(args.artifacts.glob('*.json')))} Cosmos tasks")
    failed = [t["task_id"] for t in tasks if t["state"] != "success"]
    if run["state"] != "success" or failed:
        sys.exit(f"DAG run did not succeed: {failed}")


if __name__ == "__main__":
    main()
