"""Command line entry point.

python -m ingestion.run init-db
python -m ingestion.run ingest --source encounters --date 2026-01-15
python -m ingestion.run ingest --source encounters --from 2026-01-01 --to 2026-01-31   # backfill
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta

from ingestion.config import load_settings
from ingestion.contracts import CONTRACTS
from ingestion.db import apply_ddl, connect
from ingestion.pipeline import run_ingestion
from ingestion.storage import get_storage


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m ingestion.run")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init-db", help="Create schemas, audit and raw tables")
    ingest = sub.add_parser("ingest", help="Ingest one day, or a date range")
    ingest.add_argument("--source", choices=sorted(CONTRACTS), required=True)
    ingest.add_argument("--date", type=date.fromisoformat)
    ingest.add_argument("--from", dest="from_date", type=date.fromisoformat)
    ingest.add_argument("--to", dest="to_date", type=date.fromisoformat)
    ingest.add_argument("--run-id")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("azure").setLevel(logging.WARNING)  # the SDK logs every HTTP request at INFO
    settings = load_settings()

    with connect(settings.warehouse_dsn) as conn:
        if args.command == "init-db":
            for name in apply_ddl(conn):
                print(f"applied {name}")
            return 0

        if args.date:
            days = [args.date]
        elif args.from_date and args.to_date:
            days = [args.from_date + timedelta(n) for n in range((args.to_date - args.from_date).days + 1)]
        else:
            parser.error("give --date, or both --from and --to")

        storage = get_storage(settings)
        for day in days:
            summary = run_ingestion(
                conn,
                storage,
                source=args.source,
                process_date=day,
                landing_dir=settings.landing_dir,
                run_id=args.run_id if len(days) == 1 else None,
            )
            print(
                f"{day}: loaded={summary.count('loaded')} skipped={summary.count('skipped')} "
                f"quarantined={summary.count('quarantined')} rows={summary.records_loaded} "
                f"rejected_rows={summary.records_rejected}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
