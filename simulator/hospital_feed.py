"""Turn a Synthea CSV snapshot into daily deliveries from simulated hospital source systems.

    python -m simulator.hospital_feed --synthea-csv-dir data/synthea/output/csv \\
        --from-date 2025-11-01 --to-date 2026-10-31

Writes <landing>/<entity>/available_date=YYYY-MM-DD/<entity>_YYYY-MM-DD.csv for:
    encounters   simulator/encounter_feed.py (late arrival, corrections)
    patients     simulator/patient_feed.py   (de-identified, death updates)

Same input and seed always give byte-identical files.

--inject-faults adds one schema-drifted re-send and a few corrupt rows to the encounter
feed (see simulator/faults.py), to show the quarantine working.
"""

from __future__ import annotations

import argparse
import csv
import shutil
from datetime import date
from pathlib import Path

from simulator import encounter_feed, patient_feed
from simulator.common import UPDATED_AT_COLUMN, fill_days, write_feed
from simulator.faults import inject_faults


def _read_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        return list(reader.fieldnames or []), list(reader)


def generate(
    synthea_csv_dir: Path,
    landing_dir: Path,
    *,
    from_date: date,
    to_date: date,
    seed: int = 42,
    max_lag_days: int = 2,
    correction_rate: float = 0.02,
    faults: bool = False,
) -> dict[str, tuple[int, int]]:
    """Write daily landing files for every entity. Returns {entity: (records, files)}."""
    enc_columns, enc_rows = _read_csv(Path(synthea_csv_dir) / "encounters.csv")
    _, patient_rows = _read_csv(Path(synthea_csv_dir) / "patients.csv")

    encounters = encounter_feed.build_encounter_feed(
        enc_rows,
        from_date=from_date,
        to_date=to_date,
        seed=seed,
        max_lag_days=max_lag_days,
        correction_rate=correction_rate,
    )
    patients = patient_feed.build_patient_feed(
        patient_rows, encounters, to_date=to_date, seed=seed, max_lag_days=max_lag_days
    )

    # Nothing can be delivered after to_date: that is "today" for the simulated hospital.
    # Records whose delivery would fall later (patients still in hospital, late arrivals,
    # corrections not yet made) have simply not been sent yet.
    not_yet_sent = {}
    for entity, feed in ((encounter_feed.ENTITY, encounters), (patient_feed.ENTITY, patients)):
        future = [d for d in feed if d > to_date]
        not_yet_sent[entity] = sum(len(feed.pop(d)) for d in future)

    # Both systems deliver a file every day over the same range.
    feeds = {
        encounter_feed.ENTITY: (fill_days(encounters, from_date, to_date), [*enc_columns, UPDATED_AT_COLUMN]),
        patient_feed.ENTITY: (fill_days(patients, from_date, to_date), patient_feed.FIELDNAMES),
    }
    result = {}
    for entity, (feed, fieldnames) in feeds.items():
        paths = write_feed(feed, landing_dir, entity, fieldnames)
        result[entity] = (sum(len(rows) for rows in feed.values()), len(paths))
        if not_yet_sent[entity]:
            print(f"{entity}: {not_yet_sent[entity]} records not sent yet (delivery due after {to_date})")
        if faults and entity == encounter_feed.ENTITY:
            injected = inject_faults(feed, landing_dir, entity, fieldnames, seed=seed)
            print(
                f"injected faults: schema-drifted re-send on {injected.drifted_file_day}, "
                f"{injected.bad_rows} corrupt rows on {injected.bad_rows_day}"
            )
    return result


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--synthea-csv-dir", type=Path, required=True, help="Folder with Synthea's CSV export")
    parser.add_argument("--landing-dir", type=Path, default=Path("data/landing"))
    parser.add_argument("--from-date", type=date.fromisoformat, required=True)
    parser.add_argument("--to-date", type=date.fromisoformat, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-lag-days", type=int, default=2)
    parser.add_argument("--correction-rate", type=float, default=0.02)
    parser.add_argument("--overwrite", action="store_true", help="Delete existing landing files first")
    parser.add_argument("--inject-faults", action="store_true", help="Add a drifted file and corrupt rows")
    args = parser.parse_args(argv)

    for entity in (encounter_feed.ENTITY, patient_feed.ENTITY):
        target = args.landing_dir / entity
        if target.exists() and any(target.iterdir()):
            if not args.overwrite:
                parser.error(f"{target} already has files. Use --overwrite to regenerate.")
            shutil.rmtree(target)

    result = generate(
        args.synthea_csv_dir,
        args.landing_dir,
        from_date=args.from_date,
        to_date=args.to_date,
        seed=args.seed,
        max_lag_days=args.max_lag_days,
        correction_rate=args.correction_rate,
        faults=args.inject_faults,
    )
    for entity, (records, files) in result.items():
        print(f"{entity}: {records} records in {files} daily files under {args.landing_dir / entity}")


if __name__ == "__main__":
    main()
