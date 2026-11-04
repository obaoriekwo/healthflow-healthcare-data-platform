"""Deliberate source-system faults, so the pipeline's defences can be seen working.

Real hospital extracts break in two typical ways, and the simulator can produce both:

* schema drift   - a re-sent file arrives with a renamed column. The ingestion must
                   quarantine the whole file (rejected_files) without touching the
                   day's real delivery.
* bad records    - a handful of rows in an otherwise good file are unusable (no id,
                   an unparseable timestamp). The ingestion must quarantine just
                   those rows (invalid_records) and load the rest.

Faults are placed deterministically (from the seed), so every run of the live demo
shows the same, explainable quarantine entries.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date

from simulator.common import Feed, landing_path, rand


@dataclass(frozen=True)
class InjectedFaults:
    drifted_file_day: date | None
    bad_rows_day: date | None
    bad_rows: int


def inject_faults(feed: Feed, landing_dir, entity: str, fieldnames: list[str], seed: int = 42) -> InjectedFaults:
    """Add one schema-drifted re-send and corrupt a few rows in another day's file.

    Must run after the normal files are written. Changes `feed` in place for the
    corrupted rows, and rewrites that day's file.
    """
    # The busiest quarter of days, in date order: faults land on days with plenty of rows.
    by_size = sorted((d for d, rows in feed.items() if rows), key=lambda d: (-len(feed[d]), d))
    busy_days = sorted(by_size[: max(2, len(by_size) // 4)])
    if len(busy_days) < 2:
        return InjectedFaults(None, None, 0)

    # 1. Schema drift: a re-send of one day's file where PATIENT was renamed PATIENT_ID.
    drift_day = busy_days[int(rand(seed, entity, "drift") * len(busy_days) / 2)]
    drifted_header = ["PATIENT_ID" if c == "PATIENT" else c for c in fieldnames]
    path = landing_path(landing_dir, entity, drift_day)
    resend = path.with_name(path.stem + "_resend.csv")
    with open(resend, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(drifted_header)
        for row in feed[drift_day]:
            writer.writerow([row.get(c, "") for c in fieldnames])

    # 2. Bad records: corrupt up to 3 rows on a later busy day, staying within the
    #    10% limit so the file loads and only those rows are quarantined.
    candidates = [d for d in busy_days if d > drift_day] or busy_days
    bad_day = candidates[int(rand(seed, entity, "bad_rows") * len(candidates))]
    rows = feed[bad_day]
    corrupt = [
        ("Id", ""),  # no identifier
        ("START", "2026-13-45T25:00:00Z"),  # impossible timestamp
        ("Id", ""),
    ][: min(3, len(rows) // 10)]
    if not corrupt:
        return InjectedFaults(drift_day, None, 0)
    for i, (column, value) in enumerate(corrupt):
        rows[(i * 7) % len(rows)][column] = value
    with open(landing_path(landing_dir, entity, bad_day), "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    return InjectedFaults(drift_day, bad_day, len(corrupt))
