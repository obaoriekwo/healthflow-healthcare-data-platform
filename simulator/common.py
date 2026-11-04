"""Shared helpers for the simulated hospital source systems."""

from __future__ import annotations

import csv
import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path

UPDATED_AT_COLUMN = "SOURCE_UPDATED_AT"

Feed = dict[date, list[dict[str, str]]]  # {available_date: records delivered that day}


def rand(seed: int, key: str, salt: str) -> float:
    """Deterministic pseudo-random number in [0, 1) for one record.

    Derived from a hash, so results do not depend on the order rows are processed in.
    """
    digest = hashlib.sha256(f"{seed}|{key}|{salt}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def fmt_ts(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def fill_days(feed: Feed, first_day: date, last_day: date) -> Feed:
    """A real source sends a file every day, even an empty one.

    Filling gaps means a missing file always means "source did not deliver",
    never "nothing happened that day".
    """
    day = first_day
    while day <= last_day:
        feed.setdefault(day, [])
        day += timedelta(days=1)
    for rows in feed.values():
        rows.sort(key=lambda r: (r[UPDATED_AT_COLUMN], r["Id"]))
    return dict(sorted(feed.items()))


def landing_path(landing_dir: Path, entity: str, day: date) -> Path:
    return Path(landing_dir) / entity / f"available_date={day.isoformat()}" / f"{entity}_{day.isoformat()}.csv"


def write_feed(feed: Feed, landing_dir: Path, entity: str, fieldnames: list[str]) -> list[Path]:
    written = []
    for day, rows in feed.items():
        path = landing_path(landing_dir, entity, day)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        written.append(path)
    return written
