"""Simulated admissions system: sends encounter records as daily files.

* event_date        when the encounter started (Synthea START)
* available_date    when the record is sent: discharge (Synthea STOP) plus a 0..max_lag_days
                    delay, so records arrive late
* SOURCE_UPDATED_AT added column: when the source last changed the record
* corrections       a small share of encounters is sent again 2-10 days later with a
                    corrected claim cost and a newer SOURCE_UPDATED_AT
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

from simulator.common import UPDATED_AT_COLUMN, Feed, fmt_ts, parse_ts, rand

ENTITY = "encounters"


def build_encounter_feed(
    rows: list[dict[str, str]],
    *,
    from_date: date,
    to_date: date,
    seed: int = 42,
    max_lag_days: int = 2,
    correction_rate: float = 0.02,
) -> Feed:
    """Return {available_date: [records]} for encounters whose START falls in [from_date, to_date]."""
    feed: Feed = defaultdict(list)

    for row in rows:
        start = parse_ts(row["START"])
        if not (from_date <= start.date() <= to_date):
            continue
        stop = parse_ts(row["STOP"]) if row.get("STOP") else start
        lag = int(rand(seed, row["Id"], "lag") * (max_lag_days + 1))
        sent_at = stop + timedelta(days=lag)

        original = {**row, UPDATED_AT_COLUMN: fmt_ts(sent_at)}
        feed[sent_at.date()].append(original)

        if rand(seed, row["Id"], "correct") < correction_rate:
            corrected_at = sent_at + timedelta(days=2 + int(rand(seed, row["Id"], "delay") * 9))
            corrected = dict(original)
            corrected[UPDATED_AT_COLUMN] = fmt_ts(corrected_at)
            if row.get("TOTAL_CLAIM_COST"):
                factor = 0.9 + 0.2 * rand(seed, row["Id"], "cost")
                corrected["TOTAL_CLAIM_COST"] = f"{float(row['TOTAL_CLAIM_COST']) * factor:.2f}"
            feed[corrected_at.date()].append(corrected)

    return dict(feed)
