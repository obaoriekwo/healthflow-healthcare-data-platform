"""Simulated patient registration system: sends de-identified patient records.

* A patient is sent on the day of their first encounter in the simulation window
  (registration at arrival).
* If the patient dies later in the window, an updated record with DEATHDATE is sent
  on the date of death plus a 0..max_lag_days delay. The first version has no DEATHDATE.
  This gives the warehouse a real changing attribute to track (dbt snapshot / SCD2).

De-identification happens here, in the "source system", so direct identifiers never
reach HealthFlow at all: names, SSN, driver's licence, passport, street address,
city, ZIP, FIPS and coordinates are not delivered. Synthea's data is synthetic anyway;
this mirrors what a real extract for analytics should look like.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, date, datetime, timedelta

from simulator.common import UPDATED_AT_COLUMN, Feed, fmt_ts, parse_ts, rand

ENTITY = "patients"

DELIVERED_COLUMNS = (
    "Id",
    "BIRTHDATE",
    "DEATHDATE",
    "MARITAL",
    "RACE",
    "ETHNICITY",
    "GENDER",
    "COUNTY",
    "STATE",
    "HEALTHCARE_EXPENSES",
    "HEALTHCARE_COVERAGE",
    "INCOME",
)
FIELDNAMES = [*DELIVERED_COLUMNS, UPDATED_AT_COLUMN]


def build_patient_feed(
    patients: list[dict[str, str]],
    encounter_feed: Feed,
    *,
    to_date: date,
    seed: int = 42,
    max_lag_days: int = 2,
) -> Feed:
    first_start: dict[str, datetime] = {}
    for rows in encounter_feed.values():
        for row in rows:
            start = parse_ts(row["START"])
            if row["PATIENT"] not in first_start or start < first_start[row["PATIENT"]]:
                first_start[row["PATIENT"]] = start

    feed: Feed = defaultdict(list)
    for patient in patients:
        registered_at = first_start.get(patient["Id"])
        if registered_at is None:
            continue  # never seen in the window: the source would not send them
        record = {c: patient.get(c, "") for c in DELIVERED_COLUMNS}
        death = date.fromisoformat(patient["DEATHDATE"]) if patient.get("DEATHDATE") else None

        if death and registered_at.date() <= death:
            # Alive at registration; the death is recorded later (if inside the window).
            feed[registered_at.date()].append({**record, "DEATHDATE": "", UPDATED_AT_COLUMN: fmt_ts(registered_at)})
            if death <= to_date:
                lag = int(rand(seed, patient["Id"], "death_lag") * (max_lag_days + 1))
                updated_at = datetime.combine(death + timedelta(days=lag), datetime.min.time(), UTC)
                updated_at = max(updated_at, registered_at + timedelta(seconds=1))
                feed[updated_at.date()].append({**record, UPDATED_AT_COLUMN: fmt_ts(updated_at)})
        else:
            feed[registered_at.date()].append({**record, UPDATED_AT_COLUMN: fmt_ts(registered_at)})

    return dict(feed)
