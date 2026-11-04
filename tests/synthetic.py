"""Small Synthea-shaped encounter rows for tests, so tests never need Java or Synthea."""

from __future__ import annotations

import csv
import random
import uuid
from datetime import date, datetime, timedelta
from pathlib import Path

SYNTHEA_ENCOUNTER_COLUMNS = [
    "Id",
    "START",
    "STOP",
    "PATIENT",
    "ORGANIZATION",
    "PROVIDER",
    "PAYER",
    "ENCOUNTERCLASS",
    "CODE",
    "DESCRIPTION",
    "BASE_ENCOUNTER_COST",
    "TOTAL_CLAIM_COST",
    "PAYER_COVERAGE",
    "REASONCODE",
    "REASONDESCRIPTION",
]

CLASSES = [("ambulatory", 0), ("emergency", 0), ("inpatient", 4), ("wellness", 0)]

# (code, description, reason code, reason description) per class, using real Synthea codes
# so the dbt seeds (departments, planned admissions) are exercised.
CODES = {
    "ambulatory": [("185349003", "Encounter for check up (procedure)", "", "")],
    "wellness": [("410620009", "Well child visit (procedure)", "", "")],
    "emergency": [("50849002", "Emergency room admission (procedure)", "", "")],
    "inpatient": [
        ("32485007", "Hospital admission (procedure)", "", ""),
        ("185347001", "Encounter for problem (procedure)", "74400008", "Appendicitis (disorder)"),
        ("185347001", "Encounter for problem (procedure)", "424132000",
         "Non-small cell carcinoma of lung  TNM stage 1 (disorder)"),
        ("56876005", "Drug rehabilitation and detoxification (regime/therapy)", "6525002",
         "Dependent drug abuse (disorder)"),
    ],
}  # fmt: skip


def make_encounters(n: int, first_day: date, days: int, seed: int = 7) -> list[dict[str, str]]:
    rng = random.Random(seed)
    patients = [str(uuid.UUID(int=rng.getrandbits(128))) for _ in range(max(1, n // 3))]
    rows = []
    for _ in range(n):
        cls, max_stay = rng.choice(CLASSES)
        start = datetime.combine(first_day + timedelta(rng.randrange(days)), datetime.min.time()) + timedelta(
            minutes=rng.randrange(24 * 60)
        )
        stop = start + timedelta(days=rng.randint(0, max_stay), minutes=rng.randint(15, 240))
        cost = rng.uniform(50, 5000)
        code, description, reason_code, reason_description = rng.choice(CODES[cls])
        rows.append(
            {
                "Id": str(uuid.UUID(int=rng.getrandbits(128))),
                "START": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "STOP": stop.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "PATIENT": rng.choice(patients),
                "ORGANIZATION": "org-1",
                "PROVIDER": "prov-1",
                "PAYER": "payer-1",
                "ENCOUNTERCLASS": cls,
                "CODE": code,
                "DESCRIPTION": description,
                "BASE_ENCOUNTER_COST": "85.55",
                "TOTAL_CLAIM_COST": f"{cost:.2f}",
                "PAYER_COVERAGE": f"{cost * 0.8:.2f}",
                "REASONCODE": reason_code,
                "REASONDESCRIPTION": reason_description,
            }
        )
    return rows


def make_patients(
    patient_ids: list[str], seed: int = 7, death_rate: float = 0.03, death_window: tuple[date, int] | None = None
) -> list[dict[str, str]]:
    """Synthea-shaped patients, including the identifier columns the simulator must withhold."""
    rng = random.Random(seed)
    rows = []
    for pid in sorted(patient_ids):
        birth = date(1930, 1, 1) + timedelta(days=rng.randrange(80 * 365))
        death = ""
        if death_window and rng.random() < death_rate:
            start, span = death_window
            death = (start + timedelta(days=rng.randrange(span))).isoformat()
        rows.append({
            "Id": pid, "BIRTHDATE": birth.isoformat(), "DEATHDATE": death, "SSN": "999-00-0000",
            "DRIVERS": "S99999999", "PASSPORT": "X0000000X", "PREFIX": "", "FIRST": "Test", "MIDDLE": "",
            "LAST": "Patient", "SUFFIX": "", "MAIDEN": "", "MARITAL": rng.choice(["M", "S", "D", "W", ""]),
            "RACE": rng.choice(["white", "black", "asian", "other"]), "ETHNICITY": "nonhispanic",
            "GENDER": rng.choice(["M", "F"]), "BIRTHPLACE": "Boston", "ADDRESS": "1 Test St", "CITY": "Boston",
            "STATE": "Massachusetts", "COUNTY": "Suffolk County", "FIPS": "25025", "ZIP": "02110",
            "LAT": "42.36", "LON": "-71.06", "HEALTHCARE_EXPENSES": "1000.00", "HEALTHCARE_COVERAGE": "500.00",
            "INCOME": str(rng.randrange(20000, 150000)),
        })  # fmt: skip
    return rows


def write_synthea_csv(rows: list[dict[str, str]], folder: Path, name: str = "encounters") -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{name}.csv"
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0]) if rows else SYNTHEA_ENCOUNTER_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    return path
