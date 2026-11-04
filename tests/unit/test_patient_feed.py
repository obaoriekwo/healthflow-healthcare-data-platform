from datetime import date

from simulator.common import UPDATED_AT_COLUMN
from simulator.encounter_feed import build_encounter_feed
from simulator.patient_feed import FIELDNAMES, build_patient_feed
from tests.synthetic import make_encounters

FROM, TO = date(2026, 1, 1), date(2026, 1, 31)

IDENTIFIERS = {"SSN", "DRIVERS", "PASSPORT", "PREFIX", "FIRST", "MIDDLE", "LAST", "SUFFIX", "MAIDEN",
               "ADDRESS", "CITY", "ZIP", "FIPS", "LAT", "LON", "BIRTHPLACE"}  # fmt: skip


def _patient(pid, deathdate=""):
    return {
        "Id": pid, "BIRTHDATE": "1950-05-01", "DEATHDATE": deathdate, "SSN": "999-11-2222",
        "FIRST": "Anna", "LAST": "Example", "ADDRESS": "1 Main St", "GENDER": "F", "STATE": "MA",
    }  # fmt: skip


def _setup(deathdate=""):
    encounters = make_encounters(30, date(2026, 1, 5), 3)
    pid = encounters[0]["PATIENT"]
    enc_feed = build_encounter_feed(encounters, from_date=FROM, to_date=TO, correction_rate=0)
    first_start = min(e["START"] for e in encounters if e["PATIENT"] == pid)
    feed = build_patient_feed([_patient(pid, deathdate), _patient("never-seen")], enc_feed, to_date=TO)
    records = [(d, r) for d, rows in sorted(feed.items()) for r in rows]
    return pid, first_start, records


def test_direct_identifiers_are_never_delivered():
    assert not IDENTIFIERS & set(FIELDNAMES)
    _, _, records = _setup()
    for _, record in records:
        assert not IDENTIFIERS & set(record)


def test_patient_sent_on_first_encounter_day_only_if_seen():
    pid, first_start, records = _setup()
    assert [r["Id"] for _, r in records] == [pid]
    day, record = records[0]
    assert day.isoformat() == first_start[:10] and record[UPDATED_AT_COLUMN] == first_start


def test_death_in_window_arrives_as_a_later_update():
    pid, _, records = _setup(deathdate="2026-01-20")
    (d1, v1), (d2, v2) = records
    assert v1["DEATHDATE"] == "" and v2["DEATHDATE"] == "2026-01-20"
    assert d2 >= date(2026, 1, 20) > d1 and v2[UPDATED_AT_COLUMN] > v1[UPDATED_AT_COLUMN]


def test_death_after_window_is_not_sent_yet():
    _, _, records = _setup(deathdate="2026-03-01")
    assert len(records) == 1 and records[0][1]["DEATHDATE"] == ""
