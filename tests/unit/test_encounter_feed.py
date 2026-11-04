from datetime import date, timedelta

from simulator.common import UPDATED_AT_COLUMN, fill_days
from simulator.encounter_feed import build_encounter_feed
from tests.synthetic import make_encounters

FROM, TO = date(2026, 1, 1), date(2026, 1, 10)


def _feed(**kwargs):
    rows = make_encounters(300, FROM, 10)
    return rows, build_encounter_feed(rows, from_date=FROM, to_date=TO, **kwargs)


def test_records_arrive_on_or_after_discharge():
    _, feed = _feed(max_lag_days=2, correction_rate=0)
    for available_date, rows in feed.items():
        for row in rows:
            stop_date = date.fromisoformat(row["STOP"][:10])
            assert stop_date <= available_date <= stop_date + timedelta(days=2)


def test_every_encounter_is_sent_once_without_corrections():
    rows, feed = _feed(correction_rate=0)
    sent = [r["Id"] for day in feed.values() for r in day]
    assert sorted(sent) == sorted(r["Id"] for r in rows)


def test_corrections_arrive_later_with_newer_update_time():
    _, feed = _feed(correction_rate=0.2)
    versions = {}
    for available_date, rows in feed.items():
        for row in rows:
            versions.setdefault(row["Id"], []).append((available_date, row[UPDATED_AT_COLUMN]))
    corrected = {k: v for k, v in versions.items() if len(v) > 1}
    assert corrected, "expected some corrections at a 20% rate"
    for (d1, u1), (d2, u2) in (sorted(v) for v in corrected.values()):
        assert d2 > d1 and u2 > u1


def test_same_seed_gives_identical_feed_regardless_of_row_order():
    rows, _ = _feed()
    a = fill_days(build_encounter_feed(rows, from_date=FROM, to_date=TO, seed=1), FROM, TO + timedelta(30))
    b = fill_days(build_encounter_feed(rows[::-1], from_date=FROM, to_date=TO, seed=1), FROM, TO + timedelta(30))
    assert a == b


def test_fill_days_gives_every_day_a_file():
    rows = make_encounters(5, FROM, 1)
    feed = fill_days(build_encounter_feed(rows, from_date=FROM, to_date=TO, correction_rate=0), FROM, TO)
    assert sorted(feed) == [FROM + timedelta(n) for n in range((TO - FROM).days + 1)]


def test_injected_faults_are_deterministic_and_within_limits(tmp_path):
    import csv

    from simulator.common import landing_path, write_feed
    from simulator.faults import inject_faults

    rows = make_encounters(2000, FROM, 10)
    feed = fill_days(build_encounter_feed(rows, from_date=FROM, to_date=TO, correction_rate=0), FROM, TO)
    columns = [*rows[0], UPDATED_AT_COLUMN]
    write_feed(feed, tmp_path, "encounters", columns)
    faults = inject_faults(feed, tmp_path, "encounters", columns)

    resend = landing_path(tmp_path, "encounters", faults.drifted_file_day).with_name(
        f"encounters_{faults.drifted_file_day}_resend.csv"
    )
    with open(resend, newline="") as fh:
        assert "PATIENT_ID" in next(csv.reader(fh))
    assert faults.bad_rows == 3 and faults.bad_rows_day > faults.drifted_file_day
    with open(landing_path(tmp_path, "encounters", faults.bad_rows_day), newline="") as fh:
        day_rows = list(csv.DictReader(fh))
    assert sum(1 for r in day_rows if not r["Id"]) == 2 and 3 / len(day_rows) <= 0.10
