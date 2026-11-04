"""Milestone 1 acceptance tests, against a real PostgreSQL warehouse.

Covers the review's scenarios: day-by-day loads, idempotent reruns, corrections
appended as new versions, crash recovery, quarantine and missing deliveries.
"""

from __future__ import annotations

import csv
from datetime import date

import pytest

from ingestion import pipeline
from ingestion.contracts import ENCOUNTERS
from ingestion.pipeline import SourceFileMissing, run_ingestion
from simulator.common import fill_days, landing_path, write_feed
from simulator.encounter_feed import build_encounter_feed
from tests.synthetic import make_encounters

FROM, TO = date(2026, 1, 1), date(2026, 1, 10)
COLUMNS = list(ENCOUNTERS.columns)


@pytest.fixture
def landing(tmp_path):
    rows = make_encounters(400, FROM, 10)
    feed = build_encounter_feed(rows, from_date=FROM, to_date=TO, correction_rate=0.1)
    feed = fill_days(feed, FROM, max(feed))
    landing_dir = tmp_path / "landing"
    write_feed(feed, landing_dir, "encounters", COLUMNS)
    return landing_dir, feed, rows


def _scalar(conn, query, *params):
    return conn.execute(query, params).fetchone()[0]


def _raw_count(conn):
    return _scalar(conn, "SELECT count(*) FROM raw_staging.encounters")


def _ingest(conn, storage, landing_dir, day, **kw):
    return run_ingestion(conn, storage, source="encounters", process_date=day, landing_dir=landing_dir, **kw)


def test_rerunning_a_day_creates_no_duplicates(warehouse, storage, landing):
    landing_dir, feed, _ = landing
    day = min(d for d, rows in feed.items() if rows)
    expected = len(feed[day])

    first = _ingest(warehouse, storage, landing_dir, day)
    assert first.count("loaded") == 1 and first.records_loaded == expected

    rerun = _ingest(warehouse, storage, landing_dir, day)
    assert rerun.count("skipped") == 1 and rerun.records_loaded == 0

    assert _raw_count(warehouse) == expected
    assert _scalar(warehouse, "SELECT count(*) FROM audit.ingestion_file_audit") == 1
    assert len(storage.list("bronze/encounters/")) == 1
    assert _scalar(warehouse, "SELECT count(*) FROM audit.pipeline_run_audit WHERE status = 'success'") == 2


def test_incremental_days_and_corrections(warehouse, storage, landing):
    landing_dir, feed, rows = landing
    for day in feed:
        _ingest(warehouse, storage, landing_dir, day)
    # replay the whole range again (a backfill): still nothing new
    for day in feed:
        assert _ingest(warehouse, storage, landing_dir, day).records_loaded == 0

    in_window = {r["Id"] for r in rows if FROM <= date.fromisoformat(r["START"][:10]) <= TO}
    total_sent = sum(len(r) for r in feed.values())
    assert _raw_count(warehouse) == total_sent
    assert _scalar(warehouse, "SELECT count(DISTINCT id) FROM raw_staging.encounters") == len(in_window)

    # Corrections are kept as extra versions; the later version has the newer update time.
    corrected = _scalar(
        warehouse,
        """
        SELECT count(*) FROM (SELECT id FROM raw_staging.encounters GROUP BY id HAVING count(*) > 1) t""",
    )
    assert corrected == total_sent - len(in_window) > 0
    wrong_order = _scalar(
        warehouse,
        """
        SELECT count(*) FROM (
            SELECT id, source_updated_at, _available_date,
                   lag(source_updated_at) OVER (PARTITION BY id ORDER BY _available_date) AS prev
            FROM raw_staging.encounters) t
        WHERE prev IS NOT NULL AND source_updated_at <= prev""",
    )
    assert wrong_order == 0

    # event_date and available_date are different things, and records arrive late
    assert _scalar(warehouse, "SELECT count(*) FROM raw_staging.encounters WHERE _available_date > _event_date") > 0
    assert _scalar(warehouse, "SELECT count(*) FROM raw_staging.encounters WHERE _available_date < _event_date") == 0


def test_crash_mid_load_leaves_nothing_and_rerun_recovers(warehouse, storage, landing, monkeypatch):
    landing_dir, feed, _ = landing
    day = max(feed, key=lambda d: len(feed[d]))
    real_copy = pipeline._copy_rows

    def crash_after_copy(*args, **kwargs):
        real_copy(*args, **kwargs)  # rows are written inside the transaction...
        raise ConnectionError("simulated crash")  # ...then the process dies before commit

    monkeypatch.setattr(pipeline, "_copy_rows", crash_after_copy)
    with pytest.raises(ConnectionError):
        _ingest(warehouse, storage, landing_dir, day, run_id="run-1")

    assert _raw_count(warehouse) == 0
    assert _scalar(warehouse, "SELECT count(*) FROM audit.ingestion_file_audit") == 0
    assert _scalar(warehouse, "SELECT status FROM audit.pipeline_run_audit WHERE run_id = 'run-1'") == "failed"
    assert len(storage.list("bronze/encounters/")) == 1  # Bronze copy was already written

    monkeypatch.setattr(pipeline, "_copy_rows", real_copy)
    retry = _ingest(warehouse, storage, landing_dir, day, run_id="run-1")  # Airflow retry: same run_id
    assert retry.records_loaded == len(feed[day]) == _raw_count(warehouse)
    assert _scalar(warehouse, "SELECT status FROM audit.pipeline_run_audit WHERE run_id = 'run-1'") == "success"
    assert len(storage.list("bronze/encounters/")) == 1  # identical bytes accepted, not duplicated


def test_schema_drift_quarantines_the_whole_file(warehouse, storage, tmp_path):
    day = date(2026, 2, 1)
    path = landing_path(tmp_path, "encounters", day)
    path.parent.mkdir(parents=True)
    rows = make_encounters(5, day, 1)
    drifted = [c if c != "PATIENT" else "PATIENT_ID" for c in COLUMNS]
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(drifted)
        for r in rows:
            writer.writerow([r.get(c, "") for c in COLUMNS])

    summary = _ingest(warehouse, storage, tmp_path, day)
    assert summary.count("quarantined") == 1 and _raw_count(warehouse) == 0
    assert storage.list("bronze/") == []
    assert len(storage.list("quarantine/rejected_files/encounters/")) == 1
    reason = _scalar(warehouse, "SELECT rejection_reason FROM audit.ingestion_file_audit")
    assert "missing columns: PATIENT" in reason

    assert _ingest(warehouse, storage, tmp_path, day).count("skipped") == 1  # same bad file again: no-op


def test_bad_rows_are_quarantined_and_good_rows_load(warehouse, storage, tmp_path):
    day = date(2026, 2, 2)
    rows = [{**r, "SOURCE_UPDATED_AT": r["STOP"]} for r in make_encounters(20, day, 1)]
    rows[3]["Id"] = ""
    write_feed({day: rows}, tmp_path, "encounters", COLUMNS)

    summary = _ingest(warehouse, storage, tmp_path, day)
    assert summary.records_loaded == 19 and summary.records_rejected == 1
    [reject_key] = storage.list("quarantine/invalid_records/encounters/")
    storage.download_file(reject_key, tmp_path / "rejects.csv")
    with open(tmp_path / "rejects.csv", newline="") as fh:
        [rejected] = list(csv.DictReader(fh))
    assert rejected["_row_number"] == "4" and rejected["_rejection_reason"] == "Id is empty"


def test_too_many_bad_rows_rejects_the_file(warehouse, storage, tmp_path):
    day = date(2026, 2, 3)
    rows = [{**r, "SOURCE_UPDATED_AT": r["STOP"], "START": "not-a-date"} for r in make_encounters(10, day, 1)]
    write_feed({day: rows}, tmp_path, "encounters", COLUMNS)
    summary = _ingest(warehouse, storage, tmp_path, day)
    assert summary.count("quarantined") == 1 and _raw_count(warehouse) == 0


def test_missing_delivery_fails_the_run(warehouse, storage, tmp_path):
    with pytest.raises(SourceFileMissing):
        _ingest(warehouse, storage, tmp_path, date(2026, 3, 1), run_id="run-missing")
    row = warehouse.execute(
        "SELECT status, error_message FROM audit.pipeline_run_audit WHERE run_id = 'run-missing'"
    ).fetchone()
    assert row[0] == "failed" and "SourceFileMissing" in row[1]


def test_patients_load_with_death_update_as_new_version(warehouse, storage, tmp_path):
    from simulator.patient_feed import FIELDNAMES, build_patient_feed

    encounters = make_encounters(60, FROM, 5)
    enc_feed = build_encounter_feed(encounters, from_date=FROM, to_date=TO, correction_rate=0)
    patients = [
        {"Id": pid, "BIRTHDATE": "1940-01-01", "DEATHDATE": "2026-01-08" if i == 0 else "", "GENDER": "M"}
        for i, pid in enumerate(sorted({e["PATIENT"] for e in encounters}))
    ]
    feed = fill_days(build_patient_feed(patients, enc_feed, to_date=TO), FROM, TO)
    write_feed(feed, tmp_path, "patients", FIELDNAMES)

    for day in feed:
        run_ingestion(warehouse, storage, source="patients", process_date=day, landing_dir=tmp_path)

    assert _scalar(warehouse, "SELECT count(DISTINCT id) FROM raw_staging.patients") == len(patients)
    versions = warehouse.execute(
        "SELECT deathdate FROM raw_staging.patients WHERE id = %s ORDER BY source_updated_at", (patients[0]["Id"],)
    ).fetchall()
    assert versions == [(None,), ("2026-01-08",)]


def test_identical_empty_files_on_different_days_are_separate_deliveries(warehouse, storage, tmp_path):
    """Regression: two header-only files have the same checksum but are different deliveries."""
    days = [date(2026, 4, 1), date(2026, 4, 2)]
    write_feed({d: [] for d in days}, tmp_path, "encounters", COLUMNS)
    for d in days:
        assert _ingest(warehouse, storage, tmp_path, d).count("loaded") == 1
    assert _scalar(warehouse, "SELECT count(*) FROM audit.ingestion_file_audit") == 2
    assert _scalar(warehouse, "SELECT latest_available_date FROM audit.source_freshness") == days[1]
    assert _ingest(warehouse, storage, tmp_path, days[1]).count("skipped") == 1  # same day again: still a no-op
