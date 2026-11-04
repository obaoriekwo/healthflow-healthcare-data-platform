from datetime import date

from ingestion.contracts import ENCOUNTERS, PATIENTS
from tests.synthetic import make_encounters


def _row(**overrides):
    row = make_encounters(1, date(2026, 1, 1), 1)[0]
    row["SOURCE_UPDATED_AT"] = row["STOP"]
    row.update(overrides)
    return row


def _patient(**overrides):
    row = {c: "" for c in PATIENTS.columns}
    row.update(Id="p1", BIRTHDATE="1950-01-01", GENDER="F", SOURCE_UPDATED_AT="2026-01-01T10:00:00Z")
    row.update(overrides)
    return row


def test_exact_header_passes():
    assert ENCOUNTERS.header_error(list(ENCOUNTERS.columns)) is None


def test_missing_and_extra_columns_are_schema_drift():
    header = [c for c in ENCOUNTERS.columns if c != "PATIENT"] + ["PATIENT_ID"]
    error = ENCOUNTERS.header_error(header)
    assert "missing columns: PATIENT" in error and "unexpected columns: PATIENT_ID" in error


def test_reordered_columns_are_rejected():
    assert "order" in ENCOUNTERS.header_error(list(reversed(ENCOUNTERS.columns)))


def test_valid_row_passes():
    assert ENCOUNTERS.row_error(_row()) is None


def test_row_checks():
    assert ENCOUNTERS.row_error(_row(Id="")) == "Id is empty"
    assert "not a valid timestamp" in ENCOUNTERS.row_error(_row(START="yesterday"))
    assert (
        ENCOUNTERS.row_error(_row(START="2026-01-02T10:00:00Z", STOP="2026-01-01T10:00:00Z")) == "STOP is before START"
    )


def test_short_or_long_rows_are_rejected():
    short = _row()
    short["REASONDESCRIPTION"] = None
    assert ENCOUNTERS.row_error(short) == "row has the wrong number of fields"
    long = _row()
    long[None] = ["surplus"]
    assert ENCOUNTERS.row_error(long) == "row has the wrong number of fields"


def test_update_time_is_required():
    assert ENCOUNTERS.row_error(_row(SOURCE_UPDATED_AT="")) == "SOURCE_UPDATED_AT is empty"


def test_patient_checks():
    assert PATIENTS.row_error(_patient()) is None
    assert PATIENTS.row_error(_patient(DEATHDATE="2020-02-02")) is None
    assert "not a valid date" in PATIENTS.row_error(_patient(BIRTHDATE="01/01/1950"))
    assert PATIENTS.row_error(_patient(DEATHDATE="1949-12-31")) == "DEATHDATE is before BIRTHDATE"
