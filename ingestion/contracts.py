"""Source contracts: what each source file must look like before it may enter the warehouse.

Two levels of checks:
* file level  - the header must match exactly. A mismatch (missing, extra or renamed
                columns) means schema drift, so the whole file goes to quarantine.
* row level   - each row is checked; bad rows go to quarantine, good rows are loaded.

Row checks are deliberately light. They catch records that are unusable (no id,
unparseable timestamps). Business rules belong in dbt tests.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime

RowCheck = Callable[[dict[str, str]], str | None]  # returns an error message, or None if valid


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass(frozen=True)
class SourceContract:
    name: str
    columns: tuple[str, ...]  # exact header, in order
    raw_table: str  # schema-qualified target table
    event_date: Callable[[dict[str, str]], date]
    row_checks: tuple[RowCheck, ...] = field(default=())

    def header_error(self, header: list[str] | None) -> str | None:
        if not header:
            return "file is empty or has no header"
        if tuple(header) == self.columns:
            return None
        missing = [c for c in self.columns if c not in header]
        extra = [c for c in header if c not in self.columns]
        if not missing and not extra:
            return "columns are in an unexpected order"
        parts = []
        if missing:
            parts.append(f"missing columns: {', '.join(missing)}")
        if extra:
            parts.append(f"unexpected columns: {', '.join(extra)}")
        return "schema drift - " + "; ".join(parts)

    def row_error(self, row: dict[str, str]) -> str | None:
        # csv.DictReader puts surplus values under key None and fills short rows with None.
        if None in row or any(v is None for v in row.values()):
            return "row has the wrong number of fields"
        for check in self.row_checks:
            error = check(row)
            if error:
                return error
        return None


# --- shared checks ------------------------------------------------------------


def _required(column: str) -> RowCheck:
    def check(row: dict[str, str]) -> str | None:
        return None if (row.get(column) or "").strip() else f"{column} is empty"

    return check


def _timestamp(column: str) -> RowCheck:
    def check(row: dict[str, str]) -> str | None:
        if not row.get(column):
            return None
        try:
            parse_ts(row[column])
        except ValueError:
            return f"{column} is not a valid timestamp: {row[column]!r}"
        return None

    return check


def _date(column: str) -> RowCheck:
    def check(row: dict[str, str]) -> str | None:
        if not row.get(column):
            return None
        try:
            date.fromisoformat(row[column])
        except ValueError:
            return f"{column} is not a valid date: {row[column]!r}"
        return None

    return check


def _not_before(later: str, earlier: str, parse: Callable[[str], object]) -> RowCheck:
    """Run after the format checks, so both values are known to parse."""

    def check(row: dict[str, str]) -> str | None:
        if row.get(later) and row.get(earlier) and parse(row[later]) < parse(row[earlier]):
            return f"{later} is before {earlier}"
        return None

    return check


# --- encounters ---------------------------------------------------------------

ENCOUNTERS = SourceContract(
    name="encounters",
    columns=(
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
        "SOURCE_UPDATED_AT",
    ),
    raw_table="raw_staging.encounters",
    event_date=lambda row: parse_ts(row["START"]).date(),
    row_checks=(
        _required("Id"),
        _required("PATIENT"),
        _required("START"),
        _required("SOURCE_UPDATED_AT"),
        _timestamp("START"),
        _timestamp("STOP"),
        _timestamp("SOURCE_UPDATED_AT"),
        _not_before("STOP", "START", parse_ts),
    ),
)

# --- patients -----------------------------------------------------------------
# De-identified registration extract (see simulator/patient_feed.py for what is withheld).

PATIENTS = SourceContract(
    name="patients",
    columns=(
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
        "SOURCE_UPDATED_AT",
    ),
    raw_table="raw_staging.patients",
    # The "event" for a registration record is the registration or update itself.
    event_date=lambda row: parse_ts(row["SOURCE_UPDATED_AT"]).date(),
    row_checks=(
        _required("Id"),
        _required("BIRTHDATE"),
        _required("SOURCE_UPDATED_AT"),
        _date("BIRTHDATE"),
        _date("DEATHDATE"),
        _timestamp("SOURCE_UPDATED_AT"),
        _not_before("DEATHDATE", "BIRTHDATE", date.fromisoformat),
    ),
)

CONTRACTS = {c.name: c for c in (ENCOUNTERS, PATIENTS)}
