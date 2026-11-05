from datetime import date

from simulator.hospital_feed import generate
from tests.synthetic import make_encounters, make_patients, write_synthea_csv

FROM, TO = date(2026, 1, 1), date(2026, 1, 31)


def test_nothing_is_delivered_after_the_window_ends(tmp_path):
    """Regression: stays still open at the end of the window were 'delivered' on future dates."""
    encounters = make_encounters(800, FROM, 31)  # inpatient stays of up to 4 days run past TO
    write_synthea_csv(encounters, tmp_path / "synthea", "encounters")
    write_synthea_csv(make_patients(sorted({e["PATIENT"] for e in encounters})), tmp_path / "synthea", "patients")

    generate(tmp_path / "synthea", tmp_path / "landing", from_date=FROM, to_date=TO)

    for entity in ("encounters", "patients"):
        days = sorted(date.fromisoformat(p.name.split("=")[1]) for p in (tmp_path / "landing" / entity).iterdir())
        assert days[0] == FROM and days[-1] == TO and len(days) == 31
