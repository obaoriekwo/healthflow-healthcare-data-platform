import json
from datetime import date

from scripts.build_site import dbt_results, month_end, month_range


def test_month_range_keeps_only_complete_months():
    assert month_range(date(2025, 11, 1), date(2026, 11, 3))[0] == date(2025, 11, 1)
    assert month_range(date(2025, 11, 1), date(2026, 11, 3))[-1] == date(2026, 10, 1)
    assert len(month_range(date(2025, 11, 1), date(2026, 10, 31))) == 12
    assert month_range(date(2025, 11, 4), date(2025, 12, 31)) == [date(2025, 12, 1)]
    assert month_end(date(2024, 2, 1)) == date(2024, 2, 29)


def test_dbt_results_counts_statuses_and_rule_tests(tmp_path):
    path = tmp_path / "run_results.json"
    path.write_text(
        json.dumps(
            {
                "elapsed_time": 4.21,
                "results": [
                    {"unique_id": "model.healthflow.stg_encounters", "status": "success"},
                    {"unique_id": "seed.healthflow.departments", "status": "success"},
                    {"unique_id": "test.healthflow.unique_stg_encounters_encounter_id.abc", "status": "pass"},
                    {"unique_id": "test.healthflow.assert_hospital_stays_do_not_overlap", "status": "fail"},
                ],
            }
        )
    )
    r = dbt_results(path)
    assert (r["models"], r["seeds"], r["tests"], r["passed"], r["failed"]) == (1, 1, 2, 3, 1)
    assert r["rules"] == [
        {
            "name": "assert_hospital_stays_do_not_overlap",
            "description": "A patient is never in two hospital stays at once (transfers are merged)",
            "status": "fail",
        }
    ]
