import json
from datetime import date

from scripts.build_site import dbt_results, model_metadata, month_end, month_range


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


def test_model_metadata_reads_keys_and_relationships_from_dbt_tests():
    def model(name, schema, cols):
        return {
            "unique_id": f"model.healthflow.{name}",
            "resource_type": "model",
            "name": name,
            "config": {"schema": schema, "materialized": "table"},
            "description": f"{name}. More detail.",
            "columns": {c: {"description": f"{c} desc"} for c in cols},
        }

    def test(name, model_name, column, kwargs=None):
        return {
            "resource_type": "test",
            "attached_node": f"model.healthflow.{model_name}",
            "column_name": column,
            "depends_on": {"nodes": [f"model.healthflow.{model_name}"]},
            "test_metadata": {"name": name, "kwargs": kwargs or {}},
        }

    nodes = {
        "m1": model("dim_patient", "marts", ["patient_key"]),
        "m2": model("fact_encounters", "marts", ["encounter_id", "patient_key"]),
        "m3": model("stg_encounters", "staging", ["encounter_id"]),
        "t1": test("unique", "dim_patient", "patient_key"),
        "t2": test("not_null", "dim_patient", "patient_key"),
        "t3": test("unique", "fact_encounters", "encounter_id"),
        "t4": test("not_null", "fact_encounters", "encounter_id"),
        "t5": test(
            "relationships",
            "fact_encounters",
            "patient_key",
            {"to": "ref('dim_patient')", "field": "patient_key", "column_name": "patient_key"},
        ),
    }
    catalog = {
        "nodes": {
            "model.healthflow.dim_patient": {"columns": {"patient_key": {"type": "text", "index": 1}}},
            "model.healthflow.fact_encounters": {
                "columns": {
                    "patient_key": {"type": "text", "index": 2},
                    "encounter_id": {"type": "character varying", "index": 1},
                }
            },
        }
    }
    md = model_metadata({"nodes": nodes, "sources": {}}, catalog)
    assert list(md["tables"]) == ["dim_patient", "fact_encounters"]  # gold layer only
    fact = md["tables"]["fact_encounters"]
    assert [c["name"] for c in fact["columns"]] == ["encounter_id", "patient_key"]  # catalog order
    assert fact["columns"][0] | {"tests": None} == {
        "name": "encounter_id",
        "type": "varchar",
        "pk": True,
        "fk": None,
        "tests": None,
        "description": "encounter_id desc",
    }
    assert fact["columns"][1]["fk"] == "dim_patient.patient_key" and not fact["columns"][1]["pk"]
    assert fact["description"] == "fact_encounters."
    assert md["relationships"] == [
        {"from": "fact_encounters", "column": "patient_key", "to": "dim_patient", "field": "patient_key"}
    ]
    assert md["tests"] == 5
