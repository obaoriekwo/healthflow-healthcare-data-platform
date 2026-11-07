import json
from datetime import date

from scripts.build_site import (
    dbt_results,
    flow_strip,
    history_entry,
    layer_tests,
    merge_history,
    model_metadata,
    month_end,
    month_range,
    replay_note,
    run_history,
    run_history_section,
)


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


def test_run_history_joins_api_runs_to_recorded_counts():
    from datetime import UTC, datetime

    def run(i, event, status, conclusion, start, end):
        return {
            "id": i,
            "run_number": i,
            "run_attempt": 1,
            "html_url": f"https://x/{i}",
            "event": event,
            "status": status,
            "conclusion": conclusion,
            "run_started_at": start,
            "updated_at": end,
            "head_sha": "abcdef1234",
            "head_commit": {"message": "Fix things\n\nbody"},
        }

    api = {
        "workflow_runs": [
            run(3, "push", "in_progress", None, "2026-11-07T10:00:00Z", "2026-11-07T10:05:00Z"),
            run(2, "schedule", "completed", "failure", "2026-11-06T04:17:00Z", "2026-11-06T04:20:00Z"),
            run(1, "workflow_dispatch", "completed", "success", "2026-11-05T04:17:00Z", "2026-11-05T04:25:30Z"),
        ]
    }
    now = datetime(2026, 11, 7, 10, 9, tzinfo=UTC)
    current = history_entry(
        "3",
        {"files": 4, "raw_rows": 100, "fact_encounters": 60, "fact_stays": 5},
        {"tests": 9, "failed": 0, "recon_total": 2, "recon_failed": 0},
        now,
    )
    history = merge_history([{"run_id": 1, "raw_rows": 90, "gold_rows": 50}, {"run_id": None}], current)
    assert [h["run_id"] for h in history] == [1, 3]
    rows = run_history(api, history, "3", now)
    assert [(r["result"], r["trigger"], r["raw_rows"], r["gold_rows"]) for r in rows] == [
        ("current", "Code change", 100, 65),
        ("failure", "Nightly", None, None),
        ("success", "Manual", 90, 50),
    ]
    assert rows[0]["minutes"] == 9 and rows[2]["minutes"] == 8.5 and rows[0]["message"] == "Fix things"
    html = run_history_section(rows, "o/r")
    assert "Failed, not published" in html and "1 passed, 1 failed" in html


def test_flow_strip_reports_each_stage_from_real_counts():
    manifest = {
        "nodes": {
            "model.h.stg_x": {"resource_type": "model", "config": {"schema": "staging"}},
            "model.h.fact_x": {"resource_type": "model", "config": {"schema": "marts"}},
            "test.h.a": {
                "resource_type": "test",
                "attached_node": "model.h.stg_x",
                "depends_on": {"nodes": ["model.h.stg_x"]},
            },
            "test.h.b": {
                "resource_type": "test",
                "attached_node": "model.h.fact_x",
                "depends_on": {"nodes": ["model.h.fact_x"]},
            },
            "test.h.c": {"resource_type": "test", "attached_node": None, "depends_on": {"nodes": ["source.h.raw.enc"]}},
        }
    }
    results = {
        "results": [
            {"unique_id": "test.h.a", "status": "pass"},
            {"unique_id": "test.h.b", "status": "fail"},
            {"unique_id": "test.h.c", "status": "pass"},
            {"unique_id": "model.h.fact_x", "status": "success"},
        ]
    }
    tests = layer_tests(manifest, results)
    assert tests == {
        "staging": {"passed": 1, "failed": 0},
        "marts": {"passed": 0, "failed": 1},
        "raw": {"passed": 1, "failed": 0},
    }
    counts = {
        "files": 10,
        "bronze": 9,
        "raw_rows": 100,
        "stg_encounters": 50,
        "stg_patients": 20,
        "corrected": 2,
        "fact_encounters": 50,
        "fact_stays": 4,
        "quarantined_files": 1,
        "rejected_rows": 0,
    }
    html = flow_strip(counts, tests, replay_ok=True)
    assert 'class="warn"' in html and "1 file quarantined" in html
    assert "replay added 0 rows" in html and "1 source tests passed" in html
    assert 'class="bad"' in html and "1 of 1 tests failed" in html


def test_replay_note_explains_how_file_counts_add_up():
    first = {"runs": 734, "files_loaded": 734}
    replay = {"files_loaded": 0, "files_skipped": 735, "files_quarantined": 0}
    note = replay_note(first, replay, 737)
    assert "734 runs" in note and "saw 735 files" in note and "1 deliberately broken re-send" in note
    assert "other 2 of the 737 files are the final day" in note
