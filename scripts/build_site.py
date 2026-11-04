"""Build the static live-demo site from the warehouse, the audit tables and dbt's results.

    WAREHOUSE_DSN=postgresql://... python scripts/build_site.py --out _site

Reads:
    marts.*                          dashboard figures (gold layer only)
    audit.*                          deliveries, quarantine, replay
    dbt/target/run_results.json      which models and tests ran, and their status
    dbt/target/static_index.html     dbt docs (from `dbt docs generate --static`)
    dbt/target/demo_pipeline.json    written by scripts/demo_pipeline.py

Writes _site/index.html (dashboard), _site/pipeline.html (pipeline status) and
_site/model/index.html (dbt docs). Used by .github/workflows/live-demo.yml.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import shutil
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import psycopg

ROOT = Path(__file__).resolve().parent.parent
SITE_SRC = ROOT / "site"
TARGET = ROOT / "dbt" / "target"
CHARTJS = "https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"

# Plain-English descriptions of the business-rule tests, shown on the pipeline page.
RULE_TESTS = {
    "assert_hospital_stays_do_not_overlap": "A patient is never in two hospital stays at once (transfers are merged)",
    "assert_every_inpatient_encounter_is_in_a_stay": "Every inpatient encounter belongs to exactly one hospital stay",
    "assert_planned_stays_never_count_as_readmissions": "Planned admissions never count as readmissions",
    "assert_readmission_mart_totals_match_stays": "The readmission mart adds up exactly to the stay-level fact table",
    "assert_fact_encounters_reconciles_with_source": "Every encounter received is in the fact table exactly once (source to target)",
    "assert_fact_encounters_has_latest_version": "Corrected records replace earlier versions in the fact table",
}

ICON_OK = (
    '<svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="8" '
    'fill="currentColor"/><path d="M4.5 8.2l2.3 2.3 4.7-4.8" stroke="#fff" stroke-width="1.8" fill="none" '
    'stroke-linecap="round" stroke-linejoin="round"/></svg>'
)
ICON_BAD = (
    '<svg width="16" height="16" viewBox="0 0 16 16" aria-hidden="true"><circle cx="8" cy="8" r="8" '
    'fill="currentColor"/><path d="M5.5 5.5l5 5M10.5 5.5l-5 5" stroke="#fff" stroke-width="1.8" '
    'stroke-linecap="round"/></svg>'
)
LOGO = (
    '<svg width="22" height="22" viewBox="0 0 24 24" aria-hidden="true"><rect width="24" height="24" rx="6" '
    'fill="var(--series-1)"/><path d="M4 13h4l2-5 4 9 2-4h4" stroke="#fff" stroke-width="2" fill="none" '
    'stroke-linecap="round" stroke-linejoin="round"/></svg>'
)

e = html.escape


def fmt_date(value: date | str) -> str:
    d = date.fromisoformat(value) if isinstance(value, str) else value
    return f"{d.day} {d.strftime('%b %Y')}"


def query(conn, sql: str, params=()) -> list[dict]:
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def num(value) -> float:
    return float(value) if value is not None else 0.0


# --- data -----------------------------------------------------------------------


def month_end(m: date) -> date:
    nxt = date(m.year + (m.month == 12), m.month % 12 + 1, 1)
    return date.fromordinal(nxt.toordinal() - 1)


def month_range(window_from: date, window_to: date) -> list[date]:
    """Complete calendar months inside the window."""
    months, m = [], date(window_from.year, window_from.month, 1)
    while True:
        nxt = date(m.year + (m.month == 12), m.month % 12 + 1, 1)
        if nxt.toordinal() - 1 > window_to.toordinal():
            break
        if m >= window_from:
            months.append(m)
        m = nxt
    return months


def dashboard_data(conn, months: list[date], data_through: date, window_days: int = 30) -> dict:
    first, last_excl = months[0], date(months[-1].year + (months[-1].month == 12), months[-1].month % 12 + 1, 1)
    window = (first, last_excl)
    idx = {m: i for i, m in enumerate(months)}
    # A month's readmission rate is only shown once every stay in it has a complete
    # follow-up window: month end + 30 days must be on or before the last data day.
    readmission_months = [m for m in months if month_end(m) + timedelta(days=window_days) <= data_through]
    readmission_end = month_end(readmission_months[-1]) + timedelta(days=1) if readmission_months else first
    zeros = lambda: [0] * len(months)  # noqa: E731

    hospital = {"encounters": zeros(), "ed_visits": zeros(), "ed_within_4h": zeros(), "admissions": zeros()}
    for r in query(
        conn,
        """
        SELECT date_trunc('month', calendar_date)::date AS m, sum(encounters) AS encounters,
               sum(admissions) AS admissions
        FROM marts.mart_hospital_operations WHERE calendar_date >= %s AND calendar_date < %s GROUP BY 1""",
        window,
    ):
        i = idx[r["m"]]
        hospital["encounters"][i], hospital["admissions"][i] = int(r["encounters"]), int(r["admissions"])
    for r in query(
        conn,
        """
        SELECT date_trunc('month', d.calendar_date)::date AS m, sum(ed.ed_visits) AS v, sum(ed.within_4_hours) AS w
        FROM marts.mart_emergency_department ed JOIN marts.dim_date d ON d.date_key = ed.date_key
        WHERE d.calendar_date >= %s AND d.calendar_date < %s GROUP BY 1""",
        window,
    ):
        i = idx[r["m"]]
        hospital["ed_visits"][i], hospital["ed_within_4h"][i] = int(r["v"]), int(r["w"])

    departments = query(
        conn,
        """
        SELECT d.department_key AS code, d.department_name AS name, count(*) AS stays
        FROM marts.fact_hospital_stays s JOIN marts.dim_department d ON d.department_key = s.admitting_department_key
        WHERE s.discharged_at >= %s AND s.discharged_at < %s GROUP BY 1, 2 ORDER BY 3 DESC, 2""",
        window,
    )
    dept_monthly = {
        d["code"]: {"discharges": zeros(), "stay_days": zeros(), "index_stays": zeros(), "readmissions": zeros()}
        for d in departments
    }
    for r in query(
        conn,
        """
        SELECT discharge_month AS m, department_key AS code, discharges, total_stay_days
        FROM marts.mart_department_performance WHERE discharge_month >= %s AND discharge_month < %s""",
        window,
    ):
        if r["code"] in dept_monthly and r["m"] in idx:
            dept_monthly[r["code"]]["discharges"][idx[r["m"]]] = int(r["discharges"])
            dept_monthly[r["code"]]["stay_days"][idx[r["m"]]] = round(num(r["total_stay_days"]), 2)
    for r in query(
        conn,
        """
        SELECT discharge_month AS m, department_key AS code, index_stays, readmissions
        FROM marts.mart_patient_readmissions WHERE discharge_month >= %s AND discharge_month < %s""",
        window,
    ):
        if r["code"] in dept_monthly and r["m"] in readmission_months:
            dept_monthly[r["code"]]["index_stays"][idx[r["m"]]] = int(r["index_stays"])
            dept_monthly[r["code"]]["readmissions"][idx[r["m"]]] = int(r["readmissions"])

    census = query(
        conn,
        """
        SELECT calendar_date, coalesce(midnight_census, 0) AS c FROM marts.mart_hospital_operations
        WHERE calendar_date >= %s AND calendar_date < %s ORDER BY 1""",
        window,
    )

    enc = query(
        conn,
        """
        SELECT count(*) AS n, count(DISTINCT patient_id) AS patients FROM marts.fact_encounters
        WHERE started_at >= %s AND started_at < %s""",
        window,
    )[0]
    stays = query(
        conn,
        """
        SELECT count(*) AS n, count(*) FILTER (WHERE is_planned) AS planned, avg(length_of_stay_days) AS los
        FROM marts.fact_hospital_stays WHERE discharged_at >= %s AND discharged_at < %s""",
        window,
    )[0]
    readm = query(
        conn,
        """
        SELECT coalesce(sum(index_stays), 0) AS idx, coalesce(sum(readmissions), 0) AS r
        FROM marts.mart_patient_readmissions WHERE discharge_month >= %s AND discharge_month < %s""",
        (first, readmission_end),
    )[0]

    dept_table = query(
        conn,
        """
        SELECT d.department_name AS name, count(*) AS discharges, avg(s.length_of_stay_days) AS avg_los,
               100.0 * avg(s.is_planned::int) AS planned_pct,
               count(*) FILTER (WHERE s.included_critical_care) AS critical_care,
               count(*) FILTER (WHERE s.is_index_stay AND s.discharged_at < %s) AS index_stays,
               count(*) FILTER (WHERE s.is_readmitted_within_window AND s.discharged_at < %s) AS readmissions
        FROM marts.fact_hospital_stays s JOIN marts.dim_department d ON d.department_key = s.admitting_department_key
        WHERE s.discharged_at >= %s AND s.discharged_at < %s GROUP BY 1 ORDER BY 2 DESC, 1""",
        (readmission_end, readmission_end, *window),
    )

    return {
        "months": [m.strftime("%Y-%m") for m in months],
        "readmission_months": [m.strftime("%Y-%m") for m in readmission_months],
        "hospital": hospital,
        "departments": [{"code": d["code"], "name": d["name"]} for d in departments],
        "dept_monthly": dept_monthly,
        "census_daily": {
            "dates": [r["calendar_date"].isoformat() for r in census],
            "values": [int(r["c"]) for r in census],
        },
        "kpis": {
            "encounters": int(enc["n"]),
            "patients": int(enc["patients"]),
            "stays": int(stays["n"]),
            "planned_stays": int(stays["planned"]),
            "avg_los": round(num(stays["los"]), 2),
            "index_stays": int(readm["idx"]),
            "readmissions": int(readm["r"]),
            "ed_visits": sum(hospital["ed_visits"]),
            "ed_within_4h": sum(hospital["ed_within_4h"]),
        },
        "dept_table": [{k: (num(v) if k not in ("name",) else v) for k, v in r.items()} for r in dept_table],
    }


def dbt_results(path: Path) -> dict:
    results = json.loads(path.read_text())
    out = {"models": 0, "seeds": 0, "tests": 0, "passed": 0, "failed": 0, "warned": 0, "rules": [], "elapsed": 0.0}
    out["elapsed"] = round(results.get("elapsed_time", 0.0), 1)
    for r in results["results"]:
        kind = r["unique_id"].split(".")[0]
        ok = r["status"] in ("pass", "success")
        if kind == "model":
            out["models"] += 1
        elif kind == "seed":
            out["seeds"] += 1
        elif kind == "test":
            out["tests"] += 1
            name = r["unique_id"].split(".")[2]
            if name in RULE_TESTS:
                out["rules"].append({"name": name, "description": RULE_TESTS[name], "status": r["status"]})
        if r["status"] in ("warn",):
            out["warned"] += 1
        elif ok:
            out["passed"] += 1
        else:
            out["failed"] += 1
    out["rules"].sort(key=lambda x: list(RULE_TESTS).index(x["name"]))
    return out


def pipeline_data(conn) -> dict:
    sources = query(
        conn,
        """
        SELECT source_name, count(DISTINCT available_date) AS days, count(*) AS files,
               count(*) FILTER (WHERE ingestion_status = 'loaded') AS loaded,
               count(*) FILTER (WHERE ingestion_status = 'quarantined') AS quarantined,
               sum(records_loaded) AS rows_loaded, sum(records_rejected) AS rows_rejected,
               max(available_date) AS latest
        FROM audit.ingestion_file_audit GROUP BY 1 ORDER BY 1 DESC""",
    )
    issues = query(
        conn,
        """
        SELECT available_date, source_name, original_file_name, ingestion_status, records_rejected,
               rejection_reason, storage_path
        FROM audit.ingestion_file_audit
        WHERE ingestion_status = 'quarantined' OR records_rejected > 0 ORDER BY available_date""",
    )
    counts = query(
        conn,
        """
        SELECT (SELECT count(*) FROM audit.ingestion_file_audit) AS files,
               (SELECT count(*) FROM audit.ingestion_file_audit WHERE ingestion_status = 'loaded') AS bronze,
               (SELECT count(*) FROM raw_staging.encounters) + (SELECT count(*) FROM raw_staging.patients) AS raw_rows,
               (SELECT count(*) FROM staging.stg_encounters) AS stg_encounters,
               (SELECT count(*) FROM staging.stg_patients) AS stg_patients,
               (SELECT count(*) FROM marts.fact_encounters) AS fact_encounters,
               (SELECT count(*) FROM marts.fact_hospital_stays) AS fact_stays,
               (SELECT count(*) FROM staging.stg_encounters WHERE version_count > 1) AS corrected""",
    )[0]
    return {"sources": sources, "issues": issues, "counts": counts}


# --- rendering --------------------------------------------------------------------


def header(active: str, repo: str) -> str:
    def link(href: str, label: str, key: str) -> str:
        current = ' aria-current="page"' if key == active else ""
        return f'<a href="{href}"{current}>{label}</a>'

    prefix = "../" if active == "model" else ""
    return f"""<header class="top"><div class="wrap">
  <a class="brand" href="{prefix}index.html">{LOGO}HealthFlow</a>
  <nav>{link(prefix + "index.html", "Dashboard", "dashboard")}{link(prefix + "pipeline.html", "Pipeline", "pipeline")}{link(prefix + "model/index.html", "Data model", "model")}</nav>
  <a class="gh" href="https://github.com/{e(repo)}">Source code on GitHub ↗</a>
</div></header>"""


def banner(meta: dict, checks: dict, prefix: str = "") -> str:
    ok = checks["failed"] == 0
    total = checks["passed"] + checks["failed"] + checks["warned"]
    status = (
        f'<span class="status">{ICON_OK}All {checks["passed"]} checks passed</span>'
        if ok
        else f'<span class="status bad">{ICON_BAD}{checks["failed"]} of {total} checks failed</span>'
    )
    run_link = f' · <a href="{e(meta["run_url"])}">Run log</a>' if meta.get("run_url") else ""
    return (
        f'<div class="banner" role="status">{status}'
        f"<span>Rebuilt by the pipeline {e(meta['built_at_text'])}</span>"
        f"<span>Data through {e(fmt_date(meta['window']['to']))}</span>"
        f'<span><a href="{prefix}pipeline.html">How this was built</a>{run_link}</span></div>'
    )


def footer(repo: str) -> str:
    return f"""<footer class="wrap">
  HealthFlow is a portfolio project: an ELT platform built with Python, PostgreSQL, dbt, Airflow and GitHub Actions.
  All patient data is synthetic, generated with Synthea; no real patient data is used.
  <a href="https://github.com/{e(repo)}">Source code and documentation</a>.
</footer>"""


def render_pipeline(meta: dict, checks: dict, pipe: dict, demo: dict, repo: str) -> str:
    c = pipe["counts"]
    replay = demo["replay"]
    first = demo["first_pass"]
    fmt = "{:,}".format

    steps = [
        ("Source deliveries", fmt(c["files"]), "daily files from 2 simulated hospital systems"),
        ("Bronze", fmt(c["bronze"]), "immutable file copies, by delivery date"),
        ("Raw", fmt(c["raw_rows"]), "rows as delivered, every version kept"),
        ("Staging", fmt(c["stg_encounters"] + c["stg_patients"]), f"latest versions; {fmt(c['corrected'])} corrected"),
        ("Gold", fmt(c["fact_encounters"] + c["fact_stays"]), "fact rows behind the dashboard"),
    ]
    flow = "".join(
        f'<div class="step"><div class="k">{e(k)}</div><div class="v">{v}</div><div class="d">{e(d)}</div></div>'
        for k, v, d in steps
    )

    source_rows = "".join(
        f"<tr><td>{e(s['source_name'])}</td><td class='num'>{fmt(s['days'])}</td><td class='num'>{fmt(s['loaded'])}</td>"
        f"<td class='num'>{fmt(s['quarantined'])}</td><td class='num'>{fmt(s['rows_loaded'])}</td>"
        f"<td class='num'>{fmt(s['rows_rejected'])}</td><td class='num'>{e(fmt_date(s['latest']))}</td></tr>"
        for s in pipe["sources"]
    )

    def issue_row(i: dict) -> str:
        if i["ingestion_status"] == "quarantined":
            outcome = '<span class="pill bad">File quarantined</span>'
            detail = i["rejection_reason"]
        else:
            outcome = '<span class="pill warn">Rows quarantined</span>'
            detail = (
                f"{i['records_rejected']} unusable rows set aside with the reason for each; the rest of the file loaded"
            )
        return (
            f"<tr><td>{e(fmt_date(i['available_date']))}</td><td>{e(i['original_file_name'])}</td>"
            f"<td>{outcome}</td><td style='white-space:normal'>{e(detail)}</td></tr>"
        )

    issues = (
        "".join(issue_row(i) for i in pipe["issues"]) or "<tr><td colspan='4'>No problems found in this run.</td></tr>"
    )

    def status_pill(status: str) -> str:
        if status in ("pass", "success"):
            return f'<span class="pill good">{ICON_OK}Pass</span>'
        return f'<span class="pill bad">{ICON_BAD}{e(status)}</span>'

    rules = "".join(
        f"<tr><td style='white-space:normal'>{e(r['description'])}</td><td><code>{e(r['name'])}</code></td>"
        f"<td>{status_pill(r['status'])}</td></tr>"
        for r in checks["rules"]
    )

    replay_ok = replay["rows_loaded"] == 0 and replay["raw_rows_before"] == replay["raw_rows_after"]
    replay_pill = (
        f'<span class="pill good">{ICON_OK}0 rows added</span>'
        if replay_ok
        else f'<span class="pill bad">{ICON_BAD}Changed</span>'
    )
    run_meta = []
    if meta.get("commit"):
        run_meta.append(
            f'commit <a href="https://github.com/{e(repo)}/commit/{e(meta["commit"])}"><code>{e(meta["commit"][:7])}</code></a>'
        )
    if meta.get("run_url"):
        run_meta.append(f'<a href="{e(meta["run_url"])}">full run log on GitHub Actions</a>')

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>HealthFlow · Pipeline status</title>
<meta name="description" content="What the HealthFlow pipeline did in its latest run: deliveries, quarantine, replay check and data quality tests.">
<link rel="stylesheet" href="assets/style.css">
</head>
<body>
{header("pipeline", repo)}
<main class="wrap">
  {banner(meta, checks)}
  <h1>Pipeline status</h1>
  <p class="lede">
    What the pipeline did in its latest run. GitHub Actions starts from an empty database every night, simulates a year
    of daily deliveries from two hospital systems, ingests them day by day, transforms them with dbt and runs every
    data test. If any step or test fails, this site is not updated, so what you see always passed. {" · ".join(run_meta)}
  </p>

  <h2 class="section-title">From source files to dashboard</h2>
  <div class="card flow">{flow}</div>

  <h2 class="section-title">Deliveries by source</h2>
  <div class="card scroll"><table>
    <thead><tr><th>Source system</th><th class="num">Days delivered</th><th class="num">Files loaded</th>
      <th class="num">Files quarantined</th><th class="num">Rows loaded</th><th class="num">Rows rejected</th>
      <th class="num">Latest delivery</th></tr></thead>
    <tbody>{source_rows}</tbody></table></div>

  <h2 class="section-title">Quarantine log</h2>
  <p class="lede">
    The simulator deliberately sends two kinds of bad data each run, so the defences can be seen working: a re-sent file
    with a renamed column (schema drift), and a file containing a few unusable rows. Quarantined data is kept, never deleted,
    so it can be fixed and replayed.
  </p>
  <div class="card scroll"><table>
    <thead><tr><th>Delivery date</th><th>File</th><th>Outcome</th><th>Reason</th></tr></thead>
    <tbody>{issues}</tbody></table></div>

  <h2 class="section-title">Replay check: rerunning is safe</h2>
  <div class="card scroll"><table>
    <thead><tr><th>Pass</th><th class="num">Ingestion runs</th><th class="num">Files loaded</th>
      <th class="num">Files skipped (already ingested)</th><th class="num">Rows added</th><th>Result</th></tr></thead>
    <tbody>
      <tr><td>First load</td><td class="num">{fmt(first["runs"])}</td><td class="num">{fmt(first["files_loaded"])}</td>
        <td class="num">{fmt(first["files_skipped"])}</td><td class="num">{fmt(first["rows_loaded"])}</td><td>Loaded in {first["seconds"]}s</td></tr>
      <tr><td>Replay of every day</td><td class="num">{fmt(replay["runs"])}</td><td class="num">{fmt(replay["files_loaded"])}</td>
        <td class="num">{fmt(replay["files_skipped"])}</td><td class="num">{fmt(replay["rows_loaded"])}</td><td>{replay_pill}</td></tr>
    </tbody></table></div>

  <h2 class="section-title">Data quality</h2>
  <div class="tiles">
    <div class="tile"><div class="label">dbt models built</div><div class="value">{checks["models"]}</div><div class="note">plus {checks["seeds"]} reference tables</div></div>
    <div class="tile"><div class="label">Data tests passed</div><div class="value">{checks["tests"] - checks["failed"]} / {checks["tests"]}</div><div class="note">keys, relationships, ranges, business rules</div></div>
    <div class="tile"><div class="label">dbt run time</div><div class="value">{checks["elapsed"]}s</div><div class="note">full rebuild, all layers</div></div>
  </div>
  <div class="card scroll" style="margin-top:16px"><table>
    <thead><tr><th>Business rule</th><th>Test</th><th>Status</th></tr></thead>
    <tbody>{rules}</tbody></table></div>
  <ul class="notes">
    <li>Every model and test is documented, with its lineage, on the <a href="model/index.html">data model</a> page.</li>
    <li>The 30-day readmission rules are written out in <a href="https://github.com/{e(repo)}/blob/main/docs/readmission_definition.md">docs/readmission_definition.md</a>.</li>
  </ul>
</main>
{footer(repo)}
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", type=Path, default=ROOT / "_site")
    parser.add_argument("--target", type=Path, default=TARGET, help="dbt target folder")
    parser.add_argument("--demo-summary", type=Path, default=TARGET / "demo_pipeline.json")
    parser.add_argument("--chartjs-src", default=CHARTJS)
    args = parser.parse_args()

    dsn = os.environ.get("WAREHOUSE_DSN") or sys.exit("Set WAREHOUSE_DSN")
    repo = os.environ.get("GITHUB_REPOSITORY", "obaoriekwo/healthflow-healthcare-data-platform")
    demo = json.loads(args.demo_summary.read_text())
    checks = dbt_results(args.target / "run_results.json")
    built_at = datetime.now(UTC)
    meta = {
        "window": demo["window"],
        "built_at": built_at.isoformat(timespec="seconds"),
        "built_at_text": f"on {fmt_date(built_at.date())} at {built_at.strftime('%H:%M')} UTC",
        "commit": os.environ.get("GITHUB_SHA"),
        "run_url": (
            f"{os.environ['GITHUB_SERVER_URL']}/{repo}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
            if os.environ.get("GITHUB_RUN_ID")
            else None
        ),
    }

    window_from, window_to = date.fromisoformat(demo["window"]["from"]), date.fromisoformat(demo["window"]["to"])
    months = month_range(window_from, window_to)
    with psycopg.connect(dsn) as conn:
        data_through = query(conn, "SELECT max(start_date) AS d FROM staging.stg_encounters")[0]["d"]
        data = dashboard_data(conn, months, data_through)
        pipe = pipeline_data(conn)
    data["meta"] = meta

    out = args.out
    shutil.rmtree(out, ignore_errors=True)
    shutil.copytree(SITE_SRC / "assets", out / "assets")
    (out / "data.js").write_text("window.HEALTHFLOW = " + json.dumps(data, default=str) + ";\n")
    (out / ".nojekyll").write_text("")

    index = (SITE_SRC / "index.html").read_text()
    for key, value in {
        "{{HEADER}}": header("dashboard", repo),
        "{{BANNER}}": banner(meta, checks),
        "{{FOOTER}}": footer(repo),
        "{{WINDOW_FROM}}": fmt_date(months[0]),
        "{{WINDOW_TO}}": fmt_date(month_end(months[-1])),
        "{{REPO}}": e(repo),
        "{{CHARTJS_SRC}}": e(args.chartjs_src),
    }.items():
        index = index.replace(key, value)
    (out / "index.html").write_text(index)
    (out / "pipeline.html").write_text(render_pipeline(meta, checks, pipe, demo, repo))

    docs = args.target / "static_index.html"
    if docs.exists():
        (out / "model").mkdir()
        shutil.copy(docs, out / "model" / "index.html")

    print(f"Site written to {out}: {checks['passed']} checks passed, {checks['failed']} failed")


if __name__ == "__main__":
    main()
