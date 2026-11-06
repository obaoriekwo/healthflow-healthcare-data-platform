"""Build the static live-demo site from the warehouse, the audit tables and dbt's results.

    WAREHOUSE_DSN=postgresql://... python scripts/build_site.py --out _site

Reads:
    marts.*                          dashboard figures (gold layer only)
    audit.*                          deliveries, quarantine, replay
    dbt/target/run_results.json      which models and tests ran, and their status
    dbt/target/static_index.html     dbt docs (from `dbt docs generate --static`)
    dbt/target/demo_pipeline.json    written by scripts/demo_pipeline.py
    dbt/target/airflow_run.json      written by scripts/export_airflow_run.py (optional)

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
MATCH = f'<span class="pill good">{ICON_OK}Match</span>'
MISMATCH = f'<span class="pill bad">{ICON_BAD}Mismatch</span>'
LOGO = (
    '<svg width="26" height="26" viewBox="0 0 26 26" aria-hidden="true">'
    '<rect width="26" height="26" rx="5" fill="var(--ink)"/>'
    '<path d="M4 14h4.5l2.5-6 4 11 2.5-5H22" stroke="var(--paper)" stroke-width="2.2" fill="none" '
    'stroke-linecap="round" stroke-linejoin="round"/></svg>'
)
FAVICON = (
    "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 26 26'%3E%3Crect width='26' height='26' rx='5' "
    "fill='%2313263a'/%3E%3Cpath d='M4 14h4.5l2.5-6 4 11 2.5-5H22' stroke='%23f2f5f3' stroke-width='2.2' "
    "fill='none' stroke-linecap='round' stroke-linejoin='round'/%3E%3C/svg%3E"
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


def kpi_placeholders(k: dict) -> dict[str, str]:
    """Headline figures written into the HTML, so they show without JavaScript (and to crawlers)."""

    def pct(n: int, d: int) -> str:
        return f"{100 * n / d:.1f}%" if d else "–"

    return {
        "{{KPI_ENCOUNTERS}}": f"{k['encounters']:,}",
        "{{KPI_PATIENTS}}": f"{k['patients']:,} patients",
        "{{KPI_STAYS}}": f"{k['stays']:,}",
        "{{KPI_STAYS_NOTE}}": f"{k['planned_stays']:,} planned, {k['stays'] - k['planned_stays']:,} unplanned",
        "{{KPI_LOS}}": f"{k['avg_los']:.1f} days",
        "{{KPI_READM}}": pct(k["readmissions"], k["index_stays"]),
        "{{KPI_READM_NOTE}}": f"{k['readmissions']:,} of {k['index_stays']:,} eligible stays",
        "{{KPI_ED}}": pct(k["ed_within_4h"], k["ed_visits"]),
        "{{KPI_ED_NOTE}}": f"of {k['ed_visits']:,} emergency visits",
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


def airflow_section(run: dict | None, reconciliation: list[dict], dbt_tests: int) -> str:
    """The Airflow DAG run that processed the final day, drawn as its task graph, plus reconciliation."""
    if not run:
        return ""
    k = run["by_kind"]
    total = len(run["tasks"])
    ok = run["state"] == "success" and set(run["by_state"]) == {"success"}
    pill = (
        f'<span class="pill good">{ICON_OK}{total} of {total} tasks succeeded</span>'
        if ok
        else f'<span class="pill bad">{ICON_BAD}Run {e(run["state"])}</span>'
    )
    transform = (
        f"{k.get('dbt model', 0)} models, {k.get('dbt seed', 0)} seeds, "
        f"{k.get('dbt test', 0)} test tasks ({dbt_tests} tests)"
    )
    checks = "".join(
        f"<tr><td><code>{e(r['check_name'])}</code></td><td class='wrap-text'>{e(r['detail'] or '')}</td>"
        f"<td class='num'>{r['expected']:,}</td><td class='num'>{r['actual']:,}</td>"
        f"<td>{MATCH if r['passed'] else MISMATCH}</td></tr>"
        for r in reconciliation
    )
    day = fmt_date(str(run["logical_date"])[:10])
    return f"""
  <section class="section" aria-labelledby="orchestration">
    <div class="section-head">
      <div>
        <h2 id="orchestration">Orchestration in Airflow</h2>
        <p>The final day, {e(day)}, runs through the <code>{e(run["dag_id"])}</code> DAG exactly as a daily
        production run would. Astronomer Cosmos turns every dbt seed, model and test into its own task,
        so any one of them can be retried alone.</p>
      </div>
      {pill}
    </div>
    <div class="dag" role="img" aria-label="Task graph: resolve date, then ingest patients and encounters, then transform with dbt, then reconcile">
      <div class="node"><strong>Resolve date</strong><span>from the run's schedule</span></div>
      <div class="edge"></div>
      <div class="stack">
        <div class="node"><strong>Ingest patients</strong><span>wait for file, then load</span></div>
        <div class="node"><strong>Ingest encounters</strong><span>wait for file, then load</span></div>
      </div>
      <div class="edge"></div>
      <div class="node"><strong>Transform with dbt</strong><span>{e(transform)}</span></div>
      <div class="edge"></div>
      <div class="node"><strong>Reconcile</strong><span>{len(reconciliation)} cross-layer checks</span></div>
    </div>
    <div class="panel scroll"><table>
      <thead><tr><th>Reconciliation check</th><th>What it compares</th><th class="num">Expected</th>
        <th class="num">Actual</th><th>Result</th></tr></thead>
      <tbody>{checks}</tbody></table></div>
  </section>"""


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
    reconciliation = query(
        conn,
        """
        SELECT check_name, expected, actual, passed, detail FROM audit.reconciliation_audit
        WHERE run_id = (SELECT run_id FROM audit.reconciliation_audit ORDER BY checked_at DESC LIMIT 1)
        ORDER BY check_name""",
    )
    return {"sources": sources, "issues": issues, "counts": counts, "reconciliation": reconciliation}


# --- rendering --------------------------------------------------------------------

FONTS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link href="https://fonts.googleapis.com/css2?family=Atkinson+Hyperlegible:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet">'
)


def head(title: str, description: str) -> str:
    return f"""<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{e(title)}</title>
<meta name="description" content="{e(description)}">
<meta property="og:title" content="{e(title)}">
<meta property="og:description" content="{e(description)}">
<link rel="icon" href="data:image/svg+xml,{FAVICON}">
{FONTS}
<link rel="stylesheet" href="assets/style.css">
</head>"""


def header(active: str, repo: str) -> str:
    def link(href: str, label: str, key: str) -> str:
        current = ' aria-current="page"' if key == active else ""
        return f'<a href="{href}"{current}>{label}</a>'

    return f"""<header class="top"><div class="wrap">
  <a class="brand" href="index.html">{LOGO}HealthFlow</a>
  <nav aria-label="Main">{link("index.html", "Dashboard", "dashboard")}{link("pipeline.html", "Pipeline", "pipeline")}{link("model/index.html", "Data model", "model")}</nav>
  <a class="source" href="https://github.com/{e(repo)}">Source on GitHub</a>
</div></header>"""


def run_status(meta: dict, checks: dict, show_pipeline_link: bool = True) -> str:
    ok = checks["failed"] == 0
    total = checks["passed"] + checks["failed"] + checks["warned"]
    state = (
        f'<p class="run-state">{ICON_OK}All {checks["passed"]} checks passed</p>'
        if ok
        else f'<p class="run-state">{ICON_BAD}{checks["failed"]} of {total} checks failed</p>'
    )
    links = []
    if show_pipeline_link:
        links.append('<a href="pipeline.html">See how it was built</a>')
    if meta.get("run_url"):
        links.append(f'<a href="{e(meta["run_url"])}">Run log</a>')
    return f"""<aside class="run{"" if ok else " failed"}" aria-label="Latest pipeline run">
      {state}
      <dl>
        <dt>Rebuilt</dt><dd>{e(meta["built_at_text"])}</dd>
        <dt>Data through</dt><dd>{e(fmt_date(meta["window"]["to"]))}</dd>
      </dl>
      <p class="links">{"".join(links)}</p>
    </aside>"""


def flow_strip(c: dict) -> str:
    """Source files to gold, with last night's real counts. Drawn once on page load (CSS)."""
    fmt = "{:,}".format
    stages = [
        ("Source files", fmt(c["files"]), "daily deliveries from two hospital systems"),
        ("Bronze", fmt(c["bronze"]), "immutable copies, by delivery date"),
        ("Raw", fmt(c["raw_rows"]), "rows as delivered, every version kept"),
        ("Staging", fmt(c["stg_encounters"] + c["stg_patients"]), f"latest versions, {fmt(c['corrected'])} corrected"),
        ("Gold", fmt(c["fact_encounters"] + c["fact_stays"]), "fact rows behind these figures"),
    ]
    items = "".join(
        f'<li style="--i:{i}"><span class="stage">{e(name)}</span><span class="count">{count}</span>'
        f'<span class="what">{e(what)}</span></li>'
        for i, (name, count, what) in enumerate(stages)
    )
    return f'<ol class="flow" aria-label="Last night\'s run, from source files to the gold layer">{items}</ol>'


def footer(repo: str) -> str:
    return f"""<footer><div class="wrap">
  <p>HealthFlow is a portfolio project by Oba Oriekwo: an ELT platform built with Python, PostgreSQL, dbt and Airflow,
  rebuilt every night by GitHub Actions. All patient data is synthetic, generated with Synthea; no real patient data is used.
  <a href="https://github.com/{e(repo)}">Source code and documentation</a>.</p>
</div></footer>"""


def render_pipeline(meta: dict, checks: dict, pipe: dict, demo: dict, repo: str, airflow: dict | None = None) -> str:
    replay = demo["replay"]
    first = demo["first_pass"]
    fmt = "{:,}".format

    source_rows = "".join(
        f"<tr><td>{e(s['source_name'].capitalize())}</td><td class='num'>{fmt(s['days'])}</td><td class='num'>{fmt(s['loaded'])}</td>"
        f"<td class='num'>{fmt(s['quarantined'])}</td><td class='num'>{fmt(s['rows_loaded'])}</td>"
        f"<td class='num'>{fmt(s['rows_rejected'])}</td><td class='num'>{e(fmt_date(s['latest']))}</td></tr>"
        for s in pipe["sources"]
    )

    def issue_row(i: dict) -> str:
        if i["ingestion_status"] == "quarantined":
            outcome = f'<span class="pill bad">{ICON_BAD}File quarantined</span>'
            detail = i["rejection_reason"]
        else:
            outcome = '<span class="pill warn">Rows quarantined</span>'
            detail = (
                f"{i['records_rejected']} unusable rows set aside, each with its reason; the rest of the file loaded"
            )
        return (
            f"<tr><td>{e(fmt_date(i['available_date']))}</td><td>{e(i['original_file_name'])}</td>"
            f"<td>{outcome}</td><td class='wrap-text'>{e(detail)}</td></tr>"
        )

    issues = (
        "".join(issue_row(i) for i in pipe["issues"]) or "<tr><td colspan='4'>No problems found in this run.</td></tr>"
    )

    def status_pill(status: str) -> str:
        if status in ("pass", "success"):
            return f'<span class="pill good">{ICON_OK}Pass</span>'
        return f'<span class="pill bad">{ICON_BAD}{e(status)}</span>'

    rules = "".join(
        f"<tr><td class='wrap-text'>{e(r['description'])}</td><td><code>{e(r['name'])}</code></td>"
        f"<td>{status_pill(r['status'])}</td></tr>"
        for r in checks["rules"]
    )

    replay_ok = replay["rows_loaded"] == 0 and replay["raw_rows_before"] == replay["raw_rows_after"]
    replay_pill = (
        f'<span class="pill good">{ICON_OK}0 rows added</span>'
        if replay_ok
        else f'<span class="pill bad">{ICON_BAD}Warehouse changed</span>'
    )
    commit = (
        f' It was built from commit <a href="https://github.com/{e(repo)}/commit/{e(meta["commit"])}"><code>{e(meta["commit"][:7])}</code></a>.'
        if meta.get("commit")
        else ""
    )

    return f"""<!doctype html>
<html lang="en">
{head("HealthFlow pipeline status", "What the HealthFlow pipeline did in its latest nightly run: deliveries, quarantine, Airflow orchestration, replay check and data quality tests.")}
<body>
{header("pipeline", repo)}
<main class="wrap">
  <section class="hero">
    <div>
      <h1>How last night's build ran</h1>
      <p class="lede">
        GitHub Actions starts from an empty database, simulates a year of daily deliveries from two hospital
        systems, ingests them day by day, runs the final day through Airflow and checks every result. If any step
        or test fails, the site is not updated, so what you see always passed.{commit}
      </p>
    </div>
    {run_status(meta, checks, show_pipeline_link=False)}
  </section>

  {flow_strip(pipe["counts"])}

  <section class="section" aria-labelledby="deliveries">
    <div class="section-head"><div>
      <h2 id="deliveries">Deliveries by source</h2>
      <p>One file a day from each simulated hospital system, including days with nothing to send.</p>
    </div></div>
    <div class="panel scroll"><table>
      <thead><tr><th>Source system</th><th class="num">Days delivered</th><th class="num">Files loaded</th>
        <th class="num">Files quarantined</th><th class="num">Rows loaded</th><th class="num">Rows rejected</th>
        <th class="num">Latest delivery</th></tr></thead>
      <tbody>{source_rows}</tbody></table></div>
  </section>

  <section class="section" aria-labelledby="quarantine">
    <div class="section-head"><div>
      <h2 id="quarantine">Quarantine log</h2>
      <p>Each run deliberately sends a re-sent file with a renamed column and a file with a few unusable rows,
      so the defences can be seen working. Quarantined data is kept, never deleted, so it can be fixed and replayed.</p>
    </div></div>
    <div class="panel scroll"><table>
      <thead><tr><th>Delivery date</th><th>File</th><th>Outcome</th><th>Reason</th></tr></thead>
      <tbody>{issues}</tbody></table></div>
  </section>
{airflow_section(airflow, pipe["reconciliation"], checks["tests"])}

  <section class="section" aria-labelledby="replay">
    <div class="section-head"><div>
      <h2 id="replay">Replay check</h2>
      <p>Every day is ingested a second time. The warehouse must not change: rerunning a day is always safe.</p>
    </div>{replay_pill}</div>
    <div class="panel scroll"><table>
      <thead><tr><th>Pass</th><th class="num">Ingestion runs</th><th class="num">Files loaded</th>
        <th class="num">Files skipped as already ingested</th><th class="num">Rows added</th></tr></thead>
      <tbody>
        <tr><td>First load</td><td class="num">{fmt(first["runs"])}</td><td class="num">{fmt(first["files_loaded"])}</td>
          <td class="num">{fmt(first["files_skipped"])}</td><td class="num">{fmt(first["rows_loaded"])}</td></tr>
        <tr><td>Replay of every day</td><td class="num">{fmt(replay["runs"])}</td><td class="num">{fmt(replay["files_loaded"])}</td>
          <td class="num">{fmt(replay["files_skipped"])}</td><td class="num">{fmt(replay["rows_loaded"])}</td></tr>
      </tbody></table></div>
  </section>

  <section class="section" aria-labelledby="quality">
    <div class="section-head"><div>
      <h2 id="quality">Data quality</h2>
      <p>dbt tests check keys, relationships, value ranges and the hospital's business rules after every model.</p>
    </div></div>
    <div class="readings three">
      <div class="reading"><div class="label">Models built</div><div class="value">{checks["models"]}</div><div class="note">plus {checks["seeds"]} reference tables</div></div>
      <div class="reading"><div class="label">Tests passed</div><div class="value">{checks["tests"] - checks["failed"]} of {checks["tests"]}</div><div class="note">keys, relationships, ranges, business rules</div></div>
      <div class="reading"><div class="label">dbt time</div><div class="value">{checks["elapsed"]}s</div><div class="note">all layers, summed across tasks</div></div>
    </div>
    <div class="panel scroll" style="margin-top:20px"><table>
      <thead><tr><th>Business rule</th><th>Test</th><th>Status</th></tr></thead>
      <tbody>{rules}</tbody></table></div>
    <ul class="notes">
      <li>Every model and test is documented, with its lineage, on the <a href="model/index.html">data model</a> page.</li>
      <li>The readmission rules are written out in <a href="https://github.com/{e(repo)}/blob/main/docs/readmission_definition.md">the readmission definition</a>.</li>
    </ul>
  </section>
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
        "built_at_text": f"{fmt_date(built_at.date())}, {built_at.strftime('%H:%M')} UTC",
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
        "{{HEAD}}": head(
            "HealthFlow hospital dashboard",
            "Live demo of HealthFlow, a healthcare data platform: Python ingestion, PostgreSQL, dbt and Airflow, "
            "rebuilt every night by GitHub Actions on synthetic patient data.",
        ),
        "{{HEADER}}": header("dashboard", repo),
        "{{STATUS}}": run_status(meta, checks),
        "{{FLOW}}": flow_strip(pipe["counts"]),
        "{{FOOTER}}": footer(repo),
        "{{WINDOW_FROM}}": fmt_date(months[0]),
        "{{WINDOW_TO}}": fmt_date(month_end(months[-1])),
        "{{REPO}}": e(repo),
        **kpi_placeholders(data["kpis"]),
        "{{CHARTJS_SRC}}": e(args.chartjs_src),
    }.items():
        index = index.replace(key, value)
    (out / "index.html").write_text(index)
    airflow_path = args.target / "airflow_run.json"
    airflow = json.loads(airflow_path.read_text()) if airflow_path.exists() else None
    (out / "pipeline.html").write_text(render_pipeline(meta, checks, pipe, demo, repo, airflow))

    docs = args.target / "static_index.html"
    if docs.exists():
        (out / "model").mkdir()
        shutil.copy(docs, out / "model" / "index.html")

    print(f"Site written to {out}: {checks['passed']} checks passed, {checks['failed']} failed")


if __name__ == "__main__":
    main()
