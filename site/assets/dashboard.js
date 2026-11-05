/* HealthFlow dashboard: renders window.HEALTHFLOW (written by scripts/build_site.py). */
(function () {
  "use strict";
  const D = window.HEALTHFLOW;
  const charts = {};
  const nf = new Intl.NumberFormat("en-GB");
  const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  const monthLabel = (ym) =>
    new Date(ym + "-01T00:00:00Z").toLocaleDateString("en-GB", { month: "short", year: "2-digit", timeZone: "UTC" });
  const pct = (n, d) => (d > 0 ? (100 * n) / d : null);
  const round1 = (v) => (v === null || v === undefined ? null : Math.round(v * 10) / 10);

  function el(id) { return document.getElementById(id); }
  function text(id, value) { const e = el(id); if (e) e.textContent = value; }

  // ---- Stat tiles ---------------------------------------------------------
  const k = D.kpis;
  text("kpi-encounters", nf.format(k.encounters));
  text("kpi-patients", nf.format(k.patients) + " patients");
  text("kpi-stays", nf.format(k.stays));
  text("kpi-stays-note", nf.format(k.planned_stays) + " planned, " + nf.format(k.stays - k.planned_stays) + " unplanned");
  text("kpi-los", k.avg_los.toFixed(1) + " days");
  text("kpi-readm", k.index_stays ? round1(pct(k.readmissions, k.index_stays)).toFixed(1) + "%" : "–");
  text("kpi-readm-note", nf.format(k.readmissions) + " of " + nf.format(k.index_stays) + " eligible stays");
  text("kpi-ed", round1(pct(k.ed_within_4h, k.ed_visits)).toFixed(1) + "%");
  text("kpi-ed-note", "of " + nf.format(k.ed_visits) + " emergency visits");

  // ---- Department filter -----------------------------------------------------
  const select = el("dept");
  D.departments.forEach((d) => {
    const o = document.createElement("option");
    o.value = d.code;
    o.textContent = d.name;
    select.appendChild(o);
  });
  try {
    const saved = localStorage.getItem("hf-dept");
    if (saved && D.dept_monthly[saved]) select.value = saved;
  } catch (e) { /* storage unavailable: default to all */ }

  function deptSeries(code) {
    const empty = () => D.months.map(() => 0);
    const out = { discharges: empty(), stay_days: empty(), index_stays: empty(), readmissions: empty() };
    const codes = code === "ALL" ? Object.keys(D.dept_monthly) : [code];
    codes.forEach((c) => {
      const s = D.dept_monthly[c];
      if (!s) return;
      Object.keys(out).forEach((key) => s[key].forEach((v, i) => { out[key][i] += v; }));
    });
    return out;
  }

  // ---- Charts ------------------------------------------------------------------
  function baseOptions(yTitle, opts) {
    opts = opts || {};
    const grid = css("--grid"), muted = css("--muted"), axis = css("--axis");
    return {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: css("--surface"),
          titleColor: css("--ink"),
          bodyColor: css("--ink-2"),
          borderColor: css("--axis"),
          borderWidth: 1,
          padding: 10,
          callbacks: opts.tooltip || {},
        },
      },
      scales: {
        x: {
          grid: { display: false },
          border: { color: axis },
          ticks: { color: muted, maxRotation: 0, autoSkipPadding: 12, ...(opts.xTicks || {}) },
        },
        y: {
          beginAtZero: opts.beginAtZero !== false,
          suggestedMax: opts.suggestedMax,
          grid: { color: grid },
          border: { display: false },
          title: { display: !!yTitle, text: yTitle, color: muted },
          ticks: { color: muted, precision: opts.precision, callback: opts.yTick },
        },
      },
    };
  }

  function draw(id, config, table) {
    if (charts[id]) charts[id].destroy();
    charts[id] = new Chart(el(id), config);
    if (table) renderTable(id + "-table", table.headers, table.rows);
  }

  function renderTable(id, headers, rows) {
    const t = el(id);
    if (!t) return;
    t.innerHTML =
      "<thead><tr>" + headers.map((h, i) => `<th class="${i ? "num" : ""}">${h}</th>`).join("") + "</tr></thead>" +
      "<tbody>" + rows.map((r) => "<tr>" + r.map((c, i) => `<td class="${i ? "num" : ""}">${c}</td>`).join("") + "</tr>").join("") + "</tbody>";
  }

  function bar(color, data) {
    return { data, backgroundColor: color, borderRadius: 4, borderSkipped: "bottom", maxBarThickness: 28, categoryPercentage: 0.7 };
  }
  function line(color, data, points) {
    return {
      data, borderColor: color, backgroundColor: color, borderWidth: 2, tension: 0, spanGaps: false,
      pointRadius: points ? 4 : 0, pointHoverRadius: 6, pointBorderColor: css("--surface"), pointBorderWidth: 2,
    };
  }

  function renderDeptCharts() {
    const code = select.value;
    try { localStorage.setItem("hf-dept", code); } catch (e) { /* ignore */ }
    const s = deptSeries(code);
    const labels = D.months.map(monthLabel);
    const blue = css("--series-1");
    const scope = code === "ALL" ? "all departments" : select.options[select.selectedIndex].text;
    document.querySelectorAll("[data-scope]").forEach((e) => { e.textContent = scope; });

    draw("c-discharges", { type: "bar", data: { labels, datasets: [bar(blue, s.discharges)] },
      options: baseOptions("Stays", { precision: 0 }) },
      { headers: ["Month", "Discharges"], rows: D.months.map((m, i) => [monthLabel(m), nf.format(s.discharges[i])]) });

    const los = s.discharges.map((n, i) => (n ? round1(s.stay_days[i] / n) : null));
    draw("c-los", { type: "line", data: { labels, datasets: [line(blue, los, true)] },
      options: baseOptions("Days", { tooltip: { label: (c) => ` ${c.parsed.y} days (${s.discharges[c.dataIndex]} stays)` } }) },
      { headers: ["Month", "Average length of stay (days)", "Stays"],
        rows: D.months.map((m, i) => [monthLabel(m), los[i] === null ? "–" : los[i].toFixed(1), s.discharges[i]]) });

    const measurable = new Set(D.readmission_months);
    const rate = s.index_stays.map((n, i) => (measurable.has(D.months[i]) && n ? round1(pct(s.readmissions[i], n)) : null));
    // Plain-English summary, so a flat line at 0% reads as a result, not a broken chart.
    let idxTotal = 0, readmTotal = 0;
    D.months.forEach((m, i) => { if (measurable.has(m)) { idxTotal += s.index_stays[i]; readmTotal += s.readmissions[i]; } });
    let summary;
    if (!idxTotal) {
      summary = `No eligible stays for ${scope} in this period, so there is no rate to show.`;
    } else if (!readmTotal) {
      summary = `${scope}: no unplanned readmissions within 30 days across ${nf.format(idxTotal)} eligible stays (0%).`;
    } else {
      summary = `${scope}: ${nf.format(readmTotal)} unplanned readmission${readmTotal === 1 ? "" : "s"} across ` +
        `${nf.format(idxTotal)} eligible stays (${round1(pct(readmTotal, idxTotal)).toFixed(1)}%).`;
    }
    if (code === "ONC" || code === "ADDICT") {
      summary += " Most stays here are scheduled treatment cycles, so returning for the next cycle is planned and doesn't count.";
    }
    text("readm-summary", summary.charAt(0).toUpperCase() + summary.slice(1));
    draw("c-readm", { type: "line", data: { labels, datasets: [line(blue, rate, true)] },
      options: baseOptions("% readmitted", {
        suggestedMax: 10,
        yTick: (v) => v + "%",
        tooltip: { label: (c) => ` ${c.parsed.y}% (${s.readmissions[c.dataIndex]} of ${s.index_stays[c.dataIndex]} stays)` },
      }) },
      { headers: ["Discharge month", "Readmitted", "Eligible stays", "Rate"],
        rows: D.months.map((m, i) => [monthLabel(m), s.readmissions[i], s.index_stays[i], !measurable.has(m) ? "follow-up not complete yet" : rate[i] === null ? "no eligible stays" : rate[i].toFixed(1) + "%"]) });
  }

  function renderHospitalCharts() {
    const H = D.hospital;
    const labels = D.months.map(monthLabel);
    const blue = css("--series-1");

    const ed = H.ed_visits.map((n, i) => (n ? round1(pct(H.ed_within_4h[i], n)) : null));
    draw("c-ed", { type: "line", data: { labels, datasets: [line(blue, ed, true)] },
      options: baseOptions("% within 4 hours", {
        beginAtZero: false, suggestedMax: 100, yTick: (v) => v + "%",
        tooltip: { label: (c) => ` ${c.parsed.y}% of ${H.ed_visits[c.dataIndex]} visits` },
      }) },
      { headers: ["Month", "Emergency visits", "Within 4 hours", "Share"],
        rows: D.months.map((m, i) => [monthLabel(m), H.ed_visits[i], H.ed_within_4h[i], ed[i] === null ? "–" : ed[i].toFixed(1) + "%"]) });

    draw("c-encounters", { type: "bar", data: { labels, datasets: [bar(blue, H.encounters)] },
      options: baseOptions("Encounters", { precision: 0 }) },
      { headers: ["Month", "Encounters"], rows: D.months.map((m, i) => [monthLabel(m), nf.format(H.encounters[i])]) });

    const C = D.census_daily;
    const cLabels = C.dates.map((d) => new Date(d + "T00:00:00Z").toLocaleDateString("en-GB", { day: "numeric", month: "short", timeZone: "UTC" }));
    draw("c-census", { type: "line", data: { labels: cLabels, datasets: [{ ...line(blue, C.values, false), tension: 0, stepped: true }] },
      options: baseOptions("Patients in hospital", { precision: 0, xTicks: { maxTicksLimit: 12 } }) });
  }

  // ---- Department table ------------------------------------------------------------
  renderTable("dept-table",
    ["Department", "Stays", "Avg length of stay", "Planned", "With critical care", "30-day readmissions"],
    D.dept_table.map((r) => [
      r.name, nf.format(r.discharges), r.avg_los.toFixed(1) + " days", Math.round(r.planned_pct) + "%",
      nf.format(r.critical_care), r.index_stays ? `${r.readmissions} of ${r.index_stays}` : "–",
    ]));

  function renderAll() { renderDeptCharts(); renderHospitalCharts(); }
  select.addEventListener("change", renderDeptCharts);
  window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", renderAll);
  renderAll();
})();
