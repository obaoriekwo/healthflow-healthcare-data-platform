/* HealthFlow data model page: star-schema connector lines and the data dictionary filter. */
(function () {
  "use strict";
  const NS = "http://www.w3.org/2000/svg";

  // ---- ERD lines: drawn from each foreign key row to the primary key row it references ----
  const erd = document.getElementById("erd");
  const svg = erd && erd.querySelector(".erd-lines");
  let rels = [];
  try { rels = JSON.parse(document.getElementById("erd-rels").textContent); } catch (e) { rels = []; }

  function row(table, column) {
    return erd.querySelector(`.entity[data-table="${table}"] li[data-col="${column}"]`);
  }

  function drawLines() {
    if (!svg) return;
    svg.replaceChildren();
    // Stacked layout on small screens: the "→ table" labels carry the relationships instead.
    if (getComputedStyle(svg).display === "none") return;
    const box = erd.getBoundingClientRect();
    svg.setAttribute("viewBox", `0 0 ${box.width} ${box.height}`);
    rels.forEach((r) => {
      const a = row(r.from, r.column), b = row(r.to, r.field);
      if (!a || !b) return;
      const ra = a.getBoundingClientRect(), rb = b.getBoundingClientRect();
      const ay = ra.top + ra.height / 2 - box.top, by = rb.top + rb.height / 2 - box.top;
      let ax, bx, c1, c2;
      const sameColumn = Math.abs(ra.left - rb.left) < 4;
      if (sameColumn) {
        // Same column (encounter to its stay): loop out on the left, where there are fewest lines.
        ax = ra.left - box.left; bx = rb.left - box.left;
        c1 = ax - 40; c2 = bx - 40;
      } else if (rb.left > ra.left) {
        ax = ra.right - box.left; bx = rb.left - box.left;
        c1 = ax + (bx - ax) / 2; c2 = c1;
      } else {
        ax = ra.left - box.left; bx = rb.right - box.left;
        c1 = ax - (ax - bx) / 2; c2 = c1;
      }
      const g = document.createElementNS(NS, "g");
      g.dataset.from = r.from; g.dataset.to = r.to;
      const path = document.createElementNS(NS, "path");
      path.setAttribute("d", `M${ax},${ay} C${c1},${ay} ${c2},${by} ${bx},${by}`);
      g.appendChild(path);
      // Many end: a dot on the foreign key. One end: a bar across the line at the primary key.
      const dot = document.createElementNS(NS, "circle");
      dot.setAttribute("cx", ax); dot.setAttribute("cy", ay); dot.setAttribute("r", 3.5);
      g.appendChild(dot);
      const dir = sameColumn ? -1 : bx > ax ? -1 : 1;
      const bar = document.createElementNS(NS, "line");
      bar.setAttribute("x1", bx + dir * 7); bar.setAttribute("x2", bx + dir * 7);
      bar.setAttribute("y1", by - 6); bar.setAttribute("y2", by + 6);
      g.appendChild(bar);
      svg.appendChild(g);
    });
  }

  if (erd) {
    // Hovering or focusing a table highlights its relationships.
    erd.querySelectorAll(".entity").forEach((card) => {
      const name = card.dataset.table;
      const on = (v) => () => {
        erd.classList.toggle("focusing", v);
        svg.querySelectorAll("g").forEach((g) => g.classList.toggle("lit", v && (g.dataset.from === name || g.dataset.to === name)));
        card.classList.toggle("lit", v);
      };
      card.addEventListener("mouseenter", on(true));
      card.addEventListener("mouseleave", on(false));
      card.addEventListener("focusin", on(true));
      card.addEventListener("focusout", on(false));
    });
    if (window.ResizeObserver) new ResizeObserver(drawLines).observe(erd);
    else window.addEventListener("resize", drawLines);
    if (document.fonts && document.fonts.ready) document.fonts.ready.then(drawLines);
    drawLines();
  }

  // ---- Data dictionary filter ---------------------------------------------------------
  const table = document.getElementById("dict-table");
  const q = document.getElementById("dict-q");
  const rows = Array.from(document.querySelectorAll("#dict-body tr"));
  const count = document.getElementById("dict-count");
  const empty = document.getElementById("dict-empty");
  const nf = new Intl.NumberFormat("en-GB");

  function filter() {
    const t = table.value;
    const words = q.value.toLowerCase().trim().split(/\s+/).filter(Boolean);
    let shown = 0;
    rows.forEach((r) => {
      const hay = r.textContent.toLowerCase();
      const ok = (!t || r.dataset.table === t) && words.every((w) => hay.includes(w));
      r.hidden = !ok;
      if (ok) shown++;
    });
    count.textContent = shown === rows.length
      ? `${nf.format(rows.length)} columns`
      : `Showing ${nf.format(shown)} of ${nf.format(rows.length)} columns`;
    empty.hidden = shown > 0;
  }
  if (table && q) {
    table.addEventListener("change", filter);
    q.addEventListener("input", filter);
    document.querySelectorAll("[data-filter]").forEach((a) => a.addEventListener("click", () => {
      table.value = a.dataset.filter;
      q.value = "";
      filter();
    }));
    filter();
  }
})();
