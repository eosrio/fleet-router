// Sessions view reads the same pushed revision as the fleet and event log.

import { store } from "./store.js";
import { syncOptions } from "./events.js";
import { bytes, duration, el, int, plural, setText } from "./util.js";

const filters = { listener: "", upstream: "", search: "" };
let sort = { key: "age", dir: "descending" };
let active = false;
let controlMode = "off";
let onAction = () => {};

export function initSessions({ sessionAction }) {
  onAction = sessionAction;
  const table = document.getElementById("sessions-table");
  for (const th of table.querySelectorAll("th[data-sort]")) {
    const label = th.textContent;
    const button = el("button", { type: "button" }, label);
    th.replaceChildren(button);
    button.addEventListener("click", () => {
      const key = th.dataset.sort;
      const numeric = th.classList.contains("num");
      const first = numeric ? "descending" : "ascending";
      const flip = { ascending: "descending", descending: "ascending" };
      sort = { key, dir: sort.key === key ? flip[sort.dir] : first };
      render();
    });
  }
  document.getElementById("sessions-listener").addEventListener("change", (e) => {
    filters.listener = e.target.value;
    render();
  });
  document.getElementById("sessions-upstream").addEventListener("change", (e) => {
    filters.upstream = e.target.value;
    render();
  });
  document.getElementById("sessions-search").addEventListener("input", (e) => {
    filters.search = e.target.value.trim();
    render();
  });
}

export function setSessionsActive(on) {
  active = on;
  if (on) render();
}

export function setSessionsControlMode(mode) {
  controlMode = mode;
  if (active) render();
}

/** Keep the listener and upstream filters in step with the latest status. */
export function syncSessionFilters(status) {
  syncOptions(
    document.getElementById("sessions-listener"),
    status.listeners.map((l) => [l.name, l.name]),
  );
  const ups = [];
  for (const l of status.listeners) {
    for (const u of l.upstreams) ups.push([u.endpoint, `${u.name} (${l.name})`]);
  }
  syncOptions(document.getElementById("sessions-upstream"), ups);
  const total = status.totals.sessions;
  setText(document.getElementById("nav-sessions"), int(total));
}

export function renderSessions() {
  if (active) render();
}

function filtered() {
  const q = filters.search.toLowerCase();
  return store.sessions.filter(
    (s) =>
      (!filters.listener || s.listener === filters.listener) &&
      (!filters.upstream || s.upstream === filters.upstream) &&
      (!q || s.client.toLowerCase().includes(q)),
  );
}

function value(s, key) {
  return key === "age" ? -s.started_ms : s[key];
}

function compare(a, b) {
  const av = value(a, sort.key);
  const bv = value(b, sort.key);
  let r;
  if (av === null || av === undefined) r = bv === null || bv === undefined ? 0 : 1;
  else if (bv === null || bv === undefined) r = -1;
  else if (typeof av === "number") r = av - bv;
  else r = String(av).localeCompare(String(bv));
  return sort.dir === "ascending" ? r : -r;
}

function render() {
  const table = document.getElementById("sessions-table");
  for (const th of table.querySelectorAll("th[data-sort]")) {
    if (th.dataset.sort === sort.key) th.setAttribute("aria-sort", sort.dir);
    else th.removeAttribute("aria-sort");
  }
  const list = filtered().sort(compare);
  const now = store.status?.now_ms ?? Date.now();
  const rows = store.sessions;
  const tbody = table.tBodies[0];
  const unlocked = controlMode === "unlocked";
  table.querySelector(".col-actions").hidden = !unlocked;

  const keep = new Map([...tbody.rows].map((tr) => [tr.dataset.id, tr]));
  const ordered = list.map((s) => {
    const tr = keep.get(String(s.id)) || el("tr", { dataset: { id: String(s.id) } });
    keep.delete(String(s.id));
    fillRow(tr, s, now, unlocked);
    return tr;
  });
  for (const tr of keep.values()) tr.remove();
  ordered.forEach((tr, i) => {
    if (tbody.rows[i] !== tr) tbody.insertBefore(tr, tbody.rows[i] || null);
  });

  const empty = document.getElementById("sessions-empty");
  if (!store.status) {
    empty.hidden = false;
    setText(empty, "Waiting for live state from the router.");
  } else if (!list.length) {
    empty.hidden = false;
    setText(empty, rows.length ? "No sessions match these filters." : "No client sessions are open right now.");
  } else {
    empty.hidden = true;
  }
  const summary = document.getElementById("sessions-summary");
  setText(summary, list.length === rows.length ? plural(rows.length, "session", "sessions") : `${int(list.length)} of ${plural(rows.length, "session", "sessions")}`);
}

function fillRow(tr, s, now, unlocked) {
  const cells = [
    el("td", null, el("span.mono", null, s.client), el("div.sub", null, s.listener, s.draining ? " · draining" : "", s.finality ? "  finality" : "")),
    el("td", null, el("div", null, s.upstream_name || s.upstream), el("div.sub", null, s.upstream)),
    el("td.num", { title: new Date(s.started_ms).toLocaleString() }, duration((now - s.started_ms) / 1000)),
    el("td.num", null, s.start_block === null ? "–" : int(s.start_block)),
    el("td.num", null, s.last_block === null ? "–" : int(s.last_block)),
    el("td.num", null, int(s.blocks)),
    el("td.num", null, bytes(s.bytes)),
    el("td.num", null, int(s.failovers)),
  ];
  if (unlocked) {
    cells.push(
      el(
        "td.col-actions",
        null,
        el(
          "span.row-actions",
          null,
          el("button.button.button-small", { type: "button", "aria-label": `Move session ${s.id} from ${s.client}`, onclick: () => onAction(s, "move") }, "Move"),
          el("button.button.button-small", { type: "button", "aria-label": `Close session ${s.id} from ${s.client}`, onclick: () => onAction(s, "close") }, "Close"),
        ),
      ),
    );
  }
  // Keep focus on a row's button across refreshes by only swapping changed cells.
  const focused = tr.contains(document.activeElement);
  if (focused && tr.cells.length === cells.length) {
    cells.forEach((cell, i) => {
      if (i === cells.length - 1 && unlocked) return;
      if (tr.cells[i].innerText !== cell.innerText) tr.replaceChild(cell, tr.cells[i]);
    });
  } else {
    tr.replaceChildren(...cells);
  }
}
