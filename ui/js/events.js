// Event log: the recent strip on the fleet view and the filterable Events view.

import { store } from "./store.js";
import { clock, el, int, setText } from "./util.js";

const RECENT = 8;
const filters = { level: "", listener: "" };

export function initEvents() {
  const group = document.getElementById("events-level");
  const buttons = [...group.querySelectorAll("button")];
  const select = (btn) => {
    for (const b of buttons) b.setAttribute("aria-checked", String(b === btn));
    for (const b of buttons) b.tabIndex = b === btn ? 0 : -1;
    filters.level = btn.dataset.level;
    renderAll();
  };
  buttons.forEach((btn, i) => {
    btn.tabIndex = i === 0 ? 0 : -1;
    btn.addEventListener("click", () => select(btn));
    btn.addEventListener("keydown", (e) => {
      const dir = { ArrowRight: 1, ArrowDown: 1, ArrowLeft: -1, ArrowUp: -1 }[e.key];
      if (!dir) return;
      e.preventDefault();
      const next = buttons[(i + dir + buttons.length) % buttons.length];
      next.focus();
      select(next);
    });
  });
  document.getElementById("events-listener").addEventListener("change", (e) => {
    filters.listener = e.target.value;
    renderAll();
  });
}

export function syncEventListenerOptions(names) {
  const select = document.getElementById("events-listener");
  syncOptions(select, names.map((n) => [n, n]));
}

export function syncOptions(select, pairs) {
  const want = [["", "All"], ...pairs];
  const have = [...select.options].map((o) => [o.value, o.textContent]);
  if (JSON.stringify(want) === JSON.stringify(have)) return;
  const current = select.value;
  select.replaceChildren(...want.map(([value, label]) => el("option", { value }, label)));
  select.value = want.some(([v]) => v === current) ? current : "";
}

function matches(ev) {
  if (filters.listener && ev.listener !== filters.listener) return false;
  if (filters.level === "warn") return ev.level === "warn" || ev.level === "error";
  if (filters.level === "error") return ev.level === "error";
  return true;
}

function row(ev, isNew) {
  const key = ev.level === "error" ? "key-failover" : ev.level === "warn" ? "key-stale" : ev.kind === "upstream_up" || ev.kind === "upstream_fresh" ? "key-set" : "";
  const where = [ev.listener, ev.upstream, ev.session !== null && ev.session !== undefined ? `session ${ev.session}` : null].filter(Boolean).join("  ");
  const node = el(
    `li.ev${isNew ? ".is-new" : ""}`,
    { dataset: { level: ev.level, id: String(ev.id) } },
    el("time.ev-time", { datetime: new Date(ev.t_ms).toISOString(), title: new Date(ev.t_ms).toLocaleString() }, clock(ev.t_ms)),
    el(`span.key${key ? `.${key}` : ""}`, { "aria-hidden": "true", title: ev.level }),
    el("span.ev-kind", null, ev.kind.replaceAll("_", " ")),
    el("span.ev-msg", null, ev.message),
    where ? el("span.ev-where", null, where) : null,
  );
  return node;
}

export function renderRecent(fresh = new Set()) {
  const list = document.getElementById("recent-events");
  const items = store.events.slice(-RECENT).reverse();
  if (!items.length) {
    list.replaceChildren(el("li.empty", null, "No events yet."));
    return;
  }
  list.replaceChildren(...items.map((ev) => row(ev, fresh.has(ev.id))));
}

export function renderAll(fresh = new Set()) {
  const list = document.getElementById("all-events");
  const items = store.events.filter(matches).reverse();
  list.replaceChildren(...items.map((ev) => row(ev, fresh.has(ev.id))));
  document.getElementById("events-empty").hidden = items.length > 0;
}

/** Badge on the Events tab: warnings and errors since the view was last opened. */
let seenId = 0;
export function resetEventsSeen() {
  seenId = 0;
}
export function markEventsSeen() {
  seenId = store.lastEventId;
  updateBadge();
}

export function updateBadge() {
  const badge = document.getElementById("nav-events");
  const unseen = store.events.filter((e) => e.id > seenId && (e.level === "warn" || e.level === "error")).length;
  badge.hidden = unseen === 0;
  badge.classList.toggle("count-warn", unseen > 0);
  setText(badge, int(unseen));
  badge.setAttribute("aria-label", `${unseen} new warnings`);
}
