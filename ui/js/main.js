// Dashboard entry point: theme, view routing, live stream, and wiring.

import { api, openStream } from "./api.js";
import {
  controlMode,
  initControls,
  openUpstream,
  refreshUpstreamDialog,
  sessionAction,
  syncControlState,
} from "./controls.js";
import { initEvents, markEventsSeen, renderAll, renderRecent, syncEventListenerOptions, updateBadge } from "./events.js";
import { initFleet, renderFleet, setControlMode } from "./fleet.js";
import { initSessions, refreshSessions, setSessionsActive, setSessionsControlMode, syncSessionFilters } from "./sessions.js";
import { addEvents, ingestHistory, ingestStatus, on, store } from "./store.js";
import { announce, duration, setText, storageSet } from "./util.js";

const VIEWS = ["fleet", "sessions", "events"];
let view = "fleet";
let hadStream = false;

/* ---------- theme ---------- */

function initTheme() {
  const root = document.documentElement;
  const toggle = document.getElementById("theme-toggle");
  const current = () => {
    const set = root.getAttribute("data-theme");
    if (set) return set;
    return window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark";
  };
  const label = () => {
    const next = current() === "dark" ? "light" : "dark";
    toggle.setAttribute("aria-label", `Switch to ${next} theme`);
  };
  label();
  toggle.addEventListener("click", () => {
    const next = current() === "dark" ? "light" : "dark";
    root.setAttribute("data-theme", next);
    storageSet("localStorage", "fleet-router-theme", next);
    label();
  });
}

/* ---------- views ---------- */

function showView(name, { focus = false } = {}) {
  view = VIEWS.includes(name) ? name : "fleet";
  for (const section of document.querySelectorAll("main > .view")) {
    section.hidden = section.dataset.view !== view;
  }
  for (const link of document.querySelectorAll(".views a")) {
    if (link.dataset.view === view) link.setAttribute("aria-current", "page");
    else link.removeAttribute("aria-current");
  }
  setSessionsActive(view === "sessions");
  if (view === "events") {
    renderAll();
    markEventsSeen();
  }
  document.title = view === "fleet" ? "fleet-router dashboard" : `${view[0].toUpperCase()}${view.slice(1)} – fleet-router`;
  if (focus) {
    window.scrollTo({ top: 0 });
    document.getElementById("main").focus({ preventScroll: true });
  }
}

function initRouting() {
  window.addEventListener("hashchange", () => showView(location.hash.slice(1), { focus: true }));
  showView(location.hash.slice(1));
}

/* ---------- hatch pattern shared by every track ---------- */

function injectDefs() {
  const holder = document.createElement("div");
  holder.innerHTML =
    '<svg width="0" height="0" aria-hidden="true" focusable="false" class="trk"><defs>' +
    '<pattern id="hatch" class="hatch" width="4" height="4" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">' +
    '<rect width="1.6" height="4"/></pattern></defs></svg>';
  const defs = holder.firstChild;
  defs.style.position = "absolute";
  document.body.prepend(defs);
}

/* ---------- live updates ---------- */

function setLinkState(state) {
  const node = document.getElementById("link-state");
  const before = node.dataset.state;
  node.dataset.state = state;
  setText(node.querySelector(".link-label"), state === "live" ? "Live" : state === "reconnecting" ? "Reconnecting" : "Connecting");
  if (before === "live" && state === "reconnecting") announce("Lost the live connection to the router. Reconnecting.");
  if (before === "reconnecting" && state === "live") announce("Live connection restored.");
}

async function backfillEvents() {
  try {
    const data = await api.events(store.lastEventId || undefined);
    addEvents(data.events || []);
  } catch {
    /* the stream will deliver new events; the log just has a gap */
  }
}

function startStream() {
  openStream({
    onState(state) {
      setLinkState(state);
      if (state === "live") {
        if (hadStream) backfillEvents();
        hadStream = true;
      }
    },
    onSnapshot(status) {
      ingestStatus(status);
    },
    onEvent(ev) {
      addEvents([ev], { fresh: true });
    },
  });
}

on((kind, payload) => {
  if (kind === "status") {
    const status = payload;
    syncControlState(status);
    renderFleet(status);
    syncSessionFilters(status);
    syncEventListenerOptions(status.listeners.map((l) => l.name));
    refreshUpstreamDialog();
    setText(document.getElementById("meta-version"), `v${status.version}`);
    setText(document.getElementById("meta-uptime"), `up ${duration(status.uptime_secs)}`);
  } else if (kind === "events") {
    const fresh = new Set(payload.fresh ? payload.added.map((e) => e.id) : []);
    renderRecent(fresh);
    if (view === "events") {
      renderAll(fresh);
      markEventsSeen();
    } else {
      updateBadge();
    }
    if (payload.fresh) {
      const loud = payload.added.filter((e) => e.level === "error" || e.kind === "upstream_down");
      if (loud.length) announce(loud.map((e) => e.message).join(". "));
    }
  }
});

function applyUpstream(fresh) {
  const status = store.status;
  if (!status) return;
  const listener = status.listeners.find((l) => l.name === fresh.listener);
  if (!listener) return;
  const i = listener.upstreams.findIndex((u) => u.endpoint === fresh.endpoint);
  if (i >= 0) listener.upstreams[i] = fresh;
  renderFleet(status);
}

async function main() {
  initTheme();
  injectDefs();
  initFleet({ openUpstream });
  initEvents();
  initSessions({
    sessionAction: async (session, action) => {
      await sessionAction(session, action);
      refreshSessions();
    },
  });
  initControls({
    onModeChange(mode) {
      setControlMode(mode);
      setSessionsControlMode(mode);
    },
    onUpstreamChanged: applyUpstream,
  });
  setControlMode(controlMode());
  initRouting();
  renderRecent();

  // History and events seed the charts and the log; the stream's first
  // snapshot draws the fleet.
  const [history, events] = await Promise.allSettled([api.history(), api.events()]);
  if (history.status === "fulfilled") ingestHistory(history.value);
  if (events.status === "fulfilled") addEvents(events.value.events || []);
  startStream();
}

main();
