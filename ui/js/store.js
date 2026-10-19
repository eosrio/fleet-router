// Shared dashboard state: the latest status snapshot, per-upstream time
// series (seeded from /history, extended by each snapshot), and events.

import { upstreamKey } from "./util.js";

const MAX_POINTS = 900; // 15 minutes at one sample per second
const MAX_EVENTS = 500;
const RATE_WINDOW = 5; // samples averaged for the displayed rate
const FLASH_MS = 1600;

export const store = {
  status: null,
  series: new Map(),
  events: [],
  lastEventId: 0,
  flashes: new Map(),
  listeners: new Set(),
};

/** Subscribe to "status", "event", "events", "control" notifications. */
export function on(fn) {
  store.listeners.add(fn);
  return () => store.listeners.delete(fn);
}

export function notify(kind, payload) {
  for (const fn of store.listeners) fn(kind, payload);
}

function seriesFor(key) {
  let s = store.series.get(key);
  if (!s) {
    s = { t: [], sessions: [], bps: [], Bps: [], last: null };
    store.series.set(key, s);
  }
  return s;
}

function push(s, t, sessions, bps, Bps) {
  s.t.push(t);
  s.sessions.push(sessions);
  s.bps.push(bps);
  s.Bps.push(Bps);
  if (s.t.length > MAX_POINTS) {
    s.t.shift();
    s.sessions.shift();
    s.bps.shift();
    s.Bps.shift();
  }
}

/** Add one sample for an upstream. Totals that went backwards (restart) reset the rate. */
export function ingest(key, t, sessions, blocksTotal, bytesTotal) {
  const s = seriesFor(key);
  if (s.last && t <= s.last.t) return;
  let bps = null;
  let Bps = null;
  if (s.last && blocksTotal >= s.last.blocks && bytesTotal >= s.last.bytes) {
    const dt = (t - s.last.t) / 1000;
    if (dt > 0) {
      bps = (blocksTotal - s.last.blocks) / dt;
      Bps = (bytesTotal - s.last.bytes) / dt;
    }
  }
  s.last = { t, blocks: blocksTotal, bytes: bytesTotal };
  push(s, t, sessions, bps, Bps);
}

export function ingestHistory(history) {
  if (!history || !Array.isArray(history.samples)) return;
  for (const sample of history.samples) {
    for (const [key, u] of Object.entries(sample.upstreams || {})) {
      ingest(key, sample.t_ms, u.sessions, u.blocks_total, u.bytes_total);
    }
  }
}

export function ingestStatus(status) {
  store.status = status;
  for (const l of status.listeners) {
    for (const u of l.upstreams) {
      ingest(upstreamKey(l.name, u.endpoint), status.now_ms, u.sessions, u.blocks_total, u.bytes_total);
    }
  }
  notify("status", status);
}

/** Average of the last few rate samples, so the number does not jitter. */
export function currentRates(key) {
  const s = store.series.get(key);
  if (!s) return { bps: null, Bps: null };
  const avg = (arr) => {
    const vals = arr.slice(-RATE_WINDOW).filter((v) => v !== null);
    return vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
  };
  return { bps: avg(s.bps), Bps: avg(s.Bps) };
}

export function addEvents(events, { fresh = false } = {}) {
  const added = [];
  for (const ev of events) {
    if (ev.id <= store.lastEventId) continue;
    store.events.push(ev);
    store.lastEventId = ev.id;
    added.push(ev);
    if (fresh && ev.kind === "session_failover" && ev.listener && ev.upstream) {
      store.flashes.set(upstreamKey(ev.listener, ev.upstream), Date.now() + FLASH_MS);
    }
  }
  if (store.events.length > MAX_EVENTS) store.events.splice(0, store.events.length - MAX_EVENTS);
  if (added.length) notify("events", { added, fresh });
}

export function isFlashing(key) {
  const until = store.flashes.get(key);
  if (!until) return false;
  if (until < Date.now()) {
    store.flashes.delete(key);
    return false;
  }
  return true;
}

export function findUpstream(listener, endpoint) {
  const l = store.status?.listeners.find((x) => x.name === listener);
  return l?.upstreams.find((u) => u.endpoint === endpoint) || null;
}

/** Best (highest) head block among a listener's upstreams. */
export function bestHead(listener) {
  let best = null;
  for (const u of listener.upstreams) {
    if (u.head_block !== null && u.head_block !== undefined && (best === null || u.head_block > best)) {
      best = u.head_block;
    }
  }
  return best;
}

/** Lag measured at this node's own status request, never across sample ages. */
export function headLag(upstream) {
  const lag = upstream.head_lag_blocks;
  return upstream.online && Number.isFinite(lag) && lag >= 0 ? lag : null;
}
