// Dashboard state commits once per server revision. Every panel observes the
// same status, sessions, events and sampled history before rendering.

import { upstreamKey } from "./util.js";

const MAX_POINTS = 900; // 15 minutes at one sample per second
const MAX_EVENTS = 500;
const RATE_WINDOW = 5; // samples averaged for the displayed rate
const FLASH_MS = 1600;

export const store = {
  status: null,
  sessions: [],
  instance: null,
  revision: 0,
  series: new Map(),
  events: [],
  lastEventId: 0,
  flashes: new Map(),
  listeners: new Set(),
};

/** Subscribe to complete "dashboard" publications. */
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

/** Apply a complete publication before notifying any view. A new stream starts
 * with a bootstrap, including retained history and events. A gap cannot be
 * patched safely: the transport must reconnect for another bootstrap. */
export function applyDashboardFrame(frame) {
  if (!frame || !Number.isSafeInteger(frame.revision) || frame.revision < 1 ||
      typeof frame.bootstrap !== "boolean" || !Number.isSafeInteger(frame.status?.started_ms) ||
      !Array.isArray(frame.status.listeners) || !Array.isArray(frame.sessions) || !Array.isArray(frame.events) ||
      (frame.bootstrap && !Array.isArray(frame.history?.samples))) {
    throw new Error("Invalid dashboard frame");
  }
  const instance = frame.status.started_ms;
  const restarted = store.instance !== null && store.instance !== instance;
  if (store.instance === instance && frame.revision <= store.revision) return false;
  if (!frame.bootstrap && (store.instance !== instance || frame.revision !== store.revision + 1)) {
    throw new Error("Dashboard resync required");
  }
  // Validate nested samples before changing the last usable state.
  const samples = [...(frame.bootstrap ? frame.history.samples : []), ...(frame.sample ? [frame.sample] : [])];
  for (const sample of samples) {
    if (!sample || !Number.isFinite(sample.t_ms) || !sample.upstreams || typeof sample.upstreams !== "object" ||
        Object.values(sample.upstreams).some(u => !u || ![u.sessions, u.blocks_total, u.bytes_total].every(n => Number.isFinite(n) && n >= 0))) {
      throw new Error("Invalid dashboard sample");
    }
  }
  if (frame.events.some(e => !e || !Number.isSafeInteger(e.id) || e.id < 1)) throw new Error("Invalid dashboard event");
  if (frame.bootstrap) {
    store.series.clear();
    store.events = [];
    store.lastEventId = 0;
    store.flashes.clear();
    ingestHistory(frame.history);
  }
  if (frame.sample) ingestHistory({ samples: [frame.sample] });
  const added = addEvents(frame.events, { fresh: !frame.bootstrap });
  store.status = frame.status;
  store.sessions = frame.sessions;
  store.instance = instance;
  store.revision = frame.revision;
  notify("dashboard", { added, fresh: !frame.bootstrap, reset: frame.bootstrap, restarted });
  return true;
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

function addEvents(events, { fresh = false } = {}) {
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
  return added;
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

/** Highest reported stored block; sets the history range scale, not lag. */
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
