import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";
import { applyDashboardFrame, currentRates, on, store } from "../ui/js/store.js";

function frame(revision, { bootstrap = false, started = 100, sessions = [], events = [], sample = null, history = [] } = {}) {
  return {
    revision, bootstrap,
    status: { started_ms: started, now_ms: 3000, totals: { sessions: sessions.length }, listeners: [] },
    sessions, events: events.map(e => ({ t_ms: 3000, level: "info", kind: "control", message: "Changed", ...e })), sample,
    ...(bootstrap ? { history: { interval_ms: 1000, samples: history } } : {}),
  };
}

function sample(t, blocks) {
  return { t_ms: t, upstreams: { "test|node": { sessions: 1, blocks_total: blocks, bytes_total: blocks * 100 } } };
}

beforeEach(() => {
  store.listeners.clear();
  store.revision = 0;
  store.instance = null;
  store.status = null;
  store.sessions = [];
  store.series.clear();
  store.events = [];
  store.lastEventId = 0;
  store.flashes.clear();
});

test("one notification exposes the complete revision to every panel", () => {
  const received = [];
  on((kind) => received.push({ kind, revision: store.revision, status: store.status,
    sessions: store.sessions, events: [...store.events], rates: currentRates("test|node") }));
  const initial = frame(7, { bootstrap: true, sessions: [{ id: 1 }], events: [{ id: 9 }],
    history: [sample(1000, 10), sample(2000, 20)] });
  assert.equal(applyDashboardFrame(initial), true);
  assert.equal(received.length, 1);
  assert.equal(received[0].kind, "dashboard");
  assert.equal(received[0].revision, 7);
  assert.equal(received[0].status.totals.sessions, received[0].sessions.length);
  assert.deepEqual(received[0].events, initial.events);
  assert.deepEqual(received[0].rates, { bps: 10, Bps: 1000 });
});

test("old or duplicate revisions cannot roll a control or session state backwards", () => {
  const initial = frame(4, { bootstrap: true });
  applyDashboardFrame(initial);
  const update = frame(5, { sessions: [{ id: 2 }], events: [{ id: 10 }] });
  applyDashboardFrame(update);
  let notifications = 0;
  on(() => notifications++);
  assert.equal(applyDashboardFrame(initial), false);
  assert.equal(applyDashboardFrame(frame(5)), false);
  assert.equal(notifications, 0);
  assert.equal(store.status, update.status);
  assert.deepEqual(store.sessions, [{ id: 2 }]);
  assert.equal(store.lastEventId, 10);
});

test("a gap requires a bootstrap; reconnect replaces history without duplicates", () => {
  applyDashboardFrame(frame(1, { bootstrap: true, events: [{ id: 1 }], history: [sample(1000, 10)] }));
  assert.throws(() => applyDashboardFrame(frame(3)), /resync/i);
  assert.equal(store.revision, 1);
  const resumed = frame(5, { bootstrap: true, events: [{ id: 1 }, { id: 2 }],
    history: [sample(1000, 10), sample(2000, 20), sample(3000, 30)] });
  applyDashboardFrame(resumed);
  assert.deepEqual(store.events.map(e => e.id), [1, 2]);
  assert.deepEqual(store.series.get("test|node").t, [1000, 2000, 3000]);
  assert.equal(currentRates("test|node").bps, 10);
});

test("router restart resets event IDs, rates, flashes and sessions in one update", () => {
  applyDashboardFrame(frame(99, { bootstrap: true, events: [{ id: 80 }], sessions: [{ id: 80 }],
    history: [sample(1000, 500)] }));
  store.flashes.set("test|node", 999999);
  let restart;
  on((_, payload) => { restart = payload.restarted; });
  assert.throws(() => applyDashboardFrame(frame(1, { started: 200 })), /resync/i);
  applyDashboardFrame(frame(1, { bootstrap: true, started: 200, events: [{ id: 1 }], history: [sample(2000, 1)] }));
  assert.equal(restart, true);
  assert.equal(store.revision, 1);
  assert.deepEqual(store.events.map(e => e.id), [1]);
  assert.deepEqual(store.sessions, []);
  assert.equal(store.flashes.size, 0);
  assert.equal(currentRates("test|node").bps, null);
});

test("lifecycle updates do not distort the one-second counter series", () => {
  applyDashboardFrame(frame(1, { bootstrap: true, history: [sample(1000, 10)] }));
  for (let revision = 2; revision < 5; revision++) applyDashboardFrame(frame(revision));
  assert.equal(store.series.get("test|node").t.length, 1);
  applyDashboardFrame(frame(5, { sample: sample(2000, 20) }));
  assert.equal(store.series.get("test|node").t.length, 2);
  assert.equal(currentRates("test|node").bps, 10);
});

test("a malformed bootstrap cannot clear the last usable state", () => {
  applyDashboardFrame(frame(1, { bootstrap: true, sessions: [{ id: 2 }], history: [sample(1000, 10)] }));
  const invalid = frame(2, { bootstrap: true });
  delete invalid.history;
  assert.throws(() => applyDashboardFrame(invalid), /invalid/i);
  assert.equal(store.revision, 1);
  assert.deepEqual(store.sessions, [{ id: 2 }]);
  assert.equal(store.series.get("test|node").t.length, 1);
  const badSample = frame(2, { bootstrap: true, history: [null] });
  assert.throws(() => applyDashboardFrame(badSample), /invalid/i);
  assert.equal(store.revision, 1);
  assert.equal(store.series.get("test|node").t.length, 1);
});

test("malformed events preserve all usable state and allow the corrected revision", () => {
  applyDashboardFrame(frame(1, { bootstrap: true, sessions: [{ id: 2 }], events: [{ id: 9 }],
    history: [sample(1000, 10), sample(2000, 20)] }));
  store.flashes.set("test|node", 123456);
  const snapshot = () => structuredClone({ status: store.status, sessions: store.sessions,
    instance: store.instance, revision: store.revision, series: store.series,
    events: store.events, lastEventId: store.lastEventId, flashes: store.flashes });
  const before = snapshot();
  let notifications = 0;
  on(() => notifications++);
  for (const malformed of [
    { kind: null }, { kind: 7 }, { kind: "" }, { message: null }, { level: null },
    { t_ms: "bad" }, { t_ms: NaN }, { t_ms: Infinity }, { t_ms: -1 }, { t_ms: 8640000000000001 },
    { listener: {} }, { upstream: [] }, { session: "bad" },
  ]) {
    const invalid = frame(2, { bootstrap: true, events: [{ id: 10, ...malformed }] });
    assert.throws(() => applyDashboardFrame(invalid), /invalid dashboard event/i);
    assert.deepEqual(snapshot(), before);
    assert.equal(notifications, 0);
  }
  assert.equal(applyDashboardFrame(frame(2, { bootstrap: true, events: [{ id: 10 }] })), true);
  assert.equal(store.revision, 2);
  assert.equal(notifications, 1);
});

test("the browser uses one push read path, marks live after apply, and resyncs a gap", async (t) => {
  const connections = [];
  const scheduled = [];
  const states = [];
  class EventSource {
    handlers = new Map();
    closed = false;
    constructor(url) { this.url = url; connections.push(this); }
    addEventListener(kind, fn) { this.handlers.set(kind, fn); }
    close() { this.closed = true; }
    emit(kind, data) { this.handlers.get(kind)?.({ data: JSON.stringify(data) }); }
  }
  const oldWindow = globalThis.window;
  const oldEventSource = globalThis.EventSource;
  t.after(() => {
    if (oldWindow === undefined) delete globalThis.window;
    else globalThis.window = oldWindow;
    if (oldEventSource === undefined) delete globalThis.EventSource;
    else globalThis.EventSource = oldEventSource;
  });
  globalThis.window = { location: { href: "http://router/prefix/ui/" },
    setTimeout: (fn) => { scheduled.push(fn); return scheduled.length; }, clearTimeout: () => {} };
  globalThis.EventSource = EventSource;
  t.mock.method(globalThis, "fetch", () => { assert.fail("dashboard reads must not fetch"); });
  t.mock.method(console, "error", () => {});
  const { openStream } = await import("../ui/js/api.js");
  const stream = openStream({ onFrame: applyDashboardFrame, onState: state => states.push(state) });
  assert.equal(connections[0].url, "http://router/prefix/api/v1/dashboard/stream");
  connections[0].emit("open");
  assert.deepEqual(states, [], "an open socket is not yet live state");
  connections[0].emit("dashboard", frame(1, { bootstrap: true }));
  assert.deepEqual(states, ["live"]);
  connections[0].emit("dashboard", frame(3));
  assert.equal(connections[0].closed, true);
  assert.equal(states.at(-1), "reconnecting");
  assert.equal(scheduled.length, 1);
  scheduled[0]();
  assert.equal(connections.length, 2);
  connections[1].emit("dashboard", frame(4, { bootstrap: true }));
  assert.equal(states.at(-1), "live");
  assert.equal(store.revision, 4);
  // Already-queued messages from the closed source cannot overwrite the resync.
  connections[0].emit("dashboard", frame(6, { bootstrap: true }));
  assert.equal(store.revision, 4);
  stream.close();
  assert.equal(connections[1].closed, true);
});
