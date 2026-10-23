// Fleet view: one panel per listener, one board row per upstream. The row's
// block track shows the last reported history range on the left and measured
// head lag on a separate relative scale on the right. Sampled history ends
// must never be used as a second, contradictory measure of head lag.

import { axisMarkup, trackMarkup, trackLabel } from "./track.js";
import { bestHead, currentRates, headLag, isFlashing, store } from "./store.js";
import {
  countdown,
  el,
  int,
  plural,
  rate,
  byteRate,
  setText,
  shortHash,
  STATUS_LABEL,
  upstreamKey,
} from "./util.js";

const SPARK_POINTS = 300; // last five minutes
const LAG_WARN = 10;

const panels = new Map();
let onOpenUpstream = () => {};
let controlMode = "off"; // "off" | "locked" | "unlocked"

const resize = new ResizeObserver((entries) => {
  for (const entry of entries) {
    const row = entry.target.__row;
    if (row) row.drawTrack();
    const panel = entry.target.__panel;
    if (panel) panel.drawAxes();
  }
});

export function initFleet({ openUpstream }) {
  onOpenUpstream = openUpstream;
}

export function setControlMode(mode) {
  controlMode = mode;
  for (const panel of panels.values()) {
    for (const row of panel.rows.values()) row.updateAction();
  }
}

export function renderFleet(status) {
  const root = document.getElementById("listeners");
  const seen = new Set();
  status.listeners.forEach((listener, index) => {
    seen.add(listener.name);
    let panel = panels.get(listener.name);
    if (!panel) {
      panel = new ListenerPanel(listener.name);
      panels.set(listener.name, panel);
    }
    if (root.children[index] !== panel.node) root.insertBefore(panel.node, root.children[index] || null);
    panel.update(listener, status);
  });
  for (const [name, panel] of panels) {
    if (!seen.has(name)) {
      panel.node.remove();
      panels.delete(name);
    }
  }
  root.querySelector(".placeholder")?.remove();
  renderSummary(status);
}

function renderSummary(status) {
  const t = status.totals;
  const node = document.getElementById("fleet-summary");
  // upstreams_up excludes operator-disabled upstreams, so count the two
  // reasons separately instead of calling a disabled node offline.
  const upstreams = status.listeners.flatMap((l) => l.upstreams);
  const down = upstreams.filter((u) => !u.online).length;
  const disabled = upstreams.filter((u) => u.operator_disabled).length;
  const parts = [
    el("b", null, plural(t.sessions, "session", "sessions")),
    el("span", null, `${int(t.upstreams_up)} of ${int(t.upstreams)} upstreams serving`),
    down > 0 ? el("span.warn", null, `${int(down)} offline`) : null,
    disabled > 0 ? el("span.warn", null, `${int(disabled)} disabled`) : null,
    status.listeners.length > 1 ? el("span", null, plural(status.listeners.length, "listener", "listeners")) : null,
  ];
  node.replaceChildren(...parts.filter(Boolean));
}

class ListenerPanel {
  constructor(name) {
    this.name = name;
    this.rows = new Map();
    this.axis = null;

    this.title = el("h2.plate-name", { id: `listener-${cssId(name)}` });
    this.meta = el("div.plate-meta");
    this.axisWide = el("div.h-track", { role: "columnheader" });
    this.axisWide.append(el("span.sr-only", { text: "Block track" }));
    this.axisWideSvg = svgShell("board-axis");
    this.axisWide.append(this.axisWideSvg);
    this.axisNarrow = el("div.axis-narrow", { "aria-hidden": "true" });
    this.axisNarrowSvg = svgShell("board-axis");
    this.axisNarrow.append(this.axisNarrowSvg);
    this.axisWide.__panel = this;
    this.axisNarrow.__panel = this;

    const head = el(
      "div.board-head",
      { role: "row" },
      el("div", { role: "columnheader" }, "Upstream"),
      this.axisWide,
      el("div.num", { role: "columnheader" }, "Sessions"),
      el("div.num", { role: "columnheader" }, "Throughput"),
      el("div.num", { role: "columnheader" }, "Head"),
      el("div.num", { role: "columnheader" }, "Failovers"),
      el("div", { role: "columnheader" }, el("span.sr-only", { text: "Actions" })),
    );
    this.body = el("div.board-rows", { role: "rowgroup" });
    this.board = el(
      "div.board",
      { role: "table", "aria-labelledby": this.title.id },
      el("div", { role: "rowgroup" }, head),
      this.axisNarrow,
      this.body,
    );
    this.node = el("section.panel", { "aria-labelledby": this.title.id }, el("div.plate", null, this.title, this.meta), this.board);
    resize.observe(this.axisWide);
    resize.observe(this.axisNarrow);
  }

  update(listener, status) {
    setText(this.title, listener.name);
    this.renderMeta(listener);

    const best = bestHead(listener);
    let min = null;
    for (const u of listener.upstreams) {
      if (u.trace && u.trace.end > 0 && (min === null || u.trace.begin < min)) min = u.trace.begin;
    }
    this.axis = best === null ? null : { min: min ?? 0, best };

    const seen = new Set();
    listener.upstreams.forEach((u, index) => {
      seen.add(u.endpoint);
      let row = this.rows.get(u.endpoint);
      if (!row) {
        row = new UpstreamRow(listener.name, u.endpoint);
        this.rows.set(u.endpoint, row);
      }
      if (this.body.children[index] !== row.node) this.body.insertBefore(row.node, this.body.children[index] || null);
      row.update(u, this.axis, status, listener.chain_api?.upstreams.find((api) => api.ship_endpoint === u.endpoint));
    });
    for (const [endpoint, row] of this.rows) {
      if (!seen.has(endpoint)) {
        resize.unobserve(row.trackCell);
        row.node.remove();
        this.rows.delete(endpoint);
      }
    }
    this.drawAxes();
  }

  renderMeta(l) {
    const usage = l.max_connections ? l.sessions / l.max_connections : 0;
    const meter = el("span.meter", { "aria-hidden": "true" }, el("i", { style: `width:${Math.min(100, usage * 100).toFixed(1)}%` }));
    if (usage >= 0.8) meter.classList.add("is-warn");
    const api = l.chain_api;
    const apiMeta = api
      ? el(
          `span${api.ready ? "" : ".is-warn"}`,
          { title: `Chain API at ${api.listen}; ${api.usable_upstreams} of ${api.upstreams_total} upstream APIs usable${api.listening ? "" : "; listener not listening"}` },
          "chain API ",
          el("b", null, api.listen),
          el("span.lamp", { "data-status": api.ready ? "up" : "down", "aria-hidden": "true" }),
          el("b", null, api.ready ? "ready" : api.listening ? "not ready" : "not listening"),
          el("span", null, `${int(api.usable_upstreams)}/${int(api.upstreams_total)} usable`),
        )
      : el("span", null, "chain API ", el("b", null, "not configured"));
    this.meta.replaceChildren(
      el("span", null, "port ", el("b", null, `:${l.port}`)),
      el("span", { title: l.chain_id || "chain_id not pinned" }, "chain ", el("b", null, l.chain_id ? shortHash(l.chain_id) : "not pinned")),
      el(
        `span${l.ready ? "" : ".is-warn"}`,
        null,
        el("span.lamp", { "data-status": l.ready ? "up" : "down", "aria-hidden": "true" }),
        el("b", null, l.ready ? "ready" : "not ready"),
      ),
      apiMeta,
      el("span", null, "sessions ", el("b", null, `${int(l.sessions)} / ${int(l.max_connections)}`), meter),
      el("span", null, "accepted ", el("b", null, int(l.accepted_total))),
      el(`span${l.rejected_total > 0 ? ".is-warn" : ""}`, null, "rejected ", el("b", null, int(l.rejected_total))),
    );
  }

  drawAxes() {
    for (const [cell, svgEl] of [
      [this.axisWide, this.axisWideSvg],
      [this.axisNarrow, this.axisNarrowSvg],
    ]) {
      const width = cell.clientWidth;
      if (!width) continue;
      svgEl.setAttribute("viewBox", `0 0 ${width} 22`);
      svgEl.innerHTML = this.axis ? axisMarkup(width, this.axis) : "";
    }
  }
}

class UpstreamRow {
  constructor(listener, endpoint) {
    this.listener = listener;
    this.endpoint = endpoint;
    this.key = upstreamKey(listener, endpoint);
    this.data = null;
    this.axis = null;

    this.lamp = el("span.lamp", { "aria-hidden": "true" });
    this.name = el("button.u-name", { type: "button", onclick: () => onOpenUpstream(listener, endpoint) });
    this.endpointText = el("span.u-endpoint.mono", { text: endpoint });
    this.tags = el("span.u-tags");
    this.tags.style.display = "contents";
    const id = el(
      "div.u-id",
      { role: "cell" },
      this.lamp,
      el("span", { style: "min-width:0;display:flex" }, this.name),
      el("div.u-sub", null, this.endpointText, this.tags),
    );

    this.trackSvg = svgShell("trk");
    this.trackSvg.setAttribute("role", "img");
    this.trackCell = el("div.u-track", { role: "cell" }, this.trackSvg);
    this.trackCell.__row = this;

    this.sessions = el("span.big");
    this.sessionsSpark = sparkShell();
    this.throughput = el("span.big");
    this.bytesRate = el("span.small");
    this.throughputSpark = sparkShell();
    this.lag = el("span.big");
    this.head = el("span.small");
    this.failovers = el("span.big");
    this.drops = el("span.small");

    const metric = (label, ...children) => el("div.u-metric", { role: "cell" }, el("span.u-label", { "aria-hidden": "true", text: label }), ...children);
    const metrics = el(
      "div.u-metrics-wrap",
      { role: "none" },
      metric("Sessions", this.sessions, this.sessionsSpark),
      metric("Throughput", this.throughput, this.bytesRate, this.throughputSpark),
      metric("Head", this.lag, this.head),
      metric("Failovers", this.failovers, this.drops),
    );

    this.action = el("button.button.button-small", { type: "button", onclick: () => onOpenUpstream(listener, endpoint) });
    this.node = el("div.urow", { role: "row" }, id, this.trackCell, metrics, el("div.u-act", { role: "cell" }, this.action));
    resize.observe(this.trackCell);
  }

  update(u, axis, status, api) {
    this.data = u;
    this.axis = axis;
    const flashing = isFlashing(this.key);
    const lampState = flashing ? "failover" : u.status;
    this.lamp.dataset.status = lampState;
    this.lamp.title = STATUS_LABEL[u.status] || u.status;
    setText(this.name, u.name || u.endpoint);
    this.name.setAttribute("aria-label", `${u.name}, ${u.status}. Open details`);

    const tags = [];
    if (api) {
      const label = api.status.replaceAll("_", " ");
      tags.push(el(
        `span.tag${api.status === "up" ? "" : ".tag-warn"}`,
        { title: `Chain API ${api.api_endpoint} (${api.role}): ${label}` },
        `API ${label}`,
      ));
    }
    if (u.tls) tags.push(el("span.tag", { title: "Upstream dialed with TLS (wss://)" }, "TLS"));
    const overridden = u.config && (u.config.priority !== u.priority || u.config.weight !== u.weight);
    tags.push(
      el(
        `span.tag${overridden ? ".tag-override" : ""}`,
        { title: overridden ? `Changed at runtime. Config: priority ${u.config.priority}, weight ${u.config.weight}` : "Priority and weight" },
        `p${u.priority} w${u.weight}`,
        overridden ? " runtime" : null,
      ),
    );
    if (u.operator_disabled) tags.push(el("span.tag.tag-warn", { title: "Disabled by an operator through the control API" }, "operator disabled"));
    for (const q of u.quarantines || []) {
      tags.push(
        el(
          "span.tag.tag-warn",
          { title: `Skipped for blocks ${int(q.block)} to ${int(q.block + q.range - 1)} until the quarantine expires` },
          el("span.key.key-quarantine", { "aria-hidden": "true" }),
          `${int(q.block)}${q.range > 1 ? `+${q.range - 1}` : ""} ${countdown(q.expires_in_secs)}`,
        ),
      );
    }
    this.tags.replaceChildren(...tags);

    setText(this.sessions, int(u.sessions));
    const r = currentRates(this.key);
    setText(this.throughput, rate(r.bps, "blk/s"));
    setText(this.bytesRate, byteRate(r.Bps));

    const lag = headLag(u);
    if (u.chain_head_block === null || u.chain_head_block === undefined) {
      setText(this.lag, "–");
      this.lag.className = "big";
      setText(this.head, "no status yet");
    } else {
      setText(this.lag, lag === null ? "sampled" : lag === 0 ? "at head" : `−${int(lag)}`);
      this.lag.className = `big ${lag === 0 ? "is-set" : lag > LAG_WARN ? "is-warn" : ""}`;
      this.lag.title = lag === null ? "Last reported chain head; no live peer comparison available"
        : lag === 0 ? "No lag observed against live peers at the last status request; differences within a polling interval may not be visible"
        : `At least ${int(lag)} blocks behind the live peer head already known when this status request began`;
      setText(this.head, `#${int(u.chain_head_block)}`);
      this.head.title = u.head_sample_age_ms === null || u.head_sample_age_ms === undefined
        ? "Last reported chain head"
        : `Chain head sampled ${(u.head_sample_age_ms / 1000).toFixed(1)} seconds ago`;
    }

    setText(this.failovers, int(u.failovers_total));
    this.failovers.title = "Sessions that left this upstream because the stream failed or an operator moved them";
    setText(this.drops, `${int(u.block_failures_total)} drop${u.block_failures_total === 1 ? "" : "s"}`);
    this.drops.className = `small ${u.block_failures_total ? "is-warn" : ""}`;
    this.drops.title = "Unclean mid-stream drops (fleet_router_upstream_block_failures_total)";

    this.drawSparks();
    this.drawTrack();
    this.updateAction();
  }

  updateAction() {
    const label = controlMode === "unlocked" ? "Manage" : "Details";
    setText(this.action, label);
    this.action.setAttribute("aria-label", `${label}: ${this.data?.name || this.endpoint}`);
  }

  drawSparks() {
    const s = store.series.get(this.key);
    if (!s) return;
    this.sessionsSpark.innerHTML = sparkMarkup(s.sessions.slice(-SPARK_POINTS), 1);
    this.throughputSpark.innerHTML = sparkMarkup(s.bps.slice(-SPARK_POINTS), 1);
  }

  drawTrack() {
    const u = this.data;
    if (!u) return;
    const width = this.trackCell.clientWidth;
    if (!width) return;
    const flashing = isFlashing(this.key);
    this.trackSvg.setAttribute("viewBox", `0 0 ${width} 30`);
    this.trackSvg.setAttribute("aria-label", trackLabel(u, this.axis));
    this.trackSvg.innerHTML = this.axis ? trackMarkup(width, this.axis, u, flashing ? "failover" : u.status) : "";
  }
}

/* ---------- drawing ---------- */

function svgShell(cls) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  node.setAttribute("class", cls);
  node.setAttribute("preserveAspectRatio", "none");
  node.setAttribute("focusable", "false");
  return node;
}

function sparkShell() {
  const node = svgShell("spark");
  node.setAttribute("viewBox", "0 0 100 18");
  node.setAttribute("aria-hidden", "true");
  return node;
}

/** Sparkline path in a 100 x 18 box. Buckets keep the max so short spikes survive. */
function sparkMarkup(values, floor) {
  const vals = values.map((v) => (v === null ? 0 : v));
  if (vals.length < 2) return "";
  const buckets = 50;
  const per = Math.max(1, vals.length / buckets);
  const pts = [];
  for (let i = 0; i < vals.length; i += per) {
    const slice = vals.slice(Math.floor(i), Math.floor(i + per));
    pts.push(Math.max(...slice));
  }
  const max = Math.max(floor, ...pts);
  const step = 100 / Math.max(1, pts.length - 1);
  const coords = pts.map((v, i) => [i * step, 17 - (v / max) * 15]);
  const line = coords.map(([x, y], i) => `${i ? "L" : "M"}${f(x)} ${f(y)}`).join("");
  const area = `${line}L100 18L0 18Z`;
  return `<path class="spark-area" d="${area}"/><path d="${line}"/>`;
}

function f(n) {
  return Number.isFinite(n) ? n.toFixed(1) : "0";
}

function cssId(name) {
  return name.replace(/[^a-zA-Z0-9_-]/g, "_");
}
