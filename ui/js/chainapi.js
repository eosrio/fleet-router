// Chain API backends, one section under each listener that has a chain API.
// Strings from the router are set as text, never as HTML.

import { duration, el, int, rate, setText } from "./util.js";

const RATE_WINDOW = 5;
const rates = new Map();
let seen = new Set();
let instance = null;

/** Drop rate samples for backends that disappeared from the latest status. */
export function beginChainApiPass(startedMs, reset = false) {
  if (reset || startedMs !== instance) rates.clear();
  instance = startedMs;
  seen = new Set();
}

export function endChainApiPass() {
  for (const key of rates.keys()) {
    if (!seen.has(key)) rates.delete(key);
  }
}

/** Independent listener readiness, also shown beside SHiP readiness. */
export function chainApiReadiness(api) {
  if (!api) return el("span", null, "chain API ", el("b", null, "not configured"));
  return el(
    `span${api.ready ? "" : ".is-warn"}`,
    { title: [`Chain API at ${api.listen}`, ...(api.readiness_reasons || [])].join("; ") },
    "chain API ",
    el("span.lamp", { "data-status": api.ready ? "up" : "down", "aria-hidden": "true" }),
    el("b", null, api.ready ? "ready" : api.listening ? "not ready" : "not listening"),
    ` · ${int(api.usable_upstreams)}/${int(api.upstreams_total)} usable`,
  );
}

/** Explain why a probe-up backend may not be usable. */
export function chainApiBackendStatus(backend) {
  if (backend.operator_disabled) return "disabled";
  if (backend.chain_mismatch) return "chain mismatch";
  if (backend.status !== "up") return backend.status || "down";
  if (backend.verified === false) return "unverified";
  return "up";
}

export class ChainApiSection {
  constructor(listener) {
    this.listener = listener;
    this.rows = new Map();
    this.title = el("h3.plate-name", { id: `chain-api-${cssId(listener)}` });
    setText(this.title, "Chain API");
    this.meta = el("div.plate-meta");
    const head = el(
      "div.board-head",
      { role: "row" },
      el("div", { role: "columnheader" }, "Backend"),
      el("div.num", { role: "columnheader" }, "In flight"),
      el("div.num", { role: "columnheader" }, "Requests"),
      el("div.num", { role: "columnheader" }, "Errors"),
      el("div.num", { role: "columnheader" }, "Latency"),
      el("div.num", { role: "columnheader" }, "Last ok"),
      el("div.num", { role: "columnheader" }, "Head"),
    );
    this.body = el("div.board-rows", { role: "rowgroup" });
    this.board = el(
      "div.board.api-board",
      { role: "table", "aria-labelledby": this.title.id },
      el("div", { role: "rowgroup" }, head),
      this.body,
    );
    this.node = el(
      "section.api-section",
      { "aria-labelledby": this.title.id },
      el("div.plate", null, this.title, this.meta),
      this.board,
    );
    this.node.hidden = true;
  }

  update(api, nowMs) {
    if (!api || !Array.isArray(api.upstreams)) {
      this.node.hidden = true;
      return;
    }
    this.node.hidden = false;
    const up = api.upstreams.filter((b) => b.status === "up").length;
    const down = api.upstreams.filter((b) => b.status === "down").length;
    const disabled = api.upstreams.filter((b) => b.status === "disabled").length;
    this.meta.replaceChildren(
      ...[
        el("span", null, "listen ", el("b", null, api.listen || "–")),
        chainApiReadiness(api),
        el("span", null, `${int(up)} of ${int(api.upstreams.length)} probes up`),
        down > 0 ? el("span.is-warn", null, `${int(down)} down`) : null,
        disabled > 0 ? el("span.is-warn", null, `${int(disabled)} disabled`) : null,
        ...(api.readiness_reasons || []).map((reason) => el("span.is-warn", null, reason)),
      ].filter((node) => node),
    );

    const keep = new Set();
    api.upstreams.forEach((backend, index) => {
      const key = `${this.listener}|${backend.endpoint}`;
      keep.add(key);
      seen.add(key);
      let row = this.rows.get(key);
      if (!row) {
        row = new BackendRow();
        this.rows.set(key, row);
      }
      if (this.body.children[index] !== row.node) {
        this.body.insertBefore(row.node, this.body.children[index] || null);
      }
      row.update(backend, nowMs, sampleRate(key, nowMs, backend.requests_total, backend.errors_total));
    });
    for (const [key, row] of this.rows) {
      if (!keep.has(key)) {
        row.node.remove();
        this.rows.delete(key);
      }
    }
  }
}

class BackendRow {
  constructor() {
    this.lamp = el("span.lamp", { "aria-hidden": "true" });
    this.name = el("span.u-name");
    this.endpoint = el("span.u-endpoint.mono");
    this.tags = el("span.u-tags");
    this.tags.style.display = "contents";
    const id = el(
      "div.u-id",
      { role: "cell" },
      this.lamp,
      el("span", { style: "min-width:0;display:flex" }, this.name),
      el("div.u-sub", null, this.endpoint, this.tags),
    );

    this.inFlight = el("span.big");
    this.inFlightCap = el("span.small");
    this.requests = el("span.big");
    this.requestRate = el("span.small");
    this.errors = el("span.big");
    this.errorRate = el("span.small");
    this.latency = el("span.big");
    this.latencyDetail = el("span.small");
    this.lastOk = el("span.big");
    this.lastErr = el("span.small");
    this.head = el("span.big");
    this.headDetail = el("span.small");

    const metric = (label, ...children) =>
      el("div.u-metric", { role: "cell" }, el("span.u-label", { "aria-hidden": "true", text: label }), ...children);
    const metrics = el(
      "div.u-metrics-wrap",
      { role: "none" },
      metric("In flight", this.inFlight, this.inFlightCap),
      metric("Requests", this.requests, this.requestRate),
      metric("Errors", this.errors, this.errorRate),
      metric("Latency", this.latency, this.latencyDetail),
      metric("Last ok", this.lastOk, this.lastErr),
      metric("Head", this.head, this.headDetail),
    );
    this.node = el("div.urow", { role: "row" }, id, metrics);
  }

  update(b, nowMs, ratesNow) {
    this.lamp.dataset.status = b.status === "up" && b.usable === false ? "stale" :
      b.status === "up" || b.status === "down" || b.status === "disabled" ? b.status : "down";
    this.lamp.title = chainApiBackendStatus(b);
    setText(this.name, b.name || b.endpoint || "");
    setText(this.endpoint, b.endpoint || "");
    this.tags.replaceChildren(...tagsFor(b));

    const full = b.max_in_flight > 0 && b.in_flight >= b.max_in_flight;
    setText(this.inFlight, int(b.in_flight));
    this.inFlight.className = `big ${full ? "is-warn" : ""}`;
    setText(this.inFlightCap, `/ ${int(b.max_in_flight)}`);

    setText(this.requests, int(b.requests_total));
    setText(this.requestRate, rate(ratesNow.reqPerS, "req/s"));

    const pct = percent(b.errors_total, b.requests_total);
    setText(this.errors, int(b.errors_total));
    this.errors.className = `big ${b.errors_total > 0 ? "is-warn" : ""}`;
    setText(this.errorRate, ratesNow.errPerS ? `${pct} · ${rate(ratesNow.errPerS, "/s")}` : pct);
    this.errorRate.className = `small ${b.errors_total > 0 ? "is-warn" : ""}`;
    this.errorRate.title = breakdown(b.results);

    setText(this.latency, latency(b.latency_last_us));
    this.latency.title = "Last attempt, until the response head or the failure";
    setText(this.latencyDetail, percentileLine(b));

    setText(this.lastOk, age(nowMs, b.last_success_ms));
    this.lastOk.title = "Last 2xx from this backend";
    const errAge = age(nowMs, b.last_error_ms);
    setText(this.lastErr, b.last_error_ms == null ? "no errors" : `err ${errAge}`);
    this.lastErr.className = `small ${b.last_error_ms == null ? "" : "is-warn"}`;
    this.lastErr.title = "Last busy, error, connect or timeout. Unknown Block and 404 are not counted.";

    if (b.head_block == null) {
      setText(this.head, "–");
      setText(this.headDetail, "unknown");
    } else {
      setText(this.head, `#${int(b.head_block)}`);
      setText(
        this.headDetail,
        b.blocks && b.blocks.end > b.blocks.begin
          ? `${int(b.blocks.begin)}–${int(b.blocks.end - 1)}`
          : b.role || "",
      );
    }
  }
}

function tagsFor(b) {
  const label = chainApiBackendStatus(b);
  const tags = [
    el("span.tag", null, b.role || ""),
    el(`span.tag${b.usable ? "" : ".tag-warn"}`, { title: b.operator_disabled ? "Disabled by an operator" : null }, label),
  ];
  if (b.chain_mismatch && label !== "chain mismatch") tags.push(el("span.tag.tag-warn", null, "chain mismatch"));
  if (b.busy) tags.push(el("span.tag.tag-warn", null, "busy"));
  if (b.verified === false) tags.push(el("span.tag.tag-warn", { title: "Not verified against its SHiP endpoint" }, label === "unverified" ? "not used" : "unverified; not used"));
  if (b.verified === true) tags.push(el("span.tag", { title: "Verified against its SHiP endpoint" }, "verified"));
  return tags;
}

function sampleRate(key, now, requests, errors) {
  let s = rates.get(key);
  if (!s) {
    s = { last: null, req: [], err: [] };
    rates.set(key, s);
  }
  if (s.last) {
    const dt = (now - s.last.t) / 1000;
    if (dt < 0 || dt >= 30 || requests < s.last.requests || errors < s.last.errors) {
      s.req = [];
      s.err = [];
    } else if (dt === 0) {
      // Duplicate renders must not consume increments before the next sample.
      return { reqPerS: avg(s.req), errPerS: avg(s.err) };
    } else {
      s.req.push((requests - s.last.requests) / dt);
      s.err.push((errors - s.last.errors) / dt);
      if (s.req.length > RATE_WINDOW) {
        s.req.shift();
        s.err.shift();
      }
    }
  }
  s.last = { t: now, requests, errors };
  return { reqPerS: avg(s.req), errPerS: avg(s.err) };
}

function avg(values) {
  return values.length ? values.reduce((a, b) => a + b, 0) / values.length : null;
}

function percent(errors, requests) {
  if (!requests) return "–";
  const pct = (errors / requests) * 100;
  if (pct > 0 && pct < 0.1) return "<0.1%";
  if (pct >= 10) return `${pct.toFixed(0)}%`;
  return `${pct.toFixed(1)}%`;
}

function latency(us) {
  if (us == null || !Number.isFinite(us)) return "–";
  if (us < 1_000) return `${Math.round(us)} us`;
  if (us < 10_000) return `${(us / 1_000).toFixed(1)} ms`;
  if (us < 1_000_000) return `${Math.round(us / 1_000)} ms`;
  return `${(us / 1_000_000).toFixed(1)} s`;
}

function percentileLine(b) {
  if (b.latency_last_us == null) return "no samples";
  const p50 = b.latency_p50_us == null ? "≥10s" : latency(b.latency_p50_us);
  const p95 = b.latency_p95_us == null ? "≥10s" : latency(b.latency_p95_us);
  const mean = b.latency_avg_us == null ? "–" : latency(b.latency_avg_us);
  return `p50 ${p50} · p95 ${p95} · avg ${mean}`;
}

function age(now, then) {
  if (then == null || !Number.isFinite(then) || !Number.isFinite(now) || now < then) return "–";
  return duration(Math.floor((now - then) / 1000));
}

function breakdown(results) {
  if (!results) return "";
  const parts = [
    ["ok", results.ok],
    ["unknown block", results.unknown_block],
    ["not found", results.not_found],
    ["busy", results.busy],
    ["error", results.error],
    ["connect", results.connect],
    ["timeout", results.timeout],
  ];
  return parts.map(([name, n]) => `${name} ${int(n)}`).join(", ");
}

function cssId(name) {
  return String(name).replace(/[^a-zA-Z0-9_-]/g, "_");
}
