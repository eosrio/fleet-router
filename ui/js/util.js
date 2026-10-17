// Small DOM and formatting helpers shared by every view.

export const SVG_NS = "http://www.w3.org/2000/svg";

/** Create an element: el("div.a.b", { title: "x", onclick: fn }, child, "text"). */
export function el(spec, attrs, ...children) {
  const [tag, ...classes] = spec.split(".");
  const node = document.createElement(tag || "div");
  if (classes.length) node.className = classes.join(" ");
  applyAttrs(node, attrs);
  append(node, children);
  return node;
}

/** Same as `el` for SVG elements. */
export function svg(spec, attrs, ...children) {
  const [tag, ...classes] = spec.split(".");
  const node = document.createElementNS(SVG_NS, tag);
  if (classes.length) node.setAttribute("class", classes.join(" "));
  applyAttrs(node, attrs);
  append(node, children);
  return node;
}

function applyAttrs(node, attrs) {
  if (!attrs) return;
  for (const [key, value] of Object.entries(attrs)) {
    if (value === undefined || value === null || value === false) continue;
    if (key.startsWith("on") && typeof value === "function") {
      node.addEventListener(key.slice(2), value);
    } else if (key === "style") {
      // CSSOM, not the style attribute, so a strict style-src CSP still allows it.
      node.style.cssText = value;
    } else if (key === "text") {
      node.textContent = value;
    } else if (key === "dataset") {
      Object.assign(node.dataset, value);
    } else {
      node.setAttribute(key, value === true ? "" : String(value));
    }
  }
}

function append(node, children) {
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
}

/** Set text only when it changed, so screen readers and selection are not disturbed. */
export function setText(node, text) {
  const value = String(text);
  if (node.textContent !== value) node.textContent = value;
}

const intFmt = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });

export function int(n) {
  return n === null || n === undefined ? "–" : intFmt.format(n);
}

export function bytes(n) {
  if (n === null || n === undefined) return "–";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  let v = n;
  let i = 0;
  while (v >= 1000 && i < units.length - 1) {
    v /= 1000;
    i += 1;
  }
  return `${v >= 100 || i === 0 ? v.toFixed(0) : v.toFixed(1)} ${units[i]}`;
}

export function rate(n, unit) {
  if (n === null || n === undefined || !Number.isFinite(n)) return `– ${unit}`;
  if (n === 0) return `0 ${unit}`;
  if (n < 10) return `${n.toFixed(1)} ${unit}`;
  return `${intFmt.format(n)} ${unit}`;
}

export function byteRate(n) {
  if (n === null || n === undefined || !Number.isFinite(n)) return "– B/s";
  return `${bytes(n)}/s`;
}

/** Compact duration: 45s, 12m, 3h 20m, 4d 2h. */
export function duration(secs) {
  if (secs === null || secs === undefined) return "–";
  const s = Math.max(0, Math.floor(secs));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  const d = Math.floor(h / 24);
  return `${d}d ${h % 24}h`;
}

/** Countdown as m:ss (or h:mm:ss). */
export function countdown(secs) {
  const s = Math.max(0, Math.floor(secs));
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const ss = String(s % 60).padStart(2, "0");
  return h ? `${h}:${String(m).padStart(2, "0")}:${ss}` : `${m}:${ss}`;
}

export function clock(ms) {
  const d = new Date(ms);
  return [d.getHours(), d.getMinutes(), d.getSeconds()].map((v) => String(v).padStart(2, "0")).join(":");
}

export function shortHash(hex) {
  if (!hex) return "–";
  return hex.length > 16 ? `${hex.slice(0, 8)}…${hex.slice(-4)}` : hex;
}

export function upstreamKey(listener, endpoint) {
  return `${listener}|${endpoint}`;
}

export const STATUS_LABEL = {
  up: "up",
  stale: "stale",
  quarantined: "quarantined",
  down: "down",
  disabled: "disabled",
};

export function plural(n, one, many) {
  return `${int(n)} ${n === 1 ? one : many}`;
}

export function storageGet(store, key) {
  try {
    return window[store].getItem(key);
  } catch {
    return null;
  }
}

export function storageSet(store, key, value) {
  try {
    if (value === null) window[store].removeItem(key);
    else window[store].setItem(key, value);
  } catch {
    /* storage unavailable (private mode, blocked): the dashboard still works */
  }
}

const announcer = () => document.getElementById("announcer");

/** Politely announce a short message to screen readers. */
export function announce(message) {
  const node = announcer();
  if (!node) return;
  node.textContent = "";
  window.setTimeout(() => {
    node.textContent = message;
  }, 50);
}
