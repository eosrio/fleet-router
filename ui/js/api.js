// Client for /api/v1 (see docs/api.md). Paths are resolved against the page,
// so the dashboard also works behind a reverse proxy that adds a path prefix.

import { storageGet, storageSet } from "./util.js";

const BASE = new URL("../api/v1/", window.location.href);
const TOKEN_KEY = "fleet-router-control-token";

export class ApiError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

export function apiUrl(path) {
  return new URL(path.replace(/^\//, ""), BASE).toString();
}

export function getToken() {
  return storageGet("sessionStorage", TOKEN_KEY);
}

export function setToken(token) {
  storageSet("sessionStorage", TOKEN_KEY, token);
}

async function request(method, path, { body, token, auth = false } = {}) {
  const headers = { Accept: "application/json" };
  if (body !== undefined) headers["Content-Type"] = "application/json";
  if (auth) {
    const t = token ?? getToken();
    if (t) headers.Authorization = `Bearer ${t}`;
  }
  let res;
  try {
    res = await fetch(apiUrl(path), {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      cache: "no-store",
    });
  } catch {
    throw new ApiError(0, "The router did not answer. Check that it is running and reachable.");
  }
  if (res.status === 204) return null;
  let data = null;
  const text = await res.text();
  if (text) {
    try {
      data = JSON.parse(text);
    } catch {
      data = null;
    }
  }
  if (!res.ok) {
    const message = (data && data.error) || `${res.status} ${res.statusText}`.trim();
    throw new ApiError(res.status, message);
  }
  return data;
}

export const api = {
  config: () => request("GET", "config", { auth: true }),
  validateConfig: (body) => request("POST", "config/validate", { auth: true, body }),
  saveConfig: (body) => request("PUT", "config", { auth: true, body }),
  reloadConfig: (body) => request("POST", "config/reload", { auth: true, body }),
  checkToken: (token) => request("GET", "auth", { auth: true, token }),

  upstreamAction(listener, endpoint, action) {
    return request("POST", `${upstreamPath(listener, endpoint)}/${action}`, { auth: true });
  },
  patchUpstream(listener, endpoint, body) {
    return request("PATCH", upstreamPath(listener, endpoint), { auth: true, body });
  },
  sessionAction(id, action) {
    return request("POST", `sessions/${encodeURIComponent(id)}/${action}`, { auth: true });
  },
};

function upstreamPath(listener, endpoint) {
  return `listeners/${encodeURIComponent(listener)}/upstreams/${encodeURIComponent(endpoint)}`;
}

/** One server stream bootstraps and updates every read model. EventSource
 * handles transport reconnects; protocol gaps also reconnect for a full resync.
 * "Live" means a complete frame has been applied, not just an open socket. */
export function openStream({ onFrame, onState }) {
  let source;
  let retry;
  let closed = false;
  function connect() {
    if (closed) return;
    const connection = new EventSource(apiUrl("dashboard/stream"));
    source = connection;
    connection.addEventListener("error", () => {
      if (source === connection && !closed) onState("reconnecting");
    });
    connection.addEventListener("dashboard", (e) => {
      if (source !== connection || closed) return;
      try {
        onFrame(JSON.parse(e.data));
        onState("live");
      } catch (err) {
        console.error("Invalid dashboard update; resyncing", err);
        connection.close();
        source = null;
        onState("reconnecting");
        retry = window.setTimeout(connect, 1000);
      }
    });
  }
  connect();
  return { close() { closed = true; source?.close(); window.clearTimeout(retry); } };
}
