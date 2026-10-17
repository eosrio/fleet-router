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
  status: () => request("GET", "status"),
  sessions: () => request("GET", "sessions"),
  history: () => request("GET", "history"),
  events: (after) => request("GET", after ? `events?after=${encodeURIComponent(after)}` : "events"),
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

/**
 * Open the live stream. Calls `onSnapshot(status)`, `onEvent(event)` and
 * `onState("live" | "reconnecting")`. EventSource reconnects by itself.
 */
export function openStream({ onSnapshot, onEvent, onState }) {
  const source = new EventSource(apiUrl("stream"));
  source.addEventListener("open", () => onState("live"));
  source.addEventListener("error", () => onState("reconnecting"));
  source.addEventListener("snapshot", (e) => {
    try {
      onSnapshot(JSON.parse(e.data));
    } catch (err) {
      console.error("bad snapshot", err);
    }
  });
  source.addEventListener("event", (e) => {
    try {
      onEvent(JSON.parse(e.data));
    } catch (err) {
      console.error("bad event", err);
    }
  });
  return source;
}
