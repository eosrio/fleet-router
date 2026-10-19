// Control API: unlock with a token, per-upstream details and actions, session
// actions, confirmations and toasts. Everything is in-page; no browser dialogs.

import { api, ApiError, getToken, setToken } from "./api.js";
import { headLag, findUpstream, store } from "./store.js";
import { announce, countdown, el, int, setText, shortHash, STATUS_LABEL } from "./util.js";

const DOCS_URL = "https://github.com/eosrio/fleet-router/blob/main/docs/configuration.md#dashboard";

let mode = "off"; // "off" | "locked" | "unlocked"
let enabled = false;
let verified = false;
let modeListeners = [];
let applyUpstream = () => {};
let open = null; // { listener, endpoint }
let controlsSignature = "";

const STATUS_TEXT = {
  up: "Online and advancing.",
  stale: "Online, but its chain state stopped advancing. Still routable, ranked lower.",
  quarantined: "Online, skipped only for its quarantined block ranges.",
  down: "Offline: unreachable, missed its status deadline, or rejected for chain_id.",
  disabled: "Disabled by an operator. New sessions are not routed here.",
};

export function initControls({ onModeChange, onUpstreamChanged }) {
  modeListeners.push(onModeChange);
  applyUpstream = onUpstreamChanged;

  const button = document.getElementById("control-button");
  button.addEventListener("click", () => {
    if (mode === "off") showControlsOffInfo();
    else if (mode === "locked") openUnlock();
    else showUnlockedInfo();
  });

  const unlock = document.getElementById("unlock-dialog");
  const form = document.getElementById("unlock-form");
  unlock.querySelectorAll("[data-close]").forEach((b) => b.addEventListener("click", () => unlock.close()));
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const input = document.getElementById("unlock-token");
    const error = document.getElementById("unlock-error");
    const submit = form.querySelector('button[type="submit"]');
    const token = input.value.trim();
    if (!token) return;
    submit.disabled = true;
    error.hidden = true;
    input.removeAttribute("aria-invalid");
    try {
      await api.checkToken(token);
      setToken(token);
      verified = true;
      unlock.close();
      input.value = "";
      updateMode();
      toast("Controls unlocked for this tab.");
    } catch (err) {
      error.hidden = false;
      input.setAttribute("aria-invalid", "true");
      error.textContent =
        err.status === 401 ? "That token was not accepted. Use the control token configured for this router." : err.message;
      input.focus();
      input.select();
    } finally {
      submit.disabled = false;
    }
  });

  const upstreamDialog = document.getElementById("upstream-dialog");
  upstreamDialog.addEventListener("close", () => {
    open = null;
    controlsSignature = "";
  });
}

/** Called with each status: enables the control button and re-checks a stored token. */
export function syncControlState(status) {
  const wasEnabled = enabled;
  enabled = Boolean(status.control && status.control.enabled);
  const button = document.getElementById("control-button");
  button.hidden = false;
  if (enabled && !wasEnabled && getToken() && !verified) {
    api
      .checkToken()
      .then(() => {
        verified = true;
        updateMode();
      })
      .catch(() => {
        setToken(null);
        updateMode();
      });
  }
  if (!enabled) verified = false;
  updateMode();
}

function updateMode() {
  const next = !enabled ? "off" : verified && getToken() ? "unlocked" : "locked";
  const button = document.getElementById("control-button");
  button.dataset.state = next;
  const label = button.querySelector(".control-label");
  setText(label, next === "off" ? "Controls off" : next === "locked" ? "Unlock controls" : "Controls on");
  button.setAttribute(
    "aria-label",
    next === "off" ? "Controls are off. How to enable them" : next === "locked" ? "Unlock controls" : "Controls are unlocked. Lock them",
  );
  if (next === mode) return;
  mode = next;
  for (const fn of modeListeners) fn(mode);
  if (open) renderUpstreamDialog(true);
}

export function controlMode() {
  return mode;
}

function lock() {
  setToken(null);
  verified = false;
  updateMode();
  toast("Controls locked.");
}

function handleAuthError(err) {
  if (err instanceof ApiError && err.status === 401) {
    setToken(null);
    verified = false;
    updateMode();
    toast("The control token is no longer accepted. Unlock controls again.", "error");
    openUnlock();
    return true;
  }
  if (err instanceof ApiError && err.status === 403) {
    enabled = false;
    verified = false;
    updateMode();
    toast("Controls are off on this router.", "error");
    return true;
  }
  return false;
}

function openUnlock() {
  const dialog = document.getElementById("unlock-dialog");
  document.getElementById("unlock-error").hidden = true;
  if (!dialog.open) dialog.showModal();
  document.getElementById("unlock-token").focus();
}

function offNote() {
  return el(
    "div.note",
    null,
    el("span.key.key-offline", { "aria-hidden": "true" }),
    el(
      "p",
      null,
      "Controls are off. Set ",
      el("code", null, "dashboard.control_token_file"),
      " (or ",
      el("code", null, "FLEET_ROUTER_CONTROL_TOKEN"),
      ") and restart to enable them. See ",
      el("a", { href: DOCS_URL, target: "_blank", rel: "noopener" }, "Dashboard configuration"),
      ".",
    ),
  );
}

function showInfo(title, ...body) {
  const dialog = document.getElementById("info-dialog");
  setText(document.getElementById("info-title"), title);
  document.getElementById("info-body").replaceChildren(...body);
  if (!dialog.open) dialog.showModal();
}

function showControlsOffInfo() {
  showInfo(
    "Controls are off",
    offNote(),
    el("p", null, "Everything on this dashboard stays readable without a token. Control actions change routing at runtime and need one."),
  );
}

function showUnlockedInfo() {
  const lockButton = el("button.button", { type: "button" }, "Lock controls");
  lockButton.addEventListener("click", () => {
    document.getElementById("info-dialog").close();
    lock();
  });
  showInfo(
    "Controls are unlocked",
    el("p", null, "This tab can change routing until you lock it or close the tab. The token is kept in this tab only."),
    el("p", null, "Runtime changes are not written to the config file and are lost when the router restarts."),
    el("div.sheet-actions", null, lockButton),
  );
}

/* ---------- confirm + toasts ---------- */

/** In-page confirmation. Resolves true when the operator confirms. */
export function confirmAction({ title, text, ok }) {
  const dialog = document.getElementById("confirm-dialog");
  setText(document.getElementById("confirm-title"), title);
  setText(document.getElementById("confirm-text"), text);
  setText(document.getElementById("confirm-ok"), ok);
  dialog.returnValue = "";
  dialog.showModal();
  dialog.querySelector('button[value="cancel"]').focus();
  return new Promise((resolve) => {
    dialog.addEventListener("close", () => resolve(dialog.returnValue === "ok"), { once: true });
  });
}

export function toast(message, kind = "ok") {
  const root = document.getElementById("toasts");
  const node = el(
    "div.toast",
    { dataset: { kind } },
    el(`span.key.${kind === "error" ? "key-failover" : "key-set"}`, { "aria-hidden": "true" }),
    el("span", null, message),
  );
  root.append(node);
  while (root.children.length > 4) root.firstElementChild.remove();
  raise(root);
  window.setTimeout(() => {
    node.remove();
    if (!root.children.length && root.matches(":popover-open")) root.hidePopover();
  }, kind === "error" ? 7000 : 4500);
}

/** Toasts live in a manual popover so they stay above an open modal dialog. */
function raise(root) {
  if (typeof root.showPopover !== "function") return;
  if (root.matches(":popover-open")) root.hidePopover();
  root.showPopover();
}

/* ---------- session actions ---------- */

export async function sessionAction(session, action) {
  if (mode !== "unlocked") return;
  if (action === "close") {
    const ok = await confirmAction({
      title: `Close session ${session.id}?`,
      text: `The client ${session.client} gets close code 1000 (closed by operator) and has to reconnect on its own.`,
      ok: "Close session",
    });
    if (!ok) return;
  }
  try {
    await api.sessionAction(session.id, action);
    toast(action === "close" ? `Closed session ${session.id}.` : `Moving session ${session.id} to another upstream.`);
  } catch (err) {
    if (!handleAuthError(err)) toast(`Could not ${action} session ${session.id}: ${err.message}`, "error");
  }
}

/* ---------- upstream dialog ---------- */

export function openUpstream(listener, endpoint) {
  open = { listener, endpoint };
  controlsSignature = "";
  renderUpstreamDialog(true);
  const dialog = document.getElementById("upstream-dialog");
  if (!dialog.open) dialog.showModal();
}

/** Refresh the open dialog with the latest snapshot. */
export function refreshUpstreamDialog() {
  if (open) renderUpstreamDialog(false);
}

function renderUpstreamDialog(force) {
  const u = findUpstream(open.listener, open.endpoint);
  const title = document.getElementById("upstream-dialog-title");
  const body = document.getElementById("upstream-dialog-body");
  body.dataset.mode = mode;
  if (!u) {
    title.replaceChildren("Upstream not found");
    body.replaceChildren(el("p", null, "This upstream is no longer in the router's status."));
    return;
  }
  title.replaceChildren(el("span.lamp", { "data-status": u.status, "aria-hidden": "true" }), el("span", null, u.name));

  let facts = body.querySelector(".facts");
  const newFacts = renderFacts(u);
  if (facts) facts.replaceWith(newFacts);
  else body.prepend(newFacts);

  const overridden = u.config && (u.config.priority !== u.priority || u.config.weight !== u.weight);
  const sig = [mode, u.operator_disabled, u.quarantines.length, overridden, u.sessions > 0].join("|");
  if (!force && sig === controlsSignature) return;
  const existing = body.querySelector(".ctl");
  if (!force && existing && existing.contains(document.activeElement) && document.activeElement.tagName === "INPUT") return;
  controlsSignature = sig;
  const controls = renderControls(u, overridden);
  if (existing) existing.replaceWith(controls);
  else body.append(controls);
}

function range(r) {
  if (!r || !r.end) return el("span.sub", null, "not reported");
  return `${int(r.begin)} – ${int(r.end - 1)}`;
}

function renderFacts(u) {
  const lag = headLag(u);
  const overridden = u.config && (u.config.priority !== u.priority || u.config.weight !== u.weight);
  const rows = [
    ["Status", [STATUS_LABEL[u.status] || u.status, el("div.sub", null, STATUS_TEXT[u.status] || "")]],
    ["Listener", open.listener],
    ["Endpoint", [u.endpoint, u.tls ? el("span.sub", null, "  TLS") : null]],
    [
      "Priority, weight",
      [`${u.priority}, ${u.weight}`, overridden ? el("div.sub", null, `changed at runtime; config has ${u.config.priority}, ${u.config.weight}`) : null],
    ],
    ["Sessions", [`${int(u.sessions)} now`, el("span.sub", null, `  ${int(u.sessions_total)} since start`)]],
    [
      "Head",
      u.chain_head_block === null || u.chain_head_block === undefined
        ? el("span.sub", null, "no status yet")
        : [`#${int(u.chain_head_block)}`, el("span.sub", null, lag === null ? "  last reported chain head" : lag === 0 ? "  no lag observed at last check" : `  at least ${int(lag)} behind at last check`)],
    ],
    ["Trace range", range(u.trace)],
    ["Chain state", range(u.chain_state)],
    ["Finality data", range(u.finality)],
    ["chain_id", u.chain_id ? el("span", { title: u.chain_id }, shortHash(u.chain_id)) : el("span.sub", null, "not reported")],
    ["Failovers away", int(u.failovers_total)],
    ["Unclean drops", int(u.block_failures_total)],
    ["Forwarded", `${int(u.blocks_total)} blocks`],
  ];
  if (u.quarantines.length) {
    rows.push([
      "Quarantined",
      u.quarantines.map((q) =>
        el("div", null, `#${int(q.block)}${q.range > 1 ? ` to #${int(q.block + q.range - 1)}` : ""}`, el("span.sub", null, `  expires in ${countdown(q.expires_in_secs)}`)),
      ),
    ]);
  }
  return el(
    "dl.facts",
    null,
    rows.flatMap(([k, v]) => [el("dt", null, k), el("dd", null, ...(Array.isArray(v) ? v : [v]))]),
  );
}

function renderControls(u, overridden) {
  const wrap = el("div.ctl");
  if (mode === "off") {
    wrap.append(offNote());
    return wrap;
  }
  if (mode === "locked") {
    const unlock = el("button.button.button-primary", { type: "button", onclick: openUnlock }, "Unlock controls");
    wrap.append(
      el(
        "div.ctl-section",
        null,
        el("p", null, "Unlock controls to drain, disable or re-weight this upstream."),
        el("div.sheet-actions", { style: "justify-content:flex-start" }, unlock),
      ),
    );
    return wrap;
  }

  // Routing: priority and weight.
  const prio = el("input", { type: "number", min: "0", step: "1", inputmode: "numeric", value: String(u.priority), id: "ctl-priority" });
  const weight = el("input", { type: "number", min: "1", step: "1", inputmode: "numeric", value: String(u.weight), id: "ctl-weight" });
  const formError = el("p.form-error", { role: "alert", hidden: true });
  const save = el("button.button.button-primary", { type: "submit" }, "Save");
  const form = el(
    "form.ctl-section",
    null,
    el("h3", null, "Routing"),
    el("p", null, "Lower priority is preferred. Weight splits sessions inside a priority tier. Applies to new sessions and failovers."),
    el(
      "div.ctl-row",
      null,
      el("label.field", null, el("span", null, "Priority"), prio),
      el("label.field", null, el("span", null, "Weight"), weight),
      save,
    ),
    formError,
  );
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const p = Number(prio.value);
    const w = Number(weight.value);
    formError.hidden = true;
    prio.removeAttribute("aria-invalid");
    weight.removeAttribute("aria-invalid");
    if (!Number.isInteger(p) || p < 0) {
      prio.setAttribute("aria-invalid", "true");
      formError.hidden = false;
      formError.textContent = "Priority must be a whole number, 0 or more.";
      prio.focus();
      return;
    }
    if (!Number.isInteger(w) || w < 1) {
      weight.setAttribute("aria-invalid", "true");
      formError.hidden = false;
      formError.textContent = "Weight must be a whole number, 1 or more. To stop using this upstream, disable it.";
      weight.focus();
      return;
    }
    const body = {};
    if (p !== u.priority) body.priority = p;
    if (w !== u.weight) body.weight = w;
    if (!Object.keys(body).length) {
      toast("Nothing to save: priority and weight are unchanged.");
      return;
    }
    await run(save, () => api.patchUpstream(open.listener, open.endpoint, body), (fresh) => `Saved priority ${fresh.priority} and weight ${fresh.weight} for ${fresh.name}.`);
  });
  wrap.append(form);

  // Traffic actions.
  const list = el("ul.ctl-list");
  const item = (title, text, button) => list.append(el("li", null, el("div", null, el("strong", null, title), el("p", null, text)), button));

  if (!u.operator_disabled || u.sessions > 0) {
    const drain = el("button.button.button-warn", { type: "button" }, "Drain");
    drain.addEventListener("click", async () => {
      const ok = await confirmAction({
        title: `Drain ${u.name}?`,
        text: `New sessions stop going to ${u.name}, and its ${int(u.sessions)} current session${u.sessions === 1 ? "" : "s"} move to other upstreams. Clients keep their connection and resume at the next block. A session stays here if no other online upstream has its next block, for example a reader of old history when this is the only archive node; the event log shows how many stayed.`,
        ok: "Drain",
      });
      if (ok) await run(drain, () => api.upstreamAction(open.listener, open.endpoint, "drain"), (fresh) => `Draining ${fresh.name}: its sessions are moving to other upstreams.`);
    });
    item("Drain", "Move every current session to other upstreams without disconnecting clients, and stop new ones.", drain);
  }
  if (u.operator_disabled) {
    const enable = el("button.button.button-primary", { type: "button" }, "Enable");
    enable.addEventListener("click", () => run(enable, () => api.upstreamAction(open.listener, open.endpoint, "enable"), (fresh) => `Enabled ${fresh.name}.`));
    item("Enable", "Route new sessions and failovers here again.", enable);
  } else {
    const disable = el("button.button.button-warn", { type: "button" }, "Disable");
    disable.addEventListener("click", async () => {
      const ok = await confirmAction({
        title: `Disable ${u.name}?`,
        text: `New sessions and failovers stop going to ${u.name}. Its current sessions stay until they end or you drain it. The router keeps polling its status.`,
        ok: "Disable",
      });
      if (ok) await run(disable, () => api.upstreamAction(open.listener, open.endpoint, "disable"), (fresh) => `Disabled ${fresh.name}.`);
    });
    item("Disable", "Stop routing new sessions here. Current sessions stay.", disable);
  }
  if (u.quarantines.length) {
    const clear = el("button.button", { type: "button" }, "Clear quarantine");
    clear.addEventListener("click", () => run(clear, () => api.upstreamAction(open.listener, open.endpoint, "clear-quarantine"), (fresh) => `Cleared quarantines on ${fresh.name}.`));
    item("Clear quarantine", "Route requests in the quarantined block ranges here again before the TTL ends.", clear);
  }
  if (overridden || u.operator_disabled) {
    const reset = el("button.button", { type: "button" }, "Reset to config");
    reset.addEventListener("click", () => run(reset, () => api.upstreamAction(open.listener, open.endpoint, "reset"), (fresh) => `Reset ${fresh.name} to its config.`));
    item("Reset to config", `Restore priority ${u.config.priority}, weight ${u.config.weight} and enable it.`, reset);
  }

  wrap.append(
    el("div.ctl-section", null, el("h3", null, "Traffic"), list),
    el("p.sub", { style: "font-size:0.8125rem;color:var(--ink-3);margin-top:1rem" }, "Runtime changes are not written to the config file and are lost when the router restarts."),
  );
  return wrap;
}

async function run(button, call, message) {
  const label = button.textContent;
  button.disabled = true;
  button.setAttribute("aria-busy", "true");
  try {
    const fresh = await call();
    if (fresh && fresh.endpoint) applyUpstream(fresh);
    const text = message(fresh || {});
    toast(text);
    announce(text);
    controlsSignature = "";
    if (open) renderUpstreamDialog(true);
  } catch (err) {
    if (!handleAuthError(err)) toast(`${label} failed: ${err.message}`, "error");
  } finally {
    button.disabled = false;
    button.removeAttribute("aria-busy");
  }
}
