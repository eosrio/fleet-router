// Configuration is loaded on operator actions. Revisions and pending drains
// arrive in the shared dashboard stream; this editor has no polling loop.
import { api } from "./api.js";
import { confirmAction, controlMode, toast } from "./controls.js";
import { el, setText } from "./util.js";

let loaded = null;
let original = "";
let active = false;
let busy = false;
let revision = 0;
const node = (id) => document.getElementById(`config-${id}`);
const dirty = () => Boolean(loaded && node("source").value !== original);

function feedback(message, error = false) {
  node("feedback").textContent = message;
  node("feedback").dataset.state = error ? "error" : "ok";
  node("source").setAttribute("aria-invalid", String(error));
}

function sync() {
  const unlocked = controlMode() === "unlocked";
  node("locked").hidden = unlocked;
  node("editor").hidden = !unlocked;
  node("load").disabled = busy;
  node("reload").disabled = busy;
  node("validate").disabled = busy || !loaded;
  node("save").disabled = busy || !loaded || !dirty();
  node("source").disabled = busy || !loaded;
  node("editor").setAttribute("aria-busy", String(busy));
  setText(node("draft"), busy ? "Working…" : dirty() ? "Unsaved edits" : loaded ? "File loaded" : "");
  const stale = loaded && revision > loaded.revision;
  node("stale").hidden = !stale;
}

function acceptDocument(doc) {
  loaded = doc;
  original = doc.source;
  node("source").value = original;
  setText(node("path"), doc.path);
  feedback(doc.disk_error || (doc.disk_matches_running ? "The file matches the running configuration." : "The file differs from the running configuration. Reload file to apply it."), Boolean(doc.disk_error));
  sync();
}

async function discardDraft() {
  return !dirty() || confirmAction({ title: "Discard unsaved edits?", text: "Your draft will be replaced by the configuration file.", ok: "Discard edits" });
}

async function run(action) {
  if (busy || controlMode() !== "unlocked") return;
  busy = true; sync();
  try { await action(); }
  catch (error) { feedback(error.message, true); }
  finally { busy = false; sync(); }
}

function request(source) {
  return { expected_revision: loaded.revision, expected_disk_revision: loaded.disk_revision, ...(source === undefined ? {} : { source }) };
}

function summary(result) {
  const changed = result.replaced_listeners;
  return `${result.listeners} listener(s), ${result.upstreams} upstream(s).${changed.length ? ` Existing streams on ${changed.join(", ")} keep their current configuration until they disconnect.` : " Existing listener configurations are unchanged."}${result.warnings.length ? ` ${result.warnings.join(" ")}` : ""}`;
}

async function loadFile() {
  if (!(await discardDraft())) return;
  await run(async () => acceptDocument(await api.config()));
}

async function apply(save) {
  if (!save && !(await discardDraft())) return;
  await run(async () => {
    // Reload always takes a fresh file version. The server checks it again
    // after resource preparation, so intervening file edits are refused.
    if (!save) acceptDocument(await api.config());
    const source = save ? node("source").value : undefined;
    if (save) JSON.parse(source);
    const body = request(source);
    const preview = await api.validateConfig(body);
    feedback(summary(preview));
    if (!(await confirmAction({ title: save ? "Save and apply configuration?" : "Reload configuration file?", text: summary(preview), ok: save ? "Save and apply" : "Reload file" }))) return;
    const result = save ? await api.saveConfig(body) : await api.reloadConfig(body);
    loaded = { ...loaded, revision: result.revision, disk_revision: result.disk_revision, source: result.source };
    original = result.source;
    node("source").value = original;
    feedback(`Applied revision ${result.revision}.${result.access_changed ? " Dashboard access settings changed. Use the new address or unlock controls with the new token if needed." : ""}`);
    toast(`Configuration ${result.revision} applied.`);
  });
}

export function initConfig() {
  node("unlock").addEventListener("click", () => document.getElementById("control-button").click());
  node("load").addEventListener("click", loadFile);
  node("reload").addEventListener("click", () => apply(false));
  node("save").addEventListener("click", () => apply(true));
  node("source").addEventListener("input", () => { feedback("Draft changed. Validate before applying."); sync(); });
  node("validate").addEventListener("click", () => run(async () => {
    JSON.parse(node("source").value);
    feedback(`Valid. ${summary(await api.validateConfig(request(node("source").value)))}`);
  }));
  window.addEventListener("beforeunload", (event) => { if (dirty()) { event.preventDefault(); event.returnValue = ""; } });
  sync();
}

export function setConfigActive(value) {
  active = value;
  if (active && !loaded && !busy && controlMode() === "unlocked") run(async () => acceptDocument(await api.config()));
}

export function syncConfigMode() {
  if (controlMode() !== "unlocked") {
    loaded = null; original = ""; node("source").value = ""; setText(node("path"), "");
  }
  sync();
  setConfigActive(active);
}

export function renderConfig(status) {
  revision = status.config?.revision ?? 0;
  setText(node("revision"), revision ? `Running revision ${revision}` : "");
  const drains = status.config?.draining ?? [];
  node("drains").replaceChildren(...drains.map((drain) => el("li", null,
    el("code", null, drain.listener), ` · configuration ${drain.generation} · ${drain.sessions} SHiP stream(s), ${drain.chain_api_connections ?? 0} HTTP connection(s) draining`)));
  node("draining").hidden = !drains.length;
  sync();
}
