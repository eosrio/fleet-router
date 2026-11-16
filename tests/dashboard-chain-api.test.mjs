import assert from "node:assert/strict";
import { beforeEach, test } from "node:test";
import { beginChainApiPass, ChainApiSection, endChainApiPass } from "../ui/js/chainapi.js";

// Minimal DOM for exercising the actual renderer without a browser dependency.
class TestNode {
  constructor(text = "") {
    this.children = [];
    this.value = text;
    this.style = {};
    this.dataset = {};
    this.classList = { add: (name) => { this.className += ` ${name}`; } };
  }
  get textContent() { return this.value + this.children.map((n) => n.textContent).join(""); }
  set textContent(value) { this.value = value; this.replaceChildren(); }
  setAttribute(name, value) { this[name] = value; }
  append(...nodes) { for (const node of nodes) { node.parent = this; this.children.push(node); } }
  replaceChildren(...nodes) { this.children.forEach((node) => { node.parent = null; }); this.children = []; this.append(...nodes); }
  insertBefore(node, before) {
    node.remove();
    const index = before ? this.children.indexOf(before) : this.children.length;
    this.children.splice(index, 0, node);
    node.parent = this;
  }
  remove() { if (this.parent) { this.parent.children.splice(this.parent.children.indexOf(this), 1); this.parent = null; } }
}
globalThis.Node = TestNode;
globalThis.document = { createElement: () => new TestNode(), createTextNode: (text) => new TestNode(text) };

let section;
const backend = { name: "archive", endpoint: "127.0.0.1:8888", ship_endpoint: "127.0.0.1:8080", role: "frozen", status: "up", usable: false, verified: false, requests_total: 100, errors_total: 10 };
function render(now, requests = 100, errors = 10, instance = 1, reset = false) {
  beginChainApiPass(instance, reset);
  section.update({ listen: "127.0.0.1:18888", listening: true, ready: false,
    readiness_reasons: ["no verified frozen chain API upstream up"], usable_upstreams: 0, upstreams_total: 1,
    upstreams: [{ ...backend, requests_total: requests, errors_total: errors }] }, now);
  endChainApiPass();
  return [...section.rows.values()][0];
}
beforeEach(() => {
  beginChainApiPass();
  endChainApiPass();
  section = new ChainApiSection("test");
});

test("archive probe-up does not imply listener readiness or usability", () => {
  const row = render(1000);
  assert.match(section.meta.textContent, /not ready/);
  assert.match(section.meta.textContent, /0\/1 usable/);
  assert.match(section.meta.textContent, /no verified frozen chain API upstream up/);
  assert.match(row.tags.textContent, /unverified/);
  assert.notEqual(row.lamp.dataset.status, "up", "an unusable archive must not show a green lamp");
});

test("mismatch, disabled and busy states remain visible without duplicate tags", () => {
  for (const state of [
    { status: "down", chain_mismatch: true, verified: null, expected: "chain mismatch" },
    { status: "disabled", operator_disabled: true, verified: null, expected: "disabled" },
    { status: "up", usable: true, busy: true, verified: true, expected: "busy" },
  ]) {
    section.update({ listening: true, ready: false, usable_upstreams: 0, upstreams_total: 1,
      upstreams: [{ ...backend, ...state }] }, 1000);
    const row = [...section.rows.values()][0];
    assert.equal(row.tags.textContent.split(state.expected).length - 1, 1);
  }
});

test("a long stream gap discards stale request and error rates", () => {
  render(1000);
  let row = render(2000, 110, 11);
  assert.equal(row.requestRate.textContent, "10 req/s");
  row = render(62000, 110, 11);
  assert.equal(row.requestRate.textContent, "– req/s");
  row = render(63000, 110, 11);
  assert.equal(row.requestRate.textContent, "0 req/s");
});

test("a router restart resets rates even when new counters exceed the old ones", () => {
  render(1000);
  render(2000, 110, 11);
  const row = render(3000, 200, 20, 2);
  assert.equal(row.requestRate.textContent, "– req/s");
});

test("a stream bootstrap resets rates for the same router instance", () => {
  render(1000);
  render(2000, 110, 11);
  const row = render(3000, 120, 12, 1, true);
  assert.equal(row.requestRate.textContent, "– req/s");
});

test("either counter rollback invalidates the complete rate window", () => {
  render(1000);
  render(2000, 110, 11);
  const row = render(3000, 120, 1);
  assert.equal(row.requestRate.textContent, "– req/s");
});

test("duplicate render timestamps do not consume a counter delta", () => {
  render(1000);
  render(1000, 105, 10);
  const row = render(2000, 110, 11);
  assert.equal(row.requestRate.textContent, "10 req/s");
});

test("removing and restoring an API does not reuse old rate samples", () => {
  render(1000);
  render(2000, 110, 11);
  beginChainApiPass(1);
  section.update(null, 3000);
  endChainApiPass();
  assert.equal(section.node.hidden, true);
  assert.equal(render(4000, 200, 20).requestRate.textContent, "– req/s");
});
