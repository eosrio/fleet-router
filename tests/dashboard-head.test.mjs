import assert from "node:assert/strict";
import { test } from "node:test";
import { headLag } from "../ui/js/store.js";
import { trackMarkup } from "../ui/js/track.js";

test("synchronized nodes use their own poll comparison despite different raw heads", () => {
  for (const head_block of [1000, 1008]) {
    assert.equal(headLag({ online: true, head_block, head_lag_blocks: 0 }), 0);
  }
});

function tipPosition(markup) {
  return markup.match(/class="cap"[^>]*x1="([^"]+)"/)?.[1] ?? null;
}

test("the head marker agrees with the lag label when status polls are out of phase", () => {
  const axis = { min: 2, best: 340_227_487 };
  for (const offset of [0, 1, 2, 3, 7]) {
    const upstream = { online: true, head_lag_blocks: 0, head_block: axis.best - offset,
      trace: { begin: 1, end: axis.best - offset + 1 } };
    assert.equal(headLag(upstream), 0);
    assert.equal(tipPosition(trackMarkup(400, axis, upstream, "up")), "398.0",
      `graph falsely shows ${offset} blocks of lag while the label says at head`);
  }
});

test("a measured lag stays visible even when the last stored range reaches the axis end", () => {
  const axis = { min: 1, best: 1000 };
  const upstream = { online: true, head_lag_blocks: 30, trace: { begin: 1, end: 1001 } };
  assert.equal(tipPosition(trackMarkup(400, axis, upstream, "up")), "336.0");
});

test("unavailable or offline lag does not acquire a marker from an old range", () => {
  const axis = { min: 1, best: 1000 };
  for (const upstream of [
    { online: true, head_lag_blocks: null, trace: { begin: 1, end: 1001 } },
    { online: false, head_lag_blocks: 0, trace: { begin: 1, end: 1001 } },
  ]) assert.equal(tipPosition(trackMarkup(400, axis, upstream, "down")), null);
});

test("real measured lag stays visible; unavailable readings stay unavailable", () => {
  assert.equal(headLag({ online: true, head_lag_blocks: 200 }), 200);
  for (const upstream of [
    { online: false, head_lag_blocks: 200 },
    { online: true, head_lag_blocks: null },
    { online: true },
    { online: true, head_lag_blocks: -1 },
    { online: true, head_lag_blocks: NaN },
  ]) assert.equal(headLag(upstream), null);
});
