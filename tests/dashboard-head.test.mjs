import assert from "node:assert/strict";
import { test } from "node:test";
import { headLag } from "../ui/js/store.js";

test("synchronized nodes use their own poll comparison despite different raw heads", () => {
  for (const head_block of [1000, 1008]) {
    assert.equal(headLag({ online: true, head_block, head_lag_blocks: 0 }), 0);
  }
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
