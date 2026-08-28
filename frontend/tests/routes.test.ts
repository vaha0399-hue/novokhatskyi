import assert from "node:assert/strict";
import test from "node:test";

import { safeNextPath } from "../lib/routes.ts";

test("safeNextPath accepts local destinations only", () => {
  assert.equal(safeNextPath("/account?tab=settings"), "/account?tab=settings");
  assert.equal(safeNextPath("/fixtures/42"), "/fixtures/42");
});

test("safeNextPath rejects external and control-character redirect attempts", () => {
  assert.equal(safeNextPath("//evil.example"), "/account");
  assert.equal(safeNextPath("/\\evil.example"), "/account");
  assert.equal(safeNextPath("/account\r\nLocation: https://evil.example"), "/account");
});
