import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("signup retains its form reference before awaiting Supabase", async () => {
  const source = await readFile(new URL("../components/auth-forms.tsx", import.meta.url), "utf8");

  assert.doesNotMatch(source, /event\.currentTarget\.reset\(\)/);
  assert.match(source, /const form = event\.currentTarget;/);
  assert.match(source, /form\.reset\(\)/);
});
