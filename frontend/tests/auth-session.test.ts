import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

async function source(relativePath: string): Promise<string> {
  return readFile(new URL(relativePath, import.meta.url), "utf8");
}

test("SSR proxy refreshes verified claims and forwards cookies plus anti-cache headers", async () => {
  const proxySource = await source("../lib/supabase/proxy.ts");

  assert.match(proxySource, /supabase\.auth\.getClaims\(\)/);
  assert.doesNotMatch(proxySource, /supabase\.auth\.getSession\(\)/);
  assert.match(proxySource, /setAll\(values, headers\)/);
  assert.match(proxySource, /Object\.entries\(headers\)/);
  assert.match(proxySource, /response\.cookies\.getAll\(\)/);
  assert.match(proxySource, /redirectResponse\.cookies\.set\(cookie\)/);
});

test("protected pages repeat the server-side identity check", async () => {
  const accountSource = await source("../app/account/page.tsx");
  const updatePasswordSource = await source("../app/auth/update-password/page.tsx");
  const serverSource = await source("../lib/supabase/server.ts");

  assert.match(serverSource, /client\.auth\.getClaims\(\)/);
  assert.match(accountSource, /await getCurrentIdentity\(\)/);
  assert.match(accountSource, /redirect\("\/login\?next=\/account"\)/);
  assert.match(updatePasswordSource, /await getCurrentIdentity\(\)/);
  assert.match(updatePasswordSource, /recovery_session_required/);
});

test("auth callback redirects are explicitly private and non-cacheable", async () => {
  const callbackSource = await source("../app/auth/confirm/route.ts");

  assert.match(callbackSource, /completeAuthCallback\(supabase, url\.searchParams\)/);
  assert.match(callbackSource, /private, no-cache, no-store/);
  assert.match(callbackSource, /safeNextPath\(url\.searchParams\.get\("next"\)\)/);
});

test("local Supabase redirects allow query-bearing callbacks on both dev hosts", async () => {
  const configSource = await source("../../supabase/config.toml");

  assert.match(configSource, /"http:\/\/localhost:3000\/\*\*"/);
  assert.match(configSource, /"http:\/\/127\.0\.0\.1:3000\/\*\*"/);
  assert.doesNotMatch(configSource, /"https:\/\/127\.0\.0\.1:3000"/);
});
