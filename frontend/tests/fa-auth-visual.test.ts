import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

async function source(relativePath: string): Promise<string> {
  return readFile(new URL(relativePath, import.meta.url), "utf8");
}

test("FA auth preserves the approved analytical backdrop and branding", async () => {
  const ui = await source("../components/fa-auth-ui.tsx");

  assert.match(ui, /FA Sports Intelligence/);
  assert.match(ui, /MODEL ONLINE/);
  assert.match(ui, /PREMIER LEAGUE · ROUND 5/);
  assert.match(ui, /WIN PROBABILITY/);
  assert.match(ui, /EXPECTED GOALS \(xG\)/);
  assert.match(ui, /MODEL CONFIDENCE/);
  assert.match(ui, /HEAD TO HEAD · LAST 5/);
  assert.match(ui, /RECENT FORM/);
  assert.equal((ui.match(/<article className="fa-data-card/g) ?? []).length, 6);
  assert.match(ui, /className="fa-analytics-backdrop" aria-hidden="true"/);
  assert.doesNotMatch(ui, /PitchIQ|stadium photo|background-image:\s*url/i);
});

test("FA auth keeps all approved interactive and success states", async () => {
  const experience = await source("../components/fa-auth-experience.tsx");
  const forms = await source("../components/auth-forms.tsx");
  const ui = await source("../components/fa-auth-ui.tsx");

  assert.match(experience, /"signin" \| "signup" \| "forgot" \| "success"/);
  assert.match(experience, /complete\("signin"\)/);
  assert.match(experience, /complete\("signup"\)/);
  assert.match(experience, /complete\("reset"\)/);
  assert.match(forms, /SECURE MODEL ACCESS/);
  assert.match(forms, /CREATE YOUR WORKSPACE/);
  assert.match(forms, /ACCOUNT RECOVERY/);
  assert.match(ui, /ACCESS GRANTED/);
  assert.match(ui, /ACCOUNT CREATED/);
  assert.match(ui, /CHECK YOUR INBOX/);
  assert.match(forms, /aria-label=\{showPassword \? "Hide password" : "Show password"\}/);
  assert.match(forms, /name="terms" type="checkbox"/);
  assert.match(forms, /data: \{ full_name:/);
});

test("FA auth CSS retains the approved geometry, glow, and responsive rules", async () => {
  const css = await source("../app/auth.css");

  assert.match(css, /width: min\(100%, 500px\)/);
  assert.match(css, /border-radius: 23px/);
  assert.match(css, /rgba\(75,153,255,.95\).*rgba\(255,61,98,.9\)/);
  assert.match(css, /min-height: 650px/);
  assert.match(css, /min-height: 720px/);
  assert.match(css, /height: 54px/);
  assert.match(css, /height: 56px/);
  assert.match(css, /background-size: 44px 44px/);
  assert.match(css, /@media \(max-width: 980px\)/);
  assert.match(css, /@media \(max-width: 650px\)/);
  assert.match(css, /@media \(max-height: 790px\) and \(min-width: 651px\)/);
  assert.match(css, /@media \(prefers-reduced-motion: reduce\)/);
});

test("all auth entry routes use the shared FA visual experience", async () => {
  const login = await source("../app/login/page.tsx");
  const register = await source("../app/register/page.tsx");
  const forgot = await source("../app/forgot-password/page.tsx");
  const update = await source("../app/auth/update-password/page.tsx");

  assert.match(login, /FAAuthExperience initialView="signin"/);
  assert.match(register, /FAAuthExperience initialView="signup"/);
  assert.match(forgot, /FAAuthExperience initialView="forgot"/);
  assert.match(update, /FAAuthFrame view="update"/);
});
