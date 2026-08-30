import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

async function authFormsSource(): Promise<string> {
  return readFile(new URL("../components/auth-forms.tsx", import.meta.url), "utf8");
}

function exportedSection(source: string, name: string, nextName?: string): string {
  const start = source.indexOf(`export function ${name}`);
  assert.notEqual(start, -1, `${name} must exist`);
  const end = nextName ? source.indexOf(`export function ${nextName}`, start) : source.length;
  assert.notEqual(end, -1, `${nextName} must exist after ${name}`);
  return source.slice(start, end);
}

test("signup retains its form reference before awaiting Supabase", async () => {
  const source = await authFormsSource();

  assert.doesNotMatch(source, /event\.currentTarget\.reset\(\)/);
  assert.match(source, /const form = event\.currentTarget;/);
  assert.match(source, /form\.reset\(\)/);
});

test("each auth form calls only its dedicated Supabase operation", async () => {
  const source = await authFormsSource();
  const signIn = exportedSection(source, "SignInForm", "SignUpForm");
  const signUp = exportedSection(source, "SignUpForm", "ForgotPasswordForm");
  const forgot = exportedSection(source, "ForgotPasswordForm", "UpdatePasswordForm");
  const update = exportedSection(source, "UpdatePasswordForm", "LogoutButton");
  const logout = exportedSection(source, "LogoutButton");

  assert.match(signIn, /supabase\.auth\.signInWithPassword\(/);
  assert.doesNotMatch(signIn, /supabase\.auth\.(?:signUp|resetPasswordForEmail|updateUser|signOut)\(/);

  assert.match(signUp, /supabase\.auth\.signUp\(/);
  assert.match(signUp, /signUpErrorMessage\(authData, error\)/);
  assert.doesNotMatch(signUp, /supabase\.auth\.(?:signInWithPassword|resetPasswordForEmail|updateUser|signOut)\(/);

  assert.match(forgot, /supabase\.auth\.resetPasswordForEmail\(/);
  assert.doesNotMatch(forgot, /supabase\.auth\.(?:signInWithPassword|signUp|updateUser|signOut)\(/);

  assert.match(update, /supabase\.auth\.updateUser\(\{ password \}\)/);
  assert.doesNotMatch(update, /supabase\.auth\.(?:signInWithPassword|signUp|resetPasswordForEmail|signOut)\(/);

  assert.match(logout, /supabase\.auth\.signOut\(\)/);
  assert.doesNotMatch(logout, /supabase\.auth\.(?:signInWithPassword|signUp|resetPasswordForEmail|updateUser)\(/);
});

test("password recovery routes the email link through the server callback", async () => {
  const source = await authFormsSource();
  const forgot = exportedSection(source, "ForgotPasswordForm", "UpdatePasswordForm");

  assert.match(forgot, /new URL\("\/auth\/confirm", window\.location\.origin\)/);
  assert.match(forgot, /callbackUrl\.searchParams\.set\("next", "\/auth\/update-password"\)/);
  assert.match(forgot, /redirectTo: callbackUrl\.toString\(\)/);
});

test("sign out redirects only after Supabase accepts the request", async () => {
  const source = await authFormsSource();
  const logout = exportedSection(source, "LogoutButton");
  const signOutCall = logout.indexOf("await supabase.auth.signOut()");
  const errorGuard = logout.indexOf("if (error) return setMessage(error.message)");
  const redirect = logout.indexOf('router.replace("/")');

  assert.ok(signOutCall >= 0);
  assert.ok(errorGuard > signOutCall);
  assert.ok(redirect > errorGuard);
});
