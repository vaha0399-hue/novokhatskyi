import assert from "node:assert/strict";
import test from "node:test";

import type { AuthResponse } from "@supabase/supabase-js";

import {
  authPageMessage,
  existingAccountMessage,
  signUpErrorMessage,
} from "../lib/supabase/auth-messages.ts";

const emptyData = { user: null, session: null } as AuthResponse["data"];

function authError(message: string, code?: string): AuthResponse["error"] {
  return { message, code } as AuthResponse["error"];
}

test("auth page messages expose only sanitized callback failures", () => {
  assert.equal(authPageMessage(null), null);
  assert.equal(
    authPageMessage("invalid_confirmation_link"),
    "This authentication link is invalid or incomplete. Request a new link.",
  );
  assert.equal(
    authPageMessage("provider-secret-message"),
    "Authentication could not be completed. Please try again.",
  );
});

test("signup reports an existing account from stable Supabase duplicate errors", () => {
  assert.equal(
    signUpErrorMessage(emptyData, authError("User already registered", "user_already_exists")),
    existingAccountMessage,
  );
  assert.equal(
    signUpErrorMessage(emptyData, authError("User already registered")),
    existingAccountMessage,
  );
  assert.equal(
    signUpErrorMessage(emptyData, authError("Email address is already in use", "email_exists")),
    existingAccountMessage,
  );
});

test("signup recognizes the obfuscated existing-user response", () => {
  const data = {
    user: { identities: [] },
    session: null,
  } as unknown as AuthResponse["data"];

  assert.equal(signUpErrorMessage(data, null), existingAccountMessage);
});

test("signup does not misclassify a newly created user", () => {
  const awaitingConfirmation = {
    user: { identities: [{ provider: "email" }] },
    session: null,
  } as unknown as AuthResponse["data"];
  const autoConfirmed = {
    user: { identities: [{ provider: "email" }] },
    session: {},
  } as unknown as AuthResponse["data"];

  assert.equal(signUpErrorMessage(awaitingConfirmation, null), null);
  assert.equal(signUpErrorMessage(autoConfirmed, null), null);
});

test("signup preserves unrelated provider errors", () => {
  assert.equal(
    signUpErrorMessage(emptyData, authError("Password should contain a symbol", "weak_password")),
    "Password should contain a symbol",
  );
});
