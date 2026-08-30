import assert from "node:assert/strict";
import test from "node:test";

import {
  completeAuthCallback,
  type AuthCallbackClient,
} from "../lib/supabase/auth-callback.ts";

type CallbackMockOptions = {
  exchangeError?: unknown;
  verifyError?: unknown;
  throwOnExchange?: boolean;
  throwOnVerify?: boolean;
};

function callbackClient(options: CallbackMockOptions = {}) {
  const exchanges: Array<[string, { flowId?: string } | undefined]> = [];
  const verifications: unknown[] = [];
  const client = {
    auth: {
      async exchangeCodeForSession(code: string, exchangeOptions?: { flowId?: string }) {
        exchanges.push([code, exchangeOptions]);
        if (options.throwOnExchange) throw new Error("exchange failed");
        return { data: null, error: options.exchangeError ?? null };
      },
      async verifyOtp(parameters: unknown) {
        verifications.push(parameters);
        if (options.throwOnVerify) throw new Error("verification failed");
        return { data: null, error: options.verifyError ?? null };
      },
    },
  } as unknown as AuthCallbackClient;
  return { client, exchanges, verifications };
}

test("callback exchanges a PKCE code with its flow id", async () => {
  const mock = callbackClient();
  const parameters = new URLSearchParams({ code: "auth-code", sb_flow_id: "flow-123" });

  assert.deepEqual(await completeAuthCallback(mock.client, parameters), { ok: true });
  assert.deepEqual(mock.exchanges, [["auth-code", { flowId: "flow-123" }]]);
  assert.deepEqual(mock.verifications, []);
});

test("callback verifies supported token-hash email links", async () => {
  const mock = callbackClient();
  const parameters = new URLSearchParams({ token_hash: "hashed-token", type: "recovery" });

  assert.deepEqual(await completeAuthCallback(mock.client, parameters), { ok: true });
  assert.deepEqual(mock.exchanges, []);
  assert.deepEqual(mock.verifications, [{ token_hash: "hashed-token", type: "recovery" }]);
});

test("callback rejects incomplete and unsupported links before calling Supabase", async () => {
  const mock = callbackClient();

  assert.deepEqual(await completeAuthCallback(mock.client, new URLSearchParams()), {
    ok: false,
    code: "invalid_confirmation_link",
  });
  assert.deepEqual(
    await completeAuthCallback(
      mock.client,
      new URLSearchParams({ token_hash: "hashed-token", type: "unsupported" }),
    ),
    { ok: false, code: "invalid_confirmation_link" },
  );
  assert.deepEqual(mock.exchanges, []);
  assert.deepEqual(mock.verifications, []);
});

test("callback converts provider errors and thrown failures to a stable code", async () => {
  const providerFailure = callbackClient({ exchangeError: new Error("provider details") });
  const thrownFailure = callbackClient({ throwOnVerify: true });

  assert.deepEqual(
    await completeAuthCallback(providerFailure.client, new URLSearchParams({ code: "bad-code" })),
    { ok: false, code: "confirmation_failed" },
  );
  assert.deepEqual(
    await completeAuthCallback(
      thrownFailure.client,
      new URLSearchParams({ token_hash: "bad-hash", type: "signup" }),
    ),
    { ok: false, code: "confirmation_failed" },
  );
});
