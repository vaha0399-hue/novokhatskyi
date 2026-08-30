import type { EmailOtpType, SupabaseClient } from "@supabase/supabase-js";

const allowedEmailOtpTypes = new Set<string>([
  "email",
  "signup",
  "invite",
  "magiclink",
  "recovery",
  "email_change",
]);

export type AuthCallbackClient = {
  auth: Pick<SupabaseClient["auth"], "exchangeCodeForSession" | "verifyOtp">;
};

export type AuthCallbackResult =
  | { ok: true }
  | { ok: false; code: "invalid_confirmation_link" | "confirmation_failed" };

function allowedEmailOtpType(value: string | null): value is EmailOtpType {
  return value !== null && allowedEmailOtpTypes.has(value);
}

export async function completeAuthCallback(
  client: AuthCallbackClient,
  searchParams: URLSearchParams,
): Promise<AuthCallbackResult> {
  try {
    const authCode = searchParams.get("code");
    if (authCode) {
      const flowId = searchParams.get("sb_flow_id");
      const { error } = await client.auth.exchangeCodeForSession(
        authCode,
        flowId ? { flowId } : undefined,
      );
      return error ? { ok: false, code: "confirmation_failed" } : { ok: true };
    }

    const tokenHash = searchParams.get("token_hash");
    const type = searchParams.get("type");
    if (!tokenHash || !allowedEmailOtpType(type)) {
      return { ok: false, code: "invalid_confirmation_link" };
    }

    const { error } = await client.auth.verifyOtp({
      type,
      token_hash: tokenHash,
    });
    return error ? { ok: false, code: "confirmation_failed" } : { ok: true };
  } catch {
    return { ok: false, code: "confirmation_failed" };
  }
}
