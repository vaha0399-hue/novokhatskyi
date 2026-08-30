import type { AuthResponse } from "@supabase/supabase-js";

const messages: Record<string, string> = {
  auth_not_configured: "Supabase Auth is not configured in this environment yet.",
  confirmation_failed: "This authentication link has expired or was already used. Request a new link.",
  invalid_confirmation_link: "This authentication link is invalid or incomplete. Request a new link.",
  recovery_session_required: "Open the latest password-reset link from your email before choosing a new password.",
};

export const existingAccountMessage =
  "An account with this email already exists. Sign in or reset your password.";

const duplicateSignUpErrorCodes = new Set(["email_exists", "user_already_exists"]);
const duplicateSignUpMessage =
  /(?:already (?:registered|exists|in use)|email(?: address)?(?: is)? (?:already )?(?:registered|in use|taken|exists))/i;

export function authPageMessage(code: string | null | undefined): string | null {
  return code ? messages[code] ?? "Authentication could not be completed. Please try again." : null;
}

export function signUpErrorMessage(
  data: AuthResponse["data"],
  error: AuthResponse["error"],
): string | null {
  if (
    (error && (duplicateSignUpErrorCodes.has(error.code ?? "") || duplicateSignUpMessage.test(error.message)))
    || (!data.session && data.user?.identities?.length === 0)
  ) {
    return existingAccountMessage;
  }
  return error?.message ?? null;
}
