import type { EmailOtpType } from "@supabase/supabase-js";
import { NextResponse, type NextRequest } from "next/server";

import { safeNextPath } from "@/lib/routes";
import { getSupabaseServerClient } from "@/lib/supabase/server";

const allowedTypes = new Set<EmailOtpType>(["email", "signup", "invite", "magiclink", "recovery", "email_change"]);

export async function GET(request: NextRequest) {
  const url = new URL(request.url);
  const tokenHash = url.searchParams.get("token_hash");
  const type = url.searchParams.get("type") as EmailOtpType | null;
  const nextPath = safeNextPath(url.searchParams.get("next"));
  const loginUrl = new URL("/login", url.origin);

  if (!tokenHash || !type || !allowedTypes.has(type)) {
    loginUrl.searchParams.set("error", "invalid_confirmation_link");
    return NextResponse.redirect(loginUrl);
  }
  const supabase = await getSupabaseServerClient();
  if (!supabase) {
    loginUrl.searchParams.set("error", "auth_not_configured");
    return NextResponse.redirect(loginUrl);
  }
  const { error } = await supabase.auth.verifyOtp({ type, token_hash: tokenHash });
  if (error) {
    loginUrl.searchParams.set("error", "confirmation_failed");
    return NextResponse.redirect(loginUrl);
  }
  return NextResponse.redirect(new URL(nextPath, url.origin));
}
