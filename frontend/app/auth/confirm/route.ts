import { NextResponse, type NextRequest } from "next/server";

import { safeNextPath } from "@/lib/routes";
import { completeAuthCallback } from "@/lib/supabase/auth-callback";
import { getSupabaseServerClient } from "@/lib/supabase/server";

function redirectWithoutCaching(url: URL): NextResponse {
  const response = NextResponse.redirect(url);
  response.headers.set("Cache-Control", "private, no-cache, no-store, must-revalidate, max-age=0");
  response.headers.set("Expires", "0");
  response.headers.set("Pragma", "no-cache");
  return response;
}

export async function GET(request: NextRequest) {
  const url = new URL(request.url);
  const nextPath = safeNextPath(url.searchParams.get("next"));
  const loginUrl = new URL("/login", url.origin);
  const supabase = await getSupabaseServerClient();
  if (!supabase) {
    loginUrl.searchParams.set("error", "auth_not_configured");
    loginUrl.searchParams.set("next", nextPath);
    return redirectWithoutCaching(loginUrl);
  }
  const result = await completeAuthCallback(supabase, url.searchParams);
  if (!result.ok) {
    loginUrl.searchParams.set("error", result.code);
    loginUrl.searchParams.set("next", nextPath);
    return redirectWithoutCaching(loginUrl);
  }
  return redirectWithoutCaching(new URL(nextPath, url.origin));
}
