import { redirect } from "next/navigation";

import { AuthShell } from "@/components/auth-shell";
import { UpdatePasswordForm } from "@/components/auth-forms";
import { getCurrentIdentity } from "@/lib/supabase/server";

export const dynamic = "force-dynamic";

export default async function UpdatePasswordPage() {
  if (!await getCurrentIdentity()) {
    redirect("/forgot-password?error=recovery_session_required");
  }
  return <AuthShell eyebrow="Account recovery" title="Choose a new password." description="This page requires a valid Supabase Auth session, normally established by your recovery link."><UpdatePasswordForm /></AuthShell>;
}
