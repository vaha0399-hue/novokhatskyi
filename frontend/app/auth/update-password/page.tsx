import { redirect } from "next/navigation";

import { UpdatePasswordForm } from "@/components/auth-forms";
import { FAAuthFrame } from "@/components/fa-auth-ui";
import { getCurrentIdentity } from "@/lib/supabase/server";

export const dynamic = "force-dynamic";

export default async function UpdatePasswordPage() {
  if (!await getCurrentIdentity()) {
    redirect("/forgot-password?error=recovery_session_required");
  }
  return <FAAuthFrame view="update"><UpdatePasswordForm /></FAAuthFrame>;
}
