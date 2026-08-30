import Link from "next/link";

import { AuthShell } from "@/components/auth-shell";
import { ForgotPasswordForm } from "@/components/auth-forms";
import { authPageMessage } from "@/lib/supabase/auth-messages";

export default async function ForgotPasswordPage({ searchParams }: { searchParams: Promise<{ error?: string }> }) {
  const message = authPageMessage((await searchParams).error);
  return <AuthShell eyebrow="Account recovery" title="Reset your password." description="We will send a secure recovery link through Supabase Auth."><ForgotPasswordForm initialMessage={message} /><p className="auth-switch"><Link href="/login">Back to sign in</Link></p></AuthShell>;
}
