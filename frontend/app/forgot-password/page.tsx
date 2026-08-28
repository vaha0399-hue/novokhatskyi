import Link from "next/link";

import { AuthShell } from "@/components/auth-shell";
import { ForgotPasswordForm } from "@/components/auth-forms";

export default function ForgotPasswordPage() {
  return <AuthShell eyebrow="Account recovery" title="Reset your password." description="We will send a secure recovery link through Supabase Auth."><ForgotPasswordForm /><p className="auth-switch"><Link href="/login">Back to sign in</Link></p></AuthShell>;
}
