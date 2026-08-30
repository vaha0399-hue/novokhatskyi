import Link from "next/link";

import { AuthShell } from "@/components/auth-shell";
import { SignInForm } from "@/components/auth-forms";
import { safeNextPath } from "@/lib/routes";
import { authPageMessage } from "@/lib/supabase/auth-messages";

export default async function LoginPage({ searchParams }: { searchParams: Promise<{ next?: string; error?: string }> }) {
  const parameters = await searchParams;
  const nextPath = safeNextPath(parameters.next);
  return <AuthShell eyebrow="Member access" title="Welcome back." description="Sign in to access your account and future personalised analytical tools."><SignInForm nextPath={nextPath} initialMessage={authPageMessage(parameters.error)} /><p className="auth-switch">New to Football Analytics? <Link href={`/register?next=${encodeURIComponent(nextPath)}`}>Create an account</Link></p></AuthShell>;
}
