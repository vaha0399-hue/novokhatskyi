import Link from "next/link";

import { AuthShell } from "@/components/auth-shell";
import { SignInForm } from "@/components/auth-forms";
import { safeNextPath } from "@/lib/routes";

export default async function LoginPage({ searchParams }: { searchParams: Promise<{ next?: string }> }) {
  const nextPath = safeNextPath((await searchParams).next);
  return <AuthShell eyebrow="Member access" title="Welcome back." description="Sign in to access your account and future personalised analytical tools."><SignInForm nextPath={nextPath} /><p className="auth-switch">New to Football Analytics? <Link href={`/register?next=${encodeURIComponent(nextPath)}`}>Create an account</Link></p></AuthShell>;
}
