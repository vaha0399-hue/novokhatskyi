import Link from "next/link";

import { AuthShell } from "@/components/auth-shell";
import { SignUpForm } from "@/components/auth-forms";
import { safeNextPath } from "@/lib/routes";

export default async function RegisterPage({ searchParams }: { searchParams: Promise<{ next?: string }> }) {
  const nextPath = safeNextPath((await searchParams).next);
  return <AuthShell eyebrow="Create your account" title="A clearer way to follow football." description="Register for secure access through Supabase Auth. No football data or passwords are stored in this frontend."><SignUpForm nextPath={nextPath} /><p className="auth-switch">Already a member? <Link href={`/login?next=${encodeURIComponent(nextPath)}`}>Sign in</Link></p></AuthShell>;
}
