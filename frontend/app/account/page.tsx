import Link from "next/link";
import { redirect } from "next/navigation";

import { LogoutButton } from "@/components/auth-forms";
import { getCurrentIdentity } from "@/lib/supabase/server";

export const dynamic = "force-dynamic";

export default async function AccountPage() {
  const identity = await getCurrentIdentity();
  if (!identity) redirect("/login?next=/account");

  return <section className="shell section account-page"><p className="eyebrow"><span /> Account</p><div className="account-hero"><div><h1>Welcome{identity.email ? `, ${identity.email}` : ""}.</h1><p>Your identity and session are securely managed by Supabase Auth.</p></div><LogoutButton /></div><div className="account-grid"><article><span>Access</span><strong>Free account</strong><p>Account foundation is active. Subscription and payment are intentionally not implemented yet.</p></article><article><span>Data boundary</span><strong>Private session</strong><p>Your account response is never shared through public historical-data caches.</p></article><article><span>Explore</span><Link href="/">Open competition library ↗</Link><p>Discover completed historical seasons, matches and factual comparison analytics.</p></article></div></section>;
}
