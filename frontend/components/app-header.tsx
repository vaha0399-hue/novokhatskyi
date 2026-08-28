"use client";

import Link from "next/link";
import { useEffect, useState } from "react";

import { Brand } from "@/components/brand";
import { getSupabaseBrowserClient } from "@/lib/supabase/client";

type Identity = { email: string | null } | null;

export function AppHeader() {
  const [identity, setIdentity] = useState<Identity>(null);

  useEffect(() => {
    const supabase = getSupabaseBrowserClient();
    if (!supabase) return;
    let active = true;
    void supabase.auth.getClaims().then(({ data }) => {
      const claims = data?.claims;
      if (!active || !claims?.sub) return;
      setIdentity({ email: typeof claims.email === "string" ? claims.email : null });
    });
    const { data: listener } = supabase.auth.onAuthStateChange((_event, session) => {
      if (!active) return;
      setIdentity(session?.user ? { email: session.user.email ?? null } : null);
    });
    return () => {
      active = false;
      listener.subscription.unsubscribe();
    };
  }, []);

  return (
    <header className="site-header">
      <div className="shell nav-shell">
        <Brand />
        <nav className="main-nav" aria-label="Primary navigation">
          <Link href="/">Discover</Link>
          <a href="#methodology">Methodology</a>
        </nav>
        <div className="nav-account">
          {identity ? (
            <Link className="account-link" href="/account">
              <span className="account-dot" aria-hidden="true" />
              <span>{identity.email ?? "Account"}</span>
            </Link>
          ) : (
            <Link className="button button-quiet button-small" href="/login">Sign in</Link>
          )}
        </div>
      </div>
    </header>
  );
}
