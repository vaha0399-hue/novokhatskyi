"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";

import { Brand } from "@/components/brand";
import { getSupabaseBrowserClient } from "@/lib/supabase/client";

type Identity = { email: string | null } | null;

const primaryNavigation = [
  { href: "/matches", label: "Матчи", activePrefixes: ["/matches", "/fixtures"] },
  { href: "/leagues", label: "Лиги", activePrefixes: ["/leagues"] },
  { href: "/analytics", label: "Аналитика", activePrefixes: ["/analytics", "/teams"] },
  { href: "/predictions", label: "Прогнозы", activePrefixes: ["/predictions"] },
  { href: "/favorites", label: "Избранное", activePrefixes: ["/favorites"] },
] as const;

function isActiveSection(pathname: string, prefixes: readonly string[]): boolean {
  return prefixes.some(
    (prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`),
  );
}

export function AppHeader() {
  const [identity, setIdentity] = useState<Identity>(null);
  const pathname = usePathname();

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
        <nav className="main-nav" aria-label="Основная навигация" lang="ru">
          {primaryNavigation.map((item) => {
            const active = isActiveSection(pathname, item.activePrefixes);
            return (
              <Link
                aria-current={active ? "page" : undefined}
                className={`main-nav-link${active ? " main-nav-link-active" : ""}`}
                href={item.href}
                key={item.href}
              >
                {item.label}
              </Link>
            );
          })}
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
