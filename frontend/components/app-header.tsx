"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { type ReactNode, useEffect, useRef, useState } from "react";

import { getSupabaseBrowserClient } from "@/lib/supabase/client";

type Identity = { email: string | null } | null;
type IconName =
  | "ball"
  | "bell"
  | "chevron"
  | "close"
  | "leagues"
  | "profile"
  | "search"
  | "signals"
  | "target";

const primaryNavigation = [
  { href: "/matches", label: "Matches", icon: "ball", activePrefixes: ["/matches", "/fixtures"], activeOnHome: true },
  { href: "/leagues", label: "Leagues", icon: "leagues", activePrefixes: ["/leagues"] },
  { href: "/analytics", label: "Signals", icon: "signals", activePrefixes: ["/analytics", "/teams"] },
  { href: "/predictions", label: "Predictions", icon: "target", activePrefixes: ["/predictions"] },
  { href: "/account", label: "Profile", icon: "profile", activePrefixes: ["/account"] },
] as const;

function isActiveSection(pathname: string, item: (typeof primaryNavigation)[number]): boolean {
  if ("activeOnHome" in item && item.activeOnHome && pathname === "/") return true;
  return item.activePrefixes.some(
    (prefix) => pathname === prefix || pathname.startsWith(`${prefix}/`),
  );
}

function NavigationIcon({ name }: { name: IconName }) {
  const common = {
    "aria-hidden": true,
    className: "fa-icon",
    fill: "none",
    viewBox: "0 0 24 24",
  } as const;

  if (name === "search") return <svg {...common}><circle cx="11" cy="11" r="6.5" /><path d="m16 16 4 4" /></svg>;
  if (name === "bell") return <svg {...common}><path d="M18 9.5c0-3.6-2.1-6-6-6s-6 2.4-6 6c0 4-1.7 5-2.5 6h17c-.8-1-2.5-2-2.5-6Z" /><path d="M9.5 19a2.8 2.8 0 0 0 5 0" /></svg>;
  if (name === "chevron") return <svg {...common}><path d="m8 10 4 4 4-4" /></svg>;
  if (name === "close") return <svg {...common}><path d="m6 6 12 12M18 6 6 18" /></svg>;
  if (name === "ball") return <svg {...common}><circle cx="12" cy="12" r="8.5" /><path d="m9.5 9.2 2.5-1.8 2.5 1.8-.9 3h-3.2l-.9-3ZM6 8.2l3.5 1M18 8.2l-3.5 1M7.1 16l3.3-3.8M16.9 16l-3.3-3.8M9.2 20l-2.1-4h-3M14.8 20l2.1-4h3" /></svg>;
  if (name === "leagues") return <svg {...common}><path d="M8 4h8v4.5a4 4 0 0 1-8 0V4Z" /><path d="M8 6H5.5v1.5A3.5 3.5 0 0 0 9 11M16 6h2.5v1.5A3.5 3.5 0 0 1 15 11M12 13v4M8.5 20h7M9.5 17h5" /></svg>;
  if (name === "signals") return <svg {...common}><path d="M4 18V6M4 18h16M7 15l4-4 3 2 5-7" /><path d="M16 6h3v3" /></svg>;
  if (name === "target") return <svg {...common}><circle cx="12" cy="12" r="8.5" /><circle cx="12" cy="12" r="4.5" /><circle cx="12" cy="12" r="1" /></svg>;
  return <svg {...common}><circle cx="12" cy="8" r="3.5" /><path d="M5.5 20a6.5 6.5 0 0 1 13 0" /></svg>;
}

function FABrand() {
  return (
    <Link className="fa-app-brand" href="/matches" aria-label="FA Sports Intelligence — Matches">
      <span className="fa-logo-orbit" aria-hidden="true"><span className="fa-logo-core">FA</span></span>
      <span className="fa-brand-copy" aria-hidden="true">
        <strong className="fa-wordmark">FA</strong>
        <small>SPORTS INTELLIGENCE</small>
      </span>
    </Link>
  );
}

function NavigationItems({ mobile = false }: { mobile?: boolean }) {
  const pathname = usePathname();
  return primaryNavigation.map((item) => {
    const active = isActiveSection(pathname, item);
    return (
      <Link
        aria-current={active ? "page" : undefined}
        className={mobile ? "fa-mobile-nav-link" : "fa-desktop-nav-link"}
        href={item.href}
        key={item.href}
      >
        {mobile ? <span className="fa-mobile-nav-icon"><NavigationIcon name={item.icon} /></span> : null}
        <span>{item.label}</span>
      </Link>
    );
  });
}

function HeaderAction({ children, className = "", label, onClick }: { children: ReactNode; className?: string; label: string; onClick?: () => void }) {
  return <button aria-label={label} className={`fa-header-action ${className}`} onClick={onClick} type="button">{children}</button>;
}

export function AppHeader() {
  const [identity, setIdentity] = useState<Identity>(null);
  const [mobileSearchOpen, setMobileSearchOpen] = useState(false);
  const searchInputRef = useRef<HTMLInputElement>(null);
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

  useEffect(() => setMobileSearchOpen(false), [pathname]);

  useEffect(() => {
    if (!mobileSearchOpen) return;
    searchInputRef.current?.focus();
    function closeOnEscape(event: KeyboardEvent) {
      if (event.key === "Escape") setMobileSearchOpen(false);
    }
    window.addEventListener("keydown", closeOnEscape);
    return () => window.removeEventListener("keydown", closeOnEscape);
  }, [mobileSearchOpen]);

  const email = identity?.email ?? null;
  const displayName = email?.split("@")[0] || "Profile";
  const initials = displayName.slice(0, 2).toUpperCase();
  const profileHref = identity ? "/account" : "/login?next=/account";

  return (
    <>
      <header className="site-header fa-header">
        <div className="fa-header-inner">
          <FABrand />
          <label className="fa-search fa-search-desktop">
            <NavigationIcon name="search" />
            <span className="fa-visually-hidden">Search</span>
            <input aria-label="Search" placeholder="Find a match, league or team" type="search" />
            <kbd>⌘ K</kbd>
          </label>
          <div className="fa-header-actions">
            <HeaderAction className="fa-mobile-search-trigger" label="Open search" onClick={() => setMobileSearchOpen(true)}><NavigationIcon name="search" /></HeaderAction>
            <HeaderAction className="fa-notification-button" label="Notifications"><NavigationIcon name="bell" /></HeaderAction>
            <Link className="fa-profile-control" href={profileHref} aria-label={identity ? `Profile: ${displayName}` : "Sign in to profile"}>
              <span className="fa-profile-avatar" aria-hidden="true">{identity ? initials : <NavigationIcon name="profile" />}</span>
              <span className="fa-profile-copy"><strong>{displayName}</strong><small>{identity ? "FREE PLAN" : "SIGN IN"}</small></span>
              <NavigationIcon name="chevron" />
            </Link>
          </div>
          <div className={`fa-mobile-search${mobileSearchOpen ? " fa-mobile-search-open" : ""}`} aria-hidden={!mobileSearchOpen}>
            <NavigationIcon name="search" />
            <input ref={searchInputRef} aria-label="Search" disabled={!mobileSearchOpen} placeholder="Find a match, league or team" type="search" />
            <HeaderAction label="Close search" onClick={() => setMobileSearchOpen(false)}><NavigationIcon name="close" /></HeaderAction>
          </div>
        </div>
      </header>
      <nav className="fa-desktop-nav" aria-label="Primary navigation"><div className="fa-desktop-nav-inner"><NavigationItems /></div></nav>
      <nav className="fa-mobile-nav" aria-label="Mobile navigation"><NavigationItems mobile /></nav>
    </>
  );
}
