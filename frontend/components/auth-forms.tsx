"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { type FormEvent, useState } from "react";

import { getSupabaseBrowserClient } from "@/lib/supabase/client";

type AuthFormProps = { nextPath?: string };

function FormMessage({ value, kind = "error" }: { value: string | null; kind?: "error" | "success" }) {
  return value ? <p className={`auth-message auth-message-${kind}`} role={kind === "error" ? "alert" : "status"}>{value}</p> : null;
}

function missingConfig(): string {
  return "Supabase Auth is not configured in this environment yet.";
}

export function SignInForm({ nextPath = "/account" }: AuthFormProps) {
  const router = useRouter();
  const [message, setMessage] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage(null);
    const form = event.currentTarget;
    const supabase = getSupabaseBrowserClient();
    if (!supabase) return setMessage(missingConfig());
    const data = new FormData(form);
    setPending(true);
    const { error } = await supabase.auth.signInWithPassword({
      email: String(data.get("email") ?? ""), password: String(data.get("password") ?? ""),
    });
    setPending(false);
    if (error) return setMessage(error.message);
    router.replace(nextPath);
    router.refresh();
  }

  return <form className="auth-form" onSubmit={submit}>
    <label>Email<input required name="email" type="email" autoComplete="email" placeholder="you@example.com" /></label>
    <label>Password<input required name="password" type="password" autoComplete="current-password" placeholder="••••••••" /></label>
    <div className="auth-form-row"><span>Secure Supabase session</span><Link href="/forgot-password">Forgot password?</Link></div>
    <FormMessage value={message} />
    <button className="button button-primary button-full" disabled={pending} type="submit">{pending ? "Signing in…" : "Sign in"}</button>
  </form>;
}

export function SignUpForm({ nextPath = "/account" }: AuthFormProps) {
  const [message, setMessage] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage(null);
    const form = event.currentTarget;
    const supabase = getSupabaseBrowserClient();
    if (!supabase) return setMessage(missingConfig());
    const data = new FormData(form);
    const password = String(data.get("password") ?? "");
    if (password.length < 8) return setMessage("Use at least 8 characters for your password.");
    setPending(true);
    const { error } = await supabase.auth.signUp({
      email: String(data.get("email") ?? ""),
      password,
      options: { emailRedirectTo: `${window.location.origin}/auth/confirm?next=${encodeURIComponent(nextPath)}` },
    });
    setPending(false);
    if (error) return setMessage(error.message);
    setMessage("Check your email to confirm your account, then return here to sign in.");
    form.reset();
  }

  return <form className="auth-form" onSubmit={submit}>
    <label>Email<input required name="email" type="email" autoComplete="email" placeholder="you@example.com" /></label>
    <label>Password<input required minLength={8} name="password" type="password" autoComplete="new-password" placeholder="Minimum 8 characters" /></label>
    <p className="field-hint">By registering you create an identity in Supabase Auth. We never store your password.</p>
    <FormMessage value={message} kind={message?.startsWith("Check") ? "success" : "error"} />
    <button className="button button-primary button-full" disabled={pending} type="submit">{pending ? "Creating account…" : "Create account"}</button>
  </form>;
}

export function ForgotPasswordForm() {
  const [message, setMessage] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage(null);
    const form = event.currentTarget;
    const supabase = getSupabaseBrowserClient();
    if (!supabase) return setMessage(missingConfig());
    const data = new FormData(form);
    setPending(true);
    const { error } = await supabase.auth.resetPasswordForEmail(String(data.get("email") ?? ""), {
      redirectTo: `${window.location.origin}/auth/update-password`,
    });
    setPending(false);
    if (error) return setMessage(error.message);
    setMessage("If this address belongs to an account, a password-reset link is on its way.");
  }

  return <form className="auth-form" onSubmit={submit}>
    <label>Email<input required name="email" type="email" autoComplete="email" placeholder="you@example.com" /></label>
    <FormMessage value={message} kind={message?.startsWith("If this") ? "success" : "error"} />
    <button className="button button-primary button-full" disabled={pending} type="submit">{pending ? "Sending…" : "Send reset link"}</button>
  </form>;
}

export function UpdatePasswordForm() {
  const router = useRouter();
  const [message, setMessage] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage(null);
    const form = event.currentTarget;
    const supabase = getSupabaseBrowserClient();
    if (!supabase) return setMessage(missingConfig());
    const data = new FormData(form);
    const password = String(data.get("password") ?? "");
    if (password.length < 8) return setMessage("Use at least 8 characters for your password.");
    if (password !== String(data.get("password_confirmation") ?? "")) return setMessage("Passwords do not match.");
    setPending(true);
    const { error } = await supabase.auth.updateUser({ password });
    setPending(false);
    if (error) return setMessage(error.message);
    router.replace("/account");
    router.refresh();
  }

  return <form className="auth-form" onSubmit={submit}>
    <label>New password<input required minLength={8} name="password" type="password" autoComplete="new-password" /></label>
    <label>Confirm password<input required minLength={8} name="password_confirmation" type="password" autoComplete="new-password" /></label>
    <FormMessage value={message} />
    <button className="button button-primary button-full" disabled={pending} type="submit">{pending ? "Updating…" : "Set new password"}</button>
  </form>;
}

export function LogoutButton() {
  const router = useRouter();
  const [pending, setPending] = useState(false);

  async function signOut() {
    const supabase = getSupabaseBrowserClient();
    if (!supabase) return;
    setPending(true);
    await supabase.auth.signOut();
    router.replace("/");
    router.refresh();
  }

  return <button className="button button-quiet" onClick={signOut} disabled={pending}>{pending ? "Signing out…" : "Sign out"}</button>;
}
