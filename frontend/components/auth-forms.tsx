"use client";

import { useRouter } from "next/navigation";
import { type FormEvent, useState } from "react";

import {
  FAAuthHeading,
  FABrand,
  FAField,
  FAFormMessage,
  FASubmitButton,
} from "@/components/fa-auth-ui";
import { getSupabaseBrowserClient } from "@/lib/supabase/client";
import { signUpErrorMessage } from "@/lib/supabase/auth-messages";

type AuthFormProps = {
  nextPath?: string;
  initialMessage?: string | null;
  email?: string;
  setEmail?: (value: string) => void;
  onSuccess?: () => void;
};

function missingConfig(): string {
  return "Supabase Auth is not configured in this environment yet.";
}

export function SignInForm({
  nextPath = "/matches",
  initialMessage = null,
  email = "",
  setEmail,
  onSuccess,
  onForgot,
  onSignup,
}: AuthFormProps & { onForgot?: () => void; onSignup?: () => void }) {
  const router = useRouter();
  const [message, setMessage] = useState<string | null>(initialMessage);
  const [pending, setPending] = useState(false);
  const [showPassword, setShowPassword] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage(null);
    const form = event.currentTarget;
    const supabase = getSupabaseBrowserClient();
    if (!supabase) return setMessage(missingConfig());
    const data = new FormData(form);
    setPending(true);
    try {
      const { error } = await supabase.auth.signInWithPassword({
        email: String(data.get("email") ?? ""),
        password: String(data.get("password") ?? ""),
      });
      if (error) return setMessage(error.message);
      if (onSuccess) {
        onSuccess();
        return;
      }
      router.replace(nextPath);
      router.refresh();
    } catch {
      setMessage("Sign in is temporarily unavailable. Please try again.");
    } finally {
      setPending(false);
    }
  }

  return (
    <>
      <FABrand />
      <FAAuthHeading eyebrow="SECURE MODEL ACCESS" title="Sign in" text="Welcome back. Access match analytics, model insights and predictions." />
      <form className="fa-auth-form" onSubmit={submit}>
        <FAField required label="Email address" icon="@" name="email" type="email" placeholder="name@yourdomain.com" value={email} onValueChange={setEmail} autoComplete="email" />
        <FAField
          required
          minLength={8}
          label="Password"
          icon="◇"
          name="password"
          type={showPassword ? "text" : "password"}
          placeholder="Enter your password"
          autoComplete="current-password"
          action={<button className="fa-field-action" type="button" onClick={() => setShowPassword(!showPassword)} aria-label={showPassword ? "Hide password" : "Show password"}>{showPassword ? "HIDE" : "SHOW"}</button>}
        />
        <label className="fa-remember"><input name="remember" type="checkbox" defaultChecked /><span>Remember me</span></label>
        <FAFormMessage value={message} />
        <FASubmitButton disabled={pending}>{pending ? "Signing in…" : "Sign in"}</FASubmitButton>
      </form>
      <div className="fa-auth-divider"><span>or</span></div>
      <nav className="fa-auth-links" aria-label="Account actions">
        <button type="button" onClick={onForgot}>Forgot password?</button>
        <button type="button" onClick={onSignup}>Create account <b aria-hidden="true">›</b></button>
      </nav>
    </>
  );
}

export function SignUpForm({
  nextPath = "/matches",
  email = "",
  setEmail,
  onSuccess,
  onSignin,
}: AuthFormProps & { onSignin?: () => void }) {
  const router = useRouter();
  const [message, setMessage] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const [showPassword, setShowPassword] = useState(false);

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
    try {
      const { data: authData, error } = await supabase.auth.signUp({
        email: String(data.get("email") ?? ""),
        password,
        options: {
          data: { full_name: String(data.get("full_name") ?? "").trim() },
          emailRedirectTo: `${window.location.origin}/auth/confirm?next=${encodeURIComponent(nextPath)}`,
        },
      });
      const signupError = signUpErrorMessage(authData, error);
      if (signupError) return setMessage(signupError);
      form.reset();
      if (onSuccess) {
        onSuccess();
        return;
      }
      if (authData.session) {
        router.replace(nextPath);
        router.refresh();
        return;
      }
      setMessage("Check your email to confirm your account, then return here to sign in.");
    } catch {
      setMessage("Account creation is temporarily unavailable. Please try again.");
    } finally {
      setPending(false);
    }
  }

  return (
    <>
      <FABrand />
      <FAAuthHeading eyebrow="CREATE YOUR WORKSPACE" title="Join FA" text="Build your watchlist and unlock model-backed match predictions." />
      <form className="fa-auth-form fa-signup-form" onSubmit={submit}>
        <FAField required label="Full name" icon="○" name="full_name" type="text" placeholder="Your full name" autoComplete="name" />
        <FAField required label="Email address" icon="@" name="email" type="email" placeholder="name@yourdomain.com" value={email} onValueChange={setEmail} autoComplete="email" />
        <FAField
          required
          minLength={8}
          label="Create password"
          icon="◇"
          name="password"
          type={showPassword ? "text" : "password"}
          placeholder="Minimum 8 characters"
          autoComplete="new-password"
          action={<button className="fa-field-action" type="button" onClick={() => setShowPassword(!showPassword)} aria-label={showPassword ? "Hide password" : "Show password"}>{showPassword ? "HIDE" : "SHOW"}</button>}
        />
        <label className="fa-remember"><input required name="terms" type="checkbox" /><span>I agree to the Terms and Privacy Policy</span></label>
        <FAFormMessage value={message} />
        <FASubmitButton disabled={pending}>{pending ? "Creating account…" : "Create account"}</FASubmitButton>
      </form>
      <nav className="fa-single-auth-link"><span>Already have an account?</span><button type="button" onClick={onSignin}>Sign in <b aria-hidden="true">›</b></button></nav>
    </>
  );
}

export function ForgotPasswordForm({
  initialMessage = null,
  email = "",
  setEmail,
  onSuccess,
  onBack,
}: Pick<AuthFormProps, "initialMessage" | "email" | "setEmail" | "onSuccess"> & { onBack?: () => void }) {
  const [message, setMessage] = useState<string | null>(initialMessage);
  const [pending, setPending] = useState(false);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setMessage(null);
    const form = event.currentTarget;
    const supabase = getSupabaseBrowserClient();
    if (!supabase) return setMessage(missingConfig());
    const data = new FormData(form);
    setPending(true);
    const callbackUrl = new URL("/auth/confirm", window.location.origin);
    callbackUrl.searchParams.set("next", "/auth/update-password");
    try {
      const { error } = await supabase.auth.resetPasswordForEmail(String(data.get("email") ?? ""), {
        redirectTo: callbackUrl.toString(),
      });
      if (error) return setMessage(error.message);
      if (onSuccess) {
        onSuccess();
        return;
      }
      setMessage("If this address belongs to an account, a password-reset link is on its way.");
    } catch {
      setMessage("Password recovery is temporarily unavailable. Please try again.");
    } finally {
      setPending(false);
    }
  }

  return (
    <>
      <FABrand />
      <div className="fa-recovery-icon" aria-hidden="true">↗</div>
      <FAAuthHeading eyebrow="ACCOUNT RECOVERY" title="Reset password" text="Enter your account email. We’ll send you a secure reset link." />
      <form className="fa-auth-form fa-recovery-form" onSubmit={submit}>
        <FAField required label="Email address" icon="@" name="email" type="email" placeholder="name@yourdomain.com" value={email} onValueChange={setEmail} autoComplete="email" />
        <FAFormMessage value={message} kind={message?.startsWith("If this") ? "success" : "error"} />
        <FASubmitButton disabled={pending}>{pending ? "Sending…" : "Send reset link"}</FASubmitButton>
      </form>
      <nav className="fa-single-auth-link"><button type="button" onClick={onBack}>‹ Back to sign in</button></nav>
    </>
  );
}

export function UpdatePasswordForm() {
  const router = useRouter();
  const [message, setMessage] = useState<string | null>(null);
  const [pending, setPending] = useState(false);
  const [showPassword, setShowPassword] = useState(false);

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
    try {
      const { error } = await supabase.auth.updateUser({ password });
      if (error) return setMessage(error.message);
      router.replace("/account");
      router.refresh();
    } catch {
      setMessage("Password update is temporarily unavailable. Request a new recovery link.");
    } finally {
      setPending(false);
    }
  }

  return (
    <>
      <FABrand />
      <div className="fa-recovery-icon" aria-hidden="true">↗</div>
      <FAAuthHeading eyebrow="ACCOUNT RECOVERY" title="Choose a new password" text="Create a new secure password for your FA account." />
      <form className="fa-auth-form fa-recovery-form" onSubmit={submit}>
        <FAField required minLength={8} label="New password" icon="◇" name="password" type={showPassword ? "text" : "password"} placeholder="Minimum 8 characters" autoComplete="new-password" action={<button className="fa-field-action" type="button" onClick={() => setShowPassword(!showPassword)} aria-label={showPassword ? "Hide password" : "Show password"}>{showPassword ? "HIDE" : "SHOW"}</button>} />
        <FAField required minLength={8} label="Confirm password" icon="◇" name="password_confirmation" type={showPassword ? "text" : "password"} placeholder="Repeat your password" autoComplete="new-password" />
        <FAFormMessage value={message} />
        <FASubmitButton disabled={pending}>{pending ? "Updating…" : "Set new password"}</FASubmitButton>
      </form>
    </>
  );
}

export function LogoutButton() {
  const router = useRouter();
  const [message, setMessage] = useState<string | null>(null);
  const [pending, setPending] = useState(false);

  async function signOut() {
    const supabase = getSupabaseBrowserClient();
    if (!supabase) return setMessage(missingConfig());
    setMessage(null);
    setPending(true);
    try {
      const { error } = await supabase.auth.signOut();
      if (error) return setMessage(error.message);
      router.replace("/login");
      router.refresh();
    } catch {
      setMessage("Sign out is temporarily unavailable. Please try again.");
    } finally {
      setPending(false);
    }
  }

  return <div className="logout-control">{message ? <p className="auth-message auth-message-error" role="alert">{message}</p> : null}<button className="button button-quiet" onClick={signOut} disabled={pending}>{pending ? "Signing out…" : "Sign out"}</button></div>;
}
