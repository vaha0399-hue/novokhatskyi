# Frontend authentication — Supabase Auth SSR

Status: **password-auth flows are implemented and locally verified; hosted
Dashboard redirect/template configuration and email delivery still require an
external environment smoke test.**

## Official implementation basis

This frontend follows Supabase's official Next.js App Router SSR guidance:

- [Build a User Management App with Next.js](https://supabase.com/nextjs);
- [Server-side Auth](https://supabase.com/docs/guides/auth/server-side);
- [Creating a Supabase client for SSR](https://supabase.com/docs/guides/auth/server-side/creating-a-client);
- [Password authentication](https://supabase.com/docs/guides/auth/passwords).

It uses the official `@supabase/ssr` package with `@supabase/supabase-js`:

- browser client: `frontend/lib/supabase/client.ts`;
- cookie-aware server client: `frontend/lib/supabase/server.ts`;
- session-refresh proxy: `frontend/proxy.ts` and
  `frontend/lib/supabase/proxy.ts`.

The proxy refreshes cookie state and forwards the anti-cache response headers
required whenever auth cookies change. Server authorization checks use
`auth.getClaims()` rather than trusting a stale session object. The protected
`/account` and password-update pages independently check claims again before
rendering. This cookie path preserves the Supabase session across a browser
refresh.

## Required frontend environment

Create **ignored** `frontend/.env.local` from `frontend/.env.example`:

```dotenv
NEXT_PUBLIC_SUPABASE_URL=https://<project-ref>.supabase.co
NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY=<Supabase publishable key>
BACKEND_INTERNAL_URL=http://127.0.0.1:8000
```

The first two values are Supabase's public browser configuration and are the
only Supabase values permitted in a browser bundle. `BACKEND_INTERNAL_URL` is
server-only and must not be renamed with a `NEXT_PUBLIC_` prefix.

Never place any of these in frontend environment files:

- `SUPABASE_DB_URL` or a database password;
- `SUPABASE_ACCESS_TOKEN`;
- any service-role/secret key or JWT signing secret;
- `API_FOOTBALL_KEY`.

## Required Supabase Dashboard configuration

Before testing a real account in local preview, configure Supabase Auth with
the local site URL and development-only wildcard redirects. The wildcard is
needed because the callback carries a sanitized `next` query parameter:

```text
Site URL:      http://localhost:3000
Redirect URLs: http://localhost:3000/**
               http://127.0.0.1:3000/**
```

For a deployed environment, add only its HTTPS origin and callback path. Scope
any required wildcard to `/auth/confirm` and its query string; do not use a
whole-origin wildcard in production.

The recommended default templates use `{{ .ConfirmationURL }}` and preserve the
PKCE redirect automatically. If a customized template uses token hashes, use
the runtime `{{ .RedirectTo }}` value because the forms pass the full callback
URL, including `next`:

```text
{{ .RedirectTo }}&token_hash={{ .TokenHash }}&type=email
```

For password recovery, use the same pattern in its separate template:

```text
{{ .RedirectTo }}&token_hash={{ .TokenHash }}&type=recovery
```

Supabase's default email provider is rate-limited. Configure production SMTP
later before inviting real users; it is not required to build the UI.

## Implemented flows

| Flow | UI route | Supabase operation |
| --- | --- | --- |
| Sign up | `/register` | `auth.signUp()` with a confirmation redirect |
| Confirm email | `/auth/confirm` | PKCE `auth.exchangeCodeForSession()` or token-hash `auth.verifyOtp()` |
| Sign in | `/login` | `auth.signInWithPassword()` |
| Sign out | `/account` | `auth.signOut()` |
| Forgot password | `/forgot-password` | `auth.resetPasswordForEmail()` through `/auth/confirm` |
| Update password | `/auth/update-password` | `auth.updateUser()` |
| Protected account | `/account` | proxy + server `auth.getClaims()` |

## Registration contract

The registration form invokes only `auth.signUp()`. It never attempts a login,
password reset, administrative user lookup, or direct `auth.users` query.

When Supabase reports that the email is already registered, the form displays:

```text
An account with this email already exists. Sign in or reset your password.
```

The mapping handles the explicit `user_already_exists` / `email_exists` codes,
legacy duplicate-account messages, and Supabase's current obfuscated
existing-user response with an empty `identities` collection. This deliberately
reveals account existence as a product requirement. No service-role key or
other privileged credential is added to the frontend to achieve it.

With local `enable_confirmations = false`, a successful new registration may
return a session immediately; that session belongs to the account just created
by `signUp()` and is not a fallback login for an existing account.

## Recovery and session lifecycle

1. Forgot Password sends the recovery email with `/auth/confirm` as its
   application redirect.
2. The callback exchanges either a PKCE code or a supported token hash, writes
   the Supabase auth cookies, and redirects only to a sanitized local path.
3. `/auth/update-password` requires an authenticated recovery/session context
   before calling `auth.updateUser()`.
4. The root proxy refreshes auth cookies and forwards `Cache-Control`,
   `Expires`, and `Pragma` protections from `@supabase/ssr`.
5. Sign Out redirects only after `auth.signOut()` succeeds.

Callback failures are mapped to stable public messages; raw provider details
are not reflected into query strings or rendered to the user.

## Local verification

From `frontend/`:

```bash
npm test
npm run typecheck
npm run build
```

Automated coverage includes dedicated-operation boundaries for every form,
duplicate-registration handling, PKCE and token-hash callbacks, safe redirects,
password-recovery routing, protected-page checks, sign-out error handling, and
SSR cookie/header persistence contracts.

Auth UI does not call FastAPI or domain tables. Football data pages use only
server-side FastAPI DTO calls. Later FastAPI JWT verification and free/premium
entitlements will be an independent backend access-control stage.
