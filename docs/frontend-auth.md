# Frontend authentication — Supabase Auth SSR

Status: **implemented frontend foundation; dashboard redirect configuration is
required before live signup tests.**

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

The proxy refreshes cookie state. Server authorization checks use
`auth.getClaims()` rather than trusting a stale session object. The protected
`/account` page independently checks claims again before rendering.

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
the exact local site URL and redirect URLs:

```text
Site URL:      http://localhost:3000
Redirect URLs: http://localhost:3000/auth/confirm
               http://localhost:3000/auth/update-password
```

For a deployed environment, add its exact HTTPS origin and corresponding
`/auth/confirm` and `/auth/update-password` URLs separately. Do not broadly
allow arbitrary redirect origins.

For the SSR token-hash confirmation route, set the confirmation email template
to use the application callback, for example:

```text
{{ .SiteURL }}/auth/confirm?token_hash={{ .TokenHash }}&type=email&next=/account
```

For password recovery, configure the recovery email with:

```text
{{ .SiteURL }}/auth/confirm?token_hash={{ .TokenHash }}&type=recovery&next=/auth/update-password
```

Supabase's default email provider is rate-limited. Configure production SMTP
later before inviting real users; it is not required to build the UI.

## Implemented flows

| Flow | UI route | Supabase operation |
| --- | --- | --- |
| Sign up | `/register` | `auth.signUp()` with a confirmation redirect |
| Confirm email | `/auth/confirm` | `auth.verifyOtp()` token-hash exchange |
| Sign in | `/login` | `auth.signInWithPassword()` |
| Sign out | `/account` | `auth.signOut()` |
| Forgot password | `/forgot-password` | `auth.resetPasswordForEmail()` |
| Update password | `/auth/update-password` | `auth.updateUser()` |
| Protected account | `/account` | proxy + server `auth.getClaims()` |

Auth UI does not call FastAPI or domain tables. Football data pages use only
server-side FastAPI DTO calls. Later FastAPI JWT verification and free/premium
entitlements will be an independent backend access-control stage.
