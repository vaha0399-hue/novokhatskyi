# Football Analytics frontend

Responsive Next.js App Router frontend for factual football analytics.

## Local setup

```bash
cp .env.example .env.local
npm install
npm run dev
```

`BACKEND_INTERNAL_URL` is used only by Next.js Server Components to call
FastAPI. The browser never calls API-Football or Supabase `football.*` tables.

Only the public Supabase URL and publishable key may be placed in
`.env.local`. Do not copy any database URL, password, service-role key,
API-Football key, access token, or JWT signing secret into this directory.

For required Supabase Auth redirect URLs and the production checklist, see
[`../docs/frontend-auth.md`](../docs/frontend-auth.md).
