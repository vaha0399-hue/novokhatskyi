# Football Analytics

Football Analytics is a web-first football analytics platform. It provides
historical match data, calculated statistical indicators, team comparisons,
and analytical views through a responsive Next.js website. It does not produce
match predictions or win probabilities.

## Product canon

The approved and current product boundary is recorded in
[`docs/product-scope.md`](docs/product-scope.md). It supersedes older references
to prediction functionality.

## Current phase

Stage 1 established the tested project foundation. API-Football contract
research, the reviewed development schema, completed historical imports, the
Analytics Engine, and FastAPI Read API are complete for the currently loaded
development seasons.

The current repository contains:

- a minimal FastAPI backend;
- `GET /health` and its automated test;
- a quota-bounded, manually invoked API-Football research client;
- real API-Football contract samples and analysis documentation;
- canonical historical fixtures, statistics, and historical lineups;
- a cutoff-safe Analytics Engine and stable FastAPI read DTOs;
- a responsive Next.js App Router frontend with a Supabase Auth SSR foundation;
- reviewed database migrations.

The current frontend implementation and controlled next iterations are in
[`docs/frontend-implementation-plan.md`](docs/frontend-implementation-plan.md).
Supabase Auth configuration is documented in
[`docs/frontend-auth.md`](docs/frontend-auth.md). The broader product boundary
is in [`docs/product-scope.md`](docs/product-scope.md).

The approved Stage 1 scope and acceptance criteria are recorded in
[`docs/phase-1.md`](docs/phase-1.md). Architectural boundaries are recorded in
[`docs/architecture.md`](docs/architecture.md). API-Football observations are
indexed in [`docs/api-football/overview.md`](docs/api-football/overview.md).

## Repository layout

```text
backend/    FastAPI application and backend tests
frontend/   Next.js + TypeScript responsive website
supabase/   Reviewed PostgreSQL migrations
samples/    Reserved for raw, sanitised API response samples
docs/       Architecture and stage documentation
```

## Backend development

From the repository root:

```bash
cd backend
uv sync --locked
uv run uvicorn app.main:app --reload
```

Run the tests with:

```bash
cd backend
uv run pytest
```

The health endpoint is available at `GET http://127.0.0.1:8000/health`.

## Security

- Never commit secrets or real credentials.
- The browser frontend must never call API-Football directly.
- User requests must not trigger API-Football requests.
- Database changes must eventually be made only through reviewed files in
  `supabase/migrations/`.
