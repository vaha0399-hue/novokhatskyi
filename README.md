# Football Analytics

Football Analytics is a web-first football analytics platform. It provides
historical match data, calculated statistical indicators, team comparisons,
analytical views, and an in-progress backend-owned live match-state pipeline
through a responsive Next.js website. Provider-derived predictions are a
separate, post-live-slice T-60 workflow; they are not Redis live state and do
not change after kickoff.

## Product canon

The approved and current product boundary is recorded in
[`docs/product-scope.md`](docs/product-scope.md), including the live-first,
provider-prediction-later delivery order.

## Current phase

Stage 1 established the tested project foundation. API-Football contract
research, the reviewed development schema, completed historical imports, the
Analytics Engine, and FastAPI Read API are complete for the currently loaded
development seasons. The approved plan for 2026-08-29 is the first Premier
League 2026/27 live technical checkpoint: API-Football → central 25-second
worker → live domain → Redis current state → `GET /web/v1/live`.

The current repository contains:

- a minimal FastAPI backend;
- `GET /health` and its automated test;
- a quota-bounded, manually invoked API-Football research client;
- real API-Football contract samples and analysis documentation;
- canonical historical fixtures, statistics, and historical lineups;
- a cutoff-safe Analytics Engine and stable FastAPI read DTOs;
- a tested live normalizer, central polling worker, Redis current-state store,
  and Redis-backed FastAPI live endpoint;
- a responsive Next.js App Router frontend with a Supabase Auth SSR foundation;
- reviewed database migrations.

The live worker, Redis current-state path, and `/web/v1/live` endpoint are
implemented and tested but not yet deployed. The minimal UI is the remaining
part of the first checkpoint and will follow its separate visual direction.
The ordered implementation scope and acceptance checklist are in
[`docs/2026-08-29-live-pipeline-plan.md`](docs/2026-08-29-live-pipeline-plan.md).

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
- `GET /web/v1/live` will read current state from Redis; API-Football polling
  remains the responsibility of one backend worker.
- Database changes must eventually be made only through reviewed files in
  `supabase/migrations/`.
