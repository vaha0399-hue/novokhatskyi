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
research, the reviewed development schema, and the EPL 2024 historical
fixture-statistics backfill are complete.

The current repository contains:

- a minimal FastAPI backend;
- `GET /health` and its automated test;
- a quota-bounded, manually invoked API-Football research client;
- real Premier League season 2024 contract samples and analysis documentation;
- placeholders for the future web frontend and reviewed database migrations.

Premier League 2024 is the first complete historical dataset: 380 fixtures and
760 team-level fixture-statistics rows. The next work, after a dedicated Git
checkpoint, is the Analytics Engine and its historical anti-leakage validation
layer; FastAPI read contracts follow. Frontend and Supabase Auth work do not
start before those backend contracts are stable. The detailed approved order
is in [`docs/product-scope.md`](docs/product-scope.md).

The approved Stage 1 scope and acceptance criteria are recorded in
[`docs/phase-1.md`](docs/phase-1.md). Architectural boundaries are recorded in
[`docs/architecture.md`](docs/architecture.md). API-Football observations are
indexed in [`docs/api-football/overview.md`](docs/api-football/overview.md).

## Repository layout

```text
backend/    FastAPI application and backend tests
frontend/   Reserved for the Next.js + TypeScript website
supabase/   Reserved for reviewed PostgreSQL migrations
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
