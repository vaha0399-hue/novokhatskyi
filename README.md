# Football Analytics

Football Analytics is a web-first service for analysing upcoming football
matches. The MVP is limited to the Premier League and will eventually expose
pre-match HOME/DRAW/AWAY probabilities through a Next.js website.

## Current phase

Stage 1 established the tested project foundation. Stage 2 is researching the
real API-Football data contract before any database schema is designed.

The current repository contains:

- a minimal FastAPI backend;
- `GET /health` and its automated test;
- a quota-bounded, manually invoked API-Football research client;
- real Premier League season 2024 contract samples and analysis documentation;
- placeholders for the future web frontend and reviewed database migrations.

Season 2024 is used only for response-shape research because the configured
free API plan does not permit season 2026. The production target remains
season 2026. Supabase connectivity, schema design, data import, feature
calculation, predictions, and the main frontend remain out of scope.

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
