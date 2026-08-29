# Backend

Minimal FastAPI backend for the Football Analytics web MVP.

## Local checks

```bash
uv run pytest
```

## Run locally

```bash
uv run uvicorn app.main:app --reload
```

## API-Football sample collection

The manual collector is for Stage 2 contract research only; it is not an importer or a
scheduled job. It reads `API_FOOTBALL_KEY` only from the process environment and writes no
request headers. Provide an explicit, empty output directory:

```bash
uv run python -m scripts.collect_api_football_samples \
  --output-dir ../samples/api-football \
  --season 2024 \
  --request-limit 7
```

This invokes exactly seven research-only endpoint groups: fixtures, teams, standings, team
statistics, fixture statistics, injuries, and lineups. The production target remains season
2026; the research season is recorded in the sample manifest.

### One-shot live contract sample

The live collector makes exactly one `GET /fixtures?live=all` request, writes
no database rows, and retains the unmodified response body with sanitised
metadata and a compact summary. Use a new empty output directory for every
collection:

```bash
uv run python -m scripts.collect_live_fixture_sample \
  --output-dir ../samples/api-football/live-fixtures-YYYY-MM-DDTHHMMZ
```

This sample is for provider-contract research. The production worker will poll
the configured competition scope (initially Premier League `live=39`) rather
than the global `live=all` feed.

## Active-season canonical replay

The active-season importer is separate from the completed-season backfill. It
accepts a reviewed, complete provider schedule containing only `NS` and `FT`,
persists the four raw provider responses, and verifies canonical teams,
fixtures, provider mappings, and standings. It does not call API-Football when
replaying retained artifacts.

```bash
uv run python -m app.importer.active_season \
  --replay-directory ../samples/api-football/pro-canary-2026-08-29 \
  --league-external-id 39 \
  --season-start-year 2026 \
  --expected-fixture-count 380
```

The command requires `SUPABASE_DB_URL`, validates each raw/request artifact's
scope, HTTP status, byte count, SHA-256, and timestamps before opening its
database transaction. It prints only the final verification counts.
