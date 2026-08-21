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
