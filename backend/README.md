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

## Live configuration

The first live slice polls Premier League by default while retaining a
generic, configuration-driven league scope:

```dotenv
REDIS_URL=redis://localhost:6379/0
LIVE_POLL_INTERVAL_SECONDS=25
LIVE_TERMINAL_RECHECK_INTERVAL_SECONDS=300
LIVE_LEAGUE_EXTERNAL_IDS=39
LIVE_REDIS_MAX_CONNECTIONS=10
```

Multiple league IDs may be comma- or hyphen-separated. The worker converts
them to API-Football's `live=39-2-140` form. Redis contains only current live
state under `live:fixture:{fixture_id}` and `live:active_fixtures`; it is not a
historical result store.

Run exactly one worker process on the backend/VPS:

```bash
uv run python -m app.live.worker
```

Each normal cycle starts one scoped `GET /fixtures?live=39` request on the
configured 25-second cadence. An `FT` already present in that response needs no
additional provider call. If an active fixture disappears without `FT`, the
worker permits at most one additional fixture-bound `GET /fixtures?id=...`
request in that cycle and does not recheck the same fixture for 300 seconds.
That single secondary-request budget is shared with one due post-match
reconciliation task.

Redis state is removed only after `FT` is confirmed and the worker has
successfully ensured the fixture's existing `ops.fixture_reconciliation_state`
handoff. The early live response does not write a final score or bypass the schema's
`kickoff + 3 hours` reconciliation eligibility rule. Its post-match consumer
later invokes the schema-controlled finalizer through an eligible fetch; the
early live response itself is never treated as final provenance. Transient
PostgreSQL or Redis connection failures recreate that infrastructure session;
malformed provider/domain data remains fail-closed.

## Live REST contract

`GET /web/v1/live` reads one atomic Redis snapshot through the FastAPI
process's reusable async connection pool. It never calls API-Football or
PostgreSQL, and every response is marked `Cache-Control: no-store`. An empty
live window returns `{"fixtures": []}`; an unavailable or inconsistent Redis snapshot returns `503` with
`{"detail": {"code": "live_state_unavailable"}}`.

Each `LiveFixtureDTO` contains only internal fixture, season, league, and team
identifiers plus team names, kickoff time, normalised status, current score,
elapsed/added time, and the provider observation timestamp. Provider fixture
IDs and credentials are not part of the browser contract. Team logos remain
available through the existing internal-ID asset route.

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

## Seasonal discovery worker

`app.importer.season_sync` is a backend-only, one-shot job intended for a
systemd timer. It discovers only the reviewed, versioned allow-list:
Premier League, La Liga, Serie A, Bundesliga, and Ligue 1. It never scans the
provider's global catalogue and does not expose an HTTP endpoint.

Before any provider request, it checks the canonical season dates. While an
imported season has not ended, the job records `season_in_progress` and makes
**zero API-Football calls** for that league. Schedule the timer weekly (or more
often only in the May--September preseason window); it is not a nine-month
daily API polling process.

When discovery is due, it makes one bounded `/leagues?id=...` call. Only a
single provider-marked current `League` season that is absent from canonical
mappings is eligible. The job then obtains the scope-specific
league, teams, standings, and fixtures responses. It derives the expected
fixture count from the reviewed team format, so a partial schedule is recorded
as `not_ready` without canonical writes. A complete coherent response set is
validated and passed to the existing atomic active-season importer.

```bash
uv run python -m app.importer.season_sync
```

The command requires `API_FOOTBALL_KEY` and `SUPABASE_DB_URL`. It prints a
sanitised JSON run report and writes the operational checkpoint to
`ops.sync_runs` / `ops.sync_work_items`; safe rate-limit headers go to
`source.provider_rate_limit_state`. `SEASONAL_SYNC_QUOTA_RESERVE=3` stops a
run before consuming the last known provider calls. A provider `429` stops the
whole run; transport and server failures are recorded and are retried only by
the next timer invocation, never in a tight loop.

This job performs *first imports only*. Refreshing an existing schedule or
standings is deliberately a separate worker, because the current canonical
importer treats a changed future kickoff as an identity conflict.

## Completed statistics worker

`app.importer.incremental_statistics` is the second backend worker, separate
from the 25-second live/Redis worker. It reuses one API-Football connection
pool, finalizes only provider terminal results observed at least three hours
after kickoff, then batch-fetches missing fixture statistics (up to 20 IDs per
request) and refreshes rolling metrics for affected teams. The finalization
function is schema-owned; postponed or rescheduled fixtures are not inferred
as completed.

Configure explicit league/season scopes as `league:season[:max_requests]` and
run it periodically or continuously:

```bash
INCREMENTAL_STATISTICS_SCOPES=39:2026 \
INCREMENTAL_STATISTICS_INTERVAL_SECONDS=900 \
uv run python -m app.importer.incremental_statistics --continuous
```

The worker consumes the canonical finalized catalogue and does not write live
state to Redis. It is intentionally not enabled by the API process.
