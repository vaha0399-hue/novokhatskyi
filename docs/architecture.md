# Architecture Boundaries

## Canon

The approved product scope is recorded in
[`product-scope.md`](product-scope.md). It defines the current live-first,
provider-prediction-later delivery order.

## Current scope

Football Analytics is currently a web-only MVP. The user-facing application
will be a responsive Next.js + TypeScript website supporting desktop and mobile
browsers.

The first supported competition is the Premier League. The immediate technical
checkpoint is its 2026/27 season (`season=2026` for API-Football): canonical
season data plus a backend-owned live score/status pipeline. Broader league
support is not part of this checkpoint.

## Target data flow

```text
API-Football
    -> FastAPI backend
    -> normalisation
    -> Supabase PostgreSQL
    -> Analytics Engine
    -> FastAPI backend
    -> Next.js website
    -> User
```

The live path is deliberately separate from historical import and analytics:

```text
API-Football
    -> one central Live Worker (LIVE_POLL_INTERVAL_SECONDS=25)
    -> app/live normaliser
    -> Redis current live state
    -> FastAPI GET /web/v1/live
    -> Next.js website
```

This boundary is backed by reviewed real API-Football samples, stable live
domain models, canonical provider mappings, and the two-key Redis contract.

## Invariants

1. FastAPI is the central application and orchestration layer.
2. API-Football is an upstream data source, not a browser-facing service.
3. The frontend must never receive `API_FOOTBALL_KEY` or other server secrets.
4. The frontend must never call API-Football directly.
5. User requests must not trigger API-Football requests, including live ones.
6. Football data must be synchronised to our database ahead of user requests.
7. Database changes must be versioned as migrations in
   `supabase/migrations/` after source-data analysis.
8. Imports must eventually be idempotent and must not create duplicates.
9. Analytics remains a historical, factual calculation layer. Provider
   predictions are a later, separate T-60 pipeline from schedule to Supabase
   to FastAPI; they are never Redis live state and do not change after kickoff.
10. No mobile app, desktop app, browser extension, or standalone public API is
    being designed at the current stage.

## Live domain boundary

`backend/app/live/` owns provider-payload normalisation and provider fixture-ID
resolution before current state is published. For the first slice it maps
`1H`, `HT`, `2H`, and `FT` to stable internal states `first_half`,
`half_time`, `second_half`, and `finished`.

It obtains the current score only from `goals.home` and `goals.away`; it does
not use `score.fulltime` as a live score. The displayed minute comes from
`fixture.status.elapsed` and optional added time from `fixture.status.extra`.

Only the central worker can write the two initial Redis key families:

```text
live:fixture:{fixture_id}
live:active_fixtures
```

Redis is not canonical storage or a live-event archive. When an active fixture
disappears from the league live feed, the worker permits at most one
fixture-bound provider check per cycle and a 300-second per-fixture cooldown.
Confirmed `FT` first ensures the existing Supabase reconciliation state, whose
schema guard keeps finalization at or after `kickoff + 3 hours`; only then is
Redis state removed. `FT` already present in the league response needs no
second provider request. `GET /web/v1/live` reads Redis only and returns a
stable `LiveFixtureDTO` through a process-owned reusable async Redis pool;
neither FastAPI's read endpoint nor Next.js calls API-Football. The DTO exposes
only internal IDs and normalised presentation state, and Redis failures remain
a `503` boundary rather than falling through to a provider request.

The worker shares one secondary provider-request budget per cycle between a
disappeared-fixture check and one due post-match reconciliation task.

Live fixture statistics are a subsequent, separately scheduled layer. It will
query `/fixtures/statistics?fixture={provider_fixture_id}` only for selected
active/hot fixtures, initially at `LIVE_STATS_INTERVAL_SECONDS=90`.

## Stage discipline

The historical analytics and read contracts remain independent from the live
slice. The next execution plan is
[`2026-08-29-live-pipeline-plan.md`](2026-08-29-live-pipeline-plan.md); it
requires narrow additive work and explicitly excludes a broad refactor of the
existing analytics, importer, or web modules.
