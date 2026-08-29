# Live pipeline — plan for 2026-08-29

Status: **approved execution plan**
Scope source: current product canon supplied from Notion
Boundary: this plan adds a new live slice; it does **not** authorise a broad
refactor of the existing analytics, importer, or web modules.

## Goal and first checkpoint

Deliver and verify this backend-owned path for Premier League 2026/27.  In
API-Football requests, the season parameter is `2026`.

```text
Premier League 2026/27
        ↓
API-Football
        ↓
Live Worker every 25 seconds
        ↓
Live Domain / normalizer
        ↓
Redis current state
        ↓
FastAPI GET /web/v1/live
```

The first checkpoint is complete only when the worker, live domain, Redis
state, and FastAPI endpoint work together with automated coverage for status
normalisation and internal-fixture resolution. A browser must never call
API-Football directly.

## Implementation order

This is the minimal implementation sequence reviewed by the Architect on
2026-08-29. It keeps the existing historical importer, analytics engine, and
web read layer intact unless a narrow integration change is required.

1. **Canonical gate — active Premier League season.** Before creating a worker
   or Redis state, verify the live Supabase database. If 2026/27 is absent,
   add a narrow active-season bootstrap for `league=39`, `season=2026`; do not
   broaden the completed-season importer. It must establish 20 season teams,
   the 380-fixture schedule (future and completed), current standings, and
   complete API-Football fixture-ID mappings.
2. **Live contracts.** Version the reviewed real provider samples, then add
   `app/live` provider DTOs, normalised domain states, current score/time
   selection, and PostgreSQL provider-fixture-ID resolution. Cover them with
   deterministic tests before introducing a worker or Redis.
3. **Client, configuration, and ephemeral store.** Refactor the existing
   API-Football client to own one reusable `httpx.AsyncClient` connection pool.
   Add `LIVE_POLL_INTERVAL_SECONDS=25`, a Redis connection setting, and a
   generic competition scope. The first Redis store has only
   `live:fixture:{fixture_id}` and `live:active_fixtures`.
4. **One central worker.** Implement a testable `poll_once()` before the
   25-second runtime loop. It polls only fixtures eligible from the canonical
   schedule, normalises provider responses, resolves internal IDs, and writes
   current state. It is the single live-state writer.
5. **Terminal reconciliation.** Do not assume a provider live response always
   includes `FT`. A fixture which disappears from the live feed after being
   active must be reconciled against canonical/provider final status before it
   is removed from Redis active state. No distributed locks or Pub/Sub are
   needed for this single-worker slice.
6. **REST contract.** Add a dedicated asynchronous live router and dependency;
   do not couple Redis to the historical PostgreSQL `WebReadService`.
   `GET /web/v1/live` reads Redis only and returns a stable `LiveFixtureDTO`.
7. **Minimal frontend, in the same slice.** After the REST contract is tested,
   add a small client component and a no-store backend read path. The browser
   polls FastAPI approximately every 15 seconds and never API-Football. Reuse
   the existing fixture/team presentation primitives rather than redesigning
   the web application.
8. **Verification and deployment.** Run unit, Redis integration, ASGI endpoint,
   worker-cycle, frontend typecheck/test/build, and a real match-day smoke
   check. Deploy exactly one worker process on the backend/VPS.

Live statistics and T-60 predictions remain follow-on slices. SSE, WebSocket,
Redis Pub/Sub/Streams, distributed coordination, and a large refactor are
explicitly out of scope.

## Execution checkpoint — paused at the live-domain boundary

Recorded on 2026-08-29 because this execution was paused by the session
limit. The canonical gate (implementation-order step 1) is complete:

- The retained API-Football Premier League 2026/27 canary was replayed into
  Supabase without a new provider request.
- Independent verification returned 20 season teams, 380 fixtures, 380
  API-Football-to-`football.fixtures.id` mappings, and 20 standings rows.
  The schedule contains 369 future `scheduled` fixtures and 11 `completed`
  fixtures.
- The narrow active-season importer and its initial-load batch path are
  committed in `2370ec1` and `7cfeac0`; existing historical import paths were
  not broadened.

**Resume at implementation-order step 2: live contracts.** Add
`backend/app/live` normalisation and internal fixture-ID resolution first,
then the reusable API client, Redis state, worker, REST endpoint, and minimal
frontend in that order. Do not replay the completed season import or make a
new API-Football call merely to resume this work.

## 1. Synchronise the canonical base

1. Reconcile this repository's product documentation with the current Notion
   canon. The repository copies are `docs/product-scope.md`,
   `docs/architecture.md`, and `README.md`.
2. Bootstrap or verify Premier League 2026/27 in the existing canonical data
   model:
   - exactly 20 season teams and their API-Football provider mappings;
   - the complete fixture calendar, including future and completed fixtures;
   - current-season standings;
   - every API-Football fixture ID used by the live pipeline resolves to the
     corresponding `football.fixtures.id`.
3. Preserve the existing analytics, importer, and web contracts unless a
   narrow change is necessary for this live slice. Do not start a separate
   large refactor.

## 2. Live domain and provider client

Create `backend/app/live/` as the core live domain. Its normaliser has one
stable internal state for each provider status required by the first slice:

| API-Football `fixture.status.short` | Internal state |
| --- | --- |
| `1H` | `first_half` |
| `HT` | `half_time` |
| `2H` | `second_half` |
| `FT` | `finished` |

For a live fixture, the normalised score comes exclusively from
`goals.home` and `goals.away`. `fixture.status.elapsed` is the displayed
minute and `fixture.status.extra` is added time. `score.fulltime` is final
score data and must not be used as a current live score.

Extend the backend-only API-Football client for sustained polling with a
reusable asynchronous HTTP connection pool. Make its polling cadence
configuration, rather than a code constant:

```dotenv
LIVE_POLL_INTERVAL_SECONDS=25
```

Tests must cover the four status mappings, score-field selection, elapsed and
extra-time handling, malformed/unsupported values, and API-Football fixture ID
to internal `football.fixtures.id` resolution.

## 3. Central live worker and Redis boundary

Run one central backend/VPS Live Worker. Every 25 seconds it asks
API-Football for active Premier League fixtures, resolves each provider fixture
ID in the canonical database, normalises the response, and writes current
state to Redis.

```text
API-Football
    ↓
Live Worker every 25s
    ↓
Live Domain / Normalizer
    ↓
Redis
```

Redis is only the current-live-state store. The initial key contract is:

```text
live:fixture:{fixture_id}
live:active_fixtures
```

On `FT`, remove the fixture from `live:active_fixtures` and delete or expire
its current-state key according to the worker's atomic cleanup operation. The
authoritative final result remains in Supabase and is reconciled there; Redis
is not a historical-results store.

## 4. FastAPI contract

Implement `GET /web/v1/live`. It reads only Redis and never initiates an
API-Football request. Its stable `LiveFixtureDTO` is designed for the Next.js
client and contains only internal identifiers and normalised presentation
state:

- internal `fixture_id` and home/away internal team identifiers;
- normalised status, elapsed minute, and optional added time;
- current home and away scores from `goals`;
- the display data required to render both teams without exposing provider
  credentials or requiring a frontend provider call.

The public DTO does not need to expose API-Football fixture IDs. Their mapping
is a backend integrity requirement, not a browser contract.

## 5. Work explicitly after the score/status slice

### Live match statistics

After the first checkpoint, add a separate statistics-polling layer. For each
selected active fixture it calls:

```text
/fixtures/statistics?fixture={provider_fixture_id}
```

One response supplies both teams' statistics for that fixture. Start with
configuration below and limit polling to active/hot fixtures; do not globally
poll thousands of matches.

```dotenv
LIVE_STATS_INTERVAL_SECONDS=90
```

### Predictions

Predictions remain separate from live state and begin only after the first
live slice. A T-60 worker reads the schedule, calls API-Football
`/predictions`, persists its provider-derived result in Supabase, and exposes
it through FastAPI. A prediction is never written to Redis live state and is
not changed after kickoff.

```text
Schedule → T-60 worker → API-Football /predictions → Supabase → FastAPI
```

### Minimal UI

The minimal Next.js live UI completes this first vertical slice, but begins
only after the Redis-backed REST contract is tested. It consumes
`GET /web/v1/live` through FastAPI; it does not own polling of API-Football or
bypass the backend.

## Acceptance checklist

- [x] Product, architecture, README, and API-Football overview no longer say
      that the approved scope excludes live or predictions.
- [x] Premier League `season=2026` has been verified in the canonical model:
      20 teams, mappings, full fixtures, standings, and fixture-ID resolution.
- [x] `app/live` normalisation, ID resolution, and their tests exist.
- [ ] The async API-Football client reuses connections and the live interval is
      read from `LIVE_POLL_INTERVAL_SECONDS`.
- [ ] A single central worker writes only current live state to the two Redis
      key families and removes finished fixtures from the active set.
- [ ] `GET /web/v1/live` reads Redis and returns a stable `LiveFixtureDTO`.
- [ ] A minimal Next.js client polls FastAPI only and displays live score/status.
- [ ] Live statistics and T-60 predictions remain separately scheduled follow-on
      slices; neither expands this first checkpoint.
