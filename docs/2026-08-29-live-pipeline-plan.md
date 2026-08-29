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

Only after the backend checkpoint is stable, add the minimal Next.js live UI.
It consumes `GET /web/v1/live`; it does not own polling of API-Football and
does not bypass FastAPI.

## Acceptance checklist

- [ ] Product, architecture, README, and API-Football overview no longer say
      that the approved scope excludes live or predictions.
- [ ] Premier League `season=2026` has been verified in the canonical model:
      20 teams, mappings, full fixtures, standings, and fixture-ID resolution.
- [ ] `app/live` normalisation, ID resolution, and their tests exist.
- [ ] The async API-Football client reuses connections and the live interval is
      read from `LIVE_POLL_INTERVAL_SECONDS`.
- [ ] A single central worker writes only current live state to the two Redis
      key families and removes finished fixtures from the active set.
- [ ] `GET /web/v1/live` reads Redis and returns a stable `LiveFixtureDTO`.
- [ ] Live statistics and T-60 predictions remain separately scheduled follow-on
      slices; neither expands this first checkpoint.
