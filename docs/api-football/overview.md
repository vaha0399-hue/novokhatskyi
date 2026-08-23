# API-Football contract research overview

> **Current evidence:** Pro-plan access to EPL seasons 2025 and 2026 was
> verified on 2026-08-22. See
> [the Pro contract canary](pro-canary-2025-2026.md). The current product canon
> is an analytics platform without predictions or live ingestion; older scope
> wording below is retained only as historical Stage 2 context.

## Scope and season boundary

This research used real API-Football responses for Premier League
`league_id=39`, **research season `2024`**. The product's production target
remains **season `2026`**. Season 2024 was used only because the configured free
plan returned a plan error for 2026; the research samples must never be treated
as current 2026 football data.

No Supabase connection, schema, migration, importer, cron job, Feature Engine,
Prediction Engine, or frontend integration was created.

## Request accounting

The approved research run made seven successful calls:

| Endpoint | Parameters | Results |
| --- | --- | ---: |
| `/fixtures` | `league=39`, `season=2024` | 380 |
| `/teams` | `league=39`, `season=2024` | 20 |
| `/standings` | `league=39`, `season=2024` | 1 league object / 20 rows |
| `/teams/statistics` | `league=39`, `season=2024`, `team=33` | 11 response sections |
| `/fixtures/statistics` | `fixture=1208021` | 2 team objects |
| `/injuries` | `league=39`, `season=2024` | 3,168 fixture-player records |
| `/fixtures/lineups` | `fixture=1208021` | 2 team lineups |

Before the season change, the execution session made two additional
`/leagues?id=39&season=2026` attempts. Both returned the provider plan error
that free accounts may access only seasons 2022–2024. Operational accounting is
therefore **nine HTTP attempts in total**.

Only the seven successful 2024 calls are independently reproducible from the
repository manifest. The fail-closed client did not persist either rejected
error response, so the two-attempt 2026 history is an execution-log observation,
not raw sample evidence. No JSON has been reconstructed or fabricated for those
failures. Provider headers moved `x-ratelimit-requests-remaining` from 99 to 93
during the seven successful calls, which is consistent with seven recorded
daily-quota units; it does not independently prove how the rejected attempts
were accounted for.

The canonical evidence is
[`samples/api-football/manifest.json`](../../samples/api-football/manifest.json).
Each endpoint has an unmodified `*.raw.json` response body and a separate
`*.request.json` file containing only safe parameters, timestamps, response
counts, paging, HTTP status, and allow-listed rate-limit headers.

## Common observed wrapper

All seven successful raw responses have these top-level keys:

```text
get         string
parameters  object
errors      array (empty in all successful samples)
results     integer
paging      object {current: integer, total: integer}
response    array or object depending on endpoint
```

Important contract details:

- raw `parameters` values are strings even though the collector supplied
  integer query values;
- all samples report `paging.current=1`, `paging.total=1`, including the
  3,168-record injuries response;
- `errors` is an empty array on success, while provider failures can use other
  shapes; clients must not assume one fixed error container type;
- `results` is not universally `response.length`: team statistics returns a
  response object and `results=11`, matching its top-level sections;
- `response` is an array for all sampled endpoints except team statistics,
  where it is an object.

## Availability lifecycle

### Observed

- The fixtures sample is a terminal historical snapshot: all 380 fixtures are
  `FT` with populated goals, halftime/fulltime scores, periods, referees and
  venues.
- Fixture statistics and complete lineups are present for one finished fixture.
- Injuries are fixture-specific historical availability records, not durable
  injury episodes.
- Standings and team statistics are completed-season aggregate snapshots.

### Not observed

The seven-call budget contains no future or live fixture response. Therefore
the exact null/empty shapes for `NS`, live, postponed or cancelled fixtures,
pre-match lineups, and pre-match fixture statistics remain unverified. These
must be collected from season 2026 after the subscription permits access. They
must not be fabricated from documentation.

### Operational inference for a future backend pipeline

- schedule, kickoff, venue, referee and status can be corrected over time;
- standings and team aggregates change after matches;
- injury availability can change repeatedly before kickoff;
- lineups normally become useful only near kickoff and can be corrected;
- live/post-match statistics can change until provider finalisation.

Every future snapshot-oriented collection therefore needs an application-side
`fetched_at`. User page requests must read our database and must never trigger
API-Football calls.

## Candidate collection cadence

This is a quota-conscious recommendation for one league, not an implemented
scheduler:

| Data | Candidate cadence | Reason |
| --- | --- | --- |
| league coverage and season metadata | once at season bootstrap, then on provider-plan changes | Slow-changing configuration. |
| teams and venues | once at season bootstrap; occasional refresh | Membership is season-scoped; display/venue attributes can change. |
| fixtures/schedule | daily, plus controlled matchday refreshes | Kickoff, venue and status can change. |
| standings | daily and after completed matchdays | Useful for validation; product features should be calculated locally. |
| team statistics | research/validation backend job only | Most aggregates are derivable from canonical fixtures. |
| fixture statistics | once after completion, with at most one later correction refresh if needed | Post-match metrics such as xG/shots are not score-derivable. |
| injuries | daily; closer refresh before relevant fixtures if used in pre-match availability views | Highly time-sensitive. |
| lineups | one backend job close to kickoff, only for fixtures whose lineup matters | Fixture-specific and quota-expensive; never user-triggered. |

With a free allowance of 100 calls/day, fixture-specific statistics and lineups
must be scheduled selectively and request accounting must be persisted. There
is no reason to fetch H2H or form per user request.

## Endpoint documents

- [Fixtures](fixtures.md)
- [Teams](teams.md)
- [Standings](standings.md)
- [Team statistics](team-statistics.md)
- [Fixture statistics](fixture-statistics.md)
- [Injuries](injuries.md)
- [Lineups](lineups.md)
- [Data-model notes](data-model-notes.md)
