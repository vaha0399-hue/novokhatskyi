# Analytics Engine v1

Status: **implemented backend foundation; no read endpoint yet**
Effective date: **2026-08-22**

## Scope

Analytics Engine v1 computes factual team history from canonical PostgreSQL
fixtures and `football.fixture_team_statistics`. It does not call
API-Football, does not write to Supabase, does not produce predictions, and
does not implement a frontend route.

Every calculation is parameterized by internal `team_id`, `season_id`, and an
`as_of_kickoff` timestamp. It contains no Premier League, country, or season
year constant. Adding leagues or seasons is therefore a data operation, not a
rewrite of the engine.

## Leakage boundary

For a target fixture **N**, history is selected only where:

```sql
fixture.kickoff_at < :as_of_kickoff
```

The comparison is strict: fixture N itself and a fixture at the same kickoff
timestamp are excluded. Only completed, finalized fixtures from the requested
season qualify. The repository and the pure calculator both enforce this
boundary; a record at or after the cutoff raises an invariant error.

Historical EPL 2024 data was collected retrospectively. For that reason the
v1 historical-validation boundary is kickoff ordering, not the later importer
observation timestamp. Retrospectively collected lineups/injuries are not
inputs to this engine.

## Dimensions and windows

The engine supports these views independently:

- scopes: `overall`, `home`, `away`;
- windows: Last 5, 10, 15, and 20;
- if fewer eligible matches exist, it returns the available sample size rather
  than inventing matches or zeroes.

Fixture comparison returns home overall + home-only history and away overall +
away-only history at the fixture's kickoff. It is factual input for a future
read API, not a prediction.

## Metrics

For each scope/window the typed contract returns:

- matches, W/D/L, points, PPG;
- goals scored/conceded and averages;
- average xG/xGA, total shots, shots on goal, possession, corners, yellow
  cards, and red cards;
- clean sheets, failed-to-score, and BTTS counts/rates;
- over and under rates for 0.5, 1.5, 2.5, and 3.5 total goals;
- consecutive wins, unbeaten, winless, losses, scored, clean-sheet, and BTTS
  streaks ending at the most recent eligible fixture.

Score-derived rates use every eligible completed fixture. Statistics-derived
averages preserve nullable source values: each `AverageMetric` exposes its
non-null `sample_size`, and `NULL` is never silently converted to zero. Yellow
and red cards are reported separately for the same reason.

## Validation

- Unit tests cover W/D/L, PPG, goal averages, xG/xGA, shots, possession,
  corners/cards, clean sheets, failed-to-score, BTTS, totals, streaks,
  reduced Last-N samples, and leakage rejection.
- The read-only SQL-oracle validation suite runs against
  `ANALYTICS_TEST_DB_URL`. For EPL 2024 it independently aggregates canonical
  PostgreSQL rows and compares **every v1 metric**, including streaks, for all
  12 combinations of overall/home/away × Last 5/10/15/20. It also verifies the
  strict cutoff, season predicate, unique history fixtures, and populated team
  statistics.

FastAPI DTOs and endpoints are an explicitly later stage, after this contract
is accepted.
