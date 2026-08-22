# Controlled API-Football canary import

## Scope

The first persistent importer run used one deterministic historical Premier League fixture to
validate API-Football → PostgreSQL mapping. It is not a production sync, scheduler, backfill, live
pipeline, or frontend integration.

- league: Premier League, provider ID `39`;
- research season: `2024`;
- fixture: provider ID `1208021`;
- home: Manchester United, provider ID `33`;
- away: Fulham, provider ID `36`;
- canary key: `pl-2024-fixture-1208021-v1`.

Production season remains `2026`.

## Actual provider calls

The first run made exactly six sequential calls with no retry:

| Endpoint | Parameters | Results |
| --- | --- | ---: |
| `/fixtures` | `id=1208021` | 1 |
| `/teams` | `league=39`, `season=2024` | 20 |
| `/standings` | `league=39`, `season=2024` | 1 league object / 20 rows |
| `/fixtures/statistics` | `fixture=1208021` | 2 |
| `/injuries` | `fixture=1208021` | 6 |
| `/fixtures/lineups` | `fixture=1208021` | 2 team lineups |

The immediately repeated command made zero API calls and reused all six persisted fetches.

`/teams/statistics` was intentionally excluded because Stage 3B has no normalized team-statistics
table and the endpoint was not needed to validate the canary entities.

## Persistent row inventory

| Table | Rows |
| --- | ---: |
| `source.providers` | 1 |
| `source.provider_fetches` | 6 |
| `source.provider_raw_payloads` | 6 |
| `source.league_provider_refs` | 1 |
| `source.season_provider_refs` | 1 |
| `source.team_provider_refs` | 20 |
| `source.venue_provider_refs` | 20 |
| `source.fixture_provider_refs` | 1 |
| `source.player_provider_refs` | 46 |
| `source.coach_provider_refs` | 2 |
| `football.leagues` | 1 |
| `football.seasons` | 1 |
| `football.teams` | 20 |
| `football.venues` | 20 |
| `football.season_teams` | 20 |
| `football.fixtures` | 1 |
| `football.standings_snapshots` | 1 |
| `football.standings_snapshot_groups` | 1 |
| `football.standings_snapshot_rows` | 20 |
| `football.fixture_team_statistics` | 2 |
| `football.players` | 46 |
| `football.coaches` | 2 |

Historical injury and lineup responses were collected after kickoff. They are retained as raw
research provenance and their stable player/coach identities are mapped, but they were not inserted
into any pre-match snapshot table. All five availability/lineup snapshot and child tables remain
empty, preventing retrospective data leakage into future ML inputs.

## Typed statistic evidence

| Team provider ID | Possession | Pass accuracy | xG | Goals prevented | Red cards |
| --- | ---: | ---: | ---: | ---: | --- |
| `33` | `55.00` | `85.00` | `2.430` | `1.070` | `NULL` |
| `36` | `45.00` | `80.00` | `0.440` | `1.070` | `NULL` |

The two provider `null` red-card values are PostgreSQL `NULL`, not zero. Extra time and penalty
score fields also remain `NULL`. No unknown statistic label occurred, so both `extra_metrics`
objects are empty; the unmodified provider labels and values remain in raw payloads.

## Verification result

- exact external/internal fixture, league, season, home, and away joins: passed;
- orphan checks across provider refs, season membership, and standings membership: zero;
- duplicate provider mappings: zero;
- statistics rows for non-participant teams: zero;
- raw payload SHA-256, byte length, safe parameters, typed subjects, retention, and normalized
  timestamps: six of six passed;
- direct DML grants for `anon` and `authenticated`: zero;
- normalization replay inside the first process: no row-count change;
- full second command: zero API calls, six reused fetches, no row-count change.

The importer uses the server-only PostgreSQL connection from environment variables. It never
disables RLS, constraints, triggers, or replication safeguards and performs no delete, truncate,
schema creation, frontend call, Git commit, or push.
