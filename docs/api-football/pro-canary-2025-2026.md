# API-Football Pro contract canary: EPL 2025 and 2026

## Scope

On 2026-08-22, after the API-Football Pro plan was activated, a controlled
read-only contract canary examined Premier League seasons 2025/26
(`season=2025`) and 2026/27 (`season=2026`). The run did not write to Supabase
and did not modify an importer or migration.

Evidence is stored under
[`samples/api-football/pro-canary-2026-08-22/`](../../samples/api-football/pro-canary-2026-08-22/).
Every API response is preserved as an unmodified `*.raw.json` body. Separate
`*.request.json` files contain safe request parameters, timestamps, quota
headers, byte counts, and SHA-256 hashes; they contain no authentication data.

## Request accounting

One initial `/leagues?id=39&season=2025` call confirmed the plan without
persisting its response. The persisted campaign then made nine calls:

| Endpoint | Parameters | Results |
| --- | --- | ---: |
| `/leagues` | `id=39`, `season=2025` | 1 |
| `/leagues` | `id=39`, `season=2026` | 1 |
| `/teams` | `league=39`, `season=2025` | 20 |
| `/standings` | `league=39`, `season=2025` | 1 league / 20 rows |
| `/teams/statistics` | `league=39`, `season=2025`, `team=33` | 11 sections |
| `/fixtures` | `league=39`, `season=2025`, `status=FT-AET-PEN` | 380 |
| `/fixtures` | `league=39`, `season=2026` | 380 |
| `/fixtures` | three fixture IDs in `ids` | 3 |
| `/fixtures/statistics` | `fixture=1378969` | 2 team rows |

Total physical API use was ten calls. The last response reported a daily Pro
limit of 7,500 with 7,390 remaining and a per-minute limit of 300. All calls
returned HTTP 200 with empty provider errors and one-page responses.

## Observed league and season contract

Both seasons return:

- league ID `39`, name `Premier League`, and type `League`;
- country name `England`, provider country code `GB-ENG`, and flag URL;
- season start/end dates, `current`, and per-season coverage flags.

Season 2025 is no longer current and covers 2025-08-15 through 2026-05-24.
Season 2026 is current and covers 2026-08-21 through 2027-05-30.

Both seasons advertise fixture events, lineups, fixture/player statistics,
injuries, players, standings, and top-player endpoints. Provider odds coverage
is false for season 2025 and true for season 2026. Provider prediction coverage
is present but is outside the approved Football Analytics product scope and
must not be consumed.

## Observed teams, standings, and season aggregates

The 2025 teams response contains 20 teams. A team has provider ID, name, club
code, country name, foundation year, national-team flag, and logo. Its venue
has provider ID, name, address, city, capacity, surface, and image.

The standings response contains one group and 20 rows. Each row has rank,
points, goals difference, form, movement status, description, and overall,
home, and away played/W/D/L/goals splits.

The team-statistics response retains the previously observed 11-section
contract: form, fixture W/D/L splits, goals for/against, biggest results and
streaks, clean sheets, failed-to-score counts, penalties, lineups, and card
and goal-time distributions. The sampled completed season form string has 38
characters.

## Observed fixture lifecycle

The completed 2025 request returned 380 `FT` fixtures across 38 rounds. The
current 2026 response returned the full 380-fixture schedule: six were `FT`
and 374 were `NS` at collection time.

The fixture structure is stable across both samples: fixture identity,
timestamp/date/timezone, periods, referee, venue, status, league/season/round,
home/away teams and winner flags, goals, and halftime/fulltime/extra-time/
penalty score objects. The application must continue mapping provider status
to the non-live canonical lifecycle rather than storing elapsed live state.

## Batch fixture finding

The three-ID `/fixtures?ids=...` response returned, for every selected
completed fixture:

- the standard fixture object;
- historical events;
- two lineups;
- two team-statistics blocks;
- two player-statistics blocks.

For fixture `1378969`, the embedded `statistics` array was structurally and
value-wise identical to the separately requested `/fixtures/statistics`
response. The observed statistics vocabulary remains the same 18 metrics as
the EPL 2024 contract. Values again include integers, percent strings, decimal
strings, and null. `goals_prevented` was `-0.95` for one team, confirming that
the signed mapping is necessary.

This proves the three-ID canary, not the documented maximum of 20 IDs or every
competition. A future batch importer needs its own replay/provenance and
partial-response tests. The current EPL 2024 statistics importer should not be
rewritten mid-campaign solely because batching is available.

## Comparison with the approved product roadmap

The observed contract supports the planned season page: dynamic season dates,
standings, 38 rounds, 380 fixtures, results, teams, and venues are available
without hardcoding one season.

It supports team and fixture analytics after normalization:

- Last 5/10/20 and current streak from completed fixtures;
- overall/home/away W/D/L and points per game;
- goals for/against, clean sheets, failed to score, BTTS, and over/under rates;
- xG/xGA, shots for/against, shots on target, possession, corners, cards,
  passing, saves, and goals prevented from paired fixture statistics;
- historical lineups and player performance as possible later product areas.

The contract does not justify predictions or live features. Embedded events
and live-capable status fields are provider capabilities, not approved product
requirements, and should remain unmodeled for the current MVP.

## Schema fit and known gaps

The current schema has canonical typed storage for leagues, seasons, teams,
venues, fixtures, standings, and all 18 observed fixture statistics, with
`extra_metrics` for new statistic labels and raw payload retention for audit.

Before a multi-country rollout, a separately approved additive migration
should be considered for:

1. provider country code (`GB-ENG`) and a normalized country relationship;
2. competition type (`League`/`Cup`);
3. timestamped season coverage snapshots;
4. optionally the exact terminal provider status code when needed for UI.

These gaps do not block completing the EPL 2024 statistics backfill. Team
season aggregates should continue to be calculated locally rather than copied
as authoritative provider totals. Player profiles/statistics require a later
independent schema stage if they enter the product roadmap.
