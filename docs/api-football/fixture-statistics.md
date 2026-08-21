# Fixture statistics: observed contract

## Scope and evidence

Evidence is the real response to `GET /fixtures/statistics?fixture=1208021`
in the 2024 research dataset: [request metadata](../../samples/api-football/fixture-statistics.request.json),
[raw response](../../samples/api-football/fixture-statistics.raw.json), and its
parent fixture in [fixtures.raw.json](../../samples/api-football/fixtures.raw.json).
Fixture `1208021` is Manchester United (`team_id=33`) vs Fulham (`team_id=36`),
finished `FT`; this is not evidence about production season 2026.

## Wrapper and entity relationship (observed)

The standard wrapper has `get` (string), `parameters` (object), `errors`
(empty array), `results` (int `2`), `paging` (integer `current`/`total`) and
`response` (array of two team-statistic objects). Each object has `team` and
`statistics`.

`team` is `{id: int, name: string, logo: string}`. The two IDs exactly match
the parent fixture's home and away IDs. The first response element is the home
team in this one sample, but response ordering must not be treated as a
contract; associate data by `team.id` and the parent `fixture_id`.

No `fixture_id`, league, season, timestamp, status or home/away marker is
included in the statistics response body. The request parameter (or the stored
collection context) is required to join it to the fixture.

## Statistics array and types

`statistics` is an array of `{type, value}` objects. `type` is a string and is
the semantic key; it is not a normalized enum in this response. There are 18
entries per team (36 overall), but consumers must treat both the array length
and the type vocabulary as variable.

Observed types for `value`:

| Category / observed `type` values | JSON type |
| --- | --- |
| shots, fouls, corners, offsides, cards, saves, passes | integer |
| `Ball Possession`, `Passes %` | percent-suffixed string (`"55%"`, `"85%"`) |
| `expected_goals`, `goals_prevented` | decimal string (`"2.43"`, `"1.07"`) |
| `Red Cards` | `null` for both teams |

The observed labels include case/format anomalies: `Shots insidebox`,
`Shots outsidebox`, `Total passes`, `Passes %`, `expected_goals` and
`goals_prevented`. Preserve raw labels; any canonical metric mapping later must
be explicit and versioned. A value parser must accept integer, decimal string,
percent string and null, and must not infer a missing metric as zero.

## Availability and lifecycle

**Observed fact:** statistics are returned for this `FT` fixture, whose final
score is available in the fixtures sample. No future or live fixture statistics
sample was collected, so availability before kickoff, during play, or after all
provider corrections is unknown from the dataset. It is reasonable only as an
inference that event-derived metrics can change during/after a match; a future
pipeline must capture its retrieval time if it preserves them.

Fixture statistics are post-match analytic inputs/validation data, not a
source for pre-match prediction features. Basic totals such as goals, win/draw
and home/away record should instead be recomputed from canonical completed
fixtures. xG, possession, passing and shots cannot be recreated from the
current score-only fixture record and would require preserving this endpoint's
data if they become product inputs.
