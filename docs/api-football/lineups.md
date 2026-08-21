# Fixture lineups: observed contract

## Scope and evidence

The only collected lineup response is the real request
`GET /fixtures/lineups?fixture=1208021`; see [request metadata](../../samples/api-football/lineups.request.json),
[raw response](../../samples/api-football/lineups.raw.json), and its `FT`
parent fixture in [fixtures.raw.json](../../samples/api-football/fixtures.raw.json).
It is 2024 research evidence, not a guarantee of availability for 2026 or a
sample of an unannounced future lineup.

## Wrapper, joins and top-level team entries

The wrapper has `get` string, `parameters` object, empty `errors` array,
`results` int `2`, `paging` with integer `current`/`total`, and `response` as
an array of two entries. These team IDs (`33`, `36`) match the parent fixture.
The response has no embedded `fixture_id`; bind records to the request's
fixture ID and associate home/away by matching each `team.id` to the parent
fixture, rather than relying on response array order.

Each sampled entry has:

- `team`: int `id`, string `name`/`logo`, plus `colors.player` and
  `colors.goalkeeper`, each with string `primary`, `number`, `border`;
- `coach`: int `id`, string `name`/`photo`;
- `formation`: string (both are `"4-2-3-1"` in this fixture);
- `startXI`: array of wrappers `{player: {...}}`;
- `substitutes`: array of the same wrappers.

The coach ID is the observed external `coach_id`; player IDs inside both arrays
are external `player_id` values. Team/coach/player data repeats provider
display attributes and should be reconciled by IDs rather than names or URLs.

## Players, positions and nullability

Each observed `player` has `id` int, `name` string, `number` int, `pos` string,
and `grid` string or null. `pos` values here are compact strings `G`, `D`, `M`,
`F`. Starting players (11 per team; 22 total) all have grids such as `"1:1"`
or `"2:4"`; all substitutes (9 per team; 18 total) have `grid: null`. This
is explicit nullability, not a reason to coerce null to an empty string.

The sample has two lineups of 11 starters and nine substitutes, but neither
array length nor formation vocabulary may be assumed fixed. No sampled key is
absent, but future consumers must handle missing entire team entries, empty
arrays, absent coach/formation, and null player attributes separately from the
observed complete finished-match response.

There is no substitution minute, actual appearance, captain flag or event log
in this endpoint response. `substitutes` means named bench here; it does not
show which players entered the match.

## Availability and lifecycle

**Observed fact:** full starting XIs, bench, coaches, formations and colours
were returned for a finished fixture. There is no future-fixture lineup sample,
so this dataset does not establish when a lineup first appears or how an empty
response is represented. **Inference:** provisional data can change before
kickoff and lineup information must be treated as a timestamped fixture
snapshot if it later informs prediction or presentation.

Lineups cannot be derived from fixture scores or standings. Historical
formation frequency can be calculated locally only after retaining completed
fixture lineup data. Pre-match lineups, if used at all, should be gathered by a
backend job near kickoff, never directly by frontend user requests.
