# Injuries: observed contract

## Scope and evidence

This is analysis of the unmodified real research response for
`GET /injuries?league=39&season=2024`:
[request metadata](../../samples/api-football/injuries.request.json) and
[raw response](../../samples/api-football/injuries.raw.json). It is an
historical research snapshot for season 2024; it must not be represented as
current availability for production season 2026.

## Wrapper and volume

The wrapper contains `get` (string), `parameters` (object), `errors` (empty
array), `results` (int `3168`), `paging` (`current: 1`, `total: 1`) and a
`response` array of 3,168 objects. This sample therefore demonstrates that one
league-season injury request can be high-volume even where paging reports one
page. It does not prove the endpoint will always be unpaginated.

## Record shape and observed types

Every one of the 3,168 sampled records has exactly four objects:

- `player`: `id` int, `name`/`photo`/`type`/`reason` strings;
- `team`: `id` int, `name`/`logo` strings;
- `fixture`: `id` int, `timezone`/`date` strings, `timestamp` int;
- `league`: `id`/`season` ints, `name`/`country`/`logo`/`flag` strings.

There are no missing keys or explicit null values in this particular response.
That is a dataset observation, not a future nullability guarantee. The record
identifies a player availability entry **for a fixture**, not an injury episode
with an explicit start/end date: fixture date is ISO-8601 UTC in the sample and
timestamp is Unix seconds.

## IDs, repetition and time variance

Observed cardinalities are 20 teams, 371 fixtures and 403 players. The API
IDs form the joins: `league.id=39`, `league.season=2024`, `fixture.id`,
`team.id`, and `player.id`. A single player may appear for many fixtures: the
largest observed count is 37 records (E. Gonzalez, `player_id=385726`). Six
players appear with more than one `team.id`, so a player-team relationship
cannot be assumed season-constant.

There are no duplicate full objects and no duplicate `(player.id, fixture.id)`
pairs in this sample. That is useful for identifying this snapshot's records,
but it is **not** proof that this pair is a safe immutable database key across
later API refreshes: `type` and `reason` can change as a fixture approaches.
The fixture-date range is 2024-08-16 through 2025-05-25 UTC.

`player.type` has two observed values: `Missing Fixture` (2,786) and
`Questionable` (382). `player.reason` has 65 observed type/reason pairs;
examples include injury labels, `Red Card`, `Yellow Cards`, `Suspended`,
`Coach's decision` and `Loan agreement`. Thus `reason` is provider text, not a
strict injury taxonomy, and `type`/`reason` should not be used as stable enums
without a future controlled mapping.

## Lifecycle and use

The sample associates availability with scheduled/played fixtures. It gives no
collection timestamp per record and no historical change log. **Inference:**
injury/availability is highly time-sensitive and must be collected with an
application-side `fetched_at` if used for pre-match analysis; never overwrite
away past knowledge without preserving when it was known. A later data design
should retain the raw provider payload for audit and separately model
fixture-specific availability snapshots if justified.

Injuries are not derivable from scores, standings or fixture metadata. They
should not be re-requested in response to an end-user page view; a scheduled
backend job near relevant kickoff times is the appropriate future pattern.
