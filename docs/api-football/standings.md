# API-Football: standings

## Scope and evidence

This document is based only on the real response in
[`samples/api-football/standings.raw.json`](../../samples/api-football/standings.raw.json),
from `GET /standings?league=39&season=2024`. Safe request metadata is in
[`standings.request.json`](../../samples/api-football/standings.request.json).

Season `2024` is research-only; the production target stays `2026`. This is
not a final persistence schema or an availability guarantee.

## Observed envelope

| Field | Observed type | Observation |
| --- | --- | --- |
| `get` | string | `"standings"` |
| `parameters` | object | `league` and `season` are strings (`"39"`, `"2024"`) in raw JSON. |
| `errors` | array | Empty. |
| `results` | integer | `1`, equal to `response.length`. |
| `paging` | object | integer `current=1`, `total=1`. |
| `response` | array | One object. |

`response[0].league` has `id` (integer), `name` (string), `country` (string),
`logo` (URL string), `flag` (URL string), `season` (integer), and `standings`
(array). Returned `league.id=39` and `league.season=2024` match request
context.

## Nested standings structure

`league.standings` is an array of arrays. The sample has one group containing
20 rows. Each row's `group` is `"Premier League"`; retain the outer array
dimension because the API shape permits multiple groups.

| Row field | Type | Null / missing (20) |
| --- | --- | --- |
| `rank` | integer | 0 / 0 |
| `team.id` | integer | 0 / 0 |
| `team.name` | string | 0 / 0 |
| `team.logo` | URL string | 0 / 0 |
| `points` | integer | 0 / 0 |
| `goalsDiff` | integer, including negative | 0 / 0 |
| `group` | string | 0 / 0 |
| `form` | string | 0 / 0 |
| `status` | string | 0 / 0 |
| `description` | string or `null` | 8 / 0 |
| `all`, `home`, `away` | object | 0 / 0 |
| `update` | offset timestamp string | 0 / 0 |

`team.id` matches the teams sample for every one of the 20 rows: this is the
observed cross-endpoint entity link.

Each of `all`, `home`, and `away` has identical observed shape:

```text
played: integer
win: integer
draw: integer
lose: integer
goals.for: integer
goals.against: integer
```

No null or omitted values occur in any of these 60 aggregate blocks. The
goals fields are nested; a client must not expect flattened goal names.

## Variability and time

- `rank`, `points`, `goalsDiff`, aggregate blocks, `form`, `status`, and
  `description` are time-varying standings snapshot data.
- Every row has `update="2025-05-26T00:00:00+00:00"`; treat this as an
  offset-aware provider timestamp, not a local-date-only value.
- `form` is a five-character string in this sample. It is provider output,
  not a substitute for later calculation from completed fixtures.
- `status` is only `"same"` in this terminal-season sample; possible values
  and semantics are not enumerated by this one response.
- `description` is explicitly nullable. Observed non-null labels include
  Champions League, UEFA Europa League, Conference League Qualification, and
  Relegation; eight rows contain literal `null`, which must not be inferred.

## Stable external-ID candidates

- `league.id`
- `league.season`
- `standings[group_index][row_index].team.id`

`rank` is a snapshot position, not a stable external ID. Names, URLs, form,
and descriptions are provider attributes, not identity keys.

## Derived-data boundary (inference)

The API delivers season-to-date aggregates here. In the planned product,
table strength, home/away performance, win/draw rate, average goals, and
last-five form can later be calculated from retained completed fixtures. This
is an architectural inference, not a decision to discard provider standings
snapshots; retention and schema decisions remain deferred to Stage 3.
