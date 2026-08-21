# Team statistics: observed contract

## Scope and evidence

This document records one **real research response**, not a promised production
schema.  The request was `GET /teams/statistics?league=39&season=2024&team=33`
for Premier League research season 2024; the production target remains 2026.
See [request metadata](../../samples/api-football/team-statistics.request.json)
and the unmodified [raw response](../../samples/api-football/team-statistics.raw.json).

## Wrapper and response shape (observed)

The top-level wrapper has `get` (string), `parameters` (object), `errors`
(empty array), `results` (integer `11`), `paging` (object with integer
`current` and `total`) and `response` (**object**, not array).  In this sample,
`response` has 11 top-level sections, as does `results`; that does not prove
that `results` should be used as an object-section count in all responses.

`response.league` contains `id` (int `39`), `season` (int `2024`), and
name/country/logo/flag strings. `response.team` contains `id` (int `33`),
name and logo strings. These are the observed external league/team identifiers.

## Nested data and exact observed types

- `form` is one compact string (`WLL...`), not an array of fixtures. Its order,
  cutoff and update timing must not be inferred from this one response.
- `fixtures.{played,wins,draws,loses}.{home,away,total}` are integers.
- `goals.{for,against}.total.{home,away,total}` are integers; corresponding
  `average` values are decimal **strings** (`"1.2"`, `"1.5"`), not JSON
  numbers.
- `goals.{for,against}.minute` has observed period-name keys `0-15`, `16-30`,
  `31-45`, `46-60`, `61-75`, `76-90`, `91-105`, `106-120`. Each child has
  `total` (int or `null`) and `percentage` (percent-suffixed string or `null`).
  `106-120` is `{total: null, percentage: null}` in both goal directions.
- `goals.{for,against}.under_over` is an object keyed by threshold strings
  (`"0.5"` … `"4.5"` here); each `over`/`under` value is an integer.
- `biggest.streak` and `biggest.goals` are integers. Match-score values at
  `biggest.{wins,loses}.{home,away}` are strings such as `"4-0"`, not a pair
  of integers.
- `clean_sheet` and `failed_to_score` expose integer `home`, `away`, `total`.
- `penalty.total` and `penalty.{scored,missed}.total` are integers;
  `penalty.{scored,missed}.percentage` is a percent string (`"100.00%"` and
  `"0%"` demonstrate non-uniform precision).
- `lineups` is a variable-length array: three objects in this sample, each
  `{formation: string, played: int}`.
- `cards.{yellow,red}` uses the same observed period keys and
  `{total, percentage}` shape as goal minutes. Both fields can be `null`; e.g.
  most red-card buckets and `106-120` are null in this response.

No key is absent in this single response, but that is not evidence that the API
will always include every key. Consumers must tolerate missing keys as well as
the observed explicit nulls.

## Relationships, lifecycle and interpretation

The response is scoped by the request triple `(league_id, season, team_id)`.
It gives aggregate home/away/total values but does **not** identify the fixtures
from which they were made. It is therefore a derived upstream aggregate, not a
replacement for fixture-level history.

**Observed fact:** this completed 2024 season response has totals for 38
matches. **Inference:** values such as form, totals, averages, streaks and
formation counts can change while the season is in progress and should be
treated as a time-dependent snapshot if ever retained. The sample supplies no
pre-match snapshot, so it does not prove availability before a future fixture.

## What to derive from our own fixture data

After importing canonical finished fixtures and scores, calculate Last 5/10,
home/away form, W/D/L rates, goals for/against, averages, clean sheets,
failed-to-score counts, season aggregates and table-strength inputs locally.
This avoids an API call per user request and allows an explicit cutoff time.
The compact upstream `form` string is useful only as a cross-check during
research; do not make it the authoritative feature source.

Minute buckets, cards, penalties and historical formation frequencies need
event/lineup-level data to reproduce and are not derivable from the current
fixtures sample alone.
