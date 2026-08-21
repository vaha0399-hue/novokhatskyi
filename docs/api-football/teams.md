# API-Football: teams

## Scope and evidence

This document records only the real response in
[`samples/api-football/teams.raw.json`](../../samples/api-football/teams.raw.json),
from `GET /teams?league=39&season=2024`. Safe request metadata is in
[`teams.request.json`](../../samples/api-football/teams.request.json).

Season `2024` is research-only. The production target remains `2026`; these
observations do not guarantee identical fields, counts, or values then.

## Observed envelope

| Field | Observed type | Observation |
| --- | --- | --- |
| `get` | string | `"teams"` |
| `parameters` | object | `league` and `season` are strings (`"39"`, `"2024"`) in raw JSON, although request metadata recorded numeric inputs. |
| `errors` | array | Empty. |
| `results` | integer | `20`, equal to `response.length`. |
| `paging` | object | `current` and `total` are integers, both `1`. |
| `response` | array | 20 entries. |

No `null` or omitted fields occurred in these 20 response entries. This is a
limited sample observation, not a non-null contract for other queries.

## `response[]`

Every entry has exactly two objects, `team` and `venue`.

### `team`

| Field | Type | Null / missing (20) |
| --- | --- | --- |
| `id` | integer | 0 / 0 |
| `name` | string | 0 / 0 |
| `code` | string | 0 / 0 |
| `country` | string | 0 / 0 |
| `founded` | integer | 0 / 0 |
| `national` | boolean | 0 / 0 |
| `logo` | URL string | 0 / 0 |

All 20 `team.id` values are unique. `national` is `false` for all of them;
retain it as provider boolean rather than infer it from this league.

### `venue`

| Field | Type | Null / missing (20) |
| --- | --- | --- |
| `id` | integer | 0 / 0 |
| `name` | string | 0 / 0 |
| `address` | string | 0 / 0 |
| `city` | string | 0 / 0 |
| `capacity` | integer | 0 / 0 |
| `surface` | string | 0 / 0 |
| `image` | URL string | 0 / 0 |

All 20 `venue.id` values are unique in this response. That observed
one-to-one result must not become a global database constraint without
cross-season/cross-league evidence.

## Stable external-ID candidates and relationships

- `team.id` is the team external ID. It matches `team.id` in every standings
  row in the separately collected standings sample.
- `venue.id` is the venue external ID supplied beside a team here.
- `league_id=39` and `season=2024` come from request context; neither is
  repeated in each response item.

The direct observed relationship is `response[].team` → `response[].venue`.
The samples do not establish venue history, future changes, or global
uniqueness rules.

## Handling notes

- Retain API envelope errors and paging when later ingestion is designed.
- Use numeric IDs, not names, abbreviations, images, or URLs as identity.
- Display and URL fields can change independently of identity.
- This document contains no proposed schema or importer design.
