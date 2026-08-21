# Fixtures (`GET /fixtures`)

## Scope and evidence

- **Observed source:** `samples/api-football/fixtures.raw.json`, collected at
  `2026-08-21T18:15:51.833783+00:00` with `league=39` and **research season
  `2024`**. The separate request record is
  `samples/api-football/fixtures.request.json`.
- **Production target remains `season=2026`.** The observations below prove
  the contract returned for the historical research response only; they do not
  establish that all 2026 fixtures, statuses, or nullable fields will be the
  same.
- **Completeness of this sample:** `results=380`, `paging.current=1`, and
  `paging.total=1`. It is a complete finished 2024/25 Premier League schedule
  in this response, not a representative pre-match or live response.

## Observed response contract

Top-level object (all values observed):

| Field | Observed type/value |
| --- | --- |
| `get` | string: `"fixtures"` |
| `parameters` | object; values are strings (`"39"`, `"2024"`) even though the collector request metadata uses integers |
| `errors` | empty array |
| `results` | integer: `380` |
| `paging` | object: integer `current=1`, `total=1` |
| `response` | array of 380 fixture objects |

Every observed fixture object has exactly `fixture`, `league`, `teams`,
`goals`, and `score`; no record-level arrays, missing keys, or additional keys
were seen. This does **not** make those keys non-nullable across other seasons,
competitions, or fixture states.

### `fixture`

| Field | Observed type / nullability | Notes |
| --- | --- | --- |
| `id` | integer, never null | Stable external fixture identifier in this sample; e.g. `1208021`. |
| `referee` | string, never null | Present for all 380 finished matches; do not infer this pre-match guarantee. |
| `timezone` | string, never null | Always `UTC` in this sample. |
| `date` | ISO-8601 offset datetime string, never null | Range: `2024-08-16T19:00:00+00:00` through `2025-05-25T15:00:00+00:00`. |
| `timestamp` | integer, never null | Unix-second companion to `date`. |
| `periods` | object, never null | `first` and `second` are integer Unix seconds in all 380 records. |
| `venue` | object, never null | `id` integer, `name`/`city` strings; all populated here. 20 unique venues. |
| `status` | object, never null | `long`, `short`, `elapsed`, `extra`; details below. |

The `fixture.date`, `fixture.timestamp`, and `fixture.timezone` fields describe
the scheduled/start time representation. Preserve both source fields until the
normalisation rules are decided; a database timestamp may be derived later,
but should not replace raw evidence.

### `league`

`league.id` is integer `39` and `league.season` integer `2024` in every
record. `name`, `country`, `logo`, `flag`, and `round` are strings;
`standings` is boolean `true`. The sample has 38 distinct round labels, from
the regular-season schedule. League identity is therefore repeated per
fixture, but `league.id + league.season` is the relevant external context for
this research data.

### `teams`

`teams.home` and `teams.away` are objects with:

| Field | Observed type / nullability |
| --- | --- |
| `id` | integer, never null |
| `name` | string, never null |
| `logo` | string URL, never null |
| `winner` | boolean or `null` |

The schedule has 20 distinct team IDs (each appears 19 times at home and 19
times away). `winner=true/false` occurred for 287 matches; both values were
`null` for 93 draws. Examples: fixture `1208021` is a home win, `1208022` an
away win, and `1208026` a draw. Thus `winner` is not safely boolean-only.

### Goals and score

`goals.home` and `goals.away` are integer and non-null in this finished-only
sample (ranges 0–7 and 0–6 respectively). `score` always has four objects:

- `halftime.home` / `away`: integers, non-null;
- `fulltime.home` / `away`: integers, non-null;
- `extratime.home` / `away`: present but **null in all 380**;
- `penalty.home` / `away`: present but **null in all 380**.

Premier League 2024/25 fixtures in this response were regular-season `FT`
matches, so it provides no positive sample of extra-time or penalty values.
The `null` shape must be retained; it must not be coerced to zero.

## Statuses and lifecycle evidence

All 380 fixtures have one observed status combination:

```json
{"long":"Match Finished","short":"FT","elapsed":90,"extra":null | integer}
```

`status.extra` is `null` in 53 records and an integer in 327 (observed range
3–15). The sample alone does not establish its semantic meaning, so it must
be stored/analyzed as a nullable value rather than treated as duration or
score data without the upstream definition.

No `NS`, `TBD`, postponed, cancelled, live, extra-time, or penalty status was
returned. Consequently, this sample proves **post-match** availability of
scores and outcome flags, but cannot prove the exact pre-match or live shape:

| Lifecycle statement | Evidence status |
| --- | --- |
| Finished fixture has populated scores, periods, referee, venue, and outcome flags/null draw flags | Observed in all 380 `FT` records |
| Future fixture has no lineup/statistics/score fields | Not observed by this endpoint sample; do not infer |
| Live or delayed status values and their field changes | Not observed |
| Fixture time, referee, venue, round, status, and scores may change as upstream data is updated | Operational inference; collector must treat later API responses as authoritative, not assume immutability |

## External relationships and IDs

Observed relations within each fixture record:

```text
fixture.id
  -> league.id + league.season
  -> teams.home.id and teams.away.id
  -> fixture.venue.id
```

The source repeats human-readable names and media URLs beside IDs. IDs should
remain the correlation keys; names/logos/venue text are upstream attributes
that can change independently. Do not derive identity from names or URLs.

## Official documentation boundary

The endpoint and standard wrapper names are documented by
[API-Football v3 documentation](https://www.api-football.com/documentation-v3).
This page records only what the saved response establishes. In particular,
the documentation may enumerate statuses and response shapes that are absent
from this finished-only research snapshot; those are **official possibilities,
not sample-confirmed facts**. Runtime ingestion for the future production
season 2026 must accept documented nullable/status variation and retain raw
payloads for reconciliation.

## Implications for the next design stage (not a schema)

1. Preserve `fixture_id`, `league_id`, `season`, home/away `team_id`, and
   `venue_id` as external identifiers.
2. Model home and away as role-bearing references, not an unordered pair.
3. Keep raw status values (`short`, `long`, `elapsed`, `extra`) and score
   components separate; do not reduce them to a single result field.
4. Treat `winner`, extra-time, and penalty values as nullable from the outset.
5. This response is sufficient to discuss the finished-fixture core, but
   insufficient by itself to finalise pre-match/live handling. The dedicated
   lineups, injuries, and statistics samples plus a future-season collection
   when access permits remain necessary evidence.
