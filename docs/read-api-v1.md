# FastAPI Read API v1

Status: **implemented backend contract**
Base path: `/web/v1`

## Scope and guarantees

This is an internal browser-facing read contract. Every request opens a
PostgreSQL transaction with `SET TRANSACTION READ ONLY`; it does not perform
Supabase DML, call API-Football, invoke an importer, calculate a prediction,
or expose live data.

Responses are stable DTOs, not direct PostgreSQL rows. They are public-domain,
non-user-specific responses; no cache header is committed yet, but the
deterministic IDs, fixture ordering, and finalized-score semantics allow later
historical response caching without changing the payload shape.

## Endpoints

### `GET /web/v1/seasons/{season_id}/fixtures`

Query parameters:

- `limit`: 1–500, default `100`;
- `offset`: zero-based, default `0`.

Fixtures are deterministically ordered by `kickoff_at ASC, id ASC`.
`final_score` is present only for a completed **and finalized** fixture.
Scheduled or still-unfinalized fixtures return `final_score: null`; the API
does not expose a mutable post-match score as final data.

Example:

```text
GET /web/v1/seasons/3/fixtures?limit=100&offset=0
```

```json
{
  "season_id": 3,
  "fixtures": [{
    "id": 103,
    "season_id": 3,
    "kickoff_at": "2024-08-16T19:00:00Z",
    "round_label": "Regular Season - 1",
    "lifecycle_state": "completed",
    "home_team": {"id": 10, "name": "Home"},
    "away_team": {"id": 20, "name": "Away"},
    "final_score": {"home": 2, "away": 1}
  }],
  "pagination": {"total": 380, "limit": 100, "offset": 0, "next_offset": 100}
}
```

### `GET /web/v1/teams/{team_id}/analytics`

Required query parameter: `season_id`.

Optional query parameters:

- `scope`: `overall`, `home`, or `away`; default `overall`;
- `window`: `5`, `10`, `15`, or `20`; default `10`.

The endpoint returns one selected historical window. It contains W/D/L, PPG,
goals, averages, xG/xGA, shooting, possession, corners/cards, clean sheets,
failed-to-score, BTTS, total-goal rates, and streaks. Nullable source metrics
retain `value: null` and a separate `sample_size`; they are not converted to
zero.

```text
GET /web/v1/teams/10/analytics?season_id=3&scope=home&window=10
```

### `GET /web/v1/fixtures/{fixture_id}/analytics`

Query parameter: `window` (`5`, `10`, `15`, or `20`; default `10`).

The response contains fixture metadata and factual pre-match comparison
inputs:

- `home.overall` and `home.venue_split` (home-only);
- `away.overall` and `away.venue_split` (away-only);
- `historical_cutoff_at`, exactly the target fixture's kickoff.

The target fixture itself, a fixture at the same kickoff, and a later fixture
are excluded: all history obeys `historical.kickoff_at < target.kickoff_at`.
This endpoint returns analytics, never a win probability or prediction.

```text
GET /web/v1/fixtures/103/analytics?window=5
```

## Error semantics

| Condition | Status | `detail.code` |
| --- | --- | --- |
| Unknown season | 404 | `season_not_found` |
| Unknown team or team absent from requested season | 404 | `team_not_found_in_season` |
| Unknown fixture | 404 | `fixture_not_found` |
| Team has no completed fixture in selected season | 422 | `team_has_no_completed_fixture_in_season` |
| Unsupported `scope`, `window`, invalid pagination/query type | 422 | FastAPI validation detail, or `invalid_window` |

The selected URLs intentionally contain no `fixture_id + season_id` pair, so a
fixture/season mismatch cannot be silently accepted. Fixture analytics derives
the season from the canonical fixture; team analytics rejects a team that is
not enrolled in the requested season.

## Future compatibility

All identifiers are internal IDs. The contract is season-scoped and has no EPL
or year-specific constant, so additional leagues and seasons are data additions.
Future scanner endpoints can reuse the same DTO families and read-only service
boundary. Players, lineups, odds, authentication, premium gating, frontend,
and caching infrastructure are separate stages.
