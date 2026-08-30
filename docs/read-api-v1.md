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

Calendar-date endpoints require an IANA timezone supplied by the browser.
PostgreSQL timestamps remain UTC; FastAPI converts the selected local date to
an index-friendly half-open UTC range (`kickoff_at >= start AND kickoff_at <
end`). There is no league or server-timezone hardcode, and daylight-saving
transitions are handled by the timezone database.

## Endpoints

### `GET /web/v1/leagues`

Returns active canonical competitions, ordered deterministically by country,
name, and internal ID. Each DTO includes only presentation-safe canonical
metadata (`id`, `name`, `country_name`, `logo_url`, `competition_type`). It
does not expose provider IDs or provider provenance.

### `GET /web/v1/leagues/{league_id}/seasons`

Returns a league and its canonical seasons in descending `start_year, id`
order. This is the navigation contract for a multi-league, multi-season UI;
the frontend must not hard-code EPL or a particular year.

### `GET /web/v1/matches/leagues`

Required query parameters:

- `date`: selected calendar date in `YYYY-MM-DD` format;
- `timezone`: browser-resolved IANA zone, for example `Asia/Tokyo` or
  `America/Los_Angeles`.

Returns only leagues that have at least one canonical fixture in the selected
user-local day. Each item contains the presentation-safe `LeagueReference` and
`fixture_count`; fixture arrays are deliberately omitted so desktop and mobile
clients can render the first navigation step without downloading all matches.
Results are ordered deterministically by country, league name, and internal
league ID.

```json
{
  "date": "2026-08-30",
  "timezone": "Asia/Tokyo",
  "leagues": [{
    "league": {
      "id": 3,
      "name": "Premier League",
      "country_name": "England",
      "logo_url": null,
      "competition_type": "league"
    },
    "fixture_count": 8
  }]
}
```

### `GET /web/v1/matches`

Required query parameters:

- `date`: selected calendar date in `YYYY-MM-DD` format;
- `league_id`: canonical internal league ID;
- `timezone`: browser-resolved IANA zone.

Returns the selected league and its fixtures for that user-local day, ordered
by `kickoff_at ASC, id ASC`. It reuses `FixtureSummary`, including future,
completed, postponed, cancelled, and other canonical lifecycle states. A known
league with no fixtures returns an empty `fixtures` array; an unknown league
returns `league_not_found`.

### `GET /web/v1/seasons/{season_id}/standings`

Returns the latest retained normalized standings snapshot for a season. The
response includes `captured_at`, all provider grouping rows, and the canonical
team, rank, points, W/D/L, goals, and form fields. It returns 422
`season_standings_not_available` when the known season has no standings
snapshot; it does not fabricate a table from fixture rows.

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

### `GET /web/v1/fixtures/{fixture_id}/statistics`

Returns the completed fixture metadata plus typed, canonical final
team-statistics for its home and away participants. It exposes the reviewed
metric set only:

- shots, shots on goal/off goal/blocked/inside/outside box;
- fouls, corners, offsides, cards, saves, and passes;
- possession and pass accuracy percentages;
- xG and goals prevented.

A side with no normalized statistics row returns `metrics: null`. A nullable
metric within an existing row remains JSON `null`, not `0` or an empty string.
`extra_metrics`, raw payloads, provider fetches, SHA-256 values, and provider
IDs remain internal provenance rather than browser DTO fields.

```text
GET /web/v1/fixtures/103/analytics?window=5
```

## Error semantics

| Condition | Status | `detail.code` |
| --- | --- | --- |
| Unknown season | 404 | `season_not_found` |
| Unknown league | 404 | `league_not_found` |
| Unknown team or team absent from requested season | 404 | `team_not_found_in_season` |
| Unknown fixture | 404 | `fixture_not_found` |
| Unknown browser timezone | 422 | `invalid_timezone` |
| Calendar date has no representable next day | 422 | `invalid_match_date` |
| Team has no completed fixture in selected season | 422 | `team_has_no_completed_fixture_in_season` |
| Known season without a standings snapshot | 422 | `season_standings_not_available` |
| Missing timezone, unsupported `scope`/`window`, or invalid query type | 422 | FastAPI validation detail, `invalid_timezone`, or `invalid_window` |

The selected URLs intentionally contain no `fixture_id + season_id` pair, so a
fixture/season mismatch cannot be silently accepted. Fixture analytics derives
the season from the canonical fixture; team analytics rejects a team that is
not enrolled in the requested season.

## Future compatibility

All identifiers are internal IDs. The contract is season-scoped and has no EPL
or year-specific constant, so additional leagues and seasons are data additions.
Future scanner endpoints can reuse the same DTO families and read-only service
boundary. The Next.js UI is a thin server-side consumer of this contract;
players, lineups, odds, backend JWT gating, premium access, and caching-policy
expansion remain separate stages.
