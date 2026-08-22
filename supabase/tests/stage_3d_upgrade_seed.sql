\set ON_ERROR_STOP on

INSERT INTO source.providers (code, name)
VALUES ('api-football', 'API-Football');

INSERT INTO football.leagues (name, country_name, logo_url, flag_url)
VALUES ('Premier League', 'England', 'https://example.test/epl.png', 'https://example.test/england.svg');

INSERT INTO source.league_provider_refs (provider_id, external_id, league_id)
SELECT provider.id, '39', league.id
FROM source.providers provider
CROSS JOIN football.leagues league
WHERE provider.code = 'api-football' AND league.name = 'Premier League';

INSERT INTO football.seasons (league_id, start_year, label)
SELECT id, 2024, '2024/25' FROM football.leagues WHERE name = 'Premier League';

INSERT INTO source.season_provider_refs (
    provider_id, league_external_id, external_season, season_id
)
SELECT provider.id, '39', 2024, season.id
FROM source.providers provider
CROSS JOIN football.seasons season
WHERE provider.code = 'api-football' AND season.start_year = 2024;

INSERT INTO football.teams (
    name, code, country_name, founded_year, is_national, logo_url
)
SELECT
    'EPL Team ' || value,
    'T' || lpad(value::text, 2, '0'),
    'England',
    1880 + value,
    false,
    'https://example.test/team-' || value || '.png'
FROM generate_series(1, 20) AS value;

INSERT INTO source.team_provider_refs (provider_id, external_id, team_id)
SELECT provider.id, (1000 + team.id)::text, team.id
FROM source.providers provider
CROSS JOIN football.teams team
WHERE provider.code = 'api-football';

INSERT INTO football.season_teams (season_id, team_id)
SELECT season.id, team.id
FROM football.seasons season
CROSS JOIN football.teams team
WHERE season.start_year = 2024;

INSERT INTO source.provider_fetches (
    provider_id,
    endpoint,
    request_params,
    purpose,
    request_started_at,
    response_received_at,
    http_status,
    outcome,
    provider_results,
    paging_current,
    paging_total,
    content_sha256,
    normalized_at,
    subject_season_id
)
SELECT
    provider.id,
    '/fixtures',
    '{"league": 39, "season": 2024}'::jsonb,
    'bootstrap',
    '2025-05-26 11:59:00+00',
    '2025-05-26 12:00:00+00',
    200,
    'success',
    380,
    1,
    1,
    decode(repeat('ab', 32), 'hex'),
    '2025-05-26 12:01:00+00',
    season.id
FROM source.providers provider
CROSS JOIN football.seasons season
WHERE provider.code = 'api-football' AND season.start_year = 2024;

INSERT INTO football.fixtures (
    season_id,
    home_team_id,
    away_team_id,
    round_label,
    kickoff_at,
    source_timezone,
    lifecycle_state,
    home_goals,
    away_goals,
    home_fulltime_goals,
    away_fulltime_goals,
    terminal_status_observed_at,
    result_available_at,
    availability_basis,
    first_seen_at,
    last_seen_at,
    last_source_fetch_id
)
SELECT
    season.id,
    ((fixture_number - 1) % 20) + 1,
    (fixture_number % 20) + 1,
    'Regular Season - ' || (((fixture_number - 1) / 10) + 1),
    '2024-08-01 12:00:00+00'::timestamptz + fixture_number * interval '4 hours',
    'UTC',
    'completed',
    fixture_number % 4,
    (fixture_number + 1) % 4,
    fixture_number % 4,
    (fixture_number + 1) % 4,
    '2025-05-26 12:00:00+00',
    '2025-05-26 12:00:00+00',
    'observed',
    '2024-06-01 00:00:00+00',
    '2025-05-26 12:00:00+00',
    provider_fetch.id
FROM generate_series(1, 380) AS fixture_number
CROSS JOIN football.seasons season
CROSS JOIN source.provider_fetches provider_fetch
WHERE season.start_year = 2024
  AND provider_fetch.endpoint = '/fixtures';

INSERT INTO source.fixture_provider_refs (provider_id, external_id, fixture_id)
SELECT provider.id, (2000000 + fixture.id)::text, fixture.id
FROM source.providers provider
CROSS JOIN football.fixtures fixture
WHERE provider.code = 'api-football';

WITH payload AS (
    SELECT jsonb_build_object(
        'get', 'fixtures',
        'parameters', jsonb_build_object('league', '39', 'season', '2024'),
        'errors', jsonb_build_object(),
        'results', 380,
        'paging', jsonb_build_object('current', 1, 'total', 1),
        'response', jsonb_agg(
            jsonb_build_object(
                'fixture', jsonb_build_object(
                    'id', ref.external_id::bigint,
                    'status', jsonb_build_object(
                        'long', 'Match Finished',
                        'short', 'FT',
                        'elapsed', 90,
                        'extra', NULL
                    )
                )
            ) ORDER BY ref.external_id::bigint
        )
    ) AS body
    FROM source.fixture_provider_refs ref
), encoded AS (
    SELECT convert_to(body::text, 'UTF8') AS bytes FROM payload
)
INSERT INTO source.provider_raw_payloads (
    fetch_id,
    inline_body,
    byte_count,
    retention_class,
    expires_at
)
SELECT
    provider_fetch.id,
    encoded.bytes,
    octet_length(encoded.bytes),
    'standard',
    clock_timestamp() + interval '30 days'
FROM source.provider_fetches provider_fetch
CROSS JOIN encoded;

INSERT INTO football.fixture_team_statistics (
    fixture_id,
    team_id,
    shots_on_goal,
    total_shots,
    possession_pct,
    expected_goals,
    mapping_version,
    observed_at,
    available_at,
    availability_basis,
    finalized_at
)
SELECT
    fixture.id,
    participant.team_id,
    (fixture.id % 8)::integer,
    (fixture.id % 15 + 5)::integer,
    CASE participant.side WHEN 'home' THEN 55.25 ELSE 44.75 END,
    CASE participant.side WHEN 'home' THEN 1.275 ELSE 0.925 END,
    'api-football-v1',
    fixture.kickoff_at + interval '3 hours',
    fixture.kickoff_at + interval '3 hours',
    'reconstructed_conservative',
    fixture.kickoff_at + interval '3 hours'
FROM football.fixtures fixture
CROSS JOIN LATERAL (
    VALUES ('home', fixture.home_team_id), ('away', fixture.away_team_id)
) AS participant(side, team_id)
WHERE fixture.id <= 94;

CREATE TABLE public.stage_3d_preservation_fingerprints (
    relation_name text PRIMARY KEY,
    row_count bigint NOT NULL,
    digest text NOT NULL
);

INSERT INTO public.stage_3d_preservation_fingerprints
SELECT 'football.leagues', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', id, name, country_name, logo_url, flag_url) AS row_data
    FROM football.leagues
) rows
UNION ALL
SELECT 'football.seasons', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', id, league_id, start_year, label, starts_on, ends_on) AS row_data
    FROM football.seasons
) rows
UNION ALL
SELECT 'football.teams', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', id, name, code, country_name, founded_year, is_national, logo_url) AS row_data
    FROM football.teams
) rows
UNION ALL
SELECT 'football.fixtures', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', id, season_id, home_team_id, away_team_id, venue_id, round_label,
        kickoff_at, source_timezone, referee_name, lifecycle_state, home_goals, away_goals,
        home_halftime_goals, away_halftime_goals, home_fulltime_goals, away_fulltime_goals,
        home_extratime_goals, away_extratime_goals, home_penalty_goals, away_penalty_goals,
        terminal_status_observed_at, result_available_at, availability_basis,
        result_finalized_at, first_seen_at, last_seen_at, last_source_fetch_id) AS row_data
    FROM football.fixtures
) rows
UNION ALL
SELECT 'football.fixture_team_statistics', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', fixture_id, team_id, shots_on_goal, shots_off_goal, total_shots,
        blocked_shots, shots_inside_box, shots_outside_box, fouls, corner_kicks, offsides,
        yellow_cards, red_cards, goalkeeper_saves, total_passes, passes_accurate,
        possession_pct, pass_accuracy_pct, expected_goals, goals_prevented, extra_metrics,
        mapping_version, observed_at, available_at, availability_basis,
        last_source_fetch_id, finalized_at) AS row_data
    FROM football.fixture_team_statistics
) rows
UNION ALL
SELECT 'source.fixture_provider_refs', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', provider_id, external_id, fixture_id, first_seen_at, last_seen_at) AS row_data
    FROM source.fixture_provider_refs
) rows
UNION ALL
SELECT 'source.league_provider_refs', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', provider_id, external_id, league_id, first_seen_at, last_seen_at) AS row_data
    FROM source.league_provider_refs
) rows
UNION ALL
SELECT 'source.season_provider_refs', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', provider_id, league_external_id, external_season, season_id, first_seen_at, last_seen_at) AS row_data
    FROM source.season_provider_refs
) rows
UNION ALL
SELECT 'source.team_provider_refs', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', provider_id, external_id, team_id, first_seen_at, last_seen_at) AS row_data
    FROM source.team_provider_refs
) rows
UNION ALL
SELECT 'football.season_teams', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', season_id, team_id, default_venue_id, first_seen_at, last_seen_at, last_source_fetch_id) AS row_data
    FROM football.season_teams
) rows
UNION ALL
SELECT 'football.standings_snapshots', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', id, season_id, captured_at, source_fetch_id, group_count, ingest_txid, created_at) AS row_data
    FROM football.standings_snapshots
) rows
UNION ALL
SELECT 'football.standings_snapshot_groups', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', snapshot_id, group_index, group_name, row_count) AS row_data
    FROM football.standings_snapshot_groups
) rows
UNION ALL
SELECT 'football.standings_snapshot_rows', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', snapshot_id, group_index, team_id, rank, points, goals_diff,
        form, status, description, played, wins, draws, losses, goals_for, goals_against,
        home_played, home_wins, home_draws, home_losses, home_goals_for, home_goals_against,
        away_played, away_wins, away_draws, away_losses, away_goals_for, away_goals_against,
        provider_updated_at, created_at) AS row_data
    FROM football.standings_snapshot_rows
) rows
UNION ALL
SELECT 'source.provider_fetches', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', id, provider_id, endpoint, request_params, purpose, request_started_at,
        response_received_at, http_status, outcome, provider_results, paging_current,
        paging_total, encode(content_sha256, 'hex'), normalized_at, subject_fixture_id,
        subject_season_id, subject_team_id) AS row_data
    FROM source.provider_fetches
) rows
UNION ALL
SELECT 'source.provider_raw_payloads', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
FROM (
    SELECT concat_ws('|', fetch_id, encode(inline_body, 'hex'), object_key, byte_count,
        content_type, content_encoding, retention_class, expires_at, purged_at) AS row_data
    FROM source.provider_raw_payloads
) rows;
