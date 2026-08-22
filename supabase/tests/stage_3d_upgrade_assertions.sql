\set ON_ERROR_STOP on

DO $$
DECLARE
    mismatch_count integer;
BEGIN
    WITH current_fingerprints AS (
        SELECT 'football.leagues' AS relation_name, count(*) AS row_count,
            md5(coalesce(string_agg(row_data, '' ORDER BY row_data), '')) AS digest
        FROM (SELECT concat_ws('|', id, name, country_name, logo_url, flag_url) row_data FROM football.leagues) rows
        UNION ALL
        SELECT 'football.seasons', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', id, league_id, start_year, label, starts_on, ends_on) row_data FROM football.seasons) rows
        UNION ALL
        SELECT 'football.teams', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', id, name, code, country_name, founded_year, is_national, logo_url) row_data FROM football.teams) rows
        UNION ALL
        SELECT 'football.fixtures', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', id, season_id, home_team_id, away_team_id, venue_id, round_label,
            kickoff_at, source_timezone, referee_name, lifecycle_state, home_goals, away_goals,
            home_halftime_goals, away_halftime_goals, home_fulltime_goals, away_fulltime_goals,
            home_extratime_goals, away_extratime_goals, home_penalty_goals, away_penalty_goals,
            terminal_status_observed_at, result_available_at, availability_basis,
            result_finalized_at, first_seen_at, last_seen_at, last_source_fetch_id) row_data FROM football.fixtures) rows
        UNION ALL
        SELECT 'football.fixture_team_statistics', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', fixture_id, team_id, shots_on_goal, shots_off_goal, total_shots,
            blocked_shots, shots_inside_box, shots_outside_box, fouls, corner_kicks, offsides,
            yellow_cards, red_cards, goalkeeper_saves, total_passes, passes_accurate,
            possession_pct, pass_accuracy_pct, expected_goals, goals_prevented, extra_metrics,
            mapping_version, observed_at, available_at, availability_basis,
            last_source_fetch_id, finalized_at) row_data FROM football.fixture_team_statistics) rows
        UNION ALL
        SELECT 'source.fixture_provider_refs', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', provider_id, external_id, fixture_id, first_seen_at, last_seen_at) row_data FROM source.fixture_provider_refs) rows
        UNION ALL
        SELECT 'source.league_provider_refs', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', provider_id, external_id, league_id, first_seen_at, last_seen_at) row_data FROM source.league_provider_refs) rows
        UNION ALL
        SELECT 'source.season_provider_refs', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', provider_id, league_external_id, external_season, season_id, first_seen_at, last_seen_at) row_data FROM source.season_provider_refs) rows
        UNION ALL
        SELECT 'source.team_provider_refs', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', provider_id, external_id, team_id, first_seen_at, last_seen_at) row_data FROM source.team_provider_refs) rows
        UNION ALL
        SELECT 'football.season_teams', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', season_id, team_id, default_venue_id, first_seen_at, last_seen_at, last_source_fetch_id) row_data FROM football.season_teams) rows
        UNION ALL
        SELECT 'football.standings_snapshots', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', id, season_id, captured_at, source_fetch_id, group_count, ingest_txid, created_at) row_data FROM football.standings_snapshots) rows
        UNION ALL
        SELECT 'football.standings_snapshot_groups', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', snapshot_id, group_index, group_name, row_count) row_data FROM football.standings_snapshot_groups) rows
        UNION ALL
        SELECT 'football.standings_snapshot_rows', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', snapshot_id, group_index, team_id, rank, points, goals_diff,
            form, status, description, played, wins, draws, losses, goals_for, goals_against,
            home_played, home_wins, home_draws, home_losses, home_goals_for, home_goals_against,
            away_played, away_wins, away_draws, away_losses, away_goals_for, away_goals_against,
            provider_updated_at, created_at) row_data FROM football.standings_snapshot_rows) rows
        UNION ALL
        SELECT 'source.provider_fetches', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', id, provider_id, endpoint, request_params, purpose, request_started_at,
            response_received_at, http_status, outcome, provider_results, paging_current,
            paging_total, encode(content_sha256, 'hex'), normalized_at, subject_fixture_id,
            subject_season_id, subject_team_id) row_data FROM source.provider_fetches) rows
        UNION ALL
        SELECT 'source.provider_raw_payloads', count(*), md5(coalesce(string_agg(row_data, '' ORDER BY row_data), ''))
        FROM (SELECT concat_ws('|', fetch_id, encode(inline_body, 'hex'), object_key, byte_count,
            content_type, content_encoding, retention_class, expires_at, purged_at) row_data FROM source.provider_raw_payloads) rows
    )
    SELECT count(*) INTO mismatch_count
    FROM public.stage_3d_preservation_fingerprints before
    JOIN current_fingerprints after USING (relation_name)
    WHERE before.row_count <> after.row_count OR before.digest <> after.digest;

    IF mismatch_count <> 0 THEN
        RAISE EXCEPTION 'legacy preservation fingerprint changed';
    END IF;

    IF (SELECT count(*) FROM football.leagues) <> 1
       OR (SELECT count(*) FROM football.seasons) <> 1
       OR (SELECT count(*) FROM football.teams) <> 20
       OR (SELECT count(*) FROM football.fixtures) <> 380
       OR (SELECT count(*) FROM football.fixture_team_statistics) <> 188
       OR (SELECT count(*) FROM source.fixture_provider_refs) <> 380
       OR (SELECT count(*) FROM source.fixture_provider_status) <> 380 THEN
        RAISE EXCEPTION 'EPL 2024 expected counts are not preserved/backfilled';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM source.country_provider_refs country_ref
        JOIN source.providers provider ON provider.id = country_ref.provider_id
        JOIN football.countries country ON country.id = country_ref.country_id
        WHERE provider.code = 'api-football'
          AND country_ref.external_code = 'GB-ENG'
          AND country.name = 'England'
    ) THEN
        RAISE EXCEPTION 'provider-specific GB-ENG country mapping is absent';
    END IF;

    IF EXISTS (
        SELECT 1 FROM football.leagues
        WHERE country_name <> 'England'
           OR competition_type <> 'league'
           OR country_id IS NULL
    ) OR (SELECT count(*) FROM football.teams WHERE country_name = 'England' AND country_id IS NOT NULL) <> 20 THEN
        RAISE EXCEPTION 'country backfill or denormalized compatibility failed';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM source.fixture_provider_status status
        JOIN source.fixture_status_code_mappings mapping
          ON mapping.provider_id = status.provider_id
         AND mapping.external_code = status.status_code
        JOIN football.fixtures fixture ON fixture.id = status.fixture_id
        WHERE status.status_code <> 'FT'
           OR mapping.canonical_state <> fixture.lifecycle_state
    ) THEN
        RAISE EXCEPTION 'exact provider status backfill is inconsistent';
    END IF;

    IF (
        SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema = 'football'
          AND table_name = 'leagues'
          AND column_name IN ('country_id', 'competition_type')
          AND is_nullable = 'NO'
    ) <> 2 THEN
        RAISE EXCEPTION 'league NOT NULL constraints are incomplete';
    END IF;

    IF has_table_privilege('anon', 'football.countries', 'SELECT,INSERT,UPDATE,DELETE')
       OR has_table_privilege('authenticated', 'source.fixture_provider_status', 'SELECT,INSERT,UPDATE,DELETE') THEN
        RAISE EXCEPTION 'anon/authenticated received direct privileges';
    END IF;
END
$$;

-- Duplicate provider country mappings are blocked.
DO $$
BEGIN
    BEGIN
        INSERT INTO source.country_provider_refs (provider_id, external_code, country_id)
        SELECT provider.id, 'GB-ENG', country.id
        FROM source.providers provider
        CROSS JOIN football.countries country
        WHERE provider.code = 'api-football' AND country.name = 'England';
        RAISE EXCEPTION 'duplicate country mapping unexpectedly succeeded';
    EXCEPTION WHEN unique_violation THEN NULL;
    END;
END
$$;

-- Extensible reviewed canonical values are accepted; provider casing is not.
INSERT INTO football.countries (name) VALUES ('Spain');
INSERT INTO source.country_provider_refs (provider_id, external_code, country_id)
SELECT provider.id, 'ES', country.id
FROM source.providers provider CROSS JOIN football.countries country
WHERE provider.code = 'api-football' AND country.name = 'Spain';

INSERT INTO football.leagues (name, country_name, country_id, competition_type)
SELECT 'La Liga', 'Spain', id, 'league' FROM football.countries WHERE name = 'Spain';
INSERT INTO source.league_provider_refs (provider_id, external_id, league_id)
SELECT provider.id, '140', league.id
FROM source.providers provider CROSS JOIN football.leagues league
WHERE provider.code = 'api-football' AND league.name = 'La Liga';
INSERT INTO football.seasons (league_id, start_year, label)
SELECT id, year, year || '/' || right((year + 1)::text, 2)
FROM football.leagues CROSS JOIN (VALUES (2024), (2025)) years(year)
WHERE name = 'La Liga';
INSERT INTO source.season_provider_refs (provider_id, league_external_id, external_season, season_id)
SELECT provider.id, '140', season.start_year, season.id
FROM source.providers provider
CROSS JOIN football.seasons season
JOIN football.leagues league ON league.id = season.league_id
WHERE provider.code = 'api-football' AND league.name = 'La Liga';

DO $$
BEGIN
    BEGIN
        INSERT INTO football.leagues (name, country_id, competition_type)
        SELECT 'Invalid Case Competition', id, 'League'
        FROM football.countries WHERE name = 'Spain';
        RAISE EXCEPTION 'uppercase competition type unexpectedly succeeded';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
END
$$;

-- Coverage is sourced from a provider /leagues fetch and is append-only.
INSERT INTO source.provider_fetches (
    provider_id, endpoint, request_params, purpose, request_started_at,
    response_received_at, http_status, outcome, provider_results,
    subject_season_id
)
SELECT provider.id, '/leagues', jsonb_build_object('id', 140, 'season', 2025),
    'research', '2026-08-22 10:00:00+00', '2026-08-22 10:00:01+00',
    200, 'success', 1, season.id
FROM source.providers provider
CROSS JOIN football.seasons season
JOIN football.leagues league ON league.id = season.league_id
WHERE provider.code = 'api-football' AND league.name = 'La Liga' AND season.start_year = 2025;

DO $$
BEGIN
    BEGIN
        INSERT INTO source.season_coverage_snapshots (
            provider_id, season_id, captured_at, fixture_statistics_supported,
            lineups_supported, standings_supported, injuries_supported,
            mapping_version, source_fetch_id
        )
        SELECT provider_fetch.provider_id, provider_fetch.subject_season_id,
            provider_fetch.response_received_at, true, true, true, true,
            'api-football-v1', provider_fetch.id
        FROM source.provider_fetches provider_fetch
        WHERE provider_fetch.endpoint = '/fixtures'
          AND provider_fetch.subject_season_id IS NOT NULL
        LIMIT 1;
        RAISE EXCEPTION 'coverage backed by a non-/leagues fetch unexpectedly succeeded';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
END
$$;

INSERT INTO source.season_coverage_snapshots (
    provider_id, season_id, captured_at, fixture_statistics_supported,
    lineups_supported, standings_supported, injuries_supported,
    mapping_version, source_fetch_id
)
SELECT provider_fetch.provider_id, provider_fetch.subject_season_id, provider_fetch.response_received_at,
    true, true, true, true, 'api-football-v1', provider_fetch.id
FROM source.provider_fetches provider_fetch
WHERE provider_fetch.endpoint = '/leagues';

DO $$
BEGIN
    BEGIN
        UPDATE source.season_coverage_snapshots SET injuries_supported = false;
        RAISE EXCEPTION 'coverage snapshot update unexpectedly succeeded';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;
END
$$;

DO $$
BEGIN
    BEGIN
        UPDATE source.provider_fetches
        SET response_received_at = response_received_at + interval '1 second'
        WHERE endpoint = '/leagues';
        RAISE EXCEPTION 'referenced coverage fetch metadata unexpectedly changed';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;
END
$$;

-- A second league/season provides a fixture used to prove latest-only status.
INSERT INTO football.teams (name, country_name, country_id)
SELECT team_names.team_name, 'Spain', country.id
FROM (VALUES ('Madrid Test'), ('Barcelona Test')) team_names(team_name)
CROSS JOIN football.countries country
WHERE country.name = 'Spain';
INSERT INTO source.team_provider_refs (provider_id, external_id, team_id)
SELECT provider.id, (3000 + team.id)::text, team.id
FROM source.providers provider CROSS JOIN football.teams team
WHERE provider.code = 'api-football' AND team.country_name = 'Spain';
INSERT INTO football.season_teams (season_id, team_id)
SELECT season.id, team.id
FROM football.seasons season
JOIN football.leagues league ON league.id = season.league_id
CROSS JOIN football.teams team
WHERE league.name = 'La Liga' AND season.start_year = 2025 AND team.country_name = 'Spain';

INSERT INTO football.fixtures (
    season_id, home_team_id, away_team_id, kickoff_at, lifecycle_state,
    availability_basis, first_seen_at, last_seen_at
)
SELECT season.id, min(team.id), max(team.id), '2026-08-22 12:00:00+00',
    'scheduled', 'observed', '2026-08-22 08:00:00+00', '2026-08-22 08:00:00+00'
FROM football.seasons season
JOIN football.leagues league ON league.id = season.league_id
CROSS JOIN football.teams team
WHERE league.name = 'La Liga' AND season.start_year = 2025 AND team.country_name = 'Spain'
GROUP BY season.id;

INSERT INTO source.fixture_provider_refs (provider_id, external_id, fixture_id)
SELECT provider.id, '990001', fixture.id
FROM source.providers provider CROSS JOIN football.fixtures fixture
JOIN football.seasons season ON season.id = fixture.season_id
JOIN football.leagues league ON league.id = season.league_id
WHERE provider.code = 'api-football' AND league.name = 'La Liga' AND season.start_year = 2025;

INSERT INTO source.provider_fetches (
    provider_id, endpoint, request_params, purpose, request_started_at,
    response_received_at, http_status, outcome, provider_results,
    subject_fixture_id, subject_season_id
)
SELECT provider.id, '/fixtures', jsonb_build_object('id', 990001), 'bootstrap',
    observation.requested_at, observation.received_at, 200, 'success', 1,
    fixture.id, season.id
FROM source.providers provider
CROSS JOIN football.fixtures fixture
JOIN football.seasons season ON season.id = fixture.season_id
JOIN football.leagues league ON league.id = season.league_id
CROSS JOIN (VALUES
    ('2026-08-22 08:59:00+00'::timestamptz, '2026-08-22 09:00:00+00'::timestamptz),
    ('2026-08-22 14:59:00+00'::timestamptz, '2026-08-22 15:00:00+00'::timestamptz)
) observation(requested_at, received_at)
WHERE provider.code = 'api-football' AND league.name = 'La Liga' AND season.start_year = 2025;

INSERT INTO source.fixture_provider_status (
    provider_id, fixture_id, status_code, observed_at, source_fetch_id
)
SELECT provider_fetch.provider_id, provider_fetch.subject_fixture_id, 'NS', provider_fetch.response_received_at, provider_fetch.id
FROM source.provider_fetches provider_fetch
WHERE provider_fetch.endpoint = '/fixtures' AND provider_fetch.request_params @> '{"id": 990001}' AND provider_fetch.response_received_at = '2026-08-22 09:00:00+00';

INSERT INTO football.fixtures (
    season_id, home_team_id, away_team_id, kickoff_at, lifecycle_state,
    availability_basis, first_seen_at, last_seen_at
)
SELECT season.id, min(team.id), max(team.id), '2026-08-23 12:00:00+00',
    'scheduled', 'observed', '2026-08-22 08:00:00+00', '2026-08-22 08:00:00+00'
FROM football.seasons season
JOIN football.leagues league ON league.id = season.league_id
CROSS JOIN football.teams team
WHERE league.name = 'La Liga' AND season.start_year = 2025 AND team.country_name = 'Spain'
GROUP BY season.id;

INSERT INTO source.fixture_provider_refs (provider_id, external_id, fixture_id)
SELECT provider.id, '990002', fixture.id
FROM source.providers provider CROSS JOIN football.fixtures fixture
JOIN football.seasons season ON season.id = fixture.season_id
JOIN football.leagues league ON league.id = season.league_id
WHERE provider.code = 'api-football'
  AND league.name = 'La Liga'
  AND season.start_year = 2025
  AND fixture.kickoff_at = '2026-08-23 12:00:00+00';

DO $$
BEGIN
    BEGIN
        INSERT INTO source.fixture_provider_status (
            provider_id, fixture_id, status_code, observed_at, source_fetch_id
        )
        SELECT ref.provider_id, ref.fixture_id, 'NS', provider_fetch.response_received_at,
            provider_fetch.id
        FROM source.fixture_provider_refs ref
        JOIN source.provider_fetches provider_fetch
          ON provider_fetch.provider_id = ref.provider_id
        WHERE ref.external_id = '990002'
          AND provider_fetch.request_params @> '{"id": 990001}'
          AND provider_fetch.response_received_at = '2026-08-22 09:00:00+00';
        RAISE EXCEPTION 'fixture-bound fetch was attributed to another fixture in the same season';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
END
$$;

DO $$
BEGIN
    BEGIN
        UPDATE football.fixtures fixture
        SET lifecycle_state = 'completed',
            home_goals = 2,
            away_goals = 1,
            home_fulltime_goals = 2,
            away_fulltime_goals = 1,
            terminal_status_observed_at = provider_fetch.response_received_at,
            result_available_at = provider_fetch.response_received_at,
            last_seen_at = provider_fetch.response_received_at,
            last_source_fetch_id = provider_fetch.id
        FROM source.provider_fetches provider_fetch
        WHERE fixture.id = provider_fetch.subject_fixture_id
          AND provider_fetch.request_params @> '{"id": 990001}'
          AND provider_fetch.response_received_at = '2026-08-22 15:00:00+00';

        SET CONSTRAINTS football.fixture_status_lifecycle_consistency_guard IMMEDIATE;
        RAISE EXCEPTION 'fixture lifecycle changed without exact provider status';
    EXCEPTION WHEN check_violation THEN NULL;
    END;

    IF NOT EXISTS (
        SELECT 1
        FROM football.fixtures fixture
        JOIN source.fixture_provider_refs ref ON ref.fixture_id = fixture.id
        WHERE ref.external_id = '990001'
          AND fixture.lifecycle_state = 'scheduled'
    ) THEN
        RAISE EXCEPTION 'failed lifecycle-only transaction was not rolled back';
    END IF;
END
$$;

BEGIN;

UPDATE football.fixtures fixture
SET lifecycle_state = 'completed',
    home_goals = 2,
    away_goals = 1,
    home_fulltime_goals = 2,
    away_fulltime_goals = 1,
    terminal_status_observed_at = provider_fetch.response_received_at,
    result_available_at = provider_fetch.response_received_at,
    last_seen_at = provider_fetch.response_received_at,
    last_source_fetch_id = provider_fetch.id
FROM source.provider_fetches provider_fetch
WHERE fixture.id = provider_fetch.subject_fixture_id
  AND provider_fetch.request_params @> '{"id": 990001}'
  AND provider_fetch.response_received_at = '2026-08-22 15:00:00+00';

INSERT INTO source.fixture_provider_status (
    provider_id, fixture_id, status_code, observed_at, source_fetch_id
)
SELECT provider_fetch.provider_id, provider_fetch.subject_fixture_id, 'FT', provider_fetch.response_received_at, provider_fetch.id
FROM source.provider_fetches provider_fetch
WHERE provider_fetch.request_params @> '{"id": 990001}'
  AND provider_fetch.response_received_at = '2026-08-22 15:00:00+00'
ON CONFLICT (provider_id, fixture_id) DO UPDATE
SET status_code = EXCLUDED.status_code,
    observed_at = EXCLUDED.observed_at,
    source_fetch_id = EXCLUDED.source_fetch_id;

COMMIT;

DO $$
DECLARE
    winning_fetch_id bigint;
BEGIN
    SELECT id INTO winning_fetch_id
    FROM source.provider_fetches
    WHERE request_params @> '{"id": 990001}'
      AND response_received_at = '2026-08-22 15:00:00+00';

    IF NOT EXISTS (
        SELECT 1 FROM source.fixture_provider_status
        WHERE status_code = 'FT'
          AND observed_at = '2026-08-22 15:00:00+00'
          AND source_fetch_id = winning_fetch_id
    ) THEN
        RAISE EXCEPTION 'NS to FT update did not preserve winning fetch provenance';
    END IF;

    BEGIN
        UPDATE source.fixture_provider_status status
        SET observed_at = provider_fetch.response_received_at,
            source_fetch_id = provider_fetch.id
        FROM source.provider_fetches provider_fetch
        WHERE status.fixture_id = provider_fetch.subject_fixture_id
          AND provider_fetch.request_params @> '{"id": 990001}'
          AND provider_fetch.response_received_at = '2026-08-22 09:00:00+00';
        RAISE EXCEPTION 'stale status overwrite unexpectedly succeeded';
    EXCEPTION WHEN check_violation THEN NULL;
    END;

    IF NOT EXISTS (
        SELECT 1 FROM source.fixture_provider_status
        WHERE status_code = 'FT' AND source_fetch_id = winning_fetch_id
    ) THEN
        RAISE EXCEPTION 'stale rejection changed current fixture status';
    END IF;

    BEGIN
        UPDATE source.provider_fetches
        SET response_received_at = response_received_at + interval '1 second'
        WHERE id = winning_fetch_id;
        RAISE EXCEPTION 'referenced status fetch metadata unexpectedly changed';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;
END
$$;

-- Failed work is rolled back at the statement subtransaction boundary.
DO $$
BEGIN
    BEGIN
        INSERT INTO football.countries (name) VALUES ('Rollback Test Country');
        INSERT INTO football.countries (name) VALUES ('rollback test country');
        RAISE EXCEPTION 'duplicate active country unexpectedly succeeded';
    EXCEPTION WHEN unique_violation THEN NULL;
    END;

    IF EXISTS (SELECT 1 FROM football.countries WHERE lower(name) = 'rollback test country') THEN
        RAISE EXCEPTION 'failed country transaction left a partial row';
    END IF;
END
$$;

BEGIN;
SET CONSTRAINTS ALL IMMEDIATE;
COMMIT;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM source.fixture_provider_refs ref
        LEFT JOIN football.fixtures fixture ON fixture.id = ref.fixture_id
        WHERE fixture.id IS NULL
    ) OR EXISTS (
        SELECT 1
        FROM football.fixture_team_statistics statistic
        LEFT JOIN football.fixtures fixture ON fixture.id = statistic.fixture_id
        LEFT JOIN football.teams team ON team.id = statistic.team_id
        WHERE fixture.id IS NULL OR team.id IS NULL
    ) THEN
        RAISE EXCEPTION 'orphan rows detected';
    END IF;
END
$$;
