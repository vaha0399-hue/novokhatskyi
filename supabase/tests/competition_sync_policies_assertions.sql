\set ON_ERROR_STOP on
BEGIN;

DO $$
DECLARE
    provider_one smallint;
    provider_two smallint;
    country_id bigint;
    league_id bigint;
    season_one bigint;
    season_two bigint;
    season_three bigint;
    duplicate_failed boolean := false;
    mapping_fk_failed boolean := false;
    season_fk_failed boolean := false;
    invalid_failed boolean := false;
BEGIN
    IF to_regclass('ops.competition_sync_policies') IS NULL THEN
        RAISE EXCEPTION 'competition sync policy table missing';
    END IF;
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = 'ops.competition_sync_policies'::regclass) THEN
        RAISE EXCEPTION 'competition sync policy RLS missing';
    END IF;
    IF has_table_privilege('anon', 'ops.competition_sync_policies', 'select,insert,update,delete')
       OR has_table_privilege('authenticated', 'ops.competition_sync_policies', 'select,insert,update,delete') THEN
        RAISE EXCEPTION 'anon/authenticated can manage competition sync policies';
    END IF;

    INSERT INTO source.providers(code, name) VALUES ('policy-test-one', 'Policy test one') RETURNING id INTO provider_one;
    INSERT INTO source.providers(code, name) VALUES ('policy-test-two', 'Policy test two') RETURNING id INTO provider_two;
    INSERT INTO football.countries(name) VALUES ('Policy test country') RETURNING id INTO country_id;
    INSERT INTO football.leagues(name, country_id, competition_type)
        VALUES ('Policy test competition', country_id, 'league') RETURNING id INTO league_id;
    INSERT INTO source.league_provider_refs(provider_id, external_id, league_id)
        VALUES (provider_one, 'policy-league', league_id);
    INSERT INTO football.seasons(league_id, start_year, label)
        VALUES (league_id, 2024, '2024/25') RETURNING id INTO season_one;
    INSERT INTO football.seasons(league_id, start_year, label)
        VALUES (league_id, 2025, '2025/26') RETURNING id INTO season_two;
    INSERT INTO football.seasons(league_id, start_year, label)
        VALUES (league_id, 2026, '2026/27') RETURNING id INTO season_three;
    INSERT INTO source.season_provider_refs(provider_id, league_external_id, external_season, season_id)
        VALUES (provider_one, 'policy-league', 2024, season_one),
               (provider_one, 'policy-league', 2025, season_two),
               (provider_one, 'policy-league', 2026, season_three);

    INSERT INTO ops.competition_sync_policies(
        provider_id, season_id, enabled, allowed_work_types, coverage, refresh_intervals,
        priority, history_depth_seasons
    ) VALUES
        (provider_one, season_one, true, ARRAY['coverage_refresh', 'fixtures_refresh'],
         '{"coverage_refresh":{"state":"unknown","observed_on":"2026-09-08"},"fixtures_refresh":{"state":"covered","observed_on":"2026-09-08"}}'::jsonb,
         '{"coverage_refresh":{"value":6,"unit":"hour"},"fixtures_refresh":{"value":1,"unit":"hour"}}'::jsonb, 20, 2),
        (provider_one, season_two, true, ARRAY['coverage_refresh', 'fixtures_refresh'],
         '{"coverage_refresh":{"state":"unknown","observed_on":"2026-09-08"},"fixtures_refresh":{"state":"not_covered","observed_on":"2026-09-08"}}'::jsonb,
         '{"coverage_refresh":{"value":1,"unit":"day"},"fixtures_refresh":{"value":1,"unit":"day"}}'::jsonb, 10, 1);

    IF (SELECT count(*) FROM ops.competition_sync_policies WHERE provider_id = provider_one) <> 2 THEN
        RAISE EXCEPTION 'two seasons of one competition cannot be enabled together';
    END IF;
    IF (SELECT coverage->'fixtures_refresh'->>'state' FROM ops.competition_sync_policies WHERE provider_id=provider_one AND season_id=season_one) <> 'covered'
       OR (SELECT coverage->'fixtures_refresh'->>'state' FROM ops.competition_sync_policies WHERE provider_id=provider_one AND season_id=season_two) <> 'not_covered' THEN
        RAISE EXCEPTION 'coverage state was not preserved';
    END IF;

    BEGIN
        INSERT INTO ops.competition_sync_policies(provider_id, season_id, allowed_work_types, refresh_intervals)
            VALUES (provider_one, season_one, ARRAY['coverage_refresh'], '{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN unique_violation THEN duplicate_failed := true;
    END;
    BEGIN
        INSERT INTO ops.competition_sync_policies(provider_id, season_id, allowed_work_types, refresh_intervals)
            VALUES (provider_two, season_one, ARRAY['coverage_refresh'], '{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN foreign_key_violation THEN mapping_fk_failed := true;
    END;
    BEGIN
        INSERT INTO ops.competition_sync_policies(provider_id, season_id, allowed_work_types, refresh_intervals)
            VALUES (provider_one, 999999999, ARRAY['coverage_refresh'], '{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN foreign_key_violation THEN season_fk_failed := true;
    END;
    BEGIN
        INSERT INTO ops.competition_sync_policies(provider_id, season_id, allowed_work_types, coverage, refresh_intervals)
            VALUES (provider_one, season_three, ARRAY[''], '{"fixtures_refresh":{"state":"unknown"}}', '{"":{"value":0,"unit":"month"}}');
    EXCEPTION WHEN check_violation THEN invalid_failed := true;
    END;
    IF NOT duplicate_failed OR NOT mapping_fk_failed OR NOT season_fk_failed OR NOT invalid_failed THEN
        RAISE EXCEPTION 'policy uniqueness, FK, or CHECK constraint missing';
    END IF;

    UPDATE ops.competition_sync_policies
       SET paused_until = clock_timestamp() + interval '1 hour', pause_reason = 'validation'
     WHERE provider_id = provider_one AND season_id = season_one;
    IF (SELECT policy_version FROM ops.competition_sync_policies WHERE provider_id=provider_one AND season_id=season_one) <> 2 THEN
        RAISE EXCEPTION 'policy updates must advance version';
    END IF;
END $$;
ROLLBACK;
