\set ON_ERROR_STOP on
BEGIN;

DO $$
DECLARE
    provider_one smallint; provider_two smallint; country_id bigint; league_id bigint;
    season_one bigint; season_two bigint; season_three bigint; instance_id bigint;
    update_instance_failed boolean := false;
    duplicate_failed boolean := false; mapping_fk_failed boolean := false; season_fk_failed boolean := false;
    coverage_missing_state boolean := false; coverage_state_type boolean := false; coverage_state_value boolean := false;
    coverage_missing_date boolean := false; coverage_date_type boolean := false; coverage_basic_date boolean := false;
    coverage_infinity_date boolean := false; coverage_relative_date boolean := false; coverage_invalid_date boolean := false;
    interval_empty boolean := false; interval_missing_value boolean := false; interval_value_type boolean := false;
    interval_zero boolean := false; interval_negative boolean := false; interval_missing_unit boolean := false;
    interval_unit_type boolean := false; interval_unit_value boolean := false;
BEGIN
    IF to_regclass('ops.competition_sync_policies') IS NULL
       OR NOT (SELECT relrowsecurity FROM pg_class WHERE oid = 'ops.competition_sync_policies'::regclass) THEN
        RAISE EXCEPTION 'competition sync policy table or RLS missing';
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
        provider_id, season_id, enabled, allowed_work_types, coverage, refresh_intervals, priority, history_depth_seasons
    ) VALUES
        (provider_one, season_one, true, ARRAY['coverage_refresh', 'fixtures_refresh'],
         '{"coverage_refresh":{"state":"unknown","observed_on":"2026-09-08"},"fixtures_refresh":{"state":"covered","observed_on":"2026-09-08"}}',
         '{"coverage_refresh":{"value":25,"unit":"second"},"fixtures_refresh":{"value":1,"unit":"hour"}}', 20, 2),
        (provider_one, season_two, true, ARRAY['coverage_refresh', 'fixtures_refresh'],
         '{"coverage_refresh":{"state":"unknown","observed_on":"2026-09-08"},"fixtures_refresh":{"state":"not_covered","observed_on":"2026-09-08"}}',
         '{"coverage_refresh":{"value":1,"unit":"day"},"fixtures_refresh":{"value":1,"unit":"day"}}', 10, 1);
    IF (SELECT count(*) FROM ops.competition_sync_policies WHERE provider_id = provider_one) <> 2 THEN
        RAISE EXCEPTION 'two seasons of one competition cannot be enabled together';
    END IF;
    IF (SELECT refresh_intervals->'coverage_refresh'->>'unit' FROM ops.competition_sync_policies
        WHERE provider_id=provider_one AND season_id=season_one) <> 'second' THEN
        RAISE EXCEPTION 'positive seconds interval was not preserved';
    END IF;

    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,refresh_intervals)
        VALUES (provider_one,season_one,ARRAY['coverage_refresh'],'{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN unique_violation THEN duplicate_failed := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,refresh_intervals)
        VALUES (provider_two,season_one,ARRAY['coverage_refresh'],'{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN foreign_key_violation THEN mapping_fk_failed := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,refresh_intervals)
        VALUES (provider_one,999999999,ARRAY['coverage_refresh'],'{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN foreign_key_violation THEN season_fk_failed := true; END;

    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,coverage,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"observed_on":"2026-09-08"}}','{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN check_violation THEN coverage_missing_state := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,coverage,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"state":true,"observed_on":"2026-09-08"}}','{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN check_violation THEN coverage_state_type := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,coverage,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"state":"maybe","observed_on":"2026-09-08"}}','{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN check_violation THEN coverage_state_value := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,coverage,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"state":"unknown"}}','{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN check_violation THEN coverage_missing_date := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,coverage,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"state":"unknown","observed_on":123}}','{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN check_violation THEN coverage_date_type := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,coverage,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"state":"unknown","observed_on":"20260908"}}','{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN check_violation THEN coverage_basic_date := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,coverage,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"state":"unknown","observed_on":"infinity"}}','{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN check_violation THEN coverage_infinity_date := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,coverage,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"state":"unknown","observed_on":"tomorrow"}}','{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN check_violation THEN coverage_relative_date := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,coverage,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"state":"unknown","observed_on":"2026-02-30"}}','{"coverage_refresh":{"value":1,"unit":"hour"}}');
    EXCEPTION WHEN check_violation THEN coverage_invalid_date := true; END;

    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{}');
    EXCEPTION WHEN check_violation THEN interval_empty := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"unit":"second"}}');
    EXCEPTION WHEN check_violation THEN interval_missing_value := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"value":"25","unit":"second"}}');
    EXCEPTION WHEN check_violation THEN interval_value_type := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"value":0,"unit":"second"}}');
    EXCEPTION WHEN check_violation THEN interval_zero := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"value":-1,"unit":"second"}}');
    EXCEPTION WHEN check_violation THEN interval_negative := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"value":25}}');
    EXCEPTION WHEN check_violation THEN interval_missing_unit := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"value":25,"unit":true}}');
    EXCEPTION WHEN check_violation THEN interval_unit_type := true; END;
    BEGIN INSERT INTO ops.competition_sync_policies(provider_id,season_id,allowed_work_types,refresh_intervals)
        VALUES (provider_one,season_three,ARRAY['coverage_refresh'],'{"coverage_refresh":{"value":25,"unit":"month"}}');
    EXCEPTION WHEN check_violation THEN interval_unit_value := true; END;

    IF NOT (duplicate_failed AND mapping_fk_failed AND season_fk_failed
        AND coverage_missing_state AND coverage_state_type AND coverage_state_value
        AND coverage_missing_date AND coverage_date_type AND coverage_basic_date
        AND coverage_infinity_date AND coverage_relative_date AND coverage_invalid_date
        AND interval_empty AND interval_missing_value AND interval_value_type AND interval_zero
        AND interval_negative AND interval_missing_unit AND interval_unit_type AND interval_unit_value) THEN
        RAISE EXCEPTION 'policy JSON, FK, unique, or interval validation is incomplete';
    END IF;

    SELECT policy_instance_id INTO instance_id FROM ops.competition_sync_policies
      WHERE provider_id=provider_one AND season_id=season_one;
    BEGIN
        UPDATE ops.competition_sync_policies SET policy_instance_id=DEFAULT
          WHERE provider_id=provider_one AND season_id=season_one;
    EXCEPTION WHEN check_violation THEN update_instance_failed := true;
    END;
    IF NOT update_instance_failed THEN
        RAISE EXCEPTION 'policy instance update was accepted';
    END IF;
    UPDATE ops.competition_sync_policies SET priority=21 WHERE provider_id=provider_one AND season_id=season_one;
    IF (SELECT policy_instance_id FROM ops.competition_sync_policies WHERE provider_id=provider_one AND season_id=season_one) <> instance_id
       OR (SELECT policy_version FROM ops.competition_sync_policies WHERE provider_id=provider_one AND season_id=season_one) <> 2 THEN
        RAISE EXCEPTION 'policy instance must be immutable and updates must advance version';
    END IF;
END $$;
ROLLBACK;
