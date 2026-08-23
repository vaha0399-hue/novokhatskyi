\set ON_ERROR_STOP on

DO $$
DECLARE
    missing_count integer;
    role_name text;
    relation_name text;
BEGIN
    SELECT count(*) INTO missing_count
    FROM (VALUES
        ('football', 'fixture_historical_lineup_snapshots'),
        ('football', 'fixture_historical_lineups'),
        ('football', 'fixture_historical_lineup_players')
    ) expected(schema_name, table_name)
    WHERE to_regclass(format('%I.%I', expected.schema_name, expected.table_name)) IS NULL;

    IF missing_count <> 0 THEN
        RAISE EXCEPTION 'historical lineup tables are missing';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_enum value
        JOIN pg_type type ON type.oid = value.enumtypid
        JOIN pg_namespace namespace ON namespace.oid = type.typnamespace
        WHERE namespace.nspname = 'source'
          AND type.typname = 'fetch_purpose'
          AND value.enumlabel = 'historical_backfill'
    ) THEN
        RAISE EXCEPTION 'historical_backfill fetch purpose is missing';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_class relation
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname IN ('source', 'football')
          AND relation.relkind = 'r'
          AND relation.relname ~ '(player_statistics|odds|event|timeline|live)'
    ) THEN
        RAISE EXCEPTION 'migration introduced an excluded-domain table';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_class relation
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'football'
          AND relation.relname IN (
              'fixture_historical_lineup_snapshots',
              'fixture_historical_lineups',
              'fixture_historical_lineup_players'
          )
          AND NOT relation.relrowsecurity
    ) THEN
        RAISE EXCEPTION 'RLS is missing from a historical lineup table';
    END IF;

    FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated'] LOOP
        IF has_schema_privilege(role_name, 'football', 'USAGE') THEN
            RAISE EXCEPTION '% unexpectedly has football schema usage', role_name;
        END IF;

        FOREACH relation_name IN ARRAY ARRAY[
            'football.fixture_historical_lineup_snapshots',
            'football.fixture_historical_lineups',
            'football.fixture_historical_lineup_players'
        ] LOOP
            IF has_table_privilege(
                role_name,
                relation_name,
                'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
            ) THEN
                RAISE EXCEPTION '% unexpectedly has privileges on %', role_name, relation_name;
            END IF;
        END LOOP;

        IF has_function_privilege(role_name, 'football.guard_historical_lineup_snapshot()', 'EXECUTE')
           OR has_function_privilege(role_name, 'football.guard_historical_lineup_child()', 'EXECUTE')
           OR has_function_privilege(role_name, 'football.guard_historical_lineup_coach_provider_ref()', 'EXECUTE')
           OR has_function_privilege(role_name, 'football.guard_historical_lineup_player_provider_ref()', 'EXECUTE')
           OR has_function_privilege(role_name, 'football.assert_historical_lineup_snapshot_commit_valid()', 'EXECUTE') THEN
            RAISE EXCEPTION '% unexpectedly has historical-lineup function execution privilege', role_name;
        END IF;
    END LOOP;
END
$$;
