\set ON_ERROR_STOP on

DO $$
DECLARE
    missing_count integer;
    role_name text;
    relation_name text;
BEGIN
    SELECT count(*) INTO missing_count
    FROM (VALUES
        ('football', 'countries'),
        ('source', 'country_provider_refs'),
        ('source', 'season_coverage_snapshots'),
        ('source', 'fixture_status_code_mappings'),
        ('source', 'fixture_provider_status')
    ) expected(schema_name, table_name)
    WHERE to_regclass(format('%I.%I', expected.schema_name, expected.table_name)) IS NULL;

    IF missing_count <> 0 THEN
        RAISE EXCEPTION 'approved additive tables are missing';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_class relation
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname IN ('source', 'football')
          AND relation.relkind = 'r'
          AND relation.relname ~ '(odds|event|timeline|live|player_statistics|prediction)'
    ) THEN
        RAISE EXCEPTION 'migration introduced an excluded-domain table';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_class relation
        JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname IN ('source', 'football')
          AND relation.relname IN (
              'countries', 'country_provider_refs', 'season_coverage_snapshots',
              'fixture_status_code_mappings', 'fixture_provider_status'
          )
          AND NOT relation.relrowsecurity
    ) THEN
        RAISE EXCEPTION 'RLS is missing from an additive table';
    END IF;

    FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated'] LOOP
        IF has_schema_privilege(role_name, 'source', 'USAGE')
           OR has_schema_privilege(role_name, 'football', 'USAGE') THEN
            RAISE EXCEPTION '% unexpectedly has canonical schema usage', role_name;
        END IF;

        FOREACH relation_name IN ARRAY ARRAY[
            'football.countries',
            'source.country_provider_refs',
            'source.season_coverage_snapshots',
            'source.fixture_status_code_mappings',
            'source.fixture_provider_status'
        ] LOOP
            IF has_table_privilege(
                role_name,
                relation_name,
                'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'
            ) THEN
                RAISE EXCEPTION '% unexpectedly has privileges on %', role_name, relation_name;
            END IF;
        END LOOP;

        IF has_function_privilege(role_name, 'source.guard_immutable_snapshot()', 'EXECUTE')
           OR has_function_privilege(role_name, 'source.guard_season_coverage_snapshot()', 'EXECUTE')
           OR has_function_privilege(role_name, 'source.guard_referenced_provider_fetch_response()', 'EXECUTE')
           OR has_function_privilege(role_name, 'source.guard_fixture_provider_status()', 'EXECUTE')
           OR has_function_privilege(role_name, 'source.assert_fixture_status_lifecycle_consistency()', 'EXECUTE') THEN
            RAISE EXCEPTION '% unexpectedly has additive function execution privilege', role_name;
        END IF;

        IF EXISTS (
            SELECT 1
            FROM pg_class seq
            JOIN pg_namespace namespace ON namespace.oid = seq.relnamespace
            WHERE seq.relkind = 'S'
              AND namespace.nspname IN ('source', 'football')
              AND has_sequence_privilege(
                  role_name,
                  format('%I.%I', namespace.nspname, seq.relname),
                  'USAGE,SELECT,UPDATE'
              )
        ) THEN
            RAISE EXCEPTION '% unexpectedly has canonical sequence privileges', role_name;
        END IF;
    END LOOP;
END
$$;
