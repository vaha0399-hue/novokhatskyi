\set ON_ERROR_STOP on

DO $$
DECLARE
    provider_one smallint;
    provider_two smallint;
    league_country_id bigint;
    home_country_id bigint;
    away_country_id bigint;
    league_id bigint;
    season_id bigint;
    home_team_id bigint;
    away_team_id bigint;
    target_fixture_id bigint;
    fetch_one_id bigint;
    fetch_two_id bigint;
    fetch_unknown_kickoff_id bigint;
    fetch_wrong_time_id bigint;
BEGIN
    IF to_regclass('source.fixture_schedule_observations') IS NULL THEN
        RAISE EXCEPTION 'fixture schedule observation table is missing';
    END IF;

    IF (
        SELECT count(*)
        FROM information_schema.columns
        WHERE table_schema='source'
          AND table_name='fixture_schedule_observations'
          AND (
              (column_name='provider_id' AND data_type='smallint' AND is_nullable='NO')
              OR (column_name='fixture_id' AND data_type='bigint' AND is_nullable='NO')
              OR (column_name='source_fetch_id' AND data_type='bigint' AND is_nullable='NO')
              OR (column_name='observed_kickoff_at' AND data_type='timestamp with time zone' AND is_nullable='YES')
              OR (column_name='observed_at' AND data_type='timestamp with time zone' AND is_nullable='NO')
              OR (column_name='created_at' AND data_type='timestamp with time zone' AND is_nullable='NO')
          )
    ) <> 6 THEN
        RAISE EXCEPTION 'fixture schedule observation columns do not match the contract';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='source.fixture_schedule_observations'::regclass
          AND conname='fixture_schedule_observations_pkey'
          AND contype='p'
          AND pg_get_constraintdef(oid)='PRIMARY KEY (provider_id, fixture_id, source_fetch_id)'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='source.fixture_schedule_observations'::regclass
          AND conname='fixture_schedule_observations_provider_fixture_fk'
          AND contype='f'
          AND pg_get_constraintdef(oid)='FOREIGN KEY (provider_id, fixture_id) REFERENCES source.fixture_provider_refs(provider_id, fixture_id) ON DELETE RESTRICT'
    ) OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='source.fixture_schedule_observations'::regclass
          AND conname='fixture_schedule_observations_fetch_provider_fk'
          AND contype='f'
          AND pg_get_constraintdef(oid)='FOREIGN KEY (source_fetch_id, provider_id, observed_at) REFERENCES source.provider_fetches(id, provider_id, response_received_at) ON DELETE RESTRICT'
    ) THEN
        RAISE EXCEPTION 'fixture schedule observation keys are incomplete';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_class
        WHERE oid='source.fixture_schedule_observations'::regclass
          AND relrowsecurity
    ) THEN
        RAISE EXCEPTION 'fixture schedule observations require RLS';
    END IF;

    IF has_table_privilege('anon','source.fixture_schedule_observations','SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
       OR has_table_privilege('authenticated','source.fixture_schedule_observations','SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER') THEN
        RAISE EXCEPTION 'fixture schedule observations are exposed to API roles';
    END IF;
    IF has_function_privilege('anon','source.guard_fixture_schedule_observation()','EXECUTE')
       OR has_function_privilege('authenticated','source.guard_fixture_schedule_observation()','EXECUTE') THEN
        RAISE EXCEPTION 'fixture schedule observation guards are exposed to API roles';
    END IF;

    INSERT INTO source.providers(code,name)
    VALUES('q05-observation-one','Q05 observation provider one') RETURNING id INTO provider_one;
    INSERT INTO source.providers(code,name)
    VALUES('q05-observation-two','Q05 observation provider two') RETURNING id INTO provider_two;
    INSERT INTO football.countries(name) VALUES('Q05 observation league country') RETURNING id INTO league_country_id;
    INSERT INTO football.countries(name) VALUES('Q05 observation home country') RETURNING id INTO home_country_id;
    INSERT INTO football.countries(name) VALUES('Q05 observation away country') RETURNING id INTO away_country_id;
    INSERT INTO football.leagues(name,country_id,competition_type)
    VALUES('Q05 cross-country competition',league_country_id,'cup') RETURNING id INTO league_id;
    INSERT INTO football.seasons(league_id,start_year,label)
    VALUES(league_id,2026,'Q05 observation season') RETURNING id INTO season_id;
    INSERT INTO football.teams(name,country_id)
    VALUES('Q05 foreign home',home_country_id) RETURNING id INTO home_team_id;
    INSERT INTO football.teams(name,country_id)
    VALUES('Q05 foreign away',away_country_id) RETURNING id INTO away_team_id;
    INSERT INTO football.season_teams(season_id,team_id)
    VALUES(season_id,home_team_id),(season_id,away_team_id);
    INSERT INTO football.fixtures(
        season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state,
        first_seen_at,last_seen_at
    ) VALUES (
        season_id,home_team_id,away_team_id,'2026-09-10 12:00:00+00','scheduled',
        '2026-09-10 08:00:00+00','2026-09-10 08:00:00+00'
    ) RETURNING id INTO target_fixture_id;
    INSERT INTO source.fixture_provider_refs(provider_id,external_id,fixture_id)
    VALUES(provider_one,'q05-observation-fixture',target_fixture_id);
    INSERT INTO source.provider_fetches(
        provider_id,endpoint,purpose,request_started_at,response_received_at,
        http_status,outcome,normalized_at,subject_fixture_id
    ) VALUES (
        provider_one,'/fixtures','scheduled_refresh','2026-09-10 08:09:59+00',
        '2026-09-10 08:10:00+00',200,'success','2026-09-10 08:10:01+00',target_fixture_id
    ) RETURNING id INTO fetch_one_id;
    INSERT INTO source.provider_fetches(
        provider_id,endpoint,purpose,request_started_at,response_received_at,
        http_status,outcome,normalized_at
    ) VALUES (
        provider_two,'/fixtures','scheduled_refresh','2026-09-10 08:19:59+00',
        '2026-09-10 08:20:00+00',200,'success','2026-09-10 08:20:01+00'
    ) RETURNING id INTO fetch_two_id;
    INSERT INTO source.provider_fetches(
        provider_id,endpoint,purpose,request_started_at,response_received_at,
        http_status,outcome,normalized_at,subject_fixture_id
    ) VALUES (
        provider_one,'/fixtures','scheduled_refresh','2026-09-10 08:29:59+00',
        '2026-09-10 08:30:00+00',200,'success','2026-09-10 08:30:01+00',target_fixture_id
    ) RETURNING id INTO fetch_unknown_kickoff_id;
    INSERT INTO source.provider_fetches(
        provider_id,endpoint,purpose,request_started_at,response_received_at,
        http_status,outcome,normalized_at,subject_fixture_id
    ) VALUES (
        provider_one,'/fixtures','scheduled_refresh','2026-09-10 08:39:59+00',
        '2026-09-10 08:40:00+00',200,'success','2026-09-10 08:40:01+00',target_fixture_id
    ) RETURNING id INTO fetch_wrong_time_id;

    INSERT INTO source.fixture_schedule_observations(
        provider_id,fixture_id,source_fetch_id,observed_kickoff_at,observed_at
    ) VALUES (
        provider_one,target_fixture_id,fetch_one_id,'2026-09-10 12:00:00+00','2026-09-10 08:10:00+00'
    );
    INSERT INTO source.fixture_schedule_observations(
        provider_id,fixture_id,source_fetch_id,observed_kickoff_at,observed_at
    ) VALUES (
        provider_one,target_fixture_id,fetch_unknown_kickoff_id,NULL,'2026-09-10 08:30:00+00'
    );

    IF NOT EXISTS (
        SELECT 1 FROM source.fixture_schedule_observations
        WHERE provider_id=provider_one
          AND fixture_id=target_fixture_id
          AND source_fetch_id=fetch_unknown_kickoff_id
          AND observed_kickoff_at IS NULL
    ) THEN
        RAISE EXCEPTION 'unknown observed kickoff was not retained explicitly';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM source.fixture_schedule_observations observation
        JOIN football.fixtures fixture ON fixture.id=observation.fixture_id
        JOIN football.seasons season ON season.id=fixture.season_id
        JOIN football.leagues league ON league.id=season.league_id
        JOIN football.teams home_team ON home_team.id=fixture.home_team_id
        JOIN football.teams away_team ON away_team.id=fixture.away_team_id
        WHERE observation.provider_id=provider_one
          AND observation.source_fetch_id=fetch_one_id
          AND league.country_id<>home_team.country_id
          AND league.country_id<>away_team.country_id
    ) THEN
        RAISE EXCEPTION 'cross-country fixture observation was not preserved';
    END IF;

    BEGIN
        INSERT INTO source.fixture_schedule_observations(
            provider_id,fixture_id,source_fetch_id,observed_kickoff_at,observed_at
        ) VALUES (
        provider_one,target_fixture_id,fetch_one_id,'2026-09-10 12:00:00+00','2026-09-10 08:10:00+00'
        );
        RAISE EXCEPTION 'duplicate fixture observation was accepted';
    EXCEPTION WHEN unique_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO source.fixture_schedule_observations(
            provider_id,fixture_id,source_fetch_id,observed_kickoff_at,observed_at
        ) VALUES (
            provider_one,target_fixture_id,fetch_wrong_time_id,'2026-09-10 12:00:00+00','2026-09-10 08:40:01+00'
        );
        RAISE EXCEPTION 'observation accepted a replay/write timestamp instead of response receipt time';
    EXCEPTION WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO source.fixture_schedule_observations(
            provider_id,fixture_id,source_fetch_id,observed_kickoff_at,observed_at
        ) VALUES (
        provider_two,target_fixture_id,fetch_two_id,NULL,'2026-09-10 08:20:00+00'
        );
        RAISE EXCEPTION 'observation accepted a provider without a fixture mapping';
    EXCEPTION WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO source.fixture_schedule_observations(
            provider_id,fixture_id,source_fetch_id,observed_kickoff_at,observed_at
        ) VALUES (
        provider_one,target_fixture_id,fetch_two_id,NULL,'2026-09-10 08:20:00+00'
        );
        RAISE EXCEPTION 'observation accepted a fetch from another provider';
    EXCEPTION WHEN foreign_key_violation THEN NULL;
    END;

    BEGIN
        UPDATE source.fixture_schedule_observations
        SET observed_kickoff_at='2026-09-10 12:01:00+00'
        WHERE provider_id=provider_one
          AND fixture_id=target_fixture_id
          AND source_fetch_id=fetch_one_id;
        RAISE EXCEPTION 'fixture schedule observation was mutable';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;

    BEGIN
        DELETE FROM source.fixture_schedule_observations
        WHERE provider_id=provider_one
          AND fixture_id=target_fixture_id
          AND source_fetch_id=fetch_one_id;
        RAISE EXCEPTION 'fixture schedule observation was deletable';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;

    BEGIN
        UPDATE source.provider_fetches
        SET response_received_at=response_received_at + interval '1 second'
        WHERE id=fetch_one_id;
        RAISE EXCEPTION 'referenced response receipt time was mutable';
    EXCEPTION WHEN foreign_key_violation THEN NULL;
    END;
END
$$;
