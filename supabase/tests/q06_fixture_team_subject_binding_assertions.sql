\set ON_ERROR_STOP on
BEGIN;

DO $$
DECLARE
  provider_id smallint;
  country_id bigint;
  league_id bigint;
  season_id bigint;
  home_id bigint;
  away_id bigint;
  foreign_id bigint;
  fixture_id bigint;
  run_id bigint;
  work_item_id bigint;
  blocked boolean;
BEGIN
  INSERT INTO source.providers(code,name)
  VALUES ('q06-fixture-team','Q06 fixture team') RETURNING id INTO provider_id;
  INSERT INTO football.countries(name)
  VALUES ('Q06 fixture team') RETURNING id INTO country_id;
  INSERT INTO football.leagues(name,country_id,competition_type)
  VALUES ('Q06 fixture team',country_id,'league') RETURNING id INTO league_id;
  INSERT INTO source.league_provider_refs(provider_id,external_id,league_id)
  VALUES (provider_id,'39',league_id);
  INSERT INTO football.seasons(league_id,start_year,label)
  VALUES (league_id,2026,'Q06 fixture team') RETURNING id INTO season_id;
  INSERT INTO source.season_provider_refs(provider_id,league_external_id,external_season,season_id)
  VALUES (provider_id,'39',2026,season_id);
  INSERT INTO football.teams(name) VALUES ('Q06 fixture home') RETURNING id INTO home_id;
  INSERT INTO football.teams(name) VALUES ('Q06 fixture away') RETURNING id INTO away_id;
  INSERT INTO football.teams(name) VALUES ('Q06 fixture foreign') RETURNING id INTO foreign_id;
  INSERT INTO football.season_teams(season_id,team_id)
  VALUES (season_id,home_id),(season_id,away_id),(season_id,foreign_id);
  INSERT INTO source.team_provider_refs(provider_id,external_id,team_id)
  VALUES (provider_id,'7',home_id),(provider_id,'8',away_id),(provider_id,'9',foreign_id);
  INSERT INTO football.fixtures(
    season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state,first_seen_at,last_seen_at
  ) VALUES (
    season_id,home_id,away_id,clock_timestamp()+interval '1 day','scheduled',clock_timestamp(),clock_timestamp()
  ) RETURNING id INTO fixture_id;
  INSERT INTO source.fixture_provider_refs(provider_id,external_id,fixture_id)
  VALUES (provider_id,'42',fixture_id);
  INSERT INTO ops.sync_runs(provider_id,operation)
  VALUES (provider_id,'q06-fixture-team') RETURNING id INTO run_id;
  INSERT INTO ops.sync_work_items(
    run_id,scope_key,scope,job_type,stable_key,entity_key,execution_key
  ) VALUES (
    run_id,'q06:fixture-team','{}','fixtures','q06:fixture-team','q06:fixture-team','q06:fixture-team'
  ) RETURNING id INTO work_item_id;

  INSERT INTO source.provider_fetches(
    provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
    subject_fixture_id,subject_season_id,subject_team_id,request_scope,normalization_version,
    sync_work_item_id,sync_work_item_attempt
  ) VALUES (
    provider_id,'/fixtures/statistics','{"fixture":42}','scheduled_refresh',
    clock_timestamp(),clock_timestamp(),200,'success',
    fixture_id,season_id,home_id,'{}','fixtures-v1',work_item_id,1
  );
  INSERT INTO source.provider_fetches(
    provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
    subject_fixture_id,subject_season_id,subject_team_id,request_scope,normalization_version,
    sync_work_item_id,sync_work_item_attempt
  ) VALUES (
    provider_id,'/fixtures/lineups','{"fixture":42,"team":8}','scheduled_refresh',
    clock_timestamp(),clock_timestamp(),200,'success',
    fixture_id,season_id,away_id,'{}','fixtures-v1',work_item_id,1
  );

  blocked := false;
  BEGIN
    INSERT INTO source.provider_fetches(
      provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
      subject_fixture_id,subject_season_id,subject_team_id,request_scope,normalization_version,
      sync_work_item_id,sync_work_item_attempt
    ) VALUES (
      provider_id,'/fixtures/statistics','{"fixture":42}','scheduled_refresh',
      clock_timestamp(),clock_timestamp(),200,'success',
      fixture_id,season_id,foreign_id,'{}','fixtures-v1',work_item_id,1
    );
  EXCEPTION WHEN SQLSTATE '23514' THEN blocked := true;
  END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted a nonparticipant fixture team subject'; END IF;

  blocked := false;
  BEGIN
    INSERT INTO source.provider_fetches(
      provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
      subject_fixture_id,subject_season_id,subject_team_id,request_scope,normalization_version,
      sync_work_item_id,sync_work_item_attempt
    ) VALUES (
      provider_id,'/fixtures/statistics','{"fixture":42,"team":8}','scheduled_refresh',
      clock_timestamp(),clock_timestamp(),200,'success',
      fixture_id,season_id,home_id,'{}','fixtures-v1',work_item_id,1
    );
  EXCEPTION WHEN SQLSTATE '23514' THEN blocked := true;
  END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted a mismatched provider team filter'; END IF;

  blocked := false;
  BEGIN
    INSERT INTO source.provider_fetches(
      provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
      request_scope,normalization_version,sync_work_item_id,sync_work_item_attempt
    ) VALUES (
      provider_id,'/fixtures/statistics','{"fixture":42,"team":7}','scheduled_refresh',
      clock_timestamp(),clock_timestamp(),200,'success',
      '{}','fixtures-v1',work_item_id,1
    );
  EXCEPTION WHEN SQLSTATE '23514' THEN blocked := true;
  END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted fixture/team filters without typed subjects'; END IF;

  blocked := false;
  BEGIN
    INSERT INTO source.provider_fetches(
      provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
      subject_season_id,subject_team_id,request_scope,normalization_version,
      sync_work_item_id,sync_work_item_attempt
    ) VALUES (
      provider_id,'/standings','{"league":39,"season":2026}','scheduled_refresh',
      clock_timestamp(),clock_timestamp(),200,'success',
      season_id,home_id,'{}','standings-v1',work_item_id,1
    );
  EXCEPTION WHEN SQLSTATE '23514' THEN blocked := true;
  END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted an unsupported standings team subject'; END IF;

  blocked := false;
  BEGIN
    INSERT INTO source.provider_fetches(
      provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
      subject_season_id,subject_team_id,request_scope,normalization_version,
      sync_work_item_id,sync_work_item_attempt
    ) VALUES (
      provider_id,'/fixtures','{"ids":"42","team":7}','scheduled_refresh',
      clock_timestamp(),clock_timestamp(),200,'success',
      season_id,home_id,'{}','fixtures-v1',work_item_id,1
    );
  EXCEPTION WHEN SQLSTATE '23514' THEN blocked := true;
  END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted an unsupported batch team subject'; END IF;

  -- Legacy fetches remain governed by the pre-Q06 contract.
  INSERT INTO source.provider_fetches(
    provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
    subject_fixture_id,subject_season_id,subject_team_id
  ) VALUES (
    provider_id,'/fixtures/statistics','{"fixture":42}','scheduled_refresh',
    clock_timestamp(),clock_timestamp(),200,'success',
    fixture_id,season_id,foreign_id
  );
END
$$;

ROLLBACK;
