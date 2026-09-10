\set ON_ERROR_STOP on

DO $$
DECLARE
    provider_id smallint;
    league_country_id bigint;
    team_country_id bigint;
    league_id bigint;
    season_id bigint;
    home_team_id bigint;
    away_team_id bigint;
    fixture_id bigint;
BEGIN
    SELECT id INTO provider_id FROM source.providers ORDER BY id LIMIT 1;
    INSERT INTO football.countries(name) VALUES('Q05 observation upgrade league country') RETURNING id INTO league_country_id;
    INSERT INTO football.countries(name) VALUES('Q05 observation upgrade team country') RETURNING id INTO team_country_id;
    INSERT INTO football.leagues(name,country_id,competition_type)
    VALUES('Q05 observation upgrade league',league_country_id,'league') RETURNING id INTO league_id;
    INSERT INTO football.seasons(league_id,start_year,label)
    VALUES(league_id,2026,'Q05 observation upgrade season') RETURNING id INTO season_id;
    INSERT INTO football.teams(name,country_id) VALUES('Q05 observation upgrade home',team_country_id) RETURNING id INTO home_team_id;
    INSERT INTO football.teams(name,country_id) VALUES('Q05 observation upgrade away',team_country_id) RETURNING id INTO away_team_id;
    INSERT INTO football.season_teams(season_id,team_id)
    VALUES(season_id,home_team_id),(season_id,away_team_id);
    INSERT INTO football.fixtures(
        season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state,
        first_seen_at,last_seen_at
    ) VALUES (
        season_id,home_team_id,away_team_id,'2026-09-11 18:30:00+00','scheduled',
        '2026-09-10 09:00:00+00','2026-09-10 09:00:00+00'
    ) RETURNING id INTO fixture_id;
    INSERT INTO source.fixture_provider_refs(provider_id,external_id,fixture_id)
    VALUES(provider_id,'q05-observation-upgrade-fixture',fixture_id);
    INSERT INTO source.provider_fetches(
        provider_id,endpoint,purpose,request_started_at,response_received_at,
        http_status,outcome,normalized_at,subject_fixture_id
    ) VALUES (
        provider_id,'/fixtures','scheduled_refresh','2026-09-10 09:09:59+00',
        '2026-09-10 09:10:00+00',200,'success','2026-09-10 09:10:01+00',fixture_id
    );
END
$$;
