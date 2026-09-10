-- State produced after the late-window migration but before deadline is split
-- from storage identity. Both accepted and pending rows must survive upgrade.
DO $$
DECLARE
    provider_key smallint;
    country_key bigint;
    league_key bigint;
    season_key bigint;
    home_key bigint;
    away_key bigint;
    fixture_key bigint;
    accepted_fetch bigint;
    pending_fetch bigint;
BEGIN
    SELECT id INTO provider_key FROM source.providers ORDER BY id LIMIT 1;
    INSERT INTO football.countries(name) VALUES('Q05 deadline upgrade country') RETURNING id INTO country_key;
    INSERT INTO football.leagues(name,country_id,competition_type)
      VALUES('Q05 deadline upgrade league',country_key,'league') RETURNING id INTO league_key;
    INSERT INTO football.seasons(league_id,start_year,label)
      VALUES(league_key,2026,'Q05 deadline upgrade season') RETURNING id INTO season_key;
    INSERT INTO football.teams(name) VALUES('Q05 deadline upgrade home') RETURNING id INTO home_key;
    INSERT INTO football.teams(name) VALUES('Q05 deadline upgrade away') RETURNING id INTO away_key;
    INSERT INTO football.season_teams(season_id,team_id)
      VALUES(season_key,home_key),(season_key,away_key);
    INSERT INTO football.fixtures(
        season_id,home_team_id,away_team_id,kickoff_at,lifecycle_state,first_seen_at,last_seen_at
    ) VALUES (
        season_key,home_key,away_key,'2026-09-10 12:00:00+00','scheduled',
        '2026-09-09 12:00:10+00','2026-09-09 12:01:10+00'
    ) RETURNING id INTO fixture_key;
    INSERT INTO source.provider_fetches(
        provider_id,endpoint,purpose,request_started_at,response_received_at,http_status,outcome
    ) VALUES (
        provider_key,'/fixtures','scheduled_refresh','2026-09-09 12:00:10+00',
        '2026-09-09 12:00:10+00',200,'success'
    ) RETURNING id INTO accepted_fetch;
    INSERT INTO source.provider_fetches(
        provider_id,endpoint,purpose,request_started_at,response_received_at,http_status,outcome
    ) VALUES (
        provider_key,'/fixtures','scheduled_refresh','2026-09-09 12:01:10+00',
        '2026-09-09 12:01:10+00',200,'success'
    ) RETURNING id INTO pending_fetch;
    INSERT INTO ops.fixture_analytics_recalculation_windows(
        fixture_id,window_end,latest_source_fetch_id,observed_at,accepted_input_version,accepted_at
    ) VALUES (
        fixture_key,'2026-09-09 12:01:00+00',accepted_fetch,'2026-09-09 12:00:10+00',
        accepted_fetch::text,'2026-09-09 12:01:00.000101+00'
    );
    INSERT INTO ops.fixture_analytics_recalculation_windows(
        fixture_id,window_end,latest_source_fetch_id,observed_at,source_window_end
    ) VALUES (
        fixture_key,'2026-09-09 12:03:00+00',pending_fetch,'2026-09-09 12:01:10+00',
        '2026-09-09 12:02:00+00'
    );
END $$;
