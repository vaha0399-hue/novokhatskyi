\set ON_ERROR_STOP on

DO $$
DECLARE preserved_count bigint;
BEGIN
  SELECT count(*) INTO preserved_count
    FROM source.provider_fetches provider_fetch
   WHERE provider_fetch.sync_work_item_id IS NOT NULL
     AND provider_fetch.endpoint = '/fixtures/statistics'
     AND provider_fetch.request_params = '{"fixture":42}'::jsonb
     AND provider_fetch.subject_fixture_id IS NOT NULL
     AND provider_fetch.subject_season_id IS NOT NULL
     AND provider_fetch.subject_team_id IS NOT NULL
     AND source.q06_fetch_subject_matches_request(
          provider_fetch.provider_id,
          provider_fetch.endpoint,
          provider_fetch.request_params,
          provider_fetch.subject_fixture_id,
          provider_fetch.subject_season_id,
          provider_fetch.subject_team_id
     );
  IF preserved_count <> 1 THEN
    RAISE EXCEPTION 'Q06 failed upgrade changed incompatible historical provenance';
  END IF;
END
$$;
