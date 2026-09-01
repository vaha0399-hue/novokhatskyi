-- Permit immutable fixture membership for both bounded ids batches and the
-- controlled current-season terminal discovery request used below.
CREATE OR REPLACE FUNCTION source.guard_provider_fetch_fixture_subject()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    fetch_row source.provider_fetches%ROWTYPE;
    fixture_season_id bigint;
    fixture_external_id text;
    requested_ids text[];
    is_batch boolean;
    is_terminal_discovery boolean;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'provider fetch fixture subjects are immutable' USING ERRCODE = '55000';
    END IF;
    SELECT * INTO fetch_row FROM source.provider_fetches WHERE id=NEW.fetch_id;
    is_batch := coalesce(fetch_row.request_params->>'ids','') <> '';
    is_terminal_discovery := fetch_row.request_params ? 'league'
      AND fetch_row.request_params ? 'season'
      AND fetch_row.request_params->>'status'='FT-AET-PEN';
    IF fetch_row.endpoint IS DISTINCT FROM '/fixtures'
       OR fetch_row.outcome IS DISTINCT FROM 'success'::source.fetch_outcome
       OR fetch_row.response_received_at IS NULL
       OR fetch_row.subject_fixture_id IS NOT NULL
       OR fetch_row.subject_season_id IS NULL
       OR jsonb_typeof(fetch_row.request_params) <> 'object'
       OR NOT (is_batch OR is_terminal_discovery) THEN
        RAISE EXCEPTION 'fixture subject requires a controlled successful season-bound /fixtures fetch'
          USING ERRCODE='23514';
    END IF;
    IF is_batch THEN
        requested_ids := string_to_array(fetch_row.request_params->>'ids','-');
        IF cardinality(requested_ids) NOT BETWEEN 1 AND 20
           OR cardinality(requested_ids) <> (SELECT count(DISTINCT value) FROM unnest(requested_ids) AS value)
           OR EXISTS (SELECT 1 FROM unnest(requested_ids) AS value WHERE value !~ '^[1-9][0-9]*$') THEN
            RAISE EXCEPTION 'batch fixture fetch ids must be 1..20 distinct positive provider identifiers' USING ERRCODE='23514';
        END IF;
    END IF;
    SELECT fixture.season_id,ref.external_id INTO fixture_season_id,fixture_external_id
    FROM football.fixtures fixture
    JOIN source.fixture_provider_refs ref ON ref.fixture_id=fixture.id AND ref.provider_id=fetch_row.provider_id
    WHERE fixture.id=NEW.fixture_id;
    IF fixture_season_id IS NULL OR fixture_season_id IS DISTINCT FROM fetch_row.subject_season_id
       OR (is_batch AND NOT (fixture_external_id=ANY(requested_ids))) THEN
        RAISE EXCEPTION 'fixture subject does not match provider fetch season or requested ids' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;

-- Finalize a provider-confirmed terminal fixture from a season-scoped
-- discovery response.  This is deliberately separate from the live
-- reconciliation function: the latter requires a fixture-scoped fetch,
-- whereas this path proves membership through provider_fetch_fixture_subjects.
CREATE OR REPLACE FUNCTION ops.finalize_season_discovery_fixture_result(
  p_fixture_id bigint,
  p_source_fetch_id bigint,
  p_home_goals smallint,
  p_away_goals smallint,
  p_home_halftime_goals smallint DEFAULT NULL,
  p_away_halftime_goals smallint DEFAULT NULL,
  p_home_fulltime_goals smallint DEFAULT NULL,
  p_away_fulltime_goals smallint DEFAULT NULL,
  p_home_extratime_goals smallint DEFAULT NULL,
  p_away_extratime_goals smallint DEFAULT NULL,
  p_home_penalty_goals smallint DEFAULT NULL,
  p_away_penalty_goals smallint DEFAULT NULL
) RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, source, football, ops AS $$
DECLARE
  fetch_row source.provider_fetches%ROWTYPE;
  fixture_row football.fixtures%ROWTYPE;
BEGIN
  SELECT * INTO fetch_row FROM source.provider_fetches WHERE id=p_source_fetch_id FOR UPDATE;
  SELECT * INTO fixture_row FROM football.fixtures WHERE id=p_fixture_id FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'fixture does not exist' USING ERRCODE='23503';
  END IF;
  IF fetch_row.purpose IS DISTINCT FROM 'scheduled_refresh'::source.fetch_purpose
     OR fetch_row.outcome IS DISTINCT FROM 'success'::source.fetch_outcome
     OR fetch_row.endpoint IS DISTINCT FROM '/fixtures'
     OR fetch_row.response_received_at IS NULL
     OR fetch_row.subject_season_id IS DISTINCT FROM fixture_row.season_id
     OR NOT EXISTS (
       SELECT 1 FROM source.provider_fetch_fixture_subjects subject
       WHERE subject.fetch_id=p_source_fetch_id AND subject.fixture_id=p_fixture_id
     )
  THEN
    RAISE EXCEPTION 'season discovery fetch is not valid provenance for fixture finalization'
      USING ERRCODE='23514';
  END IF;
  IF fetch_row.response_received_at < fixture_row.kickoff_at + interval '3 hours' THEN
    RAISE EXCEPTION 'season discovery result is before the finalization window' USING ERRCODE='23514';
  END IF;
  IF fixture_row.result_finalized_at IS NOT NULL THEN
    IF fixture_row.lifecycle_state <> 'completed'
       OR (fixture_row.home_goals,fixture_row.away_goals) IS DISTINCT FROM (p_home_goals,p_away_goals)
    THEN
      RAISE EXCEPTION 'season discovery conflicts with immutable finalized result' USING ERRCODE='23514';
    END IF;
    RETURN false;
  END IF;
  IF fixture_row.lifecycle_state NOT IN ('scheduled','completed') THEN
    RAISE EXCEPTION 'fixture is not eligible for season discovery finalization' USING ERRCODE='23514';
  END IF;
  IF fixture_row.lifecycle_state='completed'
     AND (fixture_row.home_goals,fixture_row.away_goals) IS DISTINCT FROM (p_home_goals,p_away_goals)
  THEN
    RAISE EXCEPTION 'season discovery conflicts with unfinalized canonical result' USING ERRCODE='23514';
  END IF;
  UPDATE football.fixtures
  SET lifecycle_state='completed',home_goals=p_home_goals,away_goals=p_away_goals,
      home_halftime_goals=p_home_halftime_goals,away_halftime_goals=p_away_halftime_goals,
      home_fulltime_goals=p_home_fulltime_goals,away_fulltime_goals=p_away_fulltime_goals,
      home_extratime_goals=p_home_extratime_goals,away_extratime_goals=p_away_extratime_goals,
      home_penalty_goals=p_home_penalty_goals,away_penalty_goals=p_away_penalty_goals,
      terminal_status_observed_at=fetch_row.response_received_at,
      result_available_at=fetch_row.response_received_at,availability_basis='observed',
      result_finalized_at=fetch_row.response_received_at,
      last_seen_at=greatest(fixture_row.last_seen_at,fetch_row.response_received_at),
      last_source_fetch_id=p_source_fetch_id
  WHERE id=p_fixture_id;
  RETURN true;
END $$;

REVOKE ALL ON FUNCTION ops.finalize_season_discovery_fixture_result(
  bigint,bigint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint,smallint
) FROM PUBLIC;
