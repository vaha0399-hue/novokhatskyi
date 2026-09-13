-- Q06: bind typed provenance subjects to the provider identifiers in the
-- concrete request without changing the legacy provider_fetches contract.
BEGIN;

CREATE OR REPLACE FUNCTION source.q06_fetch_subject_matches_request(
    p_provider_id smallint,
    p_endpoint text,
    p_request_params jsonb,
    p_subject_fixture_id bigint,
    p_subject_season_id bigint,
    p_subject_team_id bigint
) RETURNS boolean
LANGUAGE plpgsql
STABLE
SET search_path = pg_catalog, source, football AS $$
DECLARE
    fixture_external_id text;
    fixture_season_id bigint;
    team_external_id text;
    season_league_external_id text;
    season_external integer;
    batch_ids text[];
BEGIN
    IF jsonb_typeof(p_request_params) IS DISTINCT FROM 'object' THEN
        RETURN false;
    END IF;
    IF p_subject_fixture_id IS NULL
       AND p_subject_season_id IS NULL
       AND p_subject_team_id IS NULL THEN
        RETURN true;
    END IF;

    IF p_subject_fixture_id IS NOT NULL THEN
        SELECT ref.external_id, fixture.season_id
          INTO fixture_external_id, fixture_season_id
          FROM source.fixture_provider_refs ref
          JOIN football.fixtures fixture ON fixture.id = ref.fixture_id
         WHERE ref.provider_id = p_provider_id
           AND ref.fixture_id = p_subject_fixture_id;
        IF NOT FOUND
           OR (p_subject_season_id IS NOT NULL AND fixture_season_id IS DISTINCT FROM p_subject_season_id) THEN
            RETURN false;
        END IF;
    END IF;

    IF p_subject_season_id IS NOT NULL THEN
        SELECT ref.league_external_id, ref.external_season
          INTO season_league_external_id, season_external
          FROM source.season_provider_refs ref
         WHERE ref.provider_id = p_provider_id
           AND ref.season_id = p_subject_season_id;
        IF NOT FOUND THEN
            RETURN false;
        END IF;
    END IF;

    IF p_subject_team_id IS NOT NULL THEN
        SELECT ref.external_id
          INTO team_external_id
          FROM source.team_provider_refs ref
         WHERE ref.provider_id = p_provider_id
           AND ref.team_id = p_subject_team_id;
        IF NOT FOUND
           OR (p_subject_season_id IS NOT NULL AND NOT EXISTS (
                SELECT 1 FROM football.season_teams season_team
                 WHERE season_team.season_id = p_subject_season_id
                   AND season_team.team_id = p_subject_team_id
           )) THEN
            RETURN false;
        END IF;
    END IF;

    IF p_endpoint IN ('/fixtures/statistics', '/fixtures/lineups') THEN
        RETURN coalesce(
            p_subject_fixture_id IS NOT NULL
            AND p_request_params ->> 'fixture' = fixture_external_id,
            false
        );
    ELSIF p_endpoint = '/injuries' THEN
        IF p_subject_fixture_id IS NOT NULL THEN
            RETURN coalesce(p_request_params ->> 'fixture' = fixture_external_id, false);
        END IF;
        RETURN coalesce(
            p_subject_season_id IS NOT NULL
            AND p_request_params ->> 'league' = season_league_external_id
            AND p_request_params ->> 'season' = season_external::text,
            false
        );
    ELSIF p_endpoint = '/teams/statistics' THEN
        RETURN coalesce(
            p_subject_team_id IS NOT NULL
            AND p_subject_season_id IS NOT NULL
            AND p_request_params ->> 'team' = team_external_id
            AND p_request_params ->> 'league' = season_league_external_id
            AND p_request_params ->> 'season' = season_external::text,
            false
        );
    ELSIF p_endpoint = '/standings' OR p_endpoint = '/teams' THEN
        RETURN coalesce(
            p_subject_season_id IS NOT NULL
            AND p_request_params ->> 'league' = season_league_external_id
            AND p_request_params ->> 'season' = season_external::text,
            false
        );
    ELSIF p_endpoint = '/leagues' THEN
        RETURN coalesce(
            p_subject_season_id IS NOT NULL
            AND p_request_params ->> 'id' = season_league_external_id
            AND p_request_params ->> 'season' = season_external::text,
            false
        );
    ELSIF p_endpoint = '/fixtures' THEN
        IF p_subject_fixture_id IS NOT NULL THEN
            IF p_request_params ? 'id' THEN
                RETURN coalesce(p_request_params ->> 'id' = fixture_external_id, false);
            ELSIF p_request_params ? 'ids' THEN
                batch_ids := string_to_array(p_request_params ->> 'ids', '-');
                RETURN coalesce(fixture_external_id = ANY(batch_ids), false);
            END IF;
            RETURN false;
        ELSIF p_request_params ? 'ids' THEN
            IF p_subject_season_id IS NULL THEN
                RETURN false;
            END IF;
            batch_ids := string_to_array(p_request_params ->> 'ids', '-');
            RETURN coalesce(
                cardinality(batch_ids) BETWEEN 1 AND 20
                AND cardinality(batch_ids) = (
                     SELECT count(DISTINCT requested_id)
                       FROM unnest(batch_ids) AS requested(requested_id)
                )
                AND NOT EXISTS (
                     SELECT 1
                       FROM unnest(batch_ids) AS requested(requested_id)
                      WHERE requested_id !~ '^[1-9][0-9]*$'
                         OR NOT EXISTS (
                             SELECT 1
                               FROM source.fixture_provider_refs ref
                               JOIN football.fixtures fixture ON fixture.id = ref.fixture_id
                              WHERE ref.provider_id = p_provider_id
                                AND ref.external_id = requested_id
                                AND fixture.season_id = p_subject_season_id
                         )
                ),
                false
            );
        ELSIF p_request_params ? 'id' THEN
            RETURN p_subject_season_id IS NOT NULL
               AND EXISTS (
                    SELECT 1
                      FROM source.fixture_provider_refs ref
                      JOIN football.fixtures fixture ON fixture.id = ref.fixture_id
                     WHERE ref.provider_id = p_provider_id
                       AND ref.external_id = p_request_params ->> 'id'
                       AND fixture.season_id = p_subject_season_id
               );
        END IF;
        RETURN coalesce(
            p_subject_season_id IS NOT NULL
            AND p_request_params ->> 'league' = season_league_external_id
            AND p_request_params ->> 'season' = season_external::text,
            false
        );
    END IF;

    RETURN p_subject_fixture_id IS NULL
       AND p_subject_season_id IS NULL
       AND p_subject_team_id IS NULL;
END
$$;

REVOKE ALL ON FUNCTION source.q06_fetch_subject_matches_request(
    smallint, text, jsonb, bigint, bigint, bigint
) FROM PUBLIC;

DO $$
DECLARE incompatible_count bigint;
BEGIN
    SELECT count(*) INTO incompatible_count
      FROM source.provider_fetches provider_fetch
     WHERE provider_fetch.sync_work_item_id IS NOT NULL
       AND NOT source.q06_fetch_subject_matches_request(
            provider_fetch.provider_id,
            provider_fetch.endpoint,
            provider_fetch.request_params,
            provider_fetch.subject_fixture_id,
            provider_fetch.subject_season_id,
            provider_fetch.subject_team_id
       );
    IF incompatible_count > 0 THEN
        RAISE EXCEPTION 'Q06 subject/request binding migration found % incompatible historical fetch rows; historical params will not be rewritten', incompatible_count
            USING ERRCODE = '23514';
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION source.guard_q06_fetch_subject_request_binding()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, source AS $$
BEGIN
    IF NEW.sync_work_item_id IS NOT NULL
       AND NOT source.q06_fetch_subject_matches_request(
            NEW.provider_id,
            NEW.endpoint,
            NEW.request_params,
            NEW.subject_fixture_id,
            NEW.subject_season_id,
            NEW.subject_team_id
       ) THEN
        RAISE EXCEPTION 'Q06 provider fetch subject does not match endpoint request parameters'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER provider_fetches_q06_subject_request_binding
BEFORE INSERT OR UPDATE ON source.provider_fetches
FOR EACH ROW EXECUTE FUNCTION source.guard_q06_fetch_subject_request_binding();

REVOKE ALL ON FUNCTION source.guard_q06_fetch_subject_request_binding() FROM PUBLIC;

COMMIT;
