\set ON_ERROR_STOP on

DO $$
DECLARE
    target_provider_id smallint;
    target_fixture_id bigint;
    target_fetch_id bigint;
BEGIN
    IF EXISTS (SELECT 1 FROM source.fixture_schedule_observations) THEN
        RAISE EXCEPTION 'observation migration invented historical rows';
    END IF;

    SELECT ref.provider_id,fixture.id,provider_fetch.id
      INTO target_provider_id,target_fixture_id,target_fetch_id
      FROM football.fixtures fixture
      JOIN football.seasons season ON season.id=fixture.season_id
      JOIN source.fixture_provider_refs ref ON ref.fixture_id=fixture.id
      JOIN source.provider_fetches provider_fetch
        ON provider_fetch.provider_id=ref.provider_id
       AND provider_fetch.subject_fixture_id=fixture.id
     WHERE season.label='Q05 observation upgrade season';

    IF target_provider_id IS NULL OR target_fixture_id IS NULL OR target_fetch_id IS NULL THEN
        RAISE EXCEPTION 'pre-migration fixture provenance was not preserved';
    END IF;
    IF (SELECT kickoff_at FROM football.fixtures WHERE id=target_fixture_id)
       IS DISTINCT FROM '2026-09-11 18:30:00+00'::timestamptz THEN
        RAISE EXCEPTION 'pre-migration fixture changed during observation migration';
    END IF;
    IF (SELECT response_received_at FROM source.provider_fetches WHERE id=target_fetch_id)
       IS DISTINCT FROM '2026-09-10 09:10:00+00'::timestamptz THEN
        RAISE EXCEPTION 'pre-migration fetch changed during observation migration';
    END IF;

    INSERT INTO source.fixture_schedule_observations(
        provider_id,fixture_id,source_fetch_id,observed_kickoff_at,observed_at
    ) VALUES (
        target_provider_id,target_fixture_id,target_fetch_id,'2026-09-11 18:30:00+00','2026-09-10 09:10:00+00'
    );

    IF NOT EXISTS (
        SELECT 1 FROM source.fixture_schedule_observations
        WHERE fixture_schedule_observations.provider_id=target_provider_id
          AND fixture_schedule_observations.fixture_id=target_fixture_id
          AND fixture_schedule_observations.source_fetch_id=target_fetch_id
          AND observed_kickoff_at='2026-09-11 18:30:00+00'
          AND observed_at='2026-09-10 09:10:00+00'
    ) THEN
        RAISE EXCEPTION 'pre-migration provenance cannot seed a new observation';
    END IF;
END
$$;
