-- Q05 prematch freshness evidence. New observations begin at deployment;
-- current fixture state is not used to reconstruct historical responses.

BEGIN;

ALTER TABLE source.provider_fetches
    ADD CONSTRAINT provider_fetches_id_provider_response_received_key
    UNIQUE (id, provider_id, response_received_at);

CREATE TABLE source.fixture_schedule_observations (
    provider_id smallint NOT NULL,
    fixture_id bigint NOT NULL,
    source_fetch_id bigint NOT NULL,
    observed_kickoff_at timestamptz,
    observed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT fixture_schedule_observations_pkey
        PRIMARY KEY (provider_id, fixture_id, source_fetch_id),
    CONSTRAINT fixture_schedule_observations_provider_fixture_fk
        FOREIGN KEY (provider_id, fixture_id)
        REFERENCES source.fixture_provider_refs(provider_id, fixture_id)
        ON DELETE RESTRICT,
    CONSTRAINT fixture_schedule_observations_fetch_provider_fk
        FOREIGN KEY (source_fetch_id, provider_id, observed_at)
        REFERENCES source.provider_fetches(id, provider_id, response_received_at)
        ON DELETE RESTRICT
);

COMMENT ON TABLE source.fixture_schedule_observations IS
    'Append-only kickoff observations from individual fixture entries in provider responses; no historical backfill is inferred from current fixture state.';
COMMENT ON COLUMN source.fixture_schedule_observations.observed_kickoff_at IS
    'Kickoff carried by the fixture entry in this response; NULL means that response did not provide a known kickoff.';
COMMENT ON COLUMN source.fixture_schedule_observations.observed_at IS
    'Original provider response receipt time, equal to source.provider_fetches.response_received_at; never replay or row creation time.';

CREATE INDEX fixture_schedule_observations_fixture_time_idx
    ON source.fixture_schedule_observations (
        provider_id,
        fixture_id,
        observed_kickoff_at,
        observed_at DESC
    ) INCLUDE (source_fetch_id);

CREATE INDEX fixture_schedule_observations_source_fetch_idx
    ON source.fixture_schedule_observations (
        source_fetch_id,
        provider_id,
        observed_at
    );

CREATE OR REPLACE FUNCTION source.guard_fixture_schedule_observation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'fixture schedule observations are append-only'
        USING ERRCODE = '55000';
END
$$;

CREATE TRIGGER fixture_schedule_observations_guard
BEFORE UPDATE OR DELETE ON source.fixture_schedule_observations
FOR EACH ROW EXECUTE FUNCTION source.guard_fixture_schedule_observation();

ALTER TABLE source.fixture_schedule_observations ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON source.fixture_schedule_observations FROM PUBLIC, anon, authenticated;
REVOKE EXECUTE ON FUNCTION source.guard_fixture_schedule_observation()
    FROM PUBLIC, anon, authenticated;

COMMIT;
