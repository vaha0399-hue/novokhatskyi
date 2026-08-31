-- Additive current-season batch statistics foundation.
--
-- A `/fixtures?ids=...` response can contain statistics for multiple
-- canonical fixtures. The existing `source.provider_fetches.subject_fixture_id`
-- remains the correct provenance for one-fixture endpoints, so this migration
-- adds an explicit immutable many-to-many binding rather than overloading it.

BEGIN;

CREATE TABLE source.provider_fetch_fixture_subjects (
    fetch_id bigint NOT NULL REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    fixture_id bigint NOT NULL REFERENCES football.fixtures(id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (fetch_id, fixture_id)
);

CREATE INDEX provider_fetch_fixture_subjects_fixture_idx
    ON source.provider_fetch_fixture_subjects (fixture_id, fetch_id DESC);

CREATE OR REPLACE FUNCTION source.guard_provider_fetch_fixture_subject()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    fetch_row source.provider_fetches%ROWTYPE;
    fixture_season_id bigint;
    fixture_external_id text;
    requested_ids text[];
BEGIN
    IF TG_OP <> 'INSERT' THEN
        RAISE EXCEPTION 'provider fetch fixture subjects are immutable' USING ERRCODE = '55000';
    END IF;

    SELECT * INTO fetch_row FROM source.provider_fetches WHERE id = NEW.fetch_id;
    IF fetch_row.endpoint IS DISTINCT FROM '/fixtures'
       OR fetch_row.outcome IS DISTINCT FROM 'success'::source.fetch_outcome
       OR fetch_row.response_received_at IS NULL
       OR fetch_row.subject_fixture_id IS NOT NULL
       OR fetch_row.subject_season_id IS NULL
       OR jsonb_typeof(fetch_row.request_params) <> 'object'
       OR coalesce(fetch_row.request_params->>'ids', '') = '' THEN
        RAISE EXCEPTION 'batch fixture subject requires a successful season-bound /fixtures ids fetch'
            USING ERRCODE = '23514';
    END IF;

    requested_ids := string_to_array(fetch_row.request_params->>'ids', '-');
    IF cardinality(requested_ids) IS NULL
       OR cardinality(requested_ids) NOT BETWEEN 1 AND 20
       OR cardinality(requested_ids) <> (SELECT count(DISTINCT value) FROM unnest(requested_ids) AS value)
       OR EXISTS (SELECT 1 FROM unnest(requested_ids) AS value WHERE value !~ '^[1-9][0-9]*$') THEN
        RAISE EXCEPTION 'batch fixture fetch ids must be 1..20 distinct positive provider identifiers'
            USING ERRCODE = '23514';
    END IF;

    SELECT fixture.season_id, ref.external_id
      INTO fixture_season_id, fixture_external_id
      FROM football.fixtures fixture
      JOIN source.fixture_provider_refs ref
        ON ref.fixture_id = fixture.id AND ref.provider_id = fetch_row.provider_id
     WHERE fixture.id = NEW.fixture_id;
    IF fixture_season_id IS NULL
       OR fixture_season_id IS DISTINCT FROM fetch_row.subject_season_id
       OR NOT (fixture_external_id = ANY(requested_ids)) THEN
        RAISE EXCEPTION 'batch fixture subject does not match provider fetch season or requested ids'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END $$;

CREATE TRIGGER provider_fetch_fixture_subjects_guard
BEFORE INSERT OR UPDATE OR DELETE ON source.provider_fetch_fixture_subjects
FOR EACH ROW EXECUTE FUNCTION source.guard_provider_fetch_fixture_subject();

CREATE OR REPLACE FUNCTION football.guard_fixture_statistics() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    f football.fixtures%ROWTYPE;
    fetch_row source.provider_fetches%ROWTYPE;
BEGIN
    IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'fixture statistics cannot be deleted' USING ERRCODE = '55000'; END IF;
    PERFORM football.assert_fixture_participant(NEW.fixture_id, NEW.team_id);
    SELECT * INTO f FROM football.fixtures WHERE id = NEW.fixture_id;
    IF f.lifecycle_state <> 'completed' THEN RAISE EXCEPTION 'fixture statistics require a completed fixture' USING ERRCODE = '23514'; END IF;
    IF NEW.observed_at < f.kickoff_at OR NEW.available_at < f.kickoff_at THEN RAISE EXCEPTION 'fixture statistics cannot be available before kickoff' USING ERRCODE = '23514'; END IF;
    IF NEW.availability_basis = 'observed' THEN
        IF NEW.last_source_fetch_id IS NULL THEN RAISE EXCEPTION 'observed fixture statistics require source fetch provenance' USING ERRCODE = '23514'; END IF;
        SELECT * INTO fetch_row FROM source.provider_fetches WHERE id = NEW.last_source_fetch_id;
        IF fetch_row.outcome IS DISTINCT FROM 'success'::source.fetch_outcome
           OR fetch_row.response_received_at IS DISTINCT FROM NEW.available_at THEN
            RAISE EXCEPTION 'observed fixture statistics must match a successful source fetch' USING ERRCODE = '23514';
        END IF;
        IF fetch_row.endpoint = '/fixtures/statistics' THEN
            IF fetch_row.subject_fixture_id IS DISTINCT FROM NEW.fixture_id THEN
                RAISE EXCEPTION 'single-fixture statistics fetch subject mismatch' USING ERRCODE = '23514';
            END IF;
        ELSIF fetch_row.endpoint = '/fixtures' THEN
            IF NOT EXISTS (
                SELECT 1 FROM source.provider_fetch_fixture_subjects subject
                WHERE subject.fetch_id = NEW.last_source_fetch_id AND subject.fixture_id = NEW.fixture_id
            ) THEN
                RAISE EXCEPTION 'batch fixture statistics lack fetch-to-fixture provenance' USING ERRCODE = '23514';
            END IF;
        ELSE
            RAISE EXCEPTION 'observed fixture statistics require a statistics-capable endpoint' USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.available_at < f.kickoff_at + interval '3 hours' THEN
        RAISE EXCEPTION 'conservative fixture statistics availability must use a post-match safety interval' USING ERRCODE = '23514';
    END IF;
    IF NEW.finalized_at IS NOT NULL AND NEW.finalized_at < NEW.observed_at THEN RAISE EXCEPTION 'statistics finalized_at precedes observed_at' USING ERRCODE = '23514'; END IF;
    IF TG_OP = 'UPDATE' AND OLD.finalized_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'finalized fixture statistics are immutable' USING ERRCODE = '55000'; END IF;
    RETURN NEW;
END $$;

CREATE TABLE football.team_rolling_metrics (
    team_id bigint NOT NULL REFERENCES football.teams(id) ON DELETE RESTRICT,
    season_id bigint NOT NULL REFERENCES football.seasons(id) ON DELETE RESTRICT,
    scope text NOT NULL CHECK (scope IN ('overall', 'home', 'away')),
    window_size smallint NOT NULL CHECK (window_size IN (0, 5, 10)),
    matches_count smallint NOT NULL CHECK (matches_count >= 0),
    avg_xg numeric(9,4),
    avg_xga numeric(9,4),
    avg_goals_for numeric(9,4) NOT NULL,
    avg_goals_against numeric(9,4) NOT NULL,
    scored_rate numeric(6,5) NOT NULL CHECK (scored_rate BETWEEN 0 AND 1),
    conceded_rate numeric(6,5) NOT NULL CHECK (conceded_rate BETWEEN 0 AND 1),
    btts_rate numeric(6,5) NOT NULL CHECK (btts_rate BETWEEN 0 AND 1),
    over_1_5_rate numeric(6,5) NOT NULL CHECK (over_1_5_rate BETWEEN 0 AND 1),
    over_2_5_rate numeric(6,5) NOT NULL CHECK (over_2_5_rate BETWEEN 0 AND 1),
    over_3_5_rate numeric(6,5) NOT NULL CHECK (over_3_5_rate BETWEEN 0 AND 1),
    avg_shots numeric(9,4),
    avg_shots_on_goal numeric(9,4),
    avg_corners numeric(9,4),
    avg_possession numeric(9,4),
    source_last_kickoff_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (team_id, season_id, scope, window_size),
    CHECK ((matches_count = 0) = (source_last_kickoff_at IS NULL))
);

CREATE INDEX team_rolling_metrics_scanner_idx
    ON football.team_rolling_metrics (season_id, scope, window_size, team_id);

CREATE TRIGGER team_rolling_metrics_touch_updated_at
BEFORE UPDATE ON football.team_rolling_metrics
FOR EACH ROW EXECUTE FUNCTION football.touch_updated_at();

ALTER TABLE source.provider_fetch_fixture_subjects ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.team_rolling_metrics ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON source.provider_fetch_fixture_subjects, football.team_rolling_metrics FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA source, football FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION source.guard_provider_fetch_fixture_subject() FROM PUBLIC;

COMMIT;
