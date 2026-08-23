-- Additive canonical storage for factual, post-match historical lineups.
--
-- This migration deliberately does not change the existing pre-match lineup
-- snapshot lane. Historical lineups fetched after a completed fixture are never
-- evidence of pre-kickoff knowledge and must remain structurally separate.
-- No player statistics, events, odds, predictions, or live data are added.

BEGIN;

ALTER TYPE source.fetch_purpose
    ADD VALUE IF NOT EXISTS 'historical_backfill';

CREATE TABLE football.fixture_historical_lineup_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    fixture_id bigint NOT NULL REFERENCES football.fixtures(id) ON DELETE RESTRICT,
    source_fetch_id bigint NOT NULL UNIQUE REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    content_sha256 bytea NOT NULL CHECK (octet_length(content_sha256) = 32),
    captured_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL,
    availability_basis football.availability_basis NOT NULL,
    coverage_state football.snapshot_coverage_state NOT NULL,
    team_count smallint NOT NULL CHECK (team_count BETWEEN 0 AND 2),
    mapping_version text NOT NULL CHECK (btrim(mapping_version) <> ''),
    ingest_txid bigint NOT NULL DEFAULT txid_current(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (fixture_id, content_sha256),
    CHECK (available_at = captured_at),
    CHECK (availability_basis = 'reconstructed_conservative'),
    CHECK (coverage_state <> 'complete' OR team_count = 2),
    CHECK (coverage_state <> 'empty' OR team_count = 0),
    CHECK (coverage_state <> 'partial' OR team_count = 1)
);

COMMENT ON TABLE football.fixture_historical_lineup_snapshots IS
    'Append-only factual lineups retrieved after a completed fixture. This is not a pre-match availability snapshot and must never be used as evidence of pre-kickoff knowledge.';

CREATE TABLE football.fixture_historical_lineups (
    snapshot_id bigint NOT NULL REFERENCES football.fixture_historical_lineup_snapshots(id) ON DELETE RESTRICT,
    team_id bigint NOT NULL REFERENCES football.teams(id) ON DELETE RESTRICT,
    coach_id bigint REFERENCES football.coaches(id) ON DELETE RESTRICT,
    formation text,
    starter_count smallint NOT NULL CHECK (starter_count >= 0),
    substitute_count smallint NOT NULL CHECK (substitute_count >= 0),
    PRIMARY KEY (snapshot_id, team_id),
    CHECK (formation IS NULL OR btrim(formation) <> '')
);

CREATE TABLE football.fixture_historical_lineup_players (
    snapshot_id bigint NOT NULL,
    team_id bigint NOT NULL,
    player_id bigint NOT NULL REFERENCES football.players(id) ON DELETE RESTRICT,
    lineup_role football.lineup_role NOT NULL,
    position text,
    shirt_number smallint,
    grid text,
    PRIMARY KEY (snapshot_id, team_id, player_id),
    UNIQUE (snapshot_id, player_id),
    FOREIGN KEY (snapshot_id, team_id)
        REFERENCES football.fixture_historical_lineups(snapshot_id, team_id)
        ON DELETE RESTRICT,
    CHECK (shirt_number IS NULL OR shirt_number BETWEEN 0 AND 199),
    CHECK (position IS NULL OR btrim(position) <> ''),
    CHECK (grid IS NULL OR btrim(grid) <> '')
);

CREATE INDEX fixture_historical_lineup_snapshots_fixture_capture_idx
    ON football.fixture_historical_lineup_snapshots (fixture_id, captured_at DESC, id DESC);

CREATE INDEX fixture_historical_lineups_team_formation_idx
    ON football.fixture_historical_lineups (team_id, formation)
    WHERE formation IS NOT NULL;

CREATE INDEX fixture_historical_lineups_coach_snapshot_idx
    ON football.fixture_historical_lineups (coach_id, snapshot_id)
    WHERE coach_id IS NOT NULL;

CREATE INDEX fixture_historical_lineup_players_player_snapshot_idx
    ON football.fixture_historical_lineup_players (player_id, snapshot_id DESC);

CREATE OR REPLACE FUNCTION football.guard_historical_lineup_snapshot()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    fixture_row football.fixtures%ROWTYPE;
    provider_fetch source.provider_fetches%ROWTYPE;
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        RAISE EXCEPTION 'historical lineup snapshots are immutable' USING ERRCODE = '55000';
    END IF;

    SELECT * INTO fixture_row
    FROM football.fixtures
    WHERE id = NEW.fixture_id
    FOR SHARE;

    SELECT * INTO provider_fetch
    FROM source.provider_fetches
    WHERE id = NEW.source_fetch_id
    FOR SHARE;

    IF fixture_row.lifecycle_state IS DISTINCT FROM 'completed'::football.fixture_lifecycle_state
       OR fixture_row.result_finalized_at IS NULL
       OR NEW.captured_at < fixture_row.kickoff_at
       OR NEW.available_at < fixture_row.kickoff_at
       OR NEW.captured_at < fixture_row.result_finalized_at
       OR NEW.available_at < fixture_row.result_finalized_at THEN
        RAISE EXCEPTION 'historical lineup snapshots require a finalized fixture and post-finalization capture'
            USING ERRCODE = '23514';
    END IF;

    IF provider_fetch.outcome IS DISTINCT FROM 'success'::source.fetch_outcome
       OR provider_fetch.purpose IS DISTINCT FROM 'historical_backfill'::source.fetch_purpose
       OR provider_fetch.endpoint IS DISTINCT FROM '/fixtures/lineups'
       OR provider_fetch.subject_fixture_id IS DISTINCT FROM NEW.fixture_id
       OR provider_fetch.response_received_at IS NULL
       OR provider_fetch.response_received_at IS DISTINCT FROM NEW.captured_at
       OR provider_fetch.content_sha256 IS NULL
       OR provider_fetch.content_sha256 IS DISTINCT FROM NEW.content_sha256
       OR NOT EXISTS (
            SELECT 1
            FROM source.provider_raw_payloads raw
            WHERE raw.fetch_id = NEW.source_fetch_id
              AND raw.purged_at IS NULL
       )
       OR NOT EXISTS (
            SELECT 1
            FROM source.fixture_provider_refs ref
            WHERE ref.provider_id = provider_fetch.provider_id
              AND ref.fixture_id = NEW.fixture_id
              AND provider_fetch.request_params ->> 'fixture' = ref.external_id
       ) THEN
        RAISE EXCEPTION 'historical lineup snapshot requires matching retained post-match provider provenance'
            USING ERRCODE = '23514';
    END IF;

    -- The raw row is intentionally locked after its retained-state check. A
    -- concurrent purge cannot commit between provenance validation and this
    -- snapshot's commit.
    PERFORM 1
    FROM source.provider_raw_payloads raw
    WHERE raw.fetch_id = NEW.source_fetch_id
      AND raw.purged_at IS NULL
    FOR SHARE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'historical lineup snapshot requires a retained raw payload'
            USING ERRCODE = '23514';
    END IF;

    NEW.ingest_txid := txid_current();
    RETURN NEW;
END
$$;

CREATE TRIGGER fixture_historical_lineup_snapshots_guard
BEFORE INSERT OR UPDATE OR DELETE ON football.fixture_historical_lineup_snapshots
FOR EACH ROW EXECUTE FUNCTION football.guard_historical_lineup_snapshot();

CREATE OR REPLACE FUNCTION football.guard_historical_lineup_child()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    snapshot_fixture_id bigint;
    snapshot_txid bigint;
BEGIN
    IF TG_OP IN ('UPDATE', 'DELETE') THEN
        RAISE EXCEPTION 'historical lineup rows are immutable' USING ERRCODE = '55000';
    END IF;

    SELECT fixture_id, ingest_txid
    INTO snapshot_fixture_id, snapshot_txid
    FROM football.fixture_historical_lineup_snapshots
    WHERE id = NEW.snapshot_id;

    IF snapshot_fixture_id IS NULL OR snapshot_txid <> txid_current() THEN
        RAISE EXCEPTION 'historical lineup rows must be inserted in the snapshot transaction'
            USING ERRCODE = '55000';
    END IF;

    PERFORM football.assert_fixture_participant(snapshot_fixture_id, NEW.team_id);
    RETURN NEW;
END
$$;

CREATE TRIGGER fixture_historical_lineups_guard
BEFORE INSERT OR UPDATE OR DELETE ON football.fixture_historical_lineups
FOR EACH ROW EXECUTE FUNCTION football.guard_historical_lineup_child();

CREATE TRIGGER fixture_historical_lineup_players_guard
BEFORE INSERT OR UPDATE OR DELETE ON football.fixture_historical_lineup_players
FOR EACH ROW EXECUTE FUNCTION football.guard_historical_lineup_child();

CREATE OR REPLACE FUNCTION football.guard_historical_lineup_coach_provider_ref()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    snapshot_provider_id smallint;
BEGIN
    IF NEW.coach_id IS NULL THEN
        RETURN NEW;
    END IF;

    SELECT provider_fetch.provider_id
    INTO snapshot_provider_id
    FROM football.fixture_historical_lineup_snapshots snapshot
    JOIN source.provider_fetches provider_fetch ON provider_fetch.id = snapshot.source_fetch_id
    WHERE snapshot.id = NEW.snapshot_id;

    IF snapshot_provider_id IS NULL OR NOT EXISTS (
        SELECT 1
        FROM source.coach_provider_refs ref
        WHERE ref.provider_id = snapshot_provider_id
          AND ref.coach_id = NEW.coach_id
    ) THEN
        RAISE EXCEPTION 'historical lineup coach must have a provider mapping for the provenance fetch provider'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER fixture_historical_lineups_coach_provider_guard
BEFORE INSERT OR UPDATE ON football.fixture_historical_lineups
FOR EACH ROW EXECUTE FUNCTION football.guard_historical_lineup_coach_provider_ref();

CREATE OR REPLACE FUNCTION football.guard_historical_lineup_player_provider_ref()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    snapshot_provider_id smallint;
BEGIN
    SELECT provider_fetch.provider_id
    INTO snapshot_provider_id
    FROM football.fixture_historical_lineup_snapshots snapshot
    JOIN source.provider_fetches provider_fetch ON provider_fetch.id = snapshot.source_fetch_id
    WHERE snapshot.id = NEW.snapshot_id;

    IF snapshot_provider_id IS NULL OR NOT EXISTS (
        SELECT 1
        FROM source.player_provider_refs ref
        WHERE ref.provider_id = snapshot_provider_id
          AND ref.player_id = NEW.player_id
    ) THEN
        RAISE EXCEPTION 'historical lineup player must have a provider mapping for the provenance fetch provider'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER fixture_historical_lineup_players_provider_guard
BEFORE INSERT OR UPDATE ON football.fixture_historical_lineup_players
FOR EACH ROW EXECUTE FUNCTION football.guard_historical_lineup_player_provider_ref();

CREATE OR REPLACE FUNCTION football.assert_historical_lineup_snapshot_commit_valid()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (
        SELECT count(*)
        FROM football.fixture_historical_lineups lineup
        WHERE lineup.snapshot_id = NEW.id
    ) <> NEW.team_count THEN
        RAISE EXCEPTION 'historical lineup snapshot team_count does not match rows'
            USING ERRCODE = '23514';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM football.fixture_historical_lineups lineup
        WHERE lineup.snapshot_id = NEW.id
          AND (
              lineup.starter_count <> (
                  SELECT count(*)
                  FROM football.fixture_historical_lineup_players player
                  WHERE player.snapshot_id = lineup.snapshot_id
                    AND player.team_id = lineup.team_id
                    AND player.lineup_role = 'starter'::football.lineup_role
              )
              OR lineup.substitute_count <> (
                  SELECT count(*)
                  FROM football.fixture_historical_lineup_players player
                  WHERE player.snapshot_id = lineup.snapshot_id
                    AND player.team_id = lineup.team_id
                    AND player.lineup_role = 'substitute'::football.lineup_role
              )
          )
    ) THEN
        RAISE EXCEPTION 'historical lineup role counts do not match player rows'
            USING ERRCODE = '23514';
    END IF;

    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER fixture_historical_lineup_snapshot_commit_guard
AFTER INSERT ON football.fixture_historical_lineup_snapshots
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION football.assert_historical_lineup_snapshot_commit_valid();

-- Any provider fetch whose response has become canonical provenance must keep
-- its response identity. `normalized_at` remains intentionally mutable so the
-- importer can atomically mark a successful normalization transaction.
CREATE OR REPLACE FUNCTION source.guard_referenced_provider_fetch_response()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (
        EXISTS (
            SELECT 1 FROM source.season_coverage_snapshots
            WHERE source_fetch_id = OLD.id
        )
        OR EXISTS (
            SELECT 1 FROM source.fixture_provider_status
            WHERE source_fetch_id = OLD.id
        )
        OR EXISTS (
            SELECT 1 FROM football.fixture_historical_lineup_snapshots
            WHERE source_fetch_id = OLD.id
        )
    ) AND (
        NEW.provider_id IS DISTINCT FROM OLD.provider_id
        OR NEW.endpoint IS DISTINCT FROM OLD.endpoint
        OR NEW.request_params IS DISTINCT FROM OLD.request_params
        OR NEW.request_params_sha256 IS DISTINCT FROM OLD.request_params_sha256
        OR NEW.purpose IS DISTINCT FROM OLD.purpose
        OR NEW.response_received_at IS DISTINCT FROM OLD.response_received_at
        OR NEW.http_status IS DISTINCT FROM OLD.http_status
        OR NEW.outcome IS DISTINCT FROM OLD.outcome
        OR NEW.content_sha256 IS DISTINCT FROM OLD.content_sha256
        OR NEW.subject_fixture_id IS DISTINCT FROM OLD.subject_fixture_id
        OR NEW.subject_season_id IS DISTINCT FROM OLD.subject_season_id
        OR NEW.subject_team_id IS DISTINCT FROM OLD.subject_team_id
    ) THEN
        RAISE EXCEPTION 'provider fetch response metadata is immutable after canonical provenance references it'
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END
$$;

ALTER TABLE football.fixture_historical_lineup_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.fixture_historical_lineups ENABLE ROW LEVEL SECURITY;
ALTER TABLE football.fixture_historical_lineup_players ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON football.fixture_historical_lineup_snapshots,
    football.fixture_historical_lineups,
    football.fixture_historical_lineup_players FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION football.guard_historical_lineup_snapshot() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION football.guard_historical_lineup_child() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION football.guard_historical_lineup_coach_provider_ref() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION football.guard_historical_lineup_player_provider_ref() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION football.assert_historical_lineup_snapshot_commit_valid() FROM PUBLIC;

DO $$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format(
                'REVOKE ALL ON TABLE football.fixture_historical_lineup_snapshots, football.fixture_historical_lineups, football.fixture_historical_lineup_players FROM %I',
                role_name
            );
            EXECUTE format('REVOKE EXECUTE ON FUNCTION football.guard_historical_lineup_snapshot() FROM %I', role_name);
            EXECUTE format('REVOKE EXECUTE ON FUNCTION football.guard_historical_lineup_child() FROM %I', role_name);
            EXECUTE format('REVOKE EXECUTE ON FUNCTION football.guard_historical_lineup_coach_provider_ref() FROM %I', role_name);
            EXECUTE format('REVOKE EXECUTE ON FUNCTION football.guard_historical_lineup_player_provider_ref() FROM %I', role_name);
            EXECUTE format('REVOKE EXECUTE ON FUNCTION football.assert_historical_lineup_snapshot_commit_valid() FROM %I', role_name);
        END IF;
    END LOOP;
END
$$;

COMMIT;
