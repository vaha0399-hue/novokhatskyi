-- Additive multi-country / multi-competition foundation.
--
-- This migration deliberately does not add player statistics, events, odds,
-- provider predictions, live scores, live timelines, or prediction features.
-- Existing denormalized country fields remain in place for compatibility.

BEGIN;

CREATE TABLE football.countries (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL CHECK (btrim(name) <> ''),
    flag_url text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    retired_at timestamptz
);

CREATE UNIQUE INDEX countries_active_name_key
    ON football.countries (lower(btrim(name)))
    WHERE retired_at IS NULL;

CREATE TRIGGER countries_touch_updated_at
BEFORE UPDATE ON football.countries
FOR EACH ROW EXECUTE FUNCTION football.touch_updated_at();

CREATE TABLE source.country_provider_refs (
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    external_code text NOT NULL CHECK (btrim(external_code) <> ''),
    country_id bigint NOT NULL REFERENCES football.countries(id) ON DELETE RESTRICT,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, external_code),
    UNIQUE (provider_id, country_id),
    CHECK (last_seen_at >= first_seen_at)
);

ALTER TABLE football.leagues
    ADD COLUMN country_id bigint,
    ADD COLUMN competition_type text,
    ADD CONSTRAINT leagues_country_fk
        FOREIGN KEY (country_id) REFERENCES football.countries(id) ON DELETE RESTRICT,
    ADD CONSTRAINT leagues_competition_type_format_check
        CHECK (
            competition_type IS NULL
            OR (
                competition_type = lower(competition_type)
                AND competition_type = btrim(competition_type)
                AND competition_type ~ '^[a-z][a-z0-9_-]*$'
            )
        );

ALTER TABLE football.teams
    ADD COLUMN country_id bigint,
    ADD CONSTRAINT teams_country_fk
        FOREIGN KEY (country_id) REFERENCES football.countries(id) ON DELETE RESTRICT;

-- The redundant-looking provider column in this key permits child tables to
-- enforce that their provenance fetch belongs to the same provider without a
-- JSON-dependent trigger.
ALTER TABLE source.provider_fetches
    ADD CONSTRAINT provider_fetches_id_provider_key UNIQUE (id, provider_id);

CREATE TABLE source.season_coverage_snapshots (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    provider_id smallint NOT NULL,
    season_id bigint NOT NULL,
    captured_at timestamptz NOT NULL,
    fixture_statistics_supported boolean NOT NULL,
    lineups_supported boolean NOT NULL,
    standings_supported boolean NOT NULL,
    injuries_supported boolean NOT NULL,
    mapping_version text NOT NULL CHECK (btrim(mapping_version) <> ''),
    source_fetch_id bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (provider_id, season_id, source_fetch_id),
    FOREIGN KEY (provider_id, season_id)
        REFERENCES source.season_provider_refs(provider_id, season_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (source_fetch_id, provider_id)
        REFERENCES source.provider_fetches(id, provider_id)
        ON DELETE RESTRICT
);

COMMENT ON TABLE source.season_coverage_snapshots IS
    'Append-only capabilities observed in a validated provider /leagues response. Capability rows are never inferred from normalized football data.';

CREATE INDEX season_coverage_snapshots_latest_idx
    ON source.season_coverage_snapshots (provider_id, season_id, captured_at DESC);

CREATE INDEX season_coverage_snapshots_source_fetch_idx
    ON source.season_coverage_snapshots (source_fetch_id);

CREATE TABLE source.fixture_status_code_mappings (
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    external_code text NOT NULL CHECK (btrim(external_code) <> ''),
    canonical_state football.fixture_lifecycle_state NOT NULL,
    mapping_version text NOT NULL CHECK (btrim(mapping_version) <> ''),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, external_code)
);

CREATE TABLE source.fixture_provider_status (
    provider_id smallint NOT NULL,
    fixture_id bigint NOT NULL,
    status_code text NOT NULL,
    observed_at timestamptz NOT NULL,
    source_fetch_id bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, fixture_id),
    FOREIGN KEY (provider_id, fixture_id)
        REFERENCES source.fixture_provider_refs(provider_id, fixture_id)
        ON DELETE RESTRICT,
    FOREIGN KEY (provider_id, status_code)
        REFERENCES source.fixture_status_code_mappings(provider_id, external_code)
        ON DELETE RESTRICT,
    FOREIGN KEY (source_fetch_id, provider_id)
        REFERENCES source.provider_fetches(id, provider_id)
        ON DELETE RESTRICT
);

COMMENT ON TABLE source.fixture_provider_status IS
    'Latest confirmed provider status only; this is not live status history. Raw long status, elapsed, and extra remain in provider payload provenance.';

CREATE INDEX fixture_provider_status_code_idx
    ON source.fixture_provider_status (provider_id, status_code, observed_at DESC);

CREATE INDEX fixture_provider_status_fixture_idx
    ON source.fixture_provider_status (fixture_id, provider_id);

CREATE INDEX fixture_provider_status_source_fetch_idx
    ON source.fixture_provider_status (source_fetch_id);

CREATE OR REPLACE FUNCTION source.guard_immutable_snapshot()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION '% rows are append-only', TG_TABLE_NAME USING ERRCODE = '55000';
END
$$;

CREATE TRIGGER season_coverage_snapshots_immutable_guard
BEFORE UPDATE OR DELETE ON source.season_coverage_snapshots
FOR EACH ROW EXECUTE FUNCTION source.guard_immutable_snapshot();

CREATE TRIGGER fixture_status_code_mappings_immutable_guard
BEFORE UPDATE OR DELETE ON source.fixture_status_code_mappings
FOR EACH ROW EXECUTE FUNCTION source.guard_immutable_snapshot();

CREATE OR REPLACE FUNCTION source.guard_season_coverage_snapshot()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    provider_fetch source.provider_fetches%ROWTYPE;
BEGIN
    SELECT *
    INTO provider_fetch
    FROM source.provider_fetches
    WHERE id = NEW.source_fetch_id
      AND provider_id = NEW.provider_id;

    IF provider_fetch.outcome IS DISTINCT FROM 'success'::source.fetch_outcome
       OR provider_fetch.endpoint IS DISTINCT FROM '/leagues'
       OR provider_fetch.response_received_at IS NULL
       OR provider_fetch.response_received_at IS DISTINCT FROM NEW.captured_at
       OR provider_fetch.subject_season_id IS DISTINCT FROM NEW.season_id THEN
        RAISE EXCEPTION 'season coverage requires a matching successful season-bound /leagues fetch'
            USING ERRCODE = '23514';
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER season_coverage_snapshots_provenance_guard
BEFORE INSERT ON source.season_coverage_snapshots
FOR EACH ROW EXECUTE FUNCTION source.guard_season_coverage_snapshot();

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
    ) AND (
        NEW.provider_id IS DISTINCT FROM OLD.provider_id
        OR NEW.endpoint IS DISTINCT FROM OLD.endpoint
        OR NEW.response_received_at IS DISTINCT FROM OLD.response_received_at
        OR NEW.http_status IS DISTINCT FROM OLD.http_status
        OR NEW.outcome IS DISTINCT FROM OLD.outcome
        OR NEW.content_sha256 IS DISTINCT FROM OLD.content_sha256
        OR NEW.subject_fixture_id IS DISTINCT FROM OLD.subject_fixture_id
        OR NEW.subject_season_id IS DISTINCT FROM OLD.subject_season_id
    ) THEN
        RAISE EXCEPTION 'provider fetch response metadata is immutable after canonical provenance references it'
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END
$$;

CREATE TRIGGER provider_fetches_referenced_response_guard
BEFORE UPDATE ON source.provider_fetches
FOR EACH ROW EXECUTE FUNCTION source.guard_referenced_provider_fetch_response();

CREATE OR REPLACE FUNCTION source.guard_fixture_provider_status()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    fetch_received_at timestamptz;
    fetch_outcome source.fetch_outcome;
    fetch_endpoint text;
    fetch_subject_fixture_id bigint;
    fetch_subject_season_id bigint;
    mapped_state football.fixture_lifecycle_state;
    fixture_state football.fixture_lifecycle_state;
    fixture_season_id bigint;
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'current provider fixture status cannot be deleted'
            USING ERRCODE = '55000';
    END IF;

    IF TG_OP = 'UPDATE' AND (
        NEW.provider_id IS DISTINCT FROM OLD.provider_id
        OR NEW.fixture_id IS DISTINCT FROM OLD.fixture_id
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
    ) THEN
        RAISE EXCEPTION 'provider fixture status identity is immutable'
            USING ERRCODE = '55000';
    END IF;

    SELECT response_received_at, outcome, endpoint, subject_fixture_id, subject_season_id
    INTO fetch_received_at, fetch_outcome, fetch_endpoint, fetch_subject_fixture_id, fetch_subject_season_id
    FROM source.provider_fetches
    WHERE id = NEW.source_fetch_id
      AND provider_id = NEW.provider_id;

    IF fetch_outcome IS DISTINCT FROM 'success'::source.fetch_outcome
       OR fetch_endpoint IS DISTINCT FROM '/fixtures'
       OR fetch_received_at IS NULL
       OR NEW.observed_at IS DISTINCT FROM fetch_received_at THEN
        RAISE EXCEPTION 'provider fixture status requires a matching successful /fixtures fetch timestamp'
            USING ERRCODE = '23514';
    END IF;

    SELECT canonical_state
    INTO mapped_state
    FROM source.fixture_status_code_mappings
    WHERE provider_id = NEW.provider_id
      AND external_code = NEW.status_code;

    SELECT lifecycle_state, season_id
    INTO fixture_state, fixture_season_id
    FROM football.fixtures
    WHERE id = NEW.fixture_id;

    IF (
        fetch_subject_fixture_id IS NOT NULL
        AND fetch_subject_fixture_id IS DISTINCT FROM NEW.fixture_id
    ) OR (
        fetch_subject_fixture_id IS NULL
        AND fetch_subject_season_id IS DISTINCT FROM fixture_season_id
    ) THEN
        RAISE EXCEPTION 'provider fixture status fetch is not bound to the fixture or its season'
            USING ERRCODE = '23514';
    END IF;

    IF mapped_state IS DISTINCT FROM fixture_state THEN
        RAISE EXCEPTION 'provider fixture status is inconsistent with canonical lifecycle state'
            USING ERRCODE = '23514';
    END IF;

    IF TG_OP = 'UPDATE' AND NEW.observed_at <= OLD.observed_at THEN
        RAISE EXCEPTION 'stale provider fixture status cannot overwrite a newer observation'
            USING ERRCODE = '23514';
    END IF;

    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END
$$;

CREATE TRIGGER fixture_provider_status_guard
BEFORE INSERT OR UPDATE OR DELETE ON source.fixture_provider_status
FOR EACH ROW EXECUTE FUNCTION source.guard_fixture_provider_status();

CREATE OR REPLACE FUNCTION source.assert_fixture_status_lifecycle_consistency()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM source.fixture_provider_status status
        JOIN source.fixture_status_code_mappings mapping
          ON mapping.provider_id = status.provider_id
         AND mapping.external_code = status.status_code
        WHERE status.fixture_id = NEW.id
          AND mapping.canonical_state IS DISTINCT FROM NEW.lifecycle_state
    ) THEN
        RAISE EXCEPTION 'canonical fixture lifecycle and exact provider status must change atomically'
            USING ERRCODE = '23514';
    END IF;

    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER fixture_status_lifecycle_consistency_guard
AFTER INSERT OR UPDATE ON football.fixtures
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION source.assert_fixture_status_lifecycle_consistency();

-- Controlled, repeat-safe canonical country backfill. No provider country code
-- is derived from legacy country_name; the explicit GB-ENG mapping below is
-- created only for the known API-Football Premier League provider reference.
INSERT INTO football.countries (name, flag_url)
SELECT legacy.name, legacy.flag_url
FROM (
    SELECT btrim(country_name) AS name, max(flag_url) AS flag_url
    FROM football.leagues
    WHERE country_name IS NOT NULL AND btrim(country_name) <> ''
    GROUP BY btrim(country_name)
) AS legacy
WHERE NOT EXISTS (
    SELECT 1
    FROM football.countries country
    WHERE lower(btrim(country.name)) = lower(legacy.name)
      AND country.retired_at IS NULL
);

INSERT INTO source.country_provider_refs (
    provider_id, external_code, country_id, first_seen_at, last_seen_at
)
SELECT provider.id, 'GB-ENG', country.id, ref.first_seen_at, ref.last_seen_at
FROM source.providers provider
JOIN source.league_provider_refs ref
  ON ref.provider_id = provider.id
 AND ref.external_id = '39'
JOIN football.leagues league ON league.id = ref.league_id
JOIN football.countries country
  ON lower(btrim(country.name)) = lower(btrim(league.country_name))
 AND country.retired_at IS NULL
WHERE provider.code = 'api-football'
  AND lower(btrim(league.country_name)) = 'england'
ON CONFLICT (provider_id, external_code) DO NOTHING;

UPDATE football.leagues league
SET country_id = country.id
FROM football.countries country
WHERE league.country_id IS NULL
  AND league.country_name IS NOT NULL
  AND lower(btrim(country.name)) = lower(btrim(league.country_name))
  AND country.retired_at IS NULL;

UPDATE football.teams team
SET country_id = country.id
FROM football.countries country
WHERE team.country_id IS NULL
  AND team.country_name IS NOT NULL
  AND lower(btrim(country.name)) = lower(btrim(team.country_name))
  AND country.retired_at IS NULL;

UPDATE football.leagues league
SET competition_type = 'league'
FROM source.providers provider
JOIN source.league_provider_refs ref ON ref.provider_id = provider.id
WHERE league.id = ref.league_id
  AND provider.code = 'api-football'
  AND ref.external_id = '39'
  AND league.competition_type IS NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM football.leagues
        WHERE country_id IS NULL OR competition_type IS NULL
    ) THEN
        RAISE EXCEPTION 'all existing leagues require reviewed country and competition type mappings before migration'
            USING ERRCODE = '23514';
    END IF;
END
$$;

ALTER TABLE football.leagues
    ALTER COLUMN country_id SET NOT NULL,
    ALTER COLUMN competition_type SET NOT NULL;

CREATE INDEX leagues_country_competition_name_idx
    ON football.leagues (country_id, competition_type, name);

CREATE INDEX teams_country_name_idx
    ON football.teams (country_id, name);

-- Only mappings proven by the saved real API-Football samples are seeded.
INSERT INTO source.fixture_status_code_mappings (
    provider_id, external_code, canonical_state, mapping_version
)
SELECT id, mapping.external_code, mapping.canonical_state, 'api-football-v1'
FROM source.providers
CROSS JOIN (
    VALUES
        ('NS', 'scheduled'::football.fixture_lifecycle_state),
        ('FT', 'completed'::football.fixture_lifecycle_state)
) AS mapping(external_code, canonical_state)
WHERE code = 'api-football'
ON CONFLICT (provider_id, external_code) DO NOTHING;

-- Backfill exact statuses from retained, successful raw /fixtures responses.
-- This is a one-time controlled parser, not a JSON-coupled runtime trigger.
WITH raw_observations AS (
    SELECT
        provider_fetch.id AS source_fetch_id,
        provider_fetch.provider_id,
        provider_fetch.response_received_at AS observed_at,
        item #>> '{fixture,id}' AS external_fixture_id,
        item #>> '{fixture,status,short}' AS status_code
    FROM source.provider_fetches provider_fetch
    JOIN source.provider_raw_payloads raw ON raw.fetch_id = provider_fetch.id
    CROSS JOIN LATERAL jsonb_array_elements(
        convert_from(raw.inline_body, 'UTF8')::jsonb -> 'response'
    ) AS item
    WHERE provider_fetch.outcome = 'success'
      AND provider_fetch.endpoint = '/fixtures'
      AND provider_fetch.response_received_at IS NOT NULL
      AND raw.purged_at IS NULL
      AND raw.inline_body IS NOT NULL
      AND raw.content_encoding IS NULL
      AND raw.content_type = 'application/json'
), ranked AS (
    SELECT
        observation.*,
        ref.fixture_id,
        row_number() OVER (
            PARTITION BY observation.provider_id, ref.fixture_id
            ORDER BY observation.observed_at DESC, observation.source_fetch_id DESC
        ) AS rank
    FROM raw_observations observation
    JOIN source.fixture_provider_refs ref
      ON ref.provider_id = observation.provider_id
     AND ref.external_id = observation.external_fixture_id
    JOIN source.fixture_status_code_mappings mapping
      ON mapping.provider_id = observation.provider_id
     AND mapping.external_code = observation.status_code
    JOIN football.fixtures fixture
      ON fixture.id = ref.fixture_id
     AND fixture.lifecycle_state = mapping.canonical_state
)
INSERT INTO source.fixture_provider_status (
    provider_id, fixture_id, status_code, observed_at, source_fetch_id
)
SELECT provider_id, fixture_id, status_code, observed_at, source_fetch_id
FROM ranked
WHERE rank = 1
ON CONFLICT (provider_id, fixture_id) DO NOTHING;

-- The known EPL 2024 installation has a retained season /fixtures payload. If
-- that installation is present, fail atomically rather than silently accepting
-- a partial exact-status backfill or an unreviewed provider status code.
DO $$
DECLARE
    target_fixture_count bigint;
    target_status_count bigint;
    target_has_raw_fixture_payload boolean;
BEGIN
    SELECT count(*)
    INTO target_fixture_count
    FROM football.fixtures fixture
    JOIN football.seasons season ON season.id = fixture.season_id
    JOIN source.league_provider_refs league_ref ON league_ref.league_id = season.league_id
    JOIN source.providers provider ON provider.id = league_ref.provider_id
    WHERE provider.code = 'api-football'
      AND league_ref.external_id = '39'
      AND season.start_year = 2024;

    SELECT EXISTS (
        SELECT 1
        FROM source.provider_fetches provider_fetch
        JOIN source.provider_raw_payloads raw ON raw.fetch_id = provider_fetch.id
        JOIN source.providers provider ON provider.id = provider_fetch.provider_id
        WHERE provider.code = 'api-football'
          AND provider_fetch.endpoint = '/fixtures'
          AND provider_fetch.outcome = 'success'
          AND provider_fetch.request_params @> '{"league": 39, "season": 2024}'::jsonb
          AND raw.purged_at IS NULL
          AND raw.inline_body IS NOT NULL
    ) INTO target_has_raw_fixture_payload;

    SELECT count(*)
    INTO target_status_count
    FROM source.fixture_provider_status status
    JOIN football.fixtures fixture ON fixture.id = status.fixture_id
    JOIN football.seasons season ON season.id = fixture.season_id
    JOIN source.league_provider_refs league_ref
      ON league_ref.provider_id = status.provider_id
     AND league_ref.league_id = season.league_id
    JOIN source.providers provider ON provider.id = status.provider_id
    WHERE provider.code = 'api-football'
      AND league_ref.external_id = '39'
      AND season.start_year = 2024;

    IF target_fixture_count > 0
       AND target_has_raw_fixture_payload
       AND target_status_count <> target_fixture_count THEN
        RAISE EXCEPTION 'EPL 2024 exact provider status backfill is incomplete: % of %',
            target_status_count, target_fixture_count
            USING ERRCODE = '23514';
    END IF;
END
$$;

ALTER TABLE football.countries ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.country_provider_refs ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.season_coverage_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.fixture_status_code_mappings ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.fixture_provider_status ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON football.countries FROM PUBLIC;
REVOKE ALL ON source.country_provider_refs FROM PUBLIC;
REVOKE ALL ON source.season_coverage_snapshots FROM PUBLIC;
REVOKE ALL ON source.fixture_status_code_mappings FROM PUBLIC;
REVOKE ALL ON source.fixture_provider_status FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA football, source FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION source.guard_immutable_snapshot() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION source.guard_season_coverage_snapshot() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION source.guard_referenced_provider_fetch_response() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION source.guard_fixture_provider_status() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION source.assert_fixture_status_lifecycle_consistency() FROM PUBLIC;

DO $$
DECLARE
    role_name text;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['anon', 'authenticated'] LOOP
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = role_name) THEN
            EXECUTE format('REVOKE ALL ON football.countries FROM %I', role_name);
            EXECUTE format('REVOKE ALL ON source.country_provider_refs FROM %I', role_name);
            EXECUTE format('REVOKE ALL ON source.season_coverage_snapshots FROM %I', role_name);
            EXECUTE format('REVOKE ALL ON source.fixture_status_code_mappings FROM %I', role_name);
            EXECUTE format('REVOKE ALL ON source.fixture_provider_status FROM %I', role_name);
            EXECUTE format('REVOKE ALL ON ALL SEQUENCES IN SCHEMA football, source FROM %I', role_name);
            EXECUTE format('REVOKE EXECUTE ON FUNCTION source.guard_immutable_snapshot() FROM %I', role_name);
            EXECUTE format('REVOKE EXECUTE ON FUNCTION source.guard_season_coverage_snapshot() FROM %I', role_name);
            EXECUTE format('REVOKE EXECUTE ON FUNCTION source.guard_referenced_provider_fetch_response() FROM %I', role_name);
            EXECUTE format('REVOKE EXECUTE ON FUNCTION source.guard_fixture_provider_status() FROM %I', role_name);
            EXECUTE format('REVOKE EXECUTE ON FUNCTION source.assert_fixture_status_lifecycle_consistency() FROM %I', role_name);
        END IF;
    END LOOP;
END
$$;

COMMIT;
