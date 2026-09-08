-- Reviewed, per-provider canonical-season permission policies for future sync adapters.
-- This migration creates no scheduler, queue, quota governor, or production entrypoint.
BEGIN;

CREATE FUNCTION ops.has_distinct_nonblank_texts(p_values text[])
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT cardinality(p_values) > 0
       AND NOT EXISTS (SELECT 1 FROM unnest(p_values) AS items(value) WHERE btrim(items.value) = '')
       AND cardinality(p_values) = (SELECT count(DISTINCT items.value) FROM unnest(p_values) AS items(value));
$$;

CREATE FUNCTION ops.has_valid_coverage_observations(p_coverage jsonb)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
    entry record;
    observed_on date;
BEGIN
    IF jsonb_typeof(p_coverage) <> 'object' THEN
        RETURN false;
    END IF;
    FOR entry IN SELECT key, value FROM jsonb_each(p_coverage) LOOP
        IF btrim(entry.key) = ''
           OR jsonb_typeof(entry.value) <> 'object'
           OR NOT (entry.value ? 'state')
           OR jsonb_typeof(entry.value->'state') <> 'string'
           OR entry.value->>'state' NOT IN ('unknown', 'covered', 'not_covered')
           OR NOT (entry.value ? 'observed_on')
           OR jsonb_typeof(entry.value->'observed_on') <> 'string'
           OR entry.value->>'observed_on' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN
            RETURN false;
        END IF;
        BEGIN
            observed_on := (entry.value->>'observed_on')::date;
        EXCEPTION WHEN others THEN
            RETURN false;
        END;
        IF NOT isfinite(observed_on) OR to_char(observed_on, 'YYYY-MM-DD') <> entry.value->>'observed_on' THEN
            RETURN false;
        END IF;
    END LOOP;
    RETURN true;
END;
$$;

CREATE FUNCTION ops.has_valid_refresh_intervals(p_intervals jsonb)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
DECLARE
    entry record;
BEGIN
    IF jsonb_typeof(p_intervals) <> 'object' OR p_intervals = '{}'::jsonb THEN
        RETURN false;
    END IF;
    FOR entry IN SELECT key, value FROM jsonb_each(p_intervals) LOOP
        IF btrim(entry.key) = ''
           OR jsonb_typeof(entry.value) <> 'object'
           OR NOT (entry.value ? 'value')
           OR jsonb_typeof(entry.value->'value') <> 'number'
           OR (entry.value->>'value') !~ '^[1-9][0-9]*$'
           OR NOT (entry.value ? 'unit')
           OR jsonb_typeof(entry.value->'unit') <> 'string'
           OR entry.value->>'unit' NOT IN ('second', 'minute', 'hour', 'day', 'week') THEN
            RETURN false;
        END IF;
    END LOOP;
    RETURN true;
END;
$$;

CREATE FUNCTION ops.refresh_intervals_match_work_types(p_work_types text[], p_intervals jsonb)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT (SELECT count(*) FROM jsonb_object_keys(p_intervals)) = cardinality(p_work_types)
       AND NOT EXISTS (
           SELECT 1 FROM unnest(p_work_types) AS work_type
           WHERE NOT p_intervals ? work_type
       );
$$;

CREATE TABLE ops.competition_sync_policies (
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    season_id bigint NOT NULL REFERENCES football.seasons(id) ON DELETE RESTRICT,
    policy_instance_id bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
    enabled boolean NOT NULL DEFAULT false,
    allowed_work_types text[] NOT NULL CHECK (ops.has_distinct_nonblank_texts(allowed_work_types)),
    coverage jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (ops.has_valid_coverage_observations(coverage)),
    refresh_intervals jsonb NOT NULL CHECK (ops.has_valid_refresh_intervals(refresh_intervals)),
    priority integer NOT NULL DEFAULT 0 CHECK (priority BETWEEN -1000 AND 1000),
    history_depth_seasons integer NOT NULL DEFAULT 0 CHECK (history_depth_seasons >= 0),
    policy_version bigint NOT NULL DEFAULT 1 CHECK (policy_version > 0),
    paused_until timestamptz,
    pause_reason text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, season_id),
    FOREIGN KEY (provider_id, season_id)
        REFERENCES source.season_provider_refs(provider_id, season_id) ON DELETE RESTRICT,
    CHECK (ops.refresh_intervals_match_work_types(allowed_work_types, refresh_intervals)),
    CHECK (paused_until IS NULL OR btrim(coalesce(pause_reason, '')) <> '')
);

CREATE INDEX competition_sync_policies_season_idx
    ON ops.competition_sync_policies (season_id, provider_id);
CREATE INDEX competition_sync_policies_enabled_idx
    ON ops.competition_sync_policies (provider_id, season_id)
    WHERE enabled;

CREATE FUNCTION ops.version_competition_sync_policy()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.policy_instance_id IS DISTINCT FROM OLD.policy_instance_id THEN
        RAISE EXCEPTION 'competition sync policy instance is immutable' USING ERRCODE = '23514';
    END IF;
    NEW.policy_version := OLD.policy_version + 1;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER competition_sync_policies_version_on_update
BEFORE UPDATE ON ops.competition_sync_policies
FOR EACH ROW EXECUTE FUNCTION ops.version_competition_sync_policy();

ALTER TABLE ops.competition_sync_policies ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON ops.competition_sync_policies FROM PUBLIC, anon, authenticated;
REVOKE EXECUTE ON FUNCTION ops.has_distinct_nonblank_texts(text[]), ops.has_valid_coverage_observations(jsonb),
    ops.has_valid_refresh_intervals(jsonb), ops.refresh_intervals_match_work_types(text[], jsonb),
    ops.version_competition_sync_policy() FROM PUBLIC, anon, authenticated;

COMMIT;
