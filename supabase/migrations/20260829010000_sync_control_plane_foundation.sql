-- Additive control-plane foundation for resumable, multi-league provider sync.
-- This migration stores observations only; it does not encode provider plan limits.
BEGIN;

CREATE TABLE ops.sync_runs (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    operation text NOT NULL CHECK (btrim(operation) <> ''),
    scope jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(scope) = 'object'),
    status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    checkpoint jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(checkpoint) = 'object'),
    started_at timestamptz,
    finished_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (finished_at IS NULL OR started_at IS NOT NULL),
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE INDEX sync_runs_provider_status_idx ON ops.sync_runs (provider_id, status, created_at DESC);

CREATE TABLE ops.sync_work_items (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id bigint NOT NULL REFERENCES ops.sync_runs(id) ON DELETE CASCADE,
    scope_key text NOT NULL CHECK (btrim(scope_key) <> ''),
    scope jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(scope) = 'object'),
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    checkpoint jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(checkpoint) = 'object'),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    lease_owner text,
    lease_expires_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (run_id, scope_key),
    CHECK ((status = 'running') = (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CHECK (finished_at IS NULL OR started_at IS NOT NULL),
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE INDEX sync_work_items_claim_idx
    ON ops.sync_work_items (run_id, available_at, id)
    WHERE status IN ('pending', 'running');

CREATE OR REPLACE FUNCTION ops.touch_sync_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER sync_runs_touch_updated_at
BEFORE UPDATE ON ops.sync_runs FOR EACH ROW EXECUTE FUNCTION ops.touch_sync_updated_at();
CREATE TRIGGER sync_work_items_touch_updated_at
BEFORE UPDATE ON ops.sync_work_items FOR EACH ROW EXECUTE FUNCTION ops.touch_sync_updated_at();

CREATE OR REPLACE FUNCTION ops.claim_next_sync_work_item(
    p_run_id bigint,
    p_lease_owner text,
    p_lease_duration interval DEFAULT interval '5 minutes'
)
RETURNS TABLE (id bigint, scope_key text, scope jsonb, checkpoint jsonb, attempts integer)
LANGUAGE plpgsql AS $$
BEGIN
    IF btrim(coalesce(p_lease_owner, '')) = '' OR p_lease_duration <= interval '0 seconds' THEN
        RAISE EXCEPTION 'lease owner and positive lease duration are required' USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    WITH candidate AS (
        SELECT item.id
        FROM ops.sync_work_items AS item
        WHERE item.run_id = p_run_id
          AND item.available_at <= clock_timestamp()
          AND (item.status = 'pending'
               OR (item.status = 'running' AND item.lease_expires_at < clock_timestamp()))
        ORDER BY item.id
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    UPDATE ops.sync_work_items AS item
    SET status = 'running',
        attempts = item.attempts + 1,
        lease_owner = p_lease_owner,
        lease_expires_at = clock_timestamp() + p_lease_duration,
        started_at = coalesce(item.started_at, clock_timestamp()),
        last_error = NULL
    FROM candidate
    WHERE item.id = candidate.id
    RETURNING item.id, item.scope_key, item.scope, item.checkpoint, item.attempts;
END;
$$;

CREATE OR REPLACE FUNCTION ops.renew_sync_work_item(
    p_item_id bigint, p_lease_owner text, p_lease_duration interval DEFAULT interval '5 minutes'
)
RETURNS boolean LANGUAGE sql AS $$
    UPDATE ops.sync_work_items
    SET lease_expires_at = clock_timestamp() + p_lease_duration
    WHERE id = p_item_id AND status = 'running' AND lease_owner = p_lease_owner
      AND lease_expires_at >= clock_timestamp()
    RETURNING true;
$$;

CREATE OR REPLACE FUNCTION ops.checkpoint_sync_work_item(
    p_item_id bigint, p_lease_owner text, p_checkpoint jsonb
)
RETURNS boolean LANGUAGE sql AS $$
    UPDATE ops.sync_work_items
    SET checkpoint = p_checkpoint
    WHERE id = p_item_id AND status = 'running' AND lease_owner = p_lease_owner
      AND lease_expires_at >= clock_timestamp()
      AND jsonb_typeof(p_checkpoint) = 'object'
    RETURNING true;
$$;

CREATE OR REPLACE FUNCTION ops.complete_sync_work_item(
    p_item_id bigint, p_lease_owner text, p_checkpoint jsonb DEFAULT '{}'::jsonb
)
RETURNS boolean LANGUAGE sql AS $$
    UPDATE ops.sync_work_items
    SET status = 'succeeded', checkpoint = p_checkpoint, finished_at = clock_timestamp(),
        lease_owner = NULL, lease_expires_at = NULL
    WHERE id = p_item_id AND status = 'running' AND lease_owner = p_lease_owner
      AND lease_expires_at >= clock_timestamp() AND jsonb_typeof(p_checkpoint) = 'object'
    RETURNING true;
$$;

CREATE TABLE source.provider_rate_limit_state (
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    endpoint text NOT NULL CHECK (btrim(endpoint) <> ''),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    header_values jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(header_values) = 'object'),
    limit_value integer CHECK (limit_value IS NULL OR limit_value >= 0),
    remaining_value integer CHECK (remaining_value IS NULL OR remaining_value >= 0),
    reset_value text,
    PRIMARY KEY (provider_id, endpoint)
);

CREATE OR REPLACE FUNCTION source.observe_provider_rate_limit(
    p_provider_id smallint, p_endpoint text, p_headers jsonb
)
RETURNS source.provider_rate_limit_state
LANGUAGE plpgsql AS $$
DECLARE
    result source.provider_rate_limit_state;
    normalized jsonb := '{}'::jsonb;
    pair record;
    parsed integer;
BEGIN
    IF btrim(coalesce(p_endpoint, '')) = '' OR jsonb_typeof(p_headers) <> 'object' THEN
        RAISE EXCEPTION 'endpoint and object headers are required' USING ERRCODE = '22023';
    END IF;
    FOR pair IN SELECT lower(key) AS key, value FROM jsonb_each_text(p_headers) LOOP
        normalized := normalized || jsonb_build_object(pair.key, pair.value);
    END LOOP;
    parsed := CASE WHEN normalized ? 'x-ratelimit-limit' AND (normalized->>'x-ratelimit-limit') ~ '^[0-9]+$'
                   THEN (normalized->>'x-ratelimit-limit')::integer END;
    INSERT INTO source.provider_rate_limit_state
        (provider_id, endpoint, observed_at, header_values, limit_value, remaining_value, reset_value)
    VALUES (p_provider_id, p_endpoint, clock_timestamp(), normalized, parsed,
            CASE WHEN normalized ? 'x-ratelimit-remaining' AND (normalized->>'x-ratelimit-remaining') ~ '^[0-9]+$'
                 THEN (normalized->>'x-ratelimit-remaining')::integer END,
            coalesce(normalized->>'x-ratelimit-reset', normalized->>'retry-after'))
    ON CONFLICT (provider_id, endpoint) DO UPDATE SET
        observed_at = excluded.observed_at, header_values = excluded.header_values,
        limit_value = excluded.limit_value, remaining_value = excluded.remaining_value,
        reset_value = excluded.reset_value
    RETURNING * INTO result;
    RETURN result;
END;
$$;

ALTER TABLE ops.sync_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.sync_work_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE source.provider_rate_limit_state ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON ops.sync_runs, ops.sync_work_items, source.provider_rate_limit_state FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA ops, source FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION ops.touch_sync_updated_at(), ops.claim_next_sync_work_item(bigint, text, interval), ops.renew_sync_work_item(bigint, text, interval), ops.checkpoint_sync_work_item(bigint, text, jsonb), ops.complete_sync_work_item(bigint, text, jsonb), source.observe_provider_rate_limit(smallint, text, jsonb) FROM PUBLIC;

COMMIT;
