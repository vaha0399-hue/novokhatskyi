-- Q02: durable, cross-run work identity for the existing control-plane queue.
-- Existing run-scoped clients retain their columns and claim function unchanged.
BEGIN;

ALTER TABLE ops.sync_work_items
    ADD COLUMN job_type text NOT NULL DEFAULT 'legacy',
    ADD COLUMN priority integer NOT NULL DEFAULT 0,
    ADD COLUMN stable_key text,
    ADD COLUMN entity_key text,
    ADD COLUMN execution_key text;

-- Pre-Q02 rows remain addressable and do not accidentally conflict with new work.
UPDATE ops.sync_work_items
SET stable_key = 'legacy:' || id::text,
    entity_key = 'legacy:' || id::text,
    execution_key = 'legacy:' || id::text
WHERE stable_key IS NULL;

-- Identity values are assigned before row triggers.  The reviewed legacy Cup
-- and seasonal writers omit Q02 keys, so give them distinct durable legacy
-- identities without changing their insert statements or execution flow.
CREATE OR REPLACE FUNCTION ops.fill_legacy_sync_work_item_keys()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.stable_key IS NULL THEN NEW.stable_key := 'legacy:' || NEW.id::text; END IF;
    IF NEW.entity_key IS NULL THEN NEW.entity_key := 'legacy:' || NEW.id::text; END IF;
    IF NEW.execution_key IS NULL THEN NEW.execution_key := 'legacy:' || NEW.id::text; END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER sync_work_items_fill_legacy_keys
BEFORE INSERT ON ops.sync_work_items
FOR EACH ROW EXECUTE FUNCTION ops.fill_legacy_sync_work_item_keys();

ALTER TABLE ops.sync_work_items
    ALTER COLUMN stable_key SET NOT NULL,
    ALTER COLUMN entity_key SET NOT NULL,
    ALTER COLUMN execution_key SET NOT NULL,
    ADD CONSTRAINT sync_work_items_job_type_nonblank CHECK (btrim(job_type) <> ''),
    ADD CONSTRAINT sync_work_items_stable_key_nonblank CHECK (btrim(stable_key) <> ''),
    ADD CONSTRAINT sync_work_items_entity_key_nonblank CHECK (btrim(entity_key) <> ''),
    ADD CONSTRAINT sync_work_items_execution_key_nonblank CHECK (btrim(execution_key) <> '');

CREATE UNIQUE INDEX sync_work_items_stable_key_uidx ON ops.sync_work_items (stable_key);
-- A running item reserves its execution group.  Later revisions remain pending,
-- rather than being discarded, until the conflicting writer finishes.
CREATE UNIQUE INDEX sync_work_items_running_execution_key_uidx
    ON ops.sync_work_items (execution_key) WHERE status = 'running';
CREATE INDEX sync_work_items_repeatable_claim_idx
    ON ops.sync_work_items (available_at, priority DESC, id) WHERE status = 'pending';

-- A run cannot be deleted while it owns history, preventing a completed
-- durable key from disappearing.
ALTER TABLE ops.sync_work_items DROP CONSTRAINT sync_work_items_run_id_fkey;
ALTER TABLE ops.sync_work_items
    ADD CONSTRAINT sync_work_items_run_id_fkey
    FOREIGN KEY (run_id) REFERENCES ops.sync_runs(id) ON DELETE RESTRICT;

CREATE OR REPLACE FUNCTION ops.enqueue_repeatable_sync_work_item(
    p_run_id bigint,
    p_scope_key text,
    p_scope jsonb,
    p_job_type text,
    p_priority integer,
    p_available_at timestamptz,
    p_stable_key text,
    p_entity_key text,
    p_execution_key text
)
RETURNS TABLE (work_item_id bigint, enqueued boolean)
LANGUAGE plpgsql AS $$
BEGIN
    IF btrim(coalesce(p_scope_key, '')) = '' OR btrim(coalesce(p_job_type, '')) = ''
       OR btrim(coalesce(p_stable_key, '')) = '' OR btrim(coalesce(p_entity_key, '')) = ''
       OR btrim(coalesce(p_execution_key, '')) = '' OR jsonb_typeof(p_scope) <> 'object'
       OR p_available_at IS NULL THEN
        RAISE EXCEPTION 'repeatable work item requires nonblank keys, due time, and object scope' USING ERRCODE = '22023';
    END IF;

    INSERT INTO ops.sync_work_items
        (run_id, scope_key, scope, job_type, priority, available_at, stable_key, entity_key, execution_key)
    VALUES
        (p_run_id, p_scope_key, p_scope, p_job_type, p_priority, p_available_at,
         p_stable_key, p_entity_key, p_execution_key)
    ON CONFLICT (stable_key) DO NOTHING
    RETURNING id, true INTO work_item_id, enqueued;

    IF NOT FOUND THEN
        SELECT id, false INTO work_item_id, enqueued
        FROM ops.sync_work_items WHERE stable_key = p_stable_key;
    END IF;
    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION ops.claim_next_repeatable_sync_work_item(
    p_lease_owner text,
    p_lease_duration interval DEFAULT interval '5 minutes'
)
RETURNS TABLE (id bigint, run_id bigint, scope_key text, scope jsonb, checkpoint jsonb,
               attempts integer, job_type text, priority integer, stable_key text,
               entity_key text, execution_key text)
LANGUAGE plpgsql AS $$
BEGIN
    IF btrim(coalesce(p_lease_owner, '')) = '' OR p_lease_duration <= interval '0 seconds' THEN
        RAISE EXCEPTION 'lease owner and positive lease duration are required' USING ERRCODE = '22023';
    END IF;

    -- One priority point is earned per minute waiting.  This is bounded only by
    -- time, so a continuous high-priority stream cannot starve an old item.
    RETURN QUERY
    WITH candidate AS (
        SELECT item.id
        FROM ops.sync_work_items AS item
        WHERE item.status = 'pending'
          AND item.available_at <= clock_timestamp()
          AND NOT EXISTS (
              SELECT 1 FROM ops.sync_work_items AS running
              WHERE running.status = 'running'
                AND running.execution_key = item.execution_key
          )
        ORDER BY item.priority + floor(extract(epoch FROM (clock_timestamp() - item.available_at)) / 60)::integer DESC,
                 item.available_at, item.id
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    UPDATE ops.sync_work_items AS item
    SET status = 'running', attempts = item.attempts + 1,
        lease_owner = p_lease_owner, lease_expires_at = clock_timestamp() + p_lease_duration,
        started_at = coalesce(item.started_at, clock_timestamp()), last_error = NULL
    FROM candidate
    WHERE item.id = candidate.id
    RETURNING item.id, item.run_id, item.scope_key, item.scope, item.checkpoint,
              item.attempts, item.job_type, item.priority, item.stable_key,
              item.entity_key, item.execution_key;
EXCEPTION WHEN unique_violation THEN
    -- A concurrent claim of another item in the same execution group won.  Its
    -- pending sibling is intentionally retained for a later claim.
    RETURN;
END;
$$;

REVOKE EXECUTE ON FUNCTION ops.enqueue_repeatable_sync_work_item(bigint, text, jsonb, text, integer, timestamptz, text, text, text),
    ops.claim_next_repeatable_sync_work_item(text, interval) FROM PUBLIC;

COMMIT;
