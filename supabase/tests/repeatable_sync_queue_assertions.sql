-- Run after all migrations on an isolated database.  This deliberately uses
-- two rows with the same execution group to prove the blocked version survives.
DO $$
DECLARE p smallint; r1 bigint; r2 bigint; legacy_run bigint; first_id bigint; second_id bigint; legacy_id bigint; deleted_legacy_id bigint; claimed bigint;
BEGIN
    SELECT id INTO p FROM source.providers ORDER BY id LIMIT 1;
    IF p IS NULL THEN INSERT INTO source.providers(code, name) VALUES ('q02-assertion-provider', 'Q02 assertion provider') RETURNING id INTO p; END IF;
    INSERT INTO ops.sync_runs(provider_id, operation) VALUES (p, 'q02-a') RETURNING id INTO r1;
    INSERT INTO ops.sync_runs(provider_id, operation) VALUES (p, 'q02-b') RETURNING id INTO r2;

    SELECT work_item_id INTO first_id FROM ops.enqueue_repeatable_sync_work_item(
        r1, 'scope-a', '{}'::jsonb, 'metrics', 100000, clock_timestamp(),
        'recalculation:metrics:' || p || ':1:team:1:v1', 'team:1', 'entity:team:1');
    IF (SELECT enqueued FROM ops.enqueue_repeatable_sync_work_item(
        r2, 'scope-a-repeat', '{}'::jsonb, 'metrics', 9, clock_timestamp(),
        'recalculation:metrics:' || p || ':1:team:1:v1', 'team:1', 'entity:team:1')) THEN
        RAISE EXCEPTION 'durable key was duplicated';
    END IF;
    SELECT work_item_id INTO second_id FROM ops.enqueue_repeatable_sync_work_item(
        r2, 'scope-b', '{}'::jsonb, 'metrics', 99999, clock_timestamp(),
        'recalculation:metrics:' || p || ':1:team:1:v2', 'team:1', 'entity:team:1');
    SELECT id INTO claimed FROM ops.claim_next_repeatable_sync_work_item_with_lease('q02-worker', interval '1 minute');
    IF claimed IS DISTINCT FROM first_id THEN RAISE EXCEPTION 'expected first item claim'; END IF;
    IF NOT EXISTS (SELECT 1 FROM ops.sync_work_items WHERE id=second_id AND status='pending') THEN
        RAISE EXCEPTION 'blocked input version was lost';
    END IF;
    IF EXISTS (SELECT 1 FROM ops.sync_work_items WHERE execution_key='entity:team:1' AND status='running' AND id <> first_id) THEN
        RAISE EXCEPTION 'conflicting execution group ran concurrently';
    END IF;
    IF NOT ops.complete_repeatable_sync_work_item(first_id, 'q02-worker',
        (SELECT lease_token FROM ops.sync_work_items WHERE id=first_id), '{}'::jsonb) THEN RAISE EXCEPTION 'completion failed'; END IF;
    IF (SELECT enqueued FROM ops.enqueue_repeatable_sync_work_item(
        r2, 'scope-a-after-complete', '{}'::jsonb, 'metrics', 9, clock_timestamp(),
        'recalculation:metrics:' || p || ':1:team:1:v1', 'team:1', 'entity:team:1')) THEN
        RAISE EXCEPTION 'completed durable key was re-enqueued';
    END IF;
    SELECT id INTO claimed FROM ops.claim_next_repeatable_sync_work_item_with_lease('q02-worker-2', interval '1 minute');
    IF claimed IS DISTINCT FROM second_id THEN RAISE EXCEPTION 'blocked version was not subsequently claimable'; END IF;

    INSERT INTO ops.sync_runs(provider_id, operation) VALUES (p, 'q02-legacy-claim') RETURNING id INTO legacy_run;
    INSERT INTO ops.sync_work_items(run_id, scope_key, scope, priority)
    VALUES (legacy_run, 'legacy-high-priority', '{}'::jsonb, 1000000) RETURNING id INTO legacy_id;
    IF (SELECT id FROM ops.claim_next_repeatable_sync_work_item_with_lease('q02-worker-3', interval '1 minute')) = legacy_id THEN
        RAISE EXCEPTION 'repeatable claim captured a legacy row';
    END IF;
    IF (SELECT id FROM ops.claim_next_sync_work_item(legacy_run, 'q02-legacy-worker', interval '1 minute')) IS DISTINCT FROM legacy_id THEN
        RAISE EXCEPTION 'legacy row was unavailable to legacy claim';
    END IF;
    BEGIN
        PERFORM ops.enqueue_repeatable_sync_work_item(r2, 'legacy-forbidden', '{}'::jsonb, 'legacy', 0, clock_timestamp(), 'key', 'entity', 'execution');
        RAISE EXCEPTION 'repeatable enqueue accepted legacy job type';
    EXCEPTION WHEN invalid_parameter_value THEN NULL;
    END;
    BEGIN
        PERFORM ops.enqueue_repeatable_sync_work_item(r2, 'legacy-key-forbidden', '{}'::jsonb, 'metrics', 0, clock_timestamp(), 'legacy', 'entity', 'execution');
        RAISE EXCEPTION 'repeatable enqueue accepted bare legacy identity';
    EXCEPTION WHEN invalid_parameter_value THEN NULL;
    END;
    BEGIN
        UPDATE ops.sync_work_items SET stable_key='changed' WHERE id=first_id;
        RAISE EXCEPTION 'repeatable identity was mutable';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
    BEGIN
        DELETE FROM ops.sync_work_items WHERE id=first_id;
        RAISE EXCEPTION 'repeatable history was deletable';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
    DELETE FROM ops.sync_work_items WHERE id=legacy_id RETURNING id INTO deleted_legacy_id;
    IF deleted_legacy_id IS DISTINCT FROM legacy_id
       OR EXISTS (SELECT 1 FROM ops.sync_work_items WHERE id=legacy_id) THEN
        RAISE EXCEPTION 'legacy row was not deleted by DELETE RETURNING';
    END IF;
END $$;
BEGIN;
GRANT USAGE ON SCHEMA ops TO anon;
GRANT SELECT ON ops.sync_work_items TO anon;
SET LOCAL ROLE anon;
DO $$ BEGIN IF (SELECT count(*) FROM ops.sync_work_items) <> 0 THEN RAISE EXCEPTION 'RLS exposed queue rows to anon'; END IF; END $$;
RESET ROLE;
ROLLBACK;
