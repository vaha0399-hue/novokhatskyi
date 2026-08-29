\set ON_ERROR_STOP on
BEGIN;
DO $$
DECLARE p smallint; r bigint; i bigint; claimed bigint; duplicate_failed boolean := false;
BEGIN
    SELECT id INTO p FROM source.providers ORDER BY id LIMIT 1;
    IF p IS NULL THEN INSERT INTO source.providers(code, name) VALUES ('test-sync-provider', 'Disposable test provider') RETURNING id INTO p; END IF;
    IF to_regclass('ops.sync_runs') IS NULL OR to_regclass('ops.sync_work_items') IS NULL OR to_regclass('source.provider_rate_limit_state') IS NULL THEN RAISE EXCEPTION 'sync foundation tables missing'; END IF;
    IF to_regclass('source.provider_fetches') IS NULL OR to_regprocedure('source.guard_immutable_snapshot()') IS NULL THEN RAISE EXCEPTION 'existing provider provenance/immutability guard missing'; END IF;
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid = 'ops.sync_runs'::regclass)
       OR NOT (SELECT relrowsecurity FROM pg_class WHERE oid = 'ops.sync_work_items'::regclass)
       OR NOT (SELECT relrowsecurity FROM pg_class WHERE oid = 'source.provider_rate_limit_state'::regclass) THEN RAISE EXCEPTION 'RLS missing'; END IF;
    INSERT INTO ops.sync_runs(provider_id, operation) VALUES (p, 'validation') RETURNING id INTO r;
    INSERT INTO ops.sync_work_items(run_id, scope_key, scope) VALUES (r, 'league:1', '{"league":1}') RETURNING id INTO i;
    BEGIN INSERT INTO ops.sync_work_items(run_id, scope_key) VALUES (r, 'league:1'); EXCEPTION WHEN unique_violation THEN duplicate_failed := true; END;
    IF NOT duplicate_failed THEN RAISE EXCEPTION 'scoped work item is not idempotent'; END IF;
    SELECT id INTO claimed FROM ops.claim_next_sync_work_item(r, 'worker-a', interval '1 minute');
    IF claimed IS DISTINCT FROM i THEN RAISE EXCEPTION 'claim did not return expected item'; END IF;
    IF ops.claim_next_sync_work_item(r, 'worker-b', interval '1 minute') IS NOT NULL THEN RAISE EXCEPTION 'lease claim was not exclusive'; END IF;
    IF NOT ops.checkpoint_sync_work_item(i, 'worker-a', '{"page":2}') THEN RAISE EXCEPTION 'checkpoint failed'; END IF;
    IF ops.checkpoint_sync_work_item(i, 'stale-worker', '{"page":99}') THEN RAISE EXCEPTION 'stale checkpoint writer was accepted'; END IF;
    IF NOT ops.complete_sync_work_item(i, 'worker-a', '{"page":3}') THEN RAISE EXCEPTION 'completion failed'; END IF;
    IF (source.observe_provider_rate_limit(p, '/test', '{"X-RateLimit-Remaining":"7","Retry-After":"12"}')).remaining_value <> 7 THEN RAISE EXCEPTION 'header observation failed'; END IF;
    IF (source.observe_provider_rate_limit(p, '/malformed', '{"X-RateLimit-Remaining":"unknown"}')).remaining_value IS NOT NULL THEN RAISE EXCEPTION 'malformed header was treated as a limit'; END IF;
    IF position('SKIP LOCKED' IN pg_get_functiondef('ops.claim_next_sync_work_item(bigint,text,interval)'::regprocedure)) = 0 THEN RAISE EXCEPTION 'claim function lacks SKIP LOCKED'; END IF;
END $$;
ROLLBACK;
