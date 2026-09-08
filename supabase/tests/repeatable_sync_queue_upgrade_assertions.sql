DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM ops.sync_work_items WHERE scope_key='q02-legacy-scope' AND stable_key LIKE 'legacy:%' AND entity_key LIKE 'legacy:%' AND execution_key LIKE 'legacy:%') THEN RAISE EXCEPTION 'pre-Q02 queue item was not preserved and backfilled'; END IF;
    IF NOT (SELECT relrowsecurity FROM pg_class WHERE oid='ops.sync_work_items'::regclass) THEN RAISE EXCEPTION 'work-item RLS was lost'; END IF;
    IF to_regclass('ops.sync_work_items_stable_key_uidx') IS NULL OR to_regclass('ops.sync_work_items_running_execution_key_uidx') IS NULL THEN RAISE EXCEPTION 'Q02 durable/conflict indexes missing'; END IF;
    BEGIN
        DELETE FROM ops.sync_runs WHERE operation='q02-legacy-upgrade';
        RAISE EXCEPTION 'run deletion unexpectedly erased queue history';
    EXCEPTION WHEN restrict_violation THEN NULL;
    END;
    BEGIN
        INSERT INTO ops.sync_work_items(run_id,scope_key,stable_key,entity_key,execution_key) SELECT id, 'invalid-q02', '', 'entity', 'execution' FROM ops.sync_runs WHERE operation='q02-legacy-upgrade';
        RAISE EXCEPTION 'blank stable key constraint missing';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
    BEGIN
        UPDATE ops.sync_work_items SET stable_key='rewritten' WHERE scope_key='q02-legacy-scope';
    EXCEPTION WHEN check_violation THEN
        RAISE EXCEPTION 'legacy queue row unexpectedly became immutable';
    END;
END $$;
