-- This row represents a deployment already upgraded through accepted Q03.
DO $$
DECLARE p smallint; r bigint;
BEGIN
  SELECT id INTO p FROM source.providers WHERE code='q02-legacy-provider';
  SELECT id INTO r FROM ops.sync_runs WHERE operation='q02-legacy-upgrade';
  PERFORM ops.enqueue_repeatable_sync_work_item(r, 'q03-upgrade-scope', '{}'::jsonb,
    'q03-upgrade', 0, clock_timestamp(), 'q03-upgrade-stable', 'q03-upgrade-entity', 'q03-upgrade-execution');
  UPDATE ops.sync_work_items SET attempts=5
  WHERE stable_key='q03-upgrade-stable';
END $$;
