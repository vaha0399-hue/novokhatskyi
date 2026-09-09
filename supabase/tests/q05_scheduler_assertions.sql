-- Q05 additive schema contract. Two-session behavior is exercised by the
-- manifest-guarded backend integration suite.
DO $$
BEGIN
  IF to_regclass('ops.sync_scheduler_checkpoints') IS NULL THEN
    RAISE EXCEPTION 'Q05 scheduler checkpoint table is missing';
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE pronamespace='ops'::regnamespace AND proname='enqueue_repeatable_sync_work_and_checkpoint') THEN
    RAISE EXCEPTION 'Q05 atomic enqueue/checkpoint function is missing';
  END IF;
END $$;
