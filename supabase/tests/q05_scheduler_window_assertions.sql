-- Structural contract only; behavioral scheduler contention is in the
-- manifest-guarded Python integration suite.
DO $$
DECLARE
  definition text;
BEGIN
  SELECT pg_get_functiondef(
    'ops.enqueue_repeatable_sync_work_and_checkpoint(bigint,text,jsonb,text,integer,timestamptz,text,text,text,smallint,bigint,timestamptz,timestamptz,timestamptz,timestamptz)'::regprocedure
  ) INTO definition;
  IF position('work_window_start' IN definition) = 0
     OR position('work_window_end' IN definition) = 0
     OR position('queued work window must start' IN definition) = 0
     OR position('checkpoint end must equal queued' IN definition) = 0 THEN
    RAISE EXCEPTION 'Q05 window/checkpoint contract is missing';
  END IF;
END $$;
