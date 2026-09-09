-- Q05 policy-lock migration contract. Runtime contention is covered with two
-- psycopg PostgreSQL connections in the manifest-guarded integration suite.
DO $$
DECLARE
  definition text;
BEGIN
  SELECT pg_get_functiondef(
    'ops.enqueue_repeatable_sync_work_and_checkpoint(bigint,text,jsonb,text,integer,timestamptz,text,text,text,smallint,bigint,timestamptz,timestamptz,timestamptz,timestamptz)'::regprocedure
  ) INTO definition;
  IF position('competition_sync_policies' IN definition) = 0
     OR position('FOR UPDATE' IN definition) = 0
     OR position('expected_policy_version' IN definition) = 0 THEN
    RAISE EXCEPTION 'Q05 policy-locked scheduler transition is missing';
  END IF;
END $$;
