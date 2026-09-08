-- Q03: fenced leases for the opt-in repeatable queue.  Legacy queue clients
-- retain the Q02 functions and columns unchanged.
BEGIN;

ALTER TABLE ops.sync_work_items
    ADD COLUMN lease_token bigint NOT NULL DEFAULT 0,
    ADD COLUMN quarantined_at timestamptz,
    ADD COLUMN quarantine_reason text;
CREATE SEQUENCE ops.sync_work_item_lease_token_seq AS bigint;
REVOKE ALL ON SEQUENCE ops.sync_work_item_lease_token_seq FROM PUBLIC;

ALTER TABLE ops.sync_work_items DROP CONSTRAINT sync_work_items_status_check;
ALTER TABLE ops.sync_work_items
    ADD CONSTRAINT sync_work_items_status_check
    CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'quarantined'));

CREATE OR REPLACE FUNCTION ops.claim_next_repeatable_sync_work_item_with_lease(
    p_lease_owner text,
    p_lease_duration interval DEFAULT interval '5 minutes',
    p_max_attempts integer DEFAULT 5
)
RETURNS TABLE (id bigint, run_id bigint, scope_key text, scope jsonb, checkpoint jsonb,
               attempts integer, job_type text, priority integer, stable_key text,
               entity_key text, execution_key text, lease_token bigint)
LANGUAGE plpgsql AS $$
BEGIN
    IF btrim(coalesce(p_lease_owner, '')) = '' OR p_lease_duration <= interval '0 seconds'
       OR p_max_attempts < 1 THEN
        RAISE EXCEPTION 'lease owner, positive duration, and positive max attempts are required' USING ERRCODE = '22023';
    END IF;
    -- Expired repeatable work is recoverable.  Exhausted work is preserved in
    -- quarantine rather than silently retried forever.
    UPDATE ops.sync_work_items AS item SET status='quarantined', lease_owner=NULL, lease_expires_at=NULL,
        quarantined_at=clock_timestamp(), quarantine_reason=coalesce(last_error, 'attempt limit exceeded')
    WHERE item.job_type <> 'legacy' AND item.status IN ('pending','running') AND item.attempts >= p_max_attempts
      AND (item.status='pending' OR item.lease_expires_at < clock_timestamp());
    UPDATE ops.sync_work_items AS item SET status='pending', lease_owner=NULL, lease_expires_at=NULL
    WHERE item.job_type <> 'legacy' AND item.status='running' AND item.lease_expires_at < clock_timestamp()
      AND item.attempts < p_max_attempts;
    RETURN QUERY
    WITH candidate AS (
      SELECT item.id FROM ops.sync_work_items item
      WHERE item.job_type <> 'legacy' AND item.stable_key <> 'legacy' AND item.stable_key NOT LIKE 'legacy:%'
        AND item.entity_key <> 'legacy' AND item.entity_key NOT LIKE 'legacy:%'
        AND item.execution_key <> 'legacy' AND item.execution_key NOT LIKE 'legacy:%'
        AND item.status='pending' AND item.available_at <= clock_timestamp() AND item.attempts < p_max_attempts
        AND NOT EXISTS (SELECT 1 FROM ops.sync_work_items r WHERE r.status='running'
          AND r.lease_expires_at > clock_timestamp() AND r.execution_key=item.execution_key AND r.id<>item.id)
      ORDER BY item.priority + floor(extract(epoch FROM (clock_timestamp()-item.available_at))/60)::integer DESC,
               item.available_at, item.id FOR UPDATE SKIP LOCKED LIMIT 1
    )
    UPDATE ops.sync_work_items item SET status='running', attempts=item.attempts+1,
      lease_owner=p_lease_owner, lease_expires_at=clock_timestamp()+p_lease_duration,
      lease_token=nextval('ops.sync_work_item_lease_token_seq'), started_at=coalesce(item.started_at,clock_timestamp()), last_error=NULL
    FROM candidate WHERE item.id=candidate.id
    RETURNING item.id,item.run_id,item.scope_key,item.scope,item.checkpoint,item.attempts,item.job_type,
      item.priority,item.stable_key,item.entity_key,item.execution_key,item.lease_token;
EXCEPTION WHEN unique_violation THEN RETURN;
END;
$$;

CREATE OR REPLACE FUNCTION ops.heartbeat_repeatable_sync_work_item(p_item_id bigint, p_lease_owner text,
    p_lease_token bigint, p_lease_duration interval DEFAULT interval '5 minutes') RETURNS boolean
LANGUAGE sql AS $$
 UPDATE ops.sync_work_items SET lease_expires_at=clock_timestamp()+p_lease_duration
 WHERE id=p_item_id AND status='running' AND lease_owner=p_lease_owner AND lease_token=p_lease_token
   AND lease_expires_at > clock_timestamp() AND p_lease_duration > interval '0 seconds' RETURNING true;
$$;
CREATE OR REPLACE FUNCTION ops.checkpoint_repeatable_sync_work_item(p_item_id bigint, p_lease_owner text,
    p_lease_token bigint, p_checkpoint jsonb) RETURNS boolean LANGUAGE sql AS $$
 UPDATE ops.sync_work_items SET checkpoint=p_checkpoint WHERE id=p_item_id AND status='running'
   AND lease_owner=p_lease_owner AND lease_token=p_lease_token AND lease_expires_at > clock_timestamp()
   AND jsonb_typeof(p_checkpoint)='object' RETURNING true;
$$;
CREATE OR REPLACE FUNCTION ops.requeue_repeatable_sync_work_item(p_item_id bigint, p_lease_owner text,
    p_lease_token bigint, p_checkpoint jsonb, p_error text, p_delay interval, p_contract_error boolean DEFAULT false) RETURNS boolean
LANGUAGE sql AS $$
 UPDATE ops.sync_work_items SET status=CASE WHEN p_contract_error THEN 'quarantined' ELSE 'pending' END,
   checkpoint=p_checkpoint,last_error=left(coalesce(p_error,''),500),available_at=clock_timestamp()+p_delay,
   lease_owner=NULL,lease_expires_at=NULL,quarantined_at=CASE WHEN p_contract_error THEN clock_timestamp() END,
   quarantine_reason=CASE WHEN p_contract_error THEN left(coalesce(p_error,''),500) END
 WHERE id=p_item_id AND status='running' AND lease_owner=p_lease_owner AND lease_token=p_lease_token
   AND lease_expires_at > clock_timestamp() AND jsonb_typeof(p_checkpoint)='object' AND p_delay >= interval '0 seconds'
 RETURNING true;
$$;
CREATE OR REPLACE FUNCTION ops.retry_quarantined_repeatable_sync_work_item(p_item_id bigint) RETURNS boolean LANGUAGE sql AS $$
 UPDATE ops.sync_work_items SET status='pending', available_at=clock_timestamp(), lease_owner=NULL,lease_expires_at=NULL,
   lease_token=nextval('ops.sync_work_item_lease_token_seq'), quarantined_at=NULL, quarantine_reason=NULL, last_error=NULL
 WHERE id=p_item_id AND job_type <> 'legacy' AND status='quarantined' RETURNING true;
$$;

-- Call this as the first statement in the *same transaction and connection*
-- that applies domain writes, dependent queue inserts, and completion.
CREATE OR REPLACE FUNCTION ops.guard_repeatable_sync_work_item_lease(p_item_id bigint, p_lease_owner text,
    p_lease_token bigint) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1 FROM ops.sync_work_items WHERE id=p_item_id FOR UPDATE;
  RETURN EXISTS(SELECT 1 FROM ops.sync_work_items WHERE id=p_item_id AND status='running'
    AND lease_owner=p_lease_owner AND lease_token=p_lease_token AND lease_expires_at > clock_timestamp());
END;
$$;
CREATE OR REPLACE FUNCTION ops.complete_repeatable_sync_work_item(p_item_id bigint, p_lease_owner text,
    p_lease_token bigint, p_checkpoint jsonb DEFAULT '{}'::jsonb) RETURNS boolean LANGUAGE sql AS $$
 UPDATE ops.sync_work_items SET status='succeeded',checkpoint=p_checkpoint,finished_at=clock_timestamp(),
  lease_owner=NULL,lease_expires_at=NULL WHERE id=p_item_id AND status='running' AND lease_owner=p_lease_owner
  AND lease_token=p_lease_token AND lease_expires_at > clock_timestamp() AND jsonb_typeof(p_checkpoint)='object'
 RETURNING true;
$$;
REVOKE EXECUTE ON FUNCTION ops.claim_next_repeatable_sync_work_item_with_lease(text,interval,integer),
 ops.heartbeat_repeatable_sync_work_item(bigint,text,bigint,interval), ops.checkpoint_repeatable_sync_work_item(bigint,text,bigint,jsonb),
 ops.requeue_repeatable_sync_work_item(bigint,text,bigint,jsonb,text,interval,boolean), ops.retry_quarantined_repeatable_sync_work_item(bigint),
 ops.guard_repeatable_sync_work_item_lease(bigint,text,bigint), ops.complete_repeatable_sync_work_item(bigint,text,bigint,jsonb) FROM PUBLIC;
COMMIT;
