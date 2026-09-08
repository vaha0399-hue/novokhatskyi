-- Q03 hardening for deployments already at the accepted lease migration.
BEGIN;

ALTER TABLE ops.sync_work_items
    ADD COLUMN attempts_in_budget integer NOT NULL DEFAULT 0;

-- Existing Q03 rows had only ``attempts``.  Preserve that consumed budget on
-- upgrade: only the explicit retry function may open a fresh retry cycle.
UPDATE ops.sync_work_items
SET attempts_in_budget = attempts;

CREATE OR REPLACE FUNCTION ops.claim_next_repeatable_sync_work_item_with_lease(
    p_lease_owner text, p_lease_duration interval DEFAULT interval '5 minutes', p_max_attempts integer DEFAULT 5
)
RETURNS TABLE (id bigint, run_id bigint, scope_key text, scope jsonb, checkpoint jsonb,
               attempts integer, job_type text, priority integer, stable_key text,
               entity_key text, execution_key text, lease_token bigint)
LANGUAGE plpgsql AS $$
BEGIN
    IF btrim(coalesce(p_lease_owner, '')) = '' OR p_lease_duration <= interval '0 seconds' OR p_max_attempts < 1 THEN
        RAISE EXCEPTION 'lease owner, positive duration, and positive max attempts are required' USING ERRCODE = '22023';
    END IF;
    UPDATE ops.sync_work_items AS item SET status='quarantined', lease_owner=NULL, lease_expires_at=NULL,
        quarantined_at=clock_timestamp(), quarantine_reason=coalesce(last_error, 'attempt limit exceeded')
    WHERE item.job_type <> 'legacy' AND item.status IN ('pending','running') AND item.attempts_in_budget >= p_max_attempts
      AND (item.status='pending' OR item.lease_expires_at < clock_timestamp());
    UPDATE ops.sync_work_items AS item SET status='pending', lease_owner=NULL, lease_expires_at=NULL
    WHERE item.job_type <> 'legacy' AND item.status='running' AND item.lease_expires_at < clock_timestamp()
      AND item.attempts_in_budget < p_max_attempts;
    RETURN QUERY WITH candidate AS (
      SELECT item.id FROM ops.sync_work_items item
      WHERE item.job_type <> 'legacy' AND item.stable_key <> 'legacy' AND item.stable_key NOT LIKE 'legacy:%'
        AND item.entity_key <> 'legacy' AND item.entity_key NOT LIKE 'legacy:%'
        AND item.execution_key <> 'legacy' AND item.execution_key NOT LIKE 'legacy:%'
        AND item.status='pending' AND item.available_at <= clock_timestamp() AND item.attempts_in_budget < p_max_attempts
        AND NOT EXISTS (SELECT 1 FROM ops.sync_work_items r WHERE r.status='running'
          AND r.lease_expires_at > clock_timestamp() AND r.execution_key=item.execution_key AND r.id<>item.id)
      ORDER BY item.priority + floor(extract(epoch FROM (clock_timestamp()-item.available_at))/60)::integer DESC,
               item.available_at, item.id FOR UPDATE SKIP LOCKED LIMIT 1
    )
    UPDATE ops.sync_work_items item SET status='running', attempts=item.attempts+1,
      attempts_in_budget=item.attempts_in_budget+1, lease_owner=p_lease_owner,
      lease_expires_at=clock_timestamp()+p_lease_duration, lease_token=nextval('ops.sync_work_item_lease_token_seq'),
      started_at=coalesce(item.started_at,clock_timestamp()), last_error=NULL
    FROM candidate WHERE item.id=candidate.id
    RETURNING item.id,item.run_id,item.scope_key,item.scope,item.checkpoint,item.attempts,item.job_type,
      item.priority,item.stable_key,item.entity_key,item.execution_key,item.lease_token;
EXCEPTION WHEN unique_violation THEN RETURN;
END;
$$;

CREATE OR REPLACE FUNCTION ops.retry_quarantined_repeatable_sync_work_item(p_item_id bigint) RETURNS boolean LANGUAGE sql AS $$
 UPDATE ops.sync_work_items SET status='pending', available_at=clock_timestamp(), lease_owner=NULL,lease_expires_at=NULL,
   attempts_in_budget=0, lease_token=nextval('ops.sync_work_item_lease_token_seq'), quarantined_at=NULL,
   quarantine_reason=NULL, last_error=NULL
 WHERE id=p_item_id AND job_type <> 'legacy' AND status='quarantined' RETURNING true;
$$;

REVOKE EXECUTE ON FUNCTION ops.claim_next_repeatable_sync_work_item_with_lease(text,interval,integer),
 ops.retry_quarantined_repeatable_sync_work_item(bigint) FROM PUBLIC;
COMMIT;
