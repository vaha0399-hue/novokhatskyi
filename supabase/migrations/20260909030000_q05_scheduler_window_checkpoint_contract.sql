-- Q05: bind the immutable Q02 window stored in scope to scheduler progress.
BEGIN;

CREATE OR REPLACE FUNCTION ops.enqueue_repeatable_sync_work_and_checkpoint(
    p_run_id bigint, p_scope_key text, p_scope jsonb, p_job_type text, p_priority integer,
    p_available_at timestamptz, p_stable_key text, p_entity_key text, p_execution_key text,
    p_provider_id smallint, p_season_id bigint, p_expected_last_scheduled_window_end timestamptz,
    p_expected_next_deadline timestamptz, p_next_last_scheduled_window_end timestamptz,
    p_next_deadline timestamptz
) RETURNS TABLE (work_item_id bigint, enqueued boolean, checkpoint_advanced boolean)
LANGUAGE plpgsql AS $$
DECLARE
    current_checkpoint ops.sync_scheduler_checkpoints%ROWTYPE;
    policy ops.competition_sync_policies%ROWTYPE;
    expected_policy_instance_id bigint;
    expected_policy_version bigint;
    work_window_start timestamptz;
    work_window_end timestamptz;
BEGIN
    BEGIN
        expected_policy_instance_id := (p_scope #>> '{_sync_policy,instance_id}')::bigint;
        expected_policy_version := (p_scope #>> '{_sync_policy,version}')::bigint;
        work_window_start := (p_scope #>> '{window_start}')::timestamptz;
        work_window_end := (p_scope #>> '{window_end}')::timestamptz;
    EXCEPTION WHEN invalid_text_representation OR invalid_datetime_format THEN
        RAISE EXCEPTION 'scheduler work has malformed policy fingerprint or window' USING ERRCODE='22023';
    END;
    IF expected_policy_instance_id IS NULL OR expected_policy_version IS NULL
       OR work_window_start IS NULL OR work_window_end IS NULL
       OR work_window_end <= work_window_start THEN
        RAISE EXCEPTION 'scheduler work requires a policy fingerprint and ordered window' USING ERRCODE='22023';
    END IF;
    IF work_window_end IS DISTINCT FROM p_next_last_scheduled_window_end THEN
        RAISE EXCEPTION 'scheduler checkpoint end must equal queued work window end' USING ERRCODE='22023';
    END IF;
    IF p_next_deadline <= p_next_last_scheduled_window_end THEN
        RAISE EXCEPTION 'scheduler checkpoint deadline must follow its scheduled window' USING ERRCODE='22023';
    END IF;
    IF (p_expected_last_scheduled_window_end IS NULL) <> (p_expected_next_deadline IS NULL) THEN
        RAISE EXCEPTION 'scheduler checkpoint expectation is incomplete' USING ERRCODE='22023';
    END IF;

    -- Serialize a current-policy check with Q02 enqueue and checkpoint advance.
    SELECT * INTO policy FROM ops.competition_sync_policies
      WHERE provider_id=p_provider_id AND season_id=p_season_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'scheduler policy no longer exists' USING ERRCODE='55000';
    END IF;
    IF policy.policy_instance_id IS DISTINCT FROM expected_policy_instance_id THEN
        RAISE EXCEPTION 'scheduler policy instance changed since calculation' USING ERRCODE='55000';
    END IF;
    IF policy.policy_version IS DISTINCT FROM expected_policy_version THEN
        RAISE EXCEPTION 'scheduler policy version changed since calculation' USING ERRCODE='55000';
    END IF;
    IF NOT policy.enabled
       OR (policy.paused_until IS NOT NULL AND policy.paused_until > clock_timestamp())
       OR NOT (p_job_type = ANY(policy.allowed_work_types))
       OR NOT (policy.refresh_intervals ? p_job_type)
       OR (p_job_type <> 'coverage_refresh' AND coalesce(policy.coverage->p_job_type->>'state','unknown') <> 'covered') THEN
        RAISE EXCEPTION 'scheduler policy no longer authorizes work' USING ERRCODE='55000';
    END IF;

    PERFORM pg_advisory_xact_lock(p_provider_id::integer, hashtext(p_season_id::text || ':' || p_job_type));
    SELECT * INTO current_checkpoint FROM ops.sync_scheduler_checkpoints
      WHERE provider_id=p_provider_id AND season_id=p_season_id AND work_type=p_job_type FOR UPDATE;
    IF FOUND THEN
        IF current_checkpoint.last_scheduled_window_end IS DISTINCT FROM p_expected_last_scheduled_window_end
           OR current_checkpoint.next_deadline IS DISTINCT FROM p_expected_next_deadline THEN
            RETURN QUERY SELECT NULL::bigint, false, false;
            RETURN;
        END IF;
        IF work_window_start IS DISTINCT FROM current_checkpoint.last_scheduled_window_end THEN
            RAISE EXCEPTION 'queued work window must start at saved scheduler boundary' USING ERRCODE='22023';
        END IF;
        IF p_next_last_scheduled_window_end <= current_checkpoint.last_scheduled_window_end THEN
            RAISE EXCEPTION 'scheduler checkpoint cannot move its scheduled boundary backward' USING ERRCODE='22023';
        END IF;
    ELSIF p_expected_last_scheduled_window_end IS NOT NULL THEN
        RETURN QUERY SELECT NULL::bigint, false, false;
        RETURN;
    END IF;

    SELECT item.work_item_id,item.enqueued INTO work_item_id,enqueued FROM ops.enqueue_repeatable_sync_work_item(
      p_run_id,p_scope_key,p_scope,p_job_type,p_priority,p_available_at,p_stable_key,p_entity_key,p_execution_key) AS item;
    INSERT INTO ops.sync_scheduler_checkpoints(provider_id,season_id,work_type,last_scheduled_window_end,next_deadline)
      VALUES(p_provider_id,p_season_id,p_job_type,p_next_last_scheduled_window_end,p_next_deadline)
    ON CONFLICT (provider_id,season_id,work_type) DO UPDATE SET last_scheduled_window_end=EXCLUDED.last_scheduled_window_end,
      next_deadline=EXCLUDED.next_deadline,updated_at=clock_timestamp();
    checkpoint_advanced := true;
    RETURN NEXT;
END;
$$;
COMMIT;
