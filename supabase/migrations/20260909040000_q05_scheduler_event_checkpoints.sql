-- Q05 fixture/event progress is independent from season-wide periodic state.
BEGIN;

CREATE TABLE ops.sync_scheduler_event_checkpoints (
    provider_id smallint NOT NULL,
    season_id bigint NOT NULL,
    work_type text NOT NULL CHECK (btrim(work_type) <> ''),
    entity_key text NOT NULL CHECK (btrim(entity_key) <> ''),
    stable_key text NOT NULL CHECK (btrim(stable_key) <> ''),
    scheduled_window_start timestamptz NOT NULL,
    scheduled_window_end timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, season_id, work_type, entity_key, stable_key),
    FOREIGN KEY (provider_id, season_id) REFERENCES ops.competition_sync_policies(provider_id, season_id) ON DELETE RESTRICT,
    CHECK (scheduled_window_end > scheduled_window_start)
);

CREATE FUNCTION ops.enqueue_repeatable_sync_work_and_event_checkpoint(
    p_run_id bigint, p_scope_key text, p_scope jsonb, p_job_type text, p_priority integer,
    p_available_at timestamptz, p_stable_key text, p_entity_key text, p_execution_key text,
    p_provider_id smallint, p_season_id bigint
) RETURNS TABLE (work_item_id bigint, enqueued boolean, checkpoint_advanced boolean)
LANGUAGE plpgsql AS $$
DECLARE
    policy ops.competition_sync_policies%ROWTYPE;
    expected_instance bigint;
    expected_version bigint;
    window_start timestamptz;
    window_end timestamptz;
    inserted_count integer;
BEGIN
    BEGIN
        expected_instance := (p_scope #>> '{_sync_policy,instance_id}')::bigint;
        expected_version := (p_scope #>> '{_sync_policy,version}')::bigint;
        window_start := (p_scope #>> '{window_start}')::timestamptz;
        window_end := (p_scope #>> '{window_end}')::timestamptz;
    EXCEPTION WHEN invalid_text_representation OR invalid_datetime_format THEN
        RAISE EXCEPTION 'scheduler event has malformed policy fingerprint or window' USING ERRCODE='22023';
    END;
    IF expected_instance IS NULL OR expected_version IS NULL OR window_start IS NULL OR window_end IS NULL OR window_end <= window_start THEN
        RAISE EXCEPTION 'scheduler event requires a policy fingerprint and ordered window' USING ERRCODE='22023';
    END IF;
    SELECT * INTO policy FROM ops.competition_sync_policies WHERE provider_id=p_provider_id AND season_id=p_season_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'scheduler policy no longer exists' USING ERRCODE='55000'; END IF;
    IF policy.policy_instance_id IS DISTINCT FROM expected_instance THEN RAISE EXCEPTION 'scheduler policy instance changed since calculation' USING ERRCODE='55000'; END IF;
    IF policy.policy_version IS DISTINCT FROM expected_version THEN RAISE EXCEPTION 'scheduler policy version changed since calculation' USING ERRCODE='55000'; END IF;
    IF NOT policy.enabled OR (policy.paused_until IS NOT NULL AND policy.paused_until > clock_timestamp())
       OR NOT (p_job_type = ANY(policy.allowed_work_types)) OR NOT (policy.refresh_intervals ? p_job_type)
       OR (p_job_type <> 'coverage_refresh' AND coalesce(policy.coverage->p_job_type->>'state','unknown') <> 'covered') THEN
        RAISE EXCEPTION 'scheduler policy no longer authorizes work' USING ERRCODE='55000';
    END IF;
    PERFORM pg_advisory_xact_lock(p_provider_id::integer, hashtext(p_season_id::text || ':' || p_job_type || ':' || p_entity_key));
    SELECT item.work_item_id,item.enqueued INTO work_item_id,enqueued FROM ops.enqueue_repeatable_sync_work_item(
      p_run_id,p_scope_key,p_scope,p_job_type,p_priority,p_available_at,p_stable_key,p_entity_key,p_execution_key) AS item;
    INSERT INTO ops.sync_scheduler_event_checkpoints(provider_id,season_id,work_type,entity_key,stable_key,scheduled_window_start,scheduled_window_end)
      VALUES(p_provider_id,p_season_id,p_job_type,p_entity_key,p_stable_key,window_start,window_end)
    ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS inserted_count = ROW_COUNT;
    checkpoint_advanced := inserted_count > 0;
    RETURN NEXT;
END;
$$;
REVOKE ALL ON ops.sync_scheduler_event_checkpoints FROM PUBLIC, anon, authenticated;
COMMIT;
