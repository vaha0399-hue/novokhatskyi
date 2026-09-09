BEGIN;
CREATE TABLE ops.sync_scheduler_analytics_checkpoints (
 provider_id smallint NOT NULL, season_id bigint NOT NULL, entity_key text NOT NULL, input_version text NOT NULL,
 scheduled_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(provider_id,season_id,entity_key,input_version),
 FOREIGN KEY(provider_id,season_id) REFERENCES ops.competition_sync_policies(provider_id,season_id) ON DELETE RESTRICT
);
CREATE FUNCTION ops.enqueue_repeatable_analytics_work_and_checkpoint(p_run_id bigint,p_scope_key text,p_scope jsonb,p_job_type text,p_priority integer,p_available_at timestamptz,p_stable_key text,p_entity_key text,p_execution_key text,p_provider_id smallint,p_season_id bigint,p_input_version text)
RETURNS TABLE(work_item_id bigint,enqueued boolean,checkpoint_advanced boolean) LANGUAGE plpgsql AS $$
DECLARE p ops.competition_sync_policies%ROWTYPE; i bigint; v bigint; n integer;
BEGIN
 i := (p_scope #>> '{_sync_policy,instance_id}')::bigint; v := (p_scope #>> '{_sync_policy,version}')::bigint;
 SELECT * INTO p FROM ops.competition_sync_policies WHERE provider_id=p_provider_id AND season_id=p_season_id FOR UPDATE;
 IF NOT FOUND OR p.policy_instance_id IS DISTINCT FROM i OR p.policy_version IS DISTINCT FROM v OR NOT p.enabled OR NOT (p_job_type=ANY(p.allowed_work_types)) OR NOT (p.refresh_intervals ? p_job_type) OR coalesce(p.coverage->p_job_type->>'state','unknown') <> 'covered' THEN RAISE EXCEPTION 'scheduler policy changed since calculation' USING ERRCODE='55000'; END IF;
 SELECT item.work_item_id,item.enqueued INTO work_item_id,enqueued FROM ops.enqueue_repeatable_sync_work_item(p_run_id,p_scope_key,p_scope,p_job_type,p_priority,p_available_at,p_stable_key,p_entity_key,p_execution_key) AS item;
 INSERT INTO ops.sync_scheduler_analytics_checkpoints(provider_id,season_id,entity_key,input_version) VALUES(p_provider_id,p_season_id,p_entity_key,p_input_version) ON CONFLICT DO NOTHING;
 GET DIAGNOSTICS n=ROW_COUNT; checkpoint_advanced := n>0; RETURN NEXT;
END; $$;
COMMIT;
