-- Q05: durable schedule progress.  A checkpoint means "enqueued", never
-- "executed"; Q03 owns execution success separately on the work item.
BEGIN;

CREATE TABLE ops.sync_scheduler_checkpoints (
    provider_id smallint NOT NULL,
    season_id bigint NOT NULL,
    work_type text NOT NULL CHECK (btrim(work_type) <> ''),
    last_scheduled_window_end timestamptz NOT NULL,
    next_deadline timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, season_id, work_type),
    FOREIGN KEY (provider_id, season_id)
        REFERENCES ops.competition_sync_policies(provider_id, season_id) ON DELETE RESTRICT,
    CHECK (next_deadline > last_scheduled_window_end)
);

-- The advisory lock covers the absent-row case; a row lock alone cannot stop
-- two first schedulers from both observing no checkpoint.  Queue identity
-- still supplies Q02 durable deduplication.
CREATE FUNCTION ops.enqueue_repeatable_sync_work_and_checkpoint(
    p_run_id bigint,
    p_scope_key text,
    p_scope jsonb,
    p_job_type text,
    p_priority integer,
    p_available_at timestamptz,
    p_stable_key text,
    p_entity_key text,
    p_execution_key text,
    p_provider_id smallint,
    p_season_id bigint,
    p_expected_last_scheduled_window_end timestamptz,
    p_expected_next_deadline timestamptz,
    p_next_last_scheduled_window_end timestamptz,
    p_next_deadline timestamptz
)
RETURNS TABLE (work_item_id bigint, enqueued boolean, checkpoint_advanced boolean)
LANGUAGE plpgsql AS $$
DECLARE
    current_checkpoint ops.sync_scheduler_checkpoints%ROWTYPE;
BEGIN
    IF p_next_deadline <= p_next_last_scheduled_window_end THEN
        RAISE EXCEPTION 'scheduler checkpoint deadline must follow its scheduled window' USING ERRCODE = '22023';
    END IF;
    IF (p_expected_last_scheduled_window_end IS NULL) <> (p_expected_next_deadline IS NULL) THEN
        RAISE EXCEPTION 'scheduler checkpoint expectation is incomplete' USING ERRCODE = '22023';
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
    ELSIF p_expected_last_scheduled_window_end IS NOT NULL THEN
        RETURN QUERY SELECT NULL::bigint, false, false;
        RETURN;
    END IF;

    SELECT item.work_item_id, item.enqueued INTO work_item_id, enqueued
      FROM ops.enqueue_repeatable_sync_work_item(
        p_run_id,p_scope_key,p_scope,p_job_type,p_priority,p_available_at,p_stable_key,p_entity_key,p_execution_key
      ) AS item;
    INSERT INTO ops.sync_scheduler_checkpoints(provider_id,season_id,work_type,last_scheduled_window_end,next_deadline)
      VALUES(p_provider_id,p_season_id,p_job_type,p_next_last_scheduled_window_end,p_next_deadline)
    ON CONFLICT (provider_id,season_id,work_type) DO UPDATE
      SET last_scheduled_window_end=EXCLUDED.last_scheduled_window_end,
          next_deadline=EXCLUDED.next_deadline, updated_at=clock_timestamp();
    checkpoint_advanced := true;
    RETURN NEXT;
END;
$$;

REVOKE ALL ON ops.sync_scheduler_checkpoints FROM PUBLIC, anon, authenticated;
REVOKE EXECUTE ON FUNCTION ops.enqueue_repeatable_sync_work_and_checkpoint(
    bigint,text,jsonb,text,integer,timestamptz,text,text,text,smallint,bigint,timestamptz,timestamptz,timestamptz,timestamptz
) FROM PUBLIC, anon, authenticated;

COMMIT;
