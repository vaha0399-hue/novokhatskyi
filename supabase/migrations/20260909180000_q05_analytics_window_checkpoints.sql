BEGIN;

CREATE TABLE ops.fixture_analytics_recalculation_windows (
    fixture_id bigint NOT NULL REFERENCES football.fixtures(id) ON DELETE RESTRICT,
    window_end timestamptz NOT NULL,
    latest_source_fetch_id bigint NOT NULL REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    observed_at timestamptz NOT NULL,
    accepted_input_version text,
    accepted_at timestamptz,
    PRIMARY KEY (fixture_id, window_end),
    CHECK ((accepted_input_version IS NULL) = (accepted_at IS NULL))
);

CREATE INDEX fixture_analytics_recalculation_windows_due_idx
    ON ops.fixture_analytics_recalculation_windows (window_end)
    WHERE accepted_at IS NULL;

CREATE OR REPLACE FUNCTION ops.guard_fixture_analytics_recalculation_window()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.accepted_at IS NOT NULL THEN
        RAISE EXCEPTION 'accepted analytics windows are immutable' USING ERRCODE='55000';
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END $$;

CREATE TRIGGER fixture_analytics_recalculation_windows_guard
BEFORE UPDATE OR DELETE ON ops.fixture_analytics_recalculation_windows
FOR EACH ROW EXECUTE FUNCTION ops.guard_fixture_analytics_recalculation_window();

CREATE OR REPLACE FUNCTION ops.capture_fixture_analytics_recalculation_window()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    v_window_end timestamptz;
BEGIN
    IF NEW.last_source_fetch_id IS NULL OR NEW.last_seen_at IS NULL
       OR (TG_OP = 'UPDATE' AND NEW.last_source_fetch_id IS NOT DISTINCT FROM OLD.last_source_fetch_id) THEN
        RETURN NEW;
    END IF;
    v_window_end := date_trunc('minute', NEW.last_seen_at) + interval '1 minute';
    INSERT INTO ops.fixture_analytics_recalculation_windows(
        fixture_id,window_end,latest_source_fetch_id,observed_at
    ) VALUES (NEW.id,v_window_end,NEW.last_source_fetch_id,NEW.last_seen_at)
    ON CONFLICT (fixture_id,window_end) DO UPDATE
       SET latest_source_fetch_id=excluded.latest_source_fetch_id,
           observed_at=excluded.observed_at
     WHERE ops.fixture_analytics_recalculation_windows.accepted_at IS NULL;
    RETURN NEW;
END $$;

CREATE TRIGGER fixtures_capture_analytics_recalculation_window
AFTER INSERT OR UPDATE OF last_source_fetch_id ON football.fixtures
FOR EACH ROW EXECUTE FUNCTION ops.capture_fixture_analytics_recalculation_window();

CREATE FUNCTION ops.enqueue_repeatable_analytics_work_and_window_checkpoint(
    p_run_id bigint,p_scope_key text,p_scope jsonb,p_job_type text,p_priority integer,p_available_at timestamptz,
    p_stable_key text,p_entity_key text,p_execution_key text,p_provider_id smallint,p_season_id bigint,p_input_version text,
    p_window_fixture_id bigint,p_window_end timestamptz,p_window_input_version text
)
RETURNS TABLE(work_item_id bigint,enqueued boolean,checkpoint_advanced boolean) LANGUAGE plpgsql AS $$
DECLARE
    p ops.competition_sync_policies%ROWTYPE;
    i bigint;
    v bigint;
    n integer;
    current_window_version bigint;
BEGIN
    i := (p_scope #>> '{_sync_policy,instance_id}')::bigint;
    v := (p_scope #>> '{_sync_policy,version}')::bigint;
    SELECT * INTO p FROM ops.competition_sync_policies
     WHERE provider_id=p_provider_id AND season_id=p_season_id FOR UPDATE;
    IF NOT FOUND OR p.policy_instance_id IS DISTINCT FROM i OR p.policy_version IS DISTINCT FROM v
       OR NOT p.enabled OR NOT (p_job_type=ANY(p.allowed_work_types)) OR NOT (p.refresh_intervals ? p_job_type)
       OR coalesce(p.coverage->p_job_type->>'state','unknown') <> 'covered' THEN
        RAISE EXCEPTION 'scheduler policy changed since calculation' USING ERRCODE='55000';
    END IF;
    IF p_window_fixture_id IS NOT NULL THEN
        SELECT latest_source_fetch_id INTO current_window_version
          FROM ops.fixture_analytics_recalculation_windows
         WHERE fixture_id=p_window_fixture_id AND window_end=p_window_end AND accepted_at IS NULL
         FOR UPDATE;
        IF NOT FOUND OR current_window_version::text IS DISTINCT FROM p_window_input_version THEN
            work_item_id := NULL;
            enqueued := false;
            checkpoint_advanced := false;
            RETURN NEXT;
            RETURN;
        END IF;
    END IF;
    SELECT item.work_item_id,item.enqueued INTO work_item_id,enqueued
      FROM ops.enqueue_repeatable_sync_work_item(
        p_run_id,p_scope_key,p_scope,p_job_type,p_priority,p_available_at,p_stable_key,p_entity_key,p_execution_key
      ) AS item;
    INSERT INTO ops.sync_scheduler_analytics_checkpoints(provider_id,season_id,entity_key,input_version)
      VALUES(p_provider_id,p_season_id,p_entity_key,p_input_version)
      ON CONFLICT DO NOTHING;
    GET DIAGNOSTICS n=ROW_COUNT;
    checkpoint_advanced := n>0;
    IF p_window_fixture_id IS NOT NULL THEN
        UPDATE ops.fixture_analytics_recalculation_windows
           SET accepted_input_version=p_input_version,accepted_at=clock_timestamp()
         WHERE fixture_id=p_window_fixture_id AND window_end=p_window_end
           AND latest_source_fetch_id::text=p_window_input_version AND accepted_at IS NULL;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'analytics window changed during enqueue' USING ERRCODE='40001';
        END IF;
    END IF;
    RETURN NEXT;
END $$;

ALTER TABLE ops.fixture_analytics_recalculation_windows ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON ops.fixture_analytics_recalculation_windows FROM PUBLIC;

COMMIT;
