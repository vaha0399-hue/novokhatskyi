BEGIN;

ALTER TABLE ops.fixture_analytics_recalculation_windows
    ADD COLUMN source_window_end timestamptz;

ALTER TABLE ops.fixture_analytics_recalculation_windows
    ADD CONSTRAINT fixture_analytics_recalculation_windows_source_order
    CHECK (source_window_end IS NULL OR source_window_end < window_end);

CREATE INDEX fixture_analytics_recalculation_windows_source_idx
    ON ops.fixture_analytics_recalculation_windows (fixture_id,source_window_end)
    WHERE source_window_end IS NOT NULL AND accepted_at IS NULL;

CREATE OR REPLACE FUNCTION ops.capture_fixture_analytics_recalculation_window()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    v_source_window_end timestamptz;
    v_target_window_end timestamptz;
    v_pending_window_end timestamptz;
    v_inserted integer;
BEGIN
    IF NEW.last_source_fetch_id IS NULL OR NEW.last_seen_at IS NULL
       OR (TG_OP = 'UPDATE' AND NEW.last_source_fetch_id IS NOT DISTINCT FROM OLD.last_source_fetch_id) THEN
        RETURN NEW;
    END IF;

    v_source_window_end := date_trunc('minute', NEW.last_seen_at) + interval '1 minute';
    SELECT analytics_window.window_end INTO v_pending_window_end
      FROM ops.fixture_analytics_recalculation_windows analytics_window
     WHERE analytics_window.fixture_id=NEW.id
       AND analytics_window.accepted_at IS NULL
       AND coalesce(analytics_window.source_window_end,analytics_window.window_end)=v_source_window_end
     ORDER BY analytics_window.window_end
     LIMIT 1
     FOR UPDATE;
    IF FOUND THEN
        UPDATE ops.fixture_analytics_recalculation_windows
           SET latest_source_fetch_id=NEW.last_source_fetch_id,observed_at=NEW.last_seen_at
         WHERE fixture_id=NEW.id AND window_end=v_pending_window_end AND accepted_at IS NULL
           AND (NEW.last_seen_at > observed_at
                OR (NEW.last_seen_at = observed_at AND NEW.last_source_fetch_id > latest_source_fetch_id));
        RETURN NEW;
    END IF;

    v_target_window_end := v_source_window_end;
    LOOP
        INSERT INTO ops.fixture_analytics_recalculation_windows(
            fixture_id,window_end,latest_source_fetch_id,observed_at,source_window_end
        ) VALUES (
            NEW.id,v_target_window_end,NEW.last_source_fetch_id,NEW.last_seen_at,
            CASE WHEN v_target_window_end=v_source_window_end THEN NULL ELSE v_source_window_end END
        ) ON CONFLICT (fixture_id,window_end) DO NOTHING;
        GET DIAGNOSTICS v_inserted=ROW_COUNT;
        IF v_inserted > 0 THEN
            RETURN NEW;
        END IF;
        SELECT greatest(v_target_window_end,max(analytics_window.window_end)) + interval '1 minute'
          INTO v_target_window_end
          FROM ops.fixture_analytics_recalculation_windows analytics_window
         WHERE analytics_window.fixture_id=NEW.id;
    END LOOP;
END $$;

COMMIT;
