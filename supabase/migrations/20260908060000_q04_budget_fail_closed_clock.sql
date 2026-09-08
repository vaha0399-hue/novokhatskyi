-- Q04 hardening: no NULL/missing-config fail-open path and clock after lock.
BEGIN;

CREATE OR REPLACE FUNCTION ops.reserve_api_football_request(p_consumer text)
RETURNS TABLE (allowed boolean, reason text, retry_at timestamptz)
LANGUAGE plpgsql AS $$
DECLARE
    config ops.api_football_budget_config;
    state ops.api_football_budget_state;
    now_at timestamptz;
    day_at date;
    minute_at timestamptz;
    consumer_limit integer;
    consumer_used integer;
BEGIN
    IF p_consumer IS NULL OR p_consumer NOT IN ('operations', 'history', 'legacy_manual') THEN
        RAISE EXCEPTION 'known API-Football budget consumer is required' USING ERRCODE = '22023';
    END IF;

    SELECT * INTO config FROM ops.api_football_budget_config WHERE singleton FOR UPDATE;
    IF NOT FOUND OR config.daily_limit IS NULL OR config.minute_limit IS NULL
       OR config.operations_limit IS NULL OR config.history_limit IS NULL
       OR config.legacy_manual_limit IS NULL OR config.protected_reserve IS NULL THEN
        RAISE EXCEPTION 'API-Football budget configuration is unavailable' USING ERRCODE = '55000';
    END IF;

    -- The state-row lock serializes reset and debit.  Read the clock only
    -- after it is acquired: a waiter crossing an UTC/minute boundary must use
    -- the boundary it actually acquired, never the boundary it started in.
    INSERT INTO ops.api_football_budget_state(singleton, daily_window, minute_window)
    VALUES (true, (clock_timestamp() AT TIME ZONE 'UTC')::date,
            date_trunc('minute', clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')
    ON CONFLICT (singleton) DO NOTHING;
    SELECT * INTO state FROM ops.api_football_budget_state WHERE singleton FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'API-Football budget state is unavailable' USING ERRCODE = '55000';
    END IF;
    now_at := clock_timestamp();
    day_at := (now_at AT TIME ZONE 'UTC')::date;
    minute_at := date_trunc('minute', now_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';

    IF state.daily_window <> day_at THEN
        UPDATE ops.api_football_budget_state SET daily_window=day_at, daily_used=0,
          operations_used=0, history_used=0, legacy_manual_used=0 WHERE singleton;
        SELECT * INTO state FROM ops.api_football_budget_state WHERE singleton;
    END IF;
    IF state.minute_window <> minute_at THEN
        UPDATE ops.api_football_budget_state SET minute_window=minute_at, minute_used=0 WHERE singleton;
        SELECT * INTO state FROM ops.api_football_budget_state WHERE singleton;
    END IF;
    IF state.cooldown_until IS NOT NULL AND state.cooldown_until > now_at THEN
        RETURN QUERY SELECT false, 'cooldown', state.cooldown_until; RETURN;
    END IF;
    SELECT CASE p_consumer WHEN 'operations' THEN config.operations_limit WHEN 'history' THEN config.history_limit ELSE config.legacy_manual_limit END,
           CASE p_consumer WHEN 'operations' THEN state.operations_used WHEN 'history' THEN state.history_used ELSE state.legacy_manual_used END
    INTO consumer_limit, consumer_used;
    IF consumer_limit IS NULL OR consumer_used IS NULL THEN
        RAISE EXCEPTION 'API-Football budget state is invalid' USING ERRCODE = '55000';
    END IF;
    IF state.daily_used >= config.daily_limit - config.protected_reserve THEN
        RETURN QUERY SELECT false, 'daily_limit', ((day_at + 1)::timestamp AT TIME ZONE 'UTC'); RETURN;
    END IF;
    IF state.minute_used >= config.minute_limit THEN
        RETURN QUERY SELECT false, 'minute_limit', minute_at + interval '1 minute'; RETURN;
    END IF;
    IF consumer_used >= consumer_limit THEN
        RETURN QUERY SELECT false, p_consumer || '_limit', ((day_at + 1)::timestamp AT TIME ZONE 'UTC'); RETURN;
    END IF;
    UPDATE ops.api_football_budget_state SET daily_used=daily_used+1, minute_used=minute_used+1,
      operations_used=operations_used + CASE WHEN p_consumer='operations' THEN 1 ELSE 0 END,
      history_used=history_used + CASE WHEN p_consumer='history' THEN 1 ELSE 0 END,
      legacy_manual_used=legacy_manual_used + CASE WHEN p_consumer='legacy_manual' THEN 1 ELSE 0 END
    WHERE singleton;
    RETURN QUERY SELECT true, 'reserved'::text, NULL::timestamptz;
END;
$$;

REVOKE EXECUTE ON FUNCTION ops.reserve_api_football_request(text) FROM PUBLIC, anon, authenticated;
COMMIT;
