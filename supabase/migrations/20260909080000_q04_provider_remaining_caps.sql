-- Q04: a provider-reported positive remaining value is a decreasing shared cap.
BEGIN;

ALTER TABLE ops.api_football_budget_state
    ADD COLUMN provider_daily_remaining integer CHECK (provider_daily_remaining >= 0),
    ADD COLUMN provider_minute_remaining integer CHECK (provider_minute_remaining >= 0);

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
    INSERT INTO ops.api_football_budget_state(singleton, daily_window, minute_window)
    VALUES (true, (clock_timestamp() AT TIME ZONE 'UTC')::date,
            date_trunc('minute', clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')
    ON CONFLICT (singleton) DO NOTHING;
    SELECT * INTO state FROM ops.api_football_budget_state WHERE singleton FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'API-Football budget state is unavailable' USING ERRCODE = '55000'; END IF;
    now_at := clock_timestamp();
    day_at := (now_at AT TIME ZONE 'UTC')::date;
    minute_at := date_trunc('minute', now_at AT TIME ZONE 'UTC') AT TIME ZONE 'UTC';
    IF state.daily_window <> day_at THEN
        UPDATE ops.api_football_budget_state SET daily_window=day_at, daily_used=0,
          operations_used=0, history_used=0, legacy_manual_used=0 WHERE singleton;
        SELECT * INTO state FROM ops.api_football_budget_state WHERE singleton;
    END IF;
    IF state.minute_window <> minute_at THEN
        UPDATE ops.api_football_budget_state SET minute_window=minute_at, minute_used=0,
          provider_minute_remaining=NULL WHERE singleton;
        SELECT * INTO state FROM ops.api_football_budget_state WHERE singleton;
    END IF;
    IF state.provider_daily_exhausted OR state.provider_daily_remaining = 0 THEN
        RETURN QUERY SELECT false, 'provider_daily_exhausted', NULL::timestamptz; RETURN;
    END IF;
    IF state.provider_minute_remaining = 0 THEN
        RETURN QUERY SELECT false, 'provider_minute_limit', minute_at + interval '1 minute'; RETURN;
    END IF;
    IF state.cooldown_until IS NOT NULL AND state.cooldown_until > now_at THEN
        RETURN QUERY SELECT false, 'cooldown', state.cooldown_until; RETURN;
    END IF;
    SELECT CASE p_consumer WHEN 'operations' THEN config.operations_limit WHEN 'history' THEN config.history_limit ELSE config.legacy_manual_limit END,
           CASE p_consumer WHEN 'operations' THEN state.operations_used WHEN 'history' THEN state.history_used ELSE state.legacy_manual_used END
    INTO consumer_limit, consumer_used;
    IF consumer_limit IS NULL OR consumer_used IS NULL THEN RAISE EXCEPTION 'API-Football budget state is invalid' USING ERRCODE = '55000'; END IF;
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
      legacy_manual_used=legacy_manual_used + CASE WHEN p_consumer='legacy_manual' THEN 1 ELSE 0 END,
      provider_daily_remaining=CASE WHEN provider_daily_remaining IS NULL THEN NULL ELSE provider_daily_remaining-1 END,
      provider_minute_remaining=CASE WHEN provider_minute_remaining IS NULL THEN NULL ELSE provider_minute_remaining-1 END
    WHERE singleton;
    RETURN QUERY SELECT true, 'reserved'::text, NULL::timestamptz;
END;
$$;

CREATE OR REPLACE FUNCTION ops.observe_api_football_budget(p_status_code integer, p_headers jsonb)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    headers jsonb := '{}'::jsonb;
    retry_seconds integer;
    minute_limit integer;
    minute_remaining integer;
    daily_limit integer;
    daily_remaining integer;
    conservative_until timestamptz;
BEGIN
    IF p_status_code IS NULL OR p_status_code < 100 OR p_status_code > 599 THEN
        RAISE EXCEPTION 'valid HTTP status is required' USING ERRCODE = '22023';
    END IF;
    IF p_headers IS NULL OR jsonb_typeof(p_headers) <> 'object' THEN
        RAISE EXCEPTION 'headers must be an object' USING ERRCODE = '22023';
    END IF;
    SELECT coalesce(jsonb_object_agg(lower(key), value), '{}'::jsonb) INTO headers FROM jsonb_each(p_headers);
    minute_limit := CASE WHEN headers->>'x-ratelimit-limit' ~ '^[0-9]{1,9}$' THEN (headers->>'x-ratelimit-limit')::integer END;
    minute_remaining := CASE WHEN headers->>'x-ratelimit-remaining' ~ '^[0-9]{1,9}$' THEN (headers->>'x-ratelimit-remaining')::integer END;
    daily_limit := CASE WHEN headers->>'x-ratelimit-requests-limit' ~ '^[0-9]{1,9}$' THEN (headers->>'x-ratelimit-requests-limit')::integer END;
    daily_remaining := CASE WHEN headers->>'x-ratelimit-requests-remaining' ~ '^[0-9]{1,9}$' THEN (headers->>'x-ratelimit-requests-remaining')::integer END;
    IF (minute_limit IS NOT NULL AND minute_remaining IS NOT NULL AND minute_remaining <= minute_limit) THEN
        UPDATE ops.api_football_budget_state
        SET provider_minute_remaining=least(coalesce(provider_minute_remaining, minute_remaining), minute_remaining)
        WHERE singleton;
    END IF;
    IF (daily_limit IS NOT NULL AND daily_remaining IS NOT NULL AND daily_remaining <= daily_limit) THEN
        UPDATE ops.api_football_budget_state
        SET provider_daily_remaining=least(coalesce(provider_daily_remaining, daily_remaining), daily_remaining),
            provider_daily_exhausted=provider_daily_exhausted OR daily_remaining = 0
        WHERE singleton;
    END IF;
    conservative_until := date_trunc('minute', clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + interval '1 minute';
    IF p_status_code = 429 OR minute_remaining = 0 THEN
        IF coalesce(headers->>'retry-after', '') ~ '^[0-9]{1,9}$' THEN
            retry_seconds := (headers->>'retry-after')::integer;
            conservative_until := greatest(conservative_until, clock_timestamp() + make_interval(secs => retry_seconds));
        END IF;
        UPDATE ops.api_football_budget_state
        SET cooldown_until=greatest(coalesce(cooldown_until, '-infinity'::timestamptz), conservative_until)
        WHERE singleton;
    END IF;
END;
$$;

REVOKE EXECUTE ON FUNCTION ops.reserve_api_football_request(text), ops.observe_api_football_budget(integer,jsonb) FROM PUBLIC, anon, authenticated;
COMMIT;
