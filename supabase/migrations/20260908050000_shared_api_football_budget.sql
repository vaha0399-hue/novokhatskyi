-- Q04: one durable, atomic request budget for every API-Football process.
BEGIN;

CREATE TABLE ops.api_football_budget_config (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    daily_limit integer NOT NULL CHECK (daily_limit > 0),
    minute_limit integer NOT NULL CHECK (minute_limit > 0),
    operations_limit integer NOT NULL CHECK (operations_limit >= 0),
    history_limit integer NOT NULL CHECK (history_limit >= 0),
    legacy_manual_limit integer NOT NULL CHECK (legacy_manual_limit >= 0),
    protected_reserve integer NOT NULL CHECK (protected_reserve >= 0),
    CHECK (operations_limit + history_limit + legacy_manual_limit + protected_reserve <= daily_limit)
);

INSERT INTO ops.api_football_budget_config
    (daily_limit, minute_limit, operations_limit, history_limit, legacy_manual_limit, protected_reserve)
VALUES (6000, 300, 2000, 90, 1000, 2910);

CREATE TABLE ops.api_football_budget_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    daily_window date NOT NULL,
    minute_window timestamptz NOT NULL,
    daily_used integer NOT NULL DEFAULT 0 CHECK (daily_used >= 0),
    minute_used integer NOT NULL DEFAULT 0 CHECK (minute_used >= 0),
    operations_used integer NOT NULL DEFAULT 0 CHECK (operations_used >= 0),
    history_used integer NOT NULL DEFAULT 0 CHECK (history_used >= 0),
    legacy_manual_used integer NOT NULL DEFAULT 0 CHECK (legacy_manual_used >= 0),
    cooldown_until timestamptz
);

CREATE OR REPLACE FUNCTION ops.reserve_api_football_request(p_consumer text)
RETURNS TABLE (allowed boolean, reason text, retry_at timestamptz)
LANGUAGE plpgsql AS $$
DECLARE
    config ops.api_football_budget_config;
    state ops.api_football_budget_state;
    now_at timestamptz := clock_timestamp();
    day_at date := (clock_timestamp() AT TIME ZONE 'UTC')::date;
    minute_at timestamptz := date_trunc('minute', clock_timestamp());
    consumer_limit integer;
    consumer_used integer;
BEGIN
    IF p_consumer NOT IN ('operations', 'history', 'legacy_manual') THEN
        RAISE EXCEPTION 'unknown API-Football budget consumer' USING ERRCODE = '22023';
    END IF;
    SELECT * INTO config FROM ops.api_football_budget_config WHERE singleton FOR UPDATE;
    INSERT INTO ops.api_football_budget_state(singleton, daily_window, minute_window)
    VALUES (true, day_at, minute_at) ON CONFLICT (singleton) DO NOTHING;
    SELECT * INTO state FROM ops.api_football_budget_state WHERE singleton FOR UPDATE;
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
    IF state.daily_used >= config.daily_limit - config.protected_reserve THEN
        RETURN QUERY SELECT false, 'daily_limit', (day_at + 1)::timestamptz; RETURN;
    END IF;
    IF state.minute_used >= config.minute_limit THEN
        RETURN QUERY SELECT false, 'minute_limit', minute_at + interval '1 minute'; RETURN;
    END IF;
    IF consumer_used >= consumer_limit THEN
        RETURN QUERY SELECT false, p_consumer || '_limit', (day_at + 1)::timestamptz; RETURN;
    END IF;
    UPDATE ops.api_football_budget_state SET daily_used=daily_used+1, minute_used=minute_used+1,
      operations_used=operations_used + CASE WHEN p_consumer='operations' THEN 1 ELSE 0 END,
      history_used=history_used + CASE WHEN p_consumer='history' THEN 1 ELSE 0 END,
      legacy_manual_used=legacy_manual_used + CASE WHEN p_consumer='legacy_manual' THEN 1 ELSE 0 END
    WHERE singleton;
    RETURN QUERY SELECT true, 'reserved'::text, NULL::timestamptz;
END;
$$;

CREATE OR REPLACE FUNCTION ops.observe_api_football_budget(p_status_code integer, p_headers jsonb)
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    retry_seconds integer;
    conservative_until timestamptz := date_trunc('minute', clock_timestamp()) + interval '1 minute';
BEGIN
    IF p_status_code <> 429 THEN RETURN; END IF;
    IF jsonb_typeof(p_headers) <> 'object' THEN
        RAISE EXCEPTION 'headers must be an object' USING ERRCODE = '22023';
    END IF;
    -- API-Football documents 429 as a per-minute over-limit signal but does
    -- not document a reset header.  A full minute is therefore the safe base.
    IF coalesce(p_headers->>'retry-after', '') ~ '^[0-9]+$' THEN
        retry_seconds := (p_headers->>'retry-after')::integer;
        conservative_until := greatest(conservative_until, clock_timestamp() + make_interval(secs => retry_seconds));
    END IF;
    UPDATE ops.api_football_budget_state
    SET cooldown_until=greatest(coalesce(cooldown_until, '-infinity'::timestamptz), conservative_until)
    WHERE singleton;
END;
$$;

ALTER TABLE ops.api_football_budget_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE ops.api_football_budget_state ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON ops.api_football_budget_config, ops.api_football_budget_state FROM PUBLIC, anon, authenticated;
REVOKE EXECUTE ON FUNCTION ops.reserve_api_football_request(text), ops.observe_api_football_budget(integer,jsonb) FROM PUBLIC, anon, authenticated;
COMMIT;
