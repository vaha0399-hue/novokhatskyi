-- Q04: a 429 always starts a shared base cooldown before headers are trusted.
BEGIN;

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
    valid_minute_pair boolean;
    valid_daily_pair boolean;
BEGIN
    IF p_status_code IS NULL OR p_status_code < 100 OR p_status_code > 599 THEN
        RAISE EXCEPTION 'valid HTTP status is required' USING ERRCODE = '22023';
    END IF;
    IF p_headers IS NULL OR jsonb_typeof(p_headers) <> 'object' THEN
        RAISE EXCEPTION 'headers must be an object' USING ERRCODE = '22023';
    END IF;
    SELECT coalesce(jsonb_object_agg(lower(key), value), '{}'::jsonb) INTO headers FROM jsonb_each(p_headers);
    conservative_until := date_trunc('minute', clock_timestamp() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + interval '1 minute';

    -- Establish the documented 429 backoff before examining optional headers.
    -- Missing, malformed and contradictory header values therefore cannot
    -- bypass the common safety boundary.
    IF p_status_code = 429 THEN
        UPDATE ops.api_football_budget_state
        SET cooldown_until=greatest(coalesce(cooldown_until, '-infinity'::timestamptz), conservative_until)
        WHERE singleton;
    END IF;

    minute_limit := CASE WHEN headers->>'x-ratelimit-limit' ~ '^[0-9]{1,9}$' THEN (headers->>'x-ratelimit-limit')::integer END;
    minute_remaining := CASE WHEN headers->>'x-ratelimit-remaining' ~ '^[0-9]{1,9}$' THEN (headers->>'x-ratelimit-remaining')::integer END;
    daily_limit := CASE WHEN headers->>'x-ratelimit-requests-limit' ~ '^[0-9]{1,9}$' THEN (headers->>'x-ratelimit-requests-limit')::integer END;
    daily_remaining := CASE WHEN headers->>'x-ratelimit-requests-remaining' ~ '^[0-9]{1,9}$' THEN (headers->>'x-ratelimit-requests-remaining')::integer END;
    valid_minute_pair := minute_limit IS NOT NULL AND minute_remaining IS NOT NULL AND minute_remaining <= minute_limit;
    valid_daily_pair := daily_limit IS NOT NULL AND daily_remaining IS NOT NULL AND daily_remaining <= daily_limit;

    IF valid_minute_pair THEN
        UPDATE ops.api_football_budget_state
        SET provider_minute_remaining=least(coalesce(provider_minute_remaining, minute_remaining), minute_remaining)
        WHERE singleton;
    END IF;
    IF valid_daily_pair THEN
        UPDATE ops.api_football_budget_state
        SET provider_daily_remaining=least(coalesce(provider_daily_remaining, daily_remaining), daily_remaining),
            provider_daily_exhausted=provider_daily_exhausted OR daily_remaining = 0
        WHERE singleton;
    END IF;

    IF valid_minute_pair AND minute_remaining = 0 THEN
        UPDATE ops.api_football_budget_state
        SET cooldown_until=greatest(coalesce(cooldown_until, '-infinity'::timestamptz), conservative_until)
        WHERE singleton;
    END IF;
    IF coalesce(headers->>'retry-after', '') ~ '^[0-9]{1,9}$' THEN
        retry_seconds := (headers->>'retry-after')::integer;
        UPDATE ops.api_football_budget_state
        SET cooldown_until=greatest(
            coalesce(cooldown_until, '-infinity'::timestamptz),
            clock_timestamp() + make_interval(secs => retry_seconds)
        )
        WHERE singleton AND (p_status_code = 429 OR (valid_minute_pair AND minute_remaining = 0));
    END IF;
END;
$$;

REVOKE EXECUTE ON FUNCTION ops.observe_api_football_budget(integer,jsonb) FROM PUBLIC, anon, authenticated;
COMMIT;
