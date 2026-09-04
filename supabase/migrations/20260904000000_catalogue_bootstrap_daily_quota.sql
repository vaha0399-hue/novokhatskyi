-- Persistent reservation ledger for autonomous provider workers.
-- A request is reserved before network I/O, so restarts cannot undercount
-- successful raw captures that have not yet entered canonical tables.
BEGIN;

CREATE TABLE ops.provider_daily_request_usage (
    provider_id smallint NOT NULL REFERENCES source.providers(id) ON DELETE RESTRICT,
    usage_date date NOT NULL DEFAULT CURRENT_DATE,
    request_count integer NOT NULL DEFAULT 0 CHECK (request_count >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (provider_id, usage_date)
);

CREATE OR REPLACE FUNCTION ops.reserve_provider_daily_request(
    p_provider_id smallint,
    p_daily_limit integer
)
RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    reserved boolean := false;
BEGIN
    IF p_daily_limit <= 0 THEN
        RAISE EXCEPTION 'daily request limit must be positive' USING ERRCODE = '22023';
    END IF;
    INSERT INTO ops.provider_daily_request_usage(provider_id, usage_date, request_count, updated_at)
    VALUES (p_provider_id, CURRENT_DATE, 1, clock_timestamp())
    ON CONFLICT (provider_id, usage_date) DO UPDATE
        SET request_count = ops.provider_daily_request_usage.request_count + 1,
            updated_at = clock_timestamp()
        WHERE ops.provider_daily_request_usage.request_count < p_daily_limit
    RETURNING true INTO reserved;
    RETURN coalesce(reserved, false);
END;
$$;

ALTER TABLE ops.provider_daily_request_usage ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON ops.provider_daily_request_usage FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION ops.reserve_provider_daily_request(smallint, integer) FROM PUBLIC;

COMMIT;
