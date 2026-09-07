BEGIN;

ALTER TYPE football.fixture_lifecycle_state ADD VALUE IF NOT EXISTS 'in_progress';

CREATE TABLE football.fixture_statistics_coverage (
    fixture_id bigint PRIMARY KEY REFERENCES football.fixtures(id) ON DELETE RESTRICT,
    coverage_state football.snapshot_coverage_state NOT NULL,
    team_count smallint NOT NULL CHECK (team_count BETWEEN 0 AND 2),
    last_source_fetch_id bigint NOT NULL REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    observed_at timestamptz NOT NULL,
    next_retry_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 1 CHECK (attempts > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((coverage_state = 'empty' AND team_count = 0)
        OR (coverage_state = 'partial' AND team_count = 1)
        OR (coverage_state = 'complete' AND team_count = 2)
        OR coverage_state = 'unknown')
);

CREATE INDEX fixture_statistics_coverage_retry_idx
    ON football.fixture_statistics_coverage (next_retry_at)
    WHERE coverage_state IN ('empty', 'partial', 'unknown');

CREATE TRIGGER fixture_statistics_coverage_touch_updated_at
BEFORE UPDATE ON football.fixture_statistics_coverage
FOR EACH ROW EXECUTE FUNCTION football.touch_updated_at();

ALTER TABLE football.fixture_statistics_coverage ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON football.fixture_statistics_coverage FROM PUBLIC;

COMMIT;
