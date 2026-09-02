-- A provider may report a postponed fixture before publishing a replacement
-- date. Retain the fixture and its exact PST observation without inventing a
-- kickoff; only postponed fixtures may have an unknown kickoff.
BEGIN;

ALTER TABLE football.fixtures
    ALTER COLUMN kickoff_at DROP NOT NULL;

ALTER TABLE football.fixtures
    ADD CONSTRAINT fixtures_unknown_kickoff_requires_postponed
    CHECK (kickoff_at IS NOT NULL OR lifecycle_state = 'postponed');

COMMIT;
