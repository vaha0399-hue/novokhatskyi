-- Q04: add lease ownership metadata to seasonal bootstrap control-plane runs.
BEGIN;

CREATE SEQUENCE ops.sync_run_lease_token_seq AS bigint;
REVOKE ALL ON SEQUENCE ops.sync_run_lease_token_seq FROM PUBLIC;

ALTER TABLE ops.sync_runs
    ADD COLUMN lease_owner text,
    ADD COLUMN lease_token bigint NOT NULL DEFAULT 0,
    ADD COLUMN lease_expires_at timestamptz;

COMMIT;
