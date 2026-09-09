-- Q04: bind legacy seasonal items to their owning run lease token.
-- This is additive: prior Q04 migration history remains untouched.
BEGIN;

ALTER TABLE ops.sync_work_items
    ADD COLUMN run_lease_token bigint NOT NULL DEFAULT 0;

CREATE INDEX sync_work_items_legacy_run_status_idx
    ON ops.sync_work_items (run_id, status, id);

COMMIT;
