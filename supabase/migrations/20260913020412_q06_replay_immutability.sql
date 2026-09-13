-- Q06 follow-up: accepted replay records and immutable original fetch provenance.
BEGIN;

CREATE TABLE source.provider_fetch_replays (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_fetch_id bigint NOT NULL REFERENCES source.provider_fetches(id) ON DELETE RESTRICT,
    sync_work_item_id bigint NOT NULL REFERENCES ops.sync_work_items(id) ON DELETE RESTRICT,
    sync_work_item_attempt integer NOT NULL CHECK (sync_work_item_attempt >= 1),
    normalization_version text NOT NULL CHECK (btrim(normalization_version) <> ''),
    reprocessed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (source_fetch_id, sync_work_item_id, sync_work_item_attempt)
);
CREATE INDEX provider_fetch_replays_work_item_idx
    ON source.provider_fetch_replays(sync_work_item_id, sync_work_item_attempt DESC, id DESC);
ALTER TABLE source.provider_fetch_replays ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON source.provider_fetch_replays FROM PUBLIC;

CREATE OR REPLACE FUNCTION source.guard_q06_fetch_provenance_immutable() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, source AS $$
BEGIN
  IF (OLD.sync_work_item_id IS NOT NULL OR NEW.sync_work_item_id IS NOT NULL)
     AND (
       NEW.http_status IS DISTINCT FROM OLD.http_status
       OR NEW.response_received_at IS DISTINCT FROM OLD.response_received_at
       OR NEW.content_sha256 IS DISTINCT FROM OLD.content_sha256
       OR NEW.request_scope IS DISTINCT FROM OLD.request_scope
       OR NEW.normalization_version IS DISTINCT FROM OLD.normalization_version
       OR NEW.sync_work_item_id IS DISTINCT FROM OLD.sync_work_item_id
       OR NEW.sync_work_item_attempt IS DISTINCT FROM OLD.sync_work_item_attempt
     ) THEN
    RAISE EXCEPTION 'provider fetch Q06 provenance is immutable' USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER provider_fetches_q06_provenance_immutable
BEFORE UPDATE ON source.provider_fetches
FOR EACH ROW EXECUTE FUNCTION source.guard_q06_fetch_provenance_immutable();

CREATE OR REPLACE FUNCTION source.guard_provider_fetch_replay_immutable() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, source AS $$
BEGIN
  RAISE EXCEPTION 'provider fetch replay is immutable' USING ERRCODE = '55000';
END $$;

CREATE TRIGGER provider_fetch_replays_immutable
BEFORE UPDATE OR DELETE ON source.provider_fetch_replays
FOR EACH ROW EXECUTE FUNCTION source.guard_provider_fetch_replay_immutable();

COMMENT ON TABLE source.provider_fetch_replays IS
  'Accepted reprocessing of immutable provider bytes; original request and observation provenance stay on provider_fetches.';
COMMENT ON COLUMN source.provider_fetch_replays.normalization_version IS
  'Normalizer version used for this replay, independent of the immutable source fetch version.';

COMMIT;
