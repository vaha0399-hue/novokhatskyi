-- Q06: durable provider evidence survives the fenced domain transaction.
BEGIN;

ALTER TABLE source.provider_fetches
    ADD COLUMN request_scope jsonb NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN normalization_version text,
    ADD COLUMN sync_work_item_id bigint REFERENCES ops.sync_work_items(id) ON DELETE RESTRICT,
    ADD COLUMN sync_work_item_attempt integer;

ALTER TABLE source.provider_fetches
    ADD CONSTRAINT provider_fetches_request_scope_object CHECK (jsonb_typeof(request_scope) = 'object'),
    ADD CONSTRAINT provider_fetches_normalization_version_nonblank CHECK (normalization_version IS NULL OR btrim(normalization_version) <> ''),
    ADD CONSTRAINT provider_fetches_sync_attempt_pair CHECK (
        (sync_work_item_id IS NULL AND sync_work_item_attempt IS NULL)
        OR (sync_work_item_id IS NOT NULL AND sync_work_item_attempt >= 1)
    );

CREATE OR REPLACE FUNCTION source.guard_safe_fetch_provenance() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, source AS $$
BEGIN
  IF source.jsonb_contains_forbidden_metadata_key(NEW.request_scope) THEN
    RAISE EXCEPTION 'request_scope must not contain credentials or headers' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER provider_fetches_safe_provenance_guard
BEFORE INSERT OR UPDATE ON source.provider_fetches
FOR EACH ROW EXECUTE FUNCTION source.guard_safe_fetch_provenance();

CREATE INDEX provider_fetches_sync_replay_idx
    ON source.provider_fetches(sync_work_item_id, endpoint, request_params_sha256, sync_work_item_attempt DESC, id DESC)
    WHERE sync_work_item_id IS NOT NULL;

COMMENT ON COLUMN source.provider_fetches.request_scope IS
  'Safe scope snapshot for a provider request; excludes headers and credentials.';
COMMENT ON COLUMN source.provider_fetches.normalization_version IS
  'Explicit version of the normalizer that consumed this immutable provider response.';
COMMENT ON COLUMN source.provider_fetches.sync_work_item_attempt IS
  'The durable queue attempt that made this physical provider request.';

COMMIT;
