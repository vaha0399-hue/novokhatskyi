-- Q06: response summaries are observations of immutable raw bytes. The
-- normalization marker may be filled once, but never cleared or replaced.
BEGIN;

CREATE OR REPLACE FUNCTION source.guard_q06_fetch_provenance_immutable() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, source AS $$
BEGIN
  IF (OLD.sync_work_item_id IS NOT NULL OR NEW.sync_work_item_id IS NOT NULL)
     AND (
       NEW.http_status IS DISTINCT FROM OLD.http_status
       OR NEW.response_received_at IS DISTINCT FROM OLD.response_received_at
       OR NEW.provider_results IS DISTINCT FROM OLD.provider_results
       OR NEW.paging_current IS DISTINCT FROM OLD.paging_current
       OR NEW.paging_total IS DISTINCT FROM OLD.paging_total
       OR NEW.content_sha256 IS DISTINCT FROM OLD.content_sha256
       OR NEW.request_scope IS DISTINCT FROM OLD.request_scope
       OR NEW.normalization_version IS DISTINCT FROM OLD.normalization_version
       OR NEW.sync_work_item_id IS DISTINCT FROM OLD.sync_work_item_id
       OR NEW.sync_work_item_attempt IS DISTINCT FROM OLD.sync_work_item_attempt
       OR NEW.outcome IS DISTINCT FROM OLD.outcome
       OR (OLD.normalized_at IS NOT NULL AND NEW.normalized_at IS DISTINCT FROM OLD.normalized_at)
     ) THEN
    RAISE EXCEPTION 'provider fetch Q06 provenance is immutable' USING ERRCODE = '55000';
  END IF;
  RETURN NEW;
END
$$;

COMMIT;
