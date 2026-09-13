\set ON_ERROR_STOP on
BEGIN;

DO $$
DECLARE
  provider_id smallint;
  run_id bigint;
  work_item_id bigint;
  fetch_id bigint;
  blocked boolean := false;
BEGIN
  INSERT INTO source.providers(code,name)
  VALUES ('q06-outcome-immutability','Q06 outcome immutability')
  RETURNING id INTO provider_id;
  INSERT INTO ops.sync_runs(provider_id,operation)
  VALUES (provider_id,'q06-outcome-immutability')
  RETURNING id INTO run_id;
  INSERT INTO ops.sync_work_items(run_id,scope_key,scope,job_type,stable_key,entity_key,execution_key)
  VALUES (run_id,'q06:outcome','{}','q06','q06:outcome','q06:outcome','q06:outcome')
  RETURNING id INTO work_item_id;
  INSERT INTO source.provider_fetches(
    provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
    content_sha256,request_scope,normalization_version,sync_work_item_id,sync_work_item_attempt
  ) VALUES (
    provider_id,'/fixtures','{}','scheduled_refresh',clock_timestamp(),clock_timestamp(),200,'success',
    decode(repeat('aa',32),'hex'),'{}','fixtures-v1',work_item_id,1
  ) RETURNING id INTO fetch_id;

  BEGIN
    UPDATE source.provider_fetches SET outcome='provider_error' WHERE id=fetch_id;
  EXCEPTION WHEN SQLSTATE '55000' THEN
    blocked := true;
  END;
  IF NOT blocked THEN
    RAISE EXCEPTION 'Q06 accepted replayable fetch outcome rewrite';
  END IF;
END $$;

ROLLBACK;
