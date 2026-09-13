\set ON_ERROR_STOP on
BEGIN;

DO $$
DECLARE provider_id smallint; run_id bigint; work_item_id bigint; inserted_fetch_id bigint; rejected boolean := false;
BEGIN
  IF to_regclass('source.provider_raw_payloads') IS NULL
     OR NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='source' AND table_name='provider_fetches' AND column_name='normalization_version') THEN
    RAISE EXCEPTION 'Q06 provenance schema is missing';
  END IF;
  INSERT INTO source.providers(code,name) VALUES ('q06-provenance-test','Q06 provenance test') RETURNING id INTO provider_id;
  INSERT INTO ops.sync_runs(provider_id,operation) VALUES (provider_id,'q06-provenance') RETURNING id INTO run_id;
  INSERT INTO ops.sync_work_items(run_id,scope_key,scope,job_type,stable_key,entity_key,execution_key)
  VALUES(run_id,'q06:scope','{"fixture":42}','q06','q06:stable','q06:entity','q06:execution') RETURNING id INTO work_item_id;
  INSERT INTO source.provider_fetches(
    provider_id,endpoint,request_params,request_params_sha256,purpose,request_started_at,response_received_at,
    http_status,outcome,content_sha256,request_scope,normalization_version,sync_work_item_id,sync_work_item_attempt
  ) VALUES (
    provider_id,'/fixtures','{"id":42}',decode(repeat('aa',32),'hex'),'scheduled_refresh',clock_timestamp(),clock_timestamp(),
    200,'success',decode(repeat('bb',32),'hex'),'{"scope_key":"q06:scope","scope":{"fixture":42}}','fixtures-v1',work_item_id,1
  ) RETURNING id INTO inserted_fetch_id;
  INSERT INTO source.provider_raw_payloads(fetch_id,inline_body,byte_count,retention_class,expires_at)
  VALUES(inserted_fetch_id,convert_to('{"response":[]}','UTF8'),15,'standard',clock_timestamp()+interval '30 days');
  IF NOT EXISTS (SELECT 1 FROM source.provider_fetches f JOIN source.provider_raw_payloads raw ON raw.fetch_id=f.id WHERE f.id=inserted_fetch_id AND f.sync_work_item_id=work_item_id AND f.sync_work_item_attempt=1 AND f.normalization_version='fixtures-v1') THEN
    RAISE EXCEPTION 'Q06 fetch/raw linkage is missing';
  END IF;
  BEGIN
    INSERT INTO source.provider_fetches(provider_id,endpoint,request_params,purpose,request_started_at,outcome,request_scope)
    VALUES(provider_id,'/fixtures','{}','scheduled_refresh',clock_timestamp(),'transport_error','{"token":"forbidden"}');
  EXCEPTION WHEN check_violation THEN rejected := true;
  END;
  IF NOT rejected THEN RAISE EXCEPTION 'Q06 accepted a secret-bearing provenance scope'; END IF;
END $$;

ROLLBACK;
