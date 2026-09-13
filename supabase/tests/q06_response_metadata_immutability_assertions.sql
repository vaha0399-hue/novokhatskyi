\set ON_ERROR_STOP on
BEGIN;

DO $$
DECLARE
  provider_id smallint;
  run_id bigint;
  work_item_id bigint;
  fetch_id bigint;
  legacy_fetch_id bigint;
  normalization_mark timestamptz := clock_timestamp();
  blocked boolean;
BEGIN
  INSERT INTO source.providers(code,name)
  VALUES ('q06-response-metadata','Q06 response metadata') RETURNING id INTO provider_id;
  INSERT INTO ops.sync_runs(provider_id,operation)
  VALUES (provider_id,'q06-response-metadata') RETURNING id INTO run_id;
  INSERT INTO ops.sync_work_items(run_id,scope_key,scope,job_type,stable_key,entity_key,execution_key)
  VALUES (run_id,'q06:response-metadata','{}','q06','q06:response-metadata','q06:response-metadata','q06:response-metadata')
  RETURNING id INTO work_item_id;
  INSERT INTO source.provider_fetches(
    provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
    provider_results,paging_current,paging_total,request_scope,normalization_version,sync_work_item_id,sync_work_item_attempt
  ) VALUES (
    provider_id,'/fixtures','{}','scheduled_refresh',clock_timestamp(),clock_timestamp(),200,'success',
    2,1,1,'{}','fixtures-v1',work_item_id,1
  ) RETURNING id INTO fetch_id;

  blocked := false;
  BEGIN UPDATE source.provider_fetches SET provider_results=3 WHERE id=fetch_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted provider_results rewrite'; END IF;
  blocked := false;
  BEGIN UPDATE source.provider_fetches SET paging_current=2,paging_total=2 WHERE id=fetch_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted paging rewrite'; END IF;

  UPDATE source.provider_fetches SET normalized_at=normalization_mark WHERE id=fetch_id;
  IF (SELECT source.provider_fetches.normalized_at FROM source.provider_fetches WHERE id=fetch_id) IS DISTINCT FROM normalization_mark THEN
    RAISE EXCEPTION 'Q06 initial normalized_at fill failed';
  END IF;
  UPDATE source.provider_fetches SET normalized_at=normalization_mark WHERE id=fetch_id;

  blocked := false;
  BEGIN UPDATE source.provider_fetches SET normalized_at=normalized_at+interval '1 second' WHERE id=fetch_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted normalized_at replacement'; END IF;
  blocked := false;
  BEGIN UPDATE source.provider_fetches SET normalized_at=NULL WHERE id=fetch_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted normalized_at reset'; END IF;

  INSERT INTO source.provider_fetches(
    provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
    provider_results,paging_current,paging_total
  ) VALUES (
    provider_id,'/fixtures','{}','scheduled_refresh',clock_timestamp(),clock_timestamp(),200,'success',2,1,1
  ) RETURNING id INTO legacy_fetch_id;
  UPDATE source.provider_fetches
     SET provider_results=3,paging_current=2,paging_total=2,normalized_at=clock_timestamp()
   WHERE id=legacy_fetch_id;
  UPDATE source.provider_fetches SET normalized_at=clock_timestamp()+interval '1 second' WHERE id=legacy_fetch_id;
END
$$;

ROLLBACK;
