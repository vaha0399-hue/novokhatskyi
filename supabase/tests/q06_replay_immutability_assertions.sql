\set ON_ERROR_STOP on
BEGIN;

DO $$
DECLARE
  provider_id smallint;
  run_id bigint;
  work_item_id bigint;
  second_work_item_id bigint;
  fetch_id bigint;
  replay_id bigint;
  blocked boolean;
BEGIN
  IF to_regclass('source.provider_fetch_replays') IS NULL THEN
    RAISE EXCEPTION 'Q06 replay schema is missing';
  END IF;

  INSERT INTO source.providers(code,name)
  VALUES ('q06-replay-immutability','Q06 replay immutability')
  RETURNING id INTO provider_id;
  INSERT INTO ops.sync_runs(provider_id,operation)
  VALUES (provider_id,'q06-replay-immutability')
  RETURNING id INTO run_id;
  INSERT INTO ops.sync_work_items(run_id,scope_key,scope,job_type,stable_key,entity_key,execution_key)
  VALUES (run_id,'q06:replay:one','{}','q06','q06:replay:one','q06:replay:one','q06:replay:one')
  RETURNING id INTO work_item_id;
  INSERT INTO ops.sync_work_items(run_id,scope_key,scope,job_type,stable_key,entity_key,execution_key)
  VALUES (run_id,'q06:replay:two','{}','q06','q06:replay:two','q06:replay:two','q06:replay:two')
  RETURNING id INTO second_work_item_id;
  INSERT INTO source.provider_fetches(
    provider_id,endpoint,request_params,purpose,request_started_at,response_received_at,http_status,outcome,
    content_sha256,request_scope,normalization_version,sync_work_item_id,sync_work_item_attempt
  ) VALUES (
    provider_id,'/fixtures','{}','scheduled_refresh',clock_timestamp(),clock_timestamp(),200,'success',
    decode(repeat('aa',32),'hex'),'{}','fixtures-v1',work_item_id,1
  ) RETURNING id INTO fetch_id;
  INSERT INTO source.provider_fetch_replays(
    source_fetch_id,sync_work_item_id,sync_work_item_attempt,normalization_version
  ) VALUES (fetch_id,work_item_id,2,'fixtures-v2') RETURNING id INTO replay_id;

  UPDATE source.provider_fetches SET normalized_at=clock_timestamp() WHERE id=fetch_id;
  IF (SELECT normalized_at IS NOT NULL FROM source.provider_fetches WHERE id=fetch_id) IS NOT TRUE THEN
    RAISE EXCEPTION 'Q06 must allow normalized_at updates';
  END IF;

  blocked := false;
  BEGIN UPDATE source.provider_fetches SET http_status=201 WHERE id=fetch_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted HTTP status rewrite'; END IF;
  blocked := false;
  BEGIN UPDATE source.provider_fetches SET response_received_at=clock_timestamp()+interval '1 second' WHERE id=fetch_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted response time rewrite'; END IF;
  blocked := false;
  BEGIN UPDATE source.provider_fetches SET content_sha256=decode(repeat('bb',32),'hex') WHERE id=fetch_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted content hash rewrite'; END IF;
  blocked := false;
  BEGIN UPDATE source.provider_fetches SET request_scope='{"fixture":42}' WHERE id=fetch_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted scope rewrite'; END IF;
  blocked := false;
  BEGIN UPDATE source.provider_fetches SET normalization_version='fixtures-v0' WHERE id=fetch_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted original normalization version rewrite'; END IF;
  blocked := false;
  BEGIN UPDATE source.provider_fetches SET sync_work_item_id=second_work_item_id WHERE id=fetch_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted work-item rewrite'; END IF;
  blocked := false;
  BEGIN UPDATE source.provider_fetches SET sync_work_item_attempt=2 WHERE id=fetch_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted attempt rewrite'; END IF;
  blocked := false;
  BEGIN UPDATE source.provider_fetch_replays SET normalization_version='fixtures-v3' WHERE id=replay_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted replay event update'; END IF;
  blocked := false;
  BEGIN DELETE FROM source.provider_fetch_replays WHERE id=replay_id; EXCEPTION WHEN SQLSTATE '55000' THEN blocked := true; END;
  IF NOT blocked THEN RAISE EXCEPTION 'Q06 accepted replay event delete'; END IF;
END $$;

ROLLBACK;
