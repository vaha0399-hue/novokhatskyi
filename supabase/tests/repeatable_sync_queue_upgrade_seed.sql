-- Seed only the pre-Q02 queue shape immediately before the Q02 migration.
INSERT INTO source.providers(code, name)
SELECT 'q02-legacy-provider', 'Q02 legacy provider'
WHERE NOT EXISTS (SELECT 1 FROM source.providers);
INSERT INTO ops.sync_runs(provider_id, operation) SELECT id, 'q02-legacy-upgrade' FROM source.providers ORDER BY id LIMIT 1;
INSERT INTO ops.sync_work_items(run_id, scope_key, scope) SELECT id, 'q02-legacy-scope', '{"legacy":true}'::jsonb FROM ops.sync_runs WHERE operation='q02-legacy-upgrade';
