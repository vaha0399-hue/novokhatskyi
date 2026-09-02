-- API-Football's PST status is a non-terminal postponement.  Keep its latest
-- provider observation exact while excluding the fixture from scheduled scans
-- until a later NS response supplies its confirmed kickoff.
BEGIN;

INSERT INTO source.fixture_status_code_mappings (
    provider_id, external_code, canonical_state, mapping_version
)
SELECT provider.id, 'PST', 'postponed'::football.fixture_lifecycle_state, 'api-football-v2'
FROM source.providers provider
WHERE provider.code = 'api-football'
ON CONFLICT (provider_id, external_code) DO NOTHING;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM source.fixture_status_code_mappings mapping
        JOIN source.providers provider ON provider.id = mapping.provider_id
        WHERE provider.code = 'api-football'
          AND mapping.external_code = 'PST'
          AND mapping.canonical_state IS DISTINCT FROM 'postponed'::football.fixture_lifecycle_state
    ) THEN
        RAISE EXCEPTION 'API-Football PST mapping must remain postponed'
            USING ERRCODE = '23514';
    END IF;
END
$$;

COMMIT;
