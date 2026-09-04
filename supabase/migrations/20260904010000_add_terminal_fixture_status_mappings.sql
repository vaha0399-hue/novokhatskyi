-- API-Football uses AET/PEN for terminal fixtures.  They have the same
-- canonical lifecycle state as FT while preserving the exact provider status.
BEGIN;

INSERT INTO source.fixture_status_code_mappings (
    provider_id, external_code, canonical_state, mapping_version
)
SELECT provider.id, mapping.external_code,
       'completed'::football.fixture_lifecycle_state, 'api-football-v3'
FROM source.providers provider
CROSS JOIN (VALUES ('AET'), ('PEN')) AS mapping(external_code)
WHERE provider.code = 'api-football'
ON CONFLICT (provider_id, external_code) DO UPDATE
SET canonical_state = EXCLUDED.canonical_state,
    mapping_version = EXCLUDED.mapping_version;

COMMIT;
