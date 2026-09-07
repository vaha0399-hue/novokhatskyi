BEGIN;

-- These are supported by the active-season normalizer.  The immutable mapping
-- table keeps the exact provider status while fixture lifecycle remains stable.
INSERT INTO source.fixture_status_code_mappings (
    provider_id, external_code, canonical_state, mapping_version
)
SELECT provider.id, mapping.external_code,
       mapping.canonical_state::football.fixture_lifecycle_state,
       'api-football-v4'
FROM source.providers provider
CROSS JOIN (
    VALUES
        ('1H', 'in_progress'),
        ('HT', 'in_progress'),
        ('2H', 'in_progress'),
        ('AWD', 'completed'),
        ('ABD', 'abandoned')
) AS mapping(external_code, canonical_state)
WHERE provider.code = 'api-football'
ON CONFLICT (provider_id, external_code) DO NOTHING;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM source.fixture_status_code_mappings mapping
        JOIN source.providers provider ON provider.id = mapping.provider_id
        JOIN (
            VALUES
                ('1H', 'in_progress'),
                ('HT', 'in_progress'),
                ('2H', 'in_progress'),
                ('AWD', 'completed'),
                ('ABD', 'abandoned')
        ) AS expected(external_code, canonical_state)
          ON expected.external_code = mapping.external_code
        WHERE provider.code = 'api-football'
          AND mapping.canonical_state IS DISTINCT FROM expected.canonical_state::football.fixture_lifecycle_state
    ) THEN
        RAISE EXCEPTION 'API-Football live/abandoned status mappings must remain canonical'
            USING ERRCODE = '23514';
    END IF;
END
$$;

COMMIT;
