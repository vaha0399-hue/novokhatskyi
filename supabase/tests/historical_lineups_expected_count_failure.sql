\set ON_ERROR_STOP on

-- This file is intentionally expected to fail at COMMIT. It proves the
-- deferred snapshot count guard, which cannot be reliably asserted inside a
-- successful DO block.
BEGIN;

INSERT INTO source.provider_fetches (
    provider_id, endpoint, request_params, request_params_sha256, purpose,
    request_started_at, response_received_at, http_status, outcome,
    provider_results, paging_current, paging_total, content_sha256,
    subject_fixture_id
)
SELECT provider.id,
       '/fixtures/lineups',
       jsonb_build_object('fixture', ref.external_id),
       decode(repeat('de', 32), 'hex'),
       'historical_backfill',
       '2026-08-05 11:59:00+00',
       '2026-08-05 12:00:00+00',
       200,
       'success',
       1,
       1,
       1,
       decode(repeat('de', 32), 'hex'),
       fixture.id
FROM source.providers provider
JOIN source.fixture_provider_refs ref ON ref.provider_id = provider.id
JOIN football.fixtures fixture ON fixture.id = ref.fixture_id
WHERE provider.code = 'api-football'
  AND fixture.id = 1
RETURNING id AS count_failure_fetch_id
\gset

WITH payload AS (
    SELECT convert_to('{"get":"fixtures/lineups","response":[]}', 'UTF8') AS body
)
INSERT INTO source.provider_raw_payloads (
    fetch_id, inline_body, byte_count, retention_class, expires_at
)
SELECT :count_failure_fetch_id, body, octet_length(body), 'standard', clock_timestamp() + interval '30 days'
FROM payload;

INSERT INTO football.fixture_historical_lineup_snapshots (
    fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
    availability_basis, coverage_state, team_count, mapping_version
)
VALUES (
    1,
    :count_failure_fetch_id,
    decode(repeat('de', 32), 'hex'),
    '2026-08-05 12:00:00+00',
    '2026-08-05 12:00:00+00',
    'reconstructed_conservative',
    'partial',
    1,
    'api-football-lineups-v1'
);

COMMIT;
