\set ON_ERROR_STOP on

-- The Stage 3D assertion runs after this migration and proves its existing EPL
-- preservation fingerprint. This file exercises the new historical-only lane.

INSERT INTO football.players (display_name)
SELECT 'Historical lineup test player ' || number
FROM generate_series(1, 40) AS number;

INSERT INTO source.player_provider_refs (provider_id, external_id, player_id)
SELECT provider.id, 'historical-lineup-player-' || player.id, player.id
FROM source.providers provider
CROSS JOIN football.players player
WHERE provider.code = 'api-football'
  AND player.display_name LIKE 'Historical lineup test player %';

INSERT INTO football.coaches (display_name)
VALUES ('Historical lineup test coach home'), ('Historical lineup test coach away');

INSERT INTO source.coach_provider_refs (provider_id, external_id, coach_id)
SELECT provider.id, 'historical-lineup-coach-' || coach.id, coach.id
FROM source.providers provider
CROSS JOIN football.coaches coach
WHERE provider.code = 'api-football'
  AND coach.display_name LIKE 'Historical lineup test coach %';

SELECT fixture.id AS fixture_id,
       fixture.home_team_id AS home_team_id,
       fixture.away_team_id AS away_team_id,
       max(coach.id) FILTER (WHERE coach.display_name = 'Historical lineup test coach home') AS home_coach_id,
       max(coach.id) FILTER (WHERE coach.display_name = 'Historical lineup test coach away') AS away_coach_id
FROM football.fixtures fixture
CROSS JOIN football.coaches coach
WHERE fixture.id = 1
GROUP BY fixture.id, fixture.home_team_id, fixture.away_team_id
\gset

UPDATE football.fixtures
SET result_finalized_at = '2025-05-26 12:00:00+00'
WHERE id = :fixture_id;

-- A response captured during the fixture lifecycle but before our terminal
-- finalization cannot enter the post-match historical lane later.
DO $$
DECLARE
    target_fixture_id bigint := 1;
    provider_id smallint;
    external_fixture_id text;
    early_fetch_id bigint;
    raw_body bytea := convert_to('{"get":"fixtures/lineups","response":[]}', 'UTF8');
BEGIN
    SELECT ref.provider_id, ref.external_id
    INTO provider_id, external_fixture_id
    FROM source.fixture_provider_refs ref
    WHERE ref.fixture_id = target_fixture_id;

    BEGIN
        INSERT INTO source.provider_fetches (
            provider_id, endpoint, request_params, request_params_sha256, purpose,
            request_started_at, response_received_at, http_status, outcome,
            provider_results, paging_current, paging_total, content_sha256,
            subject_fixture_id
        ) VALUES (
            provider_id, '/fixtures/lineups', jsonb_build_object('fixture', external_fixture_id),
            decode(repeat('ed', 32), 'hex'), 'historical_backfill',
            '2024-08-02 11:59:00+00', '2024-08-02 12:00:00+00', 200,
            'success', 2, 1, 1, decode(repeat('ed', 32), 'hex'), target_fixture_id
        ) RETURNING id INTO early_fetch_id;

        INSERT INTO source.provider_raw_payloads (
            fetch_id, inline_body, byte_count, retention_class, expires_at
        ) VALUES (
            early_fetch_id, raw_body, octet_length(raw_body), 'standard',
            clock_timestamp() + interval '30 days'
        );

        INSERT INTO football.fixture_historical_lineup_snapshots (
            fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
            availability_basis, coverage_state, team_count, mapping_version
        ) VALUES (
            target_fixture_id, early_fetch_id, decode(repeat('ed', 32), 'hex'),
            '2024-08-02 12:00:00+00', '2024-08-02 12:00:00+00',
            'reconstructed_conservative', 'partial', 1, 'api-football-lineups-v1'
        );
        RAISE EXCEPTION 'pre-finalization lineup snapshot unexpectedly succeeded';
    EXCEPTION WHEN check_violation THEN NULL;
    END;

    IF EXISTS (
        SELECT 1 FROM source.provider_fetches
        WHERE content_sha256 = decode(repeat('ed', 32), 'hex')
    ) THEN
        RAISE EXCEPTION 'pre-finalization rejection left normalized fetch state';
    END IF;
END
$$;

INSERT INTO source.provider_fetches (
    provider_id, endpoint, request_params, request_params_sha256, purpose,
    request_started_at, response_received_at, http_status, outcome,
    provider_results, paging_current, paging_total, content_sha256,
    subject_fixture_id
)
SELECT provider.id,
       '/fixtures/lineups',
       jsonb_build_object('fixture', ref.external_id),
       decode(repeat('11', 32), 'hex'),
       'historical_backfill',
       '2026-08-01 11:59:00+00',
       '2026-08-01 12:00:00+00',
       200,
       'success',
       2,
       1,
       1,
       decode(repeat('ab', 32), 'hex'),
       fixture.id
FROM source.providers provider
JOIN source.fixture_provider_refs ref ON ref.provider_id = provider.id
JOIN football.fixtures fixture ON fixture.id = ref.fixture_id
WHERE provider.code = 'api-football'
  AND fixture.id = :fixture_id
RETURNING id AS source_fetch_id
\gset

WITH payload AS (
    SELECT convert_to('{"get":"fixtures/lineups","response":[]}', 'UTF8') AS body
)
INSERT INTO source.provider_raw_payloads (
    fetch_id, inline_body, byte_count, retention_class, expires_at
)
SELECT :source_fetch_id, body, octet_length(body), 'standard', clock_timestamp() + interval '30 days'
FROM payload;

BEGIN;

INSERT INTO football.fixture_historical_lineup_snapshots (
    fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
    availability_basis, coverage_state, team_count, mapping_version
)
VALUES (
    :fixture_id,
    :source_fetch_id,
    decode(repeat('ab', 32), 'hex'),
    '2026-08-01 12:00:00+00',
    '2026-08-01 12:00:00+00',
    'reconstructed_conservative',
    'complete',
    2,
    'api-football-lineups-v1'
)
RETURNING id AS snapshot_id
\gset

INSERT INTO football.fixture_historical_lineups (
    snapshot_id, team_id, coach_id, formation, starter_count, substitute_count
)
VALUES
    (:snapshot_id, :home_team_id, :home_coach_id, '4-2-3-1', 11, 9),
    (:snapshot_id, :away_team_id, :away_coach_id, '4-3-3', 11, 9);

WITH numbered_players AS (
    SELECT id, row_number() OVER (ORDER BY id) AS number
    FROM football.players
    WHERE display_name LIKE 'Historical lineup test player %'
)
INSERT INTO football.fixture_historical_lineup_players (
    snapshot_id, team_id, player_id, lineup_role, position, shirt_number, grid
)
SELECT
    :snapshot_id,
    CASE WHEN number <= 20 THEN :home_team_id ELSE :away_team_id END,
    id,
    CASE WHEN ((number - 1) % 20) < 11 THEN 'starter'::football.lineup_role ELSE 'substitute'::football.lineup_role END,
    CASE WHEN ((number - 1) % 20) = 0 THEN 'G' ELSE 'M' END,
    number,
    CASE WHEN ((number - 1) % 20) < 11 THEN '1:' || (((number - 1) % 11) + 1) ELSE NULL END
FROM numbered_players;

UPDATE source.provider_fetches
SET normalized_at = '2026-08-01 12:00:01+00'
WHERE id = :source_fetch_id;

COMMIT;

DO $$
BEGIN
    IF (SELECT count(*) FROM football.fixture_historical_lineup_snapshots) <> 1
       OR (SELECT count(*) FROM football.fixture_historical_lineups) <> 2
       OR (SELECT count(*) FROM football.fixture_historical_lineup_players) <> 40
       OR (SELECT count(*) FROM football.fixture_lineup_snapshots) <> 0
       OR (SELECT count(*) FROM football.fixture_lineups) <> 0
       OR (SELECT count(*) FROM football.fixture_lineup_players) <> 0 THEN
        RAISE EXCEPTION 'historical lineup happy path or pre-match separation failed';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM football.fixture_historical_lineups lineup
        WHERE lineup.starter_count <> (
                SELECT count(*) FROM football.fixture_historical_lineup_players player
                WHERE player.snapshot_id = lineup.snapshot_id
                  AND player.team_id = lineup.team_id
                  AND player.lineup_role = 'starter'
            )
           OR lineup.substitute_count <> (
                SELECT count(*) FROM football.fixture_historical_lineup_players player
                WHERE player.snapshot_id = lineup.snapshot_id
                  AND player.team_id = lineup.team_id
                  AND player.lineup_role = 'substitute'
            )
    ) THEN
        RAISE EXCEPTION 'historical lineup role counts are inconsistent';
    END IF;
END
$$;

-- The historical lane has its own participant and provider-identity guards.
-- Each rejected operation runs in a PL/pgSQL subtransaction, so a failed
-- normalization cannot leave a canonical snapshot or provenance fetch behind.
INSERT INTO football.players (display_name)
VALUES ('Historical lineup unmapped test player');

INSERT INTO football.coaches (display_name)
VALUES ('Historical lineup unmapped test coach');

DO $$
DECLARE
    v_fixture_id bigint := 1;
    v_provider_id smallint;
    v_external_fixture_id text;
    v_home_team_id bigint;
    v_away_team_id bigint;
    v_nonparticipant_team_id bigint;
    v_mapped_player_id bigint;
    v_unmapped_player_id bigint;
    v_unmapped_coach_id bigint;
    v_fetch_id bigint;
    v_raw_body bytea := convert_to('{"get":"fixtures/lineups","response":[]}', 'UTF8');
BEGIN
    SELECT ref.provider_id, ref.external_id, fixture.home_team_id, fixture.away_team_id
    INTO v_provider_id, v_external_fixture_id, v_home_team_id, v_away_team_id
    FROM source.fixture_provider_refs ref
    JOIN football.fixtures fixture ON fixture.id = ref.fixture_id
    WHERE ref.fixture_id = v_fixture_id;

    SELECT team.id INTO v_nonparticipant_team_id
    FROM football.teams team
    WHERE team.id NOT IN (v_home_team_id, v_away_team_id)
    ORDER BY team.id
    LIMIT 1;

    SELECT player.id INTO v_mapped_player_id
    FROM football.players player
    WHERE player.display_name = 'Historical lineup test player 1';

    SELECT player.id INTO v_unmapped_player_id
    FROM football.players player
    WHERE player.display_name = 'Historical lineup unmapped test player';

    SELECT coach.id INTO v_unmapped_coach_id
    FROM football.coaches coach
    WHERE coach.display_name = 'Historical lineup unmapped test coach';

    -- A team other than the fixture's home/away participants cannot receive a
    -- historical lineup header.
    BEGIN
        INSERT INTO source.provider_fetches (
            provider_id, endpoint, request_params, request_params_sha256, purpose,
            request_started_at, response_received_at, http_status, outcome,
            provider_results, paging_current, paging_total, content_sha256,
            subject_fixture_id
        ) VALUES (
            v_provider_id, '/fixtures/lineups', jsonb_build_object('fixture', v_external_fixture_id),
            decode(repeat('b1', 32), 'hex'), 'historical_backfill',
            '2026-08-05 11:59:00+00', '2026-08-05 12:00:00+00', 200, 'success',
            1, 1, 1, decode(repeat('b1', 32), 'hex'), v_fixture_id
        ) RETURNING id INTO v_fetch_id;

        INSERT INTO source.provider_raw_payloads (
            fetch_id, inline_body, byte_count, retention_class, expires_at
        ) VALUES (
            v_fetch_id, v_raw_body, octet_length(v_raw_body), 'standard',
            clock_timestamp() + interval '30 days'
        );

        INSERT INTO football.fixture_historical_lineup_snapshots (
            fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
            availability_basis, coverage_state, team_count, mapping_version
        ) VALUES (
            v_fixture_id, v_fetch_id, decode(repeat('b1', 32), 'hex'),
            '2026-08-05 12:00:00+00', '2026-08-05 12:00:00+00',
            'reconstructed_conservative', 'partial', 1, 'api-football-lineups-v1'
        ) RETURNING id INTO v_fetch_id;

        INSERT INTO football.fixture_historical_lineups (
            snapshot_id, team_id, coach_id, formation, starter_count, substitute_count
        ) VALUES (v_fetch_id, v_nonparticipant_team_id, NULL, '4-4-2', 0, 0);
        RAISE EXCEPTION 'non-participant team unexpectedly received a historical lineup';
    EXCEPTION WHEN check_violation THEN NULL;
    END;

    -- A fixture-team coach without a provider mapping cannot be normalized.
    BEGIN
        INSERT INTO source.provider_fetches (
            provider_id, endpoint, request_params, request_params_sha256, purpose,
            request_started_at, response_received_at, http_status, outcome,
            provider_results, paging_current, paging_total, content_sha256,
            subject_fixture_id
        ) VALUES (
            v_provider_id, '/fixtures/lineups', jsonb_build_object('fixture', v_external_fixture_id),
            decode(repeat('b2', 32), 'hex'), 'historical_backfill',
            '2026-08-06 11:59:00+00', '2026-08-06 12:00:00+00', 200, 'success',
            1, 1, 1, decode(repeat('b2', 32), 'hex'), v_fixture_id
        ) RETURNING id INTO v_fetch_id;

        INSERT INTO source.provider_raw_payloads (
            fetch_id, inline_body, byte_count, retention_class, expires_at
        ) VALUES (
            v_fetch_id, v_raw_body, octet_length(v_raw_body), 'standard',
            clock_timestamp() + interval '30 days'
        );

        INSERT INTO football.fixture_historical_lineup_snapshots (
            fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
            availability_basis, coverage_state, team_count, mapping_version
        ) VALUES (
            v_fixture_id, v_fetch_id, decode(repeat('b2', 32), 'hex'),
            '2026-08-06 12:00:00+00', '2026-08-06 12:00:00+00',
            'reconstructed_conservative', 'partial', 1, 'api-football-lineups-v1'
        ) RETURNING id INTO v_fetch_id;

        INSERT INTO football.fixture_historical_lineups (
            snapshot_id, team_id, coach_id, formation, starter_count, substitute_count
        ) VALUES (v_fetch_id, v_home_team_id, v_unmapped_coach_id, '4-4-2', 0, 0);
        RAISE EXCEPTION 'unmapped coach unexpectedly entered a historical lineup';
    EXCEPTION WHEN check_violation THEN NULL;
    END;

    -- A fixture-team player without a provider mapping cannot be normalized.
    BEGIN
        INSERT INTO source.provider_fetches (
            provider_id, endpoint, request_params, request_params_sha256, purpose,
            request_started_at, response_received_at, http_status, outcome,
            provider_results, paging_current, paging_total, content_sha256,
            subject_fixture_id
        ) VALUES (
            v_provider_id, '/fixtures/lineups', jsonb_build_object('fixture', v_external_fixture_id),
            decode(repeat('b3', 32), 'hex'), 'historical_backfill',
            '2026-08-07 11:59:00+00', '2026-08-07 12:00:00+00', 200, 'success',
            1, 1, 1, decode(repeat('b3', 32), 'hex'), v_fixture_id
        ) RETURNING id INTO v_fetch_id;

        INSERT INTO source.provider_raw_payloads (
            fetch_id, inline_body, byte_count, retention_class, expires_at
        ) VALUES (
            v_fetch_id, v_raw_body, octet_length(v_raw_body), 'standard',
            clock_timestamp() + interval '30 days'
        );

        INSERT INTO football.fixture_historical_lineup_snapshots (
            fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
            availability_basis, coverage_state, team_count, mapping_version
        ) VALUES (
            v_fixture_id, v_fetch_id, decode(repeat('b3', 32), 'hex'),
            '2026-08-07 12:00:00+00', '2026-08-07 12:00:00+00',
            'reconstructed_conservative', 'partial', 1, 'api-football-lineups-v1'
        ) RETURNING id INTO v_fetch_id;

        INSERT INTO football.fixture_historical_lineups (
            snapshot_id, team_id, coach_id, formation, starter_count, substitute_count
        ) VALUES (v_fetch_id, v_home_team_id, NULL, '4-4-2', 1, 0);

        INSERT INTO football.fixture_historical_lineup_players (
            snapshot_id, team_id, player_id, lineup_role, position, shirt_number, grid
        ) VALUES (v_fetch_id, v_home_team_id, v_unmapped_player_id, 'starter', 'M', 1, '1:1');
        RAISE EXCEPTION 'unmapped player unexpectedly entered a historical lineup';
    EXCEPTION WHEN check_violation THEN NULL;
    END;

    -- The same canonical player cannot be assigned to both teams in one
    -- fixture snapshot even though each (snapshot, team, player) tuple differs.
    BEGIN
        INSERT INTO source.provider_fetches (
            provider_id, endpoint, request_params, request_params_sha256, purpose,
            request_started_at, response_received_at, http_status, outcome,
            provider_results, paging_current, paging_total, content_sha256,
            subject_fixture_id
        ) VALUES (
            v_provider_id, '/fixtures/lineups', jsonb_build_object('fixture', v_external_fixture_id),
            decode(repeat('b4', 32), 'hex'), 'historical_backfill',
            '2026-08-08 11:59:00+00', '2026-08-08 12:00:00+00', 200, 'success',
            2, 1, 1, decode(repeat('b4', 32), 'hex'), v_fixture_id
        ) RETURNING id INTO v_fetch_id;

        INSERT INTO source.provider_raw_payloads (
            fetch_id, inline_body, byte_count, retention_class, expires_at
        ) VALUES (
            v_fetch_id, v_raw_body, octet_length(v_raw_body), 'standard',
            clock_timestamp() + interval '30 days'
        );

        INSERT INTO football.fixture_historical_lineup_snapshots (
            fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
            availability_basis, coverage_state, team_count, mapping_version
        ) VALUES (
            v_fixture_id, v_fetch_id, decode(repeat('b4', 32), 'hex'),
            '2026-08-08 12:00:00+00', '2026-08-08 12:00:00+00',
            'reconstructed_conservative', 'complete', 2, 'api-football-lineups-v1'
        ) RETURNING id INTO v_fetch_id;

        INSERT INTO football.fixture_historical_lineups (
            snapshot_id, team_id, coach_id, formation, starter_count, substitute_count
        ) VALUES
            (v_fetch_id, v_home_team_id, NULL, '4-4-2', 1, 0),
            (v_fetch_id, v_away_team_id, NULL, '4-4-2', 1, 0);

        INSERT INTO football.fixture_historical_lineup_players (
            snapshot_id, team_id, player_id, lineup_role, position, shirt_number, grid
        ) VALUES
            (v_fetch_id, v_home_team_id, v_mapped_player_id, 'starter', 'M', 1, '1:1'),
            (v_fetch_id, v_away_team_id, v_mapped_player_id, 'starter', 'M', 1, '1:1');
        RAISE EXCEPTION 'duplicate player unexpectedly entered both fixture teams';
    EXCEPTION WHEN unique_violation THEN NULL;
    END;

    IF EXISTS (
        SELECT 1
        FROM source.provider_fetches
        WHERE content_sha256 IN (
            decode(repeat('b1', 32), 'hex'),
            decode(repeat('b2', 32), 'hex'),
            decode(repeat('b3', 32), 'hex'),
            decode(repeat('b4', 32), 'hex')
        )
    ) OR (SELECT count(*) FROM football.fixture_historical_lineup_snapshots) <> 1 THEN
        RAISE EXCEPTION 'identity/participant rejection left historical normalization state';
    END IF;
END
$$;

-- Canonical source provenance cannot be rewritten, but bookkeeping can advance.
DO $$
BEGIN
    BEGIN
        UPDATE source.provider_fetches
        SET purpose = 'research'
        WHERE endpoint = '/fixtures/lineups'
          AND content_sha256 = decode(repeat('ab', 32), 'hex');
        RAISE EXCEPTION 'referenced historical fetch purpose changed';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;

    BEGIN
        UPDATE source.provider_fetches
        SET request_params = jsonb_build_object('fixture', 'wrong')
        WHERE endpoint = '/fixtures/lineups'
          AND content_sha256 = decode(repeat('ab', 32), 'hex');
        RAISE EXCEPTION 'referenced historical fetch request changed';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;

    BEGIN
        UPDATE source.provider_fetches
        SET content_sha256 = decode(repeat('ac', 32), 'hex')
        WHERE endpoint = '/fixtures/lineups'
          AND content_sha256 = decode(repeat('ab', 32), 'hex');
        RAISE EXCEPTION 'referenced historical fetch content hash changed';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;

    BEGIN
        UPDATE source.provider_raw_payloads raw
        SET inline_body = convert_to('{"tampered":true}', 'UTF8')
        FROM source.provider_fetches provider_fetch
        WHERE raw.fetch_id = provider_fetch.id
          AND provider_fetch.endpoint = '/fixtures/lineups'
          AND provider_fetch.content_sha256 = decode(repeat('ab', 32), 'hex');
        RAISE EXCEPTION 'historical raw bytes changed';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;

    BEGIN
        UPDATE football.fixture_historical_lineup_snapshots
        SET mapping_version = 'mutated'
        WHERE mapping_version = 'api-football-lineups-v1';
        RAISE EXCEPTION 'historical snapshot update unexpectedly succeeded';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;

    BEGIN
        UPDATE football.fixture_historical_lineups
        SET formation = 'changed'
        WHERE snapshot_id = (
            SELECT id
            FROM football.fixture_historical_lineup_snapshots
            WHERE mapping_version = 'api-football-lineups-v1'
        );
        RAISE EXCEPTION 'historical lineup update unexpectedly succeeded';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;
END
$$;

-- A new child in a later transaction is rejected even if it references the
-- correct fixture team; it cannot bypass the parent commit-count checks.
DO $$
DECLARE
    existing_player_id bigint;
    historical_snapshot_id bigint;
    historical_home_team_id bigint;
BEGIN
    SELECT id INTO existing_player_id
    FROM football.players
    WHERE display_name = 'Historical lineup test player 1';

    SELECT historical.id, fixture.home_team_id
    INTO historical_snapshot_id, historical_home_team_id
    FROM football.fixture_historical_lineup_snapshots historical
    JOIN football.fixtures fixture ON fixture.id = historical.fixture_id
    WHERE historical.mapping_version = 'api-football-lineups-v1';

    BEGIN
        INSERT INTO football.fixture_historical_lineup_players (
            snapshot_id, team_id, player_id, lineup_role, position, shirt_number, grid
        ) VALUES (
            historical_snapshot_id, historical_home_team_id, existing_player_id, 'starter', 'M', 99, NULL
        );
        RAISE EXCEPTION 'later historical lineup child insert unexpectedly succeeded';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;
END
$$;

-- The same fetch cannot create another normalized snapshot.
DO $$
BEGIN
    BEGIN
        INSERT INTO football.fixture_historical_lineup_snapshots (
            fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
            availability_basis, coverage_state, team_count, mapping_version
        )
        SELECT fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
               availability_basis, coverage_state, team_count, mapping_version
        FROM football.fixture_historical_lineup_snapshots
        WHERE mapping_version = 'api-football-lineups-v1';
        RAISE EXCEPTION 'same source fetch replay unexpectedly succeeded';
    EXCEPTION WHEN unique_violation THEN NULL;
    END;
END
$$;

-- Historical source provenance must not enter the existing pre-match lane.
DO $$
BEGIN
    BEGIN
        INSERT INTO football.fixture_lineup_snapshots (
            fixture_id, captured_at, available_at, availability_basis,
            source_fetch_id, coverage_state, team_count
        )
        SELECT fixture_id, captured_at, available_at, 'observed'::football.availability_basis,
               source_fetch_id, coverage_state, team_count
        FROM football.fixture_historical_lineup_snapshots
        WHERE mapping_version = 'api-football-lineups-v1';
        RAISE EXCEPTION 'historical source entered pre-match lane';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
END
$$;

-- The endpoint body has no fixture id. Typed subject provenance alone is not
-- sufficient: the request parameter must resolve to the same provider fixture.
DO $$
DECLARE
    v_target_fixture_id bigint;
    v_other_external_fixture_id text;
    v_provider_id smallint;
    v_invalid_fetch_id bigint;
    raw_body bytea := convert_to('{"get":"fixtures/lineups","response":[]}', 'UTF8');
BEGIN
    SELECT ref.fixture_id, ref.provider_id
    INTO v_target_fixture_id, v_provider_id
    FROM source.fixture_provider_refs ref
    WHERE ref.fixture_id = 1;

    SELECT external_id
    INTO v_other_external_fixture_id
    FROM source.fixture_provider_refs ref
    WHERE ref.provider_id = v_provider_id
      AND ref.fixture_id <> v_target_fixture_id
    ORDER BY ref.fixture_id
    LIMIT 1;

    BEGIN
        INSERT INTO source.provider_fetches (
            provider_id, endpoint, request_params, request_params_sha256, purpose,
            request_started_at, response_received_at, http_status, outcome,
            provider_results, paging_current, paging_total, content_sha256,
            subject_fixture_id
        ) VALUES (
            v_provider_id,
            '/fixtures/lineups',
            jsonb_build_object('fixture', v_other_external_fixture_id),
            decode(repeat('ef', 32), 'hex'),
            'historical_backfill',
            '2026-08-02 11:59:00+00',
            '2026-08-02 12:00:00+00',
            200,
            'success',
            2,
            1,
            1,
            decode(repeat('ef', 32), 'hex'),
            v_target_fixture_id
        ) RETURNING id INTO v_invalid_fetch_id;

        INSERT INTO source.provider_raw_payloads (
            fetch_id, inline_body, byte_count, retention_class, expires_at
        ) VALUES (
            v_invalid_fetch_id, raw_body, octet_length(raw_body), 'standard',
            clock_timestamp() + interval '30 days'
        );

        INSERT INTO football.fixture_historical_lineup_snapshots (
            fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
            availability_basis, coverage_state, team_count, mapping_version
        ) VALUES (
            v_target_fixture_id, v_invalid_fetch_id, decode(repeat('ef', 32), 'hex'),
            '2026-08-02 12:00:00+00', '2026-08-02 12:00:00+00',
            'reconstructed_conservative', 'complete', 2, 'api-football-lineups-v1'
        );
        RAISE EXCEPTION 'mismatched request fixture parameter unexpectedly normalized';
    EXCEPTION WHEN check_violation THEN NULL;
    END;

    IF EXISTS (
        SELECT 1
        FROM source.provider_fetches
        WHERE content_sha256 = decode(repeat('ef', 32), 'hex')
    ) THEN
        RAISE EXCEPTION 'failed request-parameter provenance check left fetch state';
    END IF;
END
$$;

DO $$
BEGIN
    BEGIN
        INSERT INTO football.fixture_historical_lineup_snapshots (
            fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
            availability_basis, coverage_state, team_count, mapping_version
        )
        SELECT fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
               availability_basis, 'complete'::football.snapshot_coverage_state, 1,
               mapping_version
        FROM football.fixture_historical_lineup_snapshots
        WHERE mapping_version = 'api-football-lineups-v1';
        RAISE EXCEPTION 'complete coverage with one team unexpectedly succeeded';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM football.fixture_historical_lineup_snapshots historical
        JOIN football.fixture_lineup_snapshots prematch
          ON prematch.source_fetch_id = historical.source_fetch_id
    ) THEN
        RAISE EXCEPTION 'historical and pre-match lineup lanes were mixed';
    END IF;
END
$$;

-- A different retained provider response is a new immutable correction, not an
-- update to the first historical snapshot.
INSERT INTO source.provider_fetches (
    provider_id, endpoint, request_params, request_params_sha256, purpose,
    request_started_at, response_received_at, http_status, outcome,
    provider_results, paging_current, paging_total, content_sha256,
    subject_fixture_id
)
SELECT provider.id,
       '/fixtures/lineups',
       jsonb_build_object('fixture', ref.external_id),
       decode(repeat('ac', 32), 'hex'),
       'historical_backfill',
       '2026-08-03 11:59:00+00',
       '2026-08-03 12:00:00+00',
       200,
       'success',
       0,
       1,
       1,
       decode(repeat('ac', 32), 'hex'),
       fixture.id
FROM source.providers provider
JOIN source.fixture_provider_refs ref ON ref.provider_id = provider.id
JOIN football.fixtures fixture ON fixture.id = ref.fixture_id
WHERE provider.code = 'api-football'
  AND fixture.id = :fixture_id
RETURNING id AS correction_fetch_id
\gset

WITH payload AS (
    SELECT convert_to('{"get":"fixtures/lineups","response":[]}', 'UTF8') AS body
)
INSERT INTO source.provider_raw_payloads (
    fetch_id, inline_body, byte_count, retention_class, expires_at
)
SELECT :correction_fetch_id, body, octet_length(body), 'standard', clock_timestamp() + interval '30 days'
FROM payload;

BEGIN;
INSERT INTO football.fixture_historical_lineup_snapshots (
    fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
    availability_basis, coverage_state, team_count, mapping_version
)
VALUES (
    :fixture_id,
    :correction_fetch_id,
    decode(repeat('ac', 32), 'hex'),
    '2026-08-03 12:00:00+00',
    '2026-08-03 12:00:00+00',
    'reconstructed_conservative',
    'empty',
    0,
    'api-football-lineups-v1'
);
UPDATE source.provider_fetches
SET normalized_at = '2026-08-03 12:00:01+00'
WHERE id = :correction_fetch_id;
COMMIT;

DO $$
BEGIN
    IF (SELECT count(*) FROM football.fixture_historical_lineup_snapshots) <> 2
       OR NOT EXISTS (
           SELECT 1
           FROM football.fixture_historical_lineup_snapshots
           WHERE content_sha256 = decode(repeat('ab', 32), 'hex')
       )
       OR NOT EXISTS (
           SELECT 1
           FROM football.fixture_historical_lineup_snapshots
           WHERE content_sha256 = decode(repeat('ac', 32), 'hex')
       ) THEN
        RAISE EXCEPTION 'distinct historical lineup correction was not append-only';
    END IF;
END
$$;

-- Transaction A may retain a byte-identical second fetch, but Transaction B
-- cannot create a duplicate canonical snapshot for it.
INSERT INTO source.provider_fetches (
    provider_id, endpoint, request_params, request_params_sha256, purpose,
    request_started_at, response_received_at, http_status, outcome,
    provider_results, paging_current, paging_total, content_sha256,
    subject_fixture_id
)
SELECT provider.id,
       '/fixtures/lineups',
       jsonb_build_object('fixture', ref.external_id),
       decode(repeat('ab', 32), 'hex'),
       'historical_backfill',
       '2026-08-04 11:59:00+00',
       '2026-08-04 12:00:00+00',
       200,
       'success',
       2,
       1,
       1,
       decode(repeat('ab', 32), 'hex'),
       fixture.id
FROM source.providers provider
JOIN source.fixture_provider_refs ref ON ref.provider_id = provider.id
JOIN football.fixtures fixture ON fixture.id = ref.fixture_id
WHERE provider.code = 'api-football'
  AND fixture.id = :fixture_id
RETURNING id AS identical_fetch_id
\gset

WITH payload AS (
    SELECT convert_to('{"get":"fixtures/lineups","response":[]}', 'UTF8') AS body
)
INSERT INTO source.provider_raw_payloads (
    fetch_id, inline_body, byte_count, retention_class, expires_at
)
SELECT :identical_fetch_id, body, octet_length(body), 'standard', clock_timestamp() + interval '30 days'
FROM payload;

DO $$
BEGIN
    BEGIN
        INSERT INTO football.fixture_historical_lineup_snapshots (
            fixture_id, source_fetch_id, content_sha256, captured_at, available_at,
            availability_basis, coverage_state, team_count, mapping_version
        )
        SELECT fixture.id,
               provider_fetch.id,
               provider_fetch.content_sha256,
               provider_fetch.response_received_at,
               provider_fetch.response_received_at,
               'reconstructed_conservative'::football.availability_basis,
               'complete'::football.snapshot_coverage_state,
               2,
               'api-football-lineups-v1'
        FROM source.provider_fetches provider_fetch
        JOIN football.fixtures fixture ON fixture.id = provider_fetch.subject_fixture_id
        WHERE provider_fetch.request_started_at = '2026-08-04 11:59:00+00';
        RAISE EXCEPTION 'byte-identical historical snapshot unexpectedly succeeded';
    EXCEPTION WHEN unique_violation THEN NULL;
    END;

    IF (SELECT count(*) FROM football.fixture_historical_lineup_snapshots) <> 2
       OR NOT EXISTS (
           SELECT 1
           FROM source.provider_raw_payloads raw
           JOIN source.provider_fetches provider_fetch ON provider_fetch.id = raw.fetch_id
           WHERE provider_fetch.request_started_at = '2026-08-04 11:59:00+00'
             AND raw.purged_at IS NULL
       ) THEN
        RAISE EXCEPTION 'byte-identical replay changed canonical rows or lost raw provenance';
    END IF;
END
$$;
