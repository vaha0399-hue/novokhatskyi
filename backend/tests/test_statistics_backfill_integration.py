import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

from app.api_football import APIFootballResponse
from app.api_football.errors import APIFootballAPIError, APIFootballHTTPError
from app.importer.statistics_backfill import (
    ENDPOINT,
    StatisticsBackfillError,
    StatisticsContractError,
    _existing_pair_state,
    _find_reusable_raw,
    _mark_contract_error,
    _params,
    _persist_api_error_response,
    _persist_fetch,
    acquire_context_and_lock,
    approve_contract_replays,
    normalize_raw,
    preflight_statistics_backfill,
    run_statistics_backfill,
)


SAMPLE = Path(__file__).parents[2] / "samples" / "api-football" / "fixture-statistics.raw.json"
TEST_DB_URL = os.environ.get("BACKFILL_TEST_DB_URL")

pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="BACKFILL_TEST_DB_URL is not configured")


def _payload(external_fixture_id: int, team_external_ids: tuple[int, int], *, result_count: int = 2) -> dict:
    payload = json.loads(SAMPLE.read_text())
    payload["parameters"] = {"fixture": str(external_fixture_id)}
    payload["response"][0]["team"]["id"] = team_external_ids[0]
    payload["response"][1]["team"]["id"] = team_external_ids[1]
    payload["response"] = payload["response"][:result_count]
    payload["results"] = result_count
    return payload


def _response(payload: dict, *, daily_remaining: int = 99) -> APIFootballResponse:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(
        payload,
        raw,
        200,
        {
            "x-ratelimit-requests-limit": "100",
            "x-ratelimit-requests-remaining": str(daily_remaining),
            "x-ratelimit-limit": "10",
            "x-ratelimit-remaining": "9",
        },
    )


class OneResponseClient:
    def __init__(self, responses: dict[int, APIFootballResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, int]]] = []

    async def get(self, endpoint: str, *, params: dict[str, int]) -> APIFootballResponse:
        self.calls.append((endpoint, params))
        return self.responses[params["fixture"]]

    def response_contains_api_key(self, body: bytes) -> bool:
        return False


class SequenceClient:
    def __init__(self, outcomes) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0

    async def get(self, endpoint: str, *, params: dict[str, int]) -> APIFootballResponse:
        assert endpoint == ENDPOINT
        self.calls += 1
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def response_contains_api_key(self, body: bytes) -> bool:
        return False


async def _no_sleep(_: float) -> None:
    return None


def _team_external_ids(conn, provider_id: int, target) -> tuple[int, int]:
    rows = conn.execute(
        """SELECT team_id, external_id FROM source.team_provider_refs
           WHERE provider_id=%s AND team_id IN (%s,%s)""",
        (provider_id, target.home_team_id, target.away_team_id),
    ).fetchall()
    mapped = {team_id: int(external_id) for team_id, external_id in rows}
    return mapped[target.home_team_id], mapped[target.away_team_id]


def test_controlled_batch_raw_provenance_atomic_pair_and_quota_stop(monkeypatch: pytest.MonkeyPatch) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id, season_id = acquire_context_and_lock(conn)
        queue, states = preflight_statistics_backfill(
            conn,
            provider_id=provider_id,
            season_id=season_id,
        )
        assert states == {"complete": 1, "empty": 0, "partial": 0, "pending": 379}
        target = queue[0]
        team_external_ids = _team_external_ids(conn, provider_id, target)

    client = OneResponseClient(
        {target.external_id: _response(_payload(target.external_id, team_external_ids), daily_remaining=5)}
    )
    report = run_statistics_backfill(client=client, sleep=_no_sleep, max_calls=90)  # type: ignore[arg-type]

    assert client.calls == [(ENDPOINT, _params(target.external_id))]
    assert report.physical_attempts == 1
    assert report.statistics_rows_created == 2
    assert report.complete == 1
    assert report.empty == report.partial == report.failed == 0
    assert report.stop_reason == "daily_quota_reserve"
    assert report.quota["x-ratelimit-requests-remaining"] == "5"
    assert report.verification["statistics_rows"] == 4
    assert report.verification["covered_fixtures"] == 2
    assert report.verification["remaining_fixtures"] == 378
    assert report.verification["duplicates"] == 0
    assert report.verification["nonparticipants"] == 0
    assert report.verification["orphans"] == 0
    assert report.verification["canary_unchanged"] is True
    assert all(report.verification["out_of_scope_fingerprints_unchanged"].values())

    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id, season_id = acquire_context_and_lock(conn)
        targets, _ = preflight_statistics_backfill(conn, provider_id=provider_id, season_id=season_id)
        imported = next(item for item in targets if item.fixture_id != target.fixture_id)
        assert _existing_pair_state(conn, provider_id=provider_id, target=target) == "done"
        assert _find_reusable_raw(conn, provider_id=provider_id, target=target) is None
        assert conn.execute(
            "SELECT count(*) FROM source.provider_fetches WHERE endpoint=%s AND subject_fixture_id=%s",
            (ENDPOINT, target.fixture_id),
        ).fetchone()[0] == 1

        # Corrupted mixed-fetch provenance must fail before any network access.
        original_fetch_id, team_id = conn.execute(
            """SELECT last_source_fetch_id,team_id FROM football.fixture_team_statistics
               WHERE fixture_id=%s ORDER BY team_id LIMIT 1""",
            (target.fixture_id,),
        ).fetchone()
        alternate_fetch_id = conn.execute(
            """INSERT INTO source.provider_fetches(
                 provider_id,endpoint,request_params,request_params_sha256,purpose,
                 request_started_at,response_received_at,http_status,outcome,provider_results,
                 paging_current,paging_total,content_sha256,normalized_at,
                 subject_fixture_id,subject_season_id)
               SELECT provider_id,endpoint,request_params,request_params_sha256,purpose,
                      clock_timestamp(),clock_timestamp(),http_status,outcome,provider_results,
                      paging_current,paging_total,content_sha256,clock_timestamp(),
                      subject_fixture_id,subject_season_id
               FROM source.provider_fetches WHERE id=%s RETURNING id""",
            (original_fetch_id,),
        ).fetchone()[0]
        conn.execute(
            "ALTER TABLE football.fixture_team_statistics DISABLE TRIGGER fixture_statistics_guard"
        )
        try:
            conn.execute(
                """UPDATE football.fixture_team_statistics SET last_source_fetch_id=%s
                   WHERE fixture_id=%s AND team_id=%s""",
                (alternate_fetch_id, target.fixture_id, team_id),
            )
            with pytest.raises(StatisticsBackfillError, match="exact fixture pair"):
                preflight_statistics_backfill(conn, provider_id=provider_id, season_id=season_id)
        finally:
            conn.execute(
                """UPDATE football.fixture_team_statistics SET last_source_fetch_id=%s
                   WHERE fixture_id=%s AND team_id=%s""",
                (original_fetch_id, target.fixture_id, team_id),
            )
            conn.execute(
                "ALTER TABLE football.fixture_team_statistics ENABLE TRIGGER fixture_statistics_guard"
            )

        # Explicitly approved anomaly replay reuses signed goals_prevented raw without API.
        replay_target = targets[0]
        replay_team_ids = _team_external_ids(conn, provider_id, replay_target)
        replay_payload = _payload(replay_target.external_id, replay_team_ids)
        for block in replay_payload["response"]:
            for statistic in block["statistics"]:
                if statistic["type"] == "goals_prevented":
                    statistic["value"] = "-0.30"
        now = datetime.now(UTC)
        replay_fetch = _persist_fetch(
            conn,
            provider_id=provider_id,
            target=replay_target,
            response=_response(replay_payload),
            started=now - timedelta(seconds=1),
            received=now,
        )
        _mark_contract_error(conn, replay_fetch.fetch_id)
        assert approve_contract_replays(
            conn,
            provider_id=provider_id,
            season_id=season_id,
            fetch_ids=frozenset({replay_fetch.fetch_id}),
        ) == 1
        reopened = _find_reusable_raw(conn, provider_id=provider_id, target=replay_target)
        assert reopened is not None
        assert normalize_raw(
            conn,
            provider_id=provider_id,
            target=replay_target,
            raw=reopened,
        ) == ("complete", 2)
        assert conn.execute(
            """SELECT count(*) FROM football.fixture_team_statistics
               WHERE fixture_id=%s AND goals_prevented=-0.30""",
            (replay_target.fixture_id,),
        ).fetchone()[0] == 2

        # Empty and partial are durable terminal classifications with no half-pair rows.
        for index, result_count in enumerate((0, 1)):
            terminal_target = targets[index + 1]
            team_ids = _team_external_ids(conn, provider_id, terminal_target)
            stored = _persist_fetch(
                conn,
                provider_id=provider_id,
                target=terminal_target,
                response=_response(_payload(terminal_target.external_id, team_ids, result_count=result_count)),
                started=now - timedelta(seconds=1),
                received=now,
            )
            state, created = normalize_raw(
                conn,
                provider_id=provider_id,
                target=terminal_target,
                raw=stored,
            )
            assert (state, created) == (("empty" if result_count == 0 else "partial"), 0)
            assert conn.execute(
                "SELECT count(*) FROM football.fixture_team_statistics WHERE fixture_id=%s",
                (terminal_target.fixture_id,),
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT normalized_at IS NOT NULL FROM source.provider_fetches WHERE id=%s",
                (stored.fetch_id,),
            ).fetchone()[0] is True

        retry_target = targets[3]
        retry_team_ids = _team_external_ids(conn, provider_id, retry_target)

    retry_client = SequenceClient(
        [
            APIFootballHTTPError(
                500,
                safe_headers={"x-ratelimit-requests-remaining": "97"},
            ),
            _response(_payload(retry_target.external_id, retry_team_ids), daily_remaining=5),
        ]
    )
    retry_report = run_statistics_backfill(
        client=retry_client,
        sleep=_no_sleep,
        max_calls=2,
    )  # type: ignore[arg-type]
    assert retry_client.calls == 2
    assert retry_report.physical_attempts == 2
    assert retry_report.retries == 1
    assert retry_report.errors == 1
    assert retry_report.statistics_rows_created == 2

    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id, season_id = acquire_context_and_lock(conn)
        pending, _ = preflight_statistics_backfill(conn, provider_id=provider_id, season_id=season_id)
        rate_target = pending[0]
        rate_team_ids = _team_external_ids(conn, provider_id, rate_target)
        api_error_target = pending[1]

    rate_client = SequenceClient(
        [
            APIFootballHTTPError(
                429,
                safe_headers={"x-ratelimit-requests-remaining": "5", "retry-after": "60"},
            )
        ]
    )
    with pytest.raises(StatisticsBackfillError, match="rate limit"):
        run_statistics_backfill(client=rate_client, sleep=_no_sleep, max_calls=1)  # type: ignore[arg-type]
    assert rate_client.calls == 1

    # The durable 429 checkpoint blocks a later invocation from retrying it.
    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id, season_id = acquire_context_and_lock(conn)
        with pytest.raises(StatisticsBackfillError, match="non-retryable"):
            preflight_statistics_backfill(conn, provider_id=provider_id, season_id=season_id)
        # Test-only conversion to a retryable checkpoint allows exercising attempt #2.
        conn.execute(
            """UPDATE source.provider_fetches SET http_status=500
               WHERE endpoint=%s AND subject_fixture_id=%s AND http_status=429""",
            (ENDPOINT, rate_target.fixture_id),
        )

    malformed = _payload(rate_target.external_id, rate_team_ids, result_count=1)
    malformed["results"] = True
    malformed_client = SequenceClient([_response(malformed)])
    with pytest.raises(StatisticsContractError, match="results/response mismatch"):
        run_statistics_backfill(client=malformed_client, sleep=_no_sleep, max_calls=1)  # type: ignore[arg-type]
    assert malformed_client.calls == 1

    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        provider_id, _ = acquire_context_and_lock(conn)
        now = datetime.now(UTC)
        # A received HTTP-200 provider error retains its raw bytes/hash as anomaly.
        error_raw = b'{"errors":{"rateLimit":"provider-error"},"results":0,"response":[]}'
        error_fetch_id = _persist_api_error_response(
            conn,
            provider_id=provider_id,
            target=api_error_target,
            started=now - timedelta(seconds=1),
            received=now,
            error=APIFootballAPIError(
                {"rateLimit": "provider-error"},
                raw_body=error_raw,
                status_code=200,
                safe_headers={"x-ratelimit-requests-remaining": "4"},
            ),
        )
        assert error_fetch_id is not None

    with psycopg.connect(TEST_DB_URL, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM football.fixture_team_statistics WHERE fixture_id=%s",
            (rate_target.fixture_id,),
        ).fetchone()[0] == 0
        assert conn.execute(
            """SELECT f.outcome::text,f.normalized_at,f.provider_results,
                      octet_length(f.content_sha256),r.retention_class::text
               FROM source.provider_fetches f JOIN source.provider_raw_payloads r ON r.fetch_id=f.id
               WHERE f.subject_fixture_id=%s AND f.endpoint=%s ORDER BY f.id DESC LIMIT 1""",
            (rate_target.fixture_id, ENDPOINT),
        ).fetchone() == ("provider_error", None, None, 32, "anomaly")
        assert conn.execute(
            """SELECT f.outcome::text, octet_length(f.content_sha256), r.inline_body, r.retention_class::text
               FROM source.provider_fetches f JOIN source.provider_raw_payloads r ON r.fetch_id=f.id
               WHERE f.id=%s""",
            (error_fetch_id,),
        ).fetchone() == ("provider_error", 32, error_raw, "anomaly")
