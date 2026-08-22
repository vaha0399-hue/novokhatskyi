import asyncio
import copy
import json
from pathlib import Path

import pytest

from app.api_football import APIFootballResponse
from app.api_football.errors import APIFootballHTTPError
from app.importer.season_backfill import (
    BATCH_SIZE,
    EXPECTED_FIXTURE_COUNT,
    MAX_API_ATTEMPTS,
    REQUEST_PARAMS,
    AttemptFailure,
    chunked,
    collect_fixture_season,
    validate_fixture_season_response,
)

SAMPLE = Path(__file__).parents[2] / "samples" / "api-football" / "fixtures.raw.json"


def sample_response() -> APIFootballResponse:
    raw = SAMPLE.read_bytes()
    return APIFootballResponse(
        data=json.loads(raw),
        raw_body=raw,
        status_code=200,
        headers={
            "x-ratelimit-requests-limit": "100",
            "x-ratelimit-requests-remaining": "99",
        },
    )


def sample_team_ids(response: APIFootballResponse) -> set[int]:
    return {
        entry["teams"][side]["id"]
        for entry in response.data["response"]
        for side in ("home", "away")
    }


class QueuedClient:
    def __init__(self, outcomes: list[object]) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, dict[str, int]]] = []

    async def get(self, endpoint: str, *, params: dict[str, int]) -> APIFootballResponse:
        self.calls.append((endpoint, dict(params)))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, APIFootballResponse)
        return outcome


def test_real_season_sample_contract_and_batch_shape() -> None:
    response = sample_response()
    records = validate_fixture_season_response(
        response,
        allowed_team_external_ids=sample_team_ids(response),
    )

    assert len(records) == EXPECTED_FIXTURE_COUNT == 380
    assert [len(batch) for batch in chunked(records, BATCH_SIZE)] == [50] * 7 + [30]
    assert [record.external_id for record in records] == sorted(record.external_id for record in records)
    assert len(sample_team_ids(response)) == 20


def test_request_is_exactly_one_season_fixtures_call() -> None:
    failures: list[AttemptFailure] = []
    client = QueuedClient([sample_response()])

    collected = asyncio.run(
        collect_fixture_season(
            client,  # type: ignore[arg-type]
            record_failure=failures.append,
            sleep=lambda _: asyncio.sleep(0),
        )
    )

    assert collected.attempts == 1
    assert client.calls == [("/fixtures", REQUEST_PARAMS)]
    assert failures == []


def test_transient_failures_retry_at_most_twice() -> None:
    failures: list[AttemptFailure] = []
    sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    client = QueuedClient(
        [
            APIFootballHTTPError(503),
            APIFootballHTTPError(0),
            sample_response(),
        ]
    )
    collected = asyncio.run(
        collect_fixture_season(
            client,  # type: ignore[arg-type]
            record_failure=failures.append,
            sleep=record_sleep,
        )
    )

    assert collected.attempts == MAX_API_ATTEMPTS == 3
    assert len(client.calls) == 3
    assert [failure.outcome for failure in failures] == ["http_error", "transport_error"]
    assert len(sleeps) == 2


def test_third_transient_failure_stops_at_hard_cap() -> None:
    failures: list[AttemptFailure] = []
    client = QueuedClient([APIFootballHTTPError(503)] * 3)

    with pytest.raises(APIFootballHTTPError):
        asyncio.run(
            collect_fixture_season(
                client,  # type: ignore[arg-type]
                record_failure=failures.append,
                sleep=lambda _: asyncio.sleep(0),
            )
        )

    assert len(client.calls) == len(failures) == MAX_API_ATTEMPTS


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422, 429])
def test_non_retryable_http_status_stops_after_one_attempt(status: int) -> None:
    failures: list[AttemptFailure] = []
    client = QueuedClient([APIFootballHTTPError(status)])

    with pytest.raises(APIFootballHTTPError):
        asyncio.run(
            collect_fixture_season(
                client,  # type: ignore[arg-type]
                record_failure=failures.append,
                sleep=lambda _: asyncio.sleep(0),
            )
        )

    assert len(client.calls) == len(failures) == 1


def test_unexpected_paging_fails_without_requesting_page_two() -> None:
    response = sample_response()
    response.data["paging"]["total"] = 2
    client = QueuedClient([response])
    collected = asyncio.run(
        collect_fixture_season(
            client,  # type: ignore[arg-type]
            record_failure=lambda _: None,
            sleep=lambda _: asyncio.sleep(0),
        )
    )

    with pytest.raises(ValueError, match="single complete page"):
        validate_fixture_season_response(
            collected.response,
            allowed_team_external_ids=sample_team_ids(collected.response),
        )
    assert client.calls == [("/fixtures", REQUEST_PARAMS)]


def test_contract_rejects_duplicate_fixture_id_before_dml() -> None:
    response = sample_response()
    payload = copy.deepcopy(response.data)
    payload["response"][1]["fixture"]["id"] = payload["response"][0]["fixture"]["id"]
    invalid = APIFootballResponse(payload, json.dumps(payload).encode(), 200, {})

    with pytest.raises(ValueError, match="duplicate fixture.id"):
        validate_fixture_season_response(
            invalid,
            allowed_team_external_ids=sample_team_ids(response),
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda payload: payload["response"].pop(), "exactly 380"),
        (lambda payload: payload["response"][0]["league"].update({"season": 2023}), "unexpected league"),
        (lambda payload: payload["response"][0]["fixture"]["status"].update({"short": "NS"}), "only FT"),
        (
            lambda payload: payload["response"][0]["teams"].update(
                {"away": payload["response"][0]["teams"]["home"]}
            ),
            "must differ",
        ),
    ],
)
def test_contract_rejects_incomplete_or_unsafe_fixture_data(mutation, message: str) -> None:
    response = sample_response()
    payload = copy.deepcopy(response.data)
    mutation(payload)
    if len(payload["response"]) != EXPECTED_FIXTURE_COUNT:
        payload["results"] = len(payload["response"])
    invalid = APIFootballResponse(payload, json.dumps(payload).encode(), 200, {})

    with pytest.raises(ValueError, match=message):
        validate_fixture_season_response(
            invalid,
            allowed_team_external_ids=sample_team_ids(response),
        )


def test_chunk_size_must_be_positive() -> None:
    with pytest.raises(ValueError, match="positive"):
        chunked([], 0)
