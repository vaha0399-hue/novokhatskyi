import asyncio
import json

import httpx
import pytest

from app.api_football import APIFootballClient
from app.api_football.errors import (
    APIFootballAPIError,
    APIFootballConfigurationError,
    APIFootballHTTPError,
)
from scripts.collect_api_football_samples import (
    MAX_REQUESTS,
    SampleCollector,
    _finished_fixture_id,
)


def test_from_environment_requires_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_FOOTBALL_KEY", raising=False)

    with pytest.raises(APIFootballConfigurationError, match="API_FOOTBALL_KEY is required"):
        APIFootballClient.from_environment()


def test_request_uses_header_and_preserves_raw_body() -> None:
    observed: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        observed["url"] = str(request.url)
        observed["key"] = request.headers.get("x-apisports-key")
        observed["timeout"] = request.extensions["timeout"]
        return httpx.Response(200, content=b'{"errors":{},"response":[null]}')

    async def exercise() -> None:
        client = APIFootballClient("test-secret", transport=httpx.MockTransport(handler))
        response = await client.get("fixtures", params={"league": 39})
        assert response.raw_body == b'{"errors":{},"response":[null]}'
        assert response.data["response"] == [None]

    asyncio.run(exercise())
    assert observed == {
        "url": "https://v3.football.api-sports.io/fixtures?league=39",
        "key": "test-secret",
        "timeout": {"connect": 15.0, "read": 15.0, "write": 15.0, "pool": 15.0},
    }


def test_http_error_is_sanitized_and_not_retried() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            401,
            json={"message": "test-secret must not leak"},
            headers={
                "x-ratelimit-requests-remaining": "42",
                "authorization": "test-secret",
            },
        )

    async def exercise() -> None:
        client = APIFootballClient("test-secret", transport=httpx.MockTransport(handler))
        with pytest.raises(APIFootballHTTPError) as captured:
            await client.get("fixtures")
        assert str(captured.value) == "API-Football returned HTTP 401."
        assert "test-secret" not in str(captured.value)
        assert captured.value.safe_headers == {"x-ratelimit-requests-remaining": "42"}

    asyncio.run(exercise())
    assert calls == 1


def test_api_error_is_sanitized() -> None:
    async def exercise() -> None:
        client = APIFootballClient(
            "test-secret",
            transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"errors": {"token": "test-secret"}})),
        )
        with pytest.raises(APIFootballAPIError) as captured:
            await client.get("fixtures")
        assert str(captured.value) == "API-Football returned an API-level error."
        assert "test-secret" not in str(captured.value)

    asyncio.run(exercise())


def test_collector_writes_raw_body_and_safe_metadata(tmp_path) -> None:
    async def exercise() -> None:
        client = APIFootballClient(
            "test-secret",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200,
                    content=b'{"errors":{},"results":0,"paging":{"current":1},"response":[]}',
                    headers={
                        "x-ratelimit-requests-remaining": "99",
                        "x-ratelimit-requests-limit": "100",
                        "x-secret": "test-secret",
                    },
                )
            ),
        )
        collector = SampleCollector(tmp_path, client, season=2024, request_limit=7)
        await collector.collect("fixtures", "/fixtures", {"league": 39, "season": 2024})

    asyncio.run(exercise())
    assert (tmp_path / "fixtures.raw.json").read_bytes() == (
        b'{"errors":{},"results":0,"paging":{"current":1},"response":[]}'
    )
    metadata = json.loads((tmp_path / "fixtures.request.json").read_text())
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    text = "\n".join(path.read_text() for path in tmp_path.iterdir() if path.suffix == ".json")
    assert metadata["parameters"] == {"league": 39, "season": 2024}
    assert metadata["rate_limit"] == {
        "x-ratelimit-requests-limit": "100",
        "x-ratelimit-requests-remaining": "99",
    }
    assert manifest["request_count"] == 1
    assert manifest["research_season"] == 2024
    assert manifest["production_target_season"] == 2026
    assert manifest["purpose"] == "contract-research-only"
    assert "test-secret" not in text


def test_collector_refuses_to_exceed_request_cap(tmp_path) -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={"errors": {}, "response": []})

    async def exercise() -> None:
        collector = SampleCollector(
            tmp_path,
            APIFootballClient("test-secret", transport=httpx.MockTransport(handler)),
            season=2024,
            request_limit=MAX_REQUESTS,
        )
        collector.manifest["requests"] = [{}] * MAX_REQUESTS
        with pytest.raises(RuntimeError, match="request limit"):
            await collector.collect("blocked", "/fixtures", {"league": 39})

    asyncio.run(exercise())
    assert calls == 0


def test_collector_refuses_raw_body_that_contains_key(tmp_path) -> None:
    async def exercise() -> None:
        client = APIFootballClient(
            "test-secret",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, content=b'{"errors":{},"echo":"test-secret"}')
            ),
        )
        collector = SampleCollector(tmp_path, client, season=2024, request_limit=7)
        with pytest.raises(RuntimeError, match="cannot be safely persisted") as captured:
            await collector.collect("fixtures", "/fixtures", {"league": 39})
        assert "test-secret" not in str(captured.value)

    asyncio.run(exercise())
    assert not list(tmp_path.iterdir())


def test_finished_fixture_selection_requires_a_completed_fixture() -> None:
    fixtures = {
        "response": [
            {"fixture": {"id": 2, "status": {"short": "LIVE"}}},
            {"fixture": {"id": 3, "status": {"short": "CANC"}}},
            {"fixture": {"id": 4, "status": {"short": "FT"}}},
        ]
    }

    assert _finished_fixture_id(fixtures) == 4
