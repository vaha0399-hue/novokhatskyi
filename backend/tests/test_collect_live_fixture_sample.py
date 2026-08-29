import asyncio
import hashlib
import json

import httpx
import pytest

from app.api_football import APIFootballClient
from scripts.collect_live_fixture_sample import (
    ENDPOINT,
    PARAMETERS,
    RAW_FILE,
    REQUEST_FILE,
    SUMMARY_FILE,
    collect,
    summarize_live_fixtures,
)


RAW = b'''{"get":"fixtures","parameters":{"live":"all"},"errors":[],"results":2,"paging":{"current":1,"total":1},"response":[{"fixture":{"id":1,"status":{"short":"1H","long":"First Half","elapsed":23,"extra":null}},"teams":{"home":{"name":"Home One"},"away":{"name":"Away One"}},"goals":{"home":1,"away":0},"score":{"halftime":{"home":null,"away":null},"fulltime":{"home":null,"away":null},"extratime":{"home":null,"away":null},"penalty":{"home":null,"away":null}}},{"fixture":{"id":2,"status":{"short":"HT","long":"Halftime","elapsed":45,"extra":2}},"teams":{"home":{"name":"Home Two"},"away":{"name":"Away Two"}},"goals":{"home":2,"away":2},"score":{"halftime":{"home":2,"away":2},"fulltime":{"home":null,"away":null},"extratime":{"home":null,"away":null},"penalty":{"home":null,"away":null}}}]}'''


def test_summarize_live_fixtures_preserves_status_goals_and_score_fields() -> None:
    summary = summarize_live_fixtures(json.loads(RAW))

    assert summary["response_fixture_count"] == 2
    assert summary["status_counts"] == {"1H": 1, "HT": 1}
    assert summary["fixtures"] == [
        {
            "fixture_id": 1,
            "home_team": "Home One",
            "away_team": "Away One",
            "status": {"short": "1H", "long": "First Half", "elapsed": 23, "extra": None},
            "goals": {"home": 1, "away": 0},
            "score": {
                "halftime": {"home": None, "away": None},
                "fulltime": {"home": None, "away": None},
                "extratime": {"home": None, "away": None},
                "penalty": {"home": None, "away": None},
            },
        },
        {
            "fixture_id": 2,
            "home_team": "Home Two",
            "away_team": "Away Two",
            "status": {"short": "HT", "long": "Halftime", "elapsed": 45, "extra": 2},
            "goals": {"home": 2, "away": 2},
            "score": {
                "halftime": {"home": 2, "away": 2},
                "fulltime": {"home": None, "away": None},
                "extratime": {"home": None, "away": None},
                "penalty": {"home": None, "away": None},
            },
        },
    ]


def test_collect_writes_unmodified_raw_body_and_safe_artifacts(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == ENDPOINT
        assert dict(request.url.params) == PARAMETERS
        return httpx.Response(
            200,
            content=RAW,
            headers={
                "x-ratelimit-requests-remaining": "7498",
                "x-ratelimit-requests-limit": "7500",
                "x-ignored-secret": "not-persisted",
            },
        )

    summary = asyncio.run(
        collect(
            tmp_path,
            client=APIFootballClient("test-secret", transport=httpx.MockTransport(handler)),
        )
    )

    assert summary["status_counts"] == {"1H": 1, "HT": 1}
    assert (tmp_path / RAW_FILE).read_bytes() == RAW
    metadata = json.loads((tmp_path / REQUEST_FILE).read_text())
    assert metadata["endpoint"] == ENDPOINT
    assert metadata["parameters"] == PARAMETERS
    assert metadata["content_sha256"] == hashlib.sha256(RAW).hexdigest()
    assert metadata["byte_count"] == len(RAW)
    assert metadata["rate_limit"] == {
        "x-ratelimit-requests-limit": "7500",
        "x-ratelimit-requests-remaining": "7498",
    }
    assert json.loads((tmp_path / SUMMARY_FILE).read_text())["fixtures"] == summary["fixtures"]
    assert "test-secret" not in "\n".join(path.read_text() for path in tmp_path.iterdir())


def test_collect_requires_an_empty_output_directory(tmp_path) -> None:
    (tmp_path / "existing.json").write_text("{}")

    with pytest.raises(ValueError, match="must be empty"):
        asyncio.run(
            collect(
                tmp_path,
                client=APIFootballClient(
                    "test-secret",
                    transport=httpx.MockTransport(lambda request: httpx.Response(200, content=RAW)),
                ),
            )
        )
