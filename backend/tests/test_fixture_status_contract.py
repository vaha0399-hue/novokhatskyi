from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from app.api_football import APIFootballResponse
from app.importer.fixture_status_contract import validate_fixture_status_response


SAMPLE = Path(__file__).parents[2] / "samples" / "api-football" / "fixtures.raw.json"


def response_from_raw(raw_body: bytes) -> APIFootballResponse:
    return APIFootballResponse(
        data=json.loads(raw_body),
        raw_body=raw_body,
        status_code=200,
        headers={},
    )


def test_real_epl_sample_validates_exact_fixture_status_membership() -> None:
    raw_body = SAMPLE.read_bytes()
    response = response_from_raw(raw_body)
    expected_ids = {item["fixture"]["id"] for item in response.data["response"]}

    observations = validate_fixture_status_response(
        response,
        expected_content_sha256=hashlib.sha256(raw_body).digest(),
        expected_fixture_ids=expected_ids,
        allowed_status_codes={"NS", "FT"},
    )

    assert len(observations) == response.data["results"]
    assert {item.external_fixture_id for item in observations} == expected_ids
    assert {item.status_code for item in observations} == {"FT"}


def test_rejects_raw_sha_mismatch() -> None:
    raw_body = SAMPLE.read_bytes()

    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        validate_fixture_status_response(
            response_from_raw(raw_body),
            expected_content_sha256=b"0" * 32,
            expected_fixture_ids=set(),
            allowed_status_codes={"FT"},
        )


def test_rejects_parsed_payload_that_does_not_match_raw_bytes() -> None:
    raw_body = SAMPLE.read_bytes()
    response = response_from_raw(raw_body)
    response.data["results"] = 0

    with pytest.raises(ValueError, match="does not match retained raw payload"):
        validate_fixture_status_response(
            response,
            expected_content_sha256=hashlib.sha256(raw_body).digest(),
            expected_fixture_ids=set(),
            allowed_status_codes={"FT"},
        )


def test_rejects_wrong_fixture_membership() -> None:
    raw_body = SAMPLE.read_bytes()

    with pytest.raises(ValueError, match="membership does not match"):
        validate_fixture_status_response(
            response_from_raw(raw_body),
            expected_content_sha256=hashlib.sha256(raw_body).digest(),
            expected_fixture_ids={-1},
            allowed_status_codes={"FT"},
        )


def test_rejects_unreviewed_provider_status_code() -> None:
    payload = {
        "get": "fixtures",
        "parameters": {"id": "1"},
        "errors": {},
        "results": 1,
        "paging": {"current": 1, "total": 1},
        "response": [{"fixture": {"id": 1, "status": {"short": "LIVE"}}}],
    }
    raw_body = json.dumps(payload, separators=(",", ":")).encode()

    with pytest.raises(ValueError, match="unreviewed provider fixture status"):
        validate_fixture_status_response(
            response_from_raw(raw_body),
            expected_content_sha256=hashlib.sha256(raw_body).digest(),
            expected_fixture_ids={1},
            allowed_status_codes={"NS", "FT"},
        )
