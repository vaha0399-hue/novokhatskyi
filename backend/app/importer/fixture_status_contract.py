"""Fail-closed validation for exact provider fixture-status persistence.

This module performs the JSON membership and byte-integrity checks that are
intentionally kept out of PostgreSQL triggers. It makes no API calls and does
not write to the database; a future fixture importer must validate a response
here before upserting ``source.fixture_provider_status``.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any

from app.api_football import APIFootballResponse


@dataclass(frozen=True)
class FixtureStatusObservation:
    external_fixture_id: int
    status_code: str


def validate_fixture_status_response(
    response: APIFootballResponse,
    *,
    expected_content_sha256: bytes,
    expected_fixture_ids: Collection[int],
    allowed_status_codes: Collection[str],
) -> tuple[FixtureStatusObservation, ...]:
    """Validate raw bytes and return exact fixture/status memberships.

    ``expected_fixture_ids`` must describe the complete requested result set,
    not merely a subset. Unknown status codes fail closed and require contract
    review before a new database mapping can be introduced.
    """

    if len(expected_content_sha256) != 32:
        raise ValueError("expected content SHA-256 must contain 32 bytes")
    if hashlib.sha256(response.raw_body).digest() != expected_content_sha256:
        raise ValueError("provider raw payload SHA-256 mismatch")

    try:
        raw_payload: Any = json.loads(response.raw_body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("provider raw payload is not valid UTF-8 JSON") from error

    if raw_payload != response.data:
        raise ValueError("parsed response does not match retained raw payload")
    if not isinstance(raw_payload, dict) or raw_payload.get("get") != "fixtures":
        raise ValueError("fixture status provenance requires a /fixtures response")
    if raw_payload.get("errors") not in ({}, [], None):
        raise ValueError("provider fixture response contains errors")

    items = raw_payload.get("response")
    if not isinstance(items, list):
        raise ValueError("provider fixture response must be an array")
    if raw_payload.get("results") != len(items):
        raise ValueError("provider fixture results count does not match response array")

    allowed = frozenset(allowed_status_codes)
    observations: list[FixtureStatusObservation] = []
    observed_ids: set[int] = set()
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("provider fixture item must be an object")
        fixture = item.get("fixture")
        if not isinstance(fixture, dict):
            raise ValueError("provider fixture item is missing fixture object")
        external_fixture_id = fixture.get("id")
        status = fixture.get("status")
        if not isinstance(external_fixture_id, int) or isinstance(external_fixture_id, bool):
            raise ValueError("provider fixture id must be an integer")
        if not isinstance(status, dict) or not isinstance(status.get("short"), str):
            raise ValueError("provider fixture status.short must be a string")
        status_code = status["short"]
        if status_code not in allowed:
            raise ValueError(f"unreviewed provider fixture status code: {status_code}")
        if external_fixture_id in observed_ids:
            raise ValueError("provider fixture response contains a duplicate fixture id")
        observed_ids.add(external_fixture_id)
        observations.append(FixtureStatusObservation(external_fixture_id, status_code))

    expected_ids = frozenset(expected_fixture_ids)
    if observed_ids != expected_ids:
        raise ValueError("provider fixture response membership does not match requested fixtures")

    return tuple(observations)
