"""Fail-closed validation for exact provider fixture-status persistence.

This module performs the JSON membership and byte-integrity checks that are
intentionally kept out of PostgreSQL triggers. It makes no API calls and does
not write to the database; a future fixture importer must validate a response
here before upserting ``source.fixture_provider_status``.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Collection, Mapping
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
    excluded_fixture_status_codes: Mapping[int, str] | None = None,
) -> tuple[FixtureStatusObservation, ...]:
    """Validate raw bytes and return exact fixture/status memberships.

    ``expected_fixture_ids`` normally describes the complete requested result
    set. A reviewed bootstrap projection may additionally declare exact raw
    fixtures that must remain provenance-only (for example relegation
    playoffs returned in a league-season payload). Those IDs and their status
    values are matched exactly, but are deliberately not normalized or mapped.
    Unknown canonical status codes always fail closed.
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
    excluded = dict(excluded_fixture_status_codes or {})
    expected_ids = frozenset(expected_fixture_ids)
    if expected_ids & excluded.keys():
        raise ValueError("canonical and excluded fixture status memberships overlap")
    if any(
        not isinstance(fixture_id, int) or isinstance(fixture_id, bool) or fixture_id <= 0
        or not isinstance(status_code, str) or not status_code
        for fixture_id, status_code in excluded.items()
    ):
        raise ValueError("excluded fixture status contract is invalid")
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
        if external_fixture_id in observed_ids:
            raise ValueError("provider fixture response contains a duplicate fixture id")
        observed_ids.add(external_fixture_id)
        if external_fixture_id in excluded:
            if status_code != excluded[external_fixture_id]:
                raise ValueError("excluded fixture status does not match reviewed contract")
            continue
        if status_code not in allowed:
            raise ValueError(f"unreviewed provider fixture status code: {status_code}")
        observations.append(FixtureStatusObservation(external_fixture_id, status_code))

    if observed_ids != expected_ids | frozenset(excluded):
        raise ValueError("provider fixture response membership does not match requested fixtures")

    return tuple(observations)
