"""Fail-closed validation for season coverage snapshots from API-Football."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from app.api_football import APIFootballResponse


@dataclass(frozen=True)
class SeasonCoverageObservation:
    fixture_statistics_supported: bool
    lineups_supported: bool
    standings_supported: bool
    injuries_supported: bool


def validate_season_coverage_response(
    response: APIFootballResponse,
    *,
    expected_content_sha256: bytes,
    external_league_id: int,
    external_season: int,
) -> SeasonCoverageObservation:
    """Validate retained raw bytes and extract only approved capability flags."""

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
    if not isinstance(raw_payload, dict) or raw_payload.get("get") != "leagues":
        raise ValueError("season coverage provenance requires a /leagues response")
    if raw_payload.get("errors") not in ({}, [], None):
        raise ValueError("provider leagues response contains errors")
    response_items = raw_payload.get("response")
    if not isinstance(response_items, list) or raw_payload.get("results") != len(response_items):
        raise ValueError("provider leagues results count does not match response array")

    matching_seasons: list[dict[str, Any]] = []
    for item in response_items:
        if not isinstance(item, dict):
            raise ValueError("provider league item must be an object")
        league = item.get("league")
        seasons = item.get("seasons")
        if not isinstance(league, dict) or not isinstance(seasons, list):
            raise ValueError("provider league item has an invalid contract")
        if league.get("id") == external_league_id:
            matching_seasons.extend(
                season
                for season in seasons
                if isinstance(season, dict) and season.get("year") == external_season
            )

    if len(matching_seasons) != 1:
        raise ValueError("provider leagues response must contain exactly one requested season")
    coverage = matching_seasons[0].get("coverage")
    fixtures = coverage.get("fixtures") if isinstance(coverage, dict) else None
    if not isinstance(coverage, dict) or not isinstance(fixtures, dict):
        raise ValueError("provider season coverage has an invalid contract")

    fields = {
        "fixture_statistics_supported": fixtures.get("statistics_fixtures"),
        "lineups_supported": fixtures.get("lineups"),
        "standings_supported": coverage.get("standings"),
        "injuries_supported": coverage.get("injuries"),
    }
    if not all(isinstance(value, bool) for value in fields.values()):
        raise ValueError("approved provider coverage flags must be booleans")

    return SeasonCoverageObservation(**fields)
