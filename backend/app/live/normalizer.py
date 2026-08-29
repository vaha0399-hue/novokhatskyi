"""Fail-closed API-Football live fixture normalization."""

from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import Any

from .models import LiveFixtureStatus, LiveScore, ProviderLiveFixture


class LiveNormalizationError(ValueError):
    """A provider payload cannot safely become current live state."""


STATUS_MAP = {
    "1H": LiveFixtureStatus.FIRST_HALF,
    "HT": LiveFixtureStatus.HALF_TIME,
    "2H": LiveFixtureStatus.SECOND_HALF,
    "FT": LiveFixtureStatus.FINISHED,
}


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise LiveNormalizationError(f"{field} must be an object")
    return value


def _positive_int(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise LiveNormalizationError(f"{field} must be a positive integer")
    return value


def _non_negative_int(value: Any, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise LiveNormalizationError(f"{field} must be a non-negative integer")
    return value


def _optional_non_negative_int(value: Any, field: str) -> int | None:
    if value is None:
        return None
    return _non_negative_int(value, field)


def normalize_live_fixture(item: object) -> ProviderLiveFixture:
    """Normalize one fixture using goals and status fields only.

    ``score.fulltime`` is deliberately neither read nor validated here. It is
    not a current-score field and must not influence Redis live state.
    """
    entry = _mapping(item, "fixture entry")
    fixture = _mapping(entry.get("fixture"), "fixture")
    league = _mapping(entry.get("league"), "league")
    teams = _mapping(entry.get("teams"), "teams")
    goals = _mapping(entry.get("goals"), "goals")
    status = _mapping(fixture.get("status"), "fixture.status")
    home = _mapping(teams.get("home"), "teams.home")
    away = _mapping(teams.get("away"), "teams.away")

    status_code = status.get("short")
    if not isinstance(status_code, str) or status_code not in STATUS_MAP:
        raise LiveNormalizationError(f"unsupported live fixture status: {status_code!r}")

    home_team_id = _positive_int(home.get("id"), "teams.home.id")
    away_team_id = _positive_int(away.get("id"), "teams.away.id")
    if home_team_id == away_team_id:
        raise LiveNormalizationError("live fixture teams must be distinct")

    return ProviderLiveFixture(
        external_fixture_id=_positive_int(fixture.get("id"), "fixture.id"),
        league_external_id=_positive_int(league.get("id"), "league.id"),
        season_start_year=_positive_int(league.get("season"), "league.season"),
        home_external_team_id=home_team_id,
        away_external_team_id=away_team_id,
        status=STATUS_MAP[status_code],
        score=LiveScore(
            home=_non_negative_int(goals.get("home"), "goals.home"),
            away=_non_negative_int(goals.get("away"), "goals.away"),
        ),
        elapsed_minute=_optional_non_negative_int(
            status.get("elapsed"), "fixture.status.elapsed"
        ),
        added_time=_optional_non_negative_int(status.get("extra"), "fixture.status.extra"),
    )


def normalize_live_response(
    payload: Mapping[str, Any], *, expected_league_ids: Collection[int] | None = None
) -> tuple[ProviderLiveFixture, ...]:
    """Normalize one complete ``/fixtures`` response without silent filtering."""
    if payload.get("get") != "fixtures":
        raise LiveNormalizationError("live state requires a /fixtures response")
    if payload.get("errors") not in ({}, [], None):
        raise LiveNormalizationError("provider live response contains errors")
    if payload.get("paging") != {"current": 1, "total": 1}:
        raise LiveNormalizationError("provider live response paging is incomplete")
    response = payload.get("response")
    if not isinstance(response, list) or payload.get("results") != len(response):
        raise LiveNormalizationError("provider live results count is invalid")

    fixtures = tuple(normalize_live_fixture(item) for item in response)
    external_ids = [fixture.external_fixture_id for fixture in fixtures]
    if len(external_ids) != len(set(external_ids)):
        raise LiveNormalizationError("provider live response contains duplicate fixture IDs")

    if expected_league_ids is not None:
        expected = frozenset(expected_league_ids)
        if not expected or any(fixture.league_external_id not in expected for fixture in fixtures):
            raise LiveNormalizationError("provider live response escaped the requested league scope")
    return fixtures
