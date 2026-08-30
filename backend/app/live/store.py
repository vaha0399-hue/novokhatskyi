"""Redis-only current live state using the canonical two-key contract."""

from __future__ import annotations

import json
from collections.abc import Collection, Sequence
from datetime import datetime
from typing import Any

from redis.asyncio import Redis

from .models import LiveFixtureState, LiveFixtureStatus, LiveScore


ACTIVE_FIXTURES_KEY = "live:active_fixtures"
FIXTURE_KEY_PREFIX = "live:fixture:"
STATE_SCHEMA_VERSION = 1
_ACTIVE_SNAPSHOT_SCRIPT = """
local fixture_ids = redis.call("SMEMBERS", KEYS[1])
local snapshot = {}
for _, fixture_id in ipairs(fixture_ids) do
    snapshot[#snapshot + 1] = fixture_id
    snapshot[#snapshot + 1] = redis.call("GET", ARGV[1] .. fixture_id) or ""
end
return snapshot
"""


class LiveStateConsistencyError(RuntimeError):
    """Redis current state is incomplete, corrupt, or from an unknown schema."""


def fixture_key(fixture_id: int) -> str:
    if not isinstance(fixture_id, int) or isinstance(fixture_id, bool) or fixture_id <= 0:
        raise ValueError("fixture ID must be a positive integer")
    return f"{FIXTURE_KEY_PREFIX}{fixture_id}"


def _state_payload(state: LiveFixtureState) -> dict[str, Any]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "fixture_id": state.fixture_id,
        "provider_fixture_id": state.provider_fixture_id,
        "provider_league_id": state.provider_league_id,
        "provider_season": state.provider_season,
        "season_id": state.season_id,
        "league_id": state.league_id,
        "kickoff_at": state.kickoff_at.isoformat(),
        "home_team": {"id": state.home_team_id, "name": state.home_team_name},
        "away_team": {"id": state.away_team_id, "name": state.away_team_name},
        "status": state.status.value,
        "score": {"home": state.score.home, "away": state.score.away},
        "elapsed_minute": state.elapsed_minute,
        "added_time": state.added_time,
        "observed_at": state.observed_at.isoformat(),
    }


def encode_live_state(state: LiveFixtureState) -> str:
    return json.dumps(_state_payload(state), separators=(",", ":"), sort_keys=True)


def _required_object(value: Any, field: str, keys: set[str]) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise LiveStateConsistencyError(f"stored live {field} has an invalid shape")
    return value


def decode_live_state(raw: str | bytes) -> LiveFixtureState:
    try:
        payload = json.loads(raw)
        expected_keys = {
            "schema_version",
            "fixture_id",
            "provider_fixture_id",
            "provider_league_id",
            "provider_season",
            "season_id",
            "league_id",
            "kickoff_at",
            "home_team",
            "away_team",
            "status",
            "score",
            "elapsed_minute",
            "added_time",
            "observed_at",
        }
        if not isinstance(payload, dict) or set(payload) != expected_keys:
            raise LiveStateConsistencyError("stored live fixture has an invalid shape")
        if payload["schema_version"] != STATE_SCHEMA_VERSION:
            raise LiveStateConsistencyError("stored live fixture schema is unsupported")
        home = _required_object(payload["home_team"], "home team", {"id", "name"})
        away = _required_object(payload["away_team"], "away team", {"id", "name"})
        score = _required_object(payload["score"], "score", {"home", "away"})
        return LiveFixtureState(
            fixture_id=payload["fixture_id"],
            provider_fixture_id=payload["provider_fixture_id"],
            provider_league_id=payload["provider_league_id"],
            provider_season=payload["provider_season"],
            season_id=payload["season_id"],
            league_id=payload["league_id"],
            kickoff_at=datetime.fromisoformat(payload["kickoff_at"]),
            home_team_id=home["id"],
            home_team_name=home["name"],
            away_team_id=away["id"],
            away_team_name=away["name"],
            status=LiveFixtureStatus(payload["status"]),
            score=LiveScore(home=score["home"], away=score["away"]),
            elapsed_minute=payload["elapsed_minute"],
            added_time=payload["added_time"],
            observed_at=datetime.fromisoformat(payload["observed_at"]),
        )
    except LiveStateConsistencyError:
        raise
    except (json.JSONDecodeError, TypeError, ValueError, KeyError) as error:
        raise LiveStateConsistencyError("stored live fixture is invalid") from error


class RedisLiveStore:
    """Atomic writer and read-only reader for current live fixture state."""

    def __init__(self, client: Redis) -> None:
        self._client = client

    async def apply_poll(
        self,
        active_states: Sequence[LiveFixtureState],
        *,
        finished_fixture_ids: Collection[int] = (),
    ) -> None:
        active_by_id = {state.fixture_id: state for state in active_states}
        if len(active_by_id) != len(active_states):
            raise ValueError("active live states contain duplicate fixture IDs")
        if any(state.status.is_terminal for state in active_states):
            raise ValueError("terminal fixtures cannot be stored as current live state")
        finished = frozenset(finished_fixture_ids)
        for fixture_id in finished:
            fixture_key(fixture_id)
        if active_by_id.keys() & finished:
            raise ValueError("a fixture cannot be active and finished in the same Redis update")

        pipeline = self._client.pipeline(transaction=True)
        for fixture_id, state in active_by_id.items():
            pipeline.set(fixture_key(fixture_id), encode_live_state(state))
            pipeline.sadd(ACTIVE_FIXTURES_KEY, str(fixture_id))
        for fixture_id in finished:
            pipeline.delete(fixture_key(fixture_id))
            pipeline.srem(ACTIVE_FIXTURES_KEY, str(fixture_id))
        await pipeline.execute()

    async def active(self) -> tuple[LiveFixtureState, ...]:
        snapshot = await self._client.eval(
            _ACTIVE_SNAPSHOT_SCRIPT,
            1,
            ACTIVE_FIXTURES_KEY,
            FIXTURE_KEY_PREFIX,
        )
        if not snapshot:
            return ()
        if not isinstance(snapshot, (list, tuple)) or len(snapshot) % 2:
            raise LiveStateConsistencyError("active live fixture snapshot is invalid")

        members = snapshot[::2]
        values = snapshot[1::2]
        try:
            fixture_ids = [int(member) for member in members]
            if any(fixture_id <= 0 for fixture_id in fixture_ids):
                raise ValueError
            if len(fixture_ids) != len(set(fixture_ids)):
                raise ValueError
        except (TypeError, ValueError) as error:
            raise LiveStateConsistencyError("active live fixture set is invalid") from error

        if any(value in (None, "", b"") for value in values):
            raise LiveStateConsistencyError("active live fixture state is incomplete")
        states = tuple(decode_live_state(value) for value in values)
        if any(state.fixture_id != fixture_id for fixture_id, state in zip(fixture_ids, states)):
            raise LiveStateConsistencyError("active live fixture ID does not match its key")
        return tuple(sorted(states, key=lambda state: (state.kickoff_at, state.fixture_id)))
