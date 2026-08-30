from __future__ import annotations

import asyncio
import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from app.live import (
    ACTIVE_FIXTURES_KEY,
    CanonicalFixtureReference,
    LiveFixtureStatus,
    LiveStateConsistencyError,
    RedisLiveStore,
    bind_live_fixture,
    fixture_key,
    normalize_live_fixture,
)
from app.live.store import decode_live_state, encode_live_state


class FakePipeline:
    def __init__(self, redis: "FakeRedis") -> None:
        self.redis = redis
        self.commands: list[tuple[str, tuple[object, ...]]] = []

    def set(self, *args: object) -> "FakePipeline":
        self.commands.append(("set", args))
        return self

    def sadd(self, *args: object) -> "FakePipeline":
        self.commands.append(("sadd", args))
        return self

    def delete(self, *args: object) -> "FakePipeline":
        self.commands.append(("delete", args))
        return self

    def srem(self, *args: object) -> "FakePipeline":
        self.commands.append(("srem", args))
        return self

    async def execute(self) -> list[object]:
        for command, args in self.commands:
            if command == "set":
                self.redis.values[str(args[0])] = str(args[1])
            elif command == "sadd":
                self.redis.sets.setdefault(str(args[0]), set()).add(str(args[1]))
            elif command == "delete":
                self.redis.values.pop(str(args[0]), None)
            elif command == "srem":
                self.redis.sets.setdefault(str(args[0]), set()).discard(str(args[1]))
        self.redis.executed.append(tuple(self.commands))
        return [True] * len(self.commands)


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.sets: dict[str, set[str]] = {}
        self.executed: list[tuple[tuple[str, tuple[object, ...]], ...]] = []

    def pipeline(self, *, transaction: bool) -> FakePipeline:
        assert transaction is True
        return FakePipeline(self)

    async def smembers(self, key: str) -> set[str]:
        return set(self.sets.get(key, set()))

    async def mget(self, keys: list[str]) -> list[str | None]:
        return [self.values.get(key) for key in keys]

    async def eval(
        self,
        script: str,
        numkeys: int,
        active_key: str,
        fixture_prefix: str,
    ) -> list[str]:
        assert script
        assert numkeys == 1
        snapshot: list[str] = []
        for member in sorted(self.sets.get(active_key, set())):
            snapshot.extend([member, self.values.get(f"{fixture_prefix}{member}", "")])
        return snapshot


class FinishedTransitionRedis(FakeRedis):
    """Finish one fixture immediately after Redis captured the read snapshot."""

    def __init__(self, fixture_id: int) -> None:
        super().__init__()
        self.fixture_id = fixture_id

    def _finish_fixture(self) -> None:
        self.values.pop(fixture_key(self.fixture_id), None)
        self.sets.setdefault(ACTIVE_FIXTURES_KEY, set()).discard(str(self.fixture_id))

    async def smembers(self, key: str) -> set[str]:
        members = await super().smembers(key)
        self._finish_fixture()
        return members

    async def eval(
        self,
        script: str,
        numkeys: int,
        active_key: str,
        fixture_prefix: str,
    ) -> list[str]:
        snapshot = await super().eval(
            script, numkeys, active_key, fixture_prefix
        )
        self._finish_fixture()
        return snapshot


def _state(fixture_id: int = 101, *, status: str = "2H"):
    provider = normalize_live_fixture(
        {
            "fixture": {
                "id": 1557383 + fixture_id,
                "status": {"short": status, "elapsed": 67, "extra": 1},
            },
            "league": {"id": 39, "season": 2026},
            "teams": {"home": {"id": 40}, "away": {"id": 65}},
            "goals": {"home": 2, "away": 1},
        }
    )
    canonical = CanonicalFixtureReference(
        fixture_id=fixture_id,
        season_id=14,
        league_id=7,
        kickoff_at=datetime(2026, 8, 30, 15, tzinfo=UTC),
        home_team_id=10,
        home_team_name="Liverpool",
        away_team_id=20,
        away_team_name="Nottingham Forest",
    )
    return bind_live_fixture(
        provider, canonical, observed_at=datetime(2026, 8, 30, 16, 7, tzinfo=UTC)
    )


def test_live_state_round_trip_has_stable_current_score_shape() -> None:
    state = _state()

    encoded = encode_live_state(state)
    payload = json.loads(encoded)

    assert decode_live_state(encoded) == state
    assert payload["score"] == {"home": 2, "away": 1}
    assert "fulltime" not in encoded
    assert payload["elapsed_minute"] == 67
    assert payload["added_time"] == 1


def test_store_atomically_updates_only_the_two_live_key_families() -> None:
    async def exercise() -> tuple[FakeRedis, tuple]:
        client = FakeRedis()
        store = RedisLiveStore(client)  # type: ignore[arg-type]
        await store.apply_poll([_state(101), _state(102)])
        return client, await store.active()

    client, states = asyncio.run(exercise())

    assert set(client.values) == {fixture_key(101), fixture_key(102)}
    assert client.sets == {ACTIVE_FIXTURES_KEY: {"101", "102"}}
    assert [state.fixture_id for state in states] == [101, 102]
    assert len(client.executed) == 1


def test_finished_fixture_is_removed_from_current_state() -> None:
    async def exercise() -> FakeRedis:
        client = FakeRedis()
        store = RedisLiveStore(client)  # type: ignore[arg-type]
        await store.apply_poll([_state(101)])
        await store.apply_poll([], finished_fixture_ids={101})
        assert await store.active() == ()
        return client

    client = asyncio.run(exercise())

    assert fixture_key(101) not in client.values
    assert client.sets[ACTIVE_FIXTURES_KEY] == set()


def test_terminal_state_cannot_be_persisted_as_current() -> None:
    client = FakeRedis()
    state = replace(_state(), status=LiveFixtureStatus.FINISHED)

    with pytest.raises(ValueError, match="terminal fixtures"):
        asyncio.run(RedisLiveStore(client).apply_poll([state]))  # type: ignore[arg-type]

    assert client.executed == []


def test_reader_rejects_a_terminal_state_left_in_the_active_set() -> None:
    client = FakeRedis()
    terminal = replace(_state(), status=LiveFixtureStatus.FINISHED)
    client.values[fixture_key(terminal.fixture_id)] = encode_live_state(terminal)
    client.sets[ACTIVE_FIXTURES_KEY] = {str(terminal.fixture_id)}

    with pytest.raises(LiveStateConsistencyError, match="terminal"):
        asyncio.run(RedisLiveStore(client).active())  # type: ignore[arg-type]


def test_store_rejects_active_finished_overlap_before_redis_write() -> None:
    client = FakeRedis()

    with pytest.raises(ValueError, match="active and finished"):
        asyncio.run(
            RedisLiveStore(client).apply_poll(  # type: ignore[arg-type]
                [_state(101)], finished_fixture_ids={101}
            )
        )

    assert client.executed == []


def test_reader_fails_closed_when_active_set_points_to_missing_state() -> None:
    client = FakeRedis()
    client.sets[ACTIVE_FIXTURES_KEY] = {"101"}

    with pytest.raises(LiveStateConsistencyError, match="incomplete"):
        asyncio.run(RedisLiveStore(client).active())  # type: ignore[arg-type]


def test_reader_gets_atomic_snapshot_during_finished_transition() -> None:
    async def exercise() -> tuple[FinishedTransitionRedis, tuple]:
        client = FinishedTransitionRedis(101)
        store = RedisLiveStore(client)  # type: ignore[arg-type]
        await store.apply_poll([_state(101)])
        return client, await store.active()

    client, states = asyncio.run(exercise())

    assert [state.fixture_id for state in states] == [101]
    assert fixture_key(101) not in client.values
    assert client.sets[ACTIVE_FIXTURES_KEY] == set()
