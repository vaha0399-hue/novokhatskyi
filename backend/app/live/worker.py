"""Single backend-owned API-Football live polling worker."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Protocol

from psycopg import AsyncConnection, InterfaceError, OperationalError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.api_football import APIFootballClient, APIFootballResponse
from app.api_football.errors import APIFootballAPIError, APIFootballHTTPError

from .config import LiveConfigurationError, LiveSettings, managed_redis_client
from .models import (
    CanonicalFixtureReference,
    LiveFixtureState,
    ProviderLiveFixture,
    ProviderFinalResult,
    bind_live_fixture,
)
from .normalizer import LiveNormalizationError, normalize_final_result, normalize_live_response
from .repository import (
    AsyncPostgresLiveRepository,
    FixtureReconciliationTask,
    LiveReconciliationError,
)
from .store import RedisLiveStore


LOGGER = logging.getLogger(__name__)
MAX_TERMINAL_RECHECKS_PER_POLL = 1
TRANSIENT_INFRASTRUCTURE_ERRORS = (
    OperationalError,
    InterfaceError,
    RedisConnectionError,
    RedisTimeoutError,
)


class LiveWorkerError(RuntimeError):
    """A polling cycle cannot safely publish a new Redis snapshot."""


class ProviderClient(Protocol):
    async def get(
        self, endpoint: str, *, params: Mapping[str, str | int] | None = None
    ) -> APIFootballResponse: ...

    def response_contains_api_key(self, body: bytes) -> bool: ...


class LiveRepository(Protocol):
    async def resolve(
        self, fixture: ProviderLiveFixture
    ) -> CanonicalFixtureReference: ...

    async def ensure_terminal_reconciliation(
        self,
        fixture: ProviderLiveFixture,
        *,
        observed_at: datetime,
    ) -> CanonicalFixtureReference: ...

    async def next_due_reconciliation(
        self,
        *,
        league_external_ids: tuple[int, ...],
        as_of: datetime,
        exclude_fixture_ids: Collection[int] = (),
    ) -> FixtureReconciliationTask | None: ...

    async def persist_reconciliation_response(
        self,
        task: FixtureReconciliationTask,
        fixture: ProviderLiveFixture,
        response: APIFootballResponse,
        *,
        request_started_at: datetime,
        response_received_at: datetime,
        result: ProviderFinalResult | None,
        next_attempt_at: datetime,
    ) -> None: ...

    async def record_reconciliation_failure(
        self,
        task: FixtureReconciliationTask,
        *,
        request_started_at: datetime,
        response_received_at: datetime,
        next_attempt_at: datetime,
        http_status: int | None,
        outcome: str,
        error_class: str,
    ) -> None: ...


class LiveStore(Protocol):
    async def active(self) -> tuple[LiveFixtureState, ...]: ...

    async def apply_poll(
        self,
        active_states: Sequence[LiveFixtureState],
        *,
        finished_fixture_ids: Collection[int] = (),
    ) -> None: ...


Clock = Callable[[], datetime]
Sleep = Callable[[float], Awaitable[None]]
Monotonic = Callable[[], float]
SessionRunner = Callable[[ProviderClient, LiveSettings, str], Awaitable[None]]


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class LivePollReport:
    active_count: int
    finished_count: int
    provider_request_count: int


class LiveWorker:
    """Poll one configured league scope and remain the sole live-state writer."""

    def __init__(
        self,
        *,
        provider: ProviderClient,
        repository: LiveRepository,
        store: LiveStore,
        settings: LiveSettings,
        clock: Clock = _utcnow,
        sleep: Sleep = asyncio.sleep,
        monotonic_clock: Monotonic = monotonic,
    ) -> None:
        self._provider = provider
        self._repository = repository
        self._store = store
        self._settings = settings
        self._clock = clock
        self._sleep = sleep
        self._monotonic = monotonic_clock
        self._terminal_recheck_due_at: dict[int, float] = {}
        self._prefer_disappeared = False

    def _normalize_response(
        self,
        response: APIFootballResponse,
        *,
        expected_parameters: Mapping[str, str],
    ) -> tuple[ProviderLiveFixture, ...]:
        if response.data.get("parameters") != dict(expected_parameters):
            raise LiveNormalizationError(
                "provider live response parameters do not match request"
            )
        return normalize_live_response(
            response.data,
            expected_league_ids=self._settings.league_external_ids,
        )

    async def _hand_off_terminal(
        self,
        fixture: ProviderLiveFixture,
        previous: LiveFixtureState | None,
        *,
        observed_at: datetime,
    ) -> int:
        reference = await self._repository.ensure_terminal_reconciliation(
            fixture,
            observed_at=observed_at,
        )
        if previous is not None and previous.fixture_id != reference.fixture_id:
            raise LiveWorkerError(
                "terminal reconciliation resolved a different canonical fixture"
            )
        return reference.fixture_id

    async def _recheck_disappeared_fixture(
        self,
        provider_fixture_id: int,
        previous: LiveFixtureState,
    ) -> tuple[LiveFixtureState | None, int | None]:
        response = await self._provider.get(
            "/fixtures", params={"id": provider_fixture_id}
        )
        observed_at = self._clock()
        fixtures = self._normalize_response(
            response, expected_parameters={"id": str(provider_fixture_id)}
        )
        if len(fixtures) != 1 or fixtures[0].external_fixture_id != provider_fixture_id:
            raise LiveWorkerError("fixture recheck returned the wrong membership")
        fixture = fixtures[0]

        if fixture.status.is_terminal:
            fixture_id = await self._hand_off_terminal(
                fixture,
                previous,
                observed_at=observed_at,
            )
            return None, fixture_id

        reference = await self._repository.resolve(fixture)
        if previous.fixture_id != reference.fixture_id:
            raise LiveWorkerError("live recheck resolved a different canonical fixture")
        return bind_live_fixture(fixture, reference, observed_at=observed_at), None

    async def _process_disappeared_candidate(
        self,
        provider_fixture_id: int,
        previous: LiveFixtureState,
        *,
        recheck_now: float,
    ) -> tuple[LiveFixtureState | None, int | None]:
        self._terminal_recheck_due_at[provider_fixture_id] = (
            recheck_now + self._settings.terminal_recheck_interval_seconds
        )
        state, finished_fixture_id = await self._recheck_disappeared_fixture(
            provider_fixture_id, previous
        )
        if finished_fixture_id is not None:
            self._terminal_recheck_due_at.pop(provider_fixture_id, None)
        return state, finished_fixture_id

    async def _record_reconciliation_failure(
        self,
        task: FixtureReconciliationTask,
        *,
        request_started_at: datetime,
        response_received_at: datetime,
        http_status: int | None,
        outcome: str,
        error_class: str,
    ) -> None:
        await self._repository.record_reconciliation_failure(
            task,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            next_attempt_at=response_received_at
            + timedelta(seconds=self._settings.terminal_recheck_interval_seconds),
            http_status=http_status,
            outcome=outcome,
            error_class=error_class,
        )

    async def _process_due_reconciliation(self, task: FixtureReconciliationTask) -> None:
        request_started_at = self._clock()
        try:
            response = await self._provider.get(
                "/fixtures", params={"id": task.provider_fixture_id}
            )
        except APIFootballHTTPError as error:
            response_received_at = self._clock()
            await self._record_reconciliation_failure(
                task,
                request_started_at=request_started_at,
                response_received_at=response_received_at,
                http_status=error.status_code or None,
                outcome="transport_error" if error.status_code == 0 else "http_error",
                error_class=type(error).__name__,
            )
            return
        except APIFootballAPIError as error:
            response_received_at = self._clock()
            await self._record_reconciliation_failure(
                task,
                request_started_at=request_started_at,
                response_received_at=response_received_at,
                http_status=error.status_code,
                outcome="provider_error",
                error_class=type(error).__name__,
            )
            return

        response_received_at = self._clock()
        if self._provider.response_contains_api_key(response.raw_body):
            await self._record_reconciliation_failure(
                task,
                request_started_at=request_started_at,
                response_received_at=response_received_at,
                http_status=response.status_code,
                outcome="provider_error",
                error_class="ProviderResponseCredentialError",
            )
            return
        try:
            fixtures = self._normalize_response(
                response, expected_parameters={"id": str(task.provider_fixture_id)}
            )
            if len(fixtures) != 1:
                raise LiveWorkerError("reconciliation response returned the wrong membership")
            fixture = fixtures[0]
            result = (
                normalize_final_result(response.data["response"][0])
                if fixture.status.is_terminal
                else None
            )
        except (LiveNormalizationError, LiveWorkerError):
            await self._record_reconciliation_failure(
                task,
                request_started_at=request_started_at,
                response_received_at=response_received_at,
                http_status=response.status_code,
                outcome="provider_error",
                error_class="LiveReconciliationContractError",
            )
            return
        await self._repository.persist_reconciliation_response(
            task,
            fixture,
            response,
            request_started_at=request_started_at,
            response_received_at=response_received_at,
            result=result,
            next_attempt_at=response_received_at
            + timedelta(seconds=self._settings.terminal_recheck_interval_seconds),
        )

    async def poll_once(self) -> LivePollReport:
        """Run one fail-closed provider → canonical → Redis cycle."""
        previous_states = await self._store.active()
        previous_by_provider = {
            state.provider_fixture_id: state for state in previous_states
        }
        if len(previous_by_provider) != len(previous_states):
            raise LiveWorkerError("Redis contains duplicate provider fixture identities")
        expected_leagues = frozenset(self._settings.league_external_ids)
        if any(
            state.provider_league_id not in expected_leagues
            for state in previous_states
        ):
            raise LiveWorkerError("Redis live state escaped the configured league scope")

        response = await self._provider.get(
            "/fixtures", params={"live": self._settings.provider_live_parameter}
        )
        observed_at = self._clock()
        fixtures = self._normalize_response(
            response,
            expected_parameters={"live": self._settings.provider_live_parameter},
        )

        active_states: list[LiveFixtureState] = []
        finished_fixture_ids: set[int] = set()
        current_provider_ids = {
            fixture.external_fixture_id for fixture in fixtures
        }
        for fixture in fixtures:
            previous = previous_by_provider.get(fixture.external_fixture_id)
            if fixture.status.is_terminal:
                finished_fixture_ids.add(
                    await self._hand_off_terminal(
                        fixture,
                        previous,
                        observed_at=observed_at,
                    )
                )
            else:
                reference = await self._repository.resolve(fixture)
                if previous is not None and previous.fixture_id != reference.fixture_id:
                    raise LiveWorkerError(
                        "live poll resolved a different canonical fixture"
                    )
                active_states.append(
                    bind_live_fixture(fixture, reference, observed_at=observed_at)
                )
            self._terminal_recheck_due_at.pop(fixture.external_fixture_id, None)

        disappeared_ids = sorted(
            set(previous_by_provider).difference(current_provider_ids)
        )
        secondary_request_count = 0
        due_task = await self._repository.next_due_reconciliation(
            league_external_ids=self._settings.league_external_ids,
            as_of=observed_at,
            exclude_fixture_ids=finished_fixture_ids,
        )
        recheck_now = self._monotonic() if disappeared_ids else 0.0
        candidate_id = next(
            (
                provider_fixture_id
                for provider_fixture_id in disappeared_ids
                if self._terminal_recheck_due_at.get(provider_fixture_id, 0) <= recheck_now
            ),
            None,
        )
        if due_task is not None and candidate_id is not None and self._prefer_disappeared:
            state, finished_fixture_id = await self._process_disappeared_candidate(
                candidate_id,
                previous_by_provider[candidate_id],
                recheck_now=recheck_now,
            )
            self._prefer_disappeared = False
            secondary_request_count = 1
            if state is not None:
                active_states.append(state)
            if finished_fixture_id is not None:
                finished_fixture_ids.add(finished_fixture_id)
        elif due_task is not None:
            await self._process_due_reconciliation(due_task)
            self._prefer_disappeared = candidate_id is not None
            secondary_request_count = 1
        elif candidate_id is not None:
            state, finished_fixture_id = await self._process_disappeared_candidate(
                candidate_id,
                previous_by_provider[candidate_id],
                recheck_now=recheck_now,
            )
            secondary_request_count = 1
            if state is not None:
                active_states.append(state)
            if finished_fixture_id is not None:
                finished_fixture_ids.add(finished_fixture_id)

        known_provider_ids = set(previous_by_provider) | current_provider_ids
        for provider_fixture_id in tuple(self._terminal_recheck_due_at):
            if provider_fixture_id not in known_provider_ids:
                self._terminal_recheck_due_at.pop(provider_fixture_id, None)

        await self._store.apply_poll(
            active_states, finished_fixture_ids=finished_fixture_ids
        )
        active_fixture_ids = {state.fixture_id for state in previous_states}
        active_fixture_ids.update(state.fixture_id for state in active_states)
        active_fixture_ids.difference_update(finished_fixture_ids)
        return LivePollReport(
            active_count=len(active_fixture_ids),
            finished_count=len(finished_fixture_ids),
            provider_request_count=1 + secondary_request_count,
        )

    async def run_forever(self) -> None:
        """Start each poll on its configured cadence; retry provider failures."""
        while True:
            cycle_started = self._monotonic()
            try:
                report = await self.poll_once()
                LOGGER.debug(
                    "live poll completed: active=%d finished=%d provider_requests=%d",
                    report.active_count,
                    report.finished_count,
                    report.provider_request_count,
                )
            except (APIFootballHTTPError, APIFootballAPIError) as error:
                LOGGER.warning(
                    "live provider poll failed (%s); retrying on the next cycle",
                    type(error).__name__,
                )
            elapsed = max(0.0, self._monotonic() - cycle_started)
            await self._sleep(
                max(0.0, self._settings.poll_interval_seconds - elapsed)
            )


def _database_url() -> str:
    value = os.environ.get("SUPABASE_DB_URL", "").strip()
    if not value:
        raise LiveConfigurationError("SUPABASE_DB_URL is required")
    return value


async def _run_worker_session(
    provider: ProviderClient,
    settings: LiveSettings,
    database_url: str,
) -> None:
    connection = await AsyncConnection.connect(database_url, autocommit=True)
    async with connection:
        async with managed_redis_client(settings) as redis_client:
            await redis_client.ping()
            worker = LiveWorker(
                provider=provider,
                repository=AsyncPostgresLiveRepository(connection),
                store=RedisLiveStore(redis_client),
                settings=settings,
            )
            await worker.run_forever()


async def run_supervised(
    provider: ProviderClient,
    settings: LiveSettings,
    database_url: str,
    *,
    session_runner: SessionRunner = _run_worker_session,
    sleep: Sleep = asyncio.sleep,
) -> None:
    """Reconnect only transient PostgreSQL/Redis failures with sanitized logs."""
    while True:
        try:
            await session_runner(provider, settings, database_url)
        except TRANSIENT_INFRASTRUCTURE_ERRORS as error:
            LOGGER.warning(
                "live infrastructure session failed (%s); reconnecting after %d seconds",
                type(error).__name__,
                settings.poll_interval_seconds,
            )
            await sleep(settings.poll_interval_seconds)
        else:
            raise LiveWorkerError("live worker session exited unexpectedly")


async def run_from_environment() -> None:
    """Own one reusable provider pool and reconnect backend infrastructure."""
    settings = LiveSettings.from_environment()
    async with APIFootballClient.from_environment() as provider:
        await run_supervised(provider, settings, _database_url())


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(run_from_environment())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
