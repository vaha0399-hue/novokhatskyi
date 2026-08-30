"""Provider-independent live score domain."""

from .config import (
    LiveConfigurationError,
    LiveSettings,
    create_redis_client,
    managed_redis_client,
)
from .models import (
    CanonicalFixtureReference,
    LiveFixtureState,
    LiveFixtureStatus,
    LiveScore,
    ProviderLiveFixture,
    bind_live_fixture,
)
from .normalizer import LiveNormalizationError, normalize_live_fixture, normalize_live_response
from .repository import LiveResolutionError, PostgresLiveFixtureResolver
from .store import (
    ACTIVE_FIXTURES_KEY,
    LiveStateConsistencyError,
    RedisLiveStore,
    fixture_key,
)

__all__ = [
    "CanonicalFixtureReference",
    "LiveConfigurationError",
    "LiveFixtureState",
    "LiveFixtureStatus",
    "LiveNormalizationError",
    "LiveResolutionError",
    "LiveScore",
    "LiveStateConsistencyError",
    "PostgresLiveFixtureResolver",
    "ProviderLiveFixture",
    "RedisLiveStore",
    "LiveSettings",
    "ACTIVE_FIXTURES_KEY",
    "bind_live_fixture",
    "create_redis_client",
    "fixture_key",
    "managed_redis_client",
    "normalize_live_fixture",
    "normalize_live_response",
]
