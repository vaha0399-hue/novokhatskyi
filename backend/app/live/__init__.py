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
    ProviderFinalResult,
    ProviderLiveFixture,
    bind_live_fixture,
)
from .normalizer import (
    LiveNormalizationError,
    normalize_final_result,
    normalize_live_fixture,
    normalize_live_response,
)
from .repository import (
    AsyncPostgresLiveRepository,
    LiveReconciliationError,
    LiveResolutionError,
    PostgresLiveFixtureResolver,
)
from .store import (
    ACTIVE_FIXTURES_KEY,
    LiveStateConsistencyError,
    RedisLiveStore,
    fixture_key,
)

__all__ = [
    "CanonicalFixtureReference",
    "AsyncPostgresLiveRepository",
    "LiveConfigurationError",
    "LiveFixtureState",
    "LiveFixtureStatus",
    "LiveNormalizationError",
    "LiveReconciliationError",
    "LiveResolutionError",
    "LiveScore",
    "LiveStateConsistencyError",
    "PostgresLiveFixtureResolver",
    "ProviderLiveFixture",
    "ProviderFinalResult",
    "RedisLiveStore",
    "LiveSettings",
    "ACTIVE_FIXTURES_KEY",
    "bind_live_fixture",
    "create_redis_client",
    "fixture_key",
    "managed_redis_client",
    "normalize_final_result",
    "normalize_live_fixture",
    "normalize_live_response",
]
