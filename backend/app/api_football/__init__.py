"""Small, backend-only client for API-Football."""

from .client import APIFootballClient, APIFootballResponse
from .budget import APIFootballBudgetDenied, APIFootballBudgetError, PostgresAPIFootballBudget, budget_retry_delay_seconds
from .errors import APIFootballAPIError, APIFootballConfigurationError, APIFootballHTTPError

__all__ = [
    "APIFootballAPIError",
    "APIFootballBudgetDenied",
    "APIFootballBudgetError",
    "APIFootballClient",
    "APIFootballConfigurationError",
    "APIFootballHTTPError",
    "APIFootballResponse",
    "PostgresAPIFootballBudget",
    "budget_retry_delay_seconds",
]
