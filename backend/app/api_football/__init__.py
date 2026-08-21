"""Small, backend-only client for API-Football."""

from .client import APIFootballClient, APIFootballResponse
from .errors import APIFootballAPIError, APIFootballConfigurationError, APIFootballHTTPError

__all__ = [
    "APIFootballAPIError",
    "APIFootballClient",
    "APIFootballConfigurationError",
    "APIFootballHTTPError",
    "APIFootballResponse",
]
