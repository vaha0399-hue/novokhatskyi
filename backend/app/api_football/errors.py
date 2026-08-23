"""Sanitized exceptions for API-Football requests.

Exceptions intentionally never include request headers, URLs with credentials, or API keys.
"""

from collections.abc import Mapping


class APIFootballError(RuntimeError):
    """Base error for API-Football client failures."""


class APIFootballConfigurationError(APIFootballError):
    """Raised when the backend API-Football configuration is incomplete."""


class APIFootballHTTPError(APIFootballError):
    """Raised for a non-successful HTTP response."""

    def __init__(self, status_code: int, *, safe_headers: Mapping[str, str] | None = None) -> None:
        super().__init__(f"API-Football returned HTTP {status_code}.")
        self.status_code = status_code
        self.safe_headers = dict(safe_headers or {})


class APIFootballAPIError(APIFootballError):
    """Raised when API-Football returns application-level errors in a 2xx body."""

    def __init__(
        self,
        errors: object,
        *,
        raw_body: bytes | None = None,
        status_code: int | None = None,
        safe_headers: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__("API-Football returned an API-level error.")
        # Provider error details are intentionally not retained in the exception message.
        # The importer may persist raw bytes without displaying them.
        self.error_type = type(errors).__name__
        self.raw_body = raw_body
        self.status_code = status_code
        self.safe_headers = dict(safe_headers or {})
