"""Safe, structured operational diagnostics for the opt-in sync control plane.

Events are intentionally observations only: callers must never allow a logging
failure to alter queue, lease, or domain-write semantics.  Scope is reduced to
identifiers useful for correlation; request parameters and exception text are
not loggable diagnostics.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from typing import Any


LOGGER = logging.getLogger("app.sync.lifecycle")

_SCOPE_KEYS = frozenset(
    {
        "provider_id",
        "season_id",
        "fixture_id",
        "team_id",
        "subject_fixture_id",
        "subject_season_id",
        "subject_team_id",
        "work_type",
    }
)
_REASON = re.compile(r"^[a-z0-9_]{1,80}$")
_EXTRA_FIELDS = frozenset({"run_id", "job_type", "checkpoint_advanced"})


def safe_scope(scope: Mapping[str, Any]) -> dict[str, int | str]:
    """Return only correlation identifiers from a work scope.

    This deliberately excludes arbitrary scope values because scopes can
    contain raw request-adjacent metadata in older producers.
    """
    result: dict[str, int | str] = {}
    for key in _SCOPE_KEYS:
        value = scope.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            result[key] = value
        elif key == "work_type" and isinstance(value, str) and value:
            result[key] = value
    policy = scope.get("_sync_policy")
    if isinstance(policy, Mapping):
        for key in ("provider_id", "season_id", "work_type"):
            if key not in result:
                value = policy.get(key)
                if isinstance(value, int) and not isinstance(value, bool):
                    result[key] = value
                elif key == "work_type" and isinstance(value, str) and value:
                    result[key] = value
    return result


def emit_lifecycle(
    event: str,
    *,
    job_id: int | None = None,
    attempts: int | None = None,
    scope: Mapping[str, Any] | None = None,
    duration_ms: float | None = None,
    reason: str | None = None,
    **extra: int | str | bool | None,
) -> None:
    """Best-effort JSON event with stable, secret-safe fields only."""
    try:
        payload: dict[str, object] = {"event": event}
        if job_id is not None:
            payload["job_id"] = job_id
        if attempts is not None:
            payload["attempts"] = attempts
        if scope is not None:
            payload["scope"] = safe_scope(scope)
        if duration_ms is not None:
            payload["duration_ms"] = round(max(0.0, duration_ms), 3)
        if reason is not None:
            payload["reason"] = reason if _REASON.fullmatch(reason) else "unknown_failure"
        payload.update({
            key: value for key, value in extra.items()
            if key in _EXTRA_FIELDS and value is not None
        })
        LOGGER.info("%s", json.dumps(payload, sort_keys=True, separators=(",", ":")))
    except Exception:
        # Observability cannot affect the fenced transaction or failure path.
        return


def budget_reason(reason: object) -> str:
    """Map a Q04 denial to a stable public-safe reason code."""
    allowed = {
        "cooldown",
        "daily_limit",
        "minute_limit",
        "operations_limit",
        "history_limit",
        "legacy_manual_limit",
        "provider_daily_exhausted",
        "provider_minute_limit",
    }
    return f"budget_{reason}" if reason in allowed else "budget_denied"


def provider_http_reason(status_code: object) -> str:
    """Expose only a valid HTTP status, never a provider response body."""
    return f"provider_http_{status_code}" if isinstance(status_code, int) and 0 <= status_code <= 599 else "provider_http_error"
