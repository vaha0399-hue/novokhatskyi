from __future__ import annotations

import json
import logging

from app.sync.diagnostics import budget_reason, emit_lifecycle, provider_http_reason, safe_scope


def test_lifecycle_events_keep_only_safe_scope_and_never_log_secret_like_values(caplog) -> None:
    scope = {
        "provider_id": 7,
        "season_id": 2026,
        "fixture_id": 42,
        "params": {"api_key": "test-secret"},
        "authorization": "Bearer test-secret",
    }
    with caplog.at_level(logging.INFO, logger="app.sync.lifecycle"):
        emit_lifecycle("job_quarantined", job_id=9, attempts=2, scope=scope,
                       reason="handler_failure", raw_exception="test-secret")

    event = json.loads(caplog.messages[-1])
    assert event == {
        "attempts": 2,
        "event": "job_quarantined",
        "job_id": 9,
        "reason": "handler_failure",
        "scope": {"fixture_id": 42, "provider_id": 7, "season_id": 2026},
    }
    # Callers must not pass arbitrary extras; remove that avenue explicitly.
    assert "params" not in event["scope"] and "authorization" not in event["scope"]
    assert "test-secret" not in caplog.text


def test_scope_and_reason_codes_are_stable_and_non_secret_bearing() -> None:
    assert safe_scope({"_sync_policy": {"provider_id": 1, "season_id": 2, "work_type": "fixtures"}}) == {
        "provider_id": 1, "season_id": 2, "work_type": "fixtures",
    }
    assert budget_reason("daily_limit") == "budget_daily_limit"
    assert budget_reason("token=test-secret") == "budget_denied"
    assert provider_http_reason(503) == "provider_http_503"
    assert provider_http_reason("secret") == "provider_http_error"


def test_work_and_extra_fields_are_allowlisted_before_json_logging(caplog) -> None:
    with caplog.at_level(logging.INFO, logger="app.sync.lifecycle"):
        emit_lifecycle(
            "job_claimed", scope={"work_type": "api_key=test-secret"},
            job_type="api_key=test-secret", run_id=-1, checkpoint_advanced="true",
        )
        emit_lifecycle(
            "job_claimed", scope={"work_type": "calendar_refresh"},
            job_type="calendar_refresh", run_id=7, checkpoint_advanced=True,
        )

    unsafe, safe = (json.loads(message) for message in caplog.messages[-2:])
    assert unsafe == {"event": "job_claimed", "scope": {}}
    assert safe == {
        "checkpoint_advanced": True,
        "event": "job_claimed",
        "job_type": "calendar_refresh",
        "run_id": 7,
        "scope": {"work_type": "calendar_refresh"},
    }
