from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.importer.match_data_contract import (
    MatchState,
    ObservationAction,
    PollObservation,
    PhaseTransitionAction,
    ResultKind,
    ResultObservation,
    ScorePair,
    StatisticsPeriod,
    StatusAction,
    regulation_statistics_bucket,
    observation_action,
    observation_fingerprint,
    phase_transition_action,
    resolve_result,
    status_rule,
)


@pytest.mark.parametrize(
    ("code", "state", "action", "played"),
    [
        ("TBD", MatchState.SCHEDULED, StatusAction.STORE_SCHEDULE, False),
        ("NS", MatchState.SCHEDULED, StatusAction.STORE_SCHEDULE, False),
        ("1H", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False),
        ("HT", MatchState.PAUSED, StatusAction.HOLD_AND_RECHECK, False),
        ("2H", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False),
        ("ET", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False),
        ("BT", MatchState.PAUSED, StatusAction.HOLD_AND_RECHECK, False),
        ("P", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False),
        ("LIVE", MatchState.IN_PROGRESS, StatusAction.TRACK_LIVE, False),
        ("SUSP", MatchState.SUSPENDED, StatusAction.HOLD_AND_RECHECK, False),
        ("INT", MatchState.INTERRUPTED, StatusAction.HOLD_AND_RECHECK, False),
        ("FT", MatchState.COMPLETED, StatusAction.RECONCILE_PLAYED_RESULT, True),
        ("AET", MatchState.COMPLETED, StatusAction.RECONCILE_PLAYED_RESULT, True),
        ("PEN", MatchState.COMPLETED, StatusAction.RECONCILE_PLAYED_RESULT, True),
        ("PST", MatchState.POSTPONED, StatusAction.AWAIT_REPLACEMENT_KICKOFF, False),
        ("CANC", MatchState.CANCELLED, StatusAction.MARK_NOT_PLAYED, False),
        ("ABD", MatchState.ABANDONED, StatusAction.PRESERVE_RAW_AND_REVIEW, False),
        ("AWD", MatchState.ADMINISTRATIVE, StatusAction.PRESERVE_RAW_AND_REVIEW, False),
        ("WO", MatchState.ADMINISTRATIVE, StatusAction.PRESERVE_RAW_AND_REVIEW, False),
    ],
)
def test_documented_statuses_have_explicit_contracts(
    code: str, state: MatchState, action: StatusAction, played: bool
) -> None:
    rule = status_rule(code)
    assert (rule.state, rule.action, rule.is_played_result) == (state, action, played)


def test_unknown_status_is_never_silently_normalized() -> None:
    rule = status_rule("VAR_DELAY")
    assert rule.state is MatchState.UNKNOWN
    assert rule.action is StatusAction.PRESERVE_RAW_AND_REVIEW


@pytest.mark.parametrize(
    ("status", "observation", "kind", "regulation", "eligible"),
    [
        ("FT", ResultObservation(ScorePair(2, 1), ScorePair(2, 1), None, None), ResultKind.REGULATION, ScorePair(2, 1), True),
        ("AET", ResultObservation(ScorePair(3, 2), ScorePair(2, 2), ScorePair(3, 2), None), ResultKind.AFTER_EXTRA_TIME, ScorePair(2, 2), True),
        ("PEN", ResultObservation(ScorePair(1, 1), ScorePair(1, 1), None, ScorePair(5, 4)), ResultKind.PENALTY_SHOOTOUT, ScorePair(1, 1), True),
        ("AWD", ResultObservation(ScorePair(3, 0), None, None, None), ResultKind.ADMINISTRATIVE, None, False),
    ],
)
def test_result_examples_preserve_each_provider_period(
    status: str,
    observation: ResultObservation,
    kind: ResultKind,
    regulation: ScorePair | None,
    eligible: bool,
) -> None:
    resolved = resolve_result(status, observation)
    assert (resolved.kind, resolved.regulation_90, resolved.eligible_for_played_match_analytics) == (
        kind,
        regulation,
        eligible,
    )


def test_missing_fulltime_is_not_replaced_by_provider_overall_goals() -> None:
    resolved = resolve_result("FT", ResultObservation(ScorePair(4, 0), None, None, None))
    assert resolved.kind is ResultKind.UNRESOLVED
    assert resolved.regulation_90 is None
    assert resolved.provider_overall == ScorePair(4, 0)
    assert resolved.eligible_for_played_match_analytics is False


def test_unknown_or_extra_time_statistics_never_enter_regulation_bucket() -> None:
    assert regulation_statistics_bucket(StatisticsPeriod.REGULATION_90) is StatisticsPeriod.REGULATION_90
    assert regulation_statistics_bucket(StatisticsPeriod.EXTRA_TIME) is None
    assert regulation_statistics_bucket(StatisticsPeriod.PENALTY_SHOOTOUT) is None
    assert regulation_statistics_bucket(StatisticsPeriod.UNKNOWN) is None


@pytest.mark.parametrize(
    ("previous", "incoming", "expected"),
    [
        ("NS", "HT", PhaseTransitionAction.ACCEPT),
        ("NS", "FT", PhaseTransitionAction.ACCEPT),
        ("2H", "1H", PhaseTransitionAction.REVIEW_REGRESSION),
        ("ET", "2H", PhaseTransitionAction.REVIEW_REGRESSION),
        ("ET", "BT", PhaseTransitionAction.ACCEPT),
        ("BT", "ET", PhaseTransitionAction.ACCEPT),
        ("ET", "ET", PhaseTransitionAction.ACCEPT),
        ("2H", "LIVE", PhaseTransitionAction.ACCEPT_AMBIGUOUS_LIVE),
        ("LIVE", "ET", PhaseTransitionAction.ACCEPT),
    ],
)
def test_phase_transition_allows_skipped_polls_but_not_phase_regressions(
    previous: str, incoming: str, expected: PhaseTransitionAction
) -> None:
    assert phase_transition_action(previous, incoming) is expected


def _poll(code: str, payload: bytes, request_sequence: int, received_at: datetime) -> PollObservation:
    return PollObservation(
        provider_code=code,
        content_fingerprint=observation_fingerprint(code, payload),
        request_sequence=request_sequence,
        received_at=received_at,
    )


def test_identical_terminal_observation_is_no_change_by_content_not_status() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    current = _poll("FT", b'{"goals":{"home":1,"away":0}}', 10, received)
    repeated = _poll("FT", b'{"goals":{"home":1,"away":0}}', 11, received + timedelta(seconds=1))
    corrected = _poll("FT", b'{"goals":{"home":2,"away":0}}', 12, received + timedelta(seconds=2))
    assert observation_action(current, repeated) is ObservationAction.NO_CHANGE
    assert observation_action(current, corrected) is ObservationAction.APPLY_CORRECTION


def test_newer_changed_terminal_status_is_also_a_result_correction() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    current = _poll("FT", b'{"score":"1-1"}', 10, received)
    corrected = _poll("AET", b'{"score":"2-1"}', 11, received + timedelta(seconds=1))
    assert observation_action(current, corrected) is ObservationAction.APPLY_CORRECTION


def test_late_response_from_an_older_dispatched_request_cannot_rollback_projection() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    newer_response = _poll("2H", b'{"score":"1-0"}', 8, received)
    older_response_arriving_later = _poll("1H", b'{"score":"0-0"}', 7, received + timedelta(seconds=5))
    assert observation_action(newer_response, older_response_arriving_later) is ObservationAction.IGNORE_OLDER_REQUEST


def test_same_request_sequence_with_different_content_requires_review() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    current = _poll("2H", b'{"score":"1-0"}', 8, received)
    conflicting_response = _poll("2H", b'{"score":"1-1"}', 8, received + timedelta(seconds=2))
    assert observation_action(current, conflicting_response) is ObservationAction.REVIEW_CONFLICT


def test_newer_phase_regression_requires_review_even_when_response_is_newer() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    current = _poll("ET", b'{"score":"2-2"}', 20, received)
    regression = _poll("2H", b'{"score":"1-1"}', 21, received + timedelta(seconds=1))
    assert observation_action(current, regression) is ObservationAction.REVIEW_PHASE_REGRESSION


def test_newer_terminal_to_live_observation_requires_review() -> None:
    received = datetime(2026, 9, 8, 12, tzinfo=UTC)
    current = _poll("FT", b'{"score":"1-0"}', 30, received)
    repair = _poll("SUSP", b'{"score":"1-0"}', 31, received + timedelta(seconds=1))
    assert observation_action(current, repair) is ObservationAction.REVIEW_CONFLICT
