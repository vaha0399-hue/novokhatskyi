from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from app.importer.match_data_contract import (
    MatchState,
    ResultKind,
    ResultObservation,
    ScorePair,
    StatisticsPeriod,
    StatusAction,
    TransitionAction,
    regulation_statistics_bucket,
    resolve_result,
    status_rule,
    transition_action,
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


def test_transition_ignores_stale_and_allows_later_terminal_correction() -> None:
    observed = datetime(2026, 9, 7, 12, tzinfo=UTC)
    assert transition_action("1H", "FT", previous_observed_at=observed, incoming_observed_at=observed - timedelta(seconds=1)) is TransitionAction.IGNORE_STALE
    assert transition_action("FT", "SUSP", previous_observed_at=observed, incoming_observed_at=observed + timedelta(seconds=1)) is TransitionAction.RECONCILE_RESULT_CORRECTION


@pytest.mark.parametrize(
    ("previous", "incoming", "expected"),
    [
        ("PST", "NS", TransitionAction.ACCEPT),
        ("1H", "HT", TransitionAction.ACCEPT),
        ("HT", "2H", TransitionAction.ACCEPT),
        ("CANC", "NS", TransitionAction.RECONCILE_RESULT_CORRECTION),
        ("NS", "VAR_DELAY", TransitionAction.REVIEW_CONFLICT),
    ],
)
def test_transition_table(previous: str, incoming: str, expected: TransitionAction) -> None:
    observed = datetime(2026, 9, 7, 12, tzinfo=UTC)
    assert transition_action(previous, incoming, previous_observed_at=observed, incoming_observed_at=observed + timedelta(seconds=1)) is expected
