from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.analytics.models import FixtureContext
from app.web.repository import FixtureRecord, TeamRecord
from app.web.service import WebNotFoundError, WebReadService, _fixture


NOW = datetime(2024, 8, 16, 19, tzinfo=UTC)
HOME = TeamRecord(10, "Home")
AWAY = TeamRecord(20, "Away")


def _record(*, finalized_at: datetime | None) -> FixtureRecord:
    return FixtureRecord(
        context=FixtureContext(1, 3, NOW, HOME.id, AWAY.id),
        round_label="Regular Season - 1", lifecycle_state="completed",
        home_team=HOME, away_team=AWAY, home_goals=2, away_goals=1,
        result_finalized_at=finalized_at,
    )


def test_completed_but_unfinalized_fixture_never_exposes_final_score() -> None:
    assert _fixture(_record(finalized_at=None)).final_score is None
    assert _fixture(_record(finalized_at=NOW)).final_score is not None


class MissingSeasonRepository:
    def season_exists(self, *, season_id: int) -> bool:
        return False


def test_team_analytics_reports_unknown_season_before_team_membership_check() -> None:
    service = WebReadService(MissingSeasonRepository(), analytics=None)  # type: ignore[arg-type]
    with pytest.raises(WebNotFoundError) as error:
        service.team_analytics(team_id=10, season_id=999, scope=None, window=10)  # type: ignore[arg-type]
    assert error.value.code == "season_not_found"
