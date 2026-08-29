"""Read-only canonical identity resolution for normalized live fixtures."""

from __future__ import annotations

from typing import Any

from psycopg import Connection

from .models import CanonicalFixtureReference, ProviderLiveFixture


class LiveResolutionError(RuntimeError):
    """An expected provider fixture has no exact canonical identity."""


class PostgresLiveFixtureResolver:
    """Resolve provider identities without creating or updating mappings."""

    def __init__(self, connection: Connection[Any]) -> None:
        self._connection = connection

    def resolve(self, fixture: ProviderLiveFixture) -> CanonicalFixtureReference:
        row = self._connection.execute(
            """SELECT canonical.id,canonical.season_id,season.league_id,canonical.kickoff_at,
                      home.id,home.name,away.id,away.name
               FROM source.providers provider
               JOIN source.fixture_provider_refs fixture_ref
                 ON fixture_ref.provider_id=provider.id
               JOIN football.fixtures canonical ON canonical.id=fixture_ref.fixture_id
               JOIN football.seasons season ON season.id=canonical.season_id
               JOIN source.season_provider_refs season_ref
                 ON season_ref.provider_id=provider.id
                AND season_ref.season_id=canonical.season_id
               JOIN source.team_provider_refs home_ref
                 ON home_ref.provider_id=provider.id
                AND home_ref.team_id=canonical.home_team_id
               JOIN source.team_provider_refs away_ref
                 ON away_ref.provider_id=provider.id
                AND away_ref.team_id=canonical.away_team_id
               JOIN football.teams home ON home.id=canonical.home_team_id
               JOIN football.teams away ON away.id=canonical.away_team_id
               WHERE provider.code=%s
                 AND fixture_ref.external_id=%s
                 AND season_ref.league_external_id=%s
                 AND season_ref.external_season=%s
                 AND home_ref.external_id=%s
                 AND away_ref.external_id=%s""",
            (
                "api-football",
                str(fixture.external_fixture_id),
                str(fixture.league_external_id),
                fixture.season_start_year,
                str(fixture.home_external_team_id),
                str(fixture.away_external_team_id),
            ),
        ).fetchone()
        if row is None:
            raise LiveResolutionError(
                "canonical fixture mapping is missing or conflicts with provider identity"
            )
        return CanonicalFixtureReference(
            fixture_id=int(row[0]),
            season_id=int(row[1]),
            league_id=int(row[2]),
            kickoff_at=row[3],
            home_team_id=int(row[4]),
            home_team_name=str(row[5]),
            away_team_id=int(row[6]),
            away_team_name=str(row[7]),
        )
