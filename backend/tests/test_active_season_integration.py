from __future__ import annotations

import copy
import json
import os
import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest

import app.importer.active_season as active_season
from app.api_football import APIFootballResponse
from app.importer.active_season import (
    ActiveSeasonScope,
    base_requests,
    import_active_base,
    verify_active_season,
)
from app.importer.canary import parse_datetime
from app.importer.fixture_schedule_observations import (
    FixtureScheduleObservationConflict,
)
from app.importer.season_bootstrap import CollectedBaseResponse


TEST_DB_URL = os.environ.get("ACTIVE_SEASON_TEST_DB_URL")
pytestmark = pytest.mark.skipif(
    not TEST_DB_URL, reason="ACTIVE_SEASON_TEST_DB_URL is not configured"
)
SAMPLES = Path(__file__).parents[2] / "samples" / "api-football" / "pro-canary-2026-08-29"
SCOPE = ActiveSeasonScope(league_external_id=39, season_start_year=2026, expected_fixture_count=380)
SINGLE_FIXTURE_SCOPE = ActiveSeasonScope(
    league_external_id=141,
    season_start_year=2026,
    expected_fixture_count=380,
    require_complete_schedule=False,
)


def _response(payload: dict) -> APIFootballResponse:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return APIFootballResponse(payload, raw, 200, {})


def _stored_response(path: Path) -> APIFootballResponse:
    raw = path.read_bytes()
    return APIFootballResponse(json.loads(raw), raw, 200, {})


def _collected(received_at: datetime) -> tuple[CollectedBaseResponse, ...]:
    files = {
        "/leagues": "01-leagues-epl-2026.raw.json",
        "/teams": "02-teams-epl-2026.raw.json",
        "/standings": "04-standings-epl-2026.raw.json",
        "/fixtures": "03-fixtures-epl-2026.raw.json",
    }
    return tuple(
        CollectedBaseResponse(
            request=request,
            response=_stored_response(SAMPLES / files[request.endpoint]),
            request_started_at=received_at - timedelta(seconds=1),
            response_received_at=received_at,
        )
        for request in base_requests(SCOPE)
    )


def _partial_collected(received_at: datetime) -> tuple[CollectedBaseResponse, ...]:
    """A separate regular league with one not-yet-published fixture."""
    scope = ActiveSeasonScope(
        league_external_id=140,
        season_start_year=2026,
        expected_fixture_count=380,
        require_complete_schedule=False,
    )
    original = _collected(received_at)
    transformed: list[CollectedBaseResponse] = []
    for item, request in zip(original, base_requests(scope), strict=True):
        payload = copy.deepcopy(item.response.data)
        payload["parameters"] = {key: str(value) for key, value in request.params.items()}
        if request.endpoint == "/leagues":
            payload["response"][0]["league"].update({"id": 140, "name": "Partial Test League"})
        elif request.endpoint == "/standings":
            payload["response"][0]["league"].update({"id": 140, "name": "Partial Test League"})
        elif request.endpoint == "/fixtures":
            payload["response"] = payload["response"][:-1]
            payload["results"] = len(payload["response"])
            for fixture in payload["response"]:
                fixture["league"].update({"id": 140, "name": "Partial Test League"})
                fixture["fixture"]["id"] += 1_000_000
            next(fixture for fixture in payload["response"] if fixture["fixture"]["status"]["short"] == "FT")["fixture"]["status"]["short"] = "PEN"
        transformed.append(
            CollectedBaseResponse(
                request=request,
                response=_response(payload),
                request_started_at=item.request_started_at,
                response_received_at=item.response_received_at,
            )
        )
    return tuple(transformed)


def _single_fixture_collected(received_at: datetime) -> tuple[CollectedBaseResponse, ...]:
    """A partial one-fixture calendar derived from retained provider bytes."""
    original = _collected(received_at)
    transformed: list[CollectedBaseResponse] = []
    for item, request in zip(
        original, base_requests(SINGLE_FIXTURE_SCOPE), strict=True
    ):
        payload = copy.deepcopy(item.response.data)
        payload["parameters"] = {
            key: str(value) for key, value in request.params.items()
        }
        if request.endpoint == "/leagues":
            payload["response"][0]["league"].update(
                {"id": 141, "name": "Observation Test League"}
            )
        elif request.endpoint == "/standings":
            payload["response"][0]["league"].update(
                {"id": 141, "name": "Observation Test League"}
            )
        elif request.endpoint == "/fixtures":
            fixture = next(
                fixture
                for fixture in payload["response"]
                if fixture["fixture"]["status"]["short"] == "NS"
            )
            fixture["fixture"]["id"] += 2_000_000
            fixture["league"].update(
                {"id": 141, "name": "Observation Test League"}
            )
            payload["response"] = [fixture]
            payload["results"] = 1
        transformed.append(
            CollectedBaseResponse(
                request=request,
                response=_response(payload),
                request_started_at=item.request_started_at,
                response_received_at=item.response_received_at,
            )
        )
    return tuple(transformed)


def _with_fixture_kickoff(
    collected: tuple[CollectedBaseResponse, ...], kickoff: str
) -> tuple[CollectedBaseResponse, ...]:
    changed = list(collected)
    fixture_item = changed[3]
    payload = copy.deepcopy(fixture_item.response.data)
    payload["response"][0]["fixture"]["date"] = kickoff
    changed[3] = CollectedBaseResponse(
        request=fixture_item.request,
        response=_response(payload),
        request_started_at=fixture_item.request_started_at,
        response_received_at=fixture_item.response_received_at,
    )
    return tuple(changed)


def test_partial_calendar_is_upserted_without_claiming_a_full_schedule() -> None:
    assert TEST_DB_URL is not None
    partial_scope = ActiveSeasonScope(140, 2026, 380, require_complete_schedule=False)
    full_scope = ActiveSeasonScope(140, 2026, 380)
    received_at = datetime.now(UTC)
    with psycopg.connect(TEST_DB_URL) as conn:
        first = import_active_base(conn, collected=_partial_collected(received_at), scope=partial_scope)
        partial = verify_active_season(conn, scope=partial_scope)

        assert first.season_id == partial.season_id
        assert (partial.team_count, partial.fixture_count, partial.fixture_mapping_count, partial.standing_row_count) == (20, 379, 379, 20)

        full_payload = list(_partial_collected(received_at + timedelta(minutes=1)))
        fixture_response = _collected(received_at + timedelta(minutes=1))[3].response.data
        full_fixture_payload = {
            **copy.deepcopy(fixture_response),
            "parameters": {"league": "140", "season": "2026"},
            "response": [
                {
                    **fixture,
                    "fixture": {**fixture["fixture"], "id": fixture["fixture"]["id"] + 1_000_000},
                    "league": {**fixture["league"], "id": 140, "name": "Partial Test League"},
                }
                for fixture in fixture_response["response"]
            ],
        }
        full_fixture_payload["results"] = len(full_fixture_payload["response"])
        full_payload[3] = CollectedBaseResponse(
            request=full_payload[3].request,
            response=_response(full_fixture_payload),
            request_started_at=received_at,
            response_received_at=received_at + timedelta(minutes=1),
        )
        import_active_base(conn, collected=tuple(full_payload), scope=full_scope)
        full = verify_active_season(conn, scope=full_scope)

        assert (full.fixture_count, full.fixture_mapping_count) == (380, 380)
        conn.rollback()


def test_calendar_observation_replay_conflict_and_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert TEST_DB_URL is not None
    received_at = datetime.now(UTC)
    collected = _single_fixture_collected(received_at)
    raw_kickoff = collected[3].response.data["response"][0]["fixture"]["date"]
    expected_kickoff = parse_datetime(raw_kickoff)
    original_persist_fetch = active_season._persist_fetch

    with psycopg.connect(TEST_DB_URL) as conn:
        context = import_active_base(
            conn, collected=collected, scope=SINGLE_FIXTURE_SCOPE
        )
        fixture_row = conn.execute(
            """SELECT fixture.id, fixture.kickoff_at, provider_fetch.id
               FROM source.fixture_provider_refs ref
               JOIN football.fixtures fixture ON fixture.id=ref.fixture_id
               JOIN source.provider_fetches provider_fetch
                 ON provider_fetch.id=fixture.last_source_fetch_id
               WHERE ref.provider_id=%s AND fixture.season_id=%s""",
            (context.provider_id, context.season_id),
        ).fetchone()
        assert fixture_row is not None
        fixture_id, canonical_kickoff, fixture_fetch_id = fixture_row
        assert canonical_kickoff == expected_kickoff
        assert conn.execute(
            """SELECT provider_id, fixture_id, source_fetch_id,
                      observed_kickoff_at, observed_at
               FROM source.fixture_schedule_observations
               WHERE provider_id=%s AND fixture_id=%s""",
            (context.provider_id, fixture_id),
        ).fetchall() == [
            (
                context.provider_id,
                fixture_id,
                fixture_fetch_id,
                expected_kickoff,
                received_at,
            )
        ]
        conn.commit()

        def reuse_fixture_fetch(
            connection: psycopg.Connection,
            *,
            provider_id: int,
            collected: CollectedBaseResponse,
            season_id: int,
            retention_class: str = "standard",
        ) -> int:
            if collected.request.endpoint == "/fixtures":
                return fixture_fetch_id
            return original_persist_fetch(
                connection,
                provider_id=provider_id,
                collected=collected,
                season_id=season_id,
                retention_class=retention_class,
            )

        monkeypatch.setattr(active_season, "_persist_fetch", reuse_fixture_fetch)
        replay = import_active_base(
            conn, collected=collected, scope=SINGLE_FIXTURE_SCOPE
        )
        assert replay.season_id == context.season_id
        assert conn.execute(
            """SELECT count(*) FROM source.fixture_schedule_observations
               WHERE provider_id=%s AND fixture_id=%s""",
            (context.provider_id, fixture_id),
        ).fetchone()[0] == 1
        conn.commit()

        conflicting_kickoff = "2027-06-01T18:30:00+00:00"
        with pytest.raises(
            FixtureScheduleObservationConflict,
            match="conflicting fixture schedule observation",
        ):
            import_active_base(
                conn,
                collected=_with_fixture_kickoff(collected, conflicting_kickoff),
                scope=SINGLE_FIXTURE_SCOPE,
            )
        assert conn.execute(
            "SELECT kickoff_at FROM football.fixtures WHERE id=%s", (fixture_id,)
        ).fetchone()[0] == expected_kickoff
        assert conn.execute(
            """SELECT observed_kickoff_at FROM source.fixture_schedule_observations
               WHERE provider_id=%s AND fixture_id=%s""",
            (context.provider_id, fixture_id),
        ).fetchall() == [(expected_kickoff,)]

        monkeypatch.setattr(active_season, "_persist_fetch", original_persist_fetch)
        rollback_received_at = received_at + timedelta(minutes=1)
        rollback_collected = _with_fixture_kickoff(
            _single_fixture_collected(rollback_received_at),
            "2027-06-02T19:15:00+00:00",
        )
        rollback_kickoff = parse_datetime(
            rollback_collected[3].response.data["response"][0]["fixture"]["date"]
        )

        def fail_after_observation(*args: object, **kwargs: object) -> None:
            assert conn.execute(
                """SELECT observation.observed_kickoff_at,
                          observation.observed_at,
                          provider_fetch.normalized_at
                   FROM source.fixture_schedule_observations observation
                   JOIN source.provider_fetches provider_fetch
                     ON provider_fetch.id=observation.source_fetch_id
                   WHERE observation.provider_id=%s
                     AND observation.fixture_id=%s
                     AND observation.observed_at=%s""",
                (context.provider_id, fixture_id, rollback_received_at),
            ).fetchone() == (rollback_kickoff, rollback_received_at, None)
            raise RuntimeError("forced failure after calendar observation")

        monkeypatch.setattr(active_season, "_normalize_standings", fail_after_observation)
        with pytest.raises(RuntimeError, match="forced failure after calendar observation"):
            import_active_base(
                conn,
                collected=rollback_collected,
                scope=SINGLE_FIXTURE_SCOPE,
            )
        assert conn.execute(
            "SELECT kickoff_at FROM football.fixtures WHERE id=%s", (fixture_id,)
        ).fetchone()[0] == expected_kickoff
        assert conn.execute(
            """SELECT count(*) FROM source.fixture_schedule_observations
               WHERE provider_id=%s AND fixture_id=%s""",
            (context.provider_id, fixture_id),
        ).fetchone()[0] == 1
        assert conn.execute(
            """SELECT count(*) FROM source.provider_fetches
               WHERE provider_id=%s AND endpoint='/fixtures'
                 AND response_received_at=%s""",
            (context.provider_id, rollback_received_at),
        ).fetchone()[0] == 0


def test_real_epl_2026_active_base_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replay retained provider bytes only; never call API-Football or Supabase."""
    assert TEST_DB_URL is not None
    first_received_at = datetime.now(UTC)
    initial_bulk_calls: list[int] = []
    original_bulk_insert = active_season._bulk_insert_initial_fixtures

    def observe_initial_bulk_insert(*args: object, **kwargs: object) -> dict[int, int]:
        records = kwargs["records"]
        assert isinstance(records, tuple)
        initial_bulk_calls.append(len(records))
        return original_bulk_insert(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(active_season, "_bulk_insert_initial_fixtures", observe_initial_bulk_insert)
    with psycopg.connect(TEST_DB_URL) as conn:
        first = import_active_base(conn, collected=_collected(first_received_at), scope=SCOPE)
        report = verify_active_season(conn, scope=SCOPE)

        assert initial_bulk_calls == [380]
        assert first.season_id == report.season_id
        assert (report.team_count, report.fixture_count, report.fixture_mapping_count) == (20, 380, 380)
        assert report.standing_row_count == 20
        assert report.is_complete is True
        expected_known_fixture_venues = sum(
            item["fixture"].get("venue", {}).get("id") not in (None, 0)
            for item in _collected(first_received_at)[3].response.data["response"]
        )
        assert conn.execute(
            "SELECT lifecycle_state::text, count(*) FROM football.fixtures WHERE season_id=%s GROUP BY lifecycle_state ORDER BY lifecycle_state",
            (first.season_id,),
        ).fetchall() == [("completed", 11), ("scheduled", 369)]
        assert conn.execute(
            """SELECT status.status_code, count(*)
               FROM source.fixture_provider_status status
               JOIN football.fixtures fixture ON fixture.id=status.fixture_id
               WHERE fixture.season_id=%s
               GROUP BY status.status_code ORDER BY status.status_code""",
            (first.season_id,),
        ).fetchall() == [("FT", 11), ("NS", 369)]
        assert conn.execute(
            """SELECT count(*)
               FROM source.fixture_provider_refs ref
               JOIN football.fixtures fixture ON fixture.id=ref.fixture_id
               WHERE ref.provider_id=(SELECT id FROM source.providers WHERE code='api-football')
                 AND fixture.season_id=%s""",
            (first.season_id,),
        ).fetchone()[0] == 380
        assert conn.execute(
            """SELECT count(*)
               FROM source.fixture_provider_status status
               JOIN football.fixtures fixture ON fixture.id=status.fixture_id
               WHERE status.provider_id=(SELECT id FROM source.providers WHERE code='api-football')
                 AND fixture.season_id=%s""",
            (first.season_id,),
        ).fetchone()[0] == 380
        assert conn.execute(
            """SELECT count(*)
               FROM football.fixtures fixture
               JOIN source.venue_provider_refs venue_ref
                 ON venue_ref.venue_id=fixture.venue_id
                AND venue_ref.provider_id=(SELECT id FROM source.providers WHERE code='api-football')
               WHERE fixture.season_id=%s
                 AND venue_ref.last_seen_at >= %s""",
            (first.season_id, first_received_at),
        ).fetchone()[0] == expected_known_fixture_venues
        conn.commit()

        second = import_active_base(
            conn,
            collected=_collected(first_received_at + timedelta(minutes=1)),
            scope=SCOPE,
        )
        replay = verify_active_season(conn, scope=SCOPE)

        assert second.season_id == first.season_id == replay.season_id
        assert (replay.team_count, replay.fixture_count, replay.fixture_mapping_count) == (20, 380, 380)
        assert replay.standing_row_count == 20
        assert conn.execute(
            "SELECT count(*) FROM source.fixture_provider_status status JOIN football.fixtures fixture ON fixture.id=status.fixture_id WHERE fixture.season_id=%s",
            (first.season_id,),
        ).fetchone()[0] == 380

        finalized_fixture_id = conn.execute(
            """WITH target AS (
                    SELECT id FROM football.fixtures
                    WHERE season_id=%s AND lifecycle_state='completed'
                    ORDER BY id LIMIT 1
                )
                UPDATE football.fixtures fixture
                SET result_finalized_at=fixture.result_available_at
                FROM target WHERE fixture.id=target.id
                RETURNING fixture.id""",
            (first.season_id,),
        ).fetchone()[0]
        identical_replay = import_active_base(
            conn, collected=_collected(first_received_at), scope=SCOPE
        )
        assert identical_replay.season_id == first.season_id
        assert conn.execute(
            "SELECT result_finalized_at FROM football.fixtures WHERE id=%s", (finalized_fixture_id,)
        ).fetchone()[0] is not None

        raw_by_endpoint = {
            endpoint: SAMPLES.joinpath(filename).read_bytes()
            for endpoint, filename in {
                "/leagues": "01-leagues-epl-2026.raw.json",
                "/teams": "02-teams-epl-2026.raw.json",
                "/standings": "04-standings-epl-2026.raw.json",
                "/fixtures": "03-fixtures-epl-2026.raw.json",
            }.items()
        }
        persisted = conn.execute(
            """SELECT provider_fetch.endpoint,provider_fetch.content_sha256,raw.inline_body
               FROM source.provider_fetches provider_fetch
               JOIN source.provider_raw_payloads raw ON raw.fetch_id=provider_fetch.id
               WHERE provider_fetch.subject_season_id=%s ORDER BY provider_fetch.id""",
            (first.season_id,),
        ).fetchall()
        assert len(persisted) == 12
        for endpoint, digest, body in persisted:
            expected_raw = raw_by_endpoint[endpoint]
            assert bytes(body) == expected_raw
            assert bytes(digest) == hashlib.sha256(expected_raw).digest()
        conn.commit()

        postponed_collected = list(_collected(first_received_at + timedelta(minutes=2)))
        postponed_payload = copy.deepcopy(postponed_collected[3].response.data)
        postponed_fixture = next(
            item for item in postponed_payload["response"]
            if item["fixture"]["status"]["short"] == "NS"
        )
        postponed_external_id = postponed_fixture["fixture"]["id"]
        postponed_fixture["fixture"].update({"status": {"short": "PST"}, "date": None, "timezone": None})
        postponed_collected[3] = CollectedBaseResponse(
            request=postponed_collected[3].request,
            response=_response(postponed_payload),
            request_started_at=first_received_at + timedelta(minutes=2, seconds=-1),
            response_received_at=first_received_at + timedelta(minutes=2),
        )
        import_active_base(conn, collected=tuple(postponed_collected), scope=SCOPE)
        conn.commit()

        postponed = conn.execute(
            """SELECT fixture.id,fixture.lifecycle_state::text,fixture.kickoff_at,status.status_code
               FROM football.fixtures fixture
               JOIN source.fixture_provider_refs ref ON ref.fixture_id=fixture.id
               JOIN source.fixture_provider_status status
                 ON status.fixture_id=fixture.id AND status.provider_id=ref.provider_id
               WHERE ref.provider_id=(SELECT id FROM source.providers WHERE code='api-football')
                 AND ref.external_id=%s""",
            (str(postponed_external_id),),
        ).fetchone()
        assert postponed is not None
        postponed_fixture_id, lifecycle, kickoff_at, status_code = postponed
        assert (lifecycle, kickoff_at, status_code) == ("postponed", None, "PST")
        assert conn.execute(
            """SELECT observation.observed_kickoff_at, observation.observed_at,
                      provider_fetch.endpoint, provider_fetch.normalized_at IS NOT NULL
               FROM source.fixture_schedule_observations observation
               JOIN source.provider_fetches provider_fetch
                 ON provider_fetch.id=observation.source_fetch_id
               WHERE observation.fixture_id=%s AND observation.observed_at=%s""",
            (postponed_fixture_id, first_received_at + timedelta(minutes=2)),
        ).fetchone() == (
            None,
            first_received_at + timedelta(minutes=2),
            "/fixtures",
            True,
        )

        rescheduled_collected = list(_collected(first_received_at + timedelta(minutes=3)))
        rescheduled_payload = copy.deepcopy(rescheduled_collected[3].response.data)
        rescheduled_fixture = next(
            item for item in rescheduled_payload["response"]
            if item["fixture"]["id"] == postponed_external_id
        )
        rescheduled_kickoff_raw = "2027-05-31T19:45:00+00:00"
        rescheduled_fixture["fixture"]["date"] = rescheduled_kickoff_raw
        rescheduled_collected[3] = CollectedBaseResponse(
            request=rescheduled_collected[3].request,
            response=_response(rescheduled_payload),
            request_started_at=first_received_at + timedelta(minutes=3, seconds=-1),
            response_received_at=first_received_at + timedelta(minutes=3),
        )
        import_active_base(conn, collected=tuple(rescheduled_collected), scope=SCOPE)
        conn.commit()

        rescheduled = conn.execute(
            """SELECT fixture.id,fixture.lifecycle_state::text,fixture.kickoff_at,status.status_code
               FROM football.fixtures fixture
               JOIN source.fixture_provider_refs ref ON ref.fixture_id=fixture.id
               JOIN source.fixture_provider_status status
                 ON status.fixture_id=fixture.id AND status.provider_id=ref.provider_id
               WHERE ref.provider_id=(SELECT id FROM source.providers WHERE code='api-football')
                 AND ref.external_id=%s""",
            (str(postponed_external_id),),
        ).fetchone()
        assert rescheduled == (
            postponed_fixture_id,
            "scheduled",
            parse_datetime(rescheduled_kickoff_raw),
            "NS",
        )
        assert conn.execute(
            """SELECT observation.observed_kickoff_at, observation.observed_at,
                      provider_fetch.endpoint, provider_fetch.normalized_at IS NOT NULL
               FROM source.fixture_schedule_observations observation
               JOIN source.provider_fetches provider_fetch
                 ON provider_fetch.id=observation.source_fetch_id
               WHERE observation.fixture_id=%s AND observation.observed_at=%s""",
            (postponed_fixture_id, first_received_at + timedelta(minutes=3)),
        ).fetchone() == (
            parse_datetime(rescheduled_kickoff_raw),
            first_received_at + timedelta(minutes=3),
            "/fixtures",
            True,
        )
