"""Fail-closed, resumable historical fixture-statistics backfill.

This module deliberately contains no scheduler.  A caller starts one bounded
batch; every successful provider body is persisted before it is interpreted.
It is safe to rerun: terminal raw fetches are classified locally before a new
provider request is considered.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg
from psycopg import Connection, sql
from psycopg.types.json import Jsonb

from app.api_football import APIFootballClient, APIFootballResponse
from app.api_football.client import safe_rate_limit_headers
from app.api_football.errors import APIFootballAPIError, APIFootballHTTPError
from app.importer.canary import INTEGER_STATISTICS, STATISTIC_COLUMNS, request_params_sha256

PROVIDER_CODE = "api-football"
ENDPOINT = "/fixtures/statistics"
PURPOSE = "bootstrap"
MAPPING_VERSION = "api-football-v1"
RAW_RETENTION_DAYS = 30
ANOMALY_RETENTION_DAYS = 90
# Pro quota permits 300 requests/minute.  Keep sequential work well below it
# (about 55 requests/minute) while retaining headroom for retries and operator
# checks; no concurrency is used by this importer.
PACE_SECONDS = 1.1
DEFAULT_RUN_ATTEMPT_CAP = 90
QUOTA_RESERVE = 5
DATASET_ATTEMPT_CAP = 385
GLOBAL_RETRY_CAP = 5
MAX_ATTEMPTS_PER_FIXTURE = 2
RETRYABLE_HTTP_STATUSES = frozenset({408, 499, 500, 502, 503, 504})

Sleep = Callable[[float], Awaitable[None]]
Clock = Callable[[], datetime]


class StatisticsBackfillError(RuntimeError):
    """A controlled-stop condition: callers must not continue the campaign."""


class StatisticsContractError(StatisticsBackfillError):
    """The provider response cannot safely be normalized."""


@dataclass(frozen=True)
class StatisticsImportScope:
    """Immutable provider/canonical scope for one controlled season run."""

    league_external_id: int
    season_start_year: int
    expected_fixture_count: int
    canary_fixture_external_id: int | None = None

    def __post_init__(self) -> None:
        if self.league_external_id <= 0:
            raise ValueError("league_external_id must be positive")
        if self.season_start_year < 1900:
            raise ValueError("season_start_year must be a four-digit year")
        if self.expected_fixture_count <= 0:
            raise ValueError("expected_fixture_count must be positive")
        if self.canary_fixture_external_id is not None and self.canary_fixture_external_id <= 0:
            raise ValueError("canary_fixture_external_id must be positive when provided")

    @property
    def lock_key(self) -> str:
        return (
            "api-football:fixture-statistics:"
            f"{self.league_external_id}:{self.season_start_year}:v1"
        )


# Backwards-compatible default only. New seasonal invocations pass their scope
# explicitly, so a run cannot accidentally acquire the EPL 2024 lock/context.
EPL_2024_SCOPE = StatisticsImportScope(
    league_external_id=39,
    season_start_year=2024,
    expected_fixture_count=380,
    canary_fixture_external_id=1208021,
)
LEAGUE_EXTERNAL_ID = EPL_2024_SCOPE.league_external_id
SEASON_START_YEAR = EPL_2024_SCOPE.season_start_year
EXPECTED_FIXTURE_COUNT = EPL_2024_SCOPE.expected_fixture_count
CANARY_FIXTURE_EXTERNAL_ID = EPL_2024_SCOPE.canary_fixture_external_id


@dataclass(frozen=True)
class FixtureTarget:
    fixture_id: int
    external_id: int
    home_team_id: int
    away_team_id: int
    kickoff_at: datetime
    result_available_at: datetime


@dataclass(frozen=True)
class StoredRaw:
    fetch_id: int
    response: APIFootballResponse
    request_started_at: datetime
    response_received_at: datetime
    provider_results: int | None


@dataclass(frozen=True)
class StatisticsBatchReport:
    physical_attempts: int
    reused_raw: int
    complete: int
    empty: int
    partial: int
    failed: int
    statistics_rows_created: int
    retries: int
    errors: int
    stop_reason: str | None
    quota: Mapping[str, str]
    verification: Mapping[str, Any]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _database_url() -> str:
    value = os.environ.get("SUPABASE_DB_URL")
    if not value:
        raise StatisticsBackfillError("SUPABASE_DB_URL is required")
    return value


def _params(external_fixture_id: int) -> dict[str, int]:
    return {"fixture": external_fixture_id}


def _decimal(
    value: Any,
    *,
    percentage: bool,
    scale: int,
    maximum: Decimal,
    minimum: Decimal = Decimal("0"),
) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise StatisticsContractError("boolean statistic is invalid")
    text = str(value).strip()
    if percentage:
        if text.endswith("%"):
            text = text[:-1].strip()
    elif text.endswith("%"):
        raise StatisticsContractError("non-percentage statistic has percent suffix")
    try:
        result = Decimal(text)
    except (InvalidOperation, ValueError) as error:
        raise StatisticsContractError("invalid decimal statistic") from error
    if not result.is_finite() or result < minimum or result > maximum:
        raise StatisticsContractError("decimal statistic outside permitted range")
    exponent = result.as_tuple().exponent
    if exponent < -scale:
        raise StatisticsContractError("decimal statistic exceeds database precision")
    return result


def _integer(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise StatisticsContractError("boolean statistic is invalid")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and value.strip().isdigit():
        result = int(value.strip())
    else:
        raise StatisticsContractError("integer statistic has invalid type")
    if result < 0:
        raise StatisticsContractError("negative statistic")
    return result


def map_statistics_block(block: Mapping[str, Any]) -> dict[str, Any]:
    """Strictly map one team block; unknown labels remain lossless JSON."""
    team = block.get("team")
    stats = block.get("statistics")
    if not isinstance(team, Mapping) or not isinstance(team.get("id"), int) or isinstance(team["id"], bool):
        raise StatisticsContractError("team.id must be an integer")
    if not isinstance(stats, list):
        raise StatisticsContractError("statistics must be an array")
    result: dict[str, Any] = {column: None for column in STATISTIC_COLUMNS.values()}
    extras: dict[str, Any] = {}
    labels: set[str] = set()
    for item in stats:
        if not isinstance(item, Mapping) or not isinstance(item.get("type"), str):
            raise StatisticsContractError("statistic type must be a string")
        label = item["type"]
        if not label or label in labels:
            raise StatisticsContractError("duplicate or blank statistic label")
        labels.add(label)
        column = STATISTIC_COLUMNS.get(label)
        value = item.get("value")
        if column is None:
            extras[label] = value
        elif column in INTEGER_STATISTICS:
            result[column] = _integer(value)
        elif column in {"possession_pct", "pass_accuracy_pct"}:
            result[column] = _decimal(value, percentage=True, scale=2, maximum=Decimal("100"))
        else:
            result[column] = _decimal(
                value,
                percentage=False,
                scale=3,
                minimum=(Decimal("-99999.999") if column == "goals_prevented" else Decimal("0")),
                maximum=Decimal("99999.999"),
            )
    result["external_team_id"] = team["id"]
    result["extra_metrics"] = extras
    if result["passes_accurate"] is not None and result["total_passes"] is not None and result["passes_accurate"] > result["total_passes"]:
        raise StatisticsContractError("passes accurate exceeds total passes")
    return result


def classify_response(payload: Mapping[str, Any], target: FixtureTarget) -> tuple[str, list[dict[str, Any]]]:
    """Return complete/empty/partial or raise; no half-pair may be written."""
    if payload.get("parameters") != {"fixture": str(target.external_id)}:
        raise StatisticsContractError("response parameters mismatch")
    if payload.get("errors") not in (None, {}, []):
        raise StatisticsContractError("provider response contains errors")
    response = payload.get("response")
    results = payload.get("results")
    paging = payload.get("paging")
    if (
        not isinstance(response, list)
        or type(results) is not int
        or results < 0
        or results != len(response)
    ):
        raise StatisticsContractError("results/response mismatch")
    current = paging.get("current") if isinstance(paging, Mapping) else None
    total = paging.get("total") if isinstance(paging, Mapping) else None
    if type(current) is not int or type(total) is not int or current != 1 or total != 1:
        raise StatisticsContractError("statistics paging must be one page")
    if results == 0:
        return "empty", []
    if results > 2:
        raise StatisticsContractError("more than two statistics team blocks")
    mapped = [map_statistics_block(item) for item in response if isinstance(item, Mapping)]
    if len(mapped) != results:
        raise StatisticsContractError("statistics team block is malformed")
    ids = [item["external_team_id"] for item in mapped]
    if len(ids) != len(set(ids)):
        raise StatisticsContractError("duplicate statistics team")
    # Internal IDs are checked by the caller; response IDs alone have no home/away flag.
    return ("complete" if results == 2 else "partial"), mapped


def acquire_context_and_lock(
    conn: Connection[Any], *, scope: StatisticsImportScope = EPL_2024_SCOPE
) -> tuple[int, int]:
    locked = conn.execute(
        "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (scope.lock_key,)
    ).fetchone()[0]
    if not locked:
        raise StatisticsBackfillError("statistics backfill is already running")
    row = conn.execute(
        """SELECT p.id, sr.season_id FROM source.providers p
           JOIN source.season_provider_refs sr ON sr.provider_id=p.id
           WHERE p.code=%s AND sr.league_external_id=%s AND sr.external_season=%s""",
        (PROVIDER_CODE, str(scope.league_external_id), scope.season_start_year),
    ).fetchone()
    if row is None:
        raise StatisticsBackfillError(
            "provider and season mappings are required for "
            f"league={scope.league_external_id} season={scope.season_start_year}"
        )
    return int(row[0]), int(row[1])


def load_targets(
    conn: Connection[Any], *, provider_id: int, season_id: int, scope: StatisticsImportScope
) -> list[FixtureTarget]:
    rows = conn.execute(
        """SELECT f.id, r.external_id, f.home_team_id, f.away_team_id, f.kickoff_at, f.result_available_at
           FROM football.fixtures f JOIN source.fixture_provider_refs r ON r.fixture_id=f.id AND r.provider_id=%s
           WHERE f.season_id=%s AND f.lifecycle_state='completed' AND f.result_finalized_at IS NOT NULL
           ORDER BY r.external_id::bigint""",
        (provider_id, season_id),
    ).fetchall()
    if len(rows) != scope.expected_fixture_count:
        raise StatisticsBackfillError(
            "preflight requires exactly "
            f"{scope.expected_fixture_count} completed provider-mapped fixtures "
            f"for league={scope.league_external_id} season={scope.season_start_year}"
        )
    targets = [FixtureTarget(int(a), int(b), int(c), int(d), e, f) for a, b, c, d, e, f in rows]
    if any(t.result_available_at is None for t in targets):
        raise StatisticsBackfillError("completed fixture lacks result_available_at")
    return targets


def _existing_pair_state(conn: Connection[Any], *, provider_id: int, target: FixtureTarget) -> str:
    """Return done/incomplete. Existing malformed state fails before API access."""
    rows = conn.execute(
        """SELECT s.team_id, s.finalized_at, s.mapping_version, s.last_source_fetch_id,
                  pf.endpoint, pf.outcome, pf.subject_fixture_id, pf.normalized_at,
                  pf.provider_results, pf.request_params_sha256
           FROM football.fixture_team_statistics s LEFT JOIN source.provider_fetches pf ON pf.id=s.last_source_fetch_id
           WHERE s.fixture_id=%s ORDER BY s.team_id""", (target.fixture_id,)
    ).fetchall()
    if not rows:
        return "incomplete"
    expected = {target.home_team_id, target.away_team_id}
    if len(rows) != 2 or {row[0] for row in rows} != expected:
        raise StatisticsBackfillError("existing statistics are not an exact fixture pair")
    fetch_ids = {row[3] for row in rows}
    if len(fetch_ids) != 1 or any(
        row[1] is None or row[2] != MAPPING_VERSION or row[3] is None
        or row[4] != ENDPOINT or row[5] != "success" or row[6] != target.fixture_id
        or row[7] is None or row[8] != 2 or row[9] is None
        or bytes(row[9]) != request_params_sha256(_params(target.external_id))
        for row in rows
    ):
        raise StatisticsBackfillError("existing statistics pair has invalid provenance")
    return "done"


def _find_reusable_raw(conn: Connection[Any], *, provider_id: int, target: FixtureTarget) -> StoredRaw | None:
    row = conn.execute(
        """SELECT f.id,f.request_started_at,f.response_received_at,f.http_status,f.content_sha256,
                  f.provider_results,r.inline_body,f.request_params_sha256
           FROM source.provider_fetches f JOIN source.provider_raw_payloads r ON r.fetch_id=f.id
           WHERE f.provider_id=%s AND f.endpoint=%s AND f.subject_fixture_id=%s AND f.outcome='success'
             AND f.normalized_at IS NULL AND r.purged_at IS NULL AND r.inline_body IS NOT NULL
           ORDER BY f.response_received_at DESC LIMIT 1""", (provider_id, ENDPOINT, target.fixture_id)
    ).fetchone()
    if row is None:
        return None
    fetch_id, started, received, status, digest, results, body, params_digest = row
    raw = bytes(body)
    if digest is None or hashlib.sha256(raw).digest() != bytes(digest):
        raise StatisticsBackfillError("stored raw payload hash mismatch")
    if params_digest is None or bytes(params_digest) != request_params_sha256(_params(target.external_id)):
        raise StatisticsBackfillError("stored raw payload request provenance mismatch")
    try:
        data = json.loads(raw)
    except ValueError as error:
        raise StatisticsBackfillError("stored raw payload is invalid JSON") from error
    if not isinstance(data, dict):
        raise StatisticsBackfillError("stored raw payload top level is invalid")
    return StoredRaw(int(fetch_id), APIFootballResponse(data, raw, int(status), {}), started, received, results)


def _persist_fetch(conn: Connection[Any], *, provider_id: int, target: FixtureTarget, response: APIFootballResponse, started: datetime, received: datetime, retention: str = "standard") -> StoredRaw:
    candidate_results = response.data.get("results")
    results = candidate_results if type(candidate_results) is int and candidate_results >= 0 else None
    paging = response.data.get("paging")
    candidate_current = paging.get("current") if isinstance(paging, Mapping) else None
    candidate_total = paging.get("total") if isinstance(paging, Mapping) else None
    current = candidate_current if type(candidate_current) is int and candidate_current >= 1 else None
    total = candidate_total if type(candidate_total) is int and candidate_total >= 1 else None
    with conn.transaction():
        fetch_id = conn.execute(
            """INSERT INTO source.provider_fetches(provider_id,endpoint,request_params,request_params_sha256,purpose,
                 request_started_at,response_received_at,http_status,outcome,provider_results,paging_current,paging_total,
                 content_sha256,subject_fixture_id,subject_season_id)
               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'success',%s,%s,%s,%s,%s,(SELECT season_id FROM football.fixtures WHERE id=%s)) RETURNING id""",
            (provider_id, ENDPOINT, Jsonb(_params(target.external_id)), request_params_sha256(_params(target.external_id)), PURPOSE,
             started, received, response.status_code, results, current, total,
             hashlib.sha256(response.raw_body).digest(), target.fixture_id, target.fixture_id),
        ).fetchone()[0]
        conn.execute("""INSERT INTO source.provider_raw_payloads(fetch_id,inline_body,content_type,byte_count,retention_class,expires_at)
                      VALUES(%s,%s,'application/json',%s,%s,%s)""",
                     (fetch_id,response.raw_body,len(response.raw_body),retention,received + timedelta(days=ANOMALY_RETENTION_DAYS if retention == "anomaly" else RAW_RETENTION_DAYS)))
    return StoredRaw(int(fetch_id), response, started, received, results)


def _record_failure(conn: Connection[Any], *, provider_id: int, target: FixtureTarget, started: datetime, received: datetime, status: int | None, outcome: str, error_class: str) -> None:
    conn.execute(
        """INSERT INTO source.provider_fetches(provider_id,endpoint,request_params,request_params_sha256,purpose,request_started_at,response_received_at,http_status,outcome,sanitized_error_class,sanitized_error_text,subject_fixture_id,subject_season_id)
           VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'controlled statistics request failed',%s,(SELECT season_id FROM football.fixtures WHERE id=%s))""",
        (provider_id,ENDPOINT,Jsonb(_params(target.external_id)),request_params_sha256(_params(target.external_id)),PURPOSE,started,received,status,outcome,error_class,target.fixture_id,target.fixture_id),
    )


def _persist_api_error_response(
    conn: Connection[Any],
    *,
    provider_id: int,
    target: FixtureTarget,
    started: datetime,
    received: datetime,
    error: APIFootballAPIError,
) -> int | None:
    """Persist a received 2xx error body as anomaly raw without exposing it."""
    if error.raw_body is None:
        _record_failure(
            conn,
            provider_id=provider_id,
            target=target,
            started=started,
            received=received,
            status=error.status_code or 200,
            outcome="provider_error",
            error_class=type(error).__name__,
        )
        return None
    raw = bytes(error.raw_body)
    results: int | None = None
    paging_current: int | None = None
    paging_total: int | None = None
    try:
        payload = json.loads(raw)
    except ValueError:
        payload = None
    if isinstance(payload, Mapping):
        candidate_results = payload.get("results")
        if type(candidate_results) is int and candidate_results >= 0:
            results = candidate_results
        paging = payload.get("paging")
        if isinstance(paging, Mapping):
            current = paging.get("current")
            total = paging.get("total")
            if type(current) is int and current >= 1:
                paging_current = current
            if type(total) is int and total >= 1:
                paging_total = total
    with conn.transaction():
        fetch_id = conn.execute(
            """INSERT INTO source.provider_fetches(
                 provider_id,endpoint,request_params,request_params_sha256,purpose,
                 request_started_at,response_received_at,http_status,outcome,provider_results,
                 paging_current,paging_total,content_sha256,sanitized_error_class,
                 sanitized_error_text,subject_fixture_id,subject_season_id)
               VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'provider_error',%s,%s,%s,%s,%s,
                      'provider returned an invalid or error response',%s,
                      (SELECT season_id FROM football.fixtures WHERE id=%s))
               RETURNING id""",
            (
                provider_id,
                ENDPOINT,
                Jsonb(_params(target.external_id)),
                request_params_sha256(_params(target.external_id)),
                PURPOSE,
                started,
                received,
                error.status_code or 200,
                results,
                paging_current,
                paging_total,
                hashlib.sha256(raw).digest(),
                type(error).__name__,
                target.fixture_id,
                target.fixture_id,
            ),
        ).fetchone()[0]
        conn.execute(
            """INSERT INTO source.provider_raw_payloads(
                 fetch_id,inline_body,content_type,byte_count,retention_class,expires_at)
               VALUES(%s,%s,'application/json',%s,'anomaly',%s)""",
            (fetch_id, raw, len(raw), received + timedelta(days=ANOMALY_RETENTION_DAYS)),
        )
    return int(fetch_id)


def _mark_contract_error(conn: Connection[Any], fetch_id: int) -> None:
    with conn.transaction():
        conn.execute("""UPDATE source.provider_fetches SET outcome='provider_error', sanitized_error_class='StatisticsContractError',
                      sanitized_error_text='statistics response failed controlled validation' WHERE id=%s AND normalized_at IS NULL""", (fetch_id,))
        conn.execute("UPDATE source.provider_raw_payloads SET retention_class='anomaly',expires_at=clock_timestamp()+interval '90 days' WHERE fetch_id=%s", (fetch_id,))


def normalize_raw(conn: Connection[Any], *, provider_id: int, target: FixtureTarget, raw: StoredRaw, clock: Clock = _utcnow) -> tuple[str, int]:
    """Classify and normalize one persisted fetch. Complete pairs are atomic."""
    try:
        state, mapped = classify_response(raw.response.data, target)
        resolved: list[dict[str, Any]] = []
        for item in mapped:
            row = conn.execute("SELECT team_id FROM source.team_provider_refs WHERE provider_id=%s AND external_id=%s", (provider_id, str(item["external_team_id"]))).fetchone()
            if row is None:
                raise StatisticsContractError("statistics team provider mapping missing")
            item["team_id"] = int(row[0])
            resolved.append(item)
        ids = {item["team_id"] for item in resolved}
        expected = {target.home_team_id, target.away_team_id}
        if state == "complete" and ids != expected:
            raise StatisticsContractError("statistics teams are not fixture participants")
        if state == "partial" and not ids.issubset(expected):
            raise StatisticsContractError("partial statistics team is not fixture participant")
        available = max(target.result_available_at, target.kickoff_at + timedelta(hours=3))
        columns = list(STATISTIC_COLUMNS.values())
        with conn.transaction():
            if state == "complete":
                existing = conn.execute("SELECT count(*) FROM football.fixture_team_statistics WHERE fixture_id=%s", (target.fixture_id,)).fetchone()[0]
                if existing:
                    raise StatisticsBackfillError("refusing to update existing statistics")
                for item in resolved:
                    conn.execute(sql.SQL("""INSERT INTO football.fixture_team_statistics(fixture_id,team_id,{},extra_metrics,mapping_version,observed_at,available_at,availability_basis,last_source_fetch_id,finalized_at)
                      VALUES(%s,%s,{},%s,%s,%s,%s,'reconstructed_conservative',%s,%s)""").format(
                        sql.SQL(", ").join(map(sql.Identifier, columns)), sql.SQL(", ").join(sql.Placeholder() for _ in columns)),
                        (target.fixture_id,item["team_id"],*(item[c] for c in columns),Jsonb(item["extra_metrics"]),MAPPING_VERSION,available,available,raw.fetch_id,raw.response_received_at))
            conn.execute("UPDATE source.provider_fetches SET normalized_at=%s WHERE id=%s AND normalized_at IS NULL", (clock(), raw.fetch_id))
        return state, 2 if state == "complete" else 0
    except StatisticsContractError:
        _mark_contract_error(conn, raw.fetch_id)
        raise



def _attempt_budget(conn: Connection[Any], *, provider_id: int, season_id: int) -> tuple[int, int]:
    """All durable attempts and mathematical retries (not merely failures)."""
    total = conn.execute(
        "SELECT count(*) FROM source.provider_fetches WHERE provider_id=%s AND endpoint=%s AND subject_season_id=%s",
        (provider_id, ENDPOINT, season_id),
    ).fetchone()[0]
    retries = conn.execute(
        """SELECT coalesce(sum(greatest(n - 1, 0)), 0) FROM (
              SELECT count(*) AS n FROM source.provider_fetches
              WHERE provider_id=%s AND endpoint=%s AND subject_season_id=%s
              GROUP BY subject_fixture_id
            ) attempts""".replace("\n+", "\n"),
        (provider_id, ENDPOINT, season_id),
    ).fetchone()[0]
    return int(total), int(retries)


def _terminal_fetch_state(conn: Connection[Any], *, provider_id: int, target: FixtureTarget) -> str | None:
    """Terminal empty/partial survives raw retention via normalized fetch metadata."""
    rows = conn.execute(
        """SELECT outcome, normalized_at, provider_results, request_params_sha256,
                  http_status, sanitized_error_class
           FROM source.provider_fetches WHERE provider_id=%s AND endpoint=%s AND subject_fixture_id=%s
           ORDER BY response_received_at DESC NULLS LAST""".replace("\n+", "\n"),
        (provider_id, ENDPOINT, target.fixture_id),
    ).fetchall()
    for outcome, normalized_at, results, digest, http_status, error_class in rows:
        if outcome == "provider_error" and normalized_at is None:
            raise StatisticsBackfillError("previous statistics contract anomaly requires manual review")
        if outcome == "http_error" and (
            http_status == 429 or http_status not in RETRYABLE_HTTP_STATUSES
        ):
            raise StatisticsBackfillError(
                "previous non-retryable statistics HTTP failure requires manual review"
            )
        if normalized_at is not None:
            if digest is None or bytes(digest) != request_params_sha256(_params(target.external_id)):
                raise StatisticsBackfillError("normalized statistics fetch has invalid request provenance")
            if results == 0:
                return "empty"
            if results == 1:
                return "partial"
            if results == 2:
                raise StatisticsBackfillError("complete normalized fetch lacks valid finalized pair")
            raise StatisticsBackfillError("normalized statistics fetch has invalid result count")
    if len(rows) >= MAX_ATTEMPTS_PER_FIXTURE:
        raise StatisticsBackfillError("statistics fixture lifetime attempt budget is exhausted")
    return None


def _unfinished_purged_raw(conn: Connection[Any], *, provider_id: int, target: FixtureTarget) -> bool:
    return bool(conn.execute(
        """SELECT 1 FROM source.provider_fetches f JOIN source.provider_raw_payloads r ON r.fetch_id=f.id
           WHERE f.provider_id=%s AND f.endpoint=%s AND f.subject_fixture_id=%s
             AND f.outcome='success' AND f.normalized_at IS NULL AND r.purged_at IS NOT NULL""".replace("\n+", "\n"),
        (provider_id, ENDPOINT, target.fixture_id),
    ).fetchone())


def approve_contract_replays(
    conn: Connection[Any],
    *,
    provider_id: int,
    season_id: int,
    fetch_ids: frozenset[int],
) -> int:
    """Reopen only explicitly approved, hash-verified contract anomalies for raw replay."""
    if not fetch_ids:
        return 0
    reopened = 0
    with conn.transaction():
        for fetch_id in sorted(fetch_ids):
            row = conn.execute(
                """SELECT f.outcome::text,f.normalized_at,f.sanitized_error_class,
                          f.subject_fixture_id,f.content_sha256,r.inline_body,r.purged_at,
                          x.season_id,f.http_status
                   FROM source.provider_fetches f
                   JOIN source.provider_raw_payloads r ON r.fetch_id=f.id
                   JOIN football.fixtures x ON x.id=f.subject_fixture_id
                   WHERE f.id=%s AND f.provider_id=%s AND f.endpoint=%s
                   FOR UPDATE OF f,r""",
                (fetch_id, provider_id, ENDPOINT),
            ).fetchone()
            if row is None:
                raise StatisticsBackfillError("approved contract replay fetch is missing")
            outcome, normalized_at, error_class, fixture_id, digest, body, purged_at, actual_season_id, status = row
            if (
                outcome != "provider_error"
                or normalized_at is not None
                or error_class != "StatisticsContractError"
                or actual_season_id != season_id
                or status != 200
                or purged_at is not None
                or body is None
                or digest is None
            ):
                raise StatisticsBackfillError("approved contract replay fetch is not eligible")
            raw = bytes(body)
            if hashlib.sha256(raw).digest() != bytes(digest):
                raise StatisticsBackfillError("approved contract replay raw hash mismatch")
            conn.execute(
                """UPDATE source.provider_fetches
                   SET outcome='success',
                       sanitized_error_class='ApprovedStatisticsContractReplay',
                       sanitized_error_text='explicitly approved replay from retained anomaly raw'
                   WHERE id=%s""",
                (fetch_id,),
            )
            reopened += 1
    return reopened


def preflight_statistics_backfill(
    conn: Connection[Any],
    *,
    provider_id: int,
    season_id: int,
    scope: StatisticsImportScope = EPL_2024_SCOPE,
) -> tuple[list[FixtureTarget], dict[str, int]]:
    """Scan every fixture before network and return only targets requiring a request."""
    targets = load_targets(conn, provider_id=provider_id, season_id=season_id, scope=scope)
    statistics_by_fixture: dict[int, list[tuple[Any, ...]]] = {}
    for row in conn.execute(
        """SELECT s.fixture_id,s.team_id,s.finalized_at,s.mapping_version,s.last_source_fetch_id,
                  pf.endpoint,pf.outcome::text,pf.subject_fixture_id,pf.normalized_at,
                  pf.provider_results,pf.request_params_sha256
           FROM football.fixture_team_statistics s
           JOIN football.fixtures f ON f.id=s.fixture_id
           LEFT JOIN source.provider_fetches pf ON pf.id=s.last_source_fetch_id
           WHERE f.season_id=%s ORDER BY s.fixture_id,s.team_id""",
        (season_id,),
    ).fetchall():
        statistics_by_fixture.setdefault(int(row[0]), []).append(tuple(row[1:]))
    fetches_by_fixture: dict[int, list[tuple[Any, ...]]] = {}
    for row in conn.execute(
        """SELECT pf.subject_fixture_id,pf.outcome::text,pf.normalized_at,
                  pf.provider_results,pf.request_params_sha256,pf.http_status,
                  pf.sanitized_error_class,pf.response_received_at,pf.id,
                  r.purged_at,(r.inline_body IS NOT NULL)
           FROM source.provider_fetches pf
           JOIN football.fixtures f ON f.id=pf.subject_fixture_id
           LEFT JOIN source.provider_raw_payloads r ON r.fetch_id=pf.id
           WHERE pf.provider_id=%s AND pf.endpoint=%s AND f.season_id=%s
           ORDER BY pf.subject_fixture_id,pf.response_received_at DESC NULLS LAST,pf.id DESC""",
        (provider_id, ENDPOINT, season_id),
    ).fetchall():
        fetches_by_fixture.setdefault(int(row[0]), []).append(tuple(row[1:]))
    queue: list[FixtureTarget] = []
    states = {"complete": 0, "empty": 0, "partial": 0, "pending": 0}
    for target in targets:
        statistics = statistics_by_fixture.get(target.fixture_id, [])
        if statistics:
            expected = {target.home_team_id, target.away_team_id}
            fetch_ids = {row[3] for row in statistics}
            if len(statistics) != 2 or {row[0] for row in statistics} != expected or len(fetch_ids) != 1:
                raise StatisticsBackfillError("existing statistics are not an exact fixture pair")
            expected_digest = request_params_sha256(_params(target.external_id))
            if any(
                row[1] is None or row[2] != MAPPING_VERSION or row[3] is None
                or row[4] != ENDPOINT or row[5] != "success" or row[6] != target.fixture_id
                or row[7] is None or row[8] != 2 or row[9] is None
                or bytes(row[9]) != expected_digest
                for row in statistics
            ):
                raise StatisticsBackfillError("existing statistics pair has invalid provenance")
            states["complete"] += 1
            continue
        fetches = fetches_by_fixture.get(target.fixture_id, [])
        normalized = next(
            (row for row in fetches if row[0] == "success" and row[1] is not None),
            None,
        )
        if normalized is not None:
            _, _, results, digest, *_ = normalized
            if digest is None or bytes(digest) != request_params_sha256(_params(target.external_id)):
                raise StatisticsBackfillError("normalized statistics fetch has invalid request provenance")
            if results == 0:
                states["empty"] += 1
            elif results == 1:
                states["partial"] += 1
            elif results == 2:
                raise StatisticsBackfillError("complete normalized fetch lacks valid finalized pair")
            else:
                raise StatisticsBackfillError("normalized statistics fetch has invalid result count")
            continue
        if any(row[0] == "provider_error" for row in fetches):
            raise StatisticsBackfillError("previous statistics contract anomaly requires manual review")
        if any(
            row[0] == "http_error"
            and (row[4] == 429 or row[4] not in RETRYABLE_HTTP_STATUSES)
            for row in fetches
        ):
            raise StatisticsBackfillError(
                "previous non-retryable statistics HTTP failure requires manual review"
            )
        if len(fetches) >= MAX_ATTEMPTS_PER_FIXTURE:
            raise StatisticsBackfillError("statistics fixture lifetime attempt budget is exhausted")
        unfinished_successes = [row for row in fetches if row[0] == "success" and row[1] is None]
        if any(row[8] is not None and not row[9] for row in unfinished_successes):
            raise StatisticsBackfillError(
                "unfinished statistics raw payload was purged; controlled refetch approval required"
            )
        if any(not row[9] for row in unfinished_successes):
            raise StatisticsBackfillError("unfinished statistics fetch has no reusable raw payload")
        queue.append(target)
        states["pending"] += 1
    if sum(states.values()) != scope.expected_fixture_count:
        raise AssertionError("preflight classification is incomplete")
    # A configured canary must remain a strict, already-finalized no-op. New
    # seasons do not inherit the EPL 2024 canary, so canary is optional.
    if scope.canary_fixture_external_id is not None:
        canary = next(
            (t for t in targets if t.external_id == scope.canary_fixture_external_id),
            None,
        )
        if canary is None:
            raise StatisticsBackfillError(
                "configured canary fixture is not present in the import scope"
            )
        if _existing_pair_state(conn, provider_id=provider_id, target=canary) != "done":
            raise StatisticsBackfillError(
                "configured canary statistics pair is not a valid finalized checkpoint"
            )
    return queue, states


def _fingerprint(conn: Connection[Any], table: str) -> str:
    # Stable, content-sensitive fingerprint; table names are fixed internal constants.
    row = conn.execute(sql.SQL("SELECT md5(coalesce(string_agg(row_to_json(t)::text, '' ORDER BY row_to_json(t)::text), '')) FROM (SELECT * FROM {}) t").format(sql.SQL(table))).fetchone()
    return str(row[0])


def _canary_statistics_fingerprint(
    conn: Connection[Any], *, provider_id: int, canary_fixture_external_id: int | None
) -> str | None:
    if canary_fixture_external_id is None:
        return None
    row = conn.execute(
        """SELECT md5(coalesce(string_agg(row_to_json(t)::text, '' ORDER BY row_to_json(t)::text), ''))
           FROM (
             SELECT s.* FROM football.fixture_team_statistics s
             JOIN source.fixture_provider_refs r ON r.fixture_id=s.fixture_id
             WHERE r.provider_id=%s AND r.external_id=%s
           ) t""",
        (provider_id, str(canary_fixture_external_id)),
    ).fetchone()
    return str(row[0])


def remote_verification(
    conn: Connection[Any],
    *,
    provider_id: int,
    season_id: int,
    scope: StatisticsImportScope,
    before: Mapping[str, str],
    canary_before: str | None,
) -> dict[str, Any]:
    rows, nonparticipant = conn.execute(
        """SELECT count(*), count(*) FILTER (
                 WHERE s.team_id <> f.home_team_id AND s.team_id <> f.away_team_id)
           FROM football.fixture_team_statistics s
           JOIN football.fixtures f ON f.id=s.fixture_id
           WHERE f.season_id=%s""",
        (season_id,),
    ).fetchone()
    pair_rows = conn.execute(
        """SELECT count(*) FILTER (
                    WHERE c=2 AND teams_ok AND finalized AND provenance_ok AND same_fetch),
                  count(*) FILTER (
                    WHERE c>0 AND NOT (c=2 AND teams_ok AND finalized AND provenance_ok AND same_fetch))
           FROM (
             SELECT f.id, count(s.*) c,
                    bool_and(s.team_id IN (f.home_team_id,f.away_team_id)) teams_ok,
                    bool_and(s.finalized_at IS NOT NULL) finalized,
                    bool_and(s.mapping_version=%s AND pf.outcome='success'
                             AND pf.endpoint=%s AND pf.subject_fixture_id=f.id
                             AND pf.normalized_at IS NOT NULL AND pf.provider_results=2) provenance_ok,
                    count(DISTINCT s.last_source_fetch_id)=1 same_fetch
             FROM football.fixtures f
             LEFT JOIN football.fixture_team_statistics s ON s.fixture_id=f.id
             LEFT JOIN source.provider_fetches pf ON pf.id=s.last_source_fetch_id
             WHERE f.season_id=%s GROUP BY f.id
           ) q""",
        (MAPPING_VERSION, ENDPOINT, season_id),
    ).fetchone()
    fetch_states = conn.execute(
        """SELECT count(*) FILTER (WHERE c=0), count(*) FILTER (WHERE c=1)
           FROM (
             SELECT f.id, coalesce(max(pf.provider_results)
                        FILTER (WHERE pf.outcome='success' AND pf.normalized_at IS NOT NULL), -1) c
             FROM football.fixtures f
             LEFT JOIN source.provider_fetches pf
               ON pf.subject_fixture_id=f.id AND pf.endpoint=%s
             WHERE f.season_id=%s GROUP BY f.id
           ) q""",
        (ENDPOINT, season_id),
    ).fetchone()
    duplicates = conn.execute(
        """SELECT count(*) FROM (
             SELECT fixture_id,team_id FROM football.fixture_team_statistics
             GROUP BY fixture_id,team_id HAVING count(*)>1
           ) q"""
    ).fetchone()[0]
    orphans = conn.execute(
        """SELECT count(*) FROM football.fixture_team_statistics s
           LEFT JOIN football.fixtures f ON f.id=s.fixture_id
           LEFT JOIN football.teams t ON t.id=s.team_id
           LEFT JOIN source.provider_fetches pf ON pf.id=s.last_source_fetch_id
           WHERE f.id IS NULL OR t.id IS NULL OR pf.id IS NULL"""
    ).fetchone()[0]
    failed = conn.execute(
        """SELECT count(DISTINCT pf.subject_fixture_id)
           FROM source.provider_fetches pf
           JOIN football.fixtures f ON f.id=pf.subject_fixture_id
           WHERE pf.provider_id=%s AND pf.endpoint=%s AND f.season_id=%s
             AND pf.normalized_at IS NULL
             AND (pf.sanitized_error_class='StatisticsContractError'
                  OR pf.outcome='provider_error'
                  OR (pf.outcome='http_error' AND pf.http_status NOT IN (408,499,500,502,503,504))
                  OR (SELECT count(*) FROM source.provider_fetches x
                      WHERE x.provider_id=pf.provider_id AND x.endpoint=pf.endpoint
                        AND x.subject_fixture_id=pf.subject_fixture_id) >= %s)""",
        (provider_id, ENDPOINT, season_id, MAX_ATTEMPTS_PER_FIXTURE),
    ).fetchone()[0]
    after = {table: _fingerprint(conn, table) for table in before}
    complete_count = int(pair_rows[0])
    empty_count = int(fetch_states[0])
    partial_count = int(fetch_states[1])
    failed_count = int(failed)
    return {
        "statistics_rows": int(rows),
        "complete": complete_count,
        "invalid_pairs": int(pair_rows[1]),
        "empty": empty_count,
        "partial": partial_count,
        "failed": failed_count,
        "pending": scope.expected_fixture_count-complete_count-empty_count-partial_count-failed_count,
        "covered_fixtures": complete_count,
        "remaining_fixtures": scope.expected_fixture_count-complete_count,
        "duplicates": int(duplicates),
        "nonparticipants": int(nonparticipant),
        "orphans": int(orphans),
        "canary_unchanged": (
            _canary_statistics_fingerprint(
                conn,
                provider_id=provider_id,
                canary_fixture_external_id=scope.canary_fixture_external_id,
            )
            == canary_before
        ),
        "out_of_scope_fingerprints_unchanged": {
            table: before[table] == after[table] for table in before
        },
    }


async def _fetch_once(client: APIFootballClient, target: FixtureTarget) -> APIFootballResponse:
    return await client.get(ENDPOINT, params=_params(target.external_id))


def run_statistics_backfill(
    *,
    client: APIFootballClient | None = None,
    sleep: Sleep = asyncio.sleep,
    clock: Clock = _utcnow,
    max_calls: int = DEFAULT_RUN_ATTEMPT_CAP,
    approved_replay_fetch_ids: frozenset[int] = frozenset(),
    scope: StatisticsImportScope = EPL_2024_SCOPE,
) -> StatisticsBatchReport:
    """Run one quota-bounded batch. All unsafe states stop before the next fixture."""
    if not 1 <= max_calls <= DEFAULT_RUN_ATTEMPT_CAP:
        raise ValueError("max_calls must be between 1 and 90")
    api = client or APIFootballClient.from_environment()
    protected = (
        "football.fixtures", "football.fixture_availability_snapshots", "football.fixture_player_availability",
        "football.fixture_lineup_snapshots", "football.fixture_lineups", "football.fixture_lineup_players",
        "ml.predictions", "ml.prediction_feature_snapshots", "ml.prediction_fixture_inputs",
    )
    with psycopg.connect(_database_url(), autocommit=True) as conn:
        provider_id, season_id = acquire_context_and_lock(conn, scope=scope)
        approve_contract_replays(
            conn,
            provider_id=provider_id,
            season_id=season_id,
            fetch_ids=approved_replay_fetch_ids,
        )
        queue, initial = preflight_statistics_backfill(
            conn, provider_id=provider_id, season_id=season_id, scope=scope
        )
        # Persisted, hash-verified bodies are always replayed before any network call.
        raw_fixture_ids = {
            int(row[0])
            for row in conn.execute(
                """SELECT f.subject_fixture_id
                   FROM source.provider_fetches f
                   JOIN source.provider_raw_payloads r ON r.fetch_id=f.id
                   WHERE f.provider_id=%s AND f.endpoint=%s AND f.subject_season_id=%s
                     AND f.outcome='success' AND f.normalized_at IS NULL
                     AND r.purged_at IS NULL AND r.inline_body IS NOT NULL""",
                (provider_id, ENDPOINT, season_id),
            ).fetchall()
        }
        raw_targets = [target for target in queue if target.fixture_id in raw_fixture_ids]
        queue = raw_targets + [target for target in queue if target not in raw_targets]
        lifetime_attempts = {
            int(fixture_id): int(attempt_count)
            for fixture_id, attempt_count in conn.execute(
                """SELECT subject_fixture_id,count(*)
                   FROM source.provider_fetches
                   WHERE provider_id=%s AND endpoint=%s AND subject_season_id=%s
                   GROUP BY subject_fixture_id""",
                (provider_id, ENDPOINT, season_id),
            ).fetchall()
        }
        before = {table: _fingerprint(conn, table) for table in protected}
        canary_before = _canary_statistics_fingerprint(
            conn,
            provider_id=provider_id,
            canary_fixture_external_id=scope.canary_fixture_external_id,
        )
        total_prior, retries_prior = _attempt_budget(conn, provider_id=provider_id, season_id=season_id)
        if total_prior > DATASET_ATTEMPT_CAP or retries_prior > GLOBAL_RETRY_CAP:
            raise StatisticsBackfillError("durable statistics attempt budget already exceeded")
        attempts = reused = complete = empty = partial = created = retries = errors = 0
        quota: dict[str, str] = {}
        stop_reason: str | None = None
        for target in queue:
            raw = (
                _find_reusable_raw(conn, provider_id=provider_id, target=target)
                if target.fixture_id in raw_fixture_ids
                else None
            )
            if raw is not None:
                reused += 1
                state, rows_created = normalize_raw(conn, provider_id=provider_id, target=target, raw=raw, clock=clock)
                complete += state == "complete"; empty += state == "empty"; partial += state == "partial"; created += rows_created
                continue
            lifetime = lifetime_attempts.get(target.fixture_id, 0)
            for physical in range(int(lifetime) + 1, MAX_ATTEMPTS_PER_FIXTURE + 1):
                if attempts >= max_calls or total_prior + attempts >= DATASET_ATTEMPT_CAP:
                    stop_reason = "attempt_budget"; break
                if retries_prior + retries >= GLOBAL_RETRY_CAP and physical > 1:
                    stop_reason = "retry_budget"; break
                daily = quota.get("x-ratelimit-requests-remaining")
                if daily is not None and daily.isdigit() and int(daily) <= QUOTA_RESERVE:
                    stop_reason = "daily_quota_reserve"; break
                if attempts:
                    asyncio.run(sleep(PACE_SECONDS))
                if physical > 1:
                    retries += 1
                started = clock()
                try:
                    response = asyncio.run(_fetch_once(api, target)); received = clock(); attempts += 1
                    quota = safe_rate_limit_headers(response.headers)
                    if api.response_contains_api_key(response.raw_body):
                        raise StatisticsBackfillError("provider response contains API key")
                    raw = _persist_fetch(conn, provider_id=provider_id, target=target, response=response, started=started, received=received)
                    state, rows_created = normalize_raw(conn, provider_id=provider_id, target=target, raw=raw, clock=clock)
                    complete += state == "complete"; empty += state == "empty"; partial += state == "partial"; created += rows_created
                    break
                except APIFootballHTTPError as error:
                    received = clock(); attempts += 1; errors += 1; quota = dict(error.safe_headers)
                    _record_failure(conn, provider_id=provider_id, target=target, started=started, received=received, status=error.status_code or None, outcome="transport_error" if error.status_code == 0 else "http_error", error_class=type(error).__name__)
                    if error.status_code == 429:
                        raise StatisticsBackfillError("provider rate limit reached; campaign stopped") from error
                    if error.status_code not in ({0} | RETRYABLE_HTTP_STATUSES) or physical >= MAX_ATTEMPTS_PER_FIXTURE:
                        raise StatisticsBackfillError("non-retryable or exhausted provider failure") from error
                    continue
                except APIFootballAPIError as error:
                    attempts += 1
                    errors += 1
                    quota = dict(error.safe_headers)
                    received = clock()
                    if error.raw_body is not None and api.response_contains_api_key(error.raw_body):
                        raise StatisticsBackfillError(
                            "provider error response contains API key"
                        ) from error
                    _persist_api_error_response(
                        conn,
                        provider_id=provider_id,
                        target=target,
                        started=started,
                        received=received,
                        error=error,
                    )
                    raise StatisticsBackfillError("provider reported an API error") from error
            if stop_reason:
                break
        verification = remote_verification(
            conn,
            provider_id=provider_id,
            season_id=season_id,
            scope=scope,
            before=before,
            canary_before=canary_before,
        )
        return StatisticsBatchReport(
            attempts,
            reused,
            complete,
            empty,
            partial,
            0,
            created,
            retries,
            errors,
            stop_reason,
            quota,
            verification,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Controlled fixture statistics backfill")
    parser.add_argument("--league-external-id", type=int, default=EPL_2024_SCOPE.league_external_id)
    parser.add_argument("--season-start-year", type=int, default=EPL_2024_SCOPE.season_start_year)
    parser.add_argument("--expected-fixture-count", type=int, default=EPL_2024_SCOPE.expected_fixture_count)
    parser.add_argument(
        "--canary-fixture-external-id",
        type=int,
        default=EPL_2024_SCOPE.canary_fixture_external_id,
        help="Existing completed statistics fixture required as a no-op checkpoint; omit with --no-canary",
    )
    parser.add_argument(
        "--no-canary",
        action="store_true",
        help="Disable the optional existing-statistics canary requirement for a new season",
    )
    parser.add_argument(
        "--replay-fetch-id",
        action="append",
        type=int,
        default=[],
        help="Explicitly approved retained contract-anomaly fetch ID to replay without API",
    )
    arguments = parser.parse_args()
    scope = StatisticsImportScope(
        league_external_id=arguments.league_external_id,
        season_start_year=arguments.season_start_year,
        expected_fixture_count=arguments.expected_fixture_count,
        canary_fixture_external_id=(
            None if arguments.no_canary else arguments.canary_fixture_external_id
        ),
    )
    report = run_statistics_backfill(
        approved_replay_fetch_ids=frozenset(arguments.replay_fetch_id),
        scope=scope,
    )
    print(json.dumps(report.__dict__, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
