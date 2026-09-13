"""Q06 durable raw capture and replay for the fenced sync worker."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

from psycopg import Connection, Error as PsycopgError
from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from app.api_football import APIFootballResponse
from app.importer.raw_spool import RawSpool, RawSpoolArtifact
from app.importer.season_bootstrap import BaseRequest
from app.sync.policies import AuthorizedSyncWork
from app.sync.repository import LeasedWorkItem


RAW_RETENTION_DAYS = 30
POSTGRES_INTEGER_MAX = 2_147_483_647


class ProvenanceError(RuntimeError):
    """A raw response cannot safely become a replay or domain input."""


def _safe_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _safe_mapping(value)
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_safe_value(entry) for entry in value]
    raise ProvenanceError("provenance metadata contains an unsupported value")


def _safe_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    """Reject request-like metadata that could contain credentials or headers."""
    forbidden = ("authorization", "apikey", "key", "token", "secret", "password", "credential", "cookie", "header")
    output: dict[str, Any] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ProvenanceError("provenance metadata keys must be strings")
        normalized = "".join(character for character in key.lower() if character.isalnum())
        if normalized in {"key", "apikey"} or any(part in normalized for part in forbidden):
            raise ProvenanceError("provenance metadata must not contain credentials or headers")
        output[key] = _safe_value(item)
    return output


def _params_digest(params: Mapping[str, Any]) -> bytes:
    return hashlib.sha256(json.dumps(dict(params), sort_keys=True, separators=(",", ":")).encode()).digest()


def _nonnegative_integer(value: object) -> int | None:
    return (
        value
        if isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= POSTGRES_INTEGER_MAX
        else None
    )


def _positive_integer(value: object) -> int | None:
    return (
        value
        if isinstance(value, int) and not isinstance(value, bool) and 1 <= value <= POSTGRES_INTEGER_MAX
        else None
    )


def _response_summary(response: APIFootballResponse) -> tuple[int | None, int | None, int | None]:
    """Return only database-valid summaries; the immutable raw bytes retain the rest."""
    payload = response.data
    results = _nonnegative_integer(payload.get("results"))
    paging = payload.get("paging")
    current = _positive_integer(paging.get("current")) if isinstance(paging, Mapping) else None
    total = _positive_integer(paging.get("total")) if isinstance(paging, Mapping) else None
    if current is not None and total is not None and current > total:
        current = None
    return results, current, total


def _unique_fetch_ids(fetch_ids: Sequence[int]) -> tuple[int, ...]:
    unique: list[int] = []
    for fetch_id in fetch_ids:
        if not isinstance(fetch_id, int) or isinstance(fetch_id, bool) or fetch_id < 1:
            raise ProvenanceError("source fetch ids must be positive integers")
        if fetch_id not in unique:
            unique.append(fetch_id)
    return tuple(unique)


def _subject_id(scope: Mapping[str, Any], *, field: str, legacy_field: str) -> int | None:
    values = [scope[name] for name in (field, legacy_field) if name in scope]
    if not values:
        return None
    if any(not isinstance(value, int) or isinstance(value, bool) or value < 1 for value in values):
        raise ProvenanceError(f"{field} must be a positive integer")
    if any(value != values[0] for value in values[1:]):
        raise ProvenanceError(f"{field} conflicts with {legacy_field}")
    return values[0]


def _subjects(scope: Mapping[str, Any]) -> tuple[int | None, int | None, int | None]:
    """Map the safe queue scope to the relational provenance subjects."""
    return (
        _subject_id(scope, field="subject_fixture_id", legacy_field="fixture_id"),
        _subject_id(scope, field="subject_season_id", legacy_field="season_id"),
        _subject_id(scope, field="subject_team_id", legacy_field="team_id"),
    )


def _physical_request_id(work_item_id: int, attempt: int, request_number: int) -> str:
    if min(work_item_id, attempt, request_number) < 1:
        raise ProvenanceError("physical request identity requires positive work-item values")
    return f"work-item-{work_item_id}:attempt-{attempt}:request-{request_number:06d}"


@dataclass(frozen=True)
class RawFetchCapture:
    endpoint: str
    params: Mapping[str, Any]
    response: APIFootballResponse
    request_started_at: datetime
    response_received_at: datetime
    normalization_version: str
    purpose: str = "scheduled_refresh"
    retention_class: str = "standard"
    scope: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.endpoint.startswith("/") or not self.endpoint.strip():
            raise ValueError("raw capture endpoint must be an absolute nonblank path")
        if self.request_started_at.tzinfo is None or self.response_received_at.tzinfo is None:
            raise ValueError("raw capture timestamps must be timezone-aware")
        if self.response_received_at < self.request_started_at:
            raise ValueError("raw capture response precedes request")
        if not self.normalization_version.strip():
            raise ValueError("raw capture normalization version is required")
        if self.purpose not in {"bootstrap", "scheduled_refresh", "prematch", "postmatch_reconciliation", "research"}:
            raise ValueError("raw capture purpose is unsupported")
        if self.retention_class not in {"standard", "anomaly", "prediction_input", "contract_sample"}:
            raise ValueError("raw capture retention class is unsupported")
        _safe_mapping(self.params)
        if self.scope is not None:
            _safe_mapping(self.scope)


@dataclass(frozen=True)
class PersistedRawFetch:
    fetch_id: int
    capture: RawFetchCapture


class ProviderProvenance:
    """Persists a raw response before the fenced canonical transaction begins."""

    def __init__(
        self,
        connection: Connection[Any],
        *,
        response_contains_api_key: Callable[[bytes], bool],
        spool: RawSpool | None = None,
    ) -> None:
        self._connection = connection
        self._response_contains_api_key = response_contains_api_key
        self._spool = spool
        if spool is not None:
            spool.set_purge_verifier(self._spool_candidate_is_safe_from_database)
            spool.set_purge_guard(self._spool_candidate_guard_from_database)

    def _effective_scope(self, capture: RawFetchCapture, item: LeasedWorkItem) -> dict[str, Any]:
        return _safe_mapping(capture.scope if capture.scope is not None else item.scope)

    def _contains_configured_credential(self, capture: RawFetchCapture, scope: Mapping[str, Any]) -> bool:
        metadata = json.dumps(
            {"parameters": _safe_mapping(capture.params), "scope": scope},
            sort_keys=True, separators=(",", ":"),
        ).encode()
        return self._response_contains_api_key(capture.response.raw_body + b"\n" + metadata)

    def persist(
        self, item: LeasedWorkItem, authorization: AuthorizedSyncWork, captures: Sequence[RawFetchCapture],
    ) -> tuple[PersistedRawFetch, ...]:
        if not captures:
            return ()
        if self._connection.info.transaction_status != TransactionStatus.IDLE:
            raise ProvenanceError("raw persistence requires an idle database connection")
        # Use the client-owned detector before making any filesystem or
        # database mutation. Its error deliberately contains no raw content.
        for capture in captures:
            try:
                contains_credential = self._contains_configured_credential(capture, self._effective_scope(capture, item))
            except Exception:
                raise ProvenanceError("provider response credential check failed") from None
            if contains_credential:
                raise ProvenanceError("provider response contains configured credential")
        staged: list[object] = []
        database_captures: list[tuple[RawFetchCapture, int, str]] = []
        for request_number, capture in enumerate(captures, start=1):
            physical_request_id = _physical_request_id(item.id, item.attempts, request_number)
            database_captures.append((capture, item.attempts, physical_request_id))
            if self._spool is not None:
                directory = self._spool.work_item_request_directory(
                    work_item_id=item.id, attempt=item.attempts, request_number=request_number,
                )
                artifact = RawSpoolArtifact(
                    BaseRequest(capture.endpoint, dict(_safe_mapping(capture.params))), capture.response,
                    capture.request_started_at, capture.response_received_at,
                    self._effective_scope(capture, item), item.id, item.attempts,
                    capture.normalization_version, capture.purpose, capture.retention_class, physical_request_id,
                )
                self._spool.stage(directory, artifact)
                staged.append(directory)
        persisted = self._persist_database(item, authorization, database_captures)
        for directory in set(staged):
            assert self._spool is not None
            self._spool.mark_durable(directory)  # database bytes now survive spool eviction
        return persisted

    def recover_spooled_raw(
        self, item: LeasedWorkItem, authorization: AuthorizedSyncWork,
    ) -> tuple[PersistedRawFetch, ...]:
        """Commit verified pre-DB spool captures before a replay hook can choose HTTP."""
        if self._spool is None:
            return ()
        if self._connection.info.transaction_status != TransactionStatus.IDLE:
            raise ProvenanceError("spool recovery requires an idle database connection")
        staged: list[object] = []
        recoverable: list[tuple[RawFetchCapture, int, str]] = []
        for directory, artifact in self._spool.work_item_artifacts(work_item_id=item.id):
            assert artifact.work_item_attempt is not None
            if artifact.normalization_version is None or artifact.purpose is None or artifact.retention_class is None:
                raise ProvenanceError("raw spool capture has incomplete Q06 metadata")
            request_number = int(directory.name.removeprefix("request-"))
            physical_request_id = _physical_request_id(item.id, artifact.work_item_attempt, request_number)
            if artifact.physical_request_id is not None and artifact.physical_request_id != physical_request_id:
                raise ProvenanceError("raw spool physical request identity does not match its directory")
            capture = RawFetchCapture(
                artifact.request.endpoint,
                dict(artifact.request.params),
                artifact.response,
                artifact.request_started_at,
                artifact.response_received_at,
                artifact.normalization_version,
                artifact.purpose,
                artifact.retention_class,
                artifact.scope,
            )
            staged.append(directory)
            recoverable.append((capture, artifact.work_item_attempt, physical_request_id))
        recovered: list[PersistedRawFetch] = []
        # The top-level transaction must commit before a staged directory is
        # marked durable.  Every recovery capture follows the same insert and
        # full conflict comparison as an ordinary fresh persistence.
        with self._connection.transaction():
            self._validate_captures(item, recoverable)
            recovered.extend(self._persist_database_rows(item, authorization, recoverable))
        for directory in set(staged):
            self._spool.mark_durable(directory)
        return tuple(recovered)

    def _spool_candidate_is_safe_from_database(self, directory: object) -> bool:
        """Verify an existing durable capture after a process restart."""
        with self._spool_candidate_guard_from_database(directory) as proven:
            return proven

    @contextmanager
    def _spool_candidate_guard_from_database(self, directory: object) -> Iterator[bool]:
        """Hold matching source rows against mutation until local unlink ends."""
        if self._spool is None or not isinstance(directory, Path):
            yield False
            return
        try:
            proof = directory / self._spool._CLEANUP_PROOF  # type: ignore[attr-defined]
            proof_exists = proof.exists()
            if proof.exists():
                metadata = self._spool._cleanup_proof_metadata_unlocked(directory)  # type: ignore[attr-defined]
            else:
                metadata_files = [
                    path for path in directory.iterdir()
                    if path.is_file() and not path.is_symlink() and path.name.endswith(".request.json")
                ]
                if len(metadata_files) != 1:
                    yield False
                    return
                metadata = json.loads(metadata_files[0].read_text(encoding="utf-8"))
            endpoint, params = metadata.get("endpoint"), metadata.get("parameters")
            if not isinstance(endpoint, str) or not isinstance(params, Mapping):
                yield False
                return
            identity = self._spool._repeatable_identity_unlocked(directory)  # type: ignore[attr-defined]
            if identity is None:
                yield False
                return
            work_item_id, attempt, request_number = identity
            stored_work_item_id = metadata.get("work_item_id")
            stored_attempt = metadata.get("work_item_attempt")
            physical_request_id = metadata.get("physical_request_id")
            if (
                not isinstance(stored_work_item_id, int)
                or not isinstance(stored_attempt, int)
                or not isinstance(physical_request_id, str)
                or stored_work_item_id != work_item_id
                or stored_attempt != attempt
            ):
                yield False
                return
            if request_number is not None:
                if physical_request_id != _physical_request_id(work_item_id, attempt, request_number):
                    yield False
                    return
            elif not re.fullmatch(
                rf"work-item-{work_item_id}:attempt-{attempt}:request-[0-9]+",
                physical_request_id,
            ):
                yield False
                return
            scope_value = metadata.get("scope", {})
            if not isinstance(scope_value, Mapping):
                yield False
                return
            scope = _safe_mapping(scope_value)
            subject_fixture_id, subject_season_id, subject_team_id = _subjects(scope)
            raw_hash = metadata.get("content_sha256")
            byte_count = metadata.get("byte_count")
            started = datetime.fromisoformat(str(metadata["request_started_at"]).replace("Z", "+00:00"))
            received = datetime.fromisoformat(str(metadata["response_received_at"]).replace("Z", "+00:00"))
            http_status = metadata.get("http_status")
            normalization_version = metadata.get("normalization_version")
            purpose = metadata.get("purpose")
            retention_class = metadata.get("retention_class")
            if (
                not isinstance(raw_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", raw_hash)
                or not isinstance(byte_count, int) or isinstance(byte_count, bool) or byte_count < 0
                or started.tzinfo is None or received.tzinfo is None or received < started
                or not isinstance(http_status, int) or isinstance(http_status, bool)
                or not isinstance(normalization_version, str) or not normalization_version.strip()
                or not isinstance(purpose, str) or not isinstance(retention_class, str)
            ):
                yield False
                return
            expires_at = None if retention_class == "contract_sample" else received + timedelta(days=RAW_RETENTION_DAYS)
            raw_path = directory / f"{self._spool._label(endpoint)}.raw.json"  # type: ignore[attr-defined]
            local_raw: bytes | None = None
            if raw_path.exists():
                if not raw_path.is_file() or raw_path.is_symlink():
                    yield False
                    return
                local_raw = raw_path.read_bytes()
                if len(local_raw) != byte_count or hashlib.sha256(local_raw).hexdigest() != raw_hash:
                    yield False
                    return
            elif not proof_exists:
                yield False
                return
        except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError, ProvenanceError):
            yield False
            return

        # Do not use ``with`` around the yield: an unlink exception must leave
        # this context as an unlink exception, not be recast as a failed DB
        # check.  The transaction stays open until that body ends, retaining
        # FOR SHARE locks on the exact fetch, payload and work-item rows.
        transaction = self._connection.transaction()
        try:
            transaction.__enter__()
        except PsycopgError:
            yield False
            return
        try:
            self._connection.execute("SET LOCAL lock_timeout = '2s'")
            row = self._connection.execute(
                """SELECT provider_fetch.provider_results,provider_fetch.paging_current,provider_fetch.paging_total,
                          payload.inline_body
                     FROM source.provider_fetches provider_fetch
                     JOIN source.provider_raw_payloads payload ON payload.fetch_id=provider_fetch.id
                     JOIN ops.sync_work_items item ON item.id=provider_fetch.sync_work_item_id
                     JOIN ops.sync_runs run ON run.id=item.run_id AND run.provider_id=provider_fetch.provider_id
                    WHERE item.status='succeeded'
                      AND provider_fetch.sync_work_item_id=%s
                      AND provider_fetch.sync_work_item_attempt=%s
                      AND provider_fetch.request_scope->>'physical_request_id'=%s
                      AND provider_fetch.endpoint=%s
                      AND provider_fetch.request_params=%s
                      AND provider_fetch.request_params_sha256=%s
                      AND provider_fetch.purpose=%s
                      AND provider_fetch.request_started_at=%s
                      AND provider_fetch.response_received_at=%s
                      AND provider_fetch.http_status=%s
                      AND provider_fetch.outcome='success'
                      AND provider_fetch.content_sha256=%s
                      AND provider_fetch.request_scope->'scope'=%s
                      AND provider_fetch.normalization_version=%s
                      AND provider_fetch.subject_fixture_id IS NOT DISTINCT FROM %s
                      AND provider_fetch.subject_season_id IS NOT DISTINCT FROM %s
                      AND provider_fetch.subject_team_id IS NOT DISTINCT FROM %s
                      AND payload.inline_body IS NOT NULL
                      AND payload.content_type='application/json'
                      AND payload.content_encoding IS NULL
                      AND payload.object_key IS NULL
                      AND payload.byte_count=%s
                      AND payload.retention_class=%s
                      AND payload.expires_at IS NOT DISTINCT FROM %s
                      AND payload.purged_at IS NULL
                    FOR SHARE OF provider_fetch,payload,item""",
                (
                    stored_work_item_id, stored_attempt, physical_request_id,
                    endpoint, Jsonb(dict(params)), _params_digest(params), purpose, started, received,
                    http_status, bytes.fromhex(raw_hash), Jsonb(scope), normalization_version,
                    subject_fixture_id, subject_season_id, subject_team_id,
                    byte_count, retention_class, expires_at,
                ),
            ).fetchone()
        except PsycopgError as error:
            transaction.__exit__(type(error), error, error.__traceback__)
            yield False
            return
        try:
            if row is None or not isinstance(row[3], bytes):
                proven = False
            else:
                raw = row[3]
                if len(raw) != byte_count or hashlib.sha256(raw).hexdigest() != raw_hash:
                    proven = False
                elif local_raw is not None and local_raw != raw:
                    proven = False
                else:
                    try:
                        payload = json.loads(raw)
                        if not isinstance(payload, dict):
                            proven = False
                        else:
                            results, paging_current, paging_total = _response_summary(
                                APIFootballResponse(payload, raw, http_status, {}),
                            )
                            proven = (row[0], row[1], row[2]) == (results, paging_current, paging_total)
                    except (ValueError, TypeError, json.JSONDecodeError, ProvenanceError):
                        proven = False
            try:
                yield proven
            except BaseException as error:
                transaction.__exit__(type(error), error, error.__traceback__)
                raise
            else:
                transaction.__exit__(None, None, None)
        except BaseException:
            # The only errors this level may handle are from the DB context;
            # errors from the unlink body were already re-raised above.
            raise
    def _persist_database(
        self,
        item: LeasedWorkItem,
        authorization: AuthorizedSyncWork,
        captures: Sequence[tuple[RawFetchCapture, int, str]],
    ) -> tuple[PersistedRawFetch, ...]:
        self._validate_captures(item, captures)
        # This transaction intentionally ends before the Q03 lease guard and
        # domain writer.  A later normalization rollback cannot erase evidence.
        with self._connection.transaction():
            return self._persist_database_rows(item, authorization, captures)

    def _validate_captures(
        self, item: LeasedWorkItem, captures: Sequence[tuple[RawFetchCapture, int, str]],
    ) -> None:
        for capture, source_attempt, physical_request_id in captures:
            if not isinstance(source_attempt, int) or isinstance(source_attempt, bool) or source_attempt < 1:
                raise ProvenanceError("source work-item attempt must be a positive integer")
            if not isinstance(physical_request_id, str) or not physical_request_id.strip():
                raise ProvenanceError("physical request identity is required")
            try:
                contains_credential = self._contains_configured_credential(capture, self._effective_scope(capture, item))
            except Exception:
                raise ProvenanceError("provider response credential check failed") from None
            if contains_credential:
                raise ProvenanceError("provider response contains configured credential")

    def _persist_database_rows(
        self,
        item: LeasedWorkItem,
        authorization: AuthorizedSyncWork,
        captures: Sequence[tuple[RawFetchCapture, int, str]],
    ) -> tuple[PersistedRawFetch, ...]:
        persisted: list[PersistedRawFetch] = []
        for capture, source_attempt, physical_request_id in captures:
            results, paging_current, paging_total = _response_summary(capture.response)
            capture_scope = self._effective_scope(capture, item)
            subject_fixture_id, subject_season_id, subject_team_id = _subjects(capture_scope)
            scope = {
                "scope_key": item.scope_key,
                "job_type": item.job_type,
                "scope": capture_scope,
                "physical_request_id": physical_request_id,
            }
            expires_at = (
                None
                if capture.retention_class == "contract_sample"
                else capture.response_received_at + timedelta(days=RAW_RETENTION_DAYS)
            )
            row = self._connection.execute(
                    """INSERT INTO source.provider_fetches(
                           provider_id,endpoint,request_params,request_params_sha256,purpose,
                           request_started_at,response_received_at,http_status,outcome,
                           provider_results,paging_current,paging_total,content_sha256,
                           request_scope,normalization_version,sync_work_item_id,sync_work_item_attempt,
                           subject_fixture_id,subject_season_id,subject_team_id
                       ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'success',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT ((request_scope ->> 'physical_request_id'))
                       WHERE request_scope ? 'physical_request_id' DO NOTHING
                       RETURNING id""",
                    (
                        authorization.request.provider_id, capture.endpoint, Jsonb(_safe_mapping(capture.params)),
                        _params_digest(capture.params), capture.purpose, capture.request_started_at,
                        capture.response_received_at, capture.response.status_code,
                        results, paging_current, paging_total,
                        hashlib.sha256(capture.response.raw_body).digest(), Jsonb(scope), capture.normalization_version,
                        item.id, source_attempt, subject_fixture_id, subject_season_id, subject_team_id,
                    ),
            ).fetchone()
            if row is None:
                existing = self._matching_physical_request(
                    item,
                    authorization,
                    capture,
                    source_attempt,
                    physical_request_id,
                    results,
                    paging_current,
                    paging_total,
                    scope,
                    subject_fixture_id,
                    subject_season_id,
                    subject_team_id,
                    expires_at,
                )
                if existing is None:
                    raise ProvenanceError("existing physical request does not match captured raw provenance")
                persisted.append(PersistedRawFetch(existing, capture))
                continue
            fetch_id = int(row[0])
            self._connection.execute(
                    """INSERT INTO source.provider_raw_payloads(
                           fetch_id,inline_body,content_type,byte_count,retention_class,expires_at
                       ) VALUES(%s,%s,'application/json',%s,%s,%s)""",
                    (
                        fetch_id, capture.response.raw_body, len(capture.response.raw_body), capture.retention_class,
                        expires_at,
                    ),
            )
            persisted.append(PersistedRawFetch(fetch_id, capture))
        return tuple(persisted)

    def _matching_physical_request(
        self,
        item: LeasedWorkItem,
        authorization: AuthorizedSyncWork,
        capture: RawFetchCapture,
        source_attempt: int,
        physical_request_id: str,
        results: int | None,
        paging_current: int | None,
        paging_total: int | None,
        scope: Mapping[str, Any],
        subject_fixture_id: int | None,
        subject_season_id: int | None,
        subject_team_id: int | None,
        expires_at: datetime | None,
    ) -> int | None:
        row = self._connection.execute(
            """SELECT provider_fetch.id
                 FROM source.provider_fetches provider_fetch
                 JOIN source.provider_raw_payloads payload ON payload.fetch_id=provider_fetch.id
                WHERE provider_fetch.request_scope->>'physical_request_id'=%s
                  AND provider_fetch.provider_id=%s
                  AND provider_fetch.endpoint=%s
                  AND provider_fetch.request_params=%s
                  AND provider_fetch.request_params_sha256=%s
                  AND provider_fetch.purpose=%s
                  AND provider_fetch.request_started_at=%s
                  AND provider_fetch.response_received_at=%s
                  AND provider_fetch.http_status=%s
                  AND provider_fetch.outcome='success'
                  AND provider_fetch.provider_results IS NOT DISTINCT FROM %s
                  AND provider_fetch.paging_current IS NOT DISTINCT FROM %s
                  AND provider_fetch.paging_total IS NOT DISTINCT FROM %s
                  AND provider_fetch.content_sha256=%s
                  AND provider_fetch.request_scope=%s
                  AND provider_fetch.normalization_version=%s
                  AND provider_fetch.sync_work_item_id=%s
                  AND provider_fetch.sync_work_item_attempt=%s
                  AND provider_fetch.subject_fixture_id IS NOT DISTINCT FROM %s
                  AND provider_fetch.subject_season_id IS NOT DISTINCT FROM %s
                  AND provider_fetch.subject_team_id IS NOT DISTINCT FROM %s
                  AND payload.inline_body=%s
                  AND payload.content_type='application/json'
                  AND payload.content_encoding IS NULL
                  AND payload.object_key IS NULL
                  AND payload.byte_count=%s
                  AND payload.retention_class=%s
                  AND payload.expires_at IS NOT DISTINCT FROM %s
                  AND payload.purged_at IS NULL""",
            (
                physical_request_id,
                authorization.request.provider_id,
                capture.endpoint,
                Jsonb(_safe_mapping(capture.params)),
                _params_digest(capture.params),
                capture.purpose,
                capture.request_started_at,
                capture.response_received_at,
                capture.response.status_code,
                results,
                paging_current,
                paging_total,
                hashlib.sha256(capture.response.raw_body).digest(),
                Jsonb(dict(scope)),
                capture.normalization_version,
                item.id,
                source_attempt,
                subject_fixture_id,
                subject_season_id,
                subject_team_id,
                capture.response.raw_body,
                len(capture.response.raw_body),
                capture.retention_class,
                expires_at,
            ),
        ).fetchone()
        return int(row[0]) if row is not None else None

    def latest_replay(self, item: LeasedWorkItem, *, endpoint: str, params: Mapping[str, Any]) -> PersistedRawFetch | None:
        """Return verified durable bytes from an earlier attempt without HTTP."""
        with self._connection.transaction():
            row = self._connection.execute(
                """SELECT provider_fetch.id,provider_fetch.request_params,provider_fetch.request_started_at,provider_fetch.response_received_at,
                          provider_fetch.http_status,
                          provider_fetch.normalization_version,provider_fetch.purpose,provider_fetch.request_scope,payload.retention_class,
                          provider_fetch.content_sha256,payload.inline_body
                     FROM source.provider_fetches provider_fetch
                     JOIN source.provider_raw_payloads payload ON payload.fetch_id=provider_fetch.id
                    WHERE provider_fetch.sync_work_item_id=%s AND provider_fetch.endpoint=%s
                      AND provider_fetch.request_params_sha256=%s AND provider_fetch.outcome='success'
                      AND payload.purged_at IS NULL AND payload.inline_body IS NOT NULL
                    ORDER BY provider_fetch.sync_work_item_attempt DESC,provider_fetch.id DESC LIMIT 1""",
                (item.id, endpoint, _params_digest(params)),
            ).fetchone()
        if row is None:
            return None
        fetch_id, stored_params, started, received, http_status, version, purpose, stored_scope, retention_class, expected_hash, raw = row
        if not isinstance(raw, bytes) or not isinstance(expected_hash, bytes) or hashlib.sha256(raw).digest() != expected_hash:
            raise ProvenanceError("retained provider raw payload SHA-256 mismatch")
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise ProvenanceError("retained provider raw payload is not JSON") from error
        if (
            not isinstance(payload, dict)
            or not isinstance(stored_params, Mapping)
            or not isinstance(started, datetime)
            or not isinstance(received, datetime)
            or not isinstance(http_status, int)
            or isinstance(http_status, bool)
            or not isinstance(version, str)
            or not isinstance(purpose, str)
            or not isinstance(retention_class, str)
        ):
            raise ProvenanceError("retained provider raw payload metadata is invalid")
        scope = stored_scope.get("scope") if isinstance(stored_scope, Mapping) else None
        capture = RawFetchCapture(
            endpoint, _safe_mapping(stored_params), APIFootballResponse(payload, raw, http_status, {}), started, received,
            version, purpose, retention_class, scope if isinstance(scope, Mapping) else None,
        )
        return PersistedRawFetch(int(fetch_id), capture)

    def verify_source_fetches(
        self,
        writer: Any,
        item: LeasedWorkItem,
        source_fetch_ids: Sequence[int],
        *,
        replayed_fetch_ids: Sequence[int] = (),
    ) -> None:
        """Fence provenance with the domain writes and recheck replay bytes immediately before apply."""
        source_ids = _unique_fetch_ids(source_fetch_ids)
        replayed_ids = _unique_fetch_ids(replayed_fetch_ids)
        if not set(replayed_ids).issubset(source_ids):
            raise ProvenanceError("replayed fetch ids must be source fetch ids")
        for fetch_id in source_ids:
            if fetch_id in replayed_ids:
                row = writer.execute(
                    """SELECT provider_fetch.content_sha256,payload.inline_body
                         FROM source.provider_fetches provider_fetch
                         JOIN source.provider_raw_payloads payload ON payload.fetch_id=provider_fetch.id
                        WHERE provider_fetch.id=%s AND provider_fetch.sync_work_item_id=%s
                          AND provider_fetch.outcome='success' AND payload.purged_at IS NULL
                          AND payload.inline_body IS NOT NULL
                        FOR SHARE OF provider_fetch,payload""",
                    (fetch_id, item.id),
                ).fetchone()
                if row is None:
                    raise ProvenanceError("replay source raw payload is unavailable")
                expected_hash, raw = row
                if not isinstance(expected_hash, bytes) or not isinstance(raw, bytes) or hashlib.sha256(raw).digest() != expected_hash:
                    raise ProvenanceError("retained provider raw payload SHA-256 mismatch")
                continue
            row = writer.execute(
                """SELECT id FROM source.provider_fetches
                    WHERE id=%s AND sync_work_item_id=%s AND outcome='success'""",
                (fetch_id, item.id),
            ).fetchone()
            if row is None:
                raise ProvenanceError("source fetch is unavailable for this work item")

    def record_reprocessing(
        self, writer: Any, item: LeasedWorkItem, replayed_fetch_ids: Sequence[int], normalization_version: str,
    ) -> None:
        """Record accepted replay in the same transaction as its domain writes and completion."""
        if not normalization_version.strip():
            raise ProvenanceError("replay normalization version is required")
        for fetch_id in _unique_fetch_ids(replayed_fetch_ids):
            writer.execute(
                """INSERT INTO source.provider_fetch_replays(
                       source_fetch_id,sync_work_item_id,sync_work_item_attempt,normalization_version
                   ) VALUES(%s,%s,%s,%s)
                   ON CONFLICT (source_fetch_id,sync_work_item_id,sync_work_item_attempt) DO NOTHING""",
                (fetch_id, item.id, item.attempts, normalization_version),
            )
