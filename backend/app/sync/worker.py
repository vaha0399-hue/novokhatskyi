"""Opt-in Q03 runner for fenced repeatable work; legacy workers do not use it."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from random import uniform
from threading import Event, Thread
from typing import Any, Protocol

from psycopg import Connection
from psycopg.types.json import Jsonb

from app.api_football.budget import APIFootballBudgetError, budget_retry_delay_seconds
from app.api_football.errors import APIFootballHTTPError
from app.sync.policies import AuthorizedSyncWork, SyncPolicyDenied, SyncPolicyGate, SyncWorkRequest
from app.sync.repository import LeasedWorkItem, PostgresSyncRepository
from app.sync.provenance import ProviderProvenance, RawFetchCapture


class LeaseLost(RuntimeError):
    """The result must not be applied because its fence is no longer current."""


class _AtomicCursor:
    """Cursor facade which deliberately cannot lead back to a connection."""
    __slots__ = ("_cursor",)

    def __init__(self, cursor: Any) -> None:
        object.__setattr__(self, "_cursor", cursor)

    def __getattribute__(self, name: str) -> Any:
        if name in {"connection", "_cursor"}:
            raise AttributeError("raw database connection is not exposed")
        return object.__getattribute__(self, name)

    def fetchone(self) -> Any:
        return object.__getattribute__(self, "_cursor").fetchone()

    def fetchall(self) -> Any:
        return object.__getattribute__(self, "_cursor").fetchall()

    @property
    def rowcount(self) -> int:
        return object.__getattribute__(self, "_cursor").rowcount


class _AtomicSavepoint:
    """Nested transaction facade which cannot expose psycopg's connection."""
    __slots__ = ("_context",)

    def __init__(self, context: Any) -> None:
        object.__setattr__(self, "_context", context)

    def __getattribute__(self, name: str) -> Any:
        if name in {"connection", "_context"}:
            raise AttributeError("raw database connection is not exposed")
        return object.__getattribute__(self, name)

    def __enter__(self) -> "_AtomicSavepoint":
        object.__getattribute__(self, "_context").__enter__()
        return self

    def __exit__(self, *args: Any) -> Any:
        return object.__getattribute__(self, "_context").__exit__(*args)


class AtomicWorkTransaction:
    """Restricted writer capability: no commit and no connection factory."""
    __slots__ = ("_execute", "_transaction")
    def __init__(self, connection: Connection[Any]) -> None:
        self._execute, self._transaction = connection.execute, connection.transaction

    def __getattribute__(self, name: str) -> Any:
        if name in {"_execute", "_transaction", "_connection"}:
            raise AttributeError("raw database connection is not exposed")
        return object.__getattribute__(self, name)

    def execute(self, query: str, params: Any = None) -> _AtomicCursor:
        self._validate_single_sql_statement(query)
        return _AtomicCursor(object.__getattribute__(self, "_execute")(query, params))

    def _validate_single_sql_statement(self, query: object) -> None:
        """Accept one SQL command without interpreting comments inside literals.

        This is deliberately a narrow lexer, not a SQL rewriter: statements are
        passed to PostgreSQL unchanged only after their lexical boundaries have
        been checked.  PostgreSQL treats comments as whitespace, while quoted
        and dollar-quoted constants may legitimately contain comment markers or
        semicolons.
        """
        if not isinstance(query, str):
            raise TypeError("atomic work writers accept only text SQL")

        first_keyword: str | None = None
        has_statement = False
        terminated = False
        index = 0
        length = len(query)

        while index < length:
            character = query[index]
            if character.isspace():
                index += 1
                continue
            if query.startswith("--", index):
                index = AtomicWorkTransaction._skip_line_comment(query, index)
                continue
            if query.startswith("/*", index):
                index = AtomicWorkTransaction._skip_block_comment(query, index)
                continue
            if terminated:
                raise RuntimeError("atomic work writers accept exactly one SQL statement")
            if character == "'":
                index = AtomicWorkTransaction._skip_string_literal(
                    query, index, self._string_uses_backslash_escapes(query, index),
                )
                continue
            if character == '"':
                index = AtomicWorkTransaction._skip_quoted_identifier(query, index)
                continue
            if character == "$":
                delimiter = AtomicWorkTransaction._dollar_quote_delimiter(query, index)
                if delimiter is not None:
                    closing = query.find(delimiter, index + len(delimiter))
                    if closing == -1:
                        raise RuntimeError("atomic work SQL contains an unterminated dollar-quoted literal")
                    index = closing + len(delimiter)
                    continue
            if character == ";":
                if not has_statement or terminated:
                    raise RuntimeError("atomic work writers accept exactly one SQL statement")
                terminated = True
                index += 1
                continue
            has_statement = True
            if AtomicWorkTransaction._is_identifier_start(character):
                start = index
                index += 1
                while index < length and AtomicWorkTransaction._is_identifier_continue(query[index]):
                    index += 1
                if first_keyword is None:
                    first_keyword = query[start:index].casefold()
                continue
            index += 1

        if not has_statement:
            raise RuntimeError("atomic work writers require one SQL statement")
        if first_keyword in {
            "abort", "begin", "call", "commit", "do", "end", "execute", "prepare", "release", "rollback",
            "savepoint", "set", "start",
        }:
            raise RuntimeError("atomic work writers cannot control the outer transaction")

    @staticmethod
    def _skip_line_comment(query: str, index: int) -> int:
        """Return the position after a PostgreSQL line-comment terminator.

        PostgreSQL accepts LF, CR, and CRLF as line endings.  This must match
        the server's lexical boundary: otherwise a CR can hide a following
        semicolon and transaction-control statement from the capability check.
        """
        index += 2
        while index < len(query) and query[index] not in {"\r", "\n"}:
            index += 1
        if index < len(query) and query[index] == "\r":
            index += 1
            if index < len(query) and query[index] == "\n":
                index += 1
            return index
        return index + 1 if index < len(query) else index

    @staticmethod
    def _skip_block_comment(query: str, index: int) -> int:
        depth = 1
        index += 2
        while index < len(query):
            if query.startswith("/*", index):
                depth += 1
                index += 2
            elif query.startswith("*/", index):
                depth -= 1
                index += 2
                if depth == 0:
                    return index
            else:
                index += 1
        raise RuntimeError("atomic work SQL contains an unterminated block comment")

    @staticmethod
    def _string_uses_backslash_escapes(query: str, index: int) -> bool:
        previous = query[index - 1] if index else ""
        before_previous = query[index - 2] if index > 1 else ""
        if previous in {"e", "E"} and (index == 1 or not AtomicWorkTransaction._is_identifier_continue(before_previous)):
            return True
        return False

    @staticmethod
    def _skip_string_literal(query: str, index: int, backslash_escapes: bool) -> int:
        index += 1
        while index < len(query):
            if query[index] == "'":
                if index + 1 < len(query) and query[index + 1] == "'":
                    index += 2
                    continue
                return index + 1
            if query[index] == "\\":
                if not backslash_escapes:
                    raise RuntimeError("atomic work SQL requires E strings for backslash escapes")
                index += 2
            else:
                index += 1
        raise RuntimeError("atomic work SQL contains an unterminated string literal")

    @staticmethod
    def _skip_quoted_identifier(query: str, index: int) -> int:
        index += 1
        while index < len(query):
            if query[index] == '"':
                if index + 1 < len(query) and query[index + 1] == '"':
                    index += 2
                    continue
                return index + 1
            index += 1
        raise RuntimeError("atomic work SQL contains an unterminated quoted identifier")

    @staticmethod
    def _dollar_quote_delimiter(query: str, index: int) -> str | None:
        if index and AtomicWorkTransaction._is_identifier_continue(query[index - 1]):
            return None
        end = index + 1
        if end < len(query) and AtomicWorkTransaction._is_identifier_start(query[end]):
            end += 1
            # PostgreSQL dollar-quote tags follow identifier rules except they
            # cannot themselves contain a dollar sign.
            while end < len(query) and (
                AtomicWorkTransaction._is_identifier_start(query[end]) or query[end].isdigit()
            ):
                end += 1
        if end < len(query) and query[end] == "$":
            return query[index:end + 1]
        return None

    @staticmethod
    def _is_identifier_start(character: str) -> bool:
        return character == "_" or character.isalpha()

    @staticmethod
    def _is_identifier_continue(character: str) -> bool:
        return AtomicWorkTransaction._is_identifier_start(character) or character.isdigit() or character == "$"

    def transaction(self) -> _AtomicSavepoint:
        """Permit canonical writers' nested savepoints, never a top-level commit."""
        return _AtomicSavepoint(object.__getattribute__(self, "_transaction")())


@dataclass(frozen=True)
class WorkResult:
    checkpoint: Mapping[str, Any]
    dependent_work: tuple[Callable[[AtomicWorkTransaction], None], ...] = ()
    raw_fetches: tuple[RawFetchCapture, ...] = ()
    source_fetch_ids: tuple[int, ...] = ()
    replayed_fetch_ids: tuple[int, ...] = ()
    replay_normalization_version: str | None = None


class FetchExecutor(Protocol):
    def __call__(self, item: LeasedWorkItem, authorization: AuthorizedSyncWork) -> WorkResult: ...


class ReplayableFetchExecutor(FetchExecutor, Protocol):
    """Optional Q06 hook: return a verified saved response before HTTP."""
    def replay(self, item: LeasedWorkItem, authorization: AuthorizedSyncWork, provenance: ProviderProvenance) -> WorkResult | None: ...


class RepeatableSyncWorker:
    """Runs HTTP/fetch work outside a transaction, then fences the write txn.

    ``apply_result`` gets a restricted capability, not a connection. It cannot
    commit or open another connection; this is the integration boundary for
    canonical writers until those writers are adapted for Q03.
    """
    def __init__(self, connection: Connection[Any], policy_gate: SyncPolicyGate, owner: str,
                 heartbeat_connection_factory: Callable[[], Connection[Any]] | None = None,
                 heartbeat_interval: float = 30.0, provenance: ProviderProvenance | None = None) -> None:
        if heartbeat_connection_factory is None:
            raise ValueError("repeatable worker requires a separate heartbeat connection factory")
        self._connection, self._gate, self._owner = connection, policy_gate, owner
        self.repository = PostgresSyncRepository(connection, policy_gate)
        self._heartbeat_connection_factory = heartbeat_connection_factory
        self._heartbeat_interval = heartbeat_interval
        self._provenance = provenance

    def run_once(self, fetch: FetchExecutor, apply_result: Callable[[AtomicWorkTransaction, LeasedWorkItem, WorkResult], None], *, max_attempts: int = 5) -> bool:
        # Claim and policy recheck are a short transaction, deliberately
        # committed before any network wait.  Quarantine inside this block so
        # the token-bearing claim is committed with its state transition.
        transaction_status = getattr(getattr(self._connection, "info", None), "transaction_status", None)
        if transaction_status is not None and getattr(transaction_status, "name", str(transaction_status)) != "IDLE":
            raise RuntimeError("repeatable worker requires an IDLE main connection before claim")
        with self._connection.transaction():
            try:
                item = self.repository.claim_next(self._owner, max_attempts=max_attempts)
                if item is None:
                    return False
                authorization = self._authorization(item)
            except (SyncPolicyDenied, ValueError) as exc:
                self.repository.requeue(item, self._owner, {}, str(exc), contract_error=True)
                return True
        # Fetchers may wait on HTTP; no transaction is active here.
        failed = Event()
        stop = Event()
        def beat() -> None:
            while not stop.wait(self._heartbeat_interval):
                try:
                    if not self._send_heartbeat(item):
                        failed.set(); return
                except Exception:
                    # A broken factory, connection, or heartbeat SQL is as
                    # fatal as a false fence response: never apply stale work.
                    failed.set(); return
        thread = Thread(target=beat, daemon=True)
        thread.start()
        try:
            recover_spooled_raw = getattr(self._provenance, "recover_spooled_raw", None)
            if callable(recover_spooled_raw):
                # A crash can leave verified bytes in the spool before their
                # independent raw transaction committed.  Recover them before
                # the registry decides whether it must issue HTTP again.
                recover_spooled_raw(item, authorization)
            replay = getattr(fetch, "replay", None)
            result = replay(item, authorization, self._provenance) if self._provenance is not None and callable(replay) else None
            if result is None:
                result = fetch(item, authorization)
            else:
                if (
                    result.raw_fetches
                    or not result.source_fetch_ids
                    or not isinstance(result.replay_normalization_version, str)
                    or not result.replay_normalization_version.strip()
                ):
                    raise RuntimeError("replay must return source fetch ids and a normalization version without new raw captures")
                result = replace(result, replayed_fetch_ids=result.source_fetch_ids)
            if result.raw_fetches and (result.source_fetch_ids or result.replayed_fetch_ids):
                raise RuntimeError("fresh raw captures cannot be combined with existing source fetch ids")
        except (APIFootballBudgetError, APIFootballHTTPError) as exc:
            stop.set()
            thread.join()
            if failed.is_set():
                raise LeaseLost("repeatable work-item heartbeat failed") from exc
            if isinstance(exc, APIFootballBudgetError):
                deferred = self.repository.defer_for_budget(
                    item, self._owner, delay=f"{budget_retry_delay_seconds(exc)} seconds",
                )
            elif exc.status_code == 429:
                # The client already recorded the shared cooldown. A later
                # attempt must pass Q04 reserve again, including Retry-After.
                deferred = self.repository.defer_for_budget(item, self._owner, delay="60 seconds")
            elif exc.status_code == 0 or 500 <= exc.status_code < 600:
                delay = min(60, 2 ** min(item.attempts, 6)) + uniform(0, 1)
                deferred = self.repository.defer_for_retry(
                    item, self._owner, delay=f"{delay} seconds", error=f"provider_http_{exc.status_code}",
                )
            else:
                with self._connection.transaction():
                    deferred = self.repository.requeue(item, self._owner, {}, str(exc), contract_error=True)
            if not deferred:
                raise LeaseLost("repeatable work-item lease was lost before failure handling") from exc
            return True
        except Exception as exc:
            stop.set()
            thread.join()
            with self._connection.transaction():
                self.repository.requeue(item, self._owner, {}, str(exc), contract_error=True)
            return True
        stop.set()
        thread.join()
        if failed.is_set():
            raise LeaseLost("repeatable work-item heartbeat failed")
        if result.raw_fetches:
            if self._provenance is None:
                raise RuntimeError("raw fetches require a Q06 provenance recorder")
            persisted = self._provenance.persist(item, authorization, result.raw_fetches)
            result = replace(result, source_fetch_ids=tuple(value.fetch_id for value in persisted))
        with self._connection.transaction():
            guarded = self._connection.execute(
                "SELECT ops.guard_repeatable_sync_work_item_lease(%s,%s,%s)",
                (item.id, self._owner, item.lease_token),
            ).fetchone()
            if guarded is None or guarded[0] is not True:
                raise LeaseLost("repeatable work-item lease was lost before applying its result")
            writer = AtomicWorkTransaction(self._connection)
            if result.source_fetch_ids:
                if self._provenance is None:
                    raise RuntimeError("source fetch ids require a Q06 provenance recorder")
                # Replayed bytes are hashed again here, after the lease fence
                # and before the first domain mutation.
                self._provenance.verify_source_fetches(
                    writer, item, result.source_fetch_ids, replayed_fetch_ids=result.replayed_fetch_ids,
                )
            apply_result(writer, item, result)
            for enqueue_dependent in result.dependent_work:
                enqueue_dependent(writer)
            if result.replayed_fetch_ids:
                assert self._provenance is not None
                assert isinstance(result.replay_normalization_version, str)
                self._provenance.record_reprocessing(
                    writer, item, result.replayed_fetch_ids, result.replay_normalization_version,
                )
            completed = self._connection.execute(
                "SELECT ops.complete_repeatable_sync_work_item(%s,%s,%s,%s)",
                (item.id, self._owner, item.lease_token, Jsonb(dict(result.checkpoint))),
            ).fetchone()
            if completed is None or completed[0] is not True:
                raise LeaseLost("repeatable work-item lease was lost before completion")
        return True

    def run_registered_once(self, registry: Any, *, max_attempts: int = 5) -> bool:
        """Use the same reviewed registry that allowed the producer to enqueue."""
        return self.run_once(registry, registry.apply_result, max_attempts=max_attempts)

    def _send_heartbeat(self, item: LeasedWorkItem) -> bool:
        with self._heartbeat_connection_factory() as heartbeat_connection:
            with heartbeat_connection.transaction():
                row = heartbeat_connection.execute("SELECT ops.heartbeat_repeatable_sync_work_item(%s,%s,%s,%s::interval)", (item.id, self._owner, item.lease_token, "5 minutes")).fetchone()
                return row is not None and row[0] is True

    def _authorization(self, item: LeasedWorkItem) -> AuthorizedSyncWork:
        policy = item.scope.get("_sync_policy")
        if not isinstance(policy, Mapping):
            raise ValueError("repeatable work item has no policy metadata")
        try:
            request = SyncWorkRequest(int(policy["provider_id"]), int(policy["season_id"]), str(policy["work_type"]))
            current = self._gate.before_enqueue(request)
            authorization = AuthorizedSyncWork(request, int(policy["instance_id"]), int(policy["version"]),
                current.coverage, current.refresh_interval)
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("repeatable work item has malformed policy metadata") from exc
        return self._gate.before_execution(authorization)
