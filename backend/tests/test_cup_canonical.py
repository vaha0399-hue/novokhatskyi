from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace

from app.importer.active_season import CupSeasonScope
from app.importer.cup_canonical import CupCanonicalSink, import_cup_base
from app.importer.season_bootstrap import BaseRequest, CollectedBaseResponse


@dataclass
class _Transaction:
    entered: bool = False

    def __enter__(self) -> None:
        self.entered = True

    def __exit__(self, *_: object) -> None:
        return None


class _Connection:
    def __init__(self) -> None:
        self.transaction_context = _Transaction()
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []

    def transaction(self) -> _Transaction:
        return self.transaction_context

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> SimpleNamespace:
        self.calls.append((query, params))
        return SimpleNamespace(fetchone=lambda: (1,))


def _collected() -> tuple[CollectedBaseResponse, ...]:
    now = datetime(2026, 9, 6, tzinfo=UTC)
    return tuple(
        CollectedBaseResponse(
            BaseRequest(endpoint, {"league": 2, "season": 2026}),
            SimpleNamespace(),  # helpers are patched before payload inspection.
            now,
            now,
        )
        for endpoint in ("/leagues", "/teams", "/standings", "/fixtures")
    )


def test_import_cup_base_validates_before_opening_transaction(monkeypatch) -> None:
    import app.importer.cup_canonical as subject

    conn = _Connection()
    scope = CupSeasonScope(2, 2026)
    collected = _collected()
    validated = SimpleNamespace(scope=scope)
    observed: list[object] = []

    def validate(items, *, scope):
        observed.append((items, scope, conn.transaction_context.entered))
        return validated

    monkeypatch.setattr(subject, "validate_cup_base_responses", validate)

    assert import_cup_base(
        conn,
        collected=collected,
        scope=scope,
        write_validated_base=lambda _conn, actual, _collected: actual,
    ) is validated
    assert observed == [(collected, scope, False)]


def test_sink_delegates_prevalidated_payload_without_revalidation(monkeypatch) -> None:
    import app.importer.cup_canonical as subject

    conn = _Connection()
    validated = SimpleNamespace(scope=CupSeasonScope(2, 2026))
    collected = _collected()
    observed: list[tuple[object, object, object]] = []

    def persist(actual_conn, validated, collected):
        observed.append((actual_conn, validated, collected))
        return "context"

    assert CupCanonicalSink(conn, write_validated_base=persist).write_cup_base(
        validated=validated, collected=collected
    ) is None
    assert observed == [(conn, validated, collected)]
