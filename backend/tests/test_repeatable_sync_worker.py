from __future__ import annotations

import pytest

from app.sync.worker import AtomicWorkTransaction


class _Connection:
    def execute(self, query, params=None):
        return query, params


def test_atomic_writer_capability_rejects_commit_rollback_close_and_connection_access() -> None:
    writer = AtomicWorkTransaction(_Connection())  # type: ignore[arg-type]
    assert writer.execute("SELECT 1") == ("SELECT 1", None)
    for forbidden in ("commit", "rollback", "close", "connection"):
        with pytest.raises(AttributeError):
            getattr(writer, forbidden)
