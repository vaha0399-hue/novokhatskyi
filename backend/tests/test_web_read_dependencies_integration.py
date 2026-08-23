"""Read-only transaction assertion, enabled only for an explicit test DB URL."""

from __future__ import annotations

import os

import pytest

from app.web.dependencies import get_read_connection


TEST_DB_URL = os.environ.get("READ_API_TEST_DB_URL")
pytestmark = pytest.mark.skipif(not TEST_DB_URL, reason="READ_API_TEST_DB_URL is not configured")


def test_web_dependency_opens_a_read_only_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    assert TEST_DB_URL is not None
    monkeypatch.setenv("SUPABASE_DB_URL", TEST_DB_URL)
    generator = get_read_connection()
    connection = next(generator)
    try:
        assert connection.execute("SHOW transaction_read_only").fetchone()[0] == "on"
    finally:
        generator.close()
