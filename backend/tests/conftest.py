"""Fail closed before an integration suite can open an external connection."""

from __future__ import annotations

import os

import pytest

from app.testing.isolated_resources import IsolatedResourceError, validate_test_environment


def pytest_sessionstart(session: pytest.Session) -> None:
    try:
        validate_test_environment(os.environ)
    except IsolatedResourceError as exc:
        pytest.exit(f"unsafe integration-test destination: {exc}", returncode=4)
