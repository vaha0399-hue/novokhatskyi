from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from app.testing.isolated_resources import (
    IsolatedResourceError,
    load_manifest,
    validate_database_url,
    validate_redis_url,
    validate_test_environment,
)


@pytest.fixture
def manifest_path(tmp_path: Path) -> Path:
    runtime = Path("/tmp") / f"football-analytics-isolated-guard-{tmp_path.name}"
    runtime.mkdir()
    token = "a" * 32
    (runtime / ".fa-test-resource").write_text(token, encoding="utf-8")
    path = runtime / "test-resource.json"
    path.write_text(
        json.dumps(
            {
                "runtime_dir": str(runtime),
                "token": token,
                "postgres_socket": str(runtime / "postgres-socket"),
                "postgres_port": 55490,
                "database_names": ["fa_iso_guard_clean"],
                "redis_socket": str(runtime / "redis.sock"),
            }
        ),
        encoding="utf-8",
    )
    try:
        yield path
    finally:
        shutil.rmtree(runtime)


def test_owned_database_and_redis_urls_are_accepted(manifest_path: Path) -> None:
    manifest = load_manifest(manifest_path)
    validate_database_url(
        "postgresql://postgres@/fa_iso_guard_clean?host=" f"{manifest.postgres_socket}&port=55490",
        manifest,
    )
    validate_redis_url(f"unix://{manifest.redis_socket}", manifest)


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://postgres@/fa_iso_guard_clean?host=/tmp/foreign/.s.PGSQL.55490&port=55490",
        "postgresql://postgres@db.example.test/fa_iso_guard_clean?host=/tmp/unused&port=55490",
        "postgresql://postgres@/fa_iso_not_owned?host=/tmp/unused&port=55490",
    ],
)
def test_foreign_database_url_is_rejected_before_connection(manifest_path: Path, url: str) -> None:
    with pytest.raises(IsolatedResourceError, match="not owned"):
        validate_database_url(url, load_manifest(manifest_path))


@pytest.mark.parametrize("url", ["redis://127.0.0.1:6379/0", "unix:///tmp/foreign/redis.sock"])
def test_foreign_redis_url_is_rejected_before_connection(manifest_path: Path, url: str) -> None:
    with pytest.raises(IsolatedResourceError, match="not owned"):
        validate_redis_url(url, load_manifest(manifest_path))


def test_environment_rejects_configured_url_without_runner_manifest() -> None:
    with pytest.raises(IsolatedResourceError, match="require FA_TEST_RESOURCE_MANIFEST"):
        validate_test_environment({"SUPABASE_DB_URL": "postgresql://postgres@db.example.test/app"})


def test_environment_rejects_direct_redis_url_without_runner_manifest() -> None:
    with pytest.raises(IsolatedResourceError, match="require FA_TEST_RESOURCE_MANIFEST"):
        validate_test_environment({"REDIS_URL": "redis://127.0.0.1:6379/0"})
