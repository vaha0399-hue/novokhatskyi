from __future__ import annotations

import json
import shutil
from pathlib import Path

import psycopg
import pytest

from app.testing.isolated_resources import (
    IsolatedResourceError,
    load_manifest,
    validate_database_url,
    validate_redis_url,
    validate_test_environment,
)
from conftest import pytest_sessionstart


@pytest.fixture
def manifest_path(tmp_path: Path) -> Path:
    runtime = Path("/tmp") / f"football-analytics-isolated-guard-{tmp_path.name}"
    runtime.mkdir()
    (runtime / "postgres-socket").mkdir()
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


def test_manifest_rejects_missing_ownership_marker(manifest_path: Path) -> None:
    (manifest_path.parent / ".fa-test-resource").unlink()
    with pytest.raises(IsolatedResourceError, match="marker is missing"):
        load_manifest(manifest_path)


def test_manifest_rejects_wrong_ownership_marker(manifest_path: Path) -> None:
    (manifest_path.parent / ".fa-test-resource").write_text("b" * 32, encoding="utf-8")
    with pytest.raises(IsolatedResourceError, match="does not match"):
        load_manifest(manifest_path)


def test_manifest_rejects_missing_file(manifest_path: Path) -> None:
    manifest_path.unlink()
    with pytest.raises(IsolatedResourceError, match="cannot read"):
        load_manifest(manifest_path)


def test_environment_rejects_missing_manifest_for_owned_looking_url() -> None:
    with pytest.raises(IsolatedResourceError, match="require FA_TEST_RESOURCE_MANIFEST"):
        validate_test_environment(
            {"READ_API_TEST_DB_URL": "postgresql://postgres@/fa_iso_looks_owned?host=/tmp/x&port=55490"}
        )


@pytest.mark.parametrize(
    "variable",
    ["REPEATABLE_SYNC_QUEUE_TEST_DB_URL", "Q06_PROVENANCE_TEST_DB_URL"],
)
def test_database_url_rejects_missing_manifest_before_psycopg_connect(
    variable: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    connects: list[tuple[object, ...]] = []
    monkeypatch.setattr(psycopg, "connect", lambda *args, **kwargs: connects.append(args))
    for environment_variable in (
        "SUPABASE_DB_URL",
        "ACTIVE_SEASON_TEST_DB_URL",
        "ANALYTICS_TEST_DB_URL",
        "BACKFILL_TEST_DB_URL",
        "CURRENT_SEASON_STATISTICS_TEST_DB_URL",
        "HISTORICAL_LINEUPS_TEST_DB_URL",
        "LIVE_DOMAIN_TEST_DB_URL",
        "LIVE_WORKER_TEST_DB_URL",
        "COMPETITION_SYNC_POLICIES_TEST_DB_URL",
        "READ_API_TEST_DB_URL",
        "Q06_PROVENANCE_TEST_DB_URL",
        "REPEATABLE_SYNC_QUEUE_TEST_DB_URL",
        "SEASON_BOOTSTRAP_TEST_DB_URL",
        "LIVE_REDIS_TEST_URL",
        "REDIS_URL",
        "FA_TEST_RESOURCE_MANIFEST",
    ):
        monkeypatch.delenv(environment_variable, raising=False)
    monkeypatch.setenv(
        variable,
        "postgresql://postgres@/fa_iso_guard_clean?host=/tmp/x&port=55490",
    )

    with pytest.raises(pytest.exit.Exception, match="require FA_TEST_RESOURCE_MANIFEST"):
        pytest_sessionstart(None)  # type: ignore[arg-type]
    assert connects == []


@pytest.mark.parametrize(
    "variable",
    ["REPEATABLE_SYNC_QUEUE_TEST_DB_URL", "Q06_PROVENANCE_TEST_DB_URL"],
)
def test_database_url_rejects_external_database_before_psycopg_connect(
    variable: str, manifest_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connects: list[tuple[object, ...]] = []
    monkeypatch.setattr(psycopg, "connect", lambda *args, **kwargs: connects.append(args))
    for environment_variable in (
        "SUPABASE_DB_URL",
        "ACTIVE_SEASON_TEST_DB_URL",
        "ANALYTICS_TEST_DB_URL",
        "BACKFILL_TEST_DB_URL",
        "CURRENT_SEASON_STATISTICS_TEST_DB_URL",
        "HISTORICAL_LINEUPS_TEST_DB_URL",
        "LIVE_DOMAIN_TEST_DB_URL",
        "LIVE_WORKER_TEST_DB_URL",
        "COMPETITION_SYNC_POLICIES_TEST_DB_URL",
        "READ_API_TEST_DB_URL",
        "Q06_PROVENANCE_TEST_DB_URL",
        "REPEATABLE_SYNC_QUEUE_TEST_DB_URL",
        "SEASON_BOOTSTRAP_TEST_DB_URL",
        "LIVE_REDIS_TEST_URL",
        "REDIS_URL",
    ):
        monkeypatch.delenv(environment_variable, raising=False)
    monkeypatch.setenv("FA_TEST_RESOURCE_MANIFEST", str(manifest_path))
    monkeypatch.setenv(variable, "postgresql://postgres@db.example.test/app")

    with pytest.raises(pytest.exit.Exception, match="not owned"):
        pytest_sessionstart(None)  # type: ignore[arg-type]
    assert connects == []


@pytest.mark.parametrize(
    "variable",
    ["REPEATABLE_SYNC_QUEUE_TEST_DB_URL", "Q06_PROVENANCE_TEST_DB_URL"],
)
def test_database_url_rejects_foreign_temporary_database_before_psycopg_connect(
    variable: str, manifest_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connects: list[tuple[object, ...]] = []
    monkeypatch.setattr(psycopg, "connect", lambda *args, **kwargs: connects.append(args))
    for environment_variable in (
        "SUPABASE_DB_URL",
        "ACTIVE_SEASON_TEST_DB_URL",
        "ANALYTICS_TEST_DB_URL",
        "BACKFILL_TEST_DB_URL",
        "CURRENT_SEASON_STATISTICS_TEST_DB_URL",
        "HISTORICAL_LINEUPS_TEST_DB_URL",
        "LIVE_DOMAIN_TEST_DB_URL",
        "LIVE_WORKER_TEST_DB_URL",
        "COMPETITION_SYNC_POLICIES_TEST_DB_URL",
        "READ_API_TEST_DB_URL",
        "Q06_PROVENANCE_TEST_DB_URL",
        "REPEATABLE_SYNC_QUEUE_TEST_DB_URL",
        "SEASON_BOOTSTRAP_TEST_DB_URL",
        "LIVE_REDIS_TEST_URL",
        "REDIS_URL",
    ):
        monkeypatch.delenv(environment_variable, raising=False)
    manifest = load_manifest(manifest_path)
    monkeypatch.setenv("FA_TEST_RESOURCE_MANIFEST", str(manifest_path))
    monkeypatch.setenv(
        variable,
        f"postgresql://postgres@/fa_foreign_temporary?host={manifest.postgres_socket}&port={manifest.postgres_port}",
    )

    with pytest.raises(pytest.exit.Exception, match="not owned"):
        pytest_sessionstart(None)  # type: ignore[arg-type]
    assert connects == []


def test_q06_provenance_url_from_manifest_is_accepted_without_psycopg_connect(
    manifest_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    connects: list[tuple[object, ...]] = []
    monkeypatch.setattr(psycopg, "connect", lambda *args, **kwargs: connects.append(args))
    for environment_variable in (
        "SUPABASE_DB_URL",
        "ACTIVE_SEASON_TEST_DB_URL",
        "ANALYTICS_TEST_DB_URL",
        "BACKFILL_TEST_DB_URL",
        "CURRENT_SEASON_STATISTICS_TEST_DB_URL",
        "HISTORICAL_LINEUPS_TEST_DB_URL",
        "LIVE_DOMAIN_TEST_DB_URL",
        "LIVE_WORKER_TEST_DB_URL",
        "COMPETITION_SYNC_POLICIES_TEST_DB_URL",
        "READ_API_TEST_DB_URL",
        "Q06_PROVENANCE_TEST_DB_URL",
        "REPEATABLE_SYNC_QUEUE_TEST_DB_URL",
        "SEASON_BOOTSTRAP_TEST_DB_URL",
        "LIVE_REDIS_TEST_URL",
        "REDIS_URL",
    ):
        monkeypatch.delenv(environment_variable, raising=False)
    manifest = load_manifest(manifest_path)
    monkeypatch.setenv("FA_TEST_RESOURCE_MANIFEST", str(manifest_path))
    monkeypatch.setenv(
        "Q06_PROVENANCE_TEST_DB_URL",
        f"postgresql://postgres@/fa_iso_guard_clean?host={manifest.postgres_socket}&port={manifest.postgres_port}",
    )

    pytest_sessionstart(None)  # type: ignore[arg-type]
    assert connects == []


@pytest.mark.parametrize(
    "variable",
    ["PGHOSTADDR", "PGSERVICE", "PGSERVICEFILE", "PGPASSFILE", "PGPASSWORD"],
)
def test_environment_rejects_inherited_libpq_connection_setting(variable: str) -> None:
    with pytest.raises(IsolatedResourceError, match="libpq"):
        validate_test_environment({variable: "external-value"})
