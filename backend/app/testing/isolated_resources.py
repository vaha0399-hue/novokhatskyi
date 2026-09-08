"""Validate that integration-test URLs point at a run-owned local resource.

The checks are deliberately structural: a database name containing ``test`` is
not proof that it is disposable.  A caller must supply a manifest created by
the isolated test runner, and every URL must match its private Unix socket,
port, and explicitly created database names before a connection is opened.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit


class IsolatedResourceError(RuntimeError):
    """Raised before a test could connect to a non-owned resource."""


_DATABASE_ENVIRONMENT_VARIABLES = (
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
    "REPEATABLE_SYNC_QUEUE_TEST_DB_URL",
    "SEASON_BOOTSTRAP_TEST_DB_URL",
)
_REDIS_ENVIRONMENT_VARIABLES = ("LIVE_REDIS_TEST_URL", "REDIS_URL")
_LIBPQ_ENVIRONMENT_VARIABLES = (
    "PGHOST",
    "PGHOSTADDR",
    "PGPORT",
    "PGDATABASE",
    "PGUSER",
    "PGPASSWORD",
    "PGPASSFILE",
    "PGSERVICE",
    "PGSERVICEFILE",
    "PGOPTIONS",
)
_RUNTIME_PREFIX = "/tmp/football-analytics-"


@dataclass(frozen=True)
class IsolatedResourceManifest:
    runtime_dir: Path
    token: str
    postgres_socket: Path
    postgres_port: int
    database_names: frozenset[str]
    redis_socket: Path


def _safe_runtime_dir(value: object) -> Path:
    if not isinstance(value, str):
        raise IsolatedResourceError("test resource manifest has no runtime_dir")
    runtime_dir = Path(value).resolve()
    if not str(runtime_dir).startswith(_RUNTIME_PREFIX):
        raise IsolatedResourceError("test resource runtime is outside the allowed temporary prefix")
    if not runtime_dir.is_dir():
        raise IsolatedResourceError("test resource runtime directory does not exist")
    return runtime_dir


def load_manifest(path_value: str | os.PathLike[str]) -> IsolatedResourceManifest:
    """Load a runner-owned manifest and confirm its nonce marker."""
    manifest_path = Path(path_value).resolve()
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IsolatedResourceError("cannot read isolated test resource manifest") from exc
    if not isinstance(payload, dict):
        raise IsolatedResourceError("test resource manifest must be an object")

    runtime_dir = _safe_runtime_dir(payload.get("runtime_dir"))
    if manifest_path.parent != runtime_dir:
        raise IsolatedResourceError("test resource manifest is not inside its runtime directory")
    token = payload.get("token")
    if not isinstance(token, str) or len(token) < 16:
        raise IsolatedResourceError("test resource manifest token is invalid")
    try:
        marker_token = (runtime_dir / ".fa-test-resource").read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise IsolatedResourceError("test resource ownership marker is missing") from exc
    if marker_token != token:
        raise IsolatedResourceError("test resource ownership marker does not match manifest")

    socket_value = payload.get("postgres_socket")
    redis_value = payload.get("redis_socket")
    port = payload.get("postgres_port")
    databases = payload.get("database_names")
    if not isinstance(socket_value, str) or not isinstance(redis_value, str):
        raise IsolatedResourceError("test resource socket path is invalid")
    if not isinstance(port, int) or not 1024 <= port <= 65535:
        raise IsolatedResourceError("test resource PostgreSQL port is invalid")
    if not isinstance(databases, list) or not databases or not all(
        isinstance(name, str) and name.startswith("fa_") for name in databases
    ):
        raise IsolatedResourceError("test resource database allowlist is invalid")
    postgres_socket = Path(socket_value)
    redis_socket = Path(redis_value)
    if postgres_socket.parent.resolve() != runtime_dir or not postgres_socket.is_dir():
        raise IsolatedResourceError("PostgreSQL socket directory is outside the test runtime")
    if redis_socket.parent.resolve() != runtime_dir:
        raise IsolatedResourceError("Redis socket is outside the test runtime")
    return IsolatedResourceManifest(
        runtime_dir=runtime_dir,
        token=token,
        postgres_socket=postgres_socket,
        postgres_port=port,
        database_names=frozenset(databases),
        redis_socket=redis_socket,
    )


def validate_database_url(url: str, manifest: IsolatedResourceManifest) -> None:
    """Reject an unowned PostgreSQL URL without opening a connection."""
    parsed = urlsplit(url)
    database_name = parsed.path.removeprefix("/")
    expected_query = [("host", str(manifest.postgres_socket)), ("port", str(manifest.postgres_port))]
    if (
        parsed.scheme != "postgresql"
        or parsed.hostname is not None
        or parsed.username != "postgres"
        or parsed.password is not None
        or database_name not in manifest.database_names
        or parse_qsl(parsed.query, keep_blank_values=True) != expected_query
    ):
        raise IsolatedResourceError("database URL is not owned by this isolated test run")


def validate_redis_url(url: str, manifest: IsolatedResourceManifest) -> None:
    """Reject TCP and foreign Unix-socket Redis URLs before connection."""
    parsed = urlsplit(url)
    if (
        parsed.scheme != "unix"
        or parsed.netloc
        or parsed.path != str(manifest.redis_socket)
        or parsed.query
    ):
        raise IsolatedResourceError("Redis URL is not owned by this isolated test run")


def validate_test_environment(environ: dict[str, str] | os._Environ[str]) -> None:
    """Validate every configured integration destination before pytest starts."""
    for variable in _LIBPQ_ENVIRONMENT_VARIABLES:
        if environ.get(variable):
            raise IsolatedResourceError(f"unsafe inherited libpq setting: {variable}")
    configured = {
        name: environ[name]
        for name in (*_DATABASE_ENVIRONMENT_VARIABLES, *_REDIS_ENVIRONMENT_VARIABLES)
        if environ.get(name)
    }
    if not configured:
        return
    manifest_path = environ.get("FA_TEST_RESOURCE_MANIFEST")
    if not manifest_path:
        raise IsolatedResourceError("integration URLs require FA_TEST_RESOURCE_MANIFEST")
    manifest = load_manifest(manifest_path)
    for name, url in configured.items():
        if name in _DATABASE_ENVIRONMENT_VARIABLES:
            validate_database_url(url, manifest)
        else:
            validate_redis_url(url, manifest)
