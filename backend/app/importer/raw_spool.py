"""Crash-safe local landing zone for pre-canonical provider responses."""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from app.api_football import APIFootballResponse
from app.importer.season_bootstrap import BaseRequest, CollectedBaseResponse


class RawSpoolError(RuntimeError):
    """A staged provider artifact is missing, unsafe, or corrupt."""


@dataclass(frozen=True)
class RawSpoolArtifact:
    request: BaseRequest
    response: APIFootballResponse
    request_started_at: datetime
    response_received_at: datetime
    scope: Mapping[str, Any] | None = None
    work_item_id: int | None = None
    work_item_attempt: int | None = None
    normalization_version: str | None = None


class RawSpool:
    """Stores immutable request/raw pairs with atomic file replacement.

    This is a transient VPS inbox, not a second canonical database.  File
    names come solely from fixed endpoint labels; all dynamic path segments are
    positive numeric IDs supplied by the worker.
    """

    def __init__(self, root: Path, *, max_bytes: int | None = 1024 * 1024 * 1024) -> None:
        if max_bytes is not None and max_bytes <= 0:
            raise ValueError("raw spool maximum must be positive")
        self._root = root
        self._max_bytes = max_bytes

    @property
    def root(self) -> Path:
        return self._root

    def capture_directory(self, *, run_id: int, league_external_id: int, season_start_year: int, generation: int) -> Path:
        if min(run_id, league_external_id, season_start_year, generation) <= 0:
            raise ValueError("raw spool identifiers must be positive")
        return self._root / f"run-{run_id}" / f"league-{league_external_id}-season-{season_start_year}" / f"generation-{generation}"

    def work_item_directory(self, *, work_item_id: int, attempt: int) -> Path:
        """Return the private transient directory for one fenced work attempt."""
        if work_item_id <= 0 or attempt <= 0:
            raise ValueError("work-item spool identifiers must be positive")
        return self._root / "repeatable" / f"work-item-{work_item_id}" / f"attempt-{attempt}"

    def work_item_request_directory(self, *, work_item_id: int, attempt: int, request_number: int) -> Path:
        """Keep every physical provider request separate within one attempt."""
        if request_number <= 0:
            raise ValueError("raw spool request number must be positive")
        return self.work_item_directory(work_item_id=work_item_id, attempt=attempt) / f"request-{request_number:06d}"

    @staticmethod
    def _label(endpoint: str) -> str:
        labels = {"/leagues": "leagues", "/teams": "teams", "/standings": "standings", "/fixtures": "fixtures"}
        if endpoint in labels:
            return labels[endpoint]
        if not endpoint.startswith("/") or any(character.isspace() for character in endpoint):
            raise RawSpoolError("unsafe raw spool endpoint")
        # The endpoint itself remains in metadata.  A digest makes a safe,
        # deterministic filename for Q03 work types beyond the bootstrap four.
        return f"endpoint-{hashlib.sha256(endpoint.encode()).hexdigest()[:20]}"

    @staticmethod
    def _write_atomic(path: Path, content: bytes) -> None:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(path.parent, 0o700)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
            os.chmod(path, 0o600)
            directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    def stage(self, directory: Path, artifact: RawSpoolArtifact) -> None:
        label = self._label(artifact.request.endpoint)
        raw_path = directory / f"{label}.raw.json"
        request_path = directory / f"{label}.request.json"
        raw = artifact.response.raw_body
        if artifact.work_item_id is not None and artifact.work_item_id <= 0:
            raise RawSpoolError("raw spool work item is invalid")
        if artifact.work_item_attempt is not None and artifact.work_item_attempt <= 0:
            raise RawSpoolError("raw spool work attempt is invalid")
        if (artifact.work_item_id is None) != (artifact.work_item_attempt is None):
            raise RawSpoolError("raw spool work provenance is incomplete")
        if artifact.normalization_version is not None and not artifact.normalization_version.strip():
            raise RawSpoolError("raw spool normalization version is invalid")
        metadata: dict[str, Any] = {
            "endpoint": artifact.request.endpoint,
            "parameters": dict(artifact.request.params),
            "http_status": artifact.response.status_code,
            "byte_count": len(raw),
            "content_sha256": hashlib.sha256(raw).hexdigest(),
            "request_started_at": artifact.request_started_at.isoformat(),
            "response_received_at": artifact.response_received_at.isoformat(),
        }
        if artifact.scope is not None:
            metadata["scope"] = dict(artifact.scope)
        if artifact.work_item_id is not None:
            metadata["work_item_id"] = artifact.work_item_id
            metadata["work_item_attempt"] = artifact.work_item_attempt
        if artifact.normalization_version is not None:
            metadata["normalization_version"] = artifact.normalization_version
        # A raw body is durable only once both files are atomically present.
        # Replacing an existing artifact is forbidden: one capture generation
        # represents a coherent source observation.
        if raw_path.exists() or request_path.exists():
            loaded = self.load(directory, artifact.request)
            if loaded.response.raw_body != raw:
                raise RawSpoolError("capture generation already contains a different provider response")
            return
        self._write_atomic(raw_path, raw)
        self._write_atomic(request_path, json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode())

    def load(self, directory: Path, request: BaseRequest) -> RawSpoolArtifact | None:
        label = self._label(request.endpoint)
        raw_path = directory / f"{label}.raw.json"
        request_path = directory / f"{label}.request.json"
        if not raw_path.exists() and not request_path.exists():
            return None
        if not raw_path.is_file() or not request_path.is_file():
            raise RawSpoolError("partial raw spool artifact")
        try:
            metadata = json.loads(request_path.read_text(encoding="utf-8"))
            raw = raw_path.read_bytes()
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RawSpoolError("raw spool artifact is unreadable") from error
        if not isinstance(metadata, Mapping):
            raise RawSpoolError("raw spool metadata is invalid")
        if metadata.get("endpoint") != request.endpoint or metadata.get("parameters") != dict(request.params):
            raise RawSpoolError("raw spool request scope mismatch")
        if metadata.get("http_status") != 200 or metadata.get("byte_count") != len(raw):
            raise RawSpoolError("raw spool response metadata is invalid")
        digest = metadata.get("content_sha256")
        if not isinstance(digest, str) or hashlib.sha256(raw).hexdigest() != digest:
            raise RawSpoolError("raw spool content checksum mismatch")
        try:
            payload = json.loads(raw)
            started = datetime.fromisoformat(str(metadata["request_started_at"]).replace("Z", "+00:00"))
            received = datetime.fromisoformat(str(metadata["response_received_at"]).replace("Z", "+00:00"))
        except (KeyError, ValueError, json.JSONDecodeError) as error:
            raise RawSpoolError("raw spool content or timestamps are invalid") from error
        if not isinstance(payload, dict) or started.tzinfo is None or received.tzinfo is None or received < started:
            raise RawSpoolError("raw spool response contract is invalid")
        scope = metadata.get("scope")
        if scope is not None and not isinstance(scope, Mapping):
            raise RawSpoolError("raw spool scope is invalid")
        work_item_id, work_item_attempt = metadata.get("work_item_id"), metadata.get("work_item_attempt")
        if (work_item_id is None) != (work_item_attempt is None) or (
            work_item_id is not None and (not isinstance(work_item_id, int) or not isinstance(work_item_attempt, int)
                                      or work_item_id <= 0 or work_item_attempt <= 0)
        ):
            raise RawSpoolError("raw spool work provenance is invalid")
        normalization_version = metadata.get("normalization_version")
        if normalization_version is not None and (not isinstance(normalization_version, str) or not normalization_version.strip()):
            raise RawSpoolError("raw spool normalization version is invalid")
        return RawSpoolArtifact(
            request,
            APIFootballResponse(payload, raw, 200, {}),
            started,
            received,
            dict(scope) if scope is not None else None,
            work_item_id,
            work_item_attempt,
            normalization_version,
        )

    def mark_durable(self, directory: Path) -> None:
        """Mark staged bytes as safely replayable from durable provenance."""
        if self._root not in directory.parents or not directory.is_dir():
            raise RawSpoolError("refusing to mark raw spool outside its root")
        self._write_atomic(directory / ".durable", b"durable\n")
        self.enforce_limit()

    def enforce_limit(self) -> None:
        """Evict only DB-backed captures; never an uncommitted replay input."""
        if self._max_bytes is None or not self._root.exists():
            return
        def size() -> int:
            return sum(path.stat().st_size for path in self._root.rglob("*") if path.is_file() and not path.is_symlink())
        total = size()
        if total <= self._max_bytes:
            return
        candidates = sorted(
            (marker.parent for marker in self._root.rglob(".durable") if marker.is_file() and not marker.is_symlink()),
            key=lambda path: path.stat().st_mtime,
        )
        for directory in candidates:
            if total <= self._max_bytes:
                return
            self.purge_generation(directory)
            total = size()
        if total > self._max_bytes:
            raise RawSpoolError("raw spool limit reached by unrecoverable captures")

    def discard_partial(self, directory: Path, request: BaseRequest) -> bool:
        """Recover only an interrupted single-endpoint write before refetching it."""
        label = self._label(request.endpoint)
        raw_path = directory / f"{label}.raw.json"
        request_path = directory / f"{label}.request.json"
        raw_exists, request_exists = raw_path.exists(), request_path.exists()
        if raw_exists == request_exists:
            return False
        for path in (raw_path, request_path):
            if path.exists():
                if not path.is_file() or path.is_symlink():
                    raise RawSpoolError("unsafe partial raw spool artifact")
                path.unlink()
        return True

    def purge_generation(self, directory: Path) -> None:
        """Delete only a known completed capture directory, never the spool root."""
        if self._root not in directory.parents or not directory.is_dir():
            raise RawSpoolError("refusing to purge outside raw spool")
        for item in directory.iterdir():
            if item.is_file() or item.is_symlink():
                item.unlink()
            else:
                raise RawSpoolError("raw spool generation has an unexpected nested directory")
        directory.rmdir()

    def latest_catalogue(self) -> RawSpoolArtifact | None:
        """Recover a staged catalogue after a crash before queue creation."""
        directory = self._root / "catalogue"
        if not directory.is_dir():
            return None
        request = BaseRequest("/leagues", {})
        candidates = sorted((path for path in directory.iterdir() if path.is_dir() and (path / ".queue-pending").is_file()), key=lambda path: path.stat().st_mtime, reverse=True)
        for candidate in candidates:
            try:
                value = self.load(candidate, request)
            except RawSpoolError:
                continue
            if value is not None:
                return value
        return None

    def mark_catalogue_pending(self, directory: Path) -> None:
        self._write_atomic(directory / ".queue-pending", b"pending\n")

    def consume_pending_catalogues(self) -> None:
        directory = self._root / "catalogue"
        if not directory.is_dir():
            return
        for candidate in directory.iterdir():
            marker = candidate / ".queue-pending"
            if candidate.is_dir() and marker.is_file() and not marker.is_symlink():
                marker.unlink()
