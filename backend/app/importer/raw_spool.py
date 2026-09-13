"""Crash-safe local landing zone for pre-canonical provider responses.

All filesystem operations use the root lock; DB verification is performed while
that short lock is held, and HTTP is never part of the critical section.  Purge
order is root lock, then DB ``FOR SHARE`` locks, then atomic journal publication
and unlink.  The DB transaction remains open through the final unlink.  A
verified purge first renames its directory to ``.purging-*`` so an interrupted
cleanup remains visible and is never mistaken for replay input; restart treats
the journal as an input to re-verification, never as deletion authority.
"""

from __future__ import annotations

import hashlib
import json
import os
import fcntl
import re
import uuid
from contextlib import nullcontext
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, ContextManager

from app.api_football import APIFootballResponse
from app.importer.season_bootstrap import BaseRequest, CollectedBaseResponse


class RawSpoolError(RuntimeError):
    """A staged provider artifact is missing, unsafe, or corrupt."""


class RawSpoolCapacityError(RawSpoolError):
    """The spool cannot accept more bytes without deleting protected data."""


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
    purpose: str | None = None
    retention_class: str | None = None
    physical_request_id: str | None = None


class RawSpool:
    """Stores immutable request/raw pairs with atomic file replacement.

    This is a transient VPS inbox, not a second canonical database.  File
    names come solely from fixed endpoint labels; all dynamic path segments are
    positive numeric IDs supplied by the worker.
    """

    _CLEANUP_PROOF = ".cleanup-proof.json"

    def __init__(self, root: Path, *, max_bytes: int | None = 1024 * 1024 * 1024, purge_verifier: Callable[[Path], bool] | None = None) -> None:
        if max_bytes is not None and max_bytes <= 0:
            raise ValueError("raw spool maximum must be positive")
        self._root = root
        self._max_bytes = max_bytes
        self._purge_verifier = purge_verifier
        self._purge_guard: Callable[[Path], ContextManager[bool]] | None = None

    @property
    def root(self) -> Path:
        return self._root

    def set_purge_verifier(self, verifier: Callable[[Path], bool] | None) -> None:
        self._purge_verifier = verifier

    def set_purge_guard(self, guard: Callable[[Path], ContextManager[bool]] | None) -> None:
        """Install the DB lock guard used only while verified files are unlinked."""
        self._purge_guard = guard

    @staticmethod
    def _absolute_path(path: Path) -> Path:
        """Normalize lexically without resolving (and following) symlinks."""
        return Path(os.path.normpath(os.path.abspath(os.fspath(path))))

    @staticmethod
    def _reject_symlink_components(path: Path) -> None:
        current = Path(path.anchor)
        for part in path.parts[1:]:
            current /= part
            try:
                if current.is_symlink():
                    raise RawSpoolError("raw spool path contains a symlink")
            except OSError as error:
                raise RawSpoolError("raw spool path cannot be inspected") from error
            if not current.exists():
                break

    def _root_path(self) -> Path:
        root = self._absolute_path(self._root)
        self._reject_symlink_components(root)
        return root

    def _validate_directory(self, directory: Path) -> None:
        root = self._root_path()
        candidate = self._absolute_path(directory)
        if candidate == root or root not in candidate.parents:
            raise RawSpoolError("refusing to access path outside raw spool")
        self._reject_symlink_components(candidate)

    class _Lock:
        def __init__(self, spool: "RawSpool") -> None:
            self.spool = spool
            self.handle: Any = None

        def __enter__(self) -> "RawSpool._Lock":
            root = self.spool._root_path()
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            self.spool._reject_symlink_components(root)
            try:
                root_descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            except OSError as error:
                raise RawSpoolError("raw spool root cannot be opened safely") from error
            try:
                lock_descriptor = os.open(
                    ".spool.lock",
                    os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=root_descriptor,
                )
            except OSError as error:
                raise RawSpoolError("raw spool lock cannot be opened safely") from error
            finally:
                os.close(root_descriptor)
            self.handle = os.fdopen(lock_descriptor, "a+b")
            os.chmod(root / ".spool.lock", 0o600, follow_symlinks=False)
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
            return self

        def __exit__(self, *_: object) -> None:
            assert self.handle is not None
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
            self.handle.close()

    def _lock(self) -> "RawSpool._Lock":
        return RawSpool._Lock(self)

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

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        """Persist an already-completed namespace change without following links."""
        try:
            descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        except OSError as error:
            raise RawSpoolError("raw spool directory cannot be synchronized") from error
        try:
            os.fsync(descriptor)
        except OSError as error:
            raise RawSpoolError("raw spool directory cannot be synchronized") from error
        finally:
            os.close(descriptor)

    def stage(self, directory: Path, artifact: RawSpoolArtifact) -> None:
        self._validate_directory(directory)
        with self._lock():
            self._stage_unlocked(directory, artifact)

    def _stage_unlocked(self, directory: Path, artifact: RawSpoolArtifact) -> None:
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
        if artifact.purpose is not None and artifact.purpose not in {
            "bootstrap", "scheduled_refresh", "prematch", "postmatch_reconciliation", "research",
        }:
            raise RawSpoolError("raw spool purpose is invalid")
        if artifact.retention_class is not None and artifact.retention_class not in {
            "standard", "anomaly", "prediction_input", "contract_sample",
        }:
            raise RawSpoolError("raw spool retention class is invalid")
        if artifact.physical_request_id is not None and not artifact.physical_request_id.strip():
            raise RawSpoolError("raw spool physical request identity is invalid")
        metadata: dict[str, Any] = {
            "endpoint": artifact.request.endpoint,
            "parameters": dict(artifact.request.params),
            "http_status": artifact.response.status_code,
            "byte_count": len(raw),
            "content_sha256": hashlib.sha256(raw).hexdigest(),
            "request_started_at": artifact.request_started_at.isoformat(),
            "response_received_at": artifact.response_received_at.isoformat(),
        }
        metadata["cleanup_manifest"] = {
            "version": 1,
            "files": (f"{label}.raw.json", f"{label}.request.json", ".durable"),
            "raw_sha256": metadata["content_sha256"],
            "raw_byte_count": len(raw),
        }
        if artifact.scope is not None:
            metadata["scope"] = dict(artifact.scope)
        if artifact.work_item_id is not None:
            metadata["work_item_id"] = artifact.work_item_id
            metadata["work_item_attempt"] = artifact.work_item_attempt
        if artifact.normalization_version is not None:
            metadata["normalization_version"] = artifact.normalization_version
        if artifact.purpose is not None:
            metadata["purpose"] = artifact.purpose
        if artifact.retention_class is not None:
            metadata["retention_class"] = artifact.retention_class
        if artifact.physical_request_id is not None:
            metadata["physical_request_id"] = artifact.physical_request_id
        # A raw body is durable only once both files are atomically present.
        # Replacing an existing artifact is forbidden: one capture generation
        # represents a coherent source observation.
        if raw_path.exists() or request_path.exists():
            loaded = self._load_unlocked(directory, artifact.request)
            if (
                loaded is None
                or loaded.response.raw_body != raw
                or loaded.purpose != artifact.purpose
                or loaded.retention_class != artifact.retention_class
                or loaded.physical_request_id != artifact.physical_request_id
            ):
                raise RawSpoolError("capture generation already contains a different provider response")
            return
        metadata_bytes = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()
        # Each atomic temp file has the same payload size as its final file;
        # the reservation therefore covers the peak raw+metadata footprint.
        self._ensure_capacity(len(raw) + len(metadata_bytes))
        try:
            self._write_atomic(raw_path, raw)
            self._write_atomic(request_path, metadata_bytes)
        except BaseException:
            raw_path.unlink(missing_ok=True)
            request_path.unlink(missing_ok=True)
            raise

    def load(self, directory: Path, request: BaseRequest) -> RawSpoolArtifact | None:
        self._validate_directory(directory)
        with self._lock():
            return self._load_unlocked(directory, request)

    def _load_unlocked(self, directory: Path, request: BaseRequest) -> RawSpoolArtifact | None:
        label = self._label(request.endpoint)
        raw_path = directory / f"{label}.raw.json"
        request_path = directory / f"{label}.request.json"
        if not raw_path.exists() and not request_path.exists():
            return None
        if (
            not raw_path.is_file()
            or raw_path.is_symlink()
            or not request_path.is_file()
            or request_path.is_symlink()
        ):
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
        purpose = metadata.get("purpose")
        if purpose is not None and purpose not in {
            "bootstrap", "scheduled_refresh", "prematch", "postmatch_reconciliation", "research",
        }:
            raise RawSpoolError("raw spool purpose is invalid")
        retention_class = metadata.get("retention_class")
        if retention_class is not None and retention_class not in {
            "standard", "anomaly", "prediction_input", "contract_sample",
        }:
            raise RawSpoolError("raw spool retention class is invalid")
        physical_request_id = metadata.get("physical_request_id")
        if physical_request_id is not None and (not isinstance(physical_request_id, str) or not physical_request_id.strip()):
            raise RawSpoolError("raw spool physical request identity is invalid")
        return RawSpoolArtifact(
            request,
            APIFootballResponse(payload, raw, 200, {}),
            started,
            received,
            dict(scope) if scope is not None else None,
            work_item_id,
            work_item_attempt,
            normalization_version,
            purpose,
            retention_class,
            physical_request_id,
        )

    def work_item_artifacts(self, *, work_item_id: int) -> tuple[tuple[Path, RawSpoolArtifact], ...]:
        """Return verified, pre-commit captures from prior attempts of one work item."""
        if work_item_id <= 0:
            raise ValueError("raw spool work item id must be positive")
        work_directory = self._root / "repeatable" / f"work-item-{work_item_id}"
        self._validate_directory(work_directory)
        with self._lock():
            return self._work_item_artifacts_unlocked(work_item_id=work_item_id)

    def _work_item_artifacts_unlocked(self, *, work_item_id: int) -> tuple[tuple[Path, RawSpoolArtifact], ...]:
        work_directory = self._root / "repeatable" / f"work-item-{work_item_id}"
        if not work_directory.exists():
            return ()
        if not work_directory.is_dir() or work_directory.is_symlink():
            raise RawSpoolError("unsafe raw spool work-item directory")

        artifacts: list[tuple[Path, RawSpoolArtifact]] = []
        for attempt_directory in sorted(work_directory.iterdir(), key=lambda path: path.name):
            attempt_prefix = "attempt-"
            attempt_value = attempt_directory.name.removeprefix(attempt_prefix)
            if (
                not attempt_directory.is_dir()
                or attempt_directory.is_symlink()
                or not attempt_directory.name.startswith(attempt_prefix)
                or not attempt_value.isdecimal()
                or int(attempt_value) < 1
            ):
                raise RawSpoolError("unsafe raw spool work attempt directory")
            attempt = int(attempt_value)
            for request_directory in sorted(attempt_directory.iterdir(), key=lambda path: path.name):
                if request_directory.name.startswith(".purging-"):
                    # A prior process may have crashed after quarantine rename;
                    # keep it for inspection and never treat it as replay input.
                    continue
                if (
                    not request_directory.is_dir()
                    or request_directory.is_symlink()
                    or not request_directory.name.startswith("request-")
                    or not request_directory.name.removeprefix("request-").isdecimal()
                ):
                    raise RawSpoolError("unsafe raw spool work request directory")
                durable_marker = request_directory / ".durable"
                if durable_marker.exists():
                    if not durable_marker.is_file() or durable_marker.is_symlink():
                        raise RawSpoolError("unsafe raw spool durable marker")
                    continue
                metadata_files = [
                    path for path in request_directory.iterdir()
                    if path.is_file() and not path.is_symlink() and path.name.endswith(".request.json")
                ]
                if len(metadata_files) != 1:
                    raise RawSpoolError("raw spool work request metadata is missing or ambiguous")
                try:
                    metadata = json.loads(metadata_files[0].read_text(encoding="utf-8"))
                except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise RawSpoolError("raw spool work request metadata is unreadable") from error
                endpoint = metadata.get("endpoint") if isinstance(metadata, Mapping) else None
                params = metadata.get("parameters") if isinstance(metadata, Mapping) else None
                if not isinstance(endpoint, str) or not isinstance(params, Mapping):
                    raise RawSpoolError("raw spool work request metadata is invalid")
                artifact = self._load_unlocked(request_directory, BaseRequest(endpoint, dict(params)))
                if (
                    artifact is None
                    or artifact.work_item_id != work_item_id
                    or artifact.work_item_attempt != attempt
                ):
                    raise RawSpoolError("raw spool work request provenance does not match its directory")
                artifacts.append((request_directory, artifact))
        return tuple(artifacts)

    def mark_durable(self, directory: Path) -> None:
        """Mark staged bytes as safely replayable from durable provenance."""
        self._validate_directory(directory)
        if not directory.is_dir():
            raise RawSpoolError("refusing to mark raw spool outside its root")
        with self._lock():
            self._ensure_capacity(len(b"durable\n"))
            self._write_atomic(directory / ".durable", b"durable\n")
            self._enforce_limit_unlocked()

    def _ensure_capacity(self, additional: int) -> None:
        if self._max_bytes is None:
            return
        self._enforce_limit_unlocked(required=additional)

    def _size_unlocked(self) -> int:
        if not self._root.exists():
            return 0
        seen: set[tuple[int, int]] = set()
        total = 0
        for path in self._root.rglob("*"):
            if path.name == ".spool.lock" or not path.is_file() or path.is_symlink():
                continue
            stat = path.stat()
            identity = (stat.st_dev, stat.st_ino)
            if identity not in seen:
                seen.add(identity)
                total += stat.st_size
        return total

    def _directory_size_unlocked(self, directory: Path) -> int:
        inodes: dict[tuple[int, int], tuple[int, int, int]] = {}
        for path in directory.rglob("*"):
            if not path.is_file() or path.is_symlink():
                continue
            stat = path.stat()
            identity = (stat.st_dev, stat.st_ino)
            size, links_inside, link_count = inodes.get(identity, (stat.st_size, 0, stat.st_nlink))
            inodes[identity] = (size, links_inside + 1, link_count)
        # Bytes are releasable only when every hard link to the inode belongs
        # to this candidate.  This prevents capacity accounting from assuming
        # that a link retained elsewhere will be freed.
        return sum(size for size, links_inside, link_count in inodes.values() if links_inside == link_count)

    def _repeatable_identity_unlocked(self, directory: Path) -> tuple[int, int, int | None] | None:
        """Return the identity encoded by a known Q06 directory, if any."""
        try:
            parts = self._absolute_path(directory).relative_to(self._root_path()).parts
        except ValueError:
            return None
        if (
            len(parts) != 4
            or parts[0] != "repeatable"
            or not (work_match := re.fullmatch(r"work-item-([1-9][0-9]*)", parts[1]))
            or not (attempt_match := re.fullmatch(r"attempt-([1-9][0-9]*)", parts[2]))
        ):
            return None
        request_match = re.fullmatch(r"request-([0-9]{6})", parts[3])
        if request_match and int(request_match.group(1)) > 0:
            return int(work_match.group(1)), int(attempt_match.group(1)), int(request_match.group(1))
        if re.fullmatch(r"\.purging-[0-9a-f]+", parts[3]):
            return int(work_match.group(1)), int(attempt_match.group(1)), None
        return None

    def _verified_q06_candidate_unlocked(self, directory: Path) -> bool:
        identity = self._repeatable_identity_unlocked(directory)
        marker = directory / ".durable"
        has_durable_marker = marker.is_file() and not marker.is_symlink()
        if not has_durable_marker:
            try:
                self._cleanup_proof_metadata_unlocked(directory)
            except RawSpoolError:
                return False
        if (
            identity is None
            or not directory.is_dir()
            or directory.is_symlink()
            or not has_durable_marker and not (directory / self._CLEANUP_PROOF).is_file()
            or self._purge_verifier is None
        ):
            return False
        try:
            return self._purge_verifier(directory)
        except Exception:
            return False

    def _purge_guard_unlocked(self, directory: Path) -> ContextManager[bool]:
        if self._purge_guard is not None:
            return self._purge_guard(directory)
        return nullcontext(self._verified_q06_candidate_unlocked(directory))

    def _cleanup_proof_metadata_unlocked(self, directory: Path) -> Mapping[str, Any]:
        proof = directory / self._CLEANUP_PROOF
        if not proof.is_file() or proof.is_symlink():
            raise RawSpoolError("raw spool cleanup proof is unavailable")
        try:
            metadata = json.loads(proof.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RawSpoolError("raw spool cleanup proof is unreadable") from error
        return self._validate_cleanup_metadata_unlocked(metadata)

    def _validate_cleanup_metadata_unlocked(self, metadata: object) -> Mapping[str, Any]:
        if not isinstance(metadata, Mapping):
            raise RawSpoolError("raw spool cleanup proof is invalid")
        manifest = metadata.get("cleanup_manifest")
        endpoint = metadata.get("endpoint")
        if not isinstance(manifest, Mapping) or not isinstance(endpoint, str):
            raise RawSpoolError("raw spool cleanup proof is incomplete")
        label = self._label(endpoint)
        expected = (f"{label}.raw.json", f"{label}.request.json", ".durable")
        if (
            manifest.get("version") != 1
            or tuple(manifest.get("files", ())) != expected
            or manifest.get("raw_sha256") != metadata.get("content_sha256")
            or manifest.get("raw_byte_count") != metadata.get("byte_count")
        ):
            raise RawSpoolError("raw spool cleanup proof does not match its metadata")
        return metadata

    def _recover_quarantines_unlocked(self) -> None:
        if not self._root.exists():
            return
        quarantines = tuple(
            directory for directory in self._root.rglob(".purging-*")
            if directory.is_dir()
            and not directory.is_symlink()
            and self._repeatable_identity_unlocked(directory) is not None
        )
        for directory in sorted(quarantines, key=lambda path: path.stat().st_mtime):
            self._purge_verified_q06_unlocked(directory)

    def enforce_limit(self) -> None:
        with self._lock():
            self._enforce_limit_unlocked()

    def _enforce_limit_unlocked(self, *, required: int = 0) -> None:
        """Evict only DB-backed captures; never an uncommitted replay input."""
        self._recover_quarantines_unlocked()
        if self._max_bytes is None or not self._root.exists():
            return
        total = self._size_unlocked()
        if total + required <= self._max_bytes:
            return
        candidates = sorted(
            (
                marker.parent for marker in self._root.rglob(".durable")
                if marker.is_file() and not marker.is_symlink() and self._repeatable_identity_unlocked(marker.parent) is not None
            ),
            key=lambda path: path.stat().st_mtime,
        )
        selected: list[Path] = []
        releasable = 0
        for directory in candidates:
            if not self._verified_q06_candidate_unlocked(directory):
                continue
            selected.append(directory)
            releasable += self._directory_size_unlocked(directory)
            if total + required - releasable <= self._max_bytes:
                break
        # Do not remove one proven copy if the whole verified set cannot make
        # room.  This keeps a capacity failure non-destructive.
        if total + required - releasable > self._max_bytes:
            raise RawSpoolCapacityError("raw spool capacity is exhausted")
        # Recheck every selected candidate before the first rename.  DB state
        # can change while a verifier is reading it, whereas this root lock
        # serializes all cooperating filesystem users.
        if not all(self._verified_q06_candidate_unlocked(directory) for directory in selected):
            raise RawSpoolCapacityError("raw spool capacity is exhausted")
        for directory in selected:
            if not self._purge_verified_q06_unlocked(directory):
                raise RawSpoolCapacityError("raw spool capacity is exhausted")

    def discard_partial(self, directory: Path, request: BaseRequest) -> bool:
        """Recover only an interrupted single-endpoint write before refetching it."""
        self._validate_directory(directory)
        with self._lock():
            return self._discard_partial_unlocked(directory, request)

    def _discard_partial_unlocked(self, directory: Path, request: BaseRequest) -> bool:
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
        """Delete a Q06 request only after its DB-backed proof succeeds."""
        self._validate_directory(directory)
        with self._lock():
            if not self._purge_verified_q06_unlocked(directory):
                raise RawSpoolError("raw spool Q06 capture is not proven safe to purge")

    def purge_legacy_generation(self, directory: Path) -> None:
        """Explicit bootstrap-only cleanup for a completed legacy generation."""
        self._validate_directory(directory)
        with self._lock():
            self._purge_legacy_generation_unlocked(directory)

    def _purge_legacy_generation_unlocked(self, directory: Path) -> None:
        if not directory.is_dir() or directory.is_symlink():
            raise RawSpoolError("refusing to purge outside raw spool")
        relative = self._absolute_path(directory).relative_to(self._root_path())
        parts = relative.parts
        known_generation = (
            len(parts) == 3 and re.fullmatch(r"run-[1-9][0-9]*", parts[0]) and
            re.fullmatch(r"league-[1-9][0-9]*-season-[1-9][0-9]*", parts[1]) and
            re.fullmatch(r"generation-[1-9][0-9]*", parts[2])
        )
        if not known_generation:
            raise RawSpoolError("raw spool directory is not a legacy generation")
        quarantine = directory.with_name(f".purging-{uuid.uuid4().hex}")
        directory.rename(quarantine)
        self._remove_legacy_quarantine_unlocked(quarantine)

    def _purge_verified_q06_unlocked(self, directory: Path) -> bool:
        if directory.name.startswith(".purging-") and directory.is_dir() and not directory.is_symlink():
            # A crash after the journal was unlinked can leave an empty
            # quarantine.  Removing the empty directory loses no artifact and
            # cannot be mistaken for accepting an unproven capture.
            if not any(directory.iterdir()):
                directory.rmdir()
                self._fsync_directory(directory.parent)
                return True
        quarantine = directory
        renamed = False
        if not directory.name.startswith(".purging-"):
            if not self._verified_q06_candidate_unlocked(directory):
                return False
            quarantine = directory.with_name(f".purging-{uuid.uuid4().hex}")
            directory.rename(quarantine)
            self._fsync_directory(quarantine.parent)
            renamed = True
        try:
            # This guard keeps the DB raw payload, fetch, and succeeded work
            # item immutable until the local copy has been removed.
            with self._purge_guard_unlocked(quarantine) as proven:
                if not proven:
                    return False
                self._remove_quarantine_unlocked(quarantine)
            return True
        finally:
            # Before the journal is atomically published no file was removed,
            # so the normal name can be restored.  Once it exists, recovery
            # must retain the quarantine and re-run DB proof before each later
            # unlink; a marker itself is never sufficient proof.
            if (
                renamed
                and quarantine.exists()
                and not (quarantine / self._CLEANUP_PROOF).exists()
                and any(quarantine.iterdir())
            ):
                quarantine.rename(directory)
                self._fsync_directory(directory.parent)

    @staticmethod
    def _remove_legacy_quarantine_unlocked(quarantine: Path) -> None:
        entries = tuple(quarantine.iterdir())
        if any(not item.is_file() and not item.is_symlink() for item in entries):
            raise RawSpoolError("raw spool generation has an unexpected nested directory")
        for item in entries:
            if item.is_file() or item.is_symlink():
                item.unlink()
        quarantine.rmdir()

    def _publish_cleanup_proof_unlocked(self, quarantine: Path) -> Mapping[str, Any]:
        proof = quarantine / self._CLEANUP_PROOF
        if proof.exists():
            return self._cleanup_proof_metadata_unlocked(quarantine)
        metadata_files = [
            path for path in quarantine.iterdir()
            if path.is_file() and not path.is_symlink() and path.name.endswith(".request.json")
        ]
        if len(metadata_files) != 1:
            raise RawSpoolError("raw spool cleanup proof is missing metadata")
        metadata_path = metadata_files[0]
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RawSpoolError("raw spool cleanup metadata is unreadable") from error
        if not isinstance(metadata, Mapping):
            raise RawSpoolError("raw spool cleanup metadata is invalid")
        self._validate_cleanup_metadata_unlocked(metadata)
        # A hard link publishes the already fsynced metadata atomically without
        # replacing an existing journal or creating a partially written copy.
        try:
            os.link(metadata_path, proof, follow_symlinks=False)
        except FileExistsError:
            return self._cleanup_proof_metadata_unlocked(quarantine)
        except OSError as error:
            raise RawSpoolError("raw spool cleanup proof cannot be published") from error
        self._fsync_directory(quarantine)
        return self._cleanup_proof_metadata_unlocked(quarantine)

    def _remove_quarantine_unlocked(self, quarantine: Path) -> None:
        metadata = self._publish_cleanup_proof_unlocked(quarantine)
        endpoint = metadata.get("endpoint")
        if not isinstance(endpoint, str):
            raise RawSpoolError("raw spool cleanup proof is incomplete")
        label = self._label(endpoint)
        proof = quarantine / self._CLEANUP_PROOF
        expected = {proof.name, f"{label}.raw.json", f"{label}.request.json", ".durable"}
        entries = tuple(quarantine.iterdir())
        if any(
            not item.is_file() or item.is_symlink() or item.name not in expected
            for item in entries
        ):
            raise RawSpoolError("raw spool generation has an unexpected cleanup entry")
        # The journal itself is checked by the DB guard and is always last.
        for item in entries:
            if item != proof:
                item.unlink()
                self._fsync_directory(quarantine)
        proof.unlink()
        self._fsync_directory(quarantine)
        quarantine.rmdir()

    def latest_catalogue(self) -> RawSpoolArtifact | None:
        """Recover a staged catalogue after a crash before queue creation."""
        with self._lock():
            return self._latest_catalogue_unlocked()

    def _latest_catalogue_unlocked(self) -> RawSpoolArtifact | None:
        directory = self._root_path() / "catalogue"
        if not directory.is_dir() or directory.is_symlink():
            return None
        request = BaseRequest("/leagues", {})
        candidates: list[Path] = []
        for path in directory.iterdir():
            marker = path / ".queue-pending"
            if not path.is_dir() or path.is_symlink() or not marker.is_file() or marker.is_symlink():
                continue
            try:
                self._validate_directory(path)
                # _load_unlocked rejects links at both artifact files.  Check
                # all entries here too, before it traverses a candidate found
                # by catalogue recovery under the already-held root lock.
                if any(entry.is_symlink() for entry in path.iterdir()):
                    continue
            except RawSpoolError:
                continue
            candidates.append(path)
        candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
        for candidate in candidates:
            try:
                value = self._load_unlocked(candidate, request)
            except RawSpoolError:
                continue
            if value is not None:
                return value
        return None

    def mark_catalogue_pending(self, directory: Path) -> None:
        self._validate_directory(directory)
        with self._lock():
            relative = self._absolute_path(directory).relative_to(self._root_path()).parts
            if len(relative) != 2 or relative[0] != "catalogue" or not directory.is_dir() or directory.is_symlink():
                raise RawSpoolError("raw spool directory is not a catalogue artifact")
            marker = directory / ".queue-pending"
            if marker.exists():
                if not marker.is_file() or marker.is_symlink():
                    raise RawSpoolError("unsafe raw spool catalogue marker")
                return
            self._ensure_capacity(len(b"pending\n"))
            self._write_atomic(marker, b"pending\n")

    def consume_pending_catalogues(self) -> None:
        with self._lock():
            directory = self._root_path() / "catalogue"
            if not directory.is_dir() or directory.is_symlink():
                return
            for candidate in directory.iterdir():
                marker = candidate / ".queue-pending"
                if candidate.is_dir() and not candidate.is_symlink() and marker.is_file() and not marker.is_symlink():
                    marker.unlink()
