from __future__ import annotations

import json
import multiprocessing
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from app.api_football import APIFootballResponse
from app.importer.raw_spool import RawSpool, RawSpoolArtifact, RawSpoolCapacityError, RawSpoolError
from app.importer.season_bootstrap import BaseRequest


_STARTED_AT = datetime(2026, 9, 13, 12, 0, tzinfo=UTC)


def _artifact(*, work_item_id: int, request_number: int, padding: int = 256) -> RawSpoolArtifact:
    request = BaseRequest("/fixtures", {"league": 39, "season": 2026, "page": request_number})
    payload = {
        "get": "fixtures",
        "parameters": {"league": "39", "season": "2026", "page": str(request_number)},
        "errors": {},
        "results": 0,
        "paging": {"current": request_number, "total": request_number},
        "response": [],
        "padding": "x" * padding,
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return RawSpoolArtifact(
        request,
        APIFootballResponse(payload, raw, 200, {}),
        _STARTED_AT,
        _STARTED_AT,
        {"league_external_id": 39, "season_start_year": 2026},
        work_item_id,
        1,
        "q06-v1",
        "scheduled_refresh",
        "standard",
        f"work-item-{work_item_id}:attempt-1:request-{request_number}",
    )


def _file_bytes(root: Path) -> int:
    return sum(
        path.stat().st_size
        for path in root.rglob("*")
        if path.is_file() and not path.is_symlink() and path.name != ".spool.lock"
    )


def _stage_after_barrier(
    root: str,
    max_bytes: int,
    work_item_id: int,
    request_number: int,
    barrier: Any,
    results: Any,
    allow_verified_purge: bool = False,
) -> None:
    spool = RawSpool(
        Path(root),
        max_bytes=max_bytes,
        purge_verifier=(lambda _directory: True) if allow_verified_purge else None,
    )
    artifact = _artifact(work_item_id=work_item_id, request_number=request_number)
    directory = spool.work_item_request_directory(
        work_item_id=work_item_id,
        attempt=1,
        request_number=request_number,
    )
    barrier.wait()
    try:
        spool.stage(directory, artifact)
    except RawSpoolCapacityError:
        results.put((work_item_id, "capacity"))
    else:
        results.put((work_item_id, "stored"))


def _load_after_barrier(root: str, directory: str, request: BaseRequest, barrier: Any, results: Any) -> None:
    spool = RawSpool(Path(root))
    barrier.wait()
    try:
        loaded = spool.load(Path(directory), request)
    except BaseException as error:
        results.put(("reader", type(error).__name__))
    else:
        results.put(("reader", "whole" if loaded is not None else "absent"))


def _purge_for_capacity_after_barrier(root: str, max_bytes: int, barrier: Any, results: Any) -> None:
    spool = RawSpool(Path(root), max_bytes=max_bytes, purge_verifier=lambda _directory: True)
    barrier.wait()
    try:
        spool.enforce_limit()
    except BaseException as error:
        results.put(("cleaner", type(error).__name__))
    else:
        results.put(("cleaner", "completed"))


def _crash_after_first_atomic_write(root: str, work_item_id: int) -> None:
    spool = RawSpool(Path(root))
    artifact = _artifact(work_item_id=work_item_id, request_number=1)
    directory = spool.work_item_request_directory(work_item_id=work_item_id, attempt=1, request_number=1)
    original_write = spool._write_atomic
    writes = 0

    def write_then_crash(path: Path, content: bytes) -> None:
        nonlocal writes
        original_write(path, content)
        writes += 1
        if writes == 1:
            os._exit(86)

    spool._write_atomic = write_then_crash  # type: ignore[method-assign]
    spool.stage(directory, artifact)


def _crash_after_quarantine_rename(root: str, directory: str) -> None:
    spool = RawSpool(Path(root), purge_verifier=lambda _directory: True)
    target = Path(directory)
    original_rename = Path.rename

    def rename_then_crash(path: Path, destination: Path) -> Path:
        renamed = original_rename(path, destination)
        if path == target:
            os._exit(87)
        return renamed

    Path.rename = rename_then_crash  # type: ignore[method-assign]
    spool.purge_generation(target)


def _join(process: multiprocessing.Process) -> None:
    process.join(timeout=10)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("raw spool subprocess deadlocked")


def test_two_processes_share_one_capacity_reservation_and_keep_the_accepted_artifact_intact(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    calibration_root = tmp_path / "calibration"
    calibration = RawSpool(calibration_root, max_bytes=None)
    sample = _artifact(work_item_id=100, request_number=1)
    sample_directory = calibration.work_item_request_directory(work_item_id=100, attempt=1, request_number=1)
    calibration.stage(sample_directory, sample)
    one_artifact_bytes = _file_bytes(calibration_root)

    root = tmp_path / "spool"
    barrier = context.Barrier(3)
    results = context.Queue()
    processes = [
        context.Process(target=_stage_after_barrier, args=(str(root), one_artifact_bytes, item, 1, barrier, results))
        for item in (101, 102)
    ]
    for process in processes:
        process.start()
    barrier.wait()
    for process in processes:
        _join(process)

    outcomes = sorted(results.get(timeout=2) for _ in processes)
    assert sorted(outcome for _, outcome in outcomes) == ["capacity", "stored"]
    assert _file_bytes(root) <= one_artifact_bytes

    stored_work_item = next(item for item, outcome in outcomes if outcome == "stored")
    stored = _artifact(work_item_id=stored_work_item, request_number=1)
    stored_directory = RawSpool(root).work_item_request_directory(
        work_item_id=stored_work_item,
        attempt=1,
        request_number=1,
    )
    loaded = RawSpool(root).load(stored_directory, stored.request)
    assert loaded is not None
    assert loaded.response.raw_body == stored.response.raw_body
    assert loaded.physical_request_id == stored.physical_request_id


def test_capacity_cleanup_concurrent_with_write_and_read_has_no_partial_artifact_or_deadlock(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    root = tmp_path / "spool"
    setup = RawSpool(root, max_bytes=None)
    old = _artifact(work_item_id=201, request_number=1)
    old_directory = setup.work_item_request_directory(work_item_id=201, attempt=1, request_number=1)
    setup.stage(old_directory, old)
    setup.mark_durable(old_directory)
    # Keep an unknown protected file so the root begins one byte over the
    # shared limit.  Either the writer or the cleaner may evict the verified
    # capture first, but both processes enforce exactly the same root budget.
    protected = root / "protected.bin"
    protected.write_bytes(b"protected")
    limit = _file_bytes(root) - 1

    barrier = context.Barrier(4)
    results = context.Queue()
    new = _artifact(work_item_id=202, request_number=1)
    new_directory = setup.work_item_request_directory(work_item_id=202, attempt=1, request_number=1)
    processes = [
        context.Process(
            target=_stage_after_barrier,
            args=(str(root), limit, 202, 1, barrier, results, True),
        ),
        context.Process(target=_load_after_barrier, args=(str(root), str(old_directory), old.request, barrier, results)),
        context.Process(target=_purge_for_capacity_after_barrier, args=(str(root), limit, barrier, results)),
    ]
    for process in processes:
        process.start()
    barrier.wait()
    for process in processes:
        _join(process)

    observed = dict(results.get(timeout=2) for _ in processes)
    assert observed["cleaner"] == "completed"
    assert observed["reader"] in {"whole", "absent"}
    assert observed[202] == "stored"
    loaded = RawSpool(root).load(new_directory, new.request)
    assert loaded is not None
    assert loaded.response.raw_body == new.response.raw_body
    assert protected.read_bytes() == b"protected"
    assert _file_bytes(root) <= limit


def test_restart_recognizes_partial_atomic_write_without_damaging_existing_artifacts(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    root = tmp_path / "spool"
    spool = RawSpool(root)
    existing = _artifact(work_item_id=301, request_number=1)
    existing_directory = spool.work_item_request_directory(work_item_id=301, attempt=1, request_number=1)
    spool.stage(existing_directory, existing)

    process = context.Process(target=_crash_after_first_atomic_write, args=(str(root), 302))
    process.start()
    _join(process)
    assert process.exitcode == 86

    restarted = RawSpool(root)
    interrupted = _artifact(work_item_id=302, request_number=1)
    interrupted_directory = restarted.work_item_request_directory(work_item_id=302, attempt=1, request_number=1)
    assert restarted.discard_partial(interrupted_directory, interrupted.request) is True
    assert restarted.load(interrupted_directory, interrupted.request) is None
    existing_loaded = restarted.load(existing_directory, existing.request)
    assert existing_loaded is not None
    assert existing_loaded.response.raw_body == existing.response.raw_body

    restarted.stage(interrupted_directory, interrupted)
    recovered = restarted.load(interrupted_directory, interrupted.request)
    assert recovered is not None
    assert recovered.response.raw_body == interrupted.response.raw_body


def test_restart_rechecks_and_finishes_cleanup_quarantine_after_rename_crash(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    root = tmp_path / "spool"
    spool = RawSpool(root)
    artifact = _artifact(work_item_id=401, request_number=1)
    directory = spool.work_item_request_directory(work_item_id=401, attempt=1, request_number=1)
    spool.stage(directory, artifact)
    spool.mark_durable(directory)

    process = context.Process(target=_crash_after_quarantine_rename, args=(str(root), str(directory)))
    process.start()
    _join(process)
    assert process.exitcode == 87

    quarantines = tuple(directory.parent.glob(".purging-*"))
    assert len(quarantines) == 1
    assert (quarantines[0] / "fixtures.raw.json").read_bytes() == artifact.response.raw_body

    restarted = RawSpool(root)
    assert restarted.work_item_artifacts(work_item_id=401) == ()
    restarted.set_purge_verifier(lambda _directory: True)
    restarted.enforce_limit()
    assert not quarantines[0].exists()


def test_cleanup_journal_publication_failure_restores_unmodified_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    spool = RawSpool(tmp_path / "spool", purge_verifier=lambda _directory: True)
    artifact = _artifact(work_item_id=501, request_number=1)
    directory = spool.work_item_request_directory(work_item_id=501, attempt=1, request_number=1)
    spool.stage(directory, artifact)
    spool.mark_durable(directory)
    before = {path.name: path.read_bytes() for path in directory.iterdir()}
    unlinked: list[Path] = []
    original_unlink = Path.unlink

    def reject_link(*_args: object, **_kwargs: object) -> None:
        raise OSError("journal publication interrupted")

    def record_unlink(path: Path, *args: Any, **kwargs: Any) -> None:
        unlinked.append(path)
        original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(os, "link", reject_link)
    monkeypatch.setattr(Path, "unlink", record_unlink)
    with pytest.raises(RawSpoolError, match="cleanup proof cannot be published"):
        spool.purge_generation(directory)

    assert unlinked == []
    assert directory.is_dir()
    assert {path.name: path.read_bytes() for path in directory.iterdir()} == before
    assert tuple(directory.parent.glob(".purging-*")) == ()


@pytest.mark.parametrize("failed_unlink", (1, 2, 3, 4))
def test_restart_finishes_cleanup_after_each_unlink_interruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failed_unlink: int,
) -> None:
    spool = RawSpool(tmp_path / "spool", purge_verifier=lambda _directory: True)
    artifact = _artifact(work_item_id=502, request_number=1)
    directory = spool.work_item_request_directory(work_item_id=502, attempt=1, request_number=1)
    spool.stage(directory, artifact)
    spool.mark_durable(directory)
    original_unlink = Path.unlink
    calls = 0

    def fail_once(path: Path, *args: Any, **kwargs: Any) -> None:
        nonlocal calls
        calls += 1
        if calls == failed_unlink:
            raise OSError("unlink interrupted")
        original_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_once)
    with pytest.raises(OSError, match="unlink interrupted"):
        spool.purge_generation(directory)
    monkeypatch.setattr(Path, "unlink", original_unlink)

    quarantines = tuple(directory.parent.glob(".purging-*"))
    assert len(quarantines) == 1
    assert (quarantines[0] / ".cleanup-proof.json").is_file()
    restarted = RawSpool(tmp_path / "spool", max_bytes=1, purge_verifier=lambda _directory: True)
    restarted.enforce_limit()
    assert not quarantines[0].exists()


def test_cleanup_journal_hard_link_is_counted_once_after_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "spool"
    spool = RawSpool(root, purge_verifier=lambda _directory: True)
    artifact = _artifact(work_item_id=503, request_number=1)
    directory = spool.work_item_request_directory(work_item_id=503, attempt=1, request_number=1)
    spool.stage(directory, artifact)
    spool.mark_durable(directory)
    original_size = spool._size_unlocked()
    original_unlink = Path.unlink

    def interrupt_first_unlink(_path: Path, *_args: Any, **_kwargs: Any) -> None:
        raise OSError("unlink interrupted")

    monkeypatch.setattr(Path, "unlink", interrupt_first_unlink)
    with pytest.raises(OSError, match="unlink interrupted"):
        spool.purge_generation(directory)
    monkeypatch.setattr(Path, "unlink", original_unlink)

    quarantine = next(directory.parent.glob(".purging-*"))
    metadata = quarantine / "fixtures.request.json"
    journal = quarantine / ".cleanup-proof.json"
    assert (metadata.stat().st_dev, metadata.stat().st_ino) == (journal.stat().st_dev, journal.stat().st_ino)
    assert spool._size_unlocked() == original_size

    restarted = RawSpool(root, max_bytes=original_size, purge_verifier=lambda _directory: False)
    restarted.enforce_limit()
    assert quarantine.is_dir()


@pytest.mark.parametrize("failure", ("journal-fsync", "rmdir"))
def test_cleanup_failure_after_journal_unlink_keeps_empty_quarantine_for_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    root = tmp_path / "spool"
    spool = RawSpool(root, purge_verifier=lambda _directory: True)
    artifact = _artifact(work_item_id=504, request_number=1)
    directory = spool.work_item_request_directory(work_item_id=504, attempt=1, request_number=1)
    spool.stage(directory, artifact)
    spool.mark_durable(directory)
    original_fsync = spool._fsync_directory
    original_rmdir = Path.rmdir

    def fail_after_journal_unlink(path: Path) -> None:
        if path.name.startswith(".purging-") and not (path / ".cleanup-proof.json").exists():
            raise OSError("post-journal fsync interrupted")
        original_fsync(path)

    def fail_empty_quarantine_rmdir(path: Path) -> None:
        if path.name.startswith(".purging-") and not any(path.iterdir()):
            raise OSError("post-journal rmdir interrupted")
        original_rmdir(path)

    if failure == "journal-fsync":
        monkeypatch.setattr(spool, "_fsync_directory", fail_after_journal_unlink)
        expected = "post-journal fsync interrupted"
    else:
        monkeypatch.setattr(Path, "rmdir", fail_empty_quarantine_rmdir)
        expected = "post-journal rmdir interrupted"
    with pytest.raises(OSError, match=expected):
        spool.purge_generation(directory)

    monkeypatch.setattr(Path, "rmdir", original_rmdir)
    quarantine = next(directory.parent.glob(".purging-*"))
    assert not any(quarantine.iterdir())
    assert not directory.exists()
    RawSpool(root, purge_verifier=lambda _directory: False).enforce_limit()
    assert not quarantine.exists()
