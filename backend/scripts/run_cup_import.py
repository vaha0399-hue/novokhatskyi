"""Run the reviewed API-Football Cup queue from explicit local artifacts."""

from __future__ import annotations

import asyncio
import json
import os
from dataclasses import asdict
from pathlib import Path

from psycopg import Connection

from app.api_football import APIFootballClient
from app.importer.cup_canonical import CupCanonicalSink
from app.importer.cup_queue import CupQueueSettings, Worker
from app.importer.cup_queue_repository import PostgresCupQueueRepository
from app.importer.current_season_statistics import run_current_season_statistics_backfill_async
from app.importer.raw_spool import RawSpool


ARTIFACTS = Path("/opt/football-analytics/artifacts")
DEFAULT_ALLOW_LIST = ARTIFACTS / "cups_keep_for_import.txt"
DEFAULT_WITH_STATS = (
    ARTIFACTS
    / "api-football-catalogue-classification/2026-09-05/with_fixture_stats.json"
)
DEFAULT_WITHOUT_STATS = (
    ARTIFACTS
    / "api-football-catalogue-classification/2026-09-05/without_fixture_stats.json"
)
DEFAULT_SPOOL = Path("/var/lib/football-analytics/cup-bootstrap")


def _path(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, str(default)))


async def run() -> object:
    database_url = os.environ.get("SUPABASE_DB_URL", "").strip()
    if not database_url:
        raise RuntimeError("SUPABASE_DB_URL is required")
    settings = CupQueueSettings(
        spool_dir=_path("CUP_IMPORT_SPOOL_DIR", DEFAULT_SPOOL),
        allow_list_path=_path("CUP_IMPORT_ALLOW_LIST", DEFAULT_ALLOW_LIST),
        with_fixture_stats_path=_path("CUP_IMPORT_WITH_STATS", DEFAULT_WITH_STATS),
        without_fixture_stats_path=_path("CUP_IMPORT_WITHOUT_STATS", DEFAULT_WITHOUT_STATS),
    )
    api = APIFootballClient.from_environment()
    try:
        with PostgresCupQueueRepository(database_url) as repository, Connection.connect(database_url) as write_conn:
            sink = CupCanonicalSink(write_conn)
            worker = Worker(
                provider=api,
                repository=repository,
                spool=RawSpool(settings.spool_dir),
                settings=settings,
                canonical_sink=sink,
                statistics_backfill=lambda scope: run_current_season_statistics_backfill_async(
                    scope=scope, client=api
                ),
            )
            return await worker.run_once()
    finally:
        await api.aclose()


def main() -> None:
    print(json.dumps(asdict(asyncio.run(run())), default=str, sort_keys=True))


if __name__ == "__main__":
    main()
