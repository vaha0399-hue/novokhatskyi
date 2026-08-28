"""Controlled one-time cache of known provider media URLs into local storage.

This script reads canonical URLs already stored in PostgreSQL. It never calls
the API-Football JSON API, receives no API key, and is not run by web requests.
"""

from __future__ import annotations

import argparse
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import httpx
import psycopg

from app.web.assets import AssetKind, LocalLogoStore


ALLOWED_MEDIA_HOSTS = frozenset({"media.api-sports.io"})
PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


@dataclass(frozen=True)
class LogoTarget:
    kind: AssetKind
    entity_id: int
    url: str


def _validate_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme != "https" or parsed.hostname not in ALLOWED_MEDIA_HOSTS or not parsed.path:
        raise ValueError("logo URL is not an approved API-Sports media URL")
    return value


def _targets(connection: psycopg.Connection, *, season_id: int) -> list[LogoTarget]:
    league_row = connection.execute(
        """SELECT league.id,league.logo_url FROM football.seasons season
             JOIN football.leagues league ON league.id=season.league_id
             WHERE season.id=%s""",
        (season_id,),
    ).fetchone()
    if league_row is None or league_row[1] is None:
        raise ValueError("season has no cached provider league logo URL")
    team_rows = connection.execute(
        """SELECT team.id,team.logo_url FROM football.season_teams membership
             JOIN football.teams team ON team.id=membership.team_id
             WHERE membership.season_id=%s
             ORDER BY team.id""",
        (season_id,),
    ).fetchall()
    if not team_rows or any(row[1] is None for row in team_rows):
        raise ValueError("every selected season team must have a logo URL")
    result = [LogoTarget("leagues", int(league_row[0]), _validate_url(str(league_row[1])))]
    result.extend(LogoTarget("teams", int(row[0]), _validate_url(str(row[1]))) for row in team_rows)
    return result


def _write_png(path: Path, content: bytes) -> None:
    if not content.startswith(PNG_SIGNATURE):
        raise ValueError("provider media response is not a PNG image")
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", delete=False) as output:
        output.write(content)
        temp_path = Path(output.name)
    temp_path.replace(path)


def cache_season_logos(*, database_url: str, season_id: int, store: LocalLogoStore) -> tuple[int, int]:
    """Cache missing league/team logos sequentially; returns (new, reused)."""
    with psycopg.connect(database_url) as connection:
        targets = _targets(connection, season_id=season_id)
    created = reused = 0
    with httpx.Client(timeout=15.0, follow_redirects=False, headers={"User-Agent": "football-analytics-logo-cache/1"}) as client:
        for target in targets:
            path = store.logo_path(kind=target.kind, entity_id=target.entity_id)
            if path.is_file():
                reused += 1
                continue
            response = client.get(target.url)
            response.raise_for_status()
            if response.headers.get("content-type", "").split(";", 1)[0].lower() != "image/png":
                raise ValueError("provider media response has unexpected content type")
            _write_png(path, response.content)
            created += 1
    return created, reused


def main() -> int:
    parser = argparse.ArgumentParser(description="Cache canonical league/team logo media locally.")
    parser.add_argument("--season-id", type=int, required=True)
    parser.add_argument("--asset-dir", type=Path, default=None)
    arguments = parser.parse_args()
    database_url = os.environ.get("SUPABASE_DB_URL")
    if not database_url:
        raise RuntimeError("SUPABASE_DB_URL is required")
    store = LocalLogoStore(arguments.asset_dir) if arguments.asset_dir else LocalLogoStore.from_environment()
    created, reused = cache_season_logos(database_url=database_url, season_id=arguments.season_id, store=store)
    print(f"logo cache complete: new={created} reused={reused}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
