#!/usr/bin/env python3
"""Classify current API-Football catalogue seasons using an existing raw snapshot.

This is deliberately an offline, read-only classifier: it makes no provider
requests and opens no database connection.  It writes new JSON reports only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any


def _read_catalogue(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    response = payload.get("response")
    if not isinstance(response, list):
        raise ValueError(f"{path}: expected top-level response array")
    return response


def _current_entries(catalogue: list[dict[str, Any]]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    missing_coverage: list[str] = []

    for competition in catalogue:
        league = competition.get("league")
        country = competition.get("country")
        seasons = competition.get("seasons")
        if not isinstance(league, dict) or not isinstance(country, dict) or not isinstance(seasons, list):
            raise ValueError("catalogue contains a competition with an invalid league/country/seasons shape")

        current = [season for season in seasons if isinstance(season, dict) and season.get("current") is True]
        if not current:
            continue

        league_id = league.get("id")
        if not isinstance(league_id, int):
            raise ValueError("catalogue current season has an invalid league ID")
        for season in current:
            season_year = season.get("year")
            if not isinstance(season_year, int):
                raise ValueError("catalogue current season has an invalid season year")
            coverage = season.get("coverage")
            fixtures = coverage.get("fixtures") if isinstance(coverage, dict) else None
            if not isinstance(coverage, dict) or not isinstance(fixtures, dict):
                missing_coverage.append(f"{league_id}:{season_year}")
                continue

            entries.append(
                {
                    "league_id": league_id,
                    "name": league.get("name"),
                    "country": country.get("name"),
                    "type": league.get("type"),
                    "season": season_year,
                    "coverage": coverage,
                }
            )

    if missing_coverage:
        joined = ", ".join(sorted(missing_coverage))
        raise ValueError(f"current-season coverage is absent for league_id:season entries: {joined}")
    return entries


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("raw_catalogue", type=Path, help="Saved API-Football /leagues raw JSON")
    parser.add_argument("output_dir", type=Path, help="New directory for classification JSON files")
    args = parser.parse_args()

    raw_path = args.raw_catalogue.resolve(strict=True)
    output_dir = args.output_dir.resolve()
    if output_dir.exists():
        raise ValueError(f"refusing to overwrite existing output directory: {output_dir}")

    entries = _current_entries(_read_catalogue(raw_path))
    entries.sort(key=lambda item: item["league_id"])
    with_stats = [item for item in entries if item["coverage"]["fixtures"].get("statistics_fixtures") is True]
    without_stats = [item for item in entries if item["coverage"]["fixtures"].get("statistics_fixtures") is not True]

    output_dir.mkdir(parents=True)
    _write_json(output_dir / "with_fixture_stats.json", with_stats)
    _write_json(output_dir / "without_fixture_stats.json", without_stats)

    counts = {
        "TOTAL_CURRENT": len(entries),
        "TOTAL_LEAGUE": sum(item["type"] == "League" for item in entries),
        "TOTAL_CUP": sum(item["type"] == "Cup" for item in entries),
        "WITH_FIXTURE_STATS": len(with_stats),
        "WITHOUT_FIXTURE_STATS": len(without_stats),
        "raw_catalogue_sha256": hashlib.sha256(raw_path.read_bytes()).hexdigest(),
    }
    _write_json(output_dir / "summary.json", counts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
