#!/usr/bin/env python3
"""Create an offline fixture-statistics coverage report for API-Football cups.

The input is one of the JSON arrays emitted by
``classify_catalogue_fixture_statistics.py``.  This script does not call
API-Football or open a database connection.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


WITH_STATS_FILE = "cups_with_fixture_stats.txt"
WITHOUT_STATS_FILE = "cups_without_fixture_stats.txt"
SUMMARY_FILE = "summary.json"


def _read_entries(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"{path}: expected a JSON array")
    if not all(isinstance(entry, dict) for entry in payload):
        raise ValueError(f"{path}: every catalogue entry must be an object")
    return payload


def _validated_entry(entry: dict[str, Any], *, expected_statistics_fixtures: bool) -> tuple[int, str, str, int, bool]:
    entry_type = entry.get("type")
    if not isinstance(entry_type, str):
        raise ValueError("catalogue entry has an invalid type")

    league_id = entry.get("league_id")
    name = entry.get("name")
    season = entry.get("season")
    coverage = entry.get("coverage")
    fixtures = coverage.get("fixtures") if isinstance(coverage, dict) else None
    statistics_fixtures = fixtures.get("statistics_fixtures") if isinstance(fixtures, dict) else None
    if not isinstance(league_id, int) or isinstance(league_id, bool):
        raise ValueError("Cup catalogue entry has an invalid league_id")
    if not isinstance(name, str) or not name:
        raise ValueError("catalogue entry has an invalid name")
    if not isinstance(season, int) or isinstance(season, bool):
        raise ValueError("catalogue entry has an invalid season")
    if not isinstance(statistics_fixtures, bool):
        raise ValueError("catalogue entry has an invalid coverage.fixtures.statistics_fixtures")
    if statistics_fixtures is not expected_statistics_fixtures:
        raise ValueError(
            "catalogue input has an unexpected coverage.fixtures.statistics_fixtures value"
        )
    return league_id, name, entry_type, season, statistics_fixtures


def _cup_entries(
    entries: list[dict[str, Any]], *, expected_statistics_fixtures: bool
) -> list[dict[str, Any]]:
    cups: dict[tuple[int, int], dict[str, Any]] = {}
    for entry in entries:
        league_id, name, entry_type, season, _ = _validated_entry(
            entry, expected_statistics_fixtures=expected_statistics_fixtures
        )
        if entry_type != "Cup":
            continue
        report_entry = {"league_id": league_id, "name": name}
        key = (league_id, season)
        existing = cups.setdefault(key, report_entry)
        if existing != report_entry:
            raise ValueError(f"conflicting Cup catalogue entries for league_id={league_id}, season={season}")

    return sorted(cups.values(), key=lambda item: (item["league_id"], item["name"]))


def classify_cups(
    with_statistics_entries: list[dict[str, Any]], without_statistics_entries: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return deterministically sorted cup reports from the two classified inputs."""
    return (
        _cup_entries(with_statistics_entries, expected_statistics_fixtures=True),
        _cup_entries(without_statistics_entries, expected_statistics_fixtures=False),
    )


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_cup_list(path: Path, cups: list[dict[str, Any]]) -> None:
    """Write one UTF-8 ``league_id<TAB>name`` entry per line."""
    path.write_text(
        "".join(f"{cup['league_id']}\t{cup['name']}\n" for cup in cups), encoding="utf-8"
    )


def write_report(with_statistics_path: Path, without_statistics_path: Path, output_dir: Path) -> dict[str, int]:
    """Classify cups from both catalogue reports and write a new report directory."""
    if output_dir.exists():
        raise ValueError(f"refusing to overwrite existing output directory: {output_dir}")

    with_stats, without_stats = classify_cups(
        _read_entries(with_statistics_path), _read_entries(without_statistics_path)
    )
    output_dir.mkdir(parents=True)
    _write_cup_list(output_dir / WITH_STATS_FILE, with_stats)
    _write_cup_list(output_dir / WITHOUT_STATS_FILE, without_stats)
    summary = {
        "TOTAL_CUP": len(with_stats) + len(without_stats),
        "WITH_FIXTURE_STATS": len(with_stats),
        "WITHOUT_FIXTURE_STATS": len(without_stats),
    }
    _write_json(output_dir / SUMMARY_FILE, summary)
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("with_statistics_json", type=Path, help="with_fixture_stats.json JSON array")
    parser.add_argument("without_statistics_json", type=Path, help="without_fixture_stats.json JSON array")
    parser.add_argument("output_dir", type=Path, help="New directory for cup report JSON files")
    args = parser.parse_args(argv)

    with_statistics_path = args.with_statistics_json.resolve(strict=True)
    without_statistics_path = args.without_statistics_json.resolve(strict=True)
    write_report(with_statistics_path, without_statistics_path, args.output_dir.resolve())
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
