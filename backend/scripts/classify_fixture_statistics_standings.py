#!/usr/bin/env python3
"""Classify saved fixture-stat-capable catalogue entries by /standings shape.

This one-off tool never connects to the canonical database and makes no calls
other than one API-Football ``/standings`` request for each input entry.
Provider responses are retained as raw JSON beside the resulting reports.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from app.api_football import APIFootballAPIError, APIFootballClient, APIFootballHTTPError
from app.api_football.client import safe_rate_limit_headers


Shape = Literal["SINGLE_TABLE", "MULTI_GROUP", "NO_STANDINGS", "UNKNOWN"]


@dataclass(frozen=True)
class Competition:
    league_id: int
    name: str
    country: str | None
    competition_type: str
    season: int
    coverage: dict[str, Any]

    @classmethod
    def from_json(cls, value: object) -> "Competition":
        if not isinstance(value, Mapping):
            raise ValueError("input entry must be an object")
        league_id, name, competition_type, season, coverage = (
            value.get("league_id"), value.get("name"), value.get("type"), value.get("season"), value.get("coverage")
        )
        if not isinstance(league_id, int) or league_id <= 0:
            raise ValueError("input entry league_id must be a positive integer")
        if not isinstance(name, str) or not name:
            raise ValueError(f"league {league_id}: name must be a non-empty string")
        if not isinstance(competition_type, str) or not competition_type:
            raise ValueError(f"league {league_id}: type must be a non-empty string")
        if not isinstance(season, int):
            raise ValueError(f"league {league_id}: season must be an integer")
        if not isinstance(coverage, dict):
            raise ValueError(f"league {league_id}: coverage must be an object")
        country = value.get("country")
        if country is not None and not isinstance(country, str):
            raise ValueError(f"league {league_id}: country must be a string or null")
        return cls(league_id, name, country, competition_type, season, coverage)

    @property
    def raw_stem(self) -> str:
        return f"league-{self.league_id}-season-{self.season}"

    def record(self, *, shape: Shape, groups_count: int | None, group_names: list[str]) -> dict[str, Any]:
        return {
            "league_id": self.league_id,
            "name": self.name,
            "country": self.country,
            "type": self.competition_type,
            "season": self.season,
            "standings_shape": shape,
            "groups_count": groups_count,
            "group_names": group_names,
            "coverage": self.coverage,
        }


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_competitions(path: Path) -> list[Competition]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("input must contain a JSON array")
    entries = [Competition.from_json(item) for item in payload]
    keys = [(item.league_id, item.season) for item in entries]
    if len(set(keys)) != len(keys):
        raise ValueError("input contains duplicate league_id + season entries")
    return sorted(entries, key=lambda item: (item.league_id, item.season))


def _classify(payload: Mapping[str, Any]) -> tuple[Shape, int | None, list[str]]:
    response = payload.get("response")
    if not isinstance(response, list):
        return "UNKNOWN", None, []
    if not response:
        return "NO_STANDINGS", 0, []
    if len(response) != 1 or not isinstance(response[0], Mapping):
        return "UNKNOWN", None, []

    league = response[0].get("league")
    if not isinstance(league, Mapping):
        return "UNKNOWN", None, []
    standings = league.get("standings")
    if standings is None or standings == []:
        return "NO_STANDINGS", 0, []
    if not isinstance(standings, list) or not all(isinstance(group, list) for group in standings):
        return "UNKNOWN", None, []

    group_names: list[str] = []
    for group in standings:
        for row in group:
            if isinstance(row, Mapping) and isinstance(row.get("group"), str) and row["group"] not in group_names:
                group_names.append(row["group"])

    if len(standings) == 1:
        return "SINGLE_TABLE", 1, group_names
    if len(standings) > 1:
        return "MULTI_GROUP", len(standings), group_names
    return "NO_STANDINGS", 0, []


async def run(*, input_path: Path, output_dir: Path, pacing_seconds: float) -> dict[str, int]:
    if output_dir.exists():
        raise ValueError(f"refusing to overwrite existing output directory: {output_dir}")
    if pacing_seconds < 0:
        raise ValueError("pacing_seconds must be non-negative")

    competitions = _load_competitions(input_path)
    output_dir.mkdir(parents=True)
    raw_dir = output_dir / "raw_standings"
    raw_dir.mkdir()
    results: dict[Shape, list[dict[str, Any]]] = {
        "SINGLE_TABLE": [], "MULTI_GROUP": [], "NO_STANDINGS": [], "UNKNOWN": [],
    }
    api_calls_used = 0

    async with APIFootballClient.from_environment() as client:
        for index, competition in enumerate(competitions):
            request_metadata: dict[str, Any] = {
                "endpoint": "/standings",
                "parameters": {"league": competition.league_id, "season": competition.season},
                "fetched_at": datetime.now(UTC).isoformat(),
            }
            shape: Shape
            groups_count: int | None
            group_names: list[str]
            try:
                api_calls_used += 1
                response = await client.get("/standings", params=request_metadata["parameters"])
                if client.response_contains_api_key(response.raw_body):
                    raise RuntimeError("refusing to persist a response containing the API key")
                (raw_dir / f"{competition.raw_stem}.raw.json").write_bytes(response.raw_body)
                request_metadata.update(
                    {
                        "http_status": response.status_code,
                        "rate_limit": safe_rate_limit_headers(response.headers),
                    }
                )
                shape, groups_count, group_names = _classify(response.data)
            except APIFootballAPIError as error:
                if error.raw_body is not None:
                    (raw_dir / f"{competition.raw_stem}.raw.json").write_bytes(error.raw_body)
                request_metadata.update(
                    {
                        "http_status": error.status_code,
                        "error": "api_error",
                        "rate_limit": error.safe_headers,
                    }
                )
                shape, groups_count, group_names = "UNKNOWN", None, []
            except APIFootballHTTPError as error:
                request_metadata.update(
                    {
                        "http_status": error.status_code,
                        "error": "http_error",
                        "rate_limit": error.safe_headers,
                    }
                )
                shape, groups_count, group_names = "UNKNOWN", None, []
            except Exception as error:  # Preserve progress and classify only this entry as unknown.
                request_metadata["error"] = type(error).__name__
                shape, groups_count, group_names = "UNKNOWN", None, []

            _write_json(raw_dir / f"{competition.raw_stem}.request.json", request_metadata)
            results[shape].append(competition.record(shape=shape, groups_count=groups_count, group_names=group_names))
            if index + 1 < len(competitions) and pacing_seconds:
                await asyncio.sleep(pacing_seconds)

    files = {
        "SINGLE_TABLE": "single_table.json",
        "MULTI_GROUP": "multi_group.json",
        "NO_STANDINGS": "no_standings.json",
        "UNKNOWN": "unknown.json",
    }
    for shape, filename in files.items():
        _write_json(output_dir / filename, results[shape])
    summary = {
        "TOTAL_CHECKED": len(competitions),
        "SINGLE_TABLE": len(results["SINGLE_TABLE"]),
        "MULTI_GROUP": len(results["MULTI_GROUP"]),
        "NO_STANDINGS": len(results["NO_STANDINGS"]),
        "UNKNOWN": len(results["UNKNOWN"]),
        "API_CALLS_USED": api_calls_used,
    }
    _write_json(output_dir / "summary.json", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_json", type=Path, help="with_fixture_stats.json created by the offline classifier")
    parser.add_argument("output_dir", type=Path, help="new directory for reports and raw standings responses")
    parser.add_argument("--pacing-seconds", type=float, default=0.25)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(run(input_path=args.input_json.resolve(strict=True), output_dir=args.output_dir.resolve(), pacing_seconds=args.pacing_seconds)), sort_keys=True))


if __name__ == "__main__":
    main()
