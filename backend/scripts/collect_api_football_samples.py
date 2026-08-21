"""Manual, quota-bounded collector for API-Football contract samples.

The script is intentionally not an importer, scheduler, or FastAPI endpoint. It makes the
seven approved contract-research calls and writes raw response bytes separately from safe
request metadata. Run it manually only after API_FOOTBALL_KEY is available to the process.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.api_football import APIFootballClient, APIFootballResponse

LEAGUE_ID = 39
MAX_REQUESTS = 9
MANDATORY_REQUESTS = 7
FINISHED_STATUSES = {"FT", "AET", "PEN"}


def _timestamp() -> str:
    return datetime.now(UTC).isoformat()


def _write_json(path: Path, content: Any) -> None:
    path.write_text(json.dumps(content, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _safe_headers(headers: dict[str, str]) -> dict[str, str]:
    """Persist only non-sensitive rate-limit metadata, if supplied by the provider."""
    return {
        key: value
        for key, value in headers.items()
        if key.lower()
        in {
            "x-ratelimit-limit",
            "x-ratelimit-remaining",
            "x-ratelimit-requests-limit",
            "x-ratelimit-requests-remaining",
            "retry-after",
        }
    }


class SampleCollector:
    def __init__(
        self,
        output_dir: Path,
        client: APIFootballClient,
        *,
        season: int,
        request_limit: int,
    ) -> None:
        if request_limit < 1 or request_limit > MAX_REQUESTS:
            raise ValueError(f"request_limit must be between 1 and {MAX_REQUESTS}.")
        self.output_dir = output_dir
        self.client = client
        self.request_limit = request_limit
        self.manifest: dict[str, Any] = {
            "source": "API-Football v3",
            "purpose": "contract-research-only",
            "league_id": LEAGUE_ID,
            "research_season": season,
            "production_target_season": 2026,
            "request_limit": request_limit,
            "started_at": _timestamp(),
            "requests": [],
        }

    async def collect(self, name: str, endpoint: str, params: dict[str, int]) -> APIFootballResponse:
        if len(self.manifest["requests"]) >= self.request_limit:
            raise RuntimeError(f"Sample collection request limit ({self.request_limit}) reached.")
        result = await self.client.get(endpoint, params=params)
        if self.client.response_contains_api_key(result.raw_body):
            raise RuntimeError("API-Football response cannot be safely persisted.")
        raw_path = self.output_dir / f"{name}.raw.json"
        request_path = self.output_dir / f"{name}.request.json"
        raw_path.write_bytes(result.raw_body)
        _write_json(
            request_path,
            {
                "endpoint": endpoint,
                "parameters": params,
                "fetched_at": _timestamp(),
                "http_status": result.status_code,
                "results": result.data.get("results"),
                "paging": result.data.get("paging"),
                "rate_limit": _safe_headers(dict(result.headers)),
            },
        )
        self.manifest["requests"].append(
            {"name": name, "endpoint": endpoint, "parameters": params, "raw_file": raw_path.name,
             "request_file": request_path.name}
        )
        self._write_manifest()
        return result

    def _write_manifest(self) -> None:
        current = {**self.manifest, "completed_at": _timestamp(), "request_count": len(self.manifest["requests"])}
        _write_json(self.output_dir / "manifest.json", current)


def _finished_fixture_id(fixtures: dict[str, Any]) -> int:
    response = fixtures.get("response")
    if not isinstance(response, list):
        raise RuntimeError("Fixtures response does not contain a response array.")

    finished_id: int | None = None
    for item in response:
        if not isinstance(item, dict):
            continue
        fixture = item.get("fixture")
        if not isinstance(fixture, dict) or not isinstance(fixture.get("id"), int):
            continue
        status = fixture.get("status")
        short = status.get("short") if isinstance(status, dict) else None
        if short in FINISHED_STATUSES and finished_id is None:
            finished_id = fixture["id"]

    if finished_id is None:
        raise RuntimeError("Season fixtures did not provide a finished fixture.")
    return finished_id


def _team_id(teams: dict[str, Any]) -> int:
    response = teams.get("response")
    if not isinstance(response, list) or not response:
        raise RuntimeError("Teams response does not contain a team.")
    team = response[0].get("team") if isinstance(response[0], dict) else None
    if not isinstance(team, dict) or not isinstance(team.get("id"), int):
        raise RuntimeError("First team does not contain a numeric team.id.")
    return team["id"]


async def run(output_dir: Path, *, season: int, request_limit: int) -> None:
    if request_limit != MANDATORY_REQUESTS:
        raise ValueError(f"This collector requires request_limit={MANDATORY_REQUESTS}.")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise RuntimeError(f"Output directory must be empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    collector = SampleCollector(
        output_dir,
        APIFootballClient.from_environment(),
        season=season,
        request_limit=request_limit,
    )
    common = {"league": LEAGUE_ID, "season": season}
    fixtures = await collector.collect("fixtures", "/fixtures", common)
    teams = await collector.collect("teams", "/teams", common)
    finished_fixture_id = _finished_fixture_id(fixtures.data)
    team_id = _team_id(teams.data)

    await collector.collect("standings", "/standings", common)
    await collector.collect("team-statistics", "/teams/statistics", {**common, "team": team_id})
    await collector.collect("fixture-statistics", "/fixtures/statistics", {"fixture": finished_fixture_id})
    await collector.collect("injuries", "/injuries", common)
    await collector.collect("lineups", "/fixtures/lineups", {"fixture": finished_fixture_id})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--season", type=int, required=True, help="Research season, e.g. 2024.")
    parser.add_argument(
        "--request-limit",
        type=int,
        required=True,
        choices=[MANDATORY_REQUESTS],
        help=f"Must be {MANDATORY_REQUESTS}; the collector also hard-caps requests at {MAX_REQUESTS}.",
    )
    args = parser.parse_args()
    asyncio.run(run(args.output_dir, season=args.season, request_limit=args.request_limit))


if __name__ == "__main__":
    main()
