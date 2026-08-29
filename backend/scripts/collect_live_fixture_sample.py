"""Collect one API-Football ``GET /fixtures?live=all`` contract sample.

The collector is intentionally read-only: it makes exactly one provider request,
does not invoke importers or write to the database, and saves the returned body
unchanged alongside non-sensitive request metadata and a compact field summary.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.api_football import APIFootballClient, APIFootballResponse
from app.api_football.client import safe_rate_limit_headers

ENDPOINT = "/fixtures"
PARAMETERS = {"live": "all"}
RAW_FILE = "fixtures-live-all.raw.json"
REQUEST_FILE = "fixtures-live-all.request.json"
SUMMARY_FILE = "fixtures-live-all.summary.json"
MANIFEST_FILE = "manifest.json"


def _timestamp() -> str:
    return datetime.now(UTC).isoformat()


def _write_json(path: Path, content: Any) -> None:
    path.write_text(json.dumps(content, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _fixture_summary(item: object) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None

    fixture = item.get("fixture")
    teams = item.get("teams")
    if not isinstance(fixture, dict) or not isinstance(teams, dict):
        return None

    status = fixture.get("status")
    home = teams.get("home")
    away = teams.get("away")
    if not isinstance(status, dict) or not isinstance(home, dict) or not isinstance(away, dict):
        return None

    return {
        "fixture_id": fixture.get("id"),
        "home_team": home.get("name"),
        "away_team": away.get("name"),
        "status": {
            "short": status.get("short"),
            "long": status.get("long"),
            "elapsed": status.get("elapsed"),
            "extra": status.get("extra"),
        },
        "goals": item.get("goals"),
        "score": item.get("score"),
    }


def summarize_live_fixtures(payload: dict[str, Any]) -> dict[str, Any]:
    """Extract the live status, current goals, and score fields without coercion."""
    response = payload.get("response")
    if not isinstance(response, list):
        raise ValueError("API-Football live fixtures response must contain an array.")

    fixtures = [summary for item in response if (summary := _fixture_summary(item)) is not None]
    statuses: dict[str, int] = {}
    for fixture in fixtures:
        short = fixture["status"]["short"]
        if isinstance(short, str):
            statuses[short] = statuses.get(short, 0) + 1

    return {
        "response_fixture_count": len(response),
        "summarized_fixture_count": len(fixtures),
        "status_counts": statuses,
        "fixtures": fixtures,
    }


async def collect(output_dir: Path, *, client: APIFootballClient) -> dict[str, Any]:
    """Fetch and persist one live-fixtures response into an empty directory."""
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError(f"Output directory must be empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    result: APIFootballResponse = await client.get(ENDPOINT, params=PARAMETERS)
    if client.response_contains_api_key(result.raw_body):
        raise RuntimeError("API-Football response cannot be safely persisted.")

    summary = summarize_live_fixtures(result.data)
    raw_path = output_dir / RAW_FILE
    request_path = output_dir / REQUEST_FILE
    summary_path = output_dir / SUMMARY_FILE
    raw_path.write_bytes(result.raw_body)
    _write_json(
        request_path,
        {
            "endpoint": ENDPOINT,
            "parameters": PARAMETERS,
            "fetched_at": _timestamp(),
            "http_status": result.status_code,
            "results": result.data.get("results"),
            "paging": result.data.get("paging"),
            "rate_limit": safe_rate_limit_headers(result.headers),
            "content_sha256": hashlib.sha256(result.raw_body).hexdigest(),
            "byte_count": len(result.raw_body),
        },
    )
    _write_json(summary_path, summary)
    _write_json(
        output_dir / MANIFEST_FILE,
        {
            "campaign": "api-football-live-fixtures-contract-sample",
            "purpose": "contract-research-only",
            "physical_api_calls_this_campaign": 1,
            "secrets_included": False,
            "calls": [
                {
                    "sequence": 1,
                    "name": "fixtures-live-all",
                    "endpoint": ENDPOINT,
                    "parameters": PARAMETERS,
                    "http_status": result.status_code,
                    "results": result.data.get("results"),
                    "paging": result.data.get("paging"),
                    "safe_rate_limit_headers": safe_rate_limit_headers(result.headers),
                    "content_sha256": hashlib.sha256(result.raw_body).hexdigest(),
                    "byte_count": len(result.raw_body),
                    "raw_file": raw_path.name,
                    "request_file": request_path.name,
                    "summary_file": summary_path.name,
                }
            ],
        },
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    summary = asyncio.run(collect(args.output_dir, client=APIFootballClient.from_environment()))
    print(
        "Saved one live-fixtures sample: "
        f"{summary['summarized_fixture_count']} fixtures, "
        f"statuses={summary['status_counts']}"
    )


if __name__ == "__main__":
    main()
