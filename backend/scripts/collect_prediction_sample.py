"""Collect one API-Football ``GET /predictions?fixture=...`` contract sample."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.api_football import APIFootballClient
from app.api_football.client import safe_rate_limit_headers

ENDPOINT = "/predictions"


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def summarize_predictions(payload: dict[str, Any], *, fixture_id: int) -> dict[str, Any]:
    response = payload.get("response")
    if not isinstance(response, list):
        raise ValueError("API-Football predictions response must contain an array.")
    if not response:
        return {"fixture_id": fixture_id, "response_count": 0, "prediction": None}
    item = response[0]
    if not isinstance(item, dict):
        raise ValueError("API-Football predictions response item must be an object.")
    prediction = item.get("predictions")
    if not isinstance(prediction, dict):
        raise ValueError("API-Football predictions item must contain predictions.")

    home = item.get("teams", {}).get("home", {}) if isinstance(item.get("teams"), dict) else {}
    away = item.get("teams", {}).get("away", {}) if isinstance(item.get("teams"), dict) else {}
    h2h = item.get("h2h")
    return {
        "fixture_id": fixture_id,
        "response_count": len(response),
        "league": item.get("league"),
        "teams": {"home": {"id": home.get("id"), "name": home.get("name")},
                  "away": {"id": away.get("id"), "name": away.get("name")}},
        "prediction": {
            "winner": prediction.get("winner"),
            "win_or_draw": prediction.get("win_or_draw"),
            "under_over": prediction.get("under_over"),
            "goals": prediction.get("goals"),
            "advice": prediction.get("advice"),
            "percent": prediction.get("percent"),
        },
        "comparison": item.get("comparison"),
        "h2h_count": len(h2h) if isinstance(h2h, list) else None,
    }


async def collect(output_dir: Path, *, fixture_id: int, client: APIFootballClient) -> dict[str, Any]:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError(f"Output directory must be empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    params = {"fixture": fixture_id}
    result = await client.get(ENDPOINT, params=params)
    if client.response_contains_api_key(result.raw_body):
        raise RuntimeError("API-Football response cannot be safely persisted.")

    digest = hashlib.sha256(result.raw_body).hexdigest()
    raw_name = f"predictions-{fixture_id}.raw.json"
    request_name = f"predictions-{fixture_id}.request.json"
    summary_name = f"predictions-{fixture_id}.summary.json"
    summary = summarize_predictions(result.data, fixture_id=fixture_id)
    (output_dir / raw_name).write_bytes(result.raw_body)
    _write_json(output_dir / request_name, {
        "endpoint": ENDPOINT, "parameters": params, "fetched_at": datetime.now(UTC).isoformat(),
        "http_status": result.status_code, "results": result.data.get("results"),
        "paging": result.data.get("paging"), "rate_limit": safe_rate_limit_headers(result.headers),
        "content_sha256": digest, "byte_count": len(result.raw_body),
    })
    _write_json(output_dir / summary_name, summary)
    _write_json(output_dir / "manifest.json", {
        "campaign": "api-football-predictions-contract-sample", "purpose": "contract-research-only",
        "physical_api_calls_this_campaign": 1, "secrets_included": False,
        "call": {"endpoint": ENDPOINT, "parameters": params, "raw_file": raw_name,
                  "request_file": request_name, "summary_file": summary_name,
                  "content_sha256": digest, "byte_count": len(result.raw_body)},
    })
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=int, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    summary = asyncio.run(collect(args.output_dir, fixture_id=args.fixture, client=APIFootballClient.from_environment(budget_consumer="legacy_manual")))
    print(f"Saved predictions sample for fixture {args.fixture}: results={summary['response_count']}")


if __name__ == "__main__":
    main()
