from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from app.api_football import APIFootballResponse
from app.importer.season_coverage_contract import validate_season_coverage_response


SAMPLES = (
    ("01-leagues-epl-2025.raw.json", 2025),
    ("02-leagues-epl-2026.raw.json", 2026),
)
SAMPLE_DIR = (
    Path(__file__).parents[2]
    / "samples"
    / "api-football"
    / "pro-canary-2026-08-22"
)


@pytest.mark.parametrize(("filename", "season"), SAMPLES)
def test_real_pro_sample_validates_approved_coverage(filename: str, season: int) -> None:
    raw_body = (SAMPLE_DIR / filename).read_bytes()
    response = APIFootballResponse(
        data=json.loads(raw_body), raw_body=raw_body, status_code=200, headers={}
    )

    coverage = validate_season_coverage_response(
        response,
        expected_content_sha256=hashlib.sha256(raw_body).digest(),
        external_league_id=39,
        external_season=season,
    )

    assert coverage.fixture_statistics_supported is True
    assert coverage.lineups_supported is True
    assert coverage.standings_supported is True
    assert coverage.injuries_supported is True


def test_rejects_wrong_season_membership() -> None:
    raw_body = (SAMPLE_DIR / SAMPLES[0][0]).read_bytes()
    response = APIFootballResponse(
        data=json.loads(raw_body), raw_body=raw_body, status_code=200, headers={}
    )

    with pytest.raises(ValueError, match="exactly one requested season"):
        validate_season_coverage_response(
            response,
            expected_content_sha256=hashlib.sha256(raw_body).digest(),
            external_league_id=39,
            external_season=1900,
        )


def test_rejects_raw_sha_mismatch() -> None:
    raw_body = (SAMPLE_DIR / SAMPLES[0][0]).read_bytes()
    response = APIFootballResponse(
        data=json.loads(raw_body), raw_body=raw_body, status_code=200, headers={}
    )

    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        validate_season_coverage_response(
            response,
            expected_content_sha256=b"0" * 32,
            external_league_id=39,
            external_season=2025,
        )
