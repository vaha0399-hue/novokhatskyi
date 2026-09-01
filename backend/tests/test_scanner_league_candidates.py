from __future__ import annotations

from app.importer.scanner_league_candidates import SCANNER_LEAGUE_CANDIDATES


def test_scanner_candidate_allowlist_is_a_frozen_unique_provider_snapshot() -> None:
    assert len(SCANNER_LEAGUE_CANDIDATES) == 76
    assert len({candidate.provider_league_id for candidate in SCANNER_LEAGUE_CANDIDATES}) == 76
    assert next(
        candidate for candidate in SCANNER_LEAGUE_CANDIDATES
        if candidate.provider_league_id == 39
    ).name == "Premier League"
    assert all(candidate.name and candidate.country for candidate in SCANNER_LEAGUE_CANDIDATES)
