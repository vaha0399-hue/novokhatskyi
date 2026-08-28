from __future__ import annotations

import pytest

from scripts.cache_logo_assets import PNG_SIGNATURE, _validate_url, _write_png


def test_logo_cache_accepts_only_the_known_https_media_host() -> None:
    accepted = "https://media.api-sports.io/football/teams/157.png"

    assert _validate_url(accepted) == accepted
    with pytest.raises(ValueError, match="approved"):
        _validate_url("https://api-football.com/v3/teams")
    with pytest.raises(ValueError, match="approved"):
        _validate_url("http://media.api-sports.io/football/teams/157.png")


def test_logo_cache_writes_only_valid_png_bytes_atomically(tmp_path) -> None:
    path = tmp_path / "teams" / "157.png"

    _write_png(path, PNG_SIGNATURE + b"provider-png")

    assert path.read_bytes() == PNG_SIGNATURE + b"provider-png"
    with pytest.raises(ValueError, match="not a PNG"):
        _write_png(tmp_path / "teams" / "invalid.png", b"not-a-png")
    assert not (tmp_path / "teams" / "invalid.png").exists()
