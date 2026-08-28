from __future__ import annotations

import asyncio

import httpx

from app.main import app


PNG = b"\x89PNG\r\n\x1a\nminimal-png-payload"


def _get(path: str) -> httpx.Response:
    async def request() -> httpx.Response:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            return await client.get(path)

    return asyncio.run(request())


def test_local_logo_endpoint_returns_only_a_cached_png(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("FOOTBALL_ASSET_DIR", str(tmp_path))
    target = tmp_path / "teams" / "42.png"
    target.parent.mkdir(parents=True)
    target.write_bytes(PNG)

    response = _get("/web/v1/assets/teams/42/logo")

    assert response.status_code == 200
    assert response.content == PNG
    assert response.headers["content-type"] == "image/png"
    assert response.headers["cache-control"] == "public, max-age=604800, immutable"


def test_local_logo_endpoint_never_fetches_a_missing_asset(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("FOOTBALL_ASSET_DIR", str(tmp_path))

    response = _get("/web/v1/assets/leagues/5/logo")

    assert response.status_code == 404
    assert response.json() == {"detail": {"code": "asset_not_found"}}

