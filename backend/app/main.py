from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.live import LiveSettings, RedisLiveStore, managed_redis_client

from app.web.assets import router as web_assets_router
from app.web.live import router as web_live_router
from app.web.router import router as web_read_router


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    """Own one reusable Redis connection pool for all live read requests."""
    settings = LiveSettings.from_environment()
    async with managed_redis_client(settings) as redis_client:
        application.state.live_store = RedisLiveStore(redis_client)
        try:
            yield
        finally:
            del application.state.live_store


app = FastAPI(title="Football Analytics API", lifespan=lifespan)
app.include_router(web_assets_router)
app.include_router(web_read_router)
app.include_router(web_live_router)


@app.get("/health")
async def health() -> dict[str, str]:
    """Report that the backend process is available."""
    return {"status": "ok"}
