from fastapi import FastAPI

from app.web.router import router as web_read_router


app = FastAPI(title="Football Analytics API")
app.include_router(web_read_router)


@app.get("/health")
async def health() -> dict[str, str]:
    """Report that the backend process is available."""
    return {"status": "ok"}
