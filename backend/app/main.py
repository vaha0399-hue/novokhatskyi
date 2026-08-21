from fastapi import FastAPI


app = FastAPI(title="Football Analytics API")


@app.get("/health")
async def health() -> dict[str, str]:
    """Report that the backend process is available."""
    return {"status": "ok"}
