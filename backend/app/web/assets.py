"""Read-only delivery of locally cached provider-derived media assets."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse


AssetKind = Literal["teams", "leagues"]
router = APIRouter(prefix="/web/v1/assets", tags=["web-assets"])


class LocalLogoStore:
    """Filesystem cache only; serving a logo never contacts a provider."""

    def __init__(self, root: Path) -> None:
        self._root = root

    @classmethod
    def from_environment(cls) -> "LocalLogoStore":
        configured = os.environ.get("FOOTBALL_ASSET_DIR")
        root = Path(configured) if configured else Path(__file__).resolve().parents[2] / "var" / "assets"
        return cls(root)

    def logo_path(self, *, kind: AssetKind, entity_id: int) -> Path:
        if entity_id <= 0:
            raise ValueError("entity_id must be positive")
        return self._root / kind / f"{entity_id}.png"


def get_local_logo_store() -> LocalLogoStore:
    return LocalLogoStore.from_environment()


Store = Annotated[LocalLogoStore, Depends(get_local_logo_store)]


@router.get("/{kind}/{entity_id}/logo", response_class=FileResponse)
def logo(kind: AssetKind, entity_id: int, store: Store) -> FileResponse:
    try:
        path = store.logo_path(kind=kind, entity_id=entity_id)
    except ValueError as error:
        raise HTTPException(status_code=404, detail={"code": "asset_not_found"}) from error
    if not path.is_file():
        raise HTTPException(status_code=404, detail={"code": "asset_not_found"})
    return FileResponse(
        path,
        media_type="image/png",
        headers={"Cache-Control": "public, max-age=604800, immutable"},
    )
