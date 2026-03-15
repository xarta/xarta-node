"""routes_assumptions.py — Read/write the ASSUMPTIONS.md file in the private repo.

GET  /api/v1/assumptions        → {"content": "<markdown text>"}
PUT  /api/v1/assumptions        → body: {"content": "..."} → 204
"""

import logging
import os
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from starlette.responses import Response

from . import config as cfg

log = logging.getLogger(__name__)

router = APIRouter(prefix="/assumptions", tags=["assumptions"])

_FILENAME = "ASSUMPTIONS.md"


def _assumptions_path() -> Path:
    """Return the absolute path to ASSUMPTIONS.md in the private (inner) repo."""
    inner = cfg.REPO_INNER_PATH
    if not inner:
        raise HTTPException(503, "REPO_INNER_PATH not configured — cannot locate ASSUMPTIONS.md")
    return Path(inner) / _FILENAME


@router.get("", response_model=dict)
async def get_assumptions() -> dict:
    """Return the raw markdown content of ASSUMPTIONS.md."""
    path = _assumptions_path()
    if not path.exists():
        return {"content": "", "exists": False}
    try:
        content = path.read_text(encoding="utf-8")
        return {"content": content, "exists": True}
    except Exception as exc:
        log.error("assumptions: failed to read %s: %s", path, exc)
        raise HTTPException(500, f"Failed to read ASSUMPTIONS.md: {exc}") from exc


class AssumptionsBody(BaseModel):
    content: str


@router.put("", status_code=204)
async def put_assumptions(body: AssumptionsBody) -> Response:
    """Overwrite ASSUMPTIONS.md with the provided content."""
    path = _assumptions_path()
    try:
        path.write_text(body.content, encoding="utf-8")
        log.info("assumptions: wrote %d chars to %s", len(body.content), path)
    except Exception as exc:
        log.error("assumptions: failed to write %s: %s", path, exc)
        raise HTTPException(500, f"Failed to write ASSUMPTIONS.md: {exc}") from exc
    return Response(status_code=204)
