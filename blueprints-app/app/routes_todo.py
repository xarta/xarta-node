"""routes_todo.py — Read/write the TODO.md file in the private repo.

GET  /api/v1/todo        → {"content": "<markdown text>"}
PUT  /api/v1/todo        → body: {"content": "..."} → 204
"""

import logging
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from starlette.responses import Response

from . import config as cfg

log = logging.getLogger(__name__)

router = APIRouter(prefix="/todo", tags=["todo"])

_FILENAME = "TODO.md"


def _todo_path() -> Path:
    """Return the absolute path to TODO.md in the private (inner) repo."""
    inner = cfg.REPO_INNER_PATH
    if not inner:
        raise HTTPException(503, "REPO_INNER_PATH not configured — cannot locate TODO.md")
    return Path(inner) / _FILENAME


@router.get("", response_model=dict)
async def get_todo() -> dict:
    """Return the raw markdown content of TODO.md."""
    path = _todo_path()
    if not path.exists():
        return {"content": "", "exists": False}
    try:
        content = path.read_text(encoding="utf-8")
        return {"content": content, "exists": True}
    except Exception as exc:
        log.error("todo: failed to read %s: %s", path, exc)
        raise HTTPException(500, f"Failed to read TODO.md: {exc}") from exc


class TodoBody(BaseModel):
    content: str


@router.put("", status_code=204)
async def put_todo(body: TodoBody) -> Response:
    """Overwrite TODO.md with the provided content."""
    path = _todo_path()
    try:
        path.write_text(body.content, encoding="utf-8")
        log.info("todo: wrote %d chars to %s", len(body.content), path)
    except Exception as exc:
        log.error("todo: failed to write %s: %s", path, exc)
        raise HTTPException(500, f"Failed to write TODO.md: {exc}") from exc
    return Response(status_code=204)
