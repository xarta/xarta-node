"""routes_health.py — GET /health"""

from fastapi import APIRouter

from . import config as cfg
from .db import get_conn, get_gen, get_meta
from .models import HealthOut

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthOut)
async def health() -> HealthOut:
    with get_conn() as conn:
        gen = get_gen(conn)
        integrity_ok = get_meta(conn, "integrity_ok") == "true"
    return HealthOut(
        status="ok",
        node_id=cfg.NODE_ID,
        node_name=cfg.NODE_NAME,
        gen=gen,
        integrity_ok=integrity_ok,
        ui_url=cfg.UI_URL or None,
    )
