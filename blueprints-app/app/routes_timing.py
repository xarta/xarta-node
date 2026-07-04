"""Timing inspection endpoints for local Blueprints diagnostics."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Response

from . import timing

router = APIRouter(prefix="/debug/timing", tags=["debug-timing"])


@router.get("")
async def timing_state() -> dict[str, object]:
    return timing.state()


@router.get("/jsonl")
async def timing_jsonl(
    limit: Annotated[int, Query(ge=1, le=100_000)] = 4096,
) -> Response:
    body = timing.snapshot_jsonl(limit)
    if body:
        body += "\n"
    return Response(content=body, media_type="application/x-ndjson")


@router.post("/clear")
async def clear_timing() -> dict[str, object]:
    cleared = timing.clear()
    state = timing.state()
    state["cleared"] = cleared
    return state


@router.post("/flush")
async def flush_timing_logs() -> dict[str, object]:
    return await timing.flush_disk_logs()


@router.post("/prune")
async def prune_timing_logs() -> dict[str, object]:
    return await timing.prune_disk_logs()
