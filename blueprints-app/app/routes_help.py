"""routes_help.py — Blueprints help synthesis proxy routes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import Field

from .nullclaw_docs_search import (
    SynthesisControls,
    blueprints_synthesis_response,
    ensure_succeeded,
    fetch_query_synthesis_task,
    submit_query_synthesis,
)

router = APIRouter(prefix="/help", tags=["help"])


class HelpTurnBody(SynthesisControls):
    surface: str | None = Field(default=None, max_length=200)
    voice: bool = False


def _attach_help_request_context(response: dict[str, Any], body: HelpTurnBody) -> dict[str, Any]:
    response["request_context"] = {
        "surface": body.surface,
        "voice": body.voice,
    }
    return response


@router.post("/turn", response_model=dict)
async def help_turn(body: HelpTurnBody) -> dict[str, Any]:
    task = await submit_query_synthesis(body, "help_turn")
    ensure_succeeded(task)
    response = blueprints_synthesis_response(task, route="/api/v1/help/turn", projection="turn")
    return _attach_help_request_context(response, body)


@router.post("/short", response_model=dict)
async def help_short(body: HelpTurnBody) -> dict[str, Any]:
    task = await submit_query_synthesis(body, "help_turn")
    ensure_succeeded(task)
    response = blueprints_synthesis_response(task, route="/api/v1/help/short", projection="short")
    return _attach_help_request_context(response, body)


@router.post("/action", response_model=dict)
async def help_action(body: HelpTurnBody) -> dict[str, Any]:
    task = await submit_query_synthesis(body, "help_turn")
    ensure_succeeded(task)
    response = blueprints_synthesis_response(task, route="/api/v1/help/action", projection="action")
    return _attach_help_request_context(response, body)


@router.post("/modal", response_model=dict)
async def help_modal(body: HelpTurnBody) -> dict[str, Any]:
    task = await submit_query_synthesis(body, "help_turn")
    ensure_succeeded(task)
    response = blueprints_synthesis_response(task, route="/api/v1/help/modal", projection="modal")
    return _attach_help_request_context(response, body)


@router.get("/turns/{task_id}", response_model=dict)
async def get_help_turn(task_id: str) -> dict[str, Any]:
    task = await fetch_query_synthesis_task(task_id)
    return blueprints_synthesis_response(task, route="/api/v1/help/turns/{id}", projection="turn")
