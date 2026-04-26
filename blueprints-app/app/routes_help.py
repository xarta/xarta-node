"""routes_help.py — Blueprints help synthesis proxy routes and app action catalog."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import Field

from .db import get_conn
from .nullclaw_docs_search import (
    SynthesisControls,
    blueprints_synthesis_response,
    ensure_succeeded,
    fetch_query_synthesis_task,
    submit_query_synthesis,
)

router = APIRouter(prefix="/help", tags=["help"])

_CATALOG_VERSION = "blueprints-help-catalog-v1"
_GROUP_LABELS = {
    "synthesis": "Synthesis",
    "probes": "Probes",
    "settings": "Settings",
}

_STATIC_MODAL_CATALOG: tuple[dict[str, Any], ...] = (
    {
        "catalog_id": "settings.docs.modal.docs-search",
        "route": "settings.docs",
        "modal": "docs-search",
        "kind": "blueprints_modal",
        "label": "Docs Search",
        "description": "Search registered local docs with Hybrid, Vector, or Keyword retrieval.",
        "target": {
            "group": "settings",
            "tab": "docs",
            "modal_id": "docs-search-modal",
            "opener": "openDocsSearchModal",
        },
        "aliases": ["docs search", "documentation search", "find docs", "hybrid docs search"],
    },
)


class HelpTurnBody(SynthesisControls):
    surface: str | None = Field(default=None, max_length=200)
    voice: bool = False


def _attach_help_request_context(response: dict[str, Any], body: HelpTurnBody) -> dict[str, Any]:
    response["request_context"] = {
        "surface": body.surface,
        "voice": body.voice,
    }
    return response


def _nav_row_to_catalog_page(row: Any) -> dict[str, Any]:
    group = str(row["menu_group"] or "").strip()
    tab = str(row["item_key"] or "").strip()
    label = str(row["page_label"] or row["label"] or tab).strip()
    return {
        "catalog_id": f"{group}.{tab}",
        "route": f"{group}.{tab}",
        "kind": "blueprints_page",
        "group": group,
        "group_label": _GROUP_LABELS.get(group, group.title()),
        "tab": tab,
        "label": label,
        "parent": row["parent_key"],
        "sort_order": row["sort_order"],
        "url": f"/fallback-ui/?group={group}&tab={tab}",
    }


def build_help_catalog() -> dict[str, Any]:
    """Return the deterministic Blueprints page/modal catalog used by help actions."""
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM nav_items
            WHERE COALESCE(is_fn, 0) = 0
              AND menu_group IN ('synthesis', 'probes', 'settings')
            ORDER BY menu_group, sort_order, item_key
            """
        ).fetchall()

    pages = [_nav_row_to_catalog_page(row) for row in rows]
    routes = {page["route"]: page for page in pages}
    modals = [dict(item) for item in _STATIC_MODAL_CATALOG]
    return {
        "ok": True,
        "version": _CATALOG_VERSION,
        "source": "blueprints_app",
        "contract": {
            "action_targets": ["blueprints_page", "blueprints_modal"],
            "dispatch_policy": "Only cataloged route/page/modal targets are actionable.",
        },
        "pages": pages,
        "modals": modals,
        "routes": routes,
    }


def _catalog_modal_lookup(catalog: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for modal in catalog.get("modals") or []:
        if not isinstance(modal, dict):
            continue
        route = str(modal.get("route") or "")
        modal_id = str(modal.get("modal") or "")
        if route and modal_id:
            lookup[(route, modal_id)] = modal
    return lookup


def _catalog_page_lookup(catalog: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(page.get("route")): page
        for page in catalog.get("pages") or []
        if isinstance(page, dict) and page.get("route")
    }


def _append_unique_warning(response: dict[str, Any], warning: dict[str, Any]) -> None:
    warnings = response.setdefault("warnings", [])
    if warning not in warnings:
        warnings.append(warning)


def enforce_help_action_catalog(response: dict[str, Any]) -> dict[str, Any]:
    """Constrain help actions to the Blueprints-owned deterministic catalog."""
    catalog = build_help_catalog()
    response["action_catalog"] = {
        "version": catalog["version"],
        "source": catalog["source"],
    }
    action = response.get("action")
    if action is None:
        return response
    if not isinstance(action, dict):
        response["action"] = None
        _append_unique_warning(
            response,
            {
                "code": "help_action_not_cataloged",
                "message": "Help action was ignored because it was not an object.",
            },
        )
        return response

    target = action.get("target") if isinstance(action.get("target"), dict) else {}
    kind = str(target.get("kind") or "")
    route = str(target.get("route") or "")
    modal = str(target.get("modal") or "")

    if kind == "blueprints_modal":
        match = _catalog_modal_lookup(catalog).get((route, modal))
        if match:
            response["action"] = {
                **action,
                "catalog_match": match["catalog_id"],
                "dispatch": {
                    "type": "open_modal",
                    "group": match["target"]["group"],
                    "tab": match["target"]["tab"],
                    "modal": match["modal"],
                    "modal_id": match["target"]["modal_id"],
                    "opener": match["target"]["opener"],
                },
            }
            return response
    elif kind == "blueprints_page":
        match = _catalog_page_lookup(catalog).get(route)
        if match:
            response["action"] = {
                **action,
                "catalog_match": match["catalog_id"],
                "dispatch": {
                    "type": "open_page",
                    "group": match["group"],
                    "tab": match["tab"],
                    "url": match["url"],
                },
            }
            return response

    response["action"] = None
    _append_unique_warning(
        response,
        {
            "code": "help_action_not_cataloged",
            "message": "Help action was not dispatched because the target is absent from the Blueprints help catalog.",
            "target": target,
        },
    )
    return response


@router.post("/turn", response_model=dict)
async def help_turn(body: HelpTurnBody) -> dict[str, Any]:
    task = await submit_query_synthesis(body, "help_turn")
    ensure_succeeded(task)
    response = blueprints_synthesis_response(task, route="/api/v1/help/turn", projection="turn")
    enforce_help_action_catalog(response)
    return _attach_help_request_context(response, body)


@router.post("/short", response_model=dict)
async def help_short(body: HelpTurnBody) -> dict[str, Any]:
    task = await submit_query_synthesis(body, "help_turn")
    ensure_succeeded(task)
    response = blueprints_synthesis_response(task, route="/api/v1/help/short", projection="short")
    enforce_help_action_catalog(response)
    return _attach_help_request_context(response, body)


@router.post("/action", response_model=dict)
async def help_action(body: HelpTurnBody) -> dict[str, Any]:
    task = await submit_query_synthesis(body, "help_turn")
    ensure_succeeded(task)
    response = blueprints_synthesis_response(task, route="/api/v1/help/action", projection="action")
    enforce_help_action_catalog(response)
    return _attach_help_request_context(response, body)


@router.post("/modal", response_model=dict)
async def help_modal(body: HelpTurnBody) -> dict[str, Any]:
    task = await submit_query_synthesis(body, "help_turn")
    ensure_succeeded(task)
    response = blueprints_synthesis_response(task, route="/api/v1/help/modal", projection="modal")
    enforce_help_action_catalog(response)
    return _attach_help_request_context(response, body)


@router.get("/turns/{task_id}", response_model=dict)
async def get_help_turn(task_id: str) -> dict[str, Any]:
    task = await fetch_query_synthesis_task(task_id)
    response = blueprints_synthesis_response(task, route="/api/v1/help/turns/{id}", projection="turn")
    return enforce_help_action_catalog(response)


@router.get("/catalog", response_model=dict)
async def help_catalog() -> dict[str, Any]:
    return build_help_catalog()
