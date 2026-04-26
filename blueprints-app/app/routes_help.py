"""routes_help.py — Blueprints help synthesis proxy routes and app action catalog."""

from __future__ import annotations

import json
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
_MIN_DISPATCH_CONFIDENCE = 0.80
_AMBIGUITY_CONFIDENCE_MARGIN = 0.08
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
        "dispatchable": True,
        "aliases": ["docs search", "documentation search", "find docs", "hybrid docs search"],
    },
    {
        "catalog_id": "settings.docs.modal.new-doc",
        "route": "settings.docs",
        "modal": "new-doc",
        "kind": "blueprints_modal",
        "label": "New Document",
        "description": "Create a registered Blueprints documentation entry.",
        "target": {
            "group": "settings",
            "tab": "docs",
            "modal_id": "docs-modal",
            "opener": "openNewDocModal",
        },
        "dispatchable": True,
        "aliases": ["new doc", "create doc", "new documentation page"],
    },
    {
        "catalog_id": "settings.docs.modal.add-existing-doc",
        "route": "settings.docs",
        "modal": "add-existing-doc",
        "kind": "blueprints_modal",
        "label": "Add Existing Document",
        "description": "Register an existing Markdown document in the Docs viewer.",
        "target": {
            "group": "settings",
            "tab": "docs",
            "modal_id": "add-doc-modal",
            "opener": "openAddDocModal",
        },
        "dispatchable": True,
        "aliases": ["add existing doc", "register doc", "add markdown doc"],
    },
    {
        "catalog_id": "app.help.modal.help",
        "route": "app.help",
        "modal": "help",
        "kind": "blueprints_modal",
        "label": "Help",
        "description": "Open the app-wide Blueprints help modal.",
        "target": {
            "group": None,
            "tab": None,
            "modal_id": "bp-help-modal",
            "opener": "BlueprintsHelpSurface.open",
        },
        "dispatchable": True,
        "aliases": ["help", "assistant help", "blueprints help"],
    },
    {
        "catalog_id": "settings.docs.modal.edit-doc-metadata",
        "route": "settings.docs",
        "modal": "edit-doc-metadata",
        "kind": "blueprints_modal",
        "label": "Edit Document Metadata",
        "description": "Edit metadata for the currently open document.",
        "target": {
            "group": "settings",
            "tab": "docs",
            "modal_id": "docs-modal",
            "opener": "openEditDocModal",
        },
        "context_required": "active_doc",
        "dispatchable": False,
        "aliases": ["edit doc metadata", "doc metadata"],
    },
    {
        "catalog_id": "settings.docs.modal.delete-doc",
        "route": "settings.docs",
        "modal": "delete-doc",
        "kind": "blueprints_modal",
        "label": "Delete Document",
        "description": "Confirm deletion for the currently open document.",
        "target": {
            "group": "settings",
            "tab": "docs",
            "modal_id": "docs-delete-modal",
            "opener": "openDeleteDocModal",
        },
        "context_required": "active_doc",
        "dispatchable": False,
        "aliases": ["delete doc", "remove doc"],
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


def _json_list(value: Any) -> list[str]:
    if not value:
        return []
    try:
        data = json.loads(str(value))
    except (TypeError, ValueError):
        return []
    if not isinstance(data, list):
        return []
    return [str(item) for item in data if item]


def _nav_row_to_catalog_function(row: Any) -> dict[str, Any]:
    group = str(row["menu_group"] or "").strip()
    item_key = str(row["item_key"] or "").strip()
    fn_key = str(row["fn_key"] or "").strip()
    active_on = _json_list(row["active_on"])
    return {
        "catalog_id": f"{group}.function.{fn_key or item_key}",
        "route": f"{group}.{active_on[0]}" if active_on else None,
        "kind": "blueprints_menu_function",
        "group": group,
        "item_key": item_key,
        "fn_key": fn_key,
        "label": str(row["label"] or item_key).strip(),
        "parent": row["parent_key"],
        "active_on": active_on,
        "sort_order": row["sort_order"],
        "dispatchable": False,
        "representation": "catalog_only",
    }


def _doc_row_to_catalog_document(row: Any) -> dict[str, Any]:
    path = str(row["path"] or "").strip()
    doc_id = str(row["doc_id"] or "").strip()
    label = str(row["label"] or path or doc_id).strip()
    return {
        "catalog_id": f"settings.docs.document.{doc_id}",
        "route": "settings.docs",
        "kind": "blueprints_doc",
        "doc_id": doc_id,
        "path": path,
        "label": label,
        "description": row["description"],
        "tags": row["tags"],
        "group_id": row["group_id"],
        "sort_order": row["sort_order"],
        "target": {
            "group": "settings",
            "tab": "docs",
            "doc_id": doc_id,
            "path": path,
        },
        "dispatchable": True,
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
        fn_rows = conn.execute(
            """
            SELECT *
            FROM nav_items
            WHERE COALESCE(is_fn, 0) = 1
              AND menu_group IN ('synthesis', 'probes', 'settings')
            ORDER BY menu_group, sort_order, item_key
            """
        ).fetchall()
        doc_rows = conn.execute(
            """
            SELECT doc_id, label, description, tags, path, group_id, sort_order
            FROM docs
            ORDER BY sort_order, label, doc_id
            """
        ).fetchall()

    pages = [_nav_row_to_catalog_page(row) for row in rows]
    routes = {page["route"]: page for page in pages}
    modals = [dict(item) for item in _STATIC_MODAL_CATALOG]
    documents = [_doc_row_to_catalog_document(row) for row in doc_rows]
    function_surfaces = [_nav_row_to_catalog_function(row) for row in fn_rows]
    return {
        "ok": True,
        "version": _CATALOG_VERSION,
        "source": "blueprints_app",
        "contract": {
            "action_targets": ["blueprints_page", "blueprints_modal", "blueprints_doc", "docs_path"],
            "catalog_only_targets": ["blueprints_menu_function"],
            "target_schema": {
                "blueprints_page": {"required": ["kind", "route"]},
                "blueprints_modal": {"required": ["kind", "route", "modal"]},
                "blueprints_doc": {"required_any": ["doc_id", "path"]},
                "docs_path": {"required": ["kind", "path"]},
            },
            "dispatch_policy": (
                "Only cataloged targets are actionable. Low-confidence, ambiguous, "
                "context-required, or catalog-only targets return alternatives instead of dispatch."
            ),
            "minimum_dispatch_confidence": _MIN_DISPATCH_CONFIDENCE,
            "ambiguity_confidence_margin": _AMBIGUITY_CONFIDENCE_MARGIN,
        },
        "coverage": {
            "page_count": len(pages),
            "modal_count": len(modals),
            "document_count": len(documents),
            "function_surface_count": len(function_surfaces),
        },
        "pages": pages,
        "modals": modals,
        "documents": documents,
        "function_surfaces": function_surfaces,
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


def _catalog_doc_lookup(catalog: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_id: dict[str, dict[str, Any]] = {}
    by_path: dict[str, dict[str, Any]] = {}
    for doc in catalog.get("documents") or []:
        if not isinstance(doc, dict):
            continue
        doc_id = str(doc.get("doc_id") or "")
        path = str(doc.get("path") or "")
        if doc_id:
            by_id[doc_id] = doc
        if path:
            by_path[path.lower()] = doc
    return by_id, by_path


def _append_unique_warning(response: dict[str, Any], warning: dict[str, Any]) -> None:
    warnings = response.setdefault("warnings", [])
    if warning not in warnings:
        warnings.append(warning)


def _action_confidence(action: dict[str, Any]) -> float | None:
    confidence = action.get("confidence")
    if isinstance(confidence, bool):
        return None
    if isinstance(confidence, (int, float)):
        return float(confidence)
    try:
        return float(str(confidence))
    except (TypeError, ValueError):
        return None


def _action_target(action: dict[str, Any]) -> dict[str, Any]:
    return action.get("target") if isinstance(action.get("target"), dict) else {}


def _target_identity(action: dict[str, Any]) -> tuple[str, str, str, str]:
    target = _action_target(action)
    return (
        str(target.get("kind") or ""),
        str(target.get("route") or ""),
        str(target.get("modal") or ""),
        str(target.get("doc_id") or target.get("path") or ""),
    )


def _has_ambiguous_alternative(action: dict[str, Any], alternatives: list[Any]) -> bool:
    confidence = _action_confidence(action)
    if confidence is None:
        return False
    action_identity = _target_identity(action)
    for alternative in alternatives:
        if not isinstance(alternative, dict):
            continue
        alt_confidence = _action_confidence(alternative)
        if alt_confidence is None:
            continue
        if _target_identity(alternative) == action_identity:
            continue
        if alt_confidence >= confidence - _AMBIGUITY_CONFIDENCE_MARGIN:
            return True
    return False


def _action_is_ambiguous(action: dict[str, Any], alternatives: list[Any]) -> bool:
    if action.get("ambiguous") is True or action.get("ambiguity") is True:
        return True
    return _has_ambiguous_alternative(action, alternatives)


def _catalog_action(action: dict[str, Any], catalog: dict[str, Any]) -> dict[str, Any] | None:
    target = _action_target(action)
    kind = str(target.get("kind") or "")
    route = str(target.get("route") or "")
    modal = str(target.get("modal") or "")

    if kind == "blueprints_modal":
        match = _catalog_modal_lookup(catalog).get((route, modal))
        if not match:
            return None
        if match.get("dispatchable") is False:
            return {**action, "catalog_match": match["catalog_id"], "dispatch_blocked": "context_required"}
        return {
            **action,
            "catalog_match": match["catalog_id"],
            "dispatch": {
                "type": "open_modal",
                "group": match["target"].get("group"),
                "tab": match["target"].get("tab"),
                "modal": match["modal"],
                "modal_id": match["target"]["modal_id"],
                "opener": match["target"]["opener"],
            },
        }
    if kind == "blueprints_page":
        match = _catalog_page_lookup(catalog).get(route)
        if not match:
            return None
        return {
            **action,
            "catalog_match": match["catalog_id"],
            "dispatch": {
                "type": "open_page",
                "group": match["group"],
                "tab": match["tab"],
                "url": match["url"],
            },
        }
    if kind in {"blueprints_doc", "docs_path"}:
        by_id, by_path = _catalog_doc_lookup(catalog)
        doc_id = str(target.get("doc_id") or "")
        path = str(target.get("path") or "")
        match = by_id.get(doc_id) if doc_id else None
        if match is None and path:
            match = by_path.get(path.lower())
        if not match:
            return None
        return {
            **action,
            "catalog_match": match["catalog_id"],
            "dispatch": {
                "type": "open_doc",
                "group": "settings",
                "tab": "docs",
                "doc_id": match["doc_id"],
                "path": match["path"],
            },
        }
    return None


def _catalog_alternatives(items: list[Any], catalog: dict[str, Any]) -> list[dict[str, Any]]:
    alternatives: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        cataloged = _catalog_action(item, catalog)
        if cataloged is None:
            continue
        identity = _target_identity(cataloged)
        if identity in seen:
            continue
        seen.add(identity)
        alternatives.append(cataloged)
    return alternatives


def enforce_help_action_catalog(response: dict[str, Any]) -> dict[str, Any]:
    """Constrain help actions to the Blueprints-owned deterministic catalog."""
    catalog = build_help_catalog()
    response["action_catalog"] = {
        "version": catalog["version"],
        "source": catalog["source"],
        "minimum_dispatch_confidence": _MIN_DISPATCH_CONFIDENCE,
        "ambiguity_confidence_margin": _AMBIGUITY_CONFIDENCE_MARGIN,
    }
    alternatives = response.get("alternatives") if isinstance(response.get("alternatives"), list) else []
    action = response.get("action")
    if action is None:
        response["alternatives"] = _catalog_alternatives(alternatives, catalog)
        return response
    if not isinstance(action, dict):
        response["action"] = None
        response["alternatives"] = _catalog_alternatives(alternatives, catalog)
        _append_unique_warning(
            response,
            {
                "code": "help_action_not_cataloged",
                "message": "Help action was ignored because it was not an object.",
            },
        )
        return response

    confidence = _action_confidence(action)
    target = _action_target(action)
    cataloged_action = _catalog_action(action, catalog)
    cataloged_alternatives = _catalog_alternatives(alternatives, catalog)
    if cataloged_action is not None and "dispatch" not in cataloged_action:
        response["action"] = None
        response["alternatives"] = _catalog_alternatives([cataloged_action, *alternatives], catalog)
        _append_unique_warning(
            response,
            {
                "code": "help_action_not_dispatchable",
                "message": "Help action was cataloged but not dispatched because it requires UI context or is catalog-only.",
                "target": target,
            },
        )
        return response
    if confidence is None or confidence < _MIN_DISPATCH_CONFIDENCE:
        response["action"] = None
        response["alternatives"] = _catalog_alternatives([action, *alternatives], catalog)
        _append_unique_warning(
            response,
            {
                "code": "help_action_confidence_too_low",
                "message": "Help action was not dispatched because confidence was below the catalog dispatch threshold.",
                "confidence": confidence,
                "minimum_dispatch_confidence": _MIN_DISPATCH_CONFIDENCE,
                "target": target,
            },
        )
        return response
    if _action_is_ambiguous(action, alternatives):
        response["action"] = None
        response["alternatives"] = _catalog_alternatives([action, *alternatives], catalog)
        _append_unique_warning(
            response,
            {
                "code": "help_action_ambiguous",
                "message": "Help action was not dispatched because multiple catalog targets were plausible.",
                "target": target,
            },
        )
        return response
    if cataloged_action is not None:
        response["action"] = cataloged_action
        response["alternatives"] = cataloged_alternatives
        return response

    response["action"] = None
    response["alternatives"] = cataloged_alternatives
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
