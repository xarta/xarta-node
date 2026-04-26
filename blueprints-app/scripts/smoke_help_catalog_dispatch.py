#!/usr/bin/env python3
"""Smoke-check Blueprints help catalog coverage and strict action gating."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.routes_help import build_help_catalog, enforce_help_action_catalog  # noqa: E402


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def _codes(response: dict[str, Any]) -> set[str]:
    return {
        str(item.get("code"))
        for item in response.get("warnings", [])
        if isinstance(item, dict) and item.get("code")
    }


def _action(kind: str, confidence: float, **target: str) -> dict[str, Any]:
    return {
        "type": "open_page",
        "confidence": confidence,
        "target": {"kind": kind, **target},
    }


def main() -> int:
    catalog = build_help_catalog()
    _require(catalog.get("ok") is True, "catalog did not report ok=true")
    _require(catalog.get("version") == "blueprints-help-catalog-v1", "unexpected catalog version")
    _require(catalog.get("coverage", {}).get("page_count", 0) > 0, "catalog has no pages")
    _require(catalog.get("coverage", {}).get("modal_count", 0) >= 3, "catalog has too few modals")
    _require(catalog.get("coverage", {}).get("document_count", 0) > 0, "catalog has no documents")
    _require(
        catalog.get("coverage", {}).get("function_surface_count", 0) > 0,
        "catalog has no menu function surfaces",
    )

    pages = {page.get("route"): page for page in catalog.get("pages", []) if isinstance(page, dict)}
    _require("settings.docs" in pages, "catalog missing settings.docs page")
    _require("synthesis.services" in pages, "catalog missing synthesis.services page")

    modals = {
        (modal.get("route"), modal.get("modal")): modal
        for modal in catalog.get("modals", [])
        if isinstance(modal, dict)
    }
    _require(("settings.docs", "docs-search") in modals, "catalog missing docs-search modal")
    _require(("settings.docs", "new-doc") in modals, "catalog missing new-doc modal")
    _require(("settings.docs", "edit-doc-metadata") in modals, "catalog missing context-required modal")

    docs = [doc for doc in catalog.get("documents", []) if isinstance(doc, dict) and doc.get("path")]
    first_doc = docs[0]

    dispatched_page = enforce_help_action_catalog(
        {
            "warnings": [],
            "alternatives": [],
            "action": _action("blueprints_page", 0.91, route="settings.docs"),
        }
    )
    _require(dispatched_page.get("action", {}).get("dispatch", {}).get("type") == "open_page", "page did not dispatch")

    dispatched_modal = enforce_help_action_catalog(
        {
            "warnings": [],
            "alternatives": [],
            "action": _action("blueprints_modal", 0.91, route="settings.docs", modal="docs-search"),
        }
    )
    _require(
        dispatched_modal.get("action", {}).get("dispatch", {}).get("modal") == "docs-search",
        "docs-search modal did not dispatch",
    )

    dispatched_doc = enforce_help_action_catalog(
        {
            "warnings": [],
            "alternatives": [],
            "action": _action("docs_path", 0.91, path=first_doc["path"]),
        }
    )
    _require(dispatched_doc.get("action", {}).get("dispatch", {}).get("type") == "open_doc", "doc did not dispatch")

    low_confidence = enforce_help_action_catalog(
        {
            "warnings": [],
            "alternatives": [],
            "action": _action("blueprints_modal", 0.74, route="settings.docs", modal="docs-search"),
        }
    )
    _require(low_confidence.get("action") is None, "low-confidence action dispatched")
    _require("help_action_confidence_too_low" in _codes(low_confidence), "low-confidence warning missing")

    ambiguous = enforce_help_action_catalog(
        {
            "warnings": [],
            "alternatives": [_action("blueprints_page", 0.88, route="settings.docs-list")],
            "action": _action("blueprints_page", 0.91, route="settings.docs"),
        }
    )
    _require(ambiguous.get("action") is None, "ambiguous action dispatched")
    _require("help_action_ambiguous" in _codes(ambiguous), "ambiguity warning missing")

    uncataloged = enforce_help_action_catalog(
        {
            "warnings": [],
            "alternatives": [],
            "action": _action("blueprints_page", 0.95, route="settings.not-real"),
        }
    )
    _require(uncataloged.get("action") is None, "uncataloged action dispatched")
    _require("help_action_not_cataloged" in _codes(uncataloged), "uncataloged warning missing")

    context_required = enforce_help_action_catalog(
        {
            "warnings": [],
            "alternatives": [],
            "action": _action("blueprints_modal", 0.95, route="settings.docs", modal="edit-doc-metadata"),
        }
    )
    _require(context_required.get("action") is None, "context-required action dispatched")
    _require("help_action_not_dispatchable" in _codes(context_required), "context-required warning missing")

    print(
        "ok help catalog dispatch "
        f"pages={catalog['coverage']['page_count']} "
        f"modals={catalog['coverage']['modal_count']} "
        f"documents={catalog['coverage']['document_count']} "
        f"functions={catalog['coverage']['function_surface_count']}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"smoke failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
