#!/usr/bin/env python3
"""Smoke-check Blueprints nullclaw-docs-search proxy routes."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import sys
import time
from typing import Any

import httpx


def compute_token(secret_hex: str) -> str:
    return hmac.new(
        bytes.fromhex(secret_hex),
        str(int(time.time()) // 5).encode(),
        hashlib.sha256,
    ).hexdigest()


def post_json(client: httpx.Client, path: str, body: dict[str, Any]) -> dict[str, Any]:
    response = client.post(path, json=body)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    return data


def get_json(client: httpx.Client, path: str) -> dict[str, Any]:
    response = client.get(path)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise AssertionError(f"{path} returned non-object JSON")
    return data


def require_common(path: str, data: dict[str, Any]) -> str:
    if data.get("ok") is not True:
        raise AssertionError(f"{path} did not report ok=true: {data.get('error') or data}")
    task_id = data.get("task_id")
    if not isinstance(task_id, str) or not task_id.startswith("task_"):
        raise AssertionError(f"{path} missing task_id")
    evidence = data.get("evidence")
    if not isinstance(evidence, dict):
        raise AssertionError(f"{path} missing evidence block")
    if evidence.get("content_is_untrusted_evidence") is not True:
        raise AssertionError(f"{path} did not preserve evidence policy")
    if not isinstance(evidence.get("documents"), list):
        raise AssertionError(f"{path} evidence.documents is not a list")
    documents = evidence["documents"]
    if documents and not any(isinstance(doc.get("text"), str) and doc["text"] for doc in documents):
        raise AssertionError(f"{path} did not preserve document text evidence")
    if not isinstance(data.get("sources"), list):
        raise AssertionError(f"{path} sources is not a list")
    if not isinstance(data.get("warnings"), list):
        raise AssertionError(f"{path} warnings is not a list")
    return task_id


def require_catalog(data: dict[str, Any]) -> None:
    if data.get("ok") is not True:
        raise AssertionError(f"/api/v1/help/catalog did not report ok=true: {data}")
    if data.get("version") != "blueprints-help-catalog-v1":
        raise AssertionError("/api/v1/help/catalog returned an unexpected version")
    pages = data.get("pages")
    modals = data.get("modals")
    if not isinstance(pages, list):
        raise AssertionError("/api/v1/help/catalog pages is not a list")
    if not isinstance(modals, list):
        raise AssertionError("/api/v1/help/catalog modals is not a list")
    docs_search = next(
        (
            modal for modal in modals
            if isinstance(modal, dict)
            and modal.get("route") == "settings.docs"
            and modal.get("modal") == "docs-search"
        ),
        None,
    )
    if not docs_search:
        raise AssertionError("/api/v1/help/catalog missing settings.docs docs-search modal")
    coverage = data.get("coverage")
    if not isinstance(coverage, dict):
        raise AssertionError("/api/v1/help/catalog missing coverage block")
    for key in ("page_count", "modal_count", "document_count", "function_surface_count"):
        if not isinstance(coverage.get(key), int) or coverage[key] <= 0:
            raise AssertionError(f"/api/v1/help/catalog coverage.{key} is empty")
    contract = data.get("contract")
    if not isinstance(contract, dict):
        raise AssertionError("/api/v1/help/catalog missing contract block")
    if "blueprints_doc" not in (contract.get("action_targets") or []):
        raise AssertionError("/api/v1/help/catalog does not advertise blueprints_doc targets")
    if "blueprints_menu_function" not in (contract.get("catalog_only_targets") or []):
        raise AssertionError("/api/v1/help/catalog does not advertise catalog-only menu functions")


def require_map_reduce_explain(data: dict[str, Any]) -> None:
    require_common("/api/v1/docs/search/explain map_reduce", data)
    strict = data.get("strict_evidence")
    if not isinstance(strict, dict):
        raise AssertionError("map_reduce explain missing strict_evidence")
    if strict.get("answerable") is not True:
        raise AssertionError(f"map_reduce explain was not answerable: {strict.get('answerability')}")
    if strict.get("answerability") not in {"supported", "supported_with_unknown_metadata", "direction_only"}:
        raise AssertionError(f"map_reduce explain returned unexpected answerability: {strict.get('answerability')}")
    map_reduce = strict.get("map_reduce")
    if not isinstance(map_reduce, dict) or map_reduce.get("enabled") is not True:
        raise AssertionError("map_reduce explain did not enable strict_evidence.map_reduce")
    if map_reduce.get("final_answer_input") != "strict_evidence_only":
        raise AssertionError("map_reduce explain does not declare strict_evidence_only final input")
    counts = strict.get("claim_count_by_category")
    if not isinstance(counts, dict):
        raise AssertionError("map_reduce explain missing claim_count_by_category")
    if sum(int(value or 0) for value in counts.values()) != strict.get("claim_count"):
        raise AssertionError("map_reduce explain claim count mismatch")
    if int(counts.get("current") or 0) < 1:
        raise AssertionError("map_reduce explain has no current claims")
    claims = strict.get("claims")
    if not isinstance(claims, dict) or not isinstance(claims.get("current"), list):
        raise AssertionError("map_reduce explain missing current claim list")
    if not all((claim or {}).get("source_category") == "current" for claim in claims["current"]):
        raise AssertionError("map_reduce explain current claims have wrong source category")
    graph = strict.get("graph_expansion")
    if not isinstance(graph, dict) or graph.get("enabled") is not True:
        raise AssertionError("map_reduce explain missing graph_expansion metadata")
    if graph.get("accepted") and not any(
        isinstance(source, dict) and source.get("retrieval_stage") == "graph_expansion"
        for source in strict.get("sources", [])
    ):
        raise AssertionError("map_reduce explain accepted graph docs without graph source provenance")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.getenv("BLUEPRINTS_API_URL", "http://127.0.0.1:8080"))
    parser.add_argument(
        "--query",
        default="How do I use Blueprints Docs Search hybrid mode?",
        help="Query to send through the help/docs synthesis routes.",
    )
    args = parser.parse_args()

    headers: dict[str, str] = {}
    secret = os.getenv("BLUEPRINTS_API_SECRET", "")
    if secret:
        headers["x-api-token"] = compute_token(secret)

    body = {
        "query": args.query,
        "search_mode": "hybrid",
        "max_docs": 3,
        "max_chars_per_doc": 2500,
        "top_k": 5,
    }
    checks = [
        ("/api/v1/help/turn", body, ("short_response", "modal_response", "action")),
        ("/api/v1/help/short", body, ("short_response",)),
        ("/api/v1/help/action", body, ("action", "alternatives")),
        ("/api/v1/help/modal", body, ("modal_response",)),
        ("/api/v1/docs/search/explain", {**body, "explanation_mode": "answer"}, ("answer",)),
    ]

    with httpx.Client(base_url=args.base_url.rstrip("/"), headers=headers, timeout=90.0) as client:
        catalog = get_json(client, "/api/v1/help/catalog")
        require_catalog(catalog)
        print("ok /api/v1/help/catalog")

        turn_task_id = ""
        for path, payload, expected_keys in checks:
            data = post_json(client, path, payload)
            task_id = require_common(path, data)
            if path in {"/api/v1/help/turn", "/api/v1/help/action"}:
                if not isinstance(data.get("action_catalog"), dict):
                    raise AssertionError(f"{path} missing action_catalog metadata")
                action = data.get("action")
                if action is not None and not isinstance(action.get("dispatch"), dict):
                    raise AssertionError(f"{path} action was not catalog-dispatchable")
            if path == "/api/v1/help/turn":
                turn_task_id = task_id
            for key in expected_keys:
                if key not in data:
                    raise AssertionError(f"{path} missing {key}")
            print(f"ok {path} task_id={task_id}")

        fetched = get_json(client, f"/api/v1/help/turns/{turn_task_id}")
        require_common("/api/v1/help/turns/{id}", fetched)
        print(f"ok /api/v1/help/turns/{{id}} task_id={turn_task_id}")

        map_reduce_body = {
            **body,
            "query": "How is TurboVec Docs wired into Blueprints Docs Search?",
            "folder": "turbovec",
            "allowed_paths": ["turbovec/"],
            "graph_expand": True,
            "max_graph_hops": 1,
            "max_graph_docs": 4,
            "map_reduce": True,
            "explanation_mode": "answer",
        }
        map_reduce = post_json(client, "/api/v1/docs/search/explain", map_reduce_body)
        require_map_reduce_explain(map_reduce)
        print(f"ok /api/v1/docs/search/explain map_reduce task_id={map_reduce.get('task_id')}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"smoke failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
