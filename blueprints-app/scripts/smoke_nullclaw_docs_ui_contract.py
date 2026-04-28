#!/usr/bin/env python3
"""Smoke-check Blueprints help/docs UI contract and explain display metadata."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import httpx


def compute_token(secret_hex: str) -> str:
    return hmac.new(
        bytes.fromhex(secret_hex),
        str(int(time.time()) // 5).encode(),
        hashlib.sha256,
    ).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def post_json(client: httpx.Client, path: str, body: dict[str, Any]) -> dict[str, Any]:
    response = client.post(path, json=body)
    response.raise_for_status()
    data = response.json()
    require(isinstance(data, dict), f"{path} returned non-object JSON")
    return data


def get_json(client: httpx.Client, path: str) -> dict[str, Any]:
    response = client.get(path)
    response.raise_for_status()
    data = response.json()
    require(isinstance(data, dict), f"{path} returned non-object JSON")
    return data


def check_gui_contract(gui_root: Path) -> None:
    help_js = (gui_root / "js" / "help-surface.js").read_text(encoding="utf-8")
    docs_search_js = (gui_root / "js" / "settings" / "docs-search.js").read_text(encoding="utf-8")
    docs_js = (gui_root / "js" / "settings" / "docs.js").read_text(encoding="utf-8")
    index_html = (gui_root / "index.html").read_text(encoding="utf-8")

    match = re.search(
        r"async function executeAction\(.*?\n  function open\(", help_js, flags=re.DOTALL
    )
    require(match is not None, "help-surface executeAction function not found")
    execute_action = match.group(0)
    require(
        "chosen?.dispatch" in execute_action,
        "help action executor does not require dispatch metadata",
    )
    for natural_language_field in ("short_response", "modal_response", "answer"):
        require(
            natural_language_field not in execute_action,
            f"help action executor references natural-language field {natural_language_field}",
        )
    require(
        "mode: 'stream'" in help_js or 'mode: "stream"' in help_js,
        "help TTS playback is not configured for streaming",
    )

    require(
        "/api/v1/docs/search/explain" in docs_search_js, "Docs Search explain endpoint is not wired"
    )
    require(
        "data.display" in docs_search_js,
        "Docs Search explain path does not require display metadata",
    )
    require(
        "display.summary" in docs_search_js,
        "Docs Search explain summary is not rendered from display",
    )
    require(
        "display.markdown" in docs_search_js,
        "Docs Search explain markdown is not rendered from display",
    )
    require(
        'id="docs-search-explain"' in index_html, "Docs Search Explain button missing from modal"
    )
    require(
        'id="docs-search-explain-panel"' in index_html,
        "Docs Search explain panel missing from modal",
    )
    require(
        'id="docs-folder-tree-explain-sources-btn"' in index_html,
        "Docs tree explain sources button missing",
    )
    require(
        'id="docs-folder-tree-sources-modal"' in index_html,
        "Docs tree explain sources modal missing",
    )
    require(
        'id="docs-folder-tree-explain-sources"' in index_html,
        "Docs tree explain source list missing",
    )
    require(
        'id="docs-folder-tree-status-pill"' in index_html,
        "Docs tree status pill missing from modal",
    )
    require('id="docs-folder-tree-status-modal"' in index_html, "Docs tree status modal missing")
    require("/api/v1/docs/search/status" in docs_js, "Docs tree status endpoint is not wired")
    require(
        "_docsFolderTreeRequestSeq" in docs_js,
        "Docs tree modal does not guard stale async requests",
    )
    require(
        "_docsFolderTreeSearchCache" in docs_js,
        "Docs tree search query is not cached between modal opens",
    )
    require("sessionStorage.setItem" in docs_js, "Docs tree search cache is not session-backed")
    require("data-source-path" in docs_js, "Docs tree source rows are not wired to source paths")
    require(
        "_docsFolderTreeOpenSourceDoc" in docs_js, "Docs tree source rows do not open viewer docs"
    )


def check_api_contract(client: httpx.Client, query: str) -> None:
    body = {
        "query": query,
        "search_mode": "hybrid",
        "max_docs": 3,
        "max_chars_per_doc": 2500,
        "top_k": 5,
        "voice": True,
    }

    turn = post_json(client, "/api/v1/help/turn", body)
    require(turn.get("ok") is True, "/api/v1/help/turn did not report ok=true")
    short = turn.get("short_response")
    require(isinstance(short, dict), "/api/v1/help/turn missing short_response")
    text = short.get("text")
    require(isinstance(text, str) and text.strip(), "short_response.text is empty")
    require(short.get("tts_ready") is True, "short_response.tts_ready is not true")
    require(short.get("voice_safe") is True, "short_response.voice_safe is not true")
    require(short.get("format") == "plain_text", "short_response.format is not plain_text")
    require(
        short.get("playback_transport") == "streaming_tts",
        "short_response does not advertise streaming TTS",
    )
    require(
        short.get("length_is_tts_limit") is False,
        "short_response incorrectly marks length as a TTS limit",
    )
    require(
        short.get("length_reason") == "conversational_brevity",
        "short_response length reason is unclear",
    )
    require(
        "`" not in text and not re.search(r"https?://|\[(?:S|s)\d+\]", text),
        "short_response is not TTS-shaped",
    )

    action = turn.get("action")
    require(
        isinstance(turn.get("action_catalog"), dict), "/api/v1/help/turn missing action_catalog"
    )
    if action is not None:
        require(isinstance(action, dict), "help action is not an object")
        require(
            isinstance(action.get("dispatch"), dict),
            "cataloged help action missing dispatch metadata",
        )

    explain = post_json(
        client, "/api/v1/docs/search/explain", {**body, "explanation_mode": "answer"}
    )
    require(explain.get("ok") is True, "/api/v1/docs/search/explain did not report ok=true")
    display = explain.get("display")
    require(isinstance(display, dict), "/api/v1/docs/search/explain missing display")
    require(
        isinstance(display.get("summary"), str) and display["summary"].strip(),
        "display.summary is empty",
    )
    require(isinstance(display.get("markdown"), str), "display.markdown is not a string")
    require(isinstance(display.get("source_count"), int), "display.source_count is not an int")
    require(
        isinstance(display.get("evidence_document_count"), int),
        "display.evidence_document_count is not an int",
    )
    require(isinstance(display.get("sources"), list), "display.sources is not a list")
    require(
        display.get("source_count") == len(display["sources"]),
        "display.source_count does not match display.sources",
    )
    require(
        display.get("source_count", 0) > 0,
        "display.source_count is empty for grounded explain query",
    )
    require(
        display.get("content_is_grounded_evidence") is True,
        "display missing grounded evidence flag",
    )

    status = get_json(client, "/api/v1/docs/search/status")
    require(
        status.get("status") in {"green", "amber", "red"},
        "docs search status missing traffic-light state",
    )
    metrics = status.get("metrics")
    checks = status.get("checks")
    require(isinstance(metrics, dict), "docs search status missing metrics")
    require(isinstance(checks, list) and checks, "docs search status missing checks")
    require(
        isinstance(metrics.get("turbovec_documents"), int), "status missing TurboVec document count"
    )
    require(isinstance(metrics.get("turbovec_chunks"), int), "status missing TurboVec chunk count")
    require(isinstance(metrics.get("graph_edges"), int), "status missing graph edge count")
    require(
        status.get("quality_status") in {"green", "amber", "red"},
        "status missing quality traffic-light state",
    )
    quality = status.get("quality")
    require(isinstance(quality, dict), "docs search status missing quality block")
    require(
        quality.get("backlog_endpoint") == "/api/v1/docs/search/quality",
        "quality block missing backlog endpoint",
    )
    require(
        any(isinstance(check, dict) and check.get("name") == "local_ai" for check in checks),
        "status missing local_ai check",
    )
    required_status_checks = {
        "docs_embeddings_model",
        "docs_reranker_model",
        "turbovec_llm_model",
        "synthesis_llm_model",
    }
    present = {check.get("name") for check in checks if isinstance(check, dict)}
    missing = sorted(required_status_checks - present)
    require(not missing, f"status missing model checks: {', '.join(missing)}")

    backlog = get_json(client, "/api/v1/docs/search/quality?limit=5")
    require(backlog.get("status") in {"green", "amber", "red"}, "quality report missing status")
    require(isinstance(backlog.get("metrics"), dict), "quality report missing metrics")
    require(isinstance(backlog.get("items"), list), "quality report missing items")
    for item in backlog.get("items", []):
        require(isinstance(item.get("path"), str) and item["path"], "quality item missing path")
        require(
            isinstance(item.get("retrieval_frequency"), int),
            "quality item missing retrieval frequency",
        )
        require(
            isinstance(item.get("inbound_graph_links"), int),
            "quality item missing inbound graph links",
        )
        require(
            isinstance(item.get("folder_importance"), int), "quality item missing folder importance"
        )


def check_tts_stream(client: httpx.Client) -> None:
    payload = {
        "text": "Docs Search is ready.",
        "mode": "stream",
        "interrupt": True,
        "fallback_kind": "positive",
    }
    with client.stream("POST", "/api/v1/tts/speak", json=payload) as response:
        response.raise_for_status()
        content_type = (response.headers.get("content-type") or "").lower()
        engine = response.headers.get("x-blueprints-tts-engine")
        require(content_type.startswith("audio/"), "TTS stream did not return audio content")
        require(engine == "pockettts_stream", f"TTS stream used unexpected engine: {engine}")
        first_chunk = next(response.iter_bytes(), b"")
        require(len(first_chunk) > 0, "TTS streaming response returned no audio bytes")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url", default=os.getenv("BLUEPRINTS_API_URL", "http://127.0.0.1:8080")
    )
    parser.add_argument(
        "--gui-root", default=os.getenv("BLUEPRINTS_GUI_ROOT", "/xarta-node/gui-fallback")
    )
    parser.add_argument("--query", default="How do I use Blueprints Docs Search hybrid mode?")
    args = parser.parse_args()

    gui_root = Path(args.gui_root)
    require(gui_root.exists(), f"GUI root does not exist: {gui_root}")
    check_gui_contract(gui_root)
    print("ok static help/docs UI contract")

    headers: dict[str, str] = {}
    secret = os.getenv("BLUEPRINTS_API_SECRET", "")
    if secret:
        headers["x-api-token"] = compute_token(secret)

    with httpx.Client(base_url=args.base_url.rstrip("/"), headers=headers, timeout=90.0) as client:
        check_api_contract(client, args.query)
        check_tts_stream(client)
    print("ok help/docs API display and dispatch contract")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"smoke failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
