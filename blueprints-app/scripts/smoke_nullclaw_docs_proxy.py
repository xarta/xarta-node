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
        turn_task_id = ""
        for path, payload, expected_keys in checks:
            data = post_json(client, path, payload)
            task_id = require_common(path, data)
            if path == "/api/v1/help/turn":
                turn_task_id = task_id
            for key in expected_keys:
                if key not in data:
                    raise AssertionError(f"{path} missing {key}")
            print(f"ok {path} task_id={task_id}")

        fetched = get_json(client, f"/api/v1/help/turns/{turn_task_id}")
        require_common("/api/v1/help/turns/{id}", fetched)
        print(f"ok /api/v1/help/turns/{{id}} task_id={turn_task_id}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"smoke failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
