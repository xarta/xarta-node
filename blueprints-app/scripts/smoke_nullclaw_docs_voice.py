#!/usr/bin/env python3
"""Smoke-check warm nullclaw-docs-search readiness and voice-shaped help output."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import re
import sys
import time
from typing import Any

import httpx

VOICE_MAX_CHARS = 180


def compute_token(secret_hex: str) -> str:
    return hmac.new(
        bytes.fromhex(secret_hex),
        str(int(time.time()) // 5).encode(),
        hashlib.sha256,
    ).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def get_json(client: httpx.Client, path: str) -> dict[str, Any]:
    response = client.get(path)
    response.raise_for_status()
    data = response.json()
    require(isinstance(data, dict), f"{path} returned non-object JSON")
    return data


def post_json(client: httpx.Client, path: str, body: dict[str, Any]) -> dict[str, Any]:
    response = client.post(path, json=body)
    response.raise_for_status()
    data = response.json()
    require(isinstance(data, dict), f"{path} returned non-object JSON")
    return data


def require_short_response(path: str, data: dict[str, Any]) -> None:
    require(data.get("ok") is True, f"{path} did not report ok=true")
    short = data.get("short_response")
    require(isinstance(short, dict), f"{path} missing short_response object")
    text = short.get("text")
    require(isinstance(text, str) and text.strip(), f"{path} short_response.text is empty")
    require(len(text) <= VOICE_MAX_CHARS, f"{path} short_response.text is too long: {len(text)}")
    require(short.get("tts_ready") is True, f"{path} short_response.tts_ready is not true")
    require(short.get("voice_safe") is True, f"{path} short_response.voice_safe is not true")
    require(short.get("format") == "plain_text", f"{path} short_response.format is not plain_text")
    require(
        short.get("playback_transport") == "streaming_tts",
        f"{path} short_response.playback_transport is not streaming_tts",
    )
    require(short.get("length_is_tts_limit") is False, f"{path} short response incorrectly marks length as a TTS limit")
    require(short.get("length_reason") == "conversational_brevity", f"{path} short response missing length_reason")
    require("```" not in text and "`" not in text, f"{path} short_response contains markdown code")
    require(not re.search(r"\[(?:S|s)\d+\]", text), f"{path} short_response contains citation markup")
    require(not re.search(r"https?://", text), f"{path} short_response contains a URL")
    require(isinstance(data.get("evidence"), dict), f"{path} missing evidence block")
    require(data["evidence"].get("content_is_untrusted_evidence") is True, f"{path} missing evidence policy")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.getenv("BLUEPRINTS_API_URL", "http://127.0.0.1:8080"))
    parser.add_argument(
        "--worker-url",
        default=os.getenv("NULLCLAW_DOCS_SEARCH_URL", "http://127.0.0.1:19081"),
    )
    parser.add_argument(
        "--query",
        default="How do I use Blueprints Docs Search hybrid mode?",
        help="Voice-shaped help query to send through Blueprints.",
    )
    args = parser.parse_args()

    headers: dict[str, str] = {}
    secret = os.getenv("BLUEPRINTS_API_SECRET", "")
    if secret:
        headers["x-api-token"] = compute_token(secret)

    with httpx.Client(base_url=args.worker_url.rstrip("/"), timeout=20.0) as worker:
        health = get_json(worker, "/health")
        require(health.get("ok") is True, "nullclaw-docs-search health did not report ok=true")
        deps = health.get("dependencies")
        require(isinstance(deps, dict), "nullclaw-docs-search health missing dependencies")
        require(deps.get("turbovec_docs", {}).get("ok") is True, "turbovec-docs dependency is not healthy")
        require(deps.get("task_store", {}).get("ok") is True, "task store dependency is not healthy")
        print("ok nullclaw-docs-search /health")

    body = {
        "query": args.query,
        "search_mode": "hybrid",
        "max_docs": 3,
        "max_chars_per_doc": 2500,
        "top_k": 5,
        "voice": True,
    }
    with httpx.Client(base_url=args.base_url.rstrip("/"), headers=headers, timeout=90.0) as client:
        short = post_json(client, "/api/v1/help/short", body)
        require_short_response("/api/v1/help/short", short)
        print(f"ok /api/v1/help/short chars={len(short['short_response']['text'])}")

        turn = post_json(client, "/api/v1/help/turn", body)
        require_short_response("/api/v1/help/turn", turn)
        print(f"ok /api/v1/help/turn chars={len(turn['short_response']['text'])}")

        explain = post_json(client, "/api/v1/docs/search/explain", {**body, "explanation_mode": "answer"})
        require(explain.get("ok") is True, "/api/v1/docs/search/explain did not report ok=true")
        display = explain.get("display")
        require(isinstance(display, dict), "/api/v1/docs/search/explain missing display block")
        require(isinstance(display.get("summary"), str) and display["summary"], "display.summary is empty")
        require(isinstance(display.get("source_count"), int), "display.source_count is not an int")
        print("ok /api/v1/docs/search/explain display")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"smoke failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
