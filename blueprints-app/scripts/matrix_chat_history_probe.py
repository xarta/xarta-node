#!/usr/bin/env python3
"""Probe Matrix Chat history latency with route-stage metrics.

This script intentionally prints only reduced room IDs/names and timing
summaries. It does not print Matrix credentials, Blueprints tokens, or message
bodies.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path("/root/xarta-node")
ENV_PATH = ROOT / ".env"
DEFAULT_BASE_URL = "http://127.0.0.1:8080"


def load_env(path: Path = ENV_PATH) -> None:
    if not path.is_file():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key and key not in os.environ:
            os.environ[key] = value.strip().strip("\"'")


def api_headers() -> dict[str, str]:
    secret = os.environ.get("BLUEPRINTS_API_SECRET", "").strip()
    if not secret:
        return {}
    token = hmac.new(
        bytes.fromhex(secret),
        str(int(time.time()) // 5).encode(),
        hashlib.sha256,
    ).hexdigest()
    return {"x-api-token": token}


def request_json(base_url: str, path: str, *, timeout: float) -> tuple[dict[str, Any], float]:
    request = urllib.request.Request(base_url.rstrip("/") + path, headers=api_headers())
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = json.loads(response.read().decode("utf-8"))
    except TimeoutError as exc:
        elapsed = time.monotonic() - started
        raise RuntimeError(f"timeout after {timeout:.1f}s for {path}") from exc
    except urllib.error.URLError as exc:
        elapsed = time.monotonic() - started
        raise RuntimeError(f"{type(exc).__name__} after {elapsed:.3f}s for {path}") from exc
    except json.JSONDecodeError as exc:
        elapsed = time.monotonic() - started
        raise RuntimeError(f"invalid JSON after {elapsed:.3f}s for {path}") from exc
    return body if isinstance(body, dict) else {}, time.monotonic() - started


def room_label(room: dict[str, Any]) -> str:
    for key in ("name", "canonical_alias", "room_id"):
        value = room.get(key)
        if isinstance(value, str) and value:
            return value
    return "(unnamed)"


def room_id_from(room: dict[str, Any]) -> str:
    value = room.get("room_id")
    return value if isinstance(value, str) else ""


def probe_room(
    *,
    base_url: str,
    server: str,
    room_id: str,
    label: str,
    limit: int,
    timeout: float,
) -> dict[str, Any]:
    encoded_room = urllib.parse.quote(room_id, safe="")
    path = (
        f"/api/v1/matrix-chat/rooms/{encoded_room}/messages"
        f"?server={urllib.parse.quote(server)}&limit={limit}&metrics=true&order=recent"
    )
    try:
        body, elapsed = request_json(base_url, path, timeout=timeout)
    except RuntimeError as exc:
        return {
            "server": server,
            "room_id": room_id,
            "room": label,
            "ok": False,
            "error": str(exc),
        }
    metrics = body.get("metrics") if isinstance(body.get("metrics"), dict) else {}
    return {
        "server": server,
        "room_id": room_id,
        "room": label,
        "ok": True,
        "elapsed_seconds": round(elapsed, 6),
        "message_count": len(body.get("messages") or []),
        "raw_event_count": metrics.get("raw_event_count"),
        "encrypted_event_count": metrics.get("encrypted_event_count"),
        "decoded_message_count": metrics.get("decoded_message_count"),
        "undecryptable_event_count": metrics.get("undecryptable_event_count"),
        "source_order": metrics.get("source_order"),
        "response_order": metrics.get("response_order"),
        "history_read_only_crypto": metrics.get("history_read_only_crypto"),
        "stage_seconds": metrics.get("stage_seconds") or {},
        "stage_counts": metrics.get("stage_counts") or {},
    }


def server_rooms(base_url: str, server: str, timeout: float) -> list[dict[str, Any]]:
    path = f"/api/v1/matrix-chat/rooms?server={urllib.parse.quote(server)}"
    body, _elapsed = request_json(base_url, path, timeout=timeout)
    rooms = body.get("joined")
    return [room for room in rooms if isinstance(room, dict)] if isinstance(rooms, list) else []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--server", choices=["tb1", "vps", "all"], default="all")
    parser.add_argument("--room-id", action="append", default=[])
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=6.0)
    parser.add_argument("--max-rooms", type=int, default=12)
    args = parser.parse_args(argv)

    load_env()
    servers = ["tb1", "vps"] if args.server == "all" else [args.server]
    results: list[dict[str, Any]] = []
    for server in servers:
        if args.room_id:
            rooms = [{"room_id": room_id, "name": room_id} for room_id in args.room_id]
        else:
            try:
                rooms = server_rooms(args.base_url, server, args.timeout)
            except RuntimeError as exc:
                results.append({"server": server, "ok": False, "error": str(exc)})
                continue
        for room in rooms[: max(1, args.max_rooms)]:
            room_id = room_id_from(room)
            if not room_id:
                continue
            results.append(
                probe_room(
                    base_url=args.base_url,
                    server=server,
                    room_id=room_id,
                    label=room_label(room),
                    limit=max(1, min(args.limit, 100)),
                    timeout=args.timeout,
                )
            )

    print(json.dumps({"ok": all(item.get("ok") for item in results), "results": results}, indent=2))
    return 0 if all(item.get("ok") for item in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
