#!/usr/bin/env python3
"""Probe Blueprints API cross-route contention.

The probe intentionally prints reduced timing summaries only. It does not print
API secrets, Matrix credentials, cookies, message bodies, or full response
payloads.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
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


def _short_ref(value: str) -> str:
    if not value:
        return ""
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]
    suffix = value[-8:] if len(value) > 8 else value
    return f"sha256:{digest}:{suffix}"


def _json_metrics(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        return {}
    metrics = body.get("metrics") if isinstance(body.get("metrics"), dict) else {}
    if not metrics:
        metrics = body.get("server_metrics") if isinstance(body.get("server_metrics"), dict) else {}
    reduced: dict[str, Any] = {}
    for key in (
        "schema",
        "total_seconds",
        "state_lock_wait_seconds",
        "state_read_and_payload_seconds",
        "status_thread_seconds",
        "raw_event_count",
        "filtered_event_count",
        "encrypted_event_count",
        "decoded_message_count",
        "undecryptable_event_count",
        "skipped_event_count",
        "cancelled",
        "timed_out",
        "timeout_budget_seconds",
        "include_capabilities",
        "include_recent",
        "include_contracts",
    ):
        if key in metrics:
            reduced[key] = metrics.get(key)
    stage_seconds = metrics.get("stage_seconds")
    if isinstance(stage_seconds, dict):
        reduced["stage_seconds"] = {
            str(key): stage_seconds[key] for key in sorted(stage_seconds) if key in stage_seconds
        }
    stage_counts = metrics.get("stage_counts")
    if isinstance(stage_counts, dict):
        reduced["stage_counts"] = {
            str(key): stage_counts[key] for key in sorted(stage_counts) if key in stage_counts
        }
    return reduced


def _status_hints(body: Any) -> dict[str, Any]:
    if not isinstance(body, dict):
        return {}
    hints: dict[str, Any] = {}
    if "ok" in body:
        hints["ok"] = bool(body.get("ok"))
    if "status" in body and isinstance(body.get("status"), str):
        hints["status"] = body.get("status")
    if "schema" in body and isinstance(body.get("schema"), str):
        hints["schema"] = body.get("schema")
    if "client_count" in body:
        hints["client_count"] = body.get("client_count")
    if "count" in body:
        hints["count"] = body.get("count")
    if isinstance(body.get("review_processor"), dict):
        processor = body["review_processor"]
        hints["review_processor"] = {
            "status": processor.get("status"),
            "queue_length": processor.get("queue_length"),
        }
    if isinstance(body.get("preprocessing"), dict):
        preprocessing = body["preprocessing"]
        hints["preprocessing"] = {
            "status": preprocessing.get("status"),
            "queue_length": preprocessing.get("queue_length"),
        }
    if isinstance(body.get("view"), dict):
        view = body["view"]
        page = view.get("page") if isinstance(view.get("page"), dict) else {}
        hints["view"] = {
            "page": {
                "group": page.get("group"),
                "tab": page.get("tab"),
                "ready": page.get("ready"),
                "api_in_flight": page.get("api_in_flight"),
            },
            "frontend_asset_version_match": view.get("frontend_asset_version_match"),
        }
    return hints


@dataclass(frozen=True)
class RequestSpec:
    name: str
    path: str
    category: str = "normal"


def _fetch(base_url: str, spec: RequestSpec, timeout: float) -> dict[str, Any]:
    url = base_url.rstrip("/") + spec.path
    request = urllib.request.Request(url, headers=api_headers())
    started = time.monotonic()
    status = 0
    body_bytes = b""
    error = ""
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            status = int(response.status)
            body_bytes = response.read()
    except urllib.error.HTTPError as exc:
        status = int(exc.code)
        body_bytes = exc.read(8192)
        error = f"http_{exc.code}"
    except TimeoutError:
        error = "client_timeout"
    except urllib.error.URLError as exc:
        error = type(exc.reason).__name__ if getattr(exc, "reason", None) else type(exc).__name__
    elapsed = time.monotonic() - started
    json_started = time.monotonic()
    body: Any = None
    json_error = ""
    if body_bytes:
        try:
            body = json.loads(body_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            json_error = type(exc).__name__
    json_elapsed = time.monotonic() - json_started
    return {
        "name": spec.name,
        "category": spec.category,
        "status": status,
        "ok": 200 <= status < 300 and not error,
        "wall_ms": round(elapsed * 1000, 3),
        "response_bytes": len(body_bytes),
        "json_parse_ms": round(json_elapsed * 1000, 3),
        "error": error,
        "json_error": json_error,
        "metrics": _json_metrics(body),
        "hints": _status_hints(body),
    }


async def fetch_one(
    base_url: str,
    spec: RequestSpec,
    *,
    timeout: float,
    planned_offset_ms: int = 0,
    suite_started: float,
) -> dict[str, Any]:
    if planned_offset_ms > 0:
        await asyncio.sleep(planned_offset_ms / 1000)
    actual_offset_ms = int((time.monotonic() - suite_started) * 1000)
    result = await asyncio.to_thread(_fetch, base_url, spec, timeout)
    result["planned_offset_ms"] = planned_offset_ms
    result["actual_offset_ms"] = actual_offset_ms
    return result


async def run_specs(
    base_url: str,
    specs: list[RequestSpec],
    *,
    timeout: float,
    stagger_ms: int = 0,
) -> list[dict[str, Any]]:
    suite_started = time.monotonic()
    tasks = [
        fetch_one(
            base_url,
            spec,
            timeout=timeout,
            planned_offset_ms=index * max(0, stagger_ms),
            suite_started=suite_started,
        )
        for index, spec in enumerate(specs)
    ]
    return await asyncio.gather(*tasks)


def _request_json(base_url: str, path: str, *, timeout: float) -> dict[str, Any]:
    result = _fetch(base_url, RequestSpec("discover", path), timeout)
    if not result["ok"]:
        raise RuntimeError(f"discovery failed status={result['status']} error={result['error']}")
    request = urllib.request.Request(base_url.rstrip("/") + path, headers=api_headers())
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = json.loads(response.read().decode("utf-8"))
    return body if isinstance(body, dict) else {}


def discover_matrix_room(
    base_url: str,
    *,
    server: str,
    room_id: str,
    room_hint: str,
    timeout: float,
) -> tuple[str, dict[str, Any]]:
    if room_id:
        return room_id, {"source": "argument", "room_ref": _short_ref(room_id)}
    hint = room_hint.strip().lower()
    rooms_path = f"/api/v1/matrix-chat/rooms?server={urllib.parse.quote(server)}"
    body = _request_json(base_url, rooms_path, timeout=timeout)
    joined = body.get("joined") if isinstance(body.get("joined"), list) else []
    candidates = [room for room in joined if isinstance(room, dict)]
    selected: dict[str, Any] | None = None
    if hint:
        for room in candidates:
            haystack = " ".join(
                str(room.get(key) or "") for key in ("name", "canonical_alias", "room_id")
            ).lower()
            if hint in haystack:
                selected = room
                break
    selected = selected or (candidates[0] if candidates else None)
    if not selected or not isinstance(selected.get("room_id"), str):
        raise RuntimeError(f"no joined Matrix room discovered for server={server}")
    selected_id = selected["room_id"]
    return selected_id, {
        "source": "rooms",
        "server": server,
        "room_ref": _short_ref(selected_id),
        "room_count": len(candidates),
        "matched_hint": bool(hint and selected is not None),
    }


def build_specs(args: argparse.Namespace, room_id: str | None) -> dict[str, RequestSpec]:
    specs = {
        "auth_time": RequestSpec("auth_time", "/api/v1/auth/time", "cheap"),
        "health": RequestSpec("health", "/health", "cheap"),
        "browser_view": RequestSpec(
            "active_browser_view",
            "/api/v1/voice-mode/active-browser-view?metrics=true",
            "browser",
        ),
        "browser_clients": RequestSpec(
            "browser_clients",
            "/api/v1/voice-mode/browser-clients?metrics=true",
            "browser",
        ),
        "kanban_status": RequestSpec(
            "kanban_automation_status",
            f"/api/v1/personal/kanban/automation/status?limit={args.kanban_status_limit}&include_contracts=false&metrics=true",
            "kanban",
        ),
        "kanban_datastore": RequestSpec(
            "kanban_datastore_status", "/api/v1/personal/kanban/datastore/status", "kanban"
        ),
    }
    if room_id:
        encoded_room = urllib.parse.quote(room_id, safe="")
        matrix_path = (
            f"/api/v1/matrix-chat/rooms/{encoded_room}/messages"
            f"?server={urllib.parse.quote(args.matrix_server)}"
            f"&limit={args.matrix_limit}"
            "&metrics=true"
            "&order=recent"
            f"&decrypt_timeout_ms={args.matrix_decrypt_timeout_ms}"
        )
        specs["matrix_messages"] = RequestSpec("matrix_messages", matrix_path, "matrix")
    return specs


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * fraction)))
    return ordered[index]


def summarize_batch(name: str, results: list[dict[str, Any]]) -> dict[str, Any]:
    by_name: dict[str, list[float]] = {}
    for result in results:
        by_name.setdefault(str(result["name"]), []).append(float(result["wall_ms"]))
    summary = {}
    for key, values in sorted(by_name.items()):
        summary[key] = {
            "count": len(values),
            "min_ms": round(min(values), 3),
            "p50_ms": round(percentile(values, 0.5) or 0.0, 3),
            "p95_ms": round(percentile(values, 0.95) or 0.0, 3),
            "max_ms": round(max(values), 3),
        }
    return {"name": name, "summary": summary, "results": results}


async def main_async(args: argparse.Namespace) -> dict[str, Any]:
    load_env()
    base_url = args.base_url.rstrip("/")
    room_id: str | None = None
    discovery: dict[str, Any] = {"source": "skipped"}
    if args.include_matrix:
        room_id, discovery = discover_matrix_room(
            base_url,
            server=args.matrix_server,
            room_id=args.matrix_room_id,
            room_hint=args.matrix_room_hint,
            timeout=args.timeout,
        )
    specs = build_specs(args, room_id)

    batches: list[dict[str, Any]] = []
    sequential_specs = list(specs.values())
    sequential_results: list[dict[str, Any]] = []
    for spec in sequential_specs:
        sequential_results.extend(await run_specs(base_url, [spec], timeout=args.timeout))
    batches.append(summarize_batch("sequential_baseline", sequential_results))

    all_concurrent = [
        specs[key]
        for key in (
            "matrix_messages",
            "kanban_status",
            "browser_view",
            "browser_clients",
            "kanban_datastore",
            "auth_time",
            "health",
        )
        if key in specs
    ]
    batches.append(
        summarize_batch(
            "all_routes_concurrent",
            await run_specs(base_url, all_concurrent, timeout=args.timeout),
        )
    )

    for slow_key in ("matrix_messages", "kanban_status", "browser_view"):
        if slow_key not in specs:
            continue
        sentinel_specs: list[RequestSpec] = [specs[slow_key]]
        for index in range(args.sentinel_count):
            sentinel_specs.append(
                RequestSpec(f"auth_time_{index + 1:02d}", "/api/v1/auth/time", "sentinel")
            )
        batches.append(
            summarize_batch(
                f"{slow_key}_with_auth_sentinels",
                await run_specs(
                    base_url,
                    sentinel_specs,
                    timeout=args.timeout,
                    stagger_ms=args.sentinel_stagger_ms,
                ),
            )
        )

    return {
        "ok": all(result["ok"] for batch in batches for result in batch["results"]),
        "base_url": base_url,
        "matrix_discovery": discovery,
        "settings": {
            "timeout_seconds": args.timeout,
            "matrix_server": args.matrix_server,
            "matrix_limit": args.matrix_limit,
            "matrix_decrypt_timeout_ms": args.matrix_decrypt_timeout_ms,
            "kanban_status_limit": args.kanban_status_limit,
            "sentinel_count": args.sentinel_count,
            "sentinel_stagger_ms": args.sentinel_stagger_ms,
        },
        "batches": batches,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--timeout", type=float, default=12.0)
    parser.add_argument("--include-matrix", action="store_true", default=True)
    parser.add_argument("--no-matrix", action="store_false", dest="include_matrix")
    parser.add_argument("--matrix-server", choices=["tb1", "vps"], default="tb1")
    parser.add_argument("--matrix-room-id", default="")
    parser.add_argument("--matrix-room-hint", default="Bridge")
    parser.add_argument("--matrix-limit", type=int, default=60)
    parser.add_argument("--matrix-decrypt-timeout-ms", type=int, default=0)
    parser.add_argument("--kanban-status-limit", type=int, default=1)
    parser.add_argument("--sentinel-count", type=int, default=8)
    parser.add_argument("--sentinel-stagger-ms", type=int, default=100)
    args = parser.parse_args(argv)
    args.matrix_limit = max(1, min(args.matrix_limit, 100))
    args.matrix_decrypt_timeout_ms = max(0, min(args.matrix_decrypt_timeout_ms, 30_000))
    args.kanban_status_limit = max(1, min(args.kanban_status_limit, 50))
    args.sentinel_count = max(1, min(args.sentinel_count, 50))
    args.sentinel_stagger_ms = max(0, min(args.sentinel_stagger_ms, 5000))
    result = asyncio.run(main_async(args))
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
