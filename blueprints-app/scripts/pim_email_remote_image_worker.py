#!/usr/bin/env python3
"""Remote PIM Email external image assignment worker.

Helper-node worker contract:
- ask the coordinator API for a stable block of unique canonical URL assignments;
- fetch image bytes from the public internet only;
- transform public image bytes to JPEG in memory;
- post the transformed JPEG plus hashes, dimensions, fetched metadata, or exact failure reason;
- the coordinator validates, encrypts, stores, and links all references.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import hmac
import ipaddress
import json
import os
import socket
import sys
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx
from pim_email_image_transform import (
    TRANSFORM_VERSION,
    ImageTransformError,
    jpeg_dimensions,
    sha256_hex,
    transform_image_to_jpeg,
)

TOKEN_WINDOW_SECONDS = 5
DEFAULT_API_BASE = "http://127.0.0.1:8080/api/v1"
DEFAULT_MAX_IMAGE_BYTES = 8 * 1024 * 1024


class WorkerError(RuntimeError):
    def __init__(self, message: str, *, status: str = "failed") -> None:
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class FetchResult:
    content: bytes
    content_type: str
    final_url: str


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def _compute_blueprints_token(secret_hex: str) -> str:
    window = int(time.time()) // TOKEN_WINDOW_SECONDS
    return hmac.new(bytes.fromhex(secret_hex), str(window).encode(), hashlib.sha256).hexdigest()


def _headers(*, api_secret: str, worker_secret: str) -> dict[str, str]:
    headers = {
        "X-PIM-Email-Worker-Token": worker_secret,
        "Content-Type": "application/json",
    }
    if api_secret:
        headers["x-api-token"] = _compute_blueprints_token(api_secret)
    return headers


def _json_log(event: str, **payload: Any) -> None:
    safe_payload = {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "canonical_url",
            "source_url",
            "transformed_image_base64",
            "assignment_token",
        }
    }
    print(
        json.dumps({"event": event, **safe_payload}, sort_keys=True, separators=(",", ":")),
        flush=True,
    )


def _assert_public_remote_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise WorkerError("image blocked: URL is not an allowed HTTP(S) URL", status="blocked")
    host = parsed.hostname
    try:
        host_ip = ipaddress.ip_address(host)
        ips = [host_ip]
    except ValueError:
        try:
            infos = socket.getaddrinfo(
                host, parsed.port or (443 if parsed.scheme == "https" else 80)
            )
        except socket.gaierror as exc:
            raise WorkerError("image unavailable: DNS unresolved", status="unavailable") from exc
        ips = []
        for info in infos:
            try:
                ips.append(ipaddress.ip_address(info[4][0]))
            except ValueError:
                continue
    if not ips:
        raise WorkerError("image unavailable: DNS unresolved", status="unavailable")
    for ip in ips:
        if not ip.is_global:
            raise WorkerError(
                "image blocked: URL resolved to private or unsafe address", status="blocked"
            )
    return url


def _status_for_http_error(status_code: int) -> str:
    if status_code == 429 or 500 <= status_code <= 599:
        return "retryable"
    return "unavailable"


async def _fetch_image(
    client: httpx.AsyncClient,
    source_url: str,
    *,
    max_image_bytes: int,
    max_redirects: int,
) -> FetchResult:
    current = _assert_public_remote_url(source_url)
    headers = {
        "Accept": "image/avif,image/webp,image/png,image/jpeg,image/gif,image/*;q=0.8",
        "User-Agent": "BlueprintsEmailImageRemoteWorker/1.0",
    }
    try:
        for _ in range(max_redirects):
            current = _assert_public_remote_url(current)
            async with client.stream(
                "GET", current, headers=headers, follow_redirects=False
            ) as resp:
                if resp.status_code in {301, 302, 303, 307, 308}:
                    location = resp.headers.get("location", "").strip()
                    if not location:
                        raise WorkerError(
                            "image unavailable: redirect missing location", status="unavailable"
                        )
                    current = urljoin(current, location)
                    continue
                if resp.status_code >= 400:
                    raise WorkerError(
                        f"image unavailable: HTTP {resp.status_code}",
                        status=_status_for_http_error(resp.status_code),
                    )
                content_type = resp.headers.get("content-type", "").split(";", 1)[0].strip().lower()
                if content_type and not content_type.startswith("image/"):
                    raise WorkerError(
                        "image unavailable: fetch did not return an image", status="unavailable"
                    )
                chunks: list[bytes] = []
                total = 0
                async for chunk in resp.aiter_bytes():
                    total += len(chunk)
                    if total > max_image_bytes:
                        raise WorkerError("image blocked: payload is too large", status="blocked")
                    chunks.append(chunk)
                content = b"".join(chunks)
                if not content:
                    raise WorkerError(
                        "image unavailable: empty response body", status="unavailable"
                    )
                return FetchResult(
                    content=content, content_type=content_type or "image/*", final_url=current
                )
        raise WorkerError("image unavailable: redirect chain exceeded limit", status="unavailable")
    except (httpx.ConnectTimeout, httpx.ReadTimeout, httpx.PoolTimeout) as exc:
        raise WorkerError(f"image retryable: {exc.__class__.__name__}", status="retryable") from exc
    except httpx.ConnectError as exc:
        raise WorkerError("image retryable: ConnectError", status="retryable") from exc
    except httpx.RequestError as exc:
        raise WorkerError(f"image retryable: {exc.__class__.__name__}", status="retryable") from exc


async def _post_json(
    client: httpx.AsyncClient,
    url: str,
    payload: dict[str, Any],
    *,
    api_secret: str,
    worker_secret: str,
) -> dict[str, Any]:
    resp = await client.post(
        url, headers=_headers(api_secret=api_secret, worker_secret=worker_secret), json=payload
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise WorkerError("worker API returned non-object JSON", status="failed")
    return data


async def _get_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    api_secret: str,
    worker_secret: str,
) -> dict[str, Any]:
    resp = await client.get(
        url, headers=_headers(api_secret=api_secret, worker_secret=worker_secret)
    )
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise WorkerError("worker API returned non-object JSON", status="failed")
    return data


async def _process_item(
    *,
    item: dict[str, Any],
    api_client: httpx.AsyncClient,
    fetch_client: httpx.AsyncClient,
    api_base: str,
    api_secret: str,
    worker_secret: str,
    worker_id: str,
    assignment_token: str,
    max_image_bytes: int,
    max_redirects: int,
) -> str:
    digest = str(item.get("canonical_url_digest") or "")
    source = str(item.get("source_url") or item.get("canonical_url") or "")
    if not digest:
        return "failed"
    try:
        fetched = await _fetch_image(
            fetch_client,
            source,
            max_image_bytes=max_image_bytes,
            max_redirects=max_redirects,
        )
        transformed = transform_image_to_jpeg(fetched.content)
        width, height = jpeg_dimensions(transformed)
        payload = {
            "worker_id": worker_id,
            "assignment_token": assignment_token,
            "transformed_image_base64": base64.b64encode(transformed).decode("ascii"),
            "raw_image_sha256": sha256_hex(fetched.content),
            "transformed_sha256": sha256_hex(transformed),
            "width": width,
            "height": height,
            "transform_version": TRANSFORM_VERSION,
            "fetched_content_type": fetched.content_type,
            "fetched_final_url": fetched.final_url,
            "metadata": {
                "source": "remote-pim-email-image-download-worker",
                "worker_transformed": True,
            },
        }
        result = await _post_json(
            api_client,
            f"{api_base}/personal/email/workers/external-images/assignments/{digest}/complete",
            payload,
            api_secret=api_secret,
            worker_secret=worker_secret,
        )
        ok = bool(result.get("ok", False))
        _json_log("image_assignment_complete", digest=digest[:16], ok=ok)
        return "stored" if ok else "failed"
    except WorkerError as exc:
        status = exc.status
        reason = str(exc)[:1000]
    except ImageTransformError as exc:
        status = "blocked"
        reason = str(exc)[:1000]
    except Exception as exc:
        status = "retryable"
        reason = f"image retryable: {exc.__class__.__name__}"
    await _post_json(
        api_client,
        f"{api_base}/personal/email/workers/external-images/assignments/{digest}/fail",
        {
            "worker_id": worker_id,
            "assignment_token": assignment_token,
            "status": status,
            "reason": reason,
            "metadata": {
                "source": "remote-pim-email-image-download-worker",
                "error_status": status,
            },
        },
        api_secret=api_secret,
        worker_secret=worker_secret,
    )
    _json_log("image_assignment_failed", digest=digest[:16], status=status, reason=reason)
    return status


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    api_base = args.api_base.rstrip("/")
    api_secret = os.environ.get("BLUEPRINTS_API_SECRET", "").strip()
    worker_secret = os.environ.get("BLUEPRINTS_PIM_EMAIL_WORKER_SECRET", "").strip()
    if not worker_secret:
        raise SystemExit("BLUEPRINTS_PIM_EMAIL_WORKER_SECRET is required")

    limits = httpx.Limits(
        max_connections=max(8, args.concurrency * 2),
        max_keepalive_connections=max(4, args.concurrency),
    )
    timeout = httpx.Timeout(args.request_timeout_seconds, connect=args.connect_timeout_seconds)
    async with httpx.AsyncClient(
        timeout=timeout, limits=limits, verify=args.verify_tls
    ) as api_client:
        claim = await _post_json(
            api_client,
            f"{api_base}/personal/email/workers/external-images/assignments/claim",
            {
                "worker_id": args.worker_id,
                "run_id": args.run_id,
                "limit": args.block_size,
                "metadata": {
                    "source": "remote-pim-email-image-download-worker",
                    "node_name": args.node_name,
                    "concurrency": args.concurrency,
                },
            },
            api_secret=api_secret,
            worker_secret=worker_secret,
        )
        items = list(claim.get("items") or [])
        assignment_batch_id = str(claim.get("assignment_batch_id") or "")
        assignment_token = str(claim.get("assignment_token") or "")
        _json_log(
            "image_assignment_block_claimed",
            node_name=args.node_name,
            worker_id=args.worker_id,
            run_id=args.run_id,
            assignment_batch_id=assignment_batch_id,
            claimed=len(items),
        )
        if not items:
            return {
                "claimed": 0,
                "stored": 0,
                "retryable": 0,
                "unavailable": 0,
                "blocked": 0,
                "failed": 0,
            }

        counts = {"stored": 0, "retryable": 0, "unavailable": 0, "blocked": 0, "failed": 0}
        semaphore = asyncio.Semaphore(args.concurrency)
        fetch_timeout = httpx.Timeout(
            args.fetch_timeout_seconds, connect=args.fetch_connect_timeout_seconds
        )
        async with httpx.AsyncClient(
            timeout=fetch_timeout, limits=limits, verify=True
        ) as fetch_client:

            async def run_one(item: dict[str, Any]) -> None:
                async with semaphore:
                    status = await _process_item(
                        item=item,
                        api_client=api_client,
                        fetch_client=fetch_client,
                        api_base=api_base,
                        api_secret=api_secret,
                        worker_secret=worker_secret,
                        worker_id=args.worker_id,
                        assignment_token=assignment_token,
                        max_image_bytes=args.max_image_bytes,
                        max_redirects=args.max_redirects,
                    )
                    counts[status if status in counts else "failed"] += 1
                    completed = sum(counts.values())
                    if completed % max(1, args.heartbeat_every) == 0 or completed == len(items):
                        await _post_json(
                            api_client,
                            f"{api_base}/personal/email/workers/external-images/assignments/heartbeat",
                            {
                                "assignment_batch_id": assignment_batch_id,
                                "worker_id": args.worker_id,
                                "assignment_token": assignment_token,
                            },
                            api_secret=api_secret,
                            worker_secret=worker_secret,
                        )

            await asyncio.gather(*(run_one(item) for item in items))

        summary = {"claimed": len(items), **counts}
        _json_log(
            "image_assignment_block_finished",
            node_name=args.node_name,
            worker_id=args.worker_id,
            run_id=args.run_id,
            assignment_batch_id=assignment_batch_id,
            **summary,
        )
        if args.status_after:
            status = await _get_json(
                api_client,
                f"{api_base}/personal/email/workers/external-images/status",
                api_secret=api_secret,
                worker_secret=worker_secret,
            )
            _json_log(
                "image_assignment_status_after",
                assignment_status=(status.get("url_assignments") or {}).get("assignment_status"),
                result_status=(status.get("url_assignments") or {}).get("result_status"),
            )
        return summary


def parse_args(argv: list[str]) -> argparse.Namespace:
    node_name = os.environ.get("NODE_NAME", socket.gethostname()).strip() or socket.gethostname()
    default_worker = os.environ.get("WORKER_ID", f"{node_name}-remote-image-worker").strip()
    default_run = os.environ.get("WORKER_RUN_ID", f"{default_worker}-stable").strip()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--api-base", default=os.environ.get("PIM_EMAIL_WORKER_API_BASE", DEFAULT_API_BASE)
    )
    parser.add_argument("--node-name", default=node_name)
    parser.add_argument("--worker-id", default=default_worker)
    parser.add_argument("--run-id", default=default_run)
    parser.add_argument(
        "--block-size",
        type=int,
        default=_env_int("WORKER_BLOCK_SIZE", 1000, minimum=1, maximum=5000),
    )
    parser.add_argument(
        "--concurrency", type=int, default=_env_int("WORKER_CONCURRENCY", 8, minimum=1, maximum=64)
    )
    parser.add_argument(
        "--heartbeat-every",
        type=int,
        default=_env_int("WORKER_HEARTBEAT_EVERY", 25, minimum=1, maximum=5000),
    )
    parser.add_argument(
        "--max-image-bytes",
        type=int,
        default=_env_int(
            "WORKER_MAX_IMAGE_BYTES",
            DEFAULT_MAX_IMAGE_BYTES,
            minimum=1024,
            maximum=64 * 1024 * 1024,
        ),
    )
    parser.add_argument(
        "--max-redirects",
        type=int,
        default=_env_int("WORKER_MAX_REDIRECTS", 4, minimum=1, maximum=10),
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        default=float(os.environ.get("WORKER_API_TIMEOUT_SECONDS", "60")),
    )
    parser.add_argument(
        "--connect-timeout-seconds",
        type=float,
        default=float(os.environ.get("WORKER_API_CONNECT_TIMEOUT_SECONDS", "10")),
    )
    parser.add_argument(
        "--fetch-timeout-seconds",
        type=float,
        default=float(os.environ.get("WORKER_FETCH_TIMEOUT_SECONDS", "10")),
    )
    parser.add_argument(
        "--fetch-connect-timeout-seconds",
        type=float,
        default=float(os.environ.get("WORKER_FETCH_CONNECT_TIMEOUT_SECONDS", "4")),
    )
    parser.add_argument(
        "--insecure-api-tls",
        action="store_true",
        default=os.environ.get("PIM_EMAIL_WORKER_INSECURE_API_TLS", "").strip() == "1",
    )
    parser.add_argument(
        "--status-after",
        action="store_true",
        default=os.environ.get("WORKER_STATUS_AFTER", "").strip() == "1",
    )
    parser.add_argument(
        "--repeat", action="store_true", default=os.environ.get("WORKER_REPEAT", "1").strip() != "0"
    )
    parser.add_argument(
        "--idle-sleep-seconds",
        type=float,
        default=float(os.environ.get("WORKER_IDLE_SLEEP_SECONDS", "30")),
    )
    parser.add_argument(
        "--batch-sleep-seconds",
        type=float,
        default=float(os.environ.get("WORKER_BATCH_SLEEP_SECONDS", "2")),
    )
    args = parser.parse_args(argv)
    args.verify_tls = not args.insecure_api_tls
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    if not args.repeat:
        result = asyncio.run(_run(args))
        print(json.dumps(result, sort_keys=True), flush=True)
        return 0
    while True:
        result = asyncio.run(_run(args))
        print(json.dumps(result, sort_keys=True), flush=True)
        claimed = int(result.get("claimed") or 0)
        time.sleep(args.batch_sleep_seconds if claimed else args.idle_sleep_seconds)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
