"""Blueprints PIM Email API routes."""

from __future__ import annotations

import asyncio
import base64
import hmac
import os
import re
import subprocess
import time
import uuid
from collections import OrderedDict
from pathlib import Path
from threading import RLock
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

import httpx
from fastapi import APIRouter, Header, HTTPException, Query, Response
from pydantic import BaseModel, ConfigDict, Field

from . import timing
from .events import AppEvent
from .pim_email import (
    DEFAULT_DOWNLOADED_FOLDER,
    EmailConfigError,
    EmailCredentialError,
    EmailOperationError,
    PgEmailStore,
    fetch_message,
    fetch_message_security,
    list_folder_messages,
    list_folders,
    list_inbox,
    smtp_self_send,
)

router = APIRouter(prefix="/personal/email", tags=["personal-email"])

PIM_EMAIL_STACK_DIR = Path("/xarta-node/.lone-wolf/stacks/pim-email")
PIM_EMAIL_STACK_COMMAND_TIMEOUT_SECONDS = 25.0
PIM_EMAIL_STACK_API_BASE = os.environ.get(
    "PIM_EMAIL_STACK_API_BASE",
    "http://127.0.0.1:18085",
).rstrip("/")
PIM_EMAIL_STACK_API_TIMEOUT_SECONDS = float(
    os.environ.get("PIM_EMAIL_STACK_API_TIMEOUT_SECONDS", "10")
)
PIM_EMAIL_LOCAL_IMAGE_PATH = "/api/v1/personal/email/local/images"
PIM_EMAIL_PROXY_IMAGE_WARM_CONCURRENCY = int(
    os.environ.get("PIM_EMAIL_PROXY_IMAGE_WARM_CONCURRENCY", "8")
)
PIM_EMAIL_PROXY_IMAGE_CACHE_MAX_BYTES = int(
    os.environ.get("PIM_EMAIL_PROXY_IMAGE_CACHE_MAX_BYTES", str(256 * 1024 * 1024))
)
PIM_EMAIL_PROXY_IMAGE_CACHE_HEADROOM_BYTES = int(
    os.environ.get("PIM_EMAIL_PROXY_IMAGE_CACHE_HEADROOM_BYTES", str(2 * 1024 * 1024 * 1024))
)
PIM_EMAIL_PROXY_IMAGE_WARM_MAX_TASKS = int(
    os.environ.get("PIM_EMAIL_PROXY_IMAGE_WARM_MAX_TASKS", "16")
)


def _available_memory_bytes() -> int | None:
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as handle:
            for line in handle:
                if not line.startswith("MemAvailable:"):
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    return max(0, int(parts[1]) * 1024)
    except Exception:
        return None
    return None


class _ProxyImageCache:
    def __init__(self, *, capacity_bytes: int) -> None:
        self.max_bytes = max(0, int(capacity_bytes))
        self._items: OrderedDict[tuple[str, str, str], dict[str, Any]] = OrderedDict()
        self._bytes = 0
        self._hits = 0
        self._misses = 0
        self._puts = 0
        self._evictions = 0
        self._lock = RLock()

    def capacity_bytes(self) -> int:
        available = _available_memory_bytes()
        if available is None:
            return 0
        return max(
            0,
            min(
                self.max_bytes,
                available - PIM_EMAIL_PROXY_IMAGE_CACHE_HEADROOM_BYTES,
            ),
        )

    def _evict_until_room(self, needed_bytes: int, capacity: int) -> None:
        while self._items and self._bytes + needed_bytes > capacity:
            _, removed = self._items.popitem(last=False)
            self._bytes -= int(removed.get("size") or 0)
            self._evictions += 1
        self._bytes = max(0, self._bytes)

    def get(self, key: tuple[str, str, str]) -> dict[str, Any] | None:
        with self._lock:
            item = self._items.get(key)
            if item is None:
                self._misses += 1
                return None
            self._items.move_to_end(key)
            self._hits += 1
            value = item.get("value")
            return dict(value) if isinstance(value, dict) else None

    def put(self, key: tuple[str, str, str], value: dict[str, Any]) -> bool:
        content = bytes(value.get("content") or b"")
        size = len(content) + 512
        with self._lock:
            capacity = self.capacity_bytes()
            if key in self._items:
                removed = self._items.pop(key)
                self._bytes -= int(removed.get("size") or 0)
            if capacity <= 0 or size > capacity:
                self._bytes = max(0, self._bytes)
                return False
            self._evict_until_room(size, capacity)
            if self._bytes + size > capacity:
                return False
            self._items[key] = {"value": dict(value), "size": size}
            self._bytes += size
            self._puts += 1
            return True

    def invalidate_email(self, mailbox_id: str | None, email_uid: str) -> int:
        mailbox = str(mailbox_id or "")
        uid = str(email_uid or "")
        removed_count = 0
        with self._lock:
            for key in list(self._items):
                if key[0] == mailbox and key[1] == uid:
                    removed = self._items.pop(key)
                    self._bytes -= int(removed.get("size") or 0)
                    self._evictions += 1
                    removed_count += 1
            self._bytes = max(0, self._bytes)
        return removed_count

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "schema": "xarta.pim_email.proxy_image_cache.stats.v1",
                "items": len(self._items),
                "bytes": self._bytes,
                "capacity_bytes": self.capacity_bytes(),
                "max_bytes": self.max_bytes,
                "headroom_bytes": PIM_EMAIL_PROXY_IMAGE_CACHE_HEADROOM_BYTES,
                "hits": self._hits,
                "misses": self._misses,
                "puts": self._puts,
                "evictions": self._evictions,
            }


PIM_EMAIL_PROXY_IMAGE_CACHE = _ProxyImageCache(capacity_bytes=PIM_EMAIL_PROXY_IMAGE_CACHE_MAX_BYTES)
PIM_EMAIL_PROXY_IMAGE_WARM_TASKS: set[asyncio.Task[dict[str, int]]] = set()


class SmtpSelfTestRequest(BaseModel):
    recipient: str = Field(..., min_length=3, max_length=254)


class DownloadMailboxRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    apply_remote_moves: bool = False
    downloaded_folder: str = Field(DEFAULT_DOWNLOADED_FOLDER, min_length=1, max_length=180)
    folder_allowlist: list[str] | None = None
    limit_per_folder: int | None = Field(None, ge=1, le=5000)
    max_messages: int | None = Field(None, ge=1, le=1000000)
    convergence_passes: int = Field(2, ge=1, le=5)


class LocalCacheWarmRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    email_uids: list[str] = Field(default_factory=list, max_length=200)
    limit: int = Field(100, ge=1, le=200)


class ExternalImageAssignmentClaimRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    worker_id: str = Field(..., min_length=1, max_length=160)
    run_id: str = Field("", max_length=180)
    limit: int = Field(1000, ge=1, le=5000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExternalImageAssignmentHeartbeatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    assignment_batch_id: str = Field(..., min_length=8, max_length=180)
    worker_id: str = Field(..., min_length=1, max_length=160)
    assignment_token: str = Field(..., min_length=16, max_length=240)


class ExternalImageAssignmentReleaseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    assignment_batch_id: str = Field(..., min_length=8, max_length=180)
    worker_id: str = Field(..., min_length=1, max_length=160)
    assignment_token: str = Field(..., min_length=16, max_length=240)
    reason: str = Field("worker_released_assignment", max_length=1000)


class ExternalImageAssignmentCompleteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    worker_id: str = Field(..., min_length=1, max_length=160)
    assignment_token: str = Field(..., min_length=16, max_length=240)
    transformed_image_base64: str = Field(..., min_length=1)
    raw_image_sha256: str = Field(..., pattern=r"^[0-9a-fA-F]{64}$")
    transformed_sha256: str = Field(..., pattern=r"^[0-9a-fA-F]{64}$")
    width: int = Field(..., ge=1, le=1800)
    height: int = Field(..., ge=1, le=2400)
    transform_version: str = Field("jpeg-v1", min_length=1, max_length=80)
    fetched_content_type: str = Field("", max_length=180)
    fetched_final_url: str = Field("", max_length=4096)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExternalImageAssignmentFailRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    worker_id: str = Field(..., min_length=1, max_length=160)
    assignment_token: str = Field(..., min_length=16, max_length=240)
    status: str = Field(..., min_length=3, max_length=40)
    reason: str = Field(..., min_length=1, max_length=1000)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SecurityAssignmentClaimRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    phase: str = Field(..., min_length=3, max_length=40)
    worker_id: str = Field(..., min_length=1, max_length=160)
    run_id: str = Field("", max_length=180)
    limit: int = Field(5, ge=1, le=25)
    lease_seconds: int = Field(900, ge=60, le=3600)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SecurityAssignmentHeartbeatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    assignment_batch_id: str = Field(..., min_length=8, max_length=180)
    worker_id: str = Field(..., min_length=1, max_length=160)
    assignment_token: str = Field(..., min_length=16, max_length=240)
    lease_seconds: int = Field(900, ge=60, le=3600)


class SecurityAssignmentReleaseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    assignment_batch_id: str = Field(..., min_length=8, max_length=180)
    worker_id: str = Field(..., min_length=1, max_length=160)
    assignment_token: str = Field(..., min_length=16, max_length=240)
    reason: str = Field("worker_released_assignment", max_length=1000)


class SecurityAssignmentCompleteRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(..., min_length=1, max_length=160)
    assignment_token: str = Field(..., min_length=16, max_length=240)
    phase_result: dict[str, Any] = Field(default_factory=dict)
    security_result: dict[str, Any] | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class SecurityAssignmentFailRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    worker_id: str = Field(..., min_length=1, max_length=160)
    assignment_token: str = Field(..., min_length=16, max_length=240)
    status: str = Field("retryable", min_length=3, max_length=40)
    reason: str = Field(..., min_length=1, max_length=1000)
    error_class: str = Field("SecurityWorkerError", max_length=200)
    retry_delay_seconds: int = Field(900, ge=60, le=86400)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SecurityAssignmentReconcileRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    phase: str = Field("security", min_length=3, max_length=40)
    email_uid: str | None = Field(None, min_length=1, max_length=120)
    limit: int = Field(100, ge=1, le=500)
    run_id: str = Field("", max_length=180)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExternalImageMaintenanceStartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    batch_size: int = Field(250, ge=1, le=5000)
    max_batches: int | None = Field(None, ge=1, le=100000)
    repeat_until_idle: bool = True


def _store() -> PgEmailStore:
    return PgEmailStore()


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, EmailConfigError):
        return HTTPException(status_code=503, detail=str(exc))
    if isinstance(exc, EmailCredentialError):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, EmailOperationError):
        return HTTPException(status_code=400, detail=str(exc))
    return HTTPException(status_code=502, detail="Email middleware operation failed")


def _stack_params(**params: Any) -> dict[str, Any]:
    return {key: value for key, value in params.items() if value is not None}


async def _stack_get_json(path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{PIM_EMAIL_STACK_API_BASE}{path}"
    try:
        async with httpx.AsyncClient(timeout=PIM_EMAIL_STACK_API_TIMEOUT_SECONDS) as client:
            response = await client.get(url, params=params or {})
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail="PIM Email stack API is unavailable") from exc
    if response.status_code >= 400:
        detail: Any
        try:
            detail = response.json().get("detail", response.text)
        except Exception:
            detail = response.text or "PIM Email stack API request failed"
        raise HTTPException(status_code=response.status_code, detail=detail)
    try:
        data = response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502, detail="PIM Email stack API returned invalid JSON"
        ) from exc
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="PIM Email stack API returned invalid payload")
    return data


async def _stack_post_json(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"{PIM_EMAIL_STACK_API_BASE}{path}"
    try:
        async with httpx.AsyncClient(timeout=PIM_EMAIL_STACK_API_TIMEOUT_SECONDS) as client:
            response = await client.post(url, params=params or {}, json=json_body or {})
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail="PIM Email stack API is unavailable") from exc
    if response.status_code >= 400:
        detail: Any
        try:
            detail = response.json().get("detail", response.text)
        except Exception:
            detail = response.text or "PIM Email stack API request failed"
        raise HTTPException(status_code=response.status_code, detail=detail)
    try:
        data = response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502, detail="PIM Email stack API returned invalid JSON"
        ) from exc
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="PIM Email stack API returned invalid payload")
    return data


async def _stack_get_binary(path: str, *, params: dict[str, Any] | None = None) -> Response:
    url = f"{PIM_EMAIL_STACK_API_BASE}{path}"
    try:
        async with httpx.AsyncClient(timeout=PIM_EMAIL_STACK_API_TIMEOUT_SECONDS) as client:
            response = await client.get(url, params=params or {})
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail="PIM Email stack API is unavailable") from exc
    if response.status_code >= 400:
        detail: Any
        try:
            detail = response.json().get("detail", response.text)
        except Exception:
            detail = response.text or "PIM Email stack API request failed"
        raise HTTPException(status_code=response.status_code, detail=detail)
    headers = {
        key: value
        for key, value in {
            "Cache-Control": response.headers.get("cache-control"),
            "ETag": response.headers.get("etag"),
            "X-Content-Type-Options": response.headers.get("x-content-type-options"),
        }.items()
        if value
    }
    return Response(
        content=response.content,
        media_type=response.headers.get("content-type", "application/octet-stream"),
        headers=headers,
    )


def _proxy_local_image_ref_from_src(
    src: str, *, fallback_email_uid: str = ""
) -> tuple[str, str] | None:
    clean = str(src or "").strip()
    if not clean:
        return None
    parsed = urlsplit(clean)
    if parsed.scheme or parsed.netloc or parsed.fragment:
        return None
    path = unquote(parsed.path or "")
    if not path.startswith(f"{PIM_EMAIL_LOCAL_IMAGE_PATH}/"):
        return None
    shared_uid = path[len(f"{PIM_EMAIL_LOCAL_IMAGE_PATH}/") :]
    email_uid = (parse_qs(parsed.query or "").get("email_uid") or [fallback_email_uid or ""])[0]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{7,179}", shared_uid):
        return None
    if not re.fullmatch(r"[0-9]{8}-[0-9a-f]{40}", email_uid):
        return None
    return shared_uid, email_uid


def _proxy_local_image_refs_from_html(
    html_value: str,
    *,
    fallback_email_uid: str,
    limit: int = 120,
) -> list[tuple[str, str]]:
    refs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for match in re.finditer(
        r"<img\b[^>]*\bsrc\s*=\s*(['\"])(.*?)\1", str(html_value or ""), re.I | re.S
    ):
        ref = _proxy_local_image_ref_from_src(
            match.group(2),
            fallback_email_uid=fallback_email_uid,
        )
        if ref is None or ref in seen:
            continue
        seen.add(ref)
        refs.append(ref)
        if len(refs) >= limit:
            break
    return refs


async def _warm_proxy_images_for_message(
    message: dict[str, Any] | None,
    *,
    mailbox_id: str | None,
    email_uid: str,
) -> dict[str, int]:
    html_value = str(((message or {}).get("views") or {}).get("html") or "")
    refs = _proxy_local_image_refs_from_html(
        html_value,
        fallback_email_uid=email_uid,
    )
    summary = {
        "planned": len(refs),
        "warmed": 0,
        "already_cached": 0,
        "failed": 0,
    }
    if not refs:
        return summary
    semaphore = asyncio.Semaphore(max(1, PIM_EMAIL_PROXY_IMAGE_WARM_CONCURRENCY))

    async def warm_one(ref: tuple[str, str]) -> str:
        shared_uid, ref_email_uid = ref
        key = (str(mailbox_id or ""), ref_email_uid, shared_uid)
        if PIM_EMAIL_PROXY_IMAGE_CACHE.get(key) is not None:
            return "already_cached"
        async with semaphore:
            try:
                response = await _stack_get_binary(
                    f"/local/images/{shared_uid}",
                    params=_stack_params(email_uid=ref_email_uid, mailbox_id=mailbox_id),
                )
                headers = {
                    name: value
                    for name, value in {
                        "Cache-Control": response.headers.get("cache-control"),
                        "ETag": response.headers.get("etag"),
                        "X-Content-Type-Options": response.headers.get("x-content-type-options"),
                    }.items()
                    if value
                }
                PIM_EMAIL_PROXY_IMAGE_CACHE.put(
                    key,
                    {
                        "content": bytes(response.body or b""),
                        "media_type": response.media_type or "image/jpeg",
                        "headers": headers,
                    },
                )
                return "warmed"
            except Exception:
                return "failed"

    for status in await asyncio.gather(*(warm_one(ref) for ref in refs)):
        if status == "already_cached":
            summary["already_cached"] += 1
        elif status == "warmed":
            summary["warmed"] += 1
        else:
            summary["failed"] += 1
    return summary


def _schedule_proxy_image_warm(
    message: dict[str, Any] | None,
    *,
    mailbox_id: str | None,
    email_uid: str,
) -> dict[str, Any]:
    html_value = str(((message or {}).get("views") or {}).get("html") or "")
    refs = _proxy_local_image_refs_from_html(
        html_value,
        fallback_email_uid=email_uid,
    )
    summary: dict[str, Any] = {
        "planned": len(refs),
        "scheduled": False,
        "active_tasks": len(PIM_EMAIL_PROXY_IMAGE_WARM_TASKS),
    }
    if not refs:
        return summary
    if len(PIM_EMAIL_PROXY_IMAGE_WARM_TASKS) >= max(1, PIM_EMAIL_PROXY_IMAGE_WARM_MAX_TASKS):
        summary["reason"] = "proxy_image_warm_backlog_full"
        return summary

    async def run() -> dict[str, int]:
        return await _warm_proxy_images_for_message(
            message,
            mailbox_id=mailbox_id,
            email_uid=email_uid,
        )

    task = asyncio.create_task(run())
    PIM_EMAIL_PROXY_IMAGE_WARM_TASKS.add(task)

    def cleanup(done: asyncio.Task[dict[str, int]]) -> None:
        PIM_EMAIL_PROXY_IMAGE_WARM_TASKS.discard(done)
        try:
            done.result()
        except Exception:
            pass

    task.add_done_callback(cleanup)
    summary["scheduled"] = True
    summary["active_tasks"] = len(PIM_EMAIL_PROXY_IMAGE_WARM_TASKS)
    return summary


def _clean_security_run_id(value: str | None = None) -> str:
    clean = str(value or "").strip()
    if re.fullmatch(r"[A-Za-z0-9_.:-]{8,120}", clean):
        return clean
    return uuid.uuid4().hex


def _email_worker_secret() -> str:
    return (
        os.environ.get("BLUEPRINTS_PIM_EMAIL_WORKER_SECRET")
        or os.environ.get("BLUEPRINTS_EMAIL_WORKER_SECRET")
        or ""
    ).strip()


def _require_email_worker_token(token: str | None) -> None:
    secret = _email_worker_secret()
    if not secret:
        raise HTTPException(status_code=503, detail="PIM Email worker auth is not configured")
    if not token or not hmac.compare_digest(str(token), secret):
        raise HTTPException(status_code=401, detail="PIM Email worker token is invalid")


def _worker_safe_assignment(item: dict[str, Any]) -> dict[str, Any]:
    public = dict(item or {})
    public.pop("assignment_token", None)
    public.pop("email_uid", None)
    public.pop("input_raw_sha256", None)
    return public


def _worker_safe_shared_asset(item: dict[str, Any]) -> dict[str, Any]:
    public = dict(item or {})
    public.pop("storage_relpath", None)
    public.pop("encryption", None)
    return public


def _worker_safe_security_assignment(item: dict[str, Any]) -> dict[str, Any]:
    public = dict(item or {})
    public.pop("assignment_token", None)
    return public


def _decode_transformed_image_base64(value: str) -> bytes:
    return base64.b64decode(value, validate=True)


def _attach_server_metrics(
    response: dict[str, Any],
    *,
    metrics: bool,
    started_at: float,
    stages: dict[str, float] | None = None,
) -> dict[str, Any]:
    if metrics:
        response["server_metrics"] = {
            "total_seconds": round(time.perf_counter() - started_at, 6),
            **{name: round(duration, 6) for name, duration in (stages or {}).items()},
        }
    return response


def _event_severity_for_tone(tone: str) -> str:
    if str(tone or "").lower() == "red":
        return "error"
    if str(tone or "").lower() == "amber":
        return "warn"
    return "info"


def _security_progress_emitter(
    *,
    loop: asyncio.AbstractEventLoop,
    run_id: str,
    mailbox_id: str,
    folder: str,
    uid: str,
) -> Any:
    def emit(update: dict[str, Any]) -> None:
        payload = {
            **(update or {}),
            "run_id": run_id,
            "mailbox_id": mailbox_id,
            "folder": folder,
            "uid": uid,
        }
        event = AppEvent.create(
            "pim.email.security.progress",
            "Email Security Progress",
            "Email security check progress updated.",
            severity=_event_severity_for_tone(str(payload.get("tone") or "")),
            source="pim-email",
            payload=payload,
        )
        try:
            from .routes_events import publish_event

            asyncio.run_coroutine_threadsafe(publish_event(event), loop)
        except RuntimeError:
            return

    return emit


def _attach_security_run_id(security: dict[str, Any], run_id: str) -> None:
    progress = security.get("progress") if isinstance(security, dict) else None
    if not isinstance(progress, dict):
        security["progress"] = {}
        progress = security["progress"]
    progress["run_id"] = run_id


def _parse_stack_control_output(stdout: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for line in str(stdout or "").splitlines():
        for part in line.strip().split():
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key:
                parsed[key] = value
    return parsed


def _run_stack_control_script(script_name: str, args: list[str]) -> dict[str, Any]:
    script = (PIM_EMAIL_STACK_DIR / "scripts" / script_name).resolve()
    try:
        script.relative_to(PIM_EMAIL_STACK_DIR.resolve())
    except ValueError as exc:
        raise EmailOperationError("PIM Email stack script path escaped stack root") from exc
    if not script.exists():
        raise EmailOperationError(f"PIM Email stack script is missing: {script_name}")
    completed = subprocess.run(
        [str(script), *args],
        cwd=str(PIM_EMAIL_STACK_DIR),
        text=True,
        capture_output=True,
        timeout=PIM_EMAIL_STACK_COMMAND_TIMEOUT_SECONDS,
        check=False,
    )
    if completed.returncode != 0:
        raise EmailOperationError(
            "PIM Email stack control command failed: "
            f"exit={completed.returncode} stderr={completed.stderr.strip()[:500]}"
        )
    parsed = _parse_stack_control_output(completed.stdout)
    return {
        "schema": "xarta.pim_email.stack_control.command_result.v1",
        "script": script_name,
        "returncode": completed.returncode,
        "stdout": completed.stdout.strip(),
        "stderr": completed.stderr.strip(),
        "parsed": parsed,
    }


async def _start_stack_download(body: DownloadMailboxRequest) -> dict[str, Any]:
    store = _store()
    mailbox = await store.get_mailbox(body.mailbox_id)
    run_id = f"pim-email-stack-download-{uuid.uuid4().hex}"
    args = [run_id, "--mailbox-id", mailbox.mailbox_id]
    if body.apply_remote_moves:
        args.append("--apply-remote-moves")
    if body.downloaded_folder:
        args.extend(["--downloaded-folder", body.downloaded_folder])
    for folder in body.folder_allowlist or []:
        args.extend(["--folder", folder])
    if body.limit_per_folder is not None:
        args.extend(["--limit-per-folder", str(body.limit_per_folder)])
    if body.max_messages is not None:
        args.extend(["--max-messages", str(body.max_messages)])
    args.extend(["--convergence-passes", str(body.convergence_passes)])
    command = await asyncio.to_thread(_run_stack_control_script, "start-download.sh", args)
    return {
        "schema": "xarta.pim_email.stack_control.download_start.v1",
        "mailbox": mailbox.public_dict(),
        "run_id": run_id,
        "log": command.get("parsed", {}).get("log", ""),
        "stack": str(PIM_EMAIL_STACK_DIR),
        "command": command,
    }


async def _start_stack_external_image_maintenance(
    body: ExternalImageMaintenanceStartRequest,
) -> dict[str, Any]:
    mailbox_public: dict[str, Any] | None = None
    args: list[str] = [f"pim-email-stack-shared-assets-{uuid.uuid4().hex}"]
    if body.mailbox_id:
        mailbox = await _store().get_mailbox(body.mailbox_id)
        mailbox_public = mailbox.public_dict()
        args.extend(["--mailbox-id", mailbox.mailbox_id])
    args.extend(["--batch-size", str(body.batch_size)])
    if body.repeat_until_idle:
        args.append("--repeat-until-idle")
    if body.max_batches is not None:
        args.extend(["--max-batches", str(body.max_batches)])
    command = await asyncio.to_thread(_run_stack_control_script, "start-shared-assets.sh", args)
    return {
        "schema": "xarta.pim_email.stack_control.external_image_maintenance_start.v1",
        "mailbox": mailbox_public,
        "run_id": command.get("parsed", {}).get("run_id", args[0]),
        "log": command.get("parsed", {}).get("log", ""),
        "stack": str(PIM_EMAIL_STACK_DIR),
        "command": command,
    }


@router.get("/status")
async def email_status(
    include_external_images: bool = Query(False),
    include_security_details: bool = Query(False),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    response = await _stack_get_json(
        "/status",
        params=_stack_params(
            include_external_images=include_external_images,
            include_security_details=include_security_details,
        ),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.get("/local/status")
async def email_local_status(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    include_external_images: bool = Query(False),
    include_security_details: bool = Query(False),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    response = await _stack_get_json(
        "/local/status",
        params=_stack_params(
            mailbox_id=mailbox_id,
            include_external_images=include_external_images,
            include_security_details=include_security_details,
        ),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.get("/workers/security/status")
async def email_security_worker_status(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    include_local: bool = Query(False),
    include_worker_blocks: bool = Query(False),
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    stages: dict[str, float] = {}
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        store = _store()
        stage_started = time.perf_counter()
        with timing.span(
            "pim_email.worker.security.assignment_status",
            include_worker_blocks=include_worker_blocks,
        ):
            assignments = await store.security_phase_assignment_status_counts(
                mailbox_id=mailbox_id,
                include_worker_blocks=include_worker_blocks,
            )
        stages["assignment_status_seconds"] = time.perf_counter() - stage_started
        response: dict[str, Any] = {"ok": True, "assignments": assignments}
        if include_local:
            stage_started = time.perf_counter()
            with timing.span("pim_email.worker.security.local_status"):
                response["local_corpus"] = await store.local_corpus_status(
                    mailbox_id=mailbox_id,
                    include_external_images=False,
                    include_security_details=False,
                )
            stages["local_corpus_status_seconds"] = time.perf_counter() - stage_started
        return _attach_server_metrics(
            response, metrics=metrics, started_at=started_at, stages=stages
        )
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/security/assignments/reconcile")
async def email_security_worker_reconcile_assignments(
    body: SecurityAssignmentReconcileRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        stage_started = time.perf_counter()
        with timing.span("pim_email.worker.security.reconcile", phase=body.phase):
            result = await _store().enqueue_security_phase_assignments(
                mailbox_id=body.mailbox_id,
                phase=body.phase,
                email_uid=body.email_uid,
                limit=body.limit,
                run_id=body.run_id,
                metadata={
                    **body.metadata,
                    "source_surface": "pim-email-worker-api",
                },
            )
        return _attach_server_metrics(
            {"ok": True, "result": result},
            metrics=metrics,
            started_at=started_at,
            stages={"reconcile_seconds": time.perf_counter() - stage_started},
        )
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/security/assignments/claim")
async def email_security_worker_claim_assignments(
    body: SecurityAssignmentClaimRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        stage_started = time.perf_counter()
        with timing.span("pim_email.worker.security.claim", phase=body.phase):
            result = await _store().claim_security_phase_assignment_block(
                mailbox_id=body.mailbox_id,
                phase=body.phase,
                worker_id=body.worker_id,
                run_id=body.run_id,
                limit=body.limit,
                lease_seconds=body.lease_seconds,
                metadata={
                    **body.metadata,
                    "source_surface": "pim-email-worker-api",
                },
            )
        response = {
            "ok": True,
            "schema": "xarta.pim_email.security_phase_assignment.block.v1",
            "mailbox_id": result.get("mailbox_id"),
            "phase": result.get("phase"),
            "assignment_batch_id": result.get("assignment_batch_id"),
            "assignment_token": result.get("assignment_token"),
            "worker_id": result.get("worker_id"),
            "run_id": result.get("run_id"),
            "lease_seconds": result.get("lease_seconds"),
            "claimed": result.get("claimed"),
            "payload_failures": result.get("payload_failures"),
            "reconcile": result.get("reconcile"),
            "items": [_worker_safe_security_assignment(item) for item in result.get("items", [])],
        }
        return _attach_server_metrics(
            response,
            metrics=metrics,
            started_at=started_at,
            stages={"claim_seconds": time.perf_counter() - stage_started},
        )
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/security/assignments/heartbeat")
async def email_security_worker_heartbeat_assignments(
    body: SecurityAssignmentHeartbeatRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        stage_started = time.perf_counter()
        with timing.span("pim_email.worker.security.heartbeat"):
            result = await _store().heartbeat_security_phase_assignment_block(
                assignment_batch_id=body.assignment_batch_id,
                worker_id=body.worker_id,
                assignment_token=body.assignment_token,
                lease_seconds=body.lease_seconds,
            )
        return _attach_server_metrics(
            {"ok": True, "result": result},
            metrics=metrics,
            started_at=started_at,
            stages={"heartbeat_seconds": time.perf_counter() - stage_started},
        )
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/security/assignments/release")
async def email_security_worker_release_assignments(
    body: SecurityAssignmentReleaseRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        stage_started = time.perf_counter()
        with timing.span("pim_email.worker.security.release"):
            result = await _store().release_security_phase_assignment_block(
                assignment_batch_id=body.assignment_batch_id,
                worker_id=body.worker_id,
                assignment_token=body.assignment_token,
                reason=body.reason,
            )
        return _attach_server_metrics(
            {"ok": True, "result": result},
            metrics=metrics,
            started_at=started_at,
            stages={"release_seconds": time.perf_counter() - stage_started},
        )
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/security/assignments/{assignment_id}/complete")
async def email_security_worker_complete_assignment(
    assignment_id: str,
    body: SecurityAssignmentCompleteRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        stage_started = time.perf_counter()
        with timing.span("pim_email.worker.security.complete"):
            result = await _store().complete_security_phase_assignment(
                assignment_id=assignment_id,
                worker_id=body.worker_id,
                assignment_token=body.assignment_token,
                phase_result=body.phase_result,
                security_result=body.security_result,
                metadata={
                    **body.metadata,
                    "source_surface": "pim-email-worker-api",
                },
            )
        if "assignment" in result:
            result["assignment"] = _worker_safe_security_assignment(result["assignment"])
        return _attach_server_metrics(
            {"ok": True, "result": result},
            metrics=metrics,
            started_at=started_at,
            stages={"complete_seconds": time.perf_counter() - stage_started},
        )
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/security/assignments/{assignment_id}/fail")
async def email_security_worker_fail_assignment(
    assignment_id: str,
    body: SecurityAssignmentFailRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        stage_started = time.perf_counter()
        with timing.span("pim_email.worker.security.fail"):
            result = await _store().fail_security_phase_assignment(
                assignment_id=assignment_id,
                worker_id=body.worker_id,
                assignment_token=body.assignment_token,
                status=body.status,
                reason=body.reason,
                error_class=body.error_class,
                retry_delay_seconds=body.retry_delay_seconds,
                metadata={
                    **body.metadata,
                    "source_surface": "pim-email-worker-api",
                },
            )
        if "assignment" in result:
            result["assignment"] = _worker_safe_security_assignment(result["assignment"])
        return _attach_server_metrics(
            {"ok": True, "result": result},
            metrics=metrics,
            started_at=started_at,
            stages={"fail_seconds": time.perf_counter() - stage_started},
        )
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/workers/external-images/status")
async def email_external_image_worker_status(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    include_derivatives: bool = Query(False),
    include_worker_blocks: bool = Query(False),
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    stages: dict[str, float] = {}
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        store = _store()
        stage_started = time.perf_counter()
        with timing.span(
            "pim_email.worker.external_images.assignment_status",
            include_worker_blocks=include_worker_blocks,
        ):
            assignments = await store.external_image_url_assignment_status_counts(
                mailbox_id=mailbox_id,
                include_worker_blocks=include_worker_blocks,
            )
        stages["assignment_status_seconds"] = time.perf_counter() - stage_started
        response = {
            "ok": True,
            "url_assignments": assignments,
        }
        if include_derivatives:
            stage_started = time.perf_counter()
            with timing.span("pim_email.worker.external_images.local_status"):
                local = await store.local_corpus_status(
                    mailbox_id=mailbox_id,
                    include_external_images=True,
                    include_security_details=False,
                )
            stages["local_corpus_status_seconds"] = time.perf_counter() - stage_started
            response["external_image_derivatives"] = local.get("external_image_derivatives", {})
        return _attach_server_metrics(
            response, metrics=metrics, started_at=started_at, stages=stages
        )
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/external-images/maintenance/start")
async def email_external_image_maintenance_start(
    body: ExternalImageMaintenanceStartRequest,
) -> dict[str, Any]:
    try:
        result = await _start_stack_external_image_maintenance(body)
        return {"ok": True, "started": True, "result": result}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/external-images/assignments/claim")
async def email_external_image_worker_claim_assignments(
    body: ExternalImageAssignmentClaimRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        stage_started = time.perf_counter()
        with timing.span("pim_email.worker.external_images.claim"):
            result = await _store().claim_external_image_url_assignment_block(
                mailbox_id=body.mailbox_id,
                worker_id=body.worker_id,
                run_id=body.run_id,
                limit=body.limit,
                metadata={
                    **body.metadata,
                    "source_surface": "pim-email-worker-api",
                },
            )
        response = {
            "ok": True,
            "schema": "xarta.pim_email.external_image_url_assignment.block.v1",
            "mailbox_id": result.get("mailbox_id"),
            "assignment_batch_id": result.get("assignment_batch_id"),
            "assignment_token": result.get("assignment_token"),
            "worker_id": result.get("worker_id"),
            "run_id": result.get("run_id"),
            "claimed": result.get("claimed"),
            "items": [_worker_safe_assignment(item) for item in result.get("items", [])],
        }
        return _attach_server_metrics(
            response,
            metrics=metrics,
            started_at=started_at,
            stages={"claim_seconds": time.perf_counter() - stage_started},
        )
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/external-images/assignments/heartbeat")
async def email_external_image_worker_heartbeat_assignments(
    body: ExternalImageAssignmentHeartbeatRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        stage_started = time.perf_counter()
        with timing.span("pim_email.worker.external_images.heartbeat"):
            result = await _store().heartbeat_external_image_url_assignment_block(
                assignment_batch_id=body.assignment_batch_id,
                worker_id=body.worker_id,
                assignment_token=body.assignment_token,
            )
        return _attach_server_metrics(
            {"ok": True, "result": result},
            metrics=metrics,
            started_at=started_at,
            stages={"heartbeat_seconds": time.perf_counter() - stage_started},
        )
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/external-images/assignments/release")
async def email_external_image_worker_release_assignments(
    body: ExternalImageAssignmentReleaseRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        stage_started = time.perf_counter()
        with timing.span("pim_email.worker.external_images.release"):
            result = await _store().release_external_image_url_assignment_block(
                assignment_batch_id=body.assignment_batch_id,
                worker_id=body.worker_id,
                assignment_token=body.assignment_token,
                reason=body.reason,
            )
        return _attach_server_metrics(
            {"ok": True, "result": result},
            metrics=metrics,
            started_at=started_at,
            stages={"release_seconds": time.perf_counter() - stage_started},
        )
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/external-images/assignments/{canonical_url_digest}/complete")
async def email_external_image_worker_complete_assignment(
    canonical_url_digest: str,
    body: ExternalImageAssignmentCompleteRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    stages: dict[str, float] = {}
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        try:
            stage_started = time.perf_counter()
            with timing.span("pim_email.worker.external_images.decode_payload"):
                transformed_content = await asyncio.to_thread(
                    _decode_transformed_image_base64,
                    body.transformed_image_base64,
                )
            stages["decode_base64_seconds"] = time.perf_counter() - stage_started
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail="transformed_image_base64 is invalid",
            ) from exc
        stage_started = time.perf_counter()
        with timing.span("pim_email.worker.external_images.complete"):
            result = await _store().complete_external_image_url_assignment_with_transformed_payload(
                mailbox_id=body.mailbox_id,
                canonical_url_digest=canonical_url_digest,
                worker_id=body.worker_id,
                assignment_token=body.assignment_token,
                transformed_content=transformed_content,
                raw_image_sha256=body.raw_image_sha256,
                transformed_sha256=body.transformed_sha256,
                width=body.width,
                height=body.height,
                transform_version=body.transform_version,
                fetched_content_type=body.fetched_content_type,
                fetched_final_url=body.fetched_final_url,
                metadata={
                    **body.metadata,
                    "source_surface": "pim-email-worker-api",
                },
            )
        stages["complete_assignment_seconds"] = time.perf_counter() - stage_started
        if "assignment" in result:
            result["assignment"] = _worker_safe_assignment(result.pop("assignment"))
        if "shared_asset" in result:
            result["shared_asset"] = _worker_safe_shared_asset(result["shared_asset"])
        return _attach_server_metrics(
            {"ok": bool(result.get("ok", True)), "result": result},
            metrics=metrics,
            started_at=started_at,
            stages=stages,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/workers/external-images/assignments/{canonical_url_digest}/fail")
async def email_external_image_worker_fail_assignment(
    canonical_url_digest: str,
    body: ExternalImageAssignmentFailRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    try:
        stage_started = time.perf_counter()
        with timing.span("pim_email.worker.external_images.fail"):
            result = await _store().fail_external_image_url_assignment(
                mailbox_id=body.mailbox_id,
                canonical_url_digest=canonical_url_digest,
                worker_id=body.worker_id,
                assignment_token=body.assignment_token,
                status=body.status,
                reason=body.reason,
                metadata={
                    **body.metadata,
                    "source_surface": "pim-email-worker-api",
                },
            )
        if "assignment" in result:
            result["assignment"] = _worker_safe_assignment(result.pop("assignment"))
        return _attach_server_metrics(
            {"ok": True, "result": result},
            metrics=metrics,
            started_at=started_at,
            stages={"fail_assignment_seconds": time.perf_counter() - stage_started},
        )
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/local/folders")
async def email_local_folders(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    return await _stack_get_json(
        "/local/folders",
        params=_stack_params(mailbox_id=mailbox_id),
    )


@router.get("/local/folder-messages")
async def email_local_folder_messages(
    folder: str = Query("INBOX", min_length=1, max_length=180),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    limit: int = Query(100, ge=1, le=200),
    offset: int = Query(0, ge=0, le=1000000),
) -> dict[str, Any]:
    return await _stack_get_json(
        "/local/folder-messages",
        params=_stack_params(folder=folder, mailbox_id=mailbox_id, limit=limit, offset=offset),
    )


@router.get("/local/cache/status")
async def email_local_cache_status(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    data = await _stack_get_json(
        "/local/cache/status",
        params=_stack_params(mailbox_id=mailbox_id),
    )
    data["proxy_image_cache"] = PIM_EMAIL_PROXY_IMAGE_CACHE.stats()
    return data


@router.post("/local/cache/warm")
async def email_local_cache_warm(body: LocalCacheWarmRequest) -> dict[str, Any]:
    return await _stack_post_json(
        "/local/cache/warm",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
    )


@router.get("/local/messages/{email_uid}")
async def email_local_message(
    email_uid: str,
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    data = await _stack_get_json(
        f"/local/messages/{email_uid}",
        params=_stack_params(mailbox_id=mailbox_id),
    )
    message = data.get("message") if isinstance(data.get("message"), dict) else None
    if message and not message.get("body_blocked"):
        data["proxy_image_warm"] = _schedule_proxy_image_warm(
            message,
            mailbox_id=mailbox_id,
            email_uid=email_uid,
        )
    return data


@router.post("/local/messages/{email_uid}/force-refresh")
async def email_local_message_force_refresh(
    email_uid: str,
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    proxy_invalidated = PIM_EMAIL_PROXY_IMAGE_CACHE.invalidate_email(mailbox_id, email_uid)
    try:
        data = await _stack_post_json(
            f"/local/messages/{email_uid}/force-refresh",
            params=_stack_params(mailbox_id=mailbox_id),
        )
    except HTTPException as exc:
        if isinstance(exc.detail, dict):
            exc.detail["proxy_image_cache_invalidated"] = proxy_invalidated
        raise
    proxy_invalidated += PIM_EMAIL_PROXY_IMAGE_CACHE.invalidate_email(mailbox_id, email_uid)
    data["proxy_image_cache_invalidated"] = proxy_invalidated
    return data


@router.get("/local/images/{shared_asset_uid}")
async def email_local_image(
    shared_asset_uid: str,
    email_uid: str = Query(..., min_length=8, max_length=80),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> Response:
    cache_key = (str(mailbox_id or ""), str(email_uid or ""), str(shared_asset_uid or ""))
    cached = PIM_EMAIL_PROXY_IMAGE_CACHE.get(cache_key)
    if cached is not None:
        headers = dict(cached.get("headers") or {})
        headers["X-PIM-Email-Image-Cache"] = "hit"
        return Response(
            content=bytes(cached.get("content") or b""),
            media_type=str(cached.get("media_type") or "image/jpeg"),
            headers=headers,
        )
    response = await _stack_get_binary(
        f"/local/images/{shared_asset_uid}",
        params=_stack_params(email_uid=email_uid, mailbox_id=mailbox_id),
    )
    headers = {
        key: value
        for key, value in {
            "Cache-Control": response.headers.get("cache-control"),
            "ETag": response.headers.get("etag"),
            "X-Content-Type-Options": response.headers.get("x-content-type-options"),
        }.items()
        if value
    }
    PIM_EMAIL_PROXY_IMAGE_CACHE.put(
        cache_key,
        {
            "content": bytes(response.body or b""),
            "media_type": response.media_type or "image/jpeg",
            "headers": headers,
        },
    )
    headers["X-PIM-Email-Image-Cache"] = "miss"
    return Response(
        content=bytes(response.body or b""),
        media_type=response.media_type or "image/jpeg",
        headers=headers,
    )


@router.get("/local/health")
async def email_local_health(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    return await _stack_get_json(
        "/local/health",
        params=_stack_params(mailbox_id=mailbox_id),
    )


@router.post("/local/messages/{email_uid}/security")
async def email_local_message_security(
    email_uid: str,
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    security_run_id: str | None = Query(None, min_length=8, max_length=120),
) -> dict[str, Any]:
    return await _stack_post_json(
        f"/local/messages/{email_uid}/security",
        params=_stack_params(mailbox_id=mailbox_id, security_run_id=security_run_id),
    )


@router.post("/local/messages/{email_uid}/probable-trusted-sender")
async def email_local_message_probable_trusted_sender(
    email_uid: str,
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    return await _stack_post_json(
        f"/local/messages/{email_uid}/probable-trusted-sender",
        params=_stack_params(mailbox_id=mailbox_id),
    )


@router.get("/folders")
async def email_folders(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        folders = await list_folders(mailbox)
        return {"ok": True, "mailbox": mailbox.public_dict(), "folders": folders}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/inbox")
async def email_inbox(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    limit: int = Query(25, ge=1, le=100),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        messages = await list_inbox(mailbox, limit=limit)
        return {
            "ok": True,
            "mailbox": mailbox.public_dict(),
            "folder": "INBOX",
            "messages": messages,
        }
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/folder-messages")
async def email_folder_messages(
    folder: str = Query("INBOX", min_length=1, max_length=180),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    limit: int = Query(25, ge=1, le=100),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        messages = await list_folder_messages(mailbox, folder=folder, limit=limit)
        return {
            "ok": True,
            "mailbox": mailbox.public_dict(),
            "folder": folder,
            "messages": messages,
        }
    except Exception as exc:
        raise _http_error(exc) from exc


@router.post("/download/run")
async def email_download_run(body: DownloadMailboxRequest) -> dict[str, Any]:
    try:
        result = await _start_stack_download(body)
        return {"ok": True, "started": True, "result": result}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/messages/{uid}")
async def email_message(
    uid: str,
    folder: str = Query("INBOX", min_length=1, max_length=180),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    security_run_id: str | None = Query(None, min_length=8, max_length=120),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        run_id = _clean_security_run_id(security_run_id)
        message = await fetch_message(
            mailbox,
            folder=folder,
            uid=uid,
            security_progress_callback=_security_progress_emitter(
                loop=asyncio.get_running_loop(),
                run_id=run_id,
                mailbox_id=mailbox.mailbox_id,
                folder=folder,
                uid=uid,
            ),
        )
        if isinstance(message.get("security"), dict):
            _attach_security_run_id(message["security"], run_id)
        await store.record_security_result(message, mailbox_id=mailbox.mailbox_id)
        return {"ok": True, "mailbox": mailbox.public_dict(), "message": message}
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/messages/{uid}/security")
async def email_message_security(
    uid: str,
    folder: str = Query("INBOX", min_length=1, max_length=180),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    security_run_id: str | None = Query(None, min_length=8, max_length=120),
) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox(mailbox_id)
        run_id = _clean_security_run_id(security_run_id)
        security = await fetch_message_security(
            mailbox,
            folder=folder,
            uid=uid,
            security_progress_callback=_security_progress_emitter(
                loop=asyncio.get_running_loop(),
                run_id=run_id,
                mailbox_id=mailbox.mailbox_id,
                folder=folder,
                uid=uid,
            ),
        )
        _attach_security_run_id(security, run_id)
        await store.record_security_result(
            {
                "uid": uid,
                "folder": folder,
                "headers": {"message_id": security.get("context", {}).get("message_id", "")},
                "security": security,
            },
            mailbox_id=mailbox.mailbox_id,
        )
        return {
            "ok": True,
            "mailbox": mailbox.public_dict(),
            "folder": folder,
            "uid": uid,
            "security": security,
        }
    except Exception as exc:
        raise _http_error(exc) from exc


@router.get("/image-proxy")
async def email_image_proxy(
    src: str = Query(..., min_length=8, max_length=4096),
    sig: str = Query(..., min_length=32, max_length=128),
) -> dict[str, Any]:
    raise HTTPException(
        status_code=410,
        detail=(
            "PIM Email remote image proxying is disabled. Remote images must be "
            "downloaded and transformed by the Dockge remote image worker pipeline."
        ),
    )


@router.post("/smtp-self-test")
async def email_smtp_self_test(body: SmtpSelfTestRequest) -> dict[str, Any]:
    try:
        store = _store()
        mailbox = await store.get_mailbox()
        proof = await smtp_self_send(mailbox, recipient=body.recipient)
        return {"ok": True, "proof": proof}
    except Exception as exc:
        raise _http_error(exc) from exc
