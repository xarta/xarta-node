"""Blueprints PIM Email API routes."""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import os
import subprocess
import time
import uuid
from collections.abc import AsyncIterable
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from . import timing
from .events import AppEvent, bus

router = APIRouter(prefix="/personal/email", tags=["personal-email"])
log = logging.getLogger(__name__)

DEFAULT_DOWNLOADED_FOLDER = "Downloaded"
PIM_EMAIL_STACK_DIR = Path("/xarta-node/.lone-wolf/stacks/pim-email")
PIM_EMAIL_STACK_COMMAND_TIMEOUT_SECONDS = 25.0
PIM_EMAIL_STACK_API_BASE = os.environ.get(
    "PIM_EMAIL_STACK_API_BASE",
    "http://127.0.0.1:18085",
).rstrip("/")
PIM_EMAIL_STACK_API_TIMEOUT_SECONDS = float(
    os.environ.get("PIM_EMAIL_STACK_API_TIMEOUT_SECONDS", "10")
)
PIM_EMAIL_STACK_FORCE_REFRESH_TIMEOUT_SECONDS = float(
    os.environ.get("PIM_EMAIL_STACK_FORCE_REFRESH_TIMEOUT_SECONDS", "45")
)
PIM_EMAIL_STACK_LOCAL_VIEW_REFRESH_TIMEOUT_SECONDS = float(
    os.environ.get("PIM_EMAIL_STACK_LOCAL_VIEW_REFRESH_TIMEOUT_SECONDS", "30")
)
PIM_EMAIL_STACK_SEARCH_TIMEOUT_SECONDS = float(
    os.environ.get("PIM_EMAIL_STACK_SEARCH_TIMEOUT_SECONDS", "60")
)
PIM_EMAIL_CACHE_STATE_EVENT = "pim.email.cache.state"
PIM_EMAIL_CACHE_STATE_WATCH_SECONDS = float(
    os.environ.get("PIM_EMAIL_CACHE_STATE_WATCH_SECONDS", "20")
)
PIM_EMAIL_CACHE_STATE_WATCH_INTERVAL_SECONDS = float(
    os.environ.get("PIM_EMAIL_CACHE_STATE_WATCH_INTERVAL_SECONDS", "0.75")
)
PIM_EMAIL_CACHE_STATE_WATCH_MAX_POLLS = int(
    os.environ.get("PIM_EMAIL_CACHE_STATE_WATCH_MAX_POLLS", "5")
)
PIM_EMAIL_MESSAGE_OPEN_CACHE_STATE_WATCH_DELAY_SECONDS = float(
    os.environ.get("PIM_EMAIL_MESSAGE_OPEN_CACHE_STATE_WATCH_DELAY_SECONDS", "1.0")
)
_CACHE_STATE_WATCH_KEYS: set[tuple[str, tuple[str, ...]]] = set()
_CACHE_STATE_LAST_SIGNATURES: dict[str, str] = {}
_STACK_HTTP_CLIENTS: dict[str, httpx.AsyncClient] = {}
_STACK_HTTP_CLIENT_LOCK = asyncio.Lock()
_STACK_HTTP_FOREGROUND_LIMITS = httpx.Limits(
    max_connections=int(os.environ.get("PIM_EMAIL_STACK_PROXY_MAX_CONNECTIONS", "64")),
    max_keepalive_connections=int(
        os.environ.get("PIM_EMAIL_STACK_PROXY_MAX_KEEPALIVE_CONNECTIONS", "20")
    ),
    keepalive_expiry=float(os.environ.get("PIM_EMAIL_STACK_PROXY_KEEPALIVE_SECONDS", "30")),
)
_STACK_HTTP_WORKER_LIMITS = httpx.Limits(
    max_connections=int(os.environ.get("PIM_EMAIL_STACK_PROXY_WORKER_MAX_CONNECTIONS", "4")),
    max_keepalive_connections=int(
        os.environ.get("PIM_EMAIL_STACK_PROXY_WORKER_MAX_KEEPALIVE_CONNECTIONS", "4")
    ),
    keepalive_expiry=float(os.environ.get("PIM_EMAIL_STACK_PROXY_KEEPALIVE_SECONDS", "30")),
)
_STACK_HTTP_BACKGROUND_LIMITS = httpx.Limits(
    max_connections=int(os.environ.get("PIM_EMAIL_STACK_PROXY_BACKGROUND_MAX_CONNECTIONS", "1")),
    max_keepalive_connections=int(
        os.environ.get("PIM_EMAIL_STACK_PROXY_BACKGROUND_MAX_KEEPALIVE_CONNECTIONS", "1")
    ),
    keepalive_expiry=float(os.environ.get("PIM_EMAIL_STACK_PROXY_KEEPALIVE_SECONDS", "30")),
)


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


class LocalMessageOpenedRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    actor: str = Field("email-ui", max_length=180)
    source_surface: str = Field("pim-email-ui", max_length=180)
    request_id: str = Field("", max_length=180)
    metadata: dict[str, Any] = Field(default_factory=dict)


class LocalVirtualPathRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    virtual_path: str = Field("", max_length=180)
    operation: str = Field("add", max_length=24)
    source_virtual_path: str = Field("", max_length=180)
    destination_virtual_path: str = Field("", max_length=180)
    actor: str = Field("email-ui", max_length=180)
    source_surface: str = Field("pim-email-ui", max_length=180)
    request_id: str = Field("", max_length=180)
    metadata: dict[str, Any] = Field(default_factory=dict)


class LocalVirtualPathCatalogRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    virtual_path: str = Field("", max_length=180)
    path: str = Field("", max_length=180)
    actor: str = Field("email-ui", max_length=180)
    source_surface: str = Field("pim-email-ui", max_length=180)
    request_id: str = Field("", max_length=180)
    metadata: dict[str, Any] = Field(default_factory=dict)


class LocalVirtualPathMoveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    source_virtual_path: str = Field("", max_length=180)
    destination_virtual_path: str = Field("", max_length=180)
    source_path: str = Field("", max_length=180)
    destination_path: str = Field("", max_length=180)
    dry_run: bool = True
    limit: int = Field(5000, ge=1, le=20000)
    actor: str = Field("email-ui", max_length=180)
    source_surface: str = Field("pim-email-ui", max_length=180)
    request_id: str = Field("", max_length=180)
    metadata: dict[str, Any] = Field(default_factory=dict)


class LocalVirtualPathSubtreeRequest(LocalVirtualPathMoveRequest):
    operation: str = Field(..., pattern="^(copy|move|archive|delete)$")


class LocalVirtualPathsReplaceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    virtual_paths: list[str] = Field(default_factory=list, max_length=100)
    actor: str = Field("email-ui", max_length=180)
    source_surface: str = Field("pim-email-ui", max_length=180)
    request_id: str = Field("", max_length=180)
    metadata: dict[str, Any] = Field(default_factory=dict)


class VirtualPathRuleCreateRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    display_name: str = Field("Virtual path rule", max_length=180)
    name: str = Field("", max_length=180)
    description: str = Field("", max_length=2000)
    predicate: dict[str, Any] = Field(default_factory=dict)
    action: dict[str, Any] = Field(default_factory=dict)
    scope: dict[str, Any] = Field(default_factory=dict)
    sequence: int = Field(1000, ge=0, le=1000000)
    stop_on_match: bool = False
    actor: str = Field("email-ui", max_length=180)
    source_surface: str = Field("pim-email-ui", max_length=180)
    metadata: dict[str, Any] = Field(default_factory=dict)


class VirtualPathRuleUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    display_name: str | None = Field(None, max_length=180)
    name: str | None = Field(None, max_length=180)
    description: str | None = Field(None, max_length=2000)
    status: str | None = Field(None, max_length=24)
    sequence: int | None = Field(None, ge=0, le=1000000)
    stop_on_match: bool | None = None
    predicate: dict[str, Any] | None = None
    action: dict[str, Any] | None = None
    scope: dict[str, Any] | None = None
    actor: str = Field("email-ui", max_length=180)
    metadata: dict[str, Any] = Field(default_factory=dict)


class VirtualPathRuleApplyRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    rule_ids: list[str] = Field(default_factory=list, max_length=100)
    scope: dict[str, Any] = Field(default_factory=dict)
    dry_run: bool = True
    actor: str = Field("pim-email-vpath-rule-worker", max_length=180)
    source_surface: str = Field("pim-email-blueprints-proxy", max_length=180)
    request_id: str = Field("", max_length=180)
    run_id: str = Field("", max_length=220)


class EmailSearchRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    mode: str = Field("simple", max_length=20)
    query: str = Field("", max_length=1000)
    terms: list[dict[str, Any]] = Field(default_factory=list, max_length=12)
    folder: str = Field("", max_length=180)
    folder_uid: str = Field("", max_length=180)
    sent_from: str = Field("", max_length=80)
    sent_to: str = Field("", max_length=80)
    received_from: str = Field("", max_length=80)
    received_to: str = Field("", max_length=80)
    date_ranges: dict[str, Any] = Field(default_factory=dict)
    hybrid: bool = True
    rerank: bool = True
    limit: int = Field(50, ge=1, le=200)
    offset: int = Field(0, ge=0, le=1000000)


class TrustedProbableSenderRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mailbox_id: str | None = Field(None, min_length=1, max_length=120)
    sender_email: str = Field(..., min_length=3, max_length=254)
    metadata: dict[str, Any] = Field(default_factory=dict)


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


class PimEmailControlError(RuntimeError):
    pass


def _http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, HTTPException):
        return exc
    if isinstance(exc, PimEmailControlError):
        return HTTPException(status_code=502, detail=str(exc))
    return HTTPException(status_code=502, detail="PIM Email control operation failed")


def _stack_params(**params: Any) -> dict[str, Any]:
    return {key: value for key, value in params.items() if value is not None}


def _stack_request_class(path: str) -> str:
    if path.startswith("/workers/"):
        return "worker"
    if path.startswith("/local/activity"):
        return "background_cache"
    if path.startswith("/local/cache/"):
        return "cache"
    if path.startswith("/local/messages/"):
        return "message"
    if path.startswith("/local/images/"):
        return "background_asset"
    return "control"


def _message_request_class(priority: str | None) -> str:
    clean = str(priority or "").strip().lower()
    if clean in {"background", "low", "prefetch", "warm"}:
        return "background_message"
    return "message"


def _stack_client_key(traffic_class: str) -> str:
    if traffic_class == "worker":
        return "worker"
    if traffic_class in {"background_message", "background_cache", "background_asset"}:
        return "background"
    return "foreground"


def _stack_client_limits(client_key: str) -> httpx.Limits:
    if client_key == "worker":
        return _STACK_HTTP_WORKER_LIMITS
    if client_key == "background":
        return _STACK_HTTP_BACKGROUND_LIMITS
    return _STACK_HTTP_FOREGROUND_LIMITS


async def _stack_http_client(traffic_class: str) -> httpx.AsyncClient:
    client_key = _stack_client_key(traffic_class)
    client = _STACK_HTTP_CLIENTS.get(client_key)
    if client is not None and not client.is_closed:
        return client
    async with _STACK_HTTP_CLIENT_LOCK:
        client = _STACK_HTTP_CLIENTS.get(client_key)
        if client is None or client.is_closed:
            client = httpx.AsyncClient(
                timeout=None,
                limits=_stack_client_limits(client_key),
                trust_env=False,
            )
            _STACK_HTTP_CLIENTS[client_key] = client
        return client


@router.on_event("shutdown")
async def _close_stack_http_client() -> None:
    clients = list(_STACK_HTTP_CLIENTS.values())
    _STACK_HTTP_CLIENTS.clear()
    for client in clients:
        if not client.is_closed:
            await client.aclose()


async def _stack_request(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    content: bytes | AsyncIterable[bytes] | None = None,
    headers: dict[str, str] | None = None,
    timeout_seconds: float | None = None,
    traffic_class: str | None = None,
) -> httpx.Response:
    url = f"{PIM_EMAIL_STACK_API_BASE}{path}"
    clean_method = method.upper()
    clean_traffic_class = traffic_class or _stack_request_class(path)
    client_key = _stack_client_key(clean_traffic_class)
    timeout = httpx.Timeout(
        timeout_seconds if timeout_seconds is not None else PIM_EMAIL_STACK_API_TIMEOUT_SECONDS
    )
    started_perf = time.perf_counter_ns()
    started_wall = time.time_ns()
    status_code = 0
    response_bytes = 0
    ok = True
    error_type = ""
    try:
        client = await _stack_http_client(clean_traffic_class)
        response = await client.request(
            clean_method,
            url,
            params=params or {},
            json=json_body if json_body is not None else None,
            content=content if json_body is None else None,
            headers=headers or {},
            timeout=timeout,
        )
        status_code = int(response.status_code)
        response_bytes = len(response.content or b"")
        return response
    except httpx.TimeoutException as exc:
        ok = False
        error_type = exc.__class__.__name__
        raise HTTPException(status_code=504, detail="PIM Email stack API timed out") from exc
    except httpx.HTTPError as exc:
        ok = False
        error_type = exc.__class__.__name__
        raise HTTPException(status_code=503, detail="PIM Email stack API is unavailable") from exc
    finally:
        timing.record_span(
            "pim_email.stack_proxy",
            start_perf_ns=started_perf,
            end_perf_ns=time.perf_counter_ns(),
            start_time_ns=started_wall,
            end_time_ns=time.time_ns(),
            method=clean_method,
            upstream_path=path,
            traffic_class=clean_traffic_class,
            status_code=status_code,
            response_bytes=response_bytes,
            ok=ok,
            error_type=error_type,
            client_key=client_key,
        )


def _stack_response_json(response: httpx.Response) -> dict[str, Any]:
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


async def _stack_get_json(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    traffic_class: str | None = None,
) -> dict[str, Any]:
    response = await _stack_request("GET", path, params=params, traffic_class=traffic_class)
    return _stack_response_json(response)


async def _stack_post_json(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout_seconds: float | None = None,
    traffic_class: str | None = None,
) -> dict[str, Any]:
    response = await _stack_request(
        "POST",
        path,
        params=params,
        json_body=json_body or {},
        timeout_seconds=timeout_seconds,
        traffic_class=traffic_class,
    )
    return _stack_response_json(response)


async def _stack_patch_json(
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
    timeout_seconds: float | None = None,
    traffic_class: str | None = None,
) -> dict[str, Any]:
    response = await _stack_request(
        "PATCH",
        path,
        params=params,
        json_body=json_body or {},
        timeout_seconds=timeout_seconds,
        traffic_class=traffic_class,
    )
    return _stack_response_json(response)


async def _stack_post_streamed_json(
    path: str,
    request: Request,
    *,
    timeout_seconds: float | None = None,
) -> dict[str, Any]:
    content_type = request.headers.get("content-type") or "application/json"
    response = await _stack_request(
        "POST",
        path,
        content=request.stream(),
        headers={"Content-Type": content_type},
        timeout_seconds=timeout_seconds,
    )
    return _stack_response_json(response)


async def _stack_delete_json(
    path: str,
    *,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = await _stack_request("DELETE", path, params=params)
    return _stack_response_json(response)


async def _stack_get_binary(path: str, *, params: dict[str, Any] | None = None) -> Response:
    response = await _stack_request("GET", path, params=params)
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


async def _stack_mailbox_public(mailbox_id: str | None = None) -> dict[str, Any]:
    data = await _stack_get_json(
        "/mailbox",
        params=_stack_params(mailbox_id=mailbox_id),
    )
    mailbox = data.get("mailbox")
    if not isinstance(mailbox, dict):
        raise HTTPException(status_code=502, detail="PIM Email stack API returned no mailbox")
    return mailbox


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


def _message_states_from_response(response: dict[str, Any] | None) -> dict[str, Any]:
    states = response.get("message_states") if isinstance(response, dict) else None
    if not isinstance(states, dict):
        return {}
    clean: dict[str, Any] = {}
    for uid, value in states.items():
        if not isinstance(value, dict):
            continue
        clean_uid = str(uid or value.get("email_uid") or "").strip()
        if clean_uid:
            clean[clean_uid] = value
    return clean


def _cache_state_uids_from_body(body: LocalCacheWarmRequest | None) -> list[str]:
    if body is None:
        return []
    seen: set[str] = set()
    uids: list[str] = []
    for uid in body.email_uids or []:
        clean = str(uid or "").strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        uids.append(clean)
    return uids


async def _publish_pim_email_cache_state(
    states: dict[str, Any],
    *,
    mailbox_id: str | None,
    reason: str,
) -> None:
    if not states:
        return
    event = AppEvent.create(
        PIM_EMAIL_CACHE_STATE_EVENT,
        "PIM Email Cache State",
        f"{len(states)} message cache state update{'s' if len(states) != 1 else ''}",
        source="pim-email-blueprints-proxy",
        payload={
            "schema": "xarta.pim_email.cache_state.event.v1",
            "reason": reason,
            "mailbox_id": str(mailbox_id or ""),
            "message_states": states,
            "count": len(states),
            "sent_at": time.time(),
        },
    )
    await bus.publish(event)


def _schedule_pim_email_cache_state_event(
    response: dict[str, Any] | None,
    *,
    reason: str,
    mailbox_id: str | None = None,
) -> None:
    states = _message_states_from_response(response)
    if not states:
        return

    async def run() -> None:
        await _publish_pim_email_cache_state(states, mailbox_id=mailbox_id, reason=reason)

    task = asyncio.create_task(run())
    task.add_done_callback(_log_background_task_result)


def _state_signature(value: dict[str, Any]) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    except TypeError:
        return repr(value)


def _state_is_server_complete(value: dict[str, Any] | None) -> bool:
    return bool(value and value.get("server_complete"))


async def _watch_pim_email_cache_states(
    mailbox_id: str | None,
    uids: tuple[str, ...],
    *,
    reason: str,
    initial_delay_seconds: float = 0.0,
    max_polls: int | None = None,
) -> None:
    deadline = time.monotonic() + max(1.0, PIM_EMAIL_CACHE_STATE_WATCH_SECONDS)
    interval = max(0.1, PIM_EMAIL_CACHE_STATE_WATCH_INTERVAL_SECONDS)
    poll_limit = max(1, int(max_polls or PIM_EMAIL_CACHE_STATE_WATCH_MAX_POLLS))
    if initial_delay_seconds > 0:
        await asyncio.sleep(initial_delay_seconds)
    polls = 0
    while time.monotonic() <= deadline and polls < poll_limit:
        polls += 1
        try:
            response = await _stack_post_json(
                "/local/cache/messages",
                params=_stack_params(mailbox_id=mailbox_id),
                json_body={
                    "mailbox_id": mailbox_id,
                    "email_uids": list(uids),
                    "limit": len(uids),
                },
                traffic_class="background_cache",
            )
        except Exception as exc:
            log.debug("pim email cache-state watch failed: %s", exc)
            await asyncio.sleep(interval)
            continue
        states = _message_states_from_response(response)
        changed: dict[str, Any] = {}
        for uid, value in states.items():
            key = f"{mailbox_id or ''}:{uid}"
            signature = _state_signature(value)
            if _CACHE_STATE_LAST_SIGNATURES.get(key) == signature:
                continue
            _CACHE_STATE_LAST_SIGNATURES[key] = signature
            changed[uid] = value
        if changed:
            await _publish_pim_email_cache_state(
                changed,
                mailbox_id=mailbox_id,
                reason=reason,
            )
        if states and all(_state_is_server_complete(states.get(uid)) for uid in uids):
            return
        await asyncio.sleep(interval)


def _log_background_task_result(task: asyncio.Task[Any]) -> None:
    try:
        task.result()
    except Exception as exc:
        log.debug("pim email background task failed: %s", exc)


def _schedule_pim_email_cache_state_watch(
    mailbox_id: str | None,
    uids: list[str] | tuple[str, ...],
    *,
    reason: str,
    initial_delay_seconds: float = 0.0,
    max_polls: int | None = None,
) -> None:
    clean_uids = tuple(
        dict.fromkeys(str(uid or "").strip() for uid in uids if str(uid or "").strip())
    )
    if not clean_uids:
        return
    key = (str(mailbox_id or ""), clean_uids)
    if key in _CACHE_STATE_WATCH_KEYS:
        return
    _CACHE_STATE_WATCH_KEYS.add(key)

    async def run() -> None:
        try:
            await _watch_pim_email_cache_states(
                mailbox_id,
                clean_uids,
                reason=reason,
                initial_delay_seconds=initial_delay_seconds,
                max_polls=max_polls,
            )
        finally:
            _CACHE_STATE_WATCH_KEYS.discard(key)

    task = asyncio.create_task(run())
    task.add_done_callback(_log_background_task_result)


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
        raise PimEmailControlError("PIM Email stack script path escaped stack root") from exc
    if not script.exists():
        raise PimEmailControlError(f"PIM Email stack script is missing: {script_name}")
    completed = subprocess.run(
        [str(script), *args],
        cwd=str(PIM_EMAIL_STACK_DIR),
        text=True,
        capture_output=True,
        timeout=PIM_EMAIL_STACK_COMMAND_TIMEOUT_SECONDS,
        check=False,
    )
    if completed.returncode != 0:
        raise PimEmailControlError(
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
    mailbox = await _stack_mailbox_public(body.mailbox_id)
    mailbox_id = str(mailbox.get("mailbox_id") or body.mailbox_id or "")
    run_id = f"pim-email-stack-download-{uuid.uuid4().hex}"
    args = [run_id]
    if mailbox_id:
        args.extend(["--mailbox-id", mailbox_id])
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
    try:
        await _stack_post_json(
            "/local/activity/mark",
            params=_stack_params(mailbox_id=mailbox_id),
            json_body={
                "kind": "download",
                "activity_id": run_id,
                "run_id": run_id,
                "mailbox_id": mailbox_id,
                "status": "active",
                "label": "checking IMAP folders",
                "tone": "green",
                "active_seconds": 2.5,
                "recent_seconds": 10.0,
                "metadata": {"source": "blueprints-download-start"},
            },
            timeout_seconds=1.0,
            traffic_class="background_cache",
        )
    except Exception:
        pass
    return {
        "schema": "xarta.pim_email.stack_control.download_start.v1",
        "mailbox": mailbox,
        "mailbox_id": mailbox_id,
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
        mailbox_public = await _stack_mailbox_public(body.mailbox_id)
        args.extend(["--mailbox-id", str(mailbox_public.get("mailbox_id") or body.mailbox_id)])
    args.extend(["--batch-size", str(body.batch_size)])
    if body.repeat_until_idle:
        args.append("--repeat-until-idle")
    if body.max_batches is not None:
        args.extend(["--max-batches", str(body.max_batches)])
    command = await asyncio.to_thread(_run_stack_control_script, "start-shared-assets.sh", args)
    return {
        "schema": "xarta.pim_email.stack_control.external_image_maintenance_start.v1",
        "mailbox": mailbox_public,
        "mailbox_id": str((mailbox_public or {}).get("mailbox_id") or body.mailbox_id or ""),
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
    _require_email_worker_token(x_pim_email_worker_token)
    response = await _stack_get_json(
        "/workers/security/status",
        params=_stack_params(
            mailbox_id=mailbox_id,
            include_local=include_local,
            include_worker_blocks=include_worker_blocks,
        ),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.post("/workers/security/assignments/reconcile")
async def email_security_worker_reconcile_assignments(
    body: SecurityAssignmentReconcileRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    response = await _stack_post_json(
        "/workers/security/assignments/reconcile",
        json_body=body.model_dump(exclude_none=True),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.post("/workers/security/assignments/claim")
async def email_security_worker_claim_assignments(
    body: SecurityAssignmentClaimRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    response = await _stack_post_json(
        "/workers/security/assignments/claim",
        json_body=body.model_dump(exclude_none=True),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.post("/workers/security/assignments/heartbeat")
async def email_security_worker_heartbeat_assignments(
    body: SecurityAssignmentHeartbeatRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    response = await _stack_post_json(
        "/workers/security/assignments/heartbeat",
        json_body=body.model_dump(exclude_none=True),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.post("/workers/security/assignments/release")
async def email_security_worker_release_assignments(
    body: SecurityAssignmentReleaseRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    response = await _stack_post_json(
        "/workers/security/assignments/release",
        json_body=body.model_dump(exclude_none=True),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.post("/workers/security/assignments/{assignment_id}/complete")
async def email_security_worker_complete_assignment(
    assignment_id: str,
    body: SecurityAssignmentCompleteRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    response = await _stack_post_json(
        f"/workers/security/assignments/{assignment_id}/complete",
        json_body=body.model_dump(exclude_none=True),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.post("/workers/security/assignments/{assignment_id}/fail")
async def email_security_worker_fail_assignment(
    assignment_id: str,
    body: SecurityAssignmentFailRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    response = await _stack_post_json(
        f"/workers/security/assignments/{assignment_id}/fail",
        json_body=body.model_dump(exclude_none=True),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.get("/workers/external-images/status")
async def email_external_image_worker_status(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    include_derivatives: bool = Query(False),
    include_worker_blocks: bool = Query(False),
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    response = await _stack_get_json(
        "/workers/external-images/status",
        params=_stack_params(
            mailbox_id=mailbox_id,
            include_derivatives=include_derivatives,
            include_worker_blocks=include_worker_blocks,
        ),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


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
    response = await _stack_post_json(
        "/workers/external-images/assignments/claim",
        json_body=body.model_dump(exclude_none=True),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.post("/workers/external-images/assignments/heartbeat")
async def email_external_image_worker_heartbeat_assignments(
    body: ExternalImageAssignmentHeartbeatRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    response = await _stack_post_json(
        "/workers/external-images/assignments/heartbeat",
        json_body=body.model_dump(exclude_none=True),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.post("/workers/external-images/assignments/release")
async def email_external_image_worker_release_assignments(
    body: ExternalImageAssignmentReleaseRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    response = await _stack_post_json(
        "/workers/external-images/assignments/release",
        json_body=body.model_dump(exclude_none=True),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.post("/workers/external-images/assignments/{canonical_url_digest}/complete")
async def email_external_image_worker_complete_assignment(
    canonical_url_digest: str,
    request: Request,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    response = await _stack_post_streamed_json(
        f"/workers/external-images/assignments/{canonical_url_digest}/complete",
        request,
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


@router.post("/workers/external-images/assignments/{canonical_url_digest}/fail")
async def email_external_image_worker_fail_assignment(
    canonical_url_digest: str,
    body: ExternalImageAssignmentFailRequest,
    x_pim_email_worker_token: str | None = Header(None, alias="X-PIM-Email-Worker-Token"),
    metrics: bool = Query(False),
) -> dict[str, Any]:
    started_at = time.perf_counter()
    _require_email_worker_token(x_pim_email_worker_token)
    response = await _stack_post_json(
        f"/workers/external-images/assignments/{canonical_url_digest}/fail",
        json_body=body.model_dump(exclude_none=True),
    )
    return _attach_server_metrics(
        response,
        metrics=metrics,
        started_at=started_at,
        stages={"stack_proxy_seconds": time.perf_counter() - started_at},
    )


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


@router.post("/local/search")
async def email_local_search(body: EmailSearchRequest) -> dict[str, Any]:
    return await _stack_post_json(
        "/local/search",
        json_body=body.model_dump(exclude_none=True),
        timeout_seconds=PIM_EMAIL_STACK_SEARCH_TIMEOUT_SECONDS,
    )


@router.get("/local/cache/status")
async def email_local_cache_status(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    return await _stack_get_json(
        "/local/cache/status",
        params=_stack_params(mailbox_id=mailbox_id),
    )


@router.get("/local/activity")
async def email_local_activity(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    return await _stack_get_json(
        "/local/activity",
        params=_stack_params(mailbox_id=mailbox_id),
        traffic_class="background_cache",
    )


@router.get("/local/virtual-paths/audit-gate")
async def email_local_virtual_paths_audit_gate(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    return await _stack_get_json(
        "/local/virtual-paths/audit-gate",
        params=_stack_params(mailbox_id=mailbox_id),
    )


@router.get("/local/virtual-paths")
async def email_local_virtual_paths(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    return await _stack_get_json(
        "/local/virtual-paths",
        params=_stack_params(mailbox_id=mailbox_id),
    )


@router.post("/local/virtual-paths")
async def email_local_virtual_path_create(
    body: LocalVirtualPathCatalogRequest,
) -> dict[str, Any]:
    return await _stack_post_json(
        "/local/virtual-paths",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
    )


@router.post("/local/virtual-paths/archive")
async def email_local_virtual_path_archive(
    body: LocalVirtualPathCatalogRequest,
) -> dict[str, Any]:
    return await _stack_post_json(
        "/local/virtual-paths/archive",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
    )


@router.post("/local/virtual-paths/rename")
async def email_local_virtual_path_rename(
    body: LocalVirtualPathMoveRequest,
) -> dict[str, Any]:
    return await _stack_post_json(
        "/local/virtual-paths/rename",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
        timeout_seconds=PIM_EMAIL_STACK_SEARCH_TIMEOUT_SECONDS,
    )


@router.post("/local/virtual-paths/subtree")
async def email_local_virtual_path_subtree_operation(
    body: LocalVirtualPathSubtreeRequest,
) -> dict[str, Any]:
    return await _stack_post_json(
        "/local/virtual-paths/subtree",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
        timeout_seconds=PIM_EMAIL_STACK_SEARCH_TIMEOUT_SECONDS,
    )


@router.post("/local/virtual-paths/bulk-move")
async def email_local_virtual_path_bulk_move(
    body: LocalVirtualPathMoveRequest,
) -> dict[str, Any]:
    return await _stack_post_json(
        "/local/virtual-paths/bulk-move",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
        timeout_seconds=PIM_EMAIL_STACK_SEARCH_TIMEOUT_SECONDS,
    )


@router.post("/local/cache/warm")
async def email_local_cache_warm(body: LocalCacheWarmRequest) -> dict[str, Any]:
    response = await _stack_post_json(
        "/local/cache/warm",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
        traffic_class="background_cache",
    )
    uids = _cache_state_uids_from_body(body) or list(_message_states_from_response(response))
    _schedule_pim_email_cache_state_event(
        response,
        reason="cache-warm-response",
        mailbox_id=body.mailbox_id,
    )
    _schedule_pim_email_cache_state_watch(
        body.mailbox_id,
        uids,
        reason="cache-warm-watch",
    )
    return response


@router.post("/local/cache/messages")
async def email_local_cache_messages(body: LocalCacheWarmRequest) -> dict[str, Any]:
    response = await _stack_post_json(
        "/local/cache/messages",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
        traffic_class="background_cache",
    )
    _schedule_pim_email_cache_state_event(
        response,
        reason="cache-state-response",
        mailbox_id=body.mailbox_id,
    )
    return response


@router.get("/local/messages/{email_uid}")
async def email_local_message(
    email_uid: str,
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    opened: bool = Query(True),
    x_pim_email_client_priority: str | None = Header(None, alias="X-PIM-Email-Client-Priority"),
) -> dict[str, Any]:
    response = await _stack_get_json(
        f"/local/messages/{email_uid}",
        params=_stack_params(mailbox_id=mailbox_id, opened=opened),
        traffic_class=_message_request_class(x_pim_email_client_priority),
    )
    _schedule_pim_email_cache_state_watch(
        mailbox_id,
        [email_uid],
        reason="message-open-watch" if opened else "message-read-watch",
        initial_delay_seconds=PIM_EMAIL_MESSAGE_OPEN_CACHE_STATE_WATCH_DELAY_SECONDS,
        max_polls=1,
    )
    return response


@router.post("/local/messages/{email_uid}/opened")
async def email_local_message_opened(
    email_uid: str,
    body: LocalMessageOpenedRequest,
) -> dict[str, Any]:
    return await _stack_post_json(
        f"/local/messages/{email_uid}/opened",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
    )


@router.get("/local/messages/{email_uid}/actions")
async def email_local_message_actions(
    email_uid: str,
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    return await _stack_get_json(
        f"/local/messages/{email_uid}/actions",
        params=_stack_params(mailbox_id=mailbox_id, limit=limit),
    )


@router.post("/local/messages/{email_uid}/virtual-path")
async def email_local_message_virtual_path(
    email_uid: str,
    body: LocalVirtualPathRequest,
) -> dict[str, Any]:
    return await _stack_post_json(
        f"/local/messages/{email_uid}/virtual-path",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
    )


@router.post("/local/messages/{email_uid}/virtual-paths/replace")
async def email_local_message_virtual_paths_replace(
    email_uid: str,
    body: LocalVirtualPathsReplaceRequest,
) -> dict[str, Any]:
    return await _stack_post_json(
        f"/local/messages/{email_uid}/virtual-paths/replace",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
    )


@router.get("/local/virtual-path-rules")
async def email_local_virtual_path_rules(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    include_archived: bool = Query(False),
) -> dict[str, Any]:
    return await _stack_get_json(
        "/local/virtual-path-rules",
        params=_stack_params(mailbox_id=mailbox_id, include_archived=include_archived),
    )


@router.post("/local/virtual-path-rules")
async def email_local_virtual_path_rule_create(
    body: VirtualPathRuleCreateRequest,
) -> dict[str, Any]:
    return await _stack_post_json(
        "/local/virtual-path-rules",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
    )


@router.patch("/local/virtual-path-rules/{rule_id}")
async def email_local_virtual_path_rule_update(
    rule_id: str,
    body: VirtualPathRuleUpdateRequest,
) -> dict[str, Any]:
    return await _stack_patch_json(
        f"/local/virtual-path-rules/{rule_id}",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
    )


@router.post("/local/virtual-path-rules/{rule_id}/archive")
async def email_local_virtual_path_rule_archive(
    rule_id: str,
    body: VirtualPathRuleUpdateRequest,
) -> dict[str, Any]:
    return await _stack_post_json(
        f"/local/virtual-path-rules/{rule_id}/archive",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
    )


@router.post("/local/virtual-path-rules/apply")
async def email_local_virtual_path_rules_apply(
    body: VirtualPathRuleApplyRequest,
) -> dict[str, Any]:
    return await _stack_post_json(
        "/local/virtual-path-rules/apply",
        params=_stack_params(mailbox_id=body.mailbox_id),
        json_body=body.model_dump(exclude_none=True),
        timeout_seconds=PIM_EMAIL_STACK_SEARCH_TIMEOUT_SECONDS,
        traffic_class="worker",
    )


@router.get("/local/virtual-path-rules/runs/{run_id}")
async def email_local_virtual_path_rule_run(
    run_id: str,
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    limit: int = Query(500, ge=1, le=2000),
) -> dict[str, Any]:
    return await _stack_get_json(
        f"/local/virtual-path-rules/runs/{run_id}",
        params=_stack_params(mailbox_id=mailbox_id, limit=limit),
    )


@router.get("/local/messages/{email_uid}/virtual-path-rules/history")
async def email_local_message_virtual_path_rule_history(
    email_uid: str,
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    limit: int = Query(100, ge=1, le=500),
) -> dict[str, Any]:
    return await _stack_get_json(
        f"/local/messages/{email_uid}/virtual-path-rules/history",
        params=_stack_params(mailbox_id=mailbox_id, limit=limit),
    )


@router.post("/local/messages/{email_uid}/force-refresh")
async def email_local_message_force_refresh(
    email_uid: str,
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    response = await _stack_post_json(
        f"/local/messages/{email_uid}/force-refresh",
        params=_stack_params(mailbox_id=mailbox_id),
        timeout_seconds=PIM_EMAIL_STACK_FORCE_REFRESH_TIMEOUT_SECONDS,
    )
    _schedule_pim_email_cache_state_watch(
        mailbox_id,
        [email_uid],
        reason="message-force-refresh-watch",
    )
    return response


@router.post("/local/messages/{email_uid}/refresh-local-view")
async def email_local_message_refresh_local_view(
    email_uid: str,
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    response = await _stack_post_json(
        f"/local/messages/{email_uid}/refresh-local-view",
        params=_stack_params(mailbox_id=mailbox_id),
        timeout_seconds=PIM_EMAIL_STACK_LOCAL_VIEW_REFRESH_TIMEOUT_SECONDS,
    )
    _schedule_pim_email_cache_state_watch(
        mailbox_id,
        [email_uid],
        reason="message-local-view-refresh-watch",
    )
    return response


@router.get("/local/images/{shared_asset_uid}")
async def email_local_image(
    shared_asset_uid: str,
    email_uid: str = Query(..., min_length=8, max_length=80),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> Response:
    return await _stack_get_binary(
        f"/local/images/{shared_asset_uid}",
        params=_stack_params(email_uid=email_uid, mailbox_id=mailbox_id),
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


@router.get("/local/trusted/probable-senders")
async def email_local_trusted_probable_senders(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    limit: int = Query(500, ge=1, le=2000),
) -> dict[str, Any]:
    return await _stack_get_json(
        "/local/trusted/probable-senders",
        params=_stack_params(mailbox_id=mailbox_id, limit=limit),
    )


@router.post("/local/trusted/probable-senders")
async def email_local_add_trusted_probable_sender(
    request: TrustedProbableSenderRequest,
) -> dict[str, Any]:
    payload = request.model_dump(exclude_none=True)
    mailbox_id = payload.pop("mailbox_id", None)
    return await _stack_post_json(
        "/local/trusted/probable-senders",
        params=_stack_params(mailbox_id=mailbox_id),
        json_body=payload,
    )


@router.delete("/local/trusted/probable-senders")
async def email_local_remove_trusted_probable_sender(
    sender_email: str = Query(..., min_length=3, max_length=254),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    return await _stack_delete_json(
        "/local/trusted/probable-senders",
        params=_stack_params(mailbox_id=mailbox_id, sender_email=sender_email),
    )


@router.get("/folders")
async def email_folders(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
) -> dict[str, Any]:
    return await _stack_get_json(
        "/folders",
        params=_stack_params(mailbox_id=mailbox_id),
    )


@router.get("/inbox")
async def email_inbox(
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    limit: int = Query(25, ge=1, le=100),
) -> dict[str, Any]:
    return await _stack_get_json(
        "/inbox",
        params=_stack_params(mailbox_id=mailbox_id, limit=limit),
    )


@router.get("/folder-messages")
async def email_folder_messages(
    folder: str = Query("INBOX", min_length=1, max_length=180),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    limit: int = Query(25, ge=1, le=100),
) -> dict[str, Any]:
    return await _stack_get_json(
        "/folder-messages",
        params=_stack_params(folder=folder, mailbox_id=mailbox_id, limit=limit),
    )


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
    return await _stack_get_json(
        f"/messages/{uid}",
        params=_stack_params(
            folder=folder,
            mailbox_id=mailbox_id,
            security_run_id=security_run_id,
        ),
    )


@router.get("/messages/{uid}/security")
async def email_message_security(
    uid: str,
    folder: str = Query("INBOX", min_length=1, max_length=180),
    mailbox_id: str | None = Query(None, min_length=1, max_length=120),
    security_run_id: str | None = Query(None, min_length=8, max_length=120),
) -> dict[str, Any]:
    return await _stack_get_json(
        f"/messages/{uid}/security",
        params=_stack_params(
            folder=folder,
            mailbox_id=mailbox_id,
            security_run_id=security_run_id,
        ),
    )


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
    return await _stack_post_json(
        "/smtp-self-test",
        json_body=body.model_dump(exclude_none=True),
    )
