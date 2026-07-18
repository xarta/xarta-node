"""Authenticated node-to-node bridge for scheduler plan fetch/invalidation.

The bridge never plans, signs, stores authority, selects a target, or grants
execution ownership.  It only resolves the configured prime through fleet
truth, forwards independently signed bytes, and delivers bounded generation
invalidation hints.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import ssl
from datetime import UTC, datetime
from typing import Any, Literal

import httpx
from fastapi import APIRouter, Header, HTTPException, status
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field

from . import config as cfg
from .auth import compute_token

log = logging.getLogger(__name__)
router = APIRouter(prefix="/sync/scheduler-coordination", tags=["scheduler-coordination"])

IDENTITY_SCHEMA = "xarta.blueprints.scheduler_bridge_identity.v1"
INVALIDATION_SCHEMA = "xarta.scheduler.plan_invalidation.v1"
STATUS_SCHEMA = "xarta.blueprints.scheduler_bridge_status.v1"
MAX_PLAN_BYTES = 1_048_576


class PlanInvalidation(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)
    schema_name: Literal[INVALIDATION_SCHEMA] = Field(default=INVALIDATION_SCHEMA, alias="schema")
    issuer_node_id: str = Field(min_length=3, max_length=80)
    prime_node_id: str = Field(min_length=3, max_length=80)
    generation: int = Field(ge=1, le=9_223_372_036_854_775_807)
    plan_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _build_sync_ssl_context() -> ssl.SSLContext | None:
    values = (cfg.SYNC_TLS_CA, cfg.SYNC_TLS_CERT, cfg.SYNC_TLS_KEY)
    if not any(values):
        return None
    if not all(values):
        raise RuntimeError("Scheduler bridge TLS CA/certificate/key configuration is incomplete")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(cfg.SYNC_TLS_CA)
    context.load_cert_chain(cfg.SYNC_TLS_CERT, cfg.SYNC_TLS_KEY)
    return context


def _identity_contract() -> dict[str, Any]:
    configured_owners = list(cfg.FLEET_LXC_NAMES)
    owners = sorted(set(configured_owners))
    if len(configured_owners) != len(owners):
        state = "conflict_fleet_duplicate"
    elif not 1 <= len(owners) <= 256:
        state = "conflict_fleet_unbounded"
    elif not cfg.SCHEDULER_PRIME_NODE_ID:
        state = "disabled_unconfigured"
    elif cfg.SCHEDULER_PRIME_NODE_ID not in owners:
        state = "conflict_prime_unknown"
    elif cfg.NODE_ID not in owners:
        state = "conflict_node_unknown"
    elif not cfg.SYNC_SECRET or not cfg.SCHEDULER_BRIDGE_SECRET:
        state = "disabled_missing_auth"
    elif cfg.SCHEDULER_LOCAL_URL != "http://127.0.0.1:18111":
        state = "conflict_scheduler_url"
    else:
        state = "configured"
    enabled = state == "configured"
    return {
        "schema": IDENTITY_SCHEMA,
        "node_id": cfg.NODE_ID,
        "prime_node_id": cfg.SCHEDULER_PRIME_NODE_ID,
        "role": (
            "prime"
            if enabled and cfg.NODE_ID == cfg.SCHEDULER_PRIME_NODE_ID
            else "peer"
            if enabled
            else "disabled"
        ),
        "state": state,
        "coordination_enabled": enabled,
        "allowed_owner_node_ids": owners,
        "plan_authority": "offsets_only",
        "execution_authority_granted": False,
    }


def _require_configured() -> dict[str, Any]:
    identity = _identity_contract()
    if not identity["coordination_enabled"]:
        raise HTTPException(
            status_code=503,
            detail={
                "code": identity["state"],
                "message": "Scheduler coordination bridge is fail-closed",
            },
        )
    return identity


class SchedulerCoordinationBridge:
    def __init__(self) -> None:
        self.peer_client: httpx.AsyncClient | None = None
        self.local_client: httpx.AsyncClient | None = None
        self.started_at = ""
        self.last_success_at = ""
        self.last_error = ""
        self.last_error_at = ""
        self.peer_request_count = 0
        self.local_request_count = 0
        self.notification_count = 0

    async def start(self) -> None:
        if self.local_client is not None:
            return
        limits = httpx.Limits(max_connections=8, max_keepalive_connections=4)
        timeout = httpx.Timeout(connect=3.0, read=10.0, write=10.0, pool=3.0)
        self.local_client = httpx.AsyncClient(
            base_url=cfg.SCHEDULER_LOCAL_URL,
            limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
            timeout=httpx.Timeout(connect=1.0, read=8.0, write=8.0, pool=1.0),
            follow_redirects=False,
        )
        try:
            context = await asyncio.to_thread(_build_sync_ssl_context)
            self.peer_client = httpx.AsyncClient(
                limits=limits,
                timeout=timeout,
                verify=context if context is not None else True,
                follow_redirects=False,
            )
        except Exception as exc:
            self._failure(exc)
            log.exception("scheduler coordination peer client is degraded")
        self.started_at = _utc_now()

    async def stop(self) -> None:
        peer, local = self.peer_client, self.local_client
        self.peer_client = None
        self.local_client = None
        if peer is not None:
            await peer.aclose()
        if local is not None:
            await local.aclose()

    def _success(self) -> None:
        self.last_success_at = _utc_now()
        self.last_error = ""
        self.last_error_at = ""

    def _failure(self, exc: Exception) -> None:
        self.last_error = f"{exc.__class__.__name__}: {str(exc)[:300]}"
        self.last_error_at = _utc_now()

    def _local(self) -> httpx.AsyncClient:
        if self.local_client is None:
            raise HTTPException(503, "Scheduler bridge local client is not running")
        return self.local_client

    def _peer(self) -> httpx.AsyncClient:
        if self.peer_client is None:
            raise HTTPException(503, "Scheduler bridge peer client is not running")
        return self.peer_client

    async def local_get_plan(self) -> bytes:
        try:
            response = await self._local().get("/coordination/prime/plan")
            self.local_request_count += 1
            response.raise_for_status()
            raw = response.content
            if not raw or len(raw) > MAX_PLAN_BYTES:
                raise RuntimeError("Local signed plan is empty or unbounded")
            self._success()
            return raw
        except Exception as exc:
            self._failure(exc)
            raise

    async def peer_get_plan(self, prime_node_id: str) -> bytes:
        urls = list(cfg.PEER_SYNC_URLS.get(prime_node_id, []))
        if not urls:
            raise HTTPException(503, "Configured prime has no routed sync address")
        errors: list[str] = []
        for base_url in urls:
            try:
                response = await self._peer().get(
                    f"{base_url.rstrip('/')}/api/v1/sync/scheduler-coordination/plan-source",
                    headers={"x-api-token": compute_token(cfg.SYNC_SECRET)},
                )
                self.peer_request_count += 1
                response.raise_for_status()
                raw = response.content
                if not raw or len(raw) > MAX_PLAN_BYTES:
                    raise RuntimeError("Remote signed plan is empty or unbounded")
                self._success()
                return raw
            except Exception as exc:
                errors.append(
                    f"{exc.__class__.__name__}:{getattr(exc, 'response', None) and exc.response.status_code}"
                )
                self._failure(exc)
        raise HTTPException(503, {"code": "prime_fetch_failed", "attempts": errors[:3]})

    async def local_invalidate(self, value: dict[str, Any]) -> dict[str, Any]:
        payload = {
            **value,
            "actor": f"blueprints-bridge:{value['issuer_node_id']}",
            "source_surface": "blueprints_scheduler_invalidation",
            "request_id": (
                f"scheduler-invalidation:{value['issuer_node_id']}:"
                f"{value['generation']}:{value['plan_digest'][7:23]}"
            ),
        }
        try:
            response = await self._local().post("/coordination/invalidation", json=payload)
            self.local_request_count += 1
            response.raise_for_status()
            self._success()
            return response.json()
        except Exception as exc:
            self._failure(exc)
            raise

    async def notify_peer(self, node_id: str, value: dict[str, Any]) -> dict[str, Any]:
        errors: list[str] = []
        urls = list(cfg.PEER_SYNC_URLS.get(node_id, []))
        if not urls:
            return {
                "node_id": node_id,
                "delivered": False,
                "errors": ["configured_peer_has_no_routed_sync_address"],
            }
        for base_url in urls:
            try:
                response = await self._peer().post(
                    f"{base_url.rstrip('/')}/api/v1/sync/scheduler-coordination/invalidate",
                    json=value,
                    headers={"x-api-token": compute_token(cfg.SYNC_SECRET)},
                )
                self.peer_request_count += 1
                response.raise_for_status()
                self.notification_count += 1
                self._success()
                return {"node_id": node_id, "delivered": True, "status_code": response.status_code}
            except Exception as exc:
                errors.append(
                    f"{exc.__class__.__name__}:{getattr(exc, 'response', None) and exc.response.status_code}"
                )
                self._failure(exc)
        return {"node_id": node_id, "delivered": False, "errors": errors[:3]}

    async def fanout(self, value: dict[str, Any]) -> dict[str, Any]:
        peers = sorted(node_id for node_id in set(cfg.FLEET_LXC_NAMES) if node_id != cfg.NODE_ID)
        results = await asyncio.gather(
            *(self.notify_peer(node_id, value) for node_id in peers),
            return_exceptions=True,
        )
        normalized = []
        for node_id, result in zip(peers, results, strict=True):
            if isinstance(result, Exception):
                normalized.append(
                    {"node_id": node_id, "delivered": False, "error": result.__class__.__name__}
                )
            else:
                normalized.append(result)
        delivered = sum(1 for result in normalized if result.get("delivered"))
        return {
            "schema": "xarta.blueprints.scheduler_invalidation_fanout.v1",
            "generation": value["generation"],
            "plan_digest": value["plan_digest"],
            "peer_count": len(peers),
            "delivered_count": delivered,
            "all_delivered": delivered == len(peers),
            "results": normalized,
            "execution_authority_granted": False,
        }

    def status(self) -> dict[str, Any]:
        return {
            "schema": STATUS_SCHEMA,
            "identity": _identity_contract(),
            "client": {
                "started_at": self.started_at,
                "peer_client_running": self.peer_client is not None,
                "local_client_running": self.local_client is not None,
                "max_peer_connections": 8,
                "max_local_connections": 4,
                "last_success_at": self.last_success_at,
                "last_error": self.last_error,
                "last_error_at": self.last_error_at,
                "peer_request_count": self.peer_request_count,
                "local_request_count": self.local_request_count,
                "notification_count": self.notification_count,
            },
            "ordinary_scheduling_enabled": True,
            "execution_authority_granted": False,
            "checked_at": _utc_now(),
        }


bridge = SchedulerCoordinationBridge()


async def start_scheduler_coordination_bridge() -> None:
    await bridge.start()


async def stop_scheduler_coordination_bridge() -> None:
    await bridge.stop()


@router.get("/identity")
async def scheduler_coordination_identity() -> dict[str, Any]:
    # The route itself is useful for visible conflict diagnosis, but remains
    # fail-closed when either node authentication secret is absent.
    identity = _identity_contract()
    if not cfg.SYNC_SECRET or not cfg.SCHEDULER_BRIDGE_SECRET:
        raise HTTPException(503, {"code": identity["state"]})
    return identity


@router.get("/status")
async def scheduler_coordination_status() -> dict[str, Any]:
    return bridge.status()


@router.get("/plan-source")
async def scheduler_coordination_plan_source() -> Response:
    identity = _require_configured()
    if identity["role"] != "prime":
        raise HTTPException(409, {"code": "not_configured_prime"})
    try:
        raw = await bridge.local_get_plan()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(exc.response.status_code, "Prime scheduler plan unavailable") from None
    return Response(raw, media_type="application/json", headers={"Cache-Control": "no-store"})


@router.get("/plan")
async def scheduler_coordination_plan() -> Response:
    identity = _require_configured()
    raw = (
        await bridge.local_get_plan()
        if identity["role"] == "prime"
        else await bridge.peer_get_plan(identity["prime_node_id"])
    )
    return Response(raw, media_type="application/json", headers={"Cache-Control": "no-store"})


@router.post("/notify")
async def scheduler_coordination_notify(
    payload: PlanInvalidation,
    x_blueprints_scheduler_token: str | None = Header(
        default=None,
        alias="X-Blueprints-Scheduler-Token",
    ),
) -> dict[str, Any]:
    identity = _require_configured()
    expected_token = cfg.SCHEDULER_BRIDGE_SECRET.strip()
    if not x_blueprints_scheduler_token or not secrets.compare_digest(
        x_blueprints_scheduler_token,
        expected_token,
    ):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "Scheduler notification requires dedicated bridge authentication",
        )
    value = payload.model_dump(mode="json", by_alias=True)
    if identity["role"] != "prime" or value["issuer_node_id"] != cfg.NODE_ID:
        raise HTTPException(409, {"code": "not_configured_prime"})
    if value["prime_node_id"] != cfg.SCHEDULER_PRIME_NODE_ID:
        raise HTTPException(409, {"code": "wrong_prime"})
    return await bridge.fanout(value)


@router.post("/invalidate", status_code=202)
async def scheduler_coordination_invalidate(payload: PlanInvalidation) -> dict[str, Any]:
    identity = _require_configured()
    value = payload.model_dump(mode="json", by_alias=True)
    if identity["role"] != "peer":
        raise HTTPException(409, {"code": "prime_does_not_refresh"})
    if (
        value["issuer_node_id"] != cfg.SCHEDULER_PRIME_NODE_ID
        or value["prime_node_id"] != cfg.SCHEDULER_PRIME_NODE_ID
    ):
        raise HTTPException(409, {"code": "wrong_prime"})
    try:
        return await bridge.local_invalidate(value)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            exc.response.status_code, "Local scheduler rejected invalidation"
        ) from None
