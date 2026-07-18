from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

NODES_JSON = Path(tempfile.gettempdir()) / "blueprints-test-scheduler-coordination.json"
NODES_JSON.write_text(
    json.dumps(
        {
            "nodes": [
                {
                    "node_id": "test-node",
                    "display_name": "Test Node",
                    "host_machine": "test-host",
                    "primary_hostname": "test-node.local",
                    "tailnet_hostname": "test-node.tailnet",
                    "primary_ip": "127.0.0.1",
                    "sync_port": 8080,
                    "tailnet": "test",
                    "tailnet_ip": "100.64.0.1",
                    "active": True,
                }
            ]
        }
    ),
    encoding="utf-8",
)
os.environ.setdefault("BLUEPRINTS_NODE_ID", "test-node")
os.environ.setdefault("NODES_JSON_PATH", str(NODES_JSON))
os.environ.setdefault("BLUEPRINTS_DB_DIR", tempfile.mkdtemp(prefix="blueprints-test-scheduler-db-"))
os.environ.setdefault("SEEKDB_HOST", "127.0.0.1")
os.environ.setdefault("SEEKDB_PORT", "5432")
os.environ.setdefault("SEEKDB_DB", "blueprints_test")
os.environ.setdefault("SEEKDB_USER", "blueprints_test")
os.environ.setdefault("SEEKDB_PASSWORD", "blueprints_test")

from app import config as cfg  # noqa: E402
from app import middleware_auth  # noqa: E402
from app.auth import compute_token  # noqa: E402
from app.middleware_auth import AuthMiddleware  # noqa: E402
from app.routes_scheduler_coordination import (  # noqa: E402
    IDENTITY_SCHEMA,
    INVALIDATION_SCHEMA,
    PlanInvalidation,
    SchedulerCoordinationBridge,
    _identity_contract,
    bridge,
    router,
)


def test_scheduler_bridge_identity_is_typed_and_conflicts_fail_closed(monkeypatch):
    monkeypatch.setattr(cfg, "NODE_ID", "fleet-node-b")
    monkeypatch.setattr(
        cfg,
        "FLEET_LXC_NAMES",
        ["fleet-node-a", "fleet-node-b", "fleet-node-c"],
    )
    monkeypatch.setattr(cfg, "SYNC_SECRET", "a" * 64)
    monkeypatch.setattr(cfg, "SCHEDULER_BRIDGE_SECRET", "b" * 64)
    monkeypatch.setattr(cfg, "SCHEDULER_LOCAL_URL", "http://127.0.0.1:18111")
    monkeypatch.setattr(cfg, "SCHEDULER_PRIME_NODE_ID", "fleet-node-a")
    identity = _identity_contract()
    assert identity == {
        "schema": IDENTITY_SCHEMA,
        "node_id": "fleet-node-b",
        "prime_node_id": "fleet-node-a",
        "role": "peer",
        "state": "configured",
        "coordination_enabled": True,
        "allowed_owner_node_ids": ["fleet-node-a", "fleet-node-b", "fleet-node-c"],
        "plan_authority": "offsets_only",
        "execution_authority_granted": False,
    }

    monkeypatch.setattr(cfg, "SCHEDULER_PRIME_NODE_ID", "invented-node")
    conflict = _identity_contract()
    assert conflict["state"] == "conflict_prime_unknown"
    assert conflict["coordination_enabled"] is False

    monkeypatch.setattr(cfg, "SCHEDULER_PRIME_NODE_ID", "fleet-node-a")
    monkeypatch.setattr(cfg, "SYNC_SECRET", "")
    missing_auth = _identity_contract()
    assert missing_auth["state"] == "disabled_missing_auth"
    assert missing_auth["execution_authority_granted"] is False

    monkeypatch.setattr(cfg, "SYNC_SECRET", "a" * 64)
    monkeypatch.setattr(
        cfg,
        "FLEET_LXC_NAMES",
        ["fleet-node-a", "fleet-node-a", "fleet-node-b"],
    )
    duplicate = _identity_contract()
    assert duplicate["state"] == "conflict_fleet_duplicate"
    assert duplicate["coordination_enabled"] is False


def test_scheduler_coordination_middleware_requires_route_scoped_auth(monkeypatch):
    monkeypatch.setattr(middleware_auth, "_allowed_networks", [])
    monkeypatch.setattr(cfg, "SCHEDULER_BRIDGE_SECRET", "b" * 64)
    monkeypatch.setattr(cfg, "SYNC_SECRET", "a" * 64)
    application = FastAPI()
    application.add_middleware(AuthMiddleware)

    @application.get("/api/v1/sync/scheduler-coordination/proof")
    async def proof():
        return {"ok": True}

    with TestClient(application) as client:
        assert client.get("/api/v1/sync/scheduler-coordination/proof").status_code == 401
        assert (
            client.get(
                "/api/v1/sync/scheduler-coordination/proof",
                headers={"x-blueprints-scheduler-token": "b" * 64},
            ).status_code
            == 200
        )
        assert (
            client.get(
                "/api/v1/sync/scheduler-coordination/proof",
                headers={"x-api-token": compute_token("a" * 64)},
            ).status_code
            == 200
        )

        monkeypatch.setattr(cfg, "SYNC_SECRET", "")
        assert (
            client.get(
                "/api/v1/sync/scheduler-coordination/proof",
                headers={"x-blueprints-scheduler-token": "b" * 64},
            ).status_code
            == 503
        )


@pytest.mark.asyncio
async def test_peer_plan_fetch_uses_only_configured_prime_url_and_bounded_client(monkeypatch):
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"schema": "signed-proof"})

    monkeypatch.setattr(cfg, "SYNC_SECRET", "a" * 64)
    monkeypatch.setattr(
        cfg,
        "PEER_SYNC_URLS",
        {"fleet-node-a": ["https://192.0.2.10:8443"]},
    )
    bridge = SchedulerCoordinationBridge()
    bridge.peer_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        raw = await bridge.peer_get_plan("fleet-node-a")
    finally:
        await bridge.peer_client.aclose()
        bridge.peer_client = None
    assert b"signed-proof" in raw
    assert len(seen) == 1
    assert str(seen[0].url) == (
        "https://192.0.2.10:8443/api/v1/sync/scheduler-coordination/plan-source"
    )
    assert seen[0].headers["x-api-token"]


def test_invalidation_contract_is_strict_and_carries_no_execution_payload():
    value = PlanInvalidation.model_validate(
        {
            "schema": INVALIDATION_SCHEMA,
            "issuer_node_id": "fleet-node-a",
            "prime_node_id": "fleet-node-a",
            "generation": 7,
            "plan_digest": "sha256:" + "7" * 64,
        }
    ).model_dump(mode="json", by_alias=True)
    assert set(value) == {
        "schema",
        "issuer_node_id",
        "prime_node_id",
        "generation",
        "plan_digest",
    }
    with pytest.raises(Exception):
        PlanInvalidation.model_validate({**value, "target_url": "https://attacker.invalid"})


def test_prime_notification_requires_dedicated_local_scheduler_auth(monkeypatch):
    monkeypatch.setattr(cfg, "NODE_ID", "fleet-node-a")
    monkeypatch.setattr(
        cfg,
        "FLEET_LXC_NAMES",
        ["fleet-node-a", "fleet-node-b", "fleet-node-c"],
    )
    monkeypatch.setattr(cfg, "SYNC_SECRET", "a" * 64)
    monkeypatch.setattr(cfg, "SCHEDULER_BRIDGE_SECRET", "b" * 64)
    monkeypatch.setattr(cfg, "SCHEDULER_PRIME_NODE_ID", "fleet-node-a")

    async def fanout(payload):
        return {"state": "notified", "generation": payload["generation"]}

    monkeypatch.setattr(bridge, "fanout", fanout)
    application = FastAPI()
    application.include_router(router, prefix="/api/v1")
    payload = {
        "schema": INVALIDATION_SCHEMA,
        "issuer_node_id": "fleet-node-a",
        "prime_node_id": "fleet-node-a",
        "generation": 7,
        "plan_digest": "sha256:" + "7" * 64,
    }
    with TestClient(application) as client:
        assert (
            client.post(
                "/api/v1/sync/scheduler-coordination/notify",
                json=payload,
                headers={"x-api-token": compute_token("a" * 64)},
            ).status_code
            == 401
        )
        response = client.post(
            "/api/v1/sync/scheduler-coordination/notify",
            json=payload,
            headers={"x-blueprints-scheduler-token": "b" * 64},
        )
        assert response.status_code == 200
        assert response.json() == {"state": "notified", "generation": 7}


@pytest.mark.asyncio
async def test_fanout_counts_every_configured_peer_and_missing_route_is_degraded(monkeypatch):
    monkeypatch.setattr(cfg, "NODE_ID", "fleet-node-a")
    monkeypatch.setattr(
        cfg,
        "FLEET_LXC_NAMES",
        ["fleet-node-a", "fleet-node-b", "fleet-node-c"],
    )
    monkeypatch.setattr(
        cfg,
        "PEER_SYNC_URLS",
        {"fleet-node-b": ["https://192.0.2.20:8443"]},
    )

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(202, json={"queued": True})

    local_bridge = SchedulerCoordinationBridge()
    local_bridge.peer_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        result = await local_bridge.fanout(
            {
                "schema": INVALIDATION_SCHEMA,
                "issuer_node_id": "fleet-node-a",
                "prime_node_id": "fleet-node-a",
                "generation": 8,
                "plan_digest": "sha256:" + "8" * 64,
            }
        )
    finally:
        await local_bridge.peer_client.aclose()
        local_bridge.peer_client = None
    assert result["peer_count"] == 2
    assert result["delivered_count"] == 1
    assert result["all_delivered"] is False
    missing = next(item for item in result["results"] if item["node_id"] == "fleet-node-c")
    assert missing["errors"] == ["configured_peer_has_no_routed_sync_address"]
