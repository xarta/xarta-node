"""
models.py — Pydantic request/response models for the Blueprints API.

JSON keys use snake_case throughout (per coding-style conventions).
Optional fields default to None so partial updates are expressible.
"""

from __future__ import annotations

from typing import Any, Optional
from pydantic import BaseModel, Field


# ── Machine ───────────────────────────────────────────────────────────────────

class MachineCreate(BaseModel):
    machine_id: str
    name: str
    type: str  # baremetal | vm | lxc | vps
    parent_machine_id: Optional[str] = None
    ip_addresses: Optional[list[str]] = None
    description: Optional[str] = None


class MachineUpdate(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    parent_machine_id: Optional[str] = None
    ip_addresses: Optional[list[str]] = None
    description: Optional[str] = None


class MachineOut(BaseModel):
    machine_id: str
    name: str
    type: str
    parent_machine_id: Optional[str] = None
    ip_addresses: Optional[list[str]] = None
    description: Optional[str] = None
    created_at: str
    updated_at: str


# ── Service ───────────────────────────────────────────────────────────────────

class ServiceCreate(BaseModel):
    service_id: str
    name: str
    description: Optional[str] = None
    host_machine: Optional[str] = None
    vm_or_lxc: Optional[str] = None
    ports: Optional[list[str]] = None
    caddy_routes: Optional[list[str]] = None
    dns_info: Optional[str] = None
    credential_hints: Optional[str] = None
    dependencies: Optional[list[str]] = None
    project_status: str = "deployed"
    tags: Optional[list[str]] = None
    links: Optional[list[dict[str, str]]] = None


class ServiceUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    host_machine: Optional[str] = None
    vm_or_lxc: Optional[str] = None
    ports: Optional[list[str]] = None
    caddy_routes: Optional[list[str]] = None
    dns_info: Optional[str] = None
    credential_hints: Optional[str] = None
    dependencies: Optional[list[str]] = None
    project_status: Optional[str] = None
    tags: Optional[list[str]] = None
    links: Optional[list[dict[str, str]]] = None


class ServiceOut(BaseModel):
    service_id: str
    name: str
    description: Optional[str] = None
    host_machine: Optional[str] = None
    vm_or_lxc: Optional[str] = None
    ports: Optional[list[str]] = None
    caddy_routes: Optional[list[str]] = None
    dns_info: Optional[str] = None
    credential_hints: Optional[str] = None
    dependencies: Optional[list[str]] = None
    project_status: str
    tags: Optional[list[str]] = None
    links: Optional[list[dict[str, str]]] = None
    created_at: str
    updated_at: str


# ── Node ──────────────────────────────────────────────────────────────────────

class NodeCreate(BaseModel):
    node_id: str
    display_name: str
    host_machine: Optional[str] = None
    tailnet: Optional[str] = None
    addresses: Optional[list[str]] = None
    ui_url: Optional[str] = None   # browser-facing HTTPS URL for this node


class NodeOut(BaseModel):
    node_id: str
    display_name: str
    host_machine: Optional[str] = None
    tailnet: Optional[str] = None
    addresses: Optional[list[str]] = None
    ui_url: Optional[str] = None   # browser-facing HTTPS URL for this node
    last_seen: Optional[str] = None
    created_at: str


# ── Sync ──────────────────────────────────────────────────────────────────────

class SyncAction(BaseModel):
    action_type: str    # INSERT | UPDATE | DELETE | sync_git_outer | sync_git_inner
    table_name: Optional[str] = ""   # not used for system action types
    row_id: Optional[str] = ""       # not used for system action types
    row_data: Optional[dict[str, Any]] = None
    gen: int
    source_node_id: str


class GitPullRequest(BaseModel):
    scope: str = "outer"   # "outer" | "inner" | "both"


class SyncActionsPayload(BaseModel):
    actions: list[SyncAction]
    source_node_id: str


class SyncStatus(BaseModel):
    node_id: str
    node_name: str
    gen: int
    integrity_ok: bool
    last_write_at: str
    last_write_by: str
    queue_depths: dict[str, int]      # {peer_node_id: pending_count}
    peer_count: int


# ── Health ────────────────────────────────────────────────────────────────────

class HealthOut(BaseModel):
    status: str
    node_id: str
    node_name: str
    gen: int
    integrity_ok: bool
    ui_url: Optional[str] = None   # browser-accessible URL; set via BLUEPRINTS_UI_URL
