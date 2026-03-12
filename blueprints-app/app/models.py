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
    machine_kind: Optional[str] = None      # baremetal/proxmox/vm/lxc/docker/switch/router/firewall/pikvm
    platform: Optional[str] = None           # proxmox/docker/kvm/pfsense/caddy/linux
    status: str = "active"                  # active/maintenance/offline/decommissioned
    labels: Optional[list[str]] = None       # freeform tags
    properties_json: Optional[dict] = None  # extensible metadata bag


class MachineUpdate(BaseModel):
    name: Optional[str] = None
    type: Optional[str] = None
    parent_machine_id: Optional[str] = None
    ip_addresses: Optional[list[str]] = None
    description: Optional[str] = None
    machine_kind: Optional[str] = None
    platform: Optional[str] = None
    status: Optional[str] = None
    labels: Optional[list[str]] = None
    properties_json: Optional[dict] = None


class MachineOut(BaseModel):
    machine_id: str
    name: str
    type: str
    parent_machine_id: Optional[str] = None
    ip_addresses: Optional[list[str]] = None
    description: Optional[str] = None
    machine_kind: Optional[str] = None
    platform: Optional[str] = None
    status: Optional[str] = None
    labels: Optional[list[str]] = None
    properties_json: Optional[dict] = None
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
    host_machine_id: Optional[str] = None       # FK to machines.machine_id
    service_kind: str = "app"                   # app/api/web/db/proxy/dns/infra-ui
    exposure_level: str = "internal"             # internal/tailnet/lan/public/restricted
    health_path: Optional[str] = None            # e.g. /health or /api/v1/health
    health_expected_status: int = 200
    runtime_notes_json: Optional[dict] = None    # extensible metadata bag


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
    host_machine_id: Optional[str] = None
    service_kind: Optional[str] = None
    exposure_level: Optional[str] = None
    health_path: Optional[str] = None
    health_expected_status: Optional[int] = None
    runtime_notes_json: Optional[dict] = None


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
    host_machine_id: Optional[str] = None
    service_kind: Optional[str] = None
    exposure_level: Optional[str] = None
    health_path: Optional[str] = None
    health_expected_status: Optional[int] = None
    runtime_notes_json: Optional[dict] = None
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
    machine_id: Optional[str] = None  # canonical FK to machines.machine_id


class NodeOut(BaseModel):
    node_id: str
    display_name: str
    host_machine: Optional[str] = None
    tailnet: Optional[str] = None
    addresses: Optional[list[str]] = None
    ui_url: Optional[str] = None   # browser-facing HTTPS URL for this node
    machine_id: Optional[str] = None  # canonical FK to machines.machine_id
    last_seen: Optional[str] = None
    created_at: str


# ── pfSense DNS ───────────────────────────────────────────────────────────────

class PfSenseDnsCreate(BaseModel):
    dns_entry_id: str               # composite key: "{ip}:{fqdn}"
    ip_address: str
    fqdn: str
    record_type: Optional[str] = None   # host_override | host_override_alias | dhcp_lease | domain_override
    source: Optional[str] = None        # config file name or source identifier
    mac_address: Optional[str] = None
    active: int = 1
    last_seen: Optional[str] = None
    last_probed: Optional[str] = None


class PfSenseDnsUpdate(BaseModel):
    ip_address: Optional[str] = None
    fqdn: Optional[str] = None
    record_type: Optional[str] = None
    source: Optional[str] = None
    mac_address: Optional[str] = None
    active: Optional[int] = None
    last_seen: Optional[str] = None
    last_probed: Optional[str] = None
    ping_ms: Optional[float] = None
    last_ping_check: Optional[str] = None


class PfSenseDnsOut(BaseModel):
    dns_entry_id: str
    ip_address: str
    fqdn: str
    record_type: Optional[str] = None
    source: Optional[str] = None
    mac_address: Optional[str] = None
    active: Optional[int] = 1
    last_seen: Optional[str] = None
    last_probed: Optional[str] = None
    ping_ms: Optional[float] = None
    last_ping_check: Optional[str] = None
    created_at: str
    updated_at: str


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
