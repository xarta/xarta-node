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


# ── Proxmox Config ────────────────────────────────────────────────────────────

class ProxmoxConfigCreate(BaseModel):
    config_id: str                          # "{pve_name}_{vmid}"
    pve_host: str
    pve_name: Optional[str] = None
    vmid: int
    vm_type: str                            # 'lxc' | 'qemu'
    name: Optional[str] = None
    status: Optional[str] = None
    cores: Optional[int] = None
    memory_mb: Optional[int] = None
    rootfs: Optional[str] = None
    ip_config: Optional[str] = None
    ip_address: Optional[str] = None
    gateway: Optional[str] = None
    mac_address: Optional[str] = None
    vlan_tag: Optional[int] = None
    tags: Optional[str] = None
    mountpoints_json: Optional[str] = None
    raw_conf: Optional[str] = None
    vlans_json: Optional[str] = None
    has_docker: Optional[int] = None
    dockge_stacks_dir: Optional[str] = None
    has_portainer: Optional[int] = None
    portainer_method: Optional[str] = None
    has_caddy: Optional[int] = None
    caddy_conf_path: Optional[str] = None
    dockge_json: Optional[str] = None
    portainer_json: Optional[str] = None
    caddy_json: Optional[str] = None
    last_probed: Optional[str] = None


class ProxmoxConfigUpdate(BaseModel):
    pve_host: Optional[str] = None
    pve_name: Optional[str] = None
    vmid: Optional[int] = None
    vm_type: Optional[str] = None
    name: Optional[str] = None
    status: Optional[str] = None
    cores: Optional[int] = None
    memory_mb: Optional[int] = None
    rootfs: Optional[str] = None
    ip_config: Optional[str] = None
    ip_address: Optional[str] = None
    gateway: Optional[str] = None
    mac_address: Optional[str] = None
    vlan_tag: Optional[int] = None
    tags: Optional[str] = None
    mountpoints_json: Optional[str] = None
    raw_conf: Optional[str] = None
    vlans_json: Optional[str] = None
    has_docker: Optional[int] = None
    dockge_stacks_dir: Optional[str] = None
    has_portainer: Optional[int] = None
    portainer_method: Optional[str] = None
    has_caddy: Optional[int] = None
    caddy_conf_path: Optional[str] = None
    dockge_json: Optional[str] = None
    portainer_json: Optional[str] = None
    caddy_json: Optional[str] = None
    last_probed: Optional[str] = None


class ProxmoxConfigOut(BaseModel):
    config_id: str
    pve_host: str
    pve_name: Optional[str] = None
    vmid: int
    vm_type: str
    name: Optional[str] = None
    status: Optional[str] = None
    cores: Optional[int] = None
    memory_mb: Optional[int] = None
    rootfs: Optional[str] = None
    ip_config: Optional[str] = None
    ip_address: Optional[str] = None
    gateway: Optional[str] = None
    mac_address: Optional[str] = None
    vlan_tag: Optional[int] = None
    tags: Optional[str] = None
    mountpoints_json: Optional[str] = None
    raw_conf: Optional[str] = None
    vlans_json: Optional[str] = None
    has_docker: Optional[int] = None
    dockge_stacks_dir: Optional[str] = None
    has_portainer: Optional[int] = None
    portainer_method: Optional[str] = None
    has_caddy: Optional[int] = None
    caddy_conf_path: Optional[str] = None
    dockge_json: Optional[str] = None
    portainer_json: Optional[str] = None
    caddy_json: Optional[str] = None
    last_probed: Optional[str] = None
    created_at: str
    updated_at: str


# ── Proxmox Nets ──────────────────────────────────────────────────────────────

class ProxmoxNetCreate(BaseModel):
    net_id: str                             # "{config_id}_net{N}"
    config_id: str
    pve_host: str
    vmid: int
    net_key: str                            # "net0", "net1" …
    mac_address: Optional[str] = None
    ip_address: Optional[str] = None
    ip_cidr: Optional[str] = None
    gateway: Optional[str] = None
    vlan_tag: Optional[int] = None
    bridge: Optional[str] = None
    model: Optional[str] = None
    raw_str: Optional[str] = None
    ip_source: Optional[str] = "conf"


class ProxmoxNetUpdate(BaseModel):
    ip_address: Optional[str] = None
    ip_cidr: Optional[str] = None
    gateway: Optional[str] = None
    vlan_tag: Optional[int] = None
    bridge: Optional[str] = None
    model: Optional[str] = None
    raw_str: Optional[str] = None
    ip_source: Optional[str] = None


class ProxmoxNetOut(BaseModel):
    net_id: str
    config_id: str
    pve_host: str
    vmid: int
    net_key: str
    mac_address: Optional[str] = None
    ip_address: Optional[str] = None
    ip_cidr: Optional[str] = None
    gateway: Optional[str] = None
    vlan_tag: Optional[int] = None
    bridge: Optional[str] = None
    model: Optional[str] = None
    raw_str: Optional[str] = None
    ip_source: Optional[str] = None
    created_at: str
    updated_at: str


# ── VLANs ─────────────────────────────────────────────────────────────────────

class VlanUpdate(BaseModel):
    cidr: Optional[str] = None
    description: Optional[str] = None


class VlanOut(BaseModel):
    vlan_id: int
    cidr: Optional[str] = None
    cidr_inferred: int = 1
    description: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# ── Dockge Stacks ─────────────────────────────────────────────────────────────

class DockgeStackCreate(BaseModel):
    stack_id: str                           # "{source_vmid}_{stacks_dir_slug}_{stack_name}"
    pve_host: str
    source_vmid: int
    source_lxc_name: Optional[str] = None
    stack_name: str
    status: Optional[str] = None
    compose_content: Optional[str] = None
    services_json: Optional[str] = None    # legacy: JSON array of service names
    ports_json: Optional[str] = None       # legacy: JSON array of port strings
    volumes_json: Optional[str] = None
    env_file_exists: int = 0
    stacks_dir: Optional[str] = None
    vm_type: Optional[str] = None          # lxc | qemu
    ip_address: Optional[str] = None       # IP we SSH'd into for this probe
    parent_context: Optional[str] = None   # dockge-stack | docker-compose | docker-run | portainer-stack | native
    parent_stack_name: Optional[str] = None  # if parent_context==dockge-stack
    obsolete: int = 0                        # 1 = user-marked obsolete; probe never overwrites
    notes: Optional[str] = None             # free-text user notes
    last_probed: Optional[str] = None


class DockgeStackUpdate(BaseModel):
    pve_host: Optional[str] = None
    source_vmid: Optional[int] = None
    source_lxc_name: Optional[str] = None
    stack_name: Optional[str] = None
    status: Optional[str] = None
    compose_content: Optional[str] = None
    services_json: Optional[str] = None
    ports_json: Optional[str] = None
    volumes_json: Optional[str] = None
    env_file_exists: Optional[int] = None
    stacks_dir: Optional[str] = None
    vm_type: Optional[str] = None
    ip_address: Optional[str] = None
    parent_context: Optional[str] = None
    parent_stack_name: Optional[str] = None
    obsolete: Optional[int] = None          # None = don't update; 0/1 to set
    notes: Optional[str] = None
    last_probed: Optional[str] = None


class DockgeStackOut(BaseModel):
    stack_id: str
    pve_host: str
    source_vmid: int
    source_lxc_name: Optional[str] = None
    stack_name: str
    status: Optional[str] = None
    compose_content: Optional[str] = None
    services_json: Optional[str] = None
    ports_json: Optional[str] = None
    volumes_json: Optional[str] = None
    env_file_exists: int = 0
    stacks_dir: Optional[str] = None
    vm_type: Optional[str] = None
    ip_address: Optional[str] = None
    parent_context: Optional[str] = None
    parent_stack_name: Optional[str] = None
    obsolete: int = 0
    notes: Optional[str] = None
    last_probed: Optional[str] = None
    created_at: str
    updated_at: str


# ── Dockge Stack Services ──────────────────────────────────────────────────────

class DockgeStackServiceCreate(BaseModel):
    service_id: str                         # "{stack_id}_{service_name}"
    stack_id: str
    service_name: str
    image: Optional[str] = None
    ports_json: Optional[str] = None        # JSON array of "host:container/proto"
    volumes_json: Optional[str] = None
    container_state: Optional[str] = None   # running|stopped|restarting|etc
    container_id: Optional[str] = None
    last_probed: Optional[str] = None


class DockgeStackServiceUpdate(BaseModel):
    image: Optional[str] = None
    ports_json: Optional[str] = None
    volumes_json: Optional[str] = None
    container_state: Optional[str] = None
    container_id: Optional[str] = None
    last_probed: Optional[str] = None


class DockgeStackServiceOut(BaseModel):
    service_id: str
    stack_id: str
    service_name: str
    image: Optional[str] = None
    ports_json: Optional[str] = None
    volumes_json: Optional[str] = None
    container_state: Optional[str] = None
    container_id: Optional[str] = None
    last_probed: Optional[str] = None
    created_at: str
    updated_at: str


# ── Caddy Configs ─────────────────────────────────────────────────────────────

class CaddyConfigCreate(BaseModel):
    caddy_id: str                           # "{source_vmid}_{path_slug}"
    pve_host: Optional[str] = None
    source_vmid: Optional[int] = None
    source_lxc_name: Optional[str] = None
    caddyfile_path: Optional[str] = None
    caddyfile_content: Optional[str] = None
    domains_json: Optional[str] = None
    upstreams_json: Optional[str] = None
    last_probed: Optional[str] = None


class CaddyConfigUpdate(BaseModel):
    pve_host: Optional[str] = None
    source_vmid: Optional[int] = None
    source_lxc_name: Optional[str] = None
    caddyfile_path: Optional[str] = None
    caddyfile_content: Optional[str] = None
    domains_json: Optional[str] = None
    upstreams_json: Optional[str] = None
    last_probed: Optional[str] = None


class CaddyConfigOut(BaseModel):
    caddy_id: str
    pve_host: Optional[str] = None
    source_vmid: Optional[int] = None
    source_lxc_name: Optional[str] = None
    caddyfile_path: Optional[str] = None
    caddyfile_content: Optional[str] = None
    domains_json: Optional[str] = None
    upstreams_json: Optional[str] = None
    last_probed: Optional[str] = None
    created_at: str
    updated_at: str


# ── Settings ─────────────────────────────────────────────────────────────────

class SettingUpsert(BaseModel):
    value: str
    description: Optional[str] = None

class SettingOut(BaseModel):
    key: str
    value: str
    description: Optional[str] = None
    updated_at: str


# ── PVE Hosts ─────────────────────────────────────────────────────────────────

class PveHostCreate(BaseModel):
    pve_id: str          # IP address used as primary key
    ip_address: str
    hostname: Optional[str] = None
    pve_name: Optional[str] = None   # short label e.g. "pve1" (user-editable)
    version: Optional[str] = None
    port: int = 8006
    ssh_reachable: int = 0
    last_scanned: Optional[str] = None

class PveHostUpdate(BaseModel):
    hostname: Optional[str] = None
    pve_name: Optional[str] = None
    version: Optional[str] = None
    port: Optional[int] = None
    ssh_reachable: Optional[int] = None
    last_scanned: Optional[str] = None

class PveHostOut(BaseModel):
    pve_id: str
    ip_address: str
    hostname: Optional[str] = None
    pve_name: Optional[str] = None
    version: Optional[str] = None
    port: int = 8006
    ssh_reachable: int = 0
    last_scanned: Optional[str] = None
    created_at: str
    updated_at: str


# ── ARP Manual ───────────────────────────────────────────────────────────────

class ArpManualCreate(BaseModel):
    ip_address: str
    mac_address: str
    notes: Optional[str] = None

class ArpManualUpdate(BaseModel):
    ip_address: Optional[str] = None
    mac_address: Optional[str] = None
    notes: Optional[str] = None

class ArpManualOut(BaseModel):
    entry_id: str
    ip_address: str
    mac_address: str
    notes: Optional[str] = None
    created_at: str
    updated_at: str


# ── SSH Targets ───────────────────────────────────────────────────────────────

class SshTargetCreate(BaseModel):
    ip_address: str
    key_env_var: str                 # e.g. VM_SSH_KEY, LXC_SSH_KEY
    source_ip: Optional[str] = None  # bind source IP (same VLAN)
    host_name: Optional[str] = None
    host_type: Optional[str] = None  # lxc-fleet | lxc | qemu | citadel | pve | pfsense
    notes: Optional[str] = None

class SshTargetUpdate(BaseModel):
    key_env_var: Optional[str] = None
    source_ip: Optional[str] = None
    host_name: Optional[str] = None
    host_type: Optional[str] = None
    notes: Optional[str] = None

class SshTargetOut(BaseModel):
    ip_address: str
    key_env_var: str
    source_ip: Optional[str] = None
    host_name: Optional[str] = None
    host_type: Optional[str] = None
    notes: Optional[str] = None
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
