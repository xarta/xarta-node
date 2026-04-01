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
    display_order: int = 0        # preferred sort position in the node selector
    host_machine: Optional[str] = None
    tailnet: Optional[str] = None
    primary_hostname: Optional[str] = None   # management VLAN HTTPS hostname
    tailnet_hostname: Optional[str] = None   # tailnet HTTPS hostname
    addresses: Optional[list[str]] = None
    ui_url: Optional[str] = None   # browser-facing HTTPS URL for this node
    machine_id: Optional[str] = None  # canonical FK to machines.machine_id
    last_seen: Optional[str] = None
    created_at: str
    fleet_peer: bool = True       # False if node is in DB but not a configured sync peer
    pending_count: int = 0        # unsent sync queue entries targeting this node


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
    tailnet_ip: Optional[str] = None
    last_scanned: Optional[str] = None

class PveHostUpdate(BaseModel):
    hostname: Optional[str] = None
    pve_name: Optional[str] = None
    version: Optional[str] = None
    port: Optional[int] = None
    ssh_reachable: Optional[int] = None
    tailnet_ip: Optional[str] = None
    last_scanned: Optional[str] = None

class PveHostOut(BaseModel):
    pve_id: str
    ip_address: str
    hostname: Optional[str] = None
    pve_name: Optional[str] = None
    version: Optional[str] = None
    port: int = 8006
    ssh_reachable: int = 0
    tailnet_ip: Optional[str] = None
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

# ── Manual Links ─────────────────────────────────────────────────────────────

class ManualLinkCreate(BaseModel):
    vlan_ip:     Optional[str] = None
    vlan_uri:    Optional[str] = None
    tailnet_ip:  Optional[str] = None
    tailnet_uri: Optional[str] = None
    label:       Optional[str] = None
    icon:        Optional[str] = None
    group_name:  Optional[str] = None
    parent_id:   Optional[str] = None
    sort_order:  Optional[int] = 0
    pve_host:    Optional[str] = None
    is_internet: Optional[int] = 0
    vm_id:       Optional[str] = None
    vm_name:     Optional[str] = None
    lxc_id:      Optional[str] = None
    lxc_name:    Optional[str] = None
    location:    Optional[str] = None
    notes:       Optional[str] = None

class ManualLinkUpdate(BaseModel):
    vlan_ip:     Optional[str] = None
    vlan_uri:    Optional[str] = None
    tailnet_ip:  Optional[str] = None
    tailnet_uri: Optional[str] = None
    label:       Optional[str] = None
    icon:        Optional[str] = None
    group_name:  Optional[str] = None
    parent_id:   Optional[str] = None
    sort_order:  Optional[int] = None
    pve_host:    Optional[str] = None
    is_internet: Optional[int] = None
    vm_id:       Optional[str] = None
    vm_name:     Optional[str] = None
    lxc_id:      Optional[str] = None
    lxc_name:    Optional[str] = None
    location:    Optional[str] = None
    notes:       Optional[str] = None

class ManualLinkOut(BaseModel):
    link_id:     str
    vlan_ip:     Optional[str] = None
    vlan_uri:    Optional[str] = None
    tailnet_ip:  Optional[str] = None
    tailnet_uri: Optional[str] = None
    label:       Optional[str] = None
    icon:        Optional[str] = None
    group_name:  Optional[str] = None
    parent_id:   Optional[str] = None
    sort_order:  int = 0
    pve_host:    Optional[str] = None
    is_internet: int = 0
    vm_id:       Optional[str] = None
    vm_name:     Optional[str] = None
    lxc_id:      Optional[str] = None
    lxc_name:    Optional[str] = None
    location:    Optional[str] = None
    notes:       Optional[str] = None
    created_at:  str
    updated_at:  str


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


# ── Table Layouts ─────────────────────────────────────────────────────────────

class TableLayoutCatalogCreate(BaseModel):
    table_code: str
    table_name: str
    table_meta: dict[str, Any]


class TableLayoutCatalogUpdate(BaseModel):
    table_name: Optional[str] = None
    table_meta: Optional[dict[str, Any]] = None


class TableLayoutCatalogOut(BaseModel):
    table_code: str
    table_name: str
    table_meta: dict[str, Any]
    created_at: str
    updated_at: str


class TableLayoutBucketBits(BaseModel):
    shade_up: bool = False
    horizontal_scroll: bool = False
    mobile: bool = False
    portrait: bool = False
    wide: bool = False


class TableLayoutColumnSeed(BaseModel):
    column_key: Optional[str] = None
    display_name: str
    sqlite_column: Optional[str] = None
    width_px: Optional[int] = None
    min_width_px: Optional[int] = None
    max_width_px: Optional[int] = None
    position: Optional[int] = None
    sort_direction: Optional[str] = None
    sort_priority: Optional[int] = None
    hidden: bool = False
    data_type: Optional[str] = None
    sample_max_length: Optional[int] = None


class TableLayoutResolveRequest(BaseModel):
    table_code: Optional[str] = None
    table_name: Optional[str] = None
    reserved_code: str = "00"
    user_code: str = "00"
    bucket_code: Optional[str] = None
    bucket_bits: Optional[TableLayoutBucketBits] = None
    columns: list[TableLayoutColumnSeed] = Field(default_factory=list)


class TableLayoutUpsert(BaseModel):
    layout_data: dict[str, Any]


class TableLayoutOut(BaseModel):
    layout_key: str
    reserved_code: str
    user_code: str
    table_code: str
    bucket_code: str
    layout_data: dict[str, Any]
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
    guid: str = ""  # UUID4 hex from originating node; empty = legacy (skip dedup)


class GitPullRequest(BaseModel):
    scope: str = "outer"   # "outer" | "inner" | "both" | "non_root" | "all"


class SyncActionsPayload(BaseModel):
    actions: list[SyncAction]
    source_node_id: str
    source_commit_ts: int = 0   # unix epoch of source node's HEAD commit


class SyncStatus(BaseModel):
    node_id: str
    node_name: str
    gen: int
    integrity_ok: bool
    last_write_at: str
    last_write_by: str
    queue_depths: dict[str, int]      # {peer_node_id: pending_count}
    peer_count: int


# ── Firewall ──────────────────────────────────────────────────────────────────

class FirewallPortCheck(BaseModel):
    port: int
    proto: str           # "tcp" or "udp"
    label: str
    expected: str        # "open" or "blocked"
    in_ruleset: bool     # found in XARTA_INPUT chain


class FirewallStatusOut(BaseModel):
    iptables_available: bool
    input_policy: str    # "DROP", "ACCEPT", or "unknown"
    xarta_input_chain: bool
    ports: list[FirewallPortCheck]


class FirewallProbePort(BaseModel):
    port: int
    proto: str           # "tcp" or "udp"
    label: str
    expected: str        # "open" or "blocked"
    result: str          # "open", "blocked", "timeout", "error", "skipped"
    pass_: bool = Field(default=False, alias="pass")

    model_config = {"populate_by_name": True}


class FirewallProbeOut(BaseModel):
    prober_node: str
    target: str
    ports: list[FirewallProbePort]
    all_pass: bool


# ── Health ────────────────────────────────────────────────────────────────────

# ── Docs ─────────────────────────────────────────────────────────────────────

class DocGroupCreate(BaseModel):
    name: str
    sort_order: int = 0


class DocGroupUpdate(BaseModel):
    name: Optional[str] = None
    sort_order: Optional[int] = None


class DocGroupOut(BaseModel):
    group_id: str
    name: str
    sort_order: int = 0
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class DocCreate(BaseModel):
    label: str
    description: Optional[str] = None
    tags: Optional[str] = None          # comma-separated, e.g. "menu,ops"
    path: str                           # relative path from REPO_INNER_PATH
    sort_order: int = 0
    group_id: Optional[str] = None     # NULL = Undefined Group
    initial_content: Optional[str] = None  # written to file on create if provided


class DocUpdate(BaseModel):
    label: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[str] = None
    path: Optional[str] = None
    sort_order: Optional[int] = None
    group_id: Optional[str] = None     # use empty string "" to clear group


class DocOut(BaseModel):
    doc_id: str
    label: str
    description: Optional[str] = None
    tags: Optional[str] = None
    path: str
    sort_order: int = 0
    group_id: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class DocWithContent(DocOut):
    content: Optional[str] = None
    file_exists: bool = False


class DocContentBody(BaseModel):
    content: str


# ── Doc Images ────────────────────────────────────────────────────────────────

class DocImageOut(BaseModel):
    image_id: str
    filename: str
    description: Optional[str] = None
    tags: Optional[list[str]] = None
    file_size: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class DocImageUpdate(BaseModel):
    description: Optional[str] = None
    tags: Optional[list[str]] = None


# ── Health ────────────────────────────────────────────────────────────────────

class HealthOut(BaseModel):
    status: str
    node_id: str
    node_name: str
    gen: int
    integrity_ok: bool
    ui_url: Optional[str] = None   # browser-accessible URL; set via BLUEPRINTS_UI_URL
    commit: Optional[str] = None   # short git hash of the current outer-repo checkout
    commit_ts: int = 0             # unix epoch of HEAD commit (for commit-guard ordering)


class RepoVersionOut(BaseModel):
    label: str
    path: str
    exists: bool = False
    branch: Optional[str] = None
    commit: Optional[str] = None
    commit_ts: int = 0
    dirty: bool = False


class RepoVersionsOut(BaseModel):
    node_id: str
    outer: RepoVersionOut
    inner: RepoVersionOut
    non_root: RepoVersionOut


# ── AI Providers ─────────────────────────────────────────────────────────────

class AiProviderCreate(BaseModel):
    name: str
    base_url: str
    api_key: str = ""
    model_name: str
    model_type: str                     # 'embedding' | 'reranker' | 'llm'
    dimensions: Optional[int] = None   # output dims (embedding models only)
    enabled: bool = True
    options: Optional[str] = None      # JSON blob
    notes: Optional[str] = None


class AiProviderUpdate(BaseModel):
    name: Optional[str] = None
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    model_name: Optional[str] = None
    model_type: Optional[str] = None
    dimensions: Optional[int] = None
    enabled: Optional[bool] = None
    options: Optional[str] = None
    notes: Optional[str] = None


class AiProviderOut(BaseModel):
    provider_id: str
    name: str
    base_url: str
    api_key: str
    model_name: str
    model_type: str
    dimensions: Optional[int] = None
    enabled: bool = True
    options: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# ── AI Project Assignments ────────────────────────────────────────────────────

class AiProjectAssignmentCreate(BaseModel):
    project_name: str
    provider_id: str
    role: str                   # 'embedding' | 'reranker' | 'llm'
    priority: int = 0
    enabled: bool = True


class AiProjectAssignmentUpdate(BaseModel):
    project_name: Optional[str] = None
    provider_id: Optional[str] = None
    role: Optional[str] = None
    priority: Optional[int] = None
    enabled: Optional[bool] = None


class AiProjectAssignmentOut(BaseModel):
    assignment_id: str
    project_name: str
    provider_id: str
    role: str
    priority: int = 0
    enabled: bool = True
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# ── Browser Links (Bookmarks + Visits) ──────────────────────────────────────

class BookmarkCreate(BaseModel):
    url: str
    title: str = ""
    description: str = ""
    tags: list[str] = []
    folder: str = ""
    notes: str = ""
    favicon_url: str = ""
    source: str = "manual"


class BookmarkUpdate(BaseModel):
    url: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[list[str]] = None
    folder: Optional[str] = None
    notes: Optional[str] = None
    favicon_url: Optional[str] = None
    source: Optional[str] = None
    archived: Optional[bool] = None


class BookmarkOut(BaseModel):
    bookmark_id: str
    url: str
    normalized_url: str
    title: str
    description: str
    tags: list[str] = []
    folder: str = ""
    notes: str = ""
    favicon_url: str = ""
    source: str = "manual"
    archived: bool = False
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class BookmarkImportRequest(BaseModel):
    bookmarks: list[BookmarkCreate]
    skip_duplicates: bool = True


class BookmarkImportResult(BaseModel):
    imported: int
    skipped_duplicates: int


class VisitCreate(BaseModel):
    url: str
    title: str = ""
    source: str = "visit-recorder"
    dwell_seconds: Optional[int] = None
    visited_at: Optional[str] = None


class VisitOut(BaseModel):
    visit_id: str
    url: str
    normalized_url: str
    domain: str = ""
    title: str = ""
    source: str = "visit-recorder"
    dwell_seconds: Optional[int] = None
    bookmark_id: Optional[str] = None
    visited_at: str
    visit_count: int = 1
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# ── Nav Items ─────────────────────────────────────────────────────────────────

# ── Form Controls ────────────────────────────────────────────────────────────

class FormControlCreate(BaseModel):
    control_key: str
    label: str
    control_type: Optional[str] = None
    context: Optional[str] = None
    icon_asset: Optional[str] = None
    sound_asset: Optional[str] = None
    sound_asset_off: Optional[str] = None
    notes: Optional[str] = None


class FormControlUpdate(BaseModel):
    label: Optional[str] = None
    control_type: Optional[str] = None
    context: Optional[str] = None
    icon_asset: Optional[str] = None
    sound_asset: Optional[str] = None
    sound_asset_off: Optional[str] = None
    notes: Optional[str] = None


class FormControlOut(BaseModel):
    control_id: str
    control_key: str
    label: str
    control_type: Optional[str] = None
    context: Optional[str] = None
    icon_asset: Optional[str] = None
    sound_asset: Optional[str] = None
    sound_asset_off: Optional[str] = None
    notes: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


# ── Nav Items ─────────────────────────────────────────────────────────────────

class NavItemCreate(BaseModel):
    menu_group: str
    item_key: str
    label: str
    page_label: Optional[str] = None
    icon_emoji: Optional[str] = None
    icon_asset: Optional[str] = None
    sound_asset: Optional[str] = None
    parent_key: Optional[str] = None
    sort_order: int = 0
    is_fn: int = 0
    fn_key: Optional[str] = None
    active_on: Optional[str] = None   # JSON string e.g. '["bookmarks-main"]'


class NavItemUpdate(BaseModel):
    label: Optional[str] = None
    page_label: Optional[str] = None
    icon_emoji: Optional[str] = None
    icon_asset: Optional[str] = None
    sound_asset: Optional[str] = None
    parent_key: Optional[str] = None
    sort_order: Optional[int] = None
    is_fn: Optional[int] = None
    fn_key: Optional[str] = None
    active_on: Optional[str] = None


class NavItemOut(BaseModel):
    item_id: str
    menu_group: str
    item_key: str
    label: str
    page_label: Optional[str] = None
    icon_emoji: Optional[str] = None
    icon_asset: Optional[str] = None
    sound_asset: Optional[str] = None
    parent_key: Optional[str] = None
    sort_order: int = 0
    is_fn: int = 0
    fn_key: Optional[str] = None
    active_on: Optional[str] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
