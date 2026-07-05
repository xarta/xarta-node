"""
db.py — SQLite initialisation, schema, integrity check, and connection helper.

WAL mode + synchronous=NORMAL gives crash-safe atomic commits with good
read concurrency. The generation counter in sync_meta provides a total
ordering across all committed writes — critical for the sync engine.
"""

import hashlib
import json
import logging
import os
import sqlite3
import time
from contextlib import contextmanager, suppress
from typing import Generator

from . import config as cfg
from . import timing
from .url_identity import normalize_url_identity

log = logging.getLogger(__name__)

# ── Schema DDL ────────────────────────────────────────────────────────────────
_SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS machines (
    machine_id        TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    type              TEXT NOT NULL,
    parent_machine_id TEXT,
    ip_addresses      TEXT,
    description       TEXT,
    created_at        TEXT DEFAULT (datetime('now')),
    updated_at        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS services (
    service_id        TEXT PRIMARY KEY,
    name              TEXT NOT NULL,
    description       TEXT,
    host_machine      TEXT,
    vm_or_lxc         TEXT,
    ports             TEXT,
    caddy_routes      TEXT,
    dns_info          TEXT,
    credential_hints  TEXT,
    dependencies      TEXT,
    project_status    TEXT DEFAULT 'deployed',
    tags              TEXT,
    links             TEXT,
    created_at        TEXT DEFAULT (datetime('now')),
    updated_at        TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id      TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    host_machine TEXT,
    tailnet      TEXT,
    addresses    TEXT,
    ui_url       TEXT,
    last_seen    TEXT,
    created_at   TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS sync_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_queue (
    queue_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    target_node_id TEXT    NOT NULL,
    action_type    TEXT    NOT NULL,
    table_name     TEXT    NOT NULL,
    row_id         TEXT    NOT NULL,
    row_data       TEXT,
    gen            INTEGER NOT NULL,
    created_at     TEXT    DEFAULT (datetime('now')),
    sent           INTEGER DEFAULT 0,
    sent_at        TEXT    DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_sync_queue_target
    ON sync_queue(target_node_id, sent, queue_id);

CREATE TABLE IF NOT EXISTS sync_seen_guids (
    guid        TEXT    PRIMARY KEY,  -- UUID4 hex from originating node
    received_at INTEGER NOT NULL       -- unix epoch; purged after 3 days
);

CREATE TABLE IF NOT EXISTS disks_notes (
    node_id    TEXT PRIMARY KEY,
    note       TEXT NOT NULL DEFAULT '',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS disks_filesystem_favorites (
    favorite_id    TEXT PRIMARY KEY,
    label          TEXT NOT NULL,
    host           TEXT NOT NULL,
    root_path      TEXT NOT NULL,
    path           TEXT NOT NULL DEFAULT '.',
    browse_mode    TEXT NOT NULL DEFAULT 'mounted',
    source_path    TEXT NOT NULL DEFAULT '',
    filesystem     TEXT NOT NULL DEFAULT '',
    dataset_name   TEXT NOT NULL DEFAULT '',
    guest_id       TEXT NOT NULL DEFAULT '',
    guest_name     TEXT NOT NULL DEFAULT '',
    guest_kind     TEXT NOT NULL DEFAULT '',
    sensitive_hint TEXT NOT NULL DEFAULT '',
    enabled        INTEGER NOT NULL DEFAULT 1,
    sort_order     INTEGER NOT NULL DEFAULT 0,
    created_at     TEXT DEFAULT (datetime('now')),
    updated_at     TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS disks_security (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL DEFAULT '',
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pfsense_dns (
    dns_entry_id  TEXT PRIMARY KEY,
    ip_address    TEXT NOT NULL,
    fqdn          TEXT NOT NULL,
    record_type   TEXT,
    source        TEXT,
    mac_address   TEXT,
    active        INTEGER DEFAULT 1,
    last_seen     TEXT DEFAULT (datetime('now')),
    last_probed   TEXT DEFAULT (datetime('now')),
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_pfsense_dns_ip
    ON pfsense_dns(ip_address);
CREATE INDEX IF NOT EXISTS idx_pfsense_dns_fqdn
    ON pfsense_dns(fqdn);

CREATE TABLE IF NOT EXISTS proxmox_config (
    config_id       TEXT PRIMARY KEY,       -- "{pve_name}_{vmid}" e.g. "pve1_100"
    pve_host        TEXT NOT NULL,           -- PVE host IP address
    pve_name        TEXT,                    -- short label e.g. "pve1"
    vmid            INTEGER NOT NULL,
    vm_type         TEXT NOT NULL,           -- 'lxc' | 'qemu'
    name            TEXT,                    -- hostname from conf
    status          TEXT,                    -- 'running' | 'stopped'
    cores           INTEGER,
    memory_mb       INTEGER,
    rootfs          TEXT,                    -- raw rootfs line
    ip_config       TEXT,                    -- raw net0 line
    ip_address      TEXT,                    -- parsed IP (no CIDR)
    gateway         TEXT,
    mac_address     TEXT,
    vlan_tag        INTEGER,
    tags            TEXT,                    -- comma-separated tags
    mountpoints_json  TEXT,                   -- JSON array
    raw_conf          TEXT,                    -- full conf file content
    vlans_json        TEXT,                    -- JSON array of all VLAN tags across all net interfaces
    has_docker        INTEGER DEFAULT 0,       -- docker detected in running LXC
    dockge_stacks_dir TEXT,                   -- stacks dir if dockge detected (legacy)
    has_portainer     INTEGER DEFAULT 0,       -- portainer detected (legacy)
    portainer_method  TEXT,                    -- how portainer was detected (legacy)
    has_caddy         INTEGER DEFAULT 0,       -- caddy detected (legacy)
    caddy_conf_path   TEXT,                    -- caddy binary path (legacy)
    dockge_json       TEXT,                    -- JSON array [{container, stacks_dir}]
    portainer_json    TEXT,                    -- JSON array [{container, data_dir, method}]
    caddy_json        TEXT,                    -- JSON array [{method, caddyfile, container?}]
    last_probed       TEXT,
    created_at        TEXT DEFAULT (datetime('now')),
    updated_at        TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_proxmox_config_pve
    ON proxmox_config(pve_host, vmid);

CREATE TABLE IF NOT EXISTS dockge_stacks (
    stack_id         TEXT PRIMARY KEY,       -- "{source_vmid}_{stack_name}"
    pve_host         TEXT NOT NULL,
    source_vmid      INTEGER NOT NULL,
    source_lxc_name  TEXT,
    stack_name       TEXT NOT NULL,
    status           TEXT,                   -- 'running' | 'stopped' | 'unknown' | 'partial'
    compose_content  TEXT,                   -- raw compose.yaml content
    services_json    TEXT,                   -- JSON array of service names
    ports_json       TEXT,                   -- JSON array of "host:container" strings
    volumes_json     TEXT,                   -- JSON array of volume mounts
    env_file_exists  INTEGER DEFAULT 0,      -- 1 if .env present (content not stored)
    stacks_dir       TEXT,                   -- base dir e.g. "/opt/stacks"
    last_probed      TEXT,
    created_at       TEXT DEFAULT (datetime('now')),
    updated_at       TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dockge_stacks_vmid
    ON dockge_stacks(source_vmid);

CREATE TABLE IF NOT EXISTS dockge_stack_services (
    service_id      TEXT PRIMARY KEY,   -- "{stack_id}_{service_name}"
    stack_id        TEXT NOT NULL,       -- FK → dockge_stacks
    service_name    TEXT NOT NULL,
    image           TEXT,
    ports_json      TEXT,               -- JSON array of "host:container/proto" strings
    volumes_json    TEXT,               -- JSON array of volume mounts
    container_state TEXT,               -- running|stopped|restarting|etc
    container_id    TEXT,               -- short Docker container ID
    last_probed     TEXT,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_dockge_stack_services_stack_id
    ON dockge_stack_services(stack_id);

CREATE TABLE IF NOT EXISTS caddy_configs (
    caddy_id          TEXT PRIMARY KEY,      -- "{source_vmid}_{path_slug}"
    pve_host          TEXT,
    source_vmid       INTEGER,
    source_lxc_name   TEXT,
    caddyfile_path    TEXT,                  -- e.g. "/etc/caddy/Caddyfile"
    caddyfile_content TEXT,                  -- full Caddyfile content
    domains_json      TEXT,                  -- JSON array of parsed domain/host tokens
    upstreams_json    TEXT,                  -- JSON array of parsed upstream addresses
    last_probed       TEXT,
    created_at        TEXT DEFAULT (datetime('now')),
    updated_at        TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_caddy_configs_vmid
    ON caddy_configs(source_vmid);

CREATE TABLE IF NOT EXISTS proxmox_nets (
    net_id        TEXT PRIMARY KEY,   -- "{config_id}_net{N}" e.g. "pve1_100_net0"
    config_id     TEXT NOT NULL,      -- FK → proxmox_config.config_id
    pve_host      TEXT NOT NULL,
    vmid          INTEGER NOT NULL,
    net_key       TEXT NOT NULL,      -- "net0", "net1" …
    mac_address   TEXT,
    ip_address    TEXT,               -- parsed (no CIDR); may be filled from pfsense
    ip_cidr       TEXT,               -- raw "x.x.x.x/24"
    gateway       TEXT,
    vlan_tag      INTEGER,
    bridge        TEXT,               -- "vmbr0" etc.
    model         TEXT,               -- "virtio", "e1000" etc.
    raw_str       TEXT,               -- full raw net line value
    ip_source     TEXT DEFAULT 'conf', -- 'conf' | 'pfsense' | 'manual'
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_proxmox_nets_config
    ON proxmox_nets(config_id);
CREATE INDEX IF NOT EXISTS idx_proxmox_nets_mac
    ON proxmox_nets(mac_address);

CREATE TABLE IF NOT EXISTS vlans (
    vlan_id       INTEGER PRIMARY KEY,   -- VLAN tag number (e.g. 42)
    cidr          TEXT,                  -- CIDR range e.g. "10.0.42.0/24"
    cidr_inferred INTEGER DEFAULT 1,     -- 1=auto-inferred from IP data, 0=manually confirmed
    description   TEXT,
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_vlans_cidr
    ON vlans(cidr);

CREATE TABLE IF NOT EXISTS settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL DEFAULT '',
    description TEXT,
    updated_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS personal_events (
    event_id                      TEXT PRIMARY KEY,
    source_type                   TEXT NOT NULL DEFAULT 'manual',
    source_ref                    TEXT,
    source_hash                   TEXT,
    kind                          TEXT NOT NULL DEFAULT 'event',
    title                         TEXT NOT NULL DEFAULT '',
    body_excerpt                  TEXT,
    content_projection            TEXT,
    start_at                      TEXT,
    end_at                        TEXT,
    local_date                    TEXT,
    timezone                      TEXT,
    status                        TEXT NOT NULL DEFAULT 'open',
    priority                      TEXT,
    privacy_level                 TEXT NOT NULL DEFAULT 'normal',
    tags_json                     TEXT NOT NULL DEFAULT '[]',
    entities_json                 TEXT NOT NULL DEFAULT '[]',
    related_kanban_items_json       TEXT NOT NULL DEFAULT '[]',
    related_tasks_json            TEXT NOT NULL DEFAULT '[]',
    related_import_batches_json   TEXT NOT NULL DEFAULT '[]',
    file_refs_json                TEXT NOT NULL DEFAULT '[]',
    db_refs_json                  TEXT NOT NULL DEFAULT '[]',
    provenance_json               TEXT NOT NULL DEFAULT '{}',
    projection_state              TEXT NOT NULL DEFAULT 'hot',
    provenance_state              TEXT NOT NULL DEFAULT 'linked',
    last_rendered_at              TEXT,
    projection_expires_at         TEXT,
    retention_days                INTEGER DEFAULT 60,
    created_at                    TEXT DEFAULT (datetime('now')),
    updated_at                    TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_events_local_date
    ON personal_events(local_date, start_at);
CREATE INDEX IF NOT EXISTS idx_personal_events_source
    ON personal_events(source_type, source_ref);
CREATE INDEX IF NOT EXISTS idx_personal_events_status
    ON personal_events(status, kind);
CREATE INDEX IF NOT EXISTS idx_personal_events_privacy
    ON personal_events(privacy_level);

CREATE TABLE IF NOT EXISTS personal_time_tasks (
    task_id                       TEXT PRIMARY KEY,
    source_type                   TEXT NOT NULL DEFAULT 'manual-task',
    source_ref                    TEXT,
    source_hash                   TEXT,
    title                         TEXT NOT NULL DEFAULT '',
    body_excerpt                  TEXT,
    status                        TEXT NOT NULL DEFAULT 'open',
    mode                          TEXT NOT NULL DEFAULT 'personal',
    priority                      TEXT,
    due_at                        TEXT,
    local_date                    TEXT,
    timezone                      TEXT,
    privacy_level                 TEXT NOT NULL DEFAULT 'normal',
    tags_json                     TEXT NOT NULL DEFAULT '[]',
    related_kanban_items_json       TEXT NOT NULL DEFAULT '[]',
    related_tasks_json            TEXT NOT NULL DEFAULT '[]',
    related_import_batches_json   TEXT NOT NULL DEFAULT '[]',
    file_refs_json                TEXT NOT NULL DEFAULT '[]',
    db_refs_json                  TEXT NOT NULL DEFAULT '[]',
    event_id                      TEXT,
    provenance_json               TEXT NOT NULL DEFAULT '{}',
    completed_at                  TEXT,
    archived_at                   TEXT,
    created_at                    TEXT DEFAULT (datetime('now')),
    updated_at                    TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_time_tasks_status
    ON personal_time_tasks(status, mode);
CREATE INDEX IF NOT EXISTS idx_personal_time_tasks_due
    ON personal_time_tasks(local_date, due_at);
CREATE INDEX IF NOT EXISTS idx_personal_time_tasks_source
    ON personal_time_tasks(source_type, source_ref);
CREATE INDEX IF NOT EXISTS idx_personal_time_tasks_privacy
    ON personal_time_tasks(privacy_level);

CREATE TABLE IF NOT EXISTS personal_sources (
    source_id        TEXT PRIMARY KEY,
    source_type      TEXT NOT NULL,
    label            TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL DEFAULT 'unknown',
    last_seen_at     TEXT,
    health_json      TEXT NOT NULL DEFAULT '{}',
    provenance_json  TEXT NOT NULL DEFAULT '{}',
    created_at       TEXT DEFAULT (datetime('now')),
    updated_at       TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_sources_type
    ON personal_sources(source_type, status);

CREATE TABLE IF NOT EXISTS personal_filter_meta_tags (
    meta_tag_id      TEXT PRIMARY KEY,
    label            TEXT NOT NULL DEFAULT '',
    color            TEXT NOT NULL DEFAULT 'blue',
    priority         INTEGER NOT NULL DEFAULT 0,
    provenance_json  TEXT NOT NULL DEFAULT '{}',
    created_at       TEXT DEFAULT (datetime('now')),
    updated_at       TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_filter_meta_tags_priority
    ON personal_filter_meta_tags(priority, label);

CREATE TABLE IF NOT EXISTS personal_filter_tags (
    tag_id           TEXT PRIMARY KEY,
    label            TEXT NOT NULL DEFAULT '',
    color            TEXT NOT NULL DEFAULT 'blue',
    shape            TEXT NOT NULL DEFAULT 'circle',
    fill             TEXT NOT NULL DEFAULT 'outline',
    meta_tag_id      TEXT NOT NULL DEFAULT '',
    builtin          INTEGER NOT NULL DEFAULT 0,
    provenance_json  TEXT NOT NULL DEFAULT '{}',
    created_at       TEXT DEFAULT (datetime('now')),
    updated_at       TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_filter_tags_meta
    ON personal_filter_tags(meta_tag_id, label);

CREATE TABLE IF NOT EXISTS personal_import_batches (
    import_batch_id      TEXT PRIMARY KEY,
    source_type          TEXT NOT NULL,
    source_ref           TEXT,
    title                TEXT NOT NULL DEFAULT '',
    status               TEXT NOT NULL DEFAULT 'pending_review',
    local_date           TEXT,
    started_at           TEXT,
    completed_at         TEXT,
    privacy_level        TEXT NOT NULL DEFAULT 'normal',
    artifact_refs_json   TEXT NOT NULL DEFAULT '[]',
    blocker_refs_json    TEXT NOT NULL DEFAULT '[]',
    provenance_json      TEXT NOT NULL DEFAULT '{}',
    created_at           TEXT DEFAULT (datetime('now')),
    updated_at           TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_import_batches_date
    ON personal_import_batches(local_date, started_at);
CREATE INDEX IF NOT EXISTS idx_personal_import_batches_source
    ON personal_import_batches(source_type, status);
CREATE INDEX IF NOT EXISTS idx_personal_import_batches_privacy
    ON personal_import_batches(privacy_level);

CREATE TABLE IF NOT EXISTS personal_git_repositories (
    repo_full_name        TEXT PRIMARY KEY,
    repo_id               INTEGER,
    owner_login           TEXT NOT NULL DEFAULT '',
    name                  TEXT NOT NULL DEFAULT '',
    html_url              TEXT NOT NULL DEFAULT '',
    description           TEXT NOT NULL DEFAULT '',
    default_branch        TEXT NOT NULL DEFAULT '',
    visibility            TEXT NOT NULL DEFAULT '',
    is_private            INTEGER NOT NULL DEFAULT 0,
    is_fork               INTEGER NOT NULL DEFAULT 0,
    is_archived           INTEGER NOT NULL DEFAULT 0,
    can_push              INTEGER NOT NULL DEFAULT 0,
    last_pushed_at        TEXT,
    last_seen_at          TEXT,
    provenance_json       TEXT NOT NULL DEFAULT '{}',
    created_at            TEXT DEFAULT (datetime('now')),
    updated_at            TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_git_repositories_owner
    ON personal_git_repositories(owner_login, can_push, visibility);
CREATE INDEX IF NOT EXISTS idx_personal_git_repositories_seen
    ON personal_git_repositories(last_seen_at);

CREATE TABLE IF NOT EXISTS personal_git_commits (
    commit_id             TEXT PRIMARY KEY,
    repo_full_name        TEXT NOT NULL,
    sha                   TEXT NOT NULL,
    short_sha             TEXT NOT NULL DEFAULT '',
    html_url              TEXT NOT NULL DEFAULT '',
    author_login          TEXT NOT NULL DEFAULT '',
    author_name           TEXT NOT NULL DEFAULT '',
    committed_at          TEXT NOT NULL DEFAULT '',
    local_date            TEXT NOT NULL DEFAULT '',
    message_subject       TEXT NOT NULL DEFAULT '',
    message_body          TEXT NOT NULL DEFAULT '',
    branches_json         TEXT NOT NULL DEFAULT '[]',
    pr_refs_json          TEXT NOT NULL DEFAULT '[]',
    issue_refs_json       TEXT NOT NULL DEFAULT '[]',
    feature_key           TEXT NOT NULL DEFAULT '',
    source_hash           TEXT NOT NULL DEFAULT '',
    provenance_json       TEXT NOT NULL DEFAULT '{}',
    created_at            TEXT DEFAULT (datetime('now')),
    updated_at            TEXT DEFAULT (datetime('now')),
    UNIQUE(repo_full_name, sha)
);
CREATE INDEX IF NOT EXISTS idx_personal_git_commits_day
    ON personal_git_commits(local_date, repo_full_name);
CREATE INDEX IF NOT EXISTS idx_personal_git_commits_feature
    ON personal_git_commits(feature_key, local_date);

CREATE TABLE IF NOT EXISTS personal_git_features (
    feature_id            TEXT PRIMARY KEY,
    feature_key           TEXT NOT NULL DEFAULT '',
    title                 TEXT NOT NULL DEFAULT '',
    status                TEXT NOT NULL DEFAULT 'active',
    first_seen_date       TEXT NOT NULL DEFAULT '',
    last_seen_date        TEXT NOT NULL DEFAULT '',
    repo_full_names_json  TEXT NOT NULL DEFAULT '[]',
    commit_count          INTEGER NOT NULL DEFAULT 0,
    related_kanban_item_id  TEXT NOT NULL DEFAULT '',
    project_arc_id        TEXT NOT NULL DEFAULT '',
    subproject_arc_id     TEXT NOT NULL DEFAULT '',
    parent_work_item_id   TEXT NOT NULL DEFAULT '',
    source_hash           TEXT NOT NULL DEFAULT '',
    provenance_json       TEXT NOT NULL DEFAULT '{}',
    created_at            TEXT DEFAULT (datetime('now')),
    updated_at            TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_git_features_key
    ON personal_git_features(feature_key);
CREATE INDEX IF NOT EXISTS idx_personal_git_features_dates
    ON personal_git_features(first_seen_date, last_seen_date);

CREATE TABLE IF NOT EXISTS personal_git_kanban_arcs (
    arc_id                TEXT PRIMARY KEY,
    arc_type              TEXT NOT NULL DEFAULT '',
    arc_key               TEXT NOT NULL DEFAULT '',
    title                 TEXT NOT NULL DEFAULT '',
    status                TEXT NOT NULL DEFAULT 'active',
    first_seen_date       TEXT NOT NULL DEFAULT '',
    last_seen_date        TEXT NOT NULL DEFAULT '',
    parent_arc_id         TEXT NOT NULL DEFAULT '',
    repo_full_names_json  TEXT NOT NULL DEFAULT '[]',
    feature_keys_json     TEXT NOT NULL DEFAULT '[]',
    commit_count          INTEGER NOT NULL DEFAULT 0,
    related_kanban_item_id  TEXT NOT NULL DEFAULT '',
    source_hash           TEXT NOT NULL DEFAULT '',
    provenance_json       TEXT NOT NULL DEFAULT '{}',
    created_at            TEXT DEFAULT (datetime('now')),
    updated_at            TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_git_kanban_arcs_type
    ON personal_git_kanban_arcs(arc_type, arc_key);
CREATE INDEX IF NOT EXISTS idx_personal_git_kanban_arcs_parent
    ON personal_git_kanban_arcs(parent_arc_id, arc_type);

CREATE TABLE IF NOT EXISTS personal_git_daily_summaries (
    summary_id            TEXT PRIMARY KEY,
    local_date            TEXT NOT NULL,
    title                 TEXT NOT NULL DEFAULT '',
    markdown              TEXT NOT NULL DEFAULT '',
    repo_count            INTEGER NOT NULL DEFAULT 0,
    commit_count          INTEGER NOT NULL DEFAULT 0,
    feature_count         INTEGER NOT NULL DEFAULT 0,
    related_kanban_items_json TEXT NOT NULL DEFAULT '[]',
    source_hash           TEXT NOT NULL DEFAULT '',
    provenance_json       TEXT NOT NULL DEFAULT '{}',
    event_id              TEXT NOT NULL DEFAULT '',
    created_at            TEXT DEFAULT (datetime('now')),
    updated_at            TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_git_daily_summaries_date
    ON personal_git_daily_summaries(local_date);

CREATE TABLE IF NOT EXISTS personal_git_import_runs (
    run_id                TEXT PRIMARY KEY,
    status                TEXT NOT NULL DEFAULT 'pending_review',
    mode                  TEXT NOT NULL DEFAULT 'dry-run',
    started_at            TEXT NOT NULL DEFAULT '',
    completed_at          TEXT,
    date_start            TEXT NOT NULL DEFAULT '',
    date_end              TEXT NOT NULL DEFAULT '',
    repo_count            INTEGER NOT NULL DEFAULT 0,
    commit_count          INTEGER NOT NULL DEFAULT 0,
    summary_count         INTEGER NOT NULL DEFAULT 0,
    params_json           TEXT NOT NULL DEFAULT '{}',
    report_json           TEXT NOT NULL DEFAULT '{}',
    source_hash           TEXT NOT NULL DEFAULT '',
    created_at            TEXT DEFAULT (datetime('now')),
    updated_at            TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_git_import_runs_dates
    ON personal_git_import_runs(date_start, date_end, started_at);

CREATE TABLE IF NOT EXISTS personal_time_audit (
    audit_id        TEXT PRIMARY KEY,
    actor           TEXT NOT NULL DEFAULT '',
    source_surface  TEXT NOT NULL DEFAULT '',
    action          TEXT NOT NULL DEFAULT '',
    target_ref      TEXT NOT NULL DEFAULT '',
    file_ref        TEXT NOT NULL DEFAULT '',
    db_ref          TEXT NOT NULL DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now')),
    request_id      TEXT NOT NULL DEFAULT '',
    run_id          TEXT NOT NULL DEFAULT '',
    result          TEXT NOT NULL DEFAULT '',
    source_hash     TEXT NOT NULL DEFAULT '',
    metadata_json   TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_personal_time_audit_target
    ON personal_time_audit(target_ref, created_at);
CREATE INDEX IF NOT EXISTS idx_personal_time_audit_actor
    ON personal_time_audit(actor, source_surface);

CREATE TABLE IF NOT EXISTS personal_search_documents (
    document_id             TEXT PRIMARY KEY,
    record_type             TEXT NOT NULL DEFAULT '',
    record_table            TEXT NOT NULL DEFAULT '',
    record_id               TEXT NOT NULL DEFAULT '',
    source_type             TEXT NOT NULL DEFAULT '',
    source_ref              TEXT NOT NULL DEFAULT '',
    source_hash             TEXT NOT NULL DEFAULT '',
    title                   TEXT NOT NULL DEFAULT '',
    body                    TEXT NOT NULL DEFAULT '',
    search_text             TEXT NOT NULL DEFAULT '',
    local_date              TEXT,
    status                  TEXT NOT NULL DEFAULT '',
    mode                    TEXT NOT NULL DEFAULT '',
    privacy_level           TEXT NOT NULL DEFAULT 'normal',
    tags_json               TEXT NOT NULL DEFAULT '[]',
    related_refs_json       TEXT NOT NULL DEFAULT '[]',
    page_ref_json           TEXT NOT NULL DEFAULT '{}',
    source_refs_json        TEXT NOT NULL DEFAULT '[]',
    provenance_json         TEXT NOT NULL DEFAULT '{}',
    score_metadata_json     TEXT NOT NULL DEFAULT '{}',
    embedding_ref           TEXT NOT NULL DEFAULT '',
    embedding_model         TEXT NOT NULL DEFAULT '',
    embedding_updated_at    TEXT,
    vector_index_key        TEXT NOT NULL DEFAULT '',
    vector_index_status     TEXT NOT NULL DEFAULT 'pending',
    vector_index_updated_at TEXT,
    created_at              TEXT DEFAULT (datetime('now')),
    updated_at              TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_search_documents_filters
    ON personal_search_documents(local_date, source_type, status, record_type);
CREATE INDEX IF NOT EXISTS idx_personal_search_documents_mode
    ON personal_search_documents(mode, status);
CREATE INDEX IF NOT EXISTS idx_personal_search_documents_record
    ON personal_search_documents(record_table, record_id);
CREATE INDEX IF NOT EXISTS idx_personal_search_documents_vector
    ON personal_search_documents(vector_index_key, vector_index_status);

CREATE VIRTUAL TABLE IF NOT EXISTS personal_search_fts USING fts5(
    document_id UNINDEXED,
    title,
    body,
    search_text,
    tags,
    source_type,
    record_type,
    tokenize='porter unicode61'
);

CREATE TABLE IF NOT EXISTS personal_graph_links (
    link_id          TEXT PRIMARY KEY,
    source_ref       TEXT NOT NULL,
    source_table     TEXT NOT NULL DEFAULT '',
    source_id        TEXT NOT NULL DEFAULT '',
    target_ref       TEXT NOT NULL,
    target_table     TEXT NOT NULL DEFAULT '',
    target_id        TEXT NOT NULL DEFAULT '',
    link_type        TEXT NOT NULL DEFAULT 'relates_to',
    link_state       TEXT NOT NULL DEFAULT 'declared',
    risk_level       TEXT NOT NULL DEFAULT 'normal',
    title            TEXT NOT NULL DEFAULT '',
    metadata_json    TEXT NOT NULL DEFAULT '{}',
    provenance_json  TEXT NOT NULL DEFAULT '{}',
    created_by       TEXT NOT NULL DEFAULT '',
    request_id       TEXT NOT NULL DEFAULT '',
    created_at       TEXT DEFAULT (datetime('now')),
    updated_at       TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_personal_graph_links_source
    ON personal_graph_links(source_ref, link_type, link_state);
CREATE INDEX IF NOT EXISTS idx_personal_graph_links_target
    ON personal_graph_links(target_ref, link_type, link_state);
CREATE INDEX IF NOT EXISTS idx_personal_graph_links_type
    ON personal_graph_links(link_type, link_state, updated_at);

CREATE TABLE IF NOT EXISTS pve_hosts (
    pve_id        TEXT PRIMARY KEY,   -- IP address — stable, no external config needed
    ip_address    TEXT NOT NULL,
    hostname      TEXT,               -- parsed from Proxmox web response
    pve_name      TEXT,               -- short label e.g. "pve1" (user-editable)
    version       TEXT,               -- Proxmox version string
    port          INTEGER DEFAULT 8006,
    ssh_reachable INTEGER DEFAULT 0,  -- updated by proxmox-config probe after SSH attempt
    last_scanned  TEXT,
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS arp_manual (
    entry_id    TEXT PRIMARY KEY,   -- UUID
    ip_address  TEXT NOT NULL,
    mac_address TEXT NOT NULL,
    notes       TEXT,               -- optional human label
    created_at  TEXT DEFAULT (datetime('now')),
    updated_at  TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_arp_manual_mac ON arp_manual(mac_address);
CREATE INDEX IF NOT EXISTS idx_arp_manual_ip  ON arp_manual(ip_address);

CREATE TABLE IF NOT EXISTS ssh_targets (
    ip_address   TEXT PRIMARY KEY,   -- target IP (authoritative lookup key)
    key_env_var  TEXT NOT NULL,       -- VM_SSH_KEY | LXC_SSH_KEY | XARTA_NODE_SSH_KEY | CITADEL_SSH_KEY | PROXMOX_SSH_KEY | PFSENSE_SSH_KEY
    source_ip    TEXT,                -- local bind IP (same VLAN as target)
    host_name    TEXT,                -- friendly label (from proxmox_config.name or env)
    host_type    TEXT,                -- lxc-fleet | lxc | qemu | citadel | pve | pfsense
    notes        TEXT,
    created_at   TEXT DEFAULT (datetime('now')),
    updated_at   TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_ssh_targets_host_type ON ssh_targets(host_type);

CREATE TABLE IF NOT EXISTS manual_links (
    link_id         TEXT PRIMARY KEY,   -- UUID
    -- Network addresses (nullable; at least one pair is expected)
    vlan_ip         TEXT,               -- VLAN IP address including port, e.g. 10.0.42.5:8080
    vlan_uri        TEXT,               -- VLAN hostname URI including port
    tailnet_ip      TEXT,               -- Tailnet IP including port
    tailnet_uri     TEXT,               -- Tailnet host.domain address including port
    -- Display
    label           TEXT,               -- short description / display name
    icon            TEXT,               -- favicon URL, SVG data, or emoji
    -- Grouping / hierarchy
    group_name      TEXT,               -- optional group label for clustering
    parent_id       TEXT,               -- optional FK to another link_id (for nesting)
    sort_order      INTEGER DEFAULT 0,  -- ordering within a group
    -- Host context
    pve_host        TEXT,               -- Proxmox host name if applicable
    is_internet     INTEGER DEFAULT 0,  -- 1 = internet-facing service
    vm_id           TEXT,               -- QEMU VM ID if applicable
    vm_name         TEXT,               -- VM arbitrary name
    lxc_id          TEXT,               -- LXC container ID
    lxc_name        TEXT,               -- LXC arbitrary name
    -- Notes
    notes           TEXT,
    created_at  TEXT DEFAULT (datetime('now')),
    updated_at  TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_manual_links_group  ON manual_links(group_name);
CREATE INDEX IF NOT EXISTS idx_manual_links_parent ON manual_links(parent_id);
CREATE INDEX IF NOT EXISTS idx_manual_links_sort   ON manual_links(sort_order);

CREATE TABLE IF NOT EXISTS manual_link_categories (
    category_id        TEXT PRIMARY KEY,
    label              TEXT NOT NULL,
    icon               TEXT,
    parent_category_id TEXT,
    sort_order         INTEGER DEFAULT 0,
    is_page            INTEGER DEFAULT 0,
    page_label         TEXT,
    page_sort_order    INTEGER DEFAULT 0,
    show_panel         INTEGER DEFAULT 0,
    panel_color        TEXT,
    panel_background   TEXT,
    notes              TEXT,
    created_at         TEXT DEFAULT (datetime('now')),
    updated_at         TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_manual_link_categories_parent
    ON manual_link_categories(parent_category_id, sort_order, label);

CREATE TABLE IF NOT EXISTS manual_link_category_items (
    mapping_id  TEXT PRIMARY KEY,
    category_id TEXT NOT NULL,
    link_id     TEXT NOT NULL,
    parent_mapping_id TEXT,
    sort_order  INTEGER DEFAULT 0,
    label_override TEXT,
    notes       TEXT,
    created_at  TEXT DEFAULT (datetime('now')),
    updated_at  TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_manual_link_category_items_category
    ON manual_link_category_items(category_id, sort_order);
CREATE INDEX IF NOT EXISTS idx_manual_link_category_items_link
    ON manual_link_category_items(link_id);

CREATE TABLE IF NOT EXISTS doc_groups (
    group_id    TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    sort_order  INTEGER DEFAULT 0,
    created_at  TEXT DEFAULT (datetime('now')),
    updated_at  TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_doc_groups_sort ON doc_groups(sort_order, name);

CREATE TABLE IF NOT EXISTS docs (
    doc_id      TEXT PRIMARY KEY,   -- UUID
    label       TEXT NOT NULL,      -- display name shown in sidebar
    description TEXT,               -- short subtitle / explanation
    tags        TEXT,               -- comma-separated tags; "menu" = show in docs navbar
    path        TEXT NOT NULL,      -- path relative to REPO_INNER_PATH (e.g. "docs/ASSUMPTIONS.md")
    sort_order  INTEGER DEFAULT 0,  -- ordering within the sidebar / group
    group_id    TEXT REFERENCES doc_groups(group_id),  -- NULL = Undefined Group
    created_at  TEXT DEFAULT (datetime('now')),
    updated_at  TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_docs_sort ON docs(sort_order, label);

CREATE TABLE IF NOT EXISTS doc_images (
    image_id    TEXT PRIMARY KEY,
    filename    TEXT NOT NULL,
    description TEXT,
    file_size   INTEGER,
    created_at  TEXT DEFAULT (datetime('now')),
    updated_at  TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_doc_images_filename ON doc_images(filename);

-- ── AI providers & project assignments ──────────────────────────────────────

CREATE TABLE IF NOT EXISTS ai_providers (
    provider_id  TEXT PRIMARY KEY,          -- UUID
    name         TEXT NOT NULL,             -- human label e.g. "Local GPU LLM"
    base_url     TEXT NOT NULL,             -- LiteLLM-compatible base URL
    api_key      TEXT NOT NULL DEFAULT '',  -- Bearer token; stored in fleet DB (infra-internal only)
    model_name   TEXT NOT NULL,             -- stable alias e.g. "PRIMARY-LOCAL"
    model_type   TEXT NOT NULL,             -- 'embedding' | 'reranker' | 'llm'
    dimensions   INTEGER,                   -- output dimensions (embedding models only)
    enabled      INTEGER DEFAULT 1,
    options      TEXT,                      -- JSON options blob (verify_tls, timeout, no_think_supported…)
    notes        TEXT,
    created_at   TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_ai_providers_type ON ai_providers(model_type);

CREATE TABLE IF NOT EXISTS ai_project_assignments (
    assignment_id TEXT PRIMARY KEY,                  -- UUID
    project_name  TEXT NOT NULL,                     -- e.g. "browser-links"
    provider_id   TEXT NOT NULL,                     -- FK → ai_providers.provider_id
    role          TEXT NOT NULL,                     -- 'embedding' | 'reranker' | 'llm'
    priority      INTEGER DEFAULT 0,                 -- higher = preferred when multiple match
    enabled       INTEGER DEFAULT 1,
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_ai_assignments_project ON ai_project_assignments(project_name, role);

-- ── Browser links (SQLite canonical store) ─────────────────────────────────

CREATE TABLE IF NOT EXISTS bookmarks (
    bookmark_id      TEXT PRIMARY KEY,
    url              TEXT NOT NULL,
    normalized_url   TEXT NOT NULL,
    title            TEXT NOT NULL DEFAULT '',
    description      TEXT NOT NULL DEFAULT '',
    tags_json        TEXT NOT NULL DEFAULT '[]',
    folder           TEXT NOT NULL DEFAULT '',
    notes            TEXT NOT NULL DEFAULT '',
    favicon_url      TEXT NOT NULL DEFAULT '',
    source           TEXT NOT NULL DEFAULT 'manual',
    archived         INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_bookmarks_norm_url ON bookmarks(normalized_url);
CREATE INDEX IF NOT EXISTS idx_bookmarks_updated  ON bookmarks(updated_at);

CREATE TABLE IF NOT EXISTS visits (
    visit_id         TEXT PRIMARY KEY,
    url              TEXT NOT NULL,
    normalized_url   TEXT NOT NULL,
    domain           TEXT NOT NULL DEFAULT '',
    title            TEXT NOT NULL DEFAULT '',
    source           TEXT NOT NULL DEFAULT 'visit-recorder',
    dwell_seconds    INTEGER,
    bookmark_id      TEXT,
    visited_at       TEXT NOT NULL DEFAULT (datetime('now')),
    created_at       TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_visits_norm_url   ON visits(normalized_url);
CREATE INDEX IF NOT EXISTS idx_visits_visited_at ON visits(visited_at);
CREATE INDEX IF NOT EXISTS idx_visits_updated    ON visits(updated_at);

CREATE TABLE IF NOT EXISTS bookmark_deletions (
    bookmark_id      TEXT PRIMARY KEY,
    deleted_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_bm_deletions_deleted_at ON bookmark_deletions(deleted_at);

CREATE TABLE IF NOT EXISTS visit_events (
    event_id         TEXT PRIMARY KEY,
    normalized_url   TEXT NOT NULL,
    visited_at       TEXT NOT NULL DEFAULT (datetime('now')),
    dwell_seconds    INTEGER,
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_visit_events_norm_url   ON visit_events(normalized_url);
CREATE INDEX IF NOT EXISTS idx_visit_events_visited_at ON visit_events(visited_at);

-- ── CMS-driven navigation items ──────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS nav_items (
    item_id       TEXT PRIMARY KEY,           -- UUID (auto-generated)
    menu_group    TEXT NOT NULL,              -- 'probes', 'synthesis', 'settings'
    item_key      TEXT NOT NULL,              -- the JS 'id' value e.g. 'pfsense-dns'
    label         TEXT NOT NULL,              -- display label e.g. 'pfSense DNS'
    page_label    TEXT,                       -- label shown when item is active tab
    icon_emoji    TEXT,                       -- emoji icon (current system, backward compat)
    icon_asset    TEXT,                       -- relative path to custom icon e.g. 'icons/pfsense.svg'
    sound_asset   TEXT,                       -- relative path to sound e.g. 'sounds/beep.wav'
    parent_key    TEXT,                       -- parent item_key for nesting (NULL = top-level)
    sort_order    INTEGER NOT NULL DEFAULT 0,
    is_fn         INTEGER NOT NULL DEFAULT 0, -- 1 if this is a function item
    fn_key        TEXT,                       -- function registry key e.g. 'bm.add' (read-only ref)
    active_on     TEXT,                       -- JSON array of tab IDs e.g. '["bookmarks-main"]'
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_nav_items_group_key ON nav_items(menu_group, item_key);

-- ── CMS-driven form controls (inputs, selects, toggles, buttons) ─────────────

CREATE TABLE IF NOT EXISTS form_controls (
    control_id    TEXT PRIMARY KEY,       -- UUID (auto-generated)
    control_key   TEXT NOT NULL UNIQUE,   -- unique identifier e.g. 'bookmarks.filter.archived'
    label         TEXT NOT NULL,          -- human-readable name e.g. 'Bookmarks: Archived Toggle'
    control_type  TEXT,                   -- 'input', 'select', 'toggle', 'button', 'checkbox', 'range', 'textarea'
    context       TEXT,                   -- informational: where this control appears
    icon_asset      TEXT,                   -- relative path under assets/ e.g. 'icons/search.svg'
    sound_asset     TEXT,                   -- relative path under assets/ e.g. 'sounds/click.wav' (on/default)
    sound_asset_off TEXT,                   -- sound played when a toggle/checkbox is turned OFF
    notes           TEXT,                   -- free-text human notes
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_form_controls_key ON form_controls(control_key);

-- ── CMS-driven embedded selector menu items (action slots only) ────────────

CREATE TABLE IF NOT EXISTS embed_menu_items (
    item_id       TEXT PRIMARY KEY,          -- UUID (auto-generated)
    item_key      TEXT NOT NULL,             -- selector action key (e.g. 'database-tables')
    label         TEXT NOT NULL,             -- display label
    icon_emoji    TEXT,                      -- fallback emoji/icon glyph
    icon_asset    TEXT,                      -- relative path under assets/ (optional)
    sound_asset   TEXT,                      -- relative path under assets/ (optional)
    page_index    INTEGER NOT NULL DEFAULT 0,
    sort_order    INTEGER NOT NULL DEFAULT 0,
    enabled       INTEGER NOT NULL DEFAULT 1,
    menu_context  TEXT NOT NULL DEFAULT 'embed',  -- embed | fallback-ui | db
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now'))
);
-- composite unique index is created by _migrate_embed_menu_items_composite_unique()
-- which runs after _run_migrations() adds the menu_context column on existing tables

CREATE TABLE IF NOT EXISTS table_layout_catalog (
    table_code   TEXT PRIMARY KEY,
    table_name   TEXT NOT NULL UNIQUE,
    table_meta   TEXT NOT NULL,
    created_at   TEXT DEFAULT (datetime('now')),
    updated_at   TEXT DEFAULT (datetime('now')),
    CHECK(length(table_code) = 2)
);
CREATE INDEX IF NOT EXISTS idx_table_layout_catalog_name
    ON table_layout_catalog(table_name);

CREATE TABLE IF NOT EXISTS table_layouts (
    layout_key     TEXT PRIMARY KEY,
    reserved_code  TEXT NOT NULL,
    user_code      TEXT NOT NULL,
    table_code     TEXT NOT NULL,
    bucket_code    TEXT NOT NULL,
    layout_data    TEXT NOT NULL,
    created_at     TEXT DEFAULT (datetime('now')),
    updated_at     TEXT DEFAULT (datetime('now')),
    CHECK(length(layout_key) = 8),
    CHECK(length(reserved_code) = 2),
    CHECK(length(user_code) = 2),
    CHECK(length(table_code) = 2),
    CHECK(length(bucket_code) = 2)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_table_layouts_parts
    ON table_layouts(reserved_code, user_code, table_code, bucket_code);
CREATE INDEX IF NOT EXISTS idx_table_layouts_lookup
    ON table_layouts(table_code, user_code, bucket_code);

-- ── PocketTTS metadata (PocketTTS-specific, not generic TTS) ───────────────

CREATE TABLE IF NOT EXISTS pockettts_tags (
    tag_id          TEXT PRIMARY KEY,
    slug            TEXT NOT NULL UNIQUE,
    label           TEXT NOT NULL,
    color_hex       TEXT NOT NULL DEFAULT '#5c6ef8',
    parent_tag_id   TEXT,
    sort_order      INTEGER NOT NULL DEFAULT 0,
    is_seed_default INTEGER NOT NULL DEFAULT 0,
    is_active       INTEGER NOT NULL DEFAULT 1,
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_pockettts_tags_parent
    ON pockettts_tags(parent_tag_id, sort_order);

CREATE TABLE IF NOT EXISTS pockettts_voices (
    voice_id          TEXT PRIMARY KEY,
    display_name      TEXT,
    source_type       TEXT NOT NULL DEFAULT 'unknown',
    is_user_uploaded  INTEGER NOT NULL DEFAULT 0,
    is_active         INTEGER NOT NULL DEFAULT 1,
    first_seen_at     TEXT DEFAULT (datetime('now')),
    last_seen_at      TEXT DEFAULT (datetime('now')),
    created_at        TEXT DEFAULT (datetime('now')),
    updated_at        TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_pockettts_voices_source
    ON pockettts_voices(source_type);

CREATE TABLE IF NOT EXISTS pockettts_voice_meta (
    voice_id      TEXT PRIMARY KEY,
    hidden        INTEGER NOT NULL DEFAULT 0,
    note          TEXT,
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS pockettts_voice_tags (
    assignment_id      TEXT PRIMARY KEY,
    voice_id           TEXT NOT NULL,
    tag_id             TEXT NOT NULL,
    assignment_source  TEXT NOT NULL DEFAULT 'user',
    confidence         TEXT,
    created_at         TEXT DEFAULT (datetime('now')),
    updated_at         TEXT DEFAULT (datetime('now')),
    UNIQUE(voice_id, tag_id)
);
CREATE INDEX IF NOT EXISTS idx_pockettts_voice_tags_tag
    ON pockettts_voice_tags(tag_id);
CREATE INDEX IF NOT EXISTS idx_pockettts_voice_tags_voice
    ON pockettts_voice_tags(voice_id);

CREATE TABLE IF NOT EXISTS pockettts_tag_order (
    order_id      TEXT PRIMARY KEY,
    scope_key     TEXT NOT NULL,
    tag_id        TEXT NOT NULL,
    order_index   INTEGER NOT NULL DEFAULT 0,
    created_at    TEXT DEFAULT (datetime('now')),
    updated_at    TEXT DEFAULT (datetime('now')),
    UNIQUE(scope_key, tag_id)
);
CREATE INDEX IF NOT EXISTS idx_pockettts_tag_order_scope
    ON pockettts_tag_order(scope_key, order_index);

-- ── Node-local push-notification events (NOT fleet-synced) ───────────────────
-- Each node maintains its own event log.  Events are pruned after 7 days.
-- Excluded from _ALLOWED_TABLES in routes_sync.py — never replicated to peers.
CREATE TABLE IF NOT EXISTS events (
    event_id      TEXT PRIMARY KEY,
    event_type    TEXT NOT NULL,   -- e.g. "model.changed", "alias.tests.completed"
    severity      TEXT NOT NULL DEFAULT 'info',  -- info | warn | error
    title         TEXT NOT NULL DEFAULT '',
    message       TEXT NOT NULL DEFAULT '',
    source        TEXT NOT NULL DEFAULT 'blueprints-app',
    created_at    REAL NOT NULL,   -- unix epoch float
    payload_json  TEXT DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events(created_at);
CREATE INDEX IF NOT EXISTS idx_events_type       ON events(event_type);
"""

_TABLE_LAYOUT_CATALOG_SEED = [
    (
        "01",
        "settings",
        {
            "display_name": "Settings",
            "sql_table": "settings",
            "table_kind": "table",
            "dom_table_id": "settings-table",
            "tab_id": "settings",
        },
    ),
    (
        "02",
        "docs-images",
        {
            "display_name": "Docs Images",
            "sql_table": "doc_images",
            "table_kind": "table",
            "dom_table_id": "doc-images-table",
            "tab_id": "docs-images",
        },
    ),
    (
        "03",
        "manual-links",
        {
            "display_name": "Manual Links",
            "sql_table": "manual_links",
            "table_kind": "table",
            "dom_table_id": "ml-table",
            "tab_id": "manual-links",
        },
    ),
    (
        "04",
        "services",
        {
            "display_name": "Services",
            "sql_table": "services",
            "table_kind": "table",
            "dom_table_id": "services-table",
            "tab_id": "services",
        },
    ),
    (
        "05",
        "machines",
        {
            "display_name": "Machines",
            "sql_table": "machines",
            "table_kind": "table",
            "dom_table_id": "machines-table",
            "tab_id": "machines",
        },
    ),
    (
        "06",
        "fleet-nodes",
        {
            "display_name": "Fleet Nodes",
            "sql_table": "nodes",
            "table_kind": "table",
            "dom_table_id": "nodes-table",
            "tab_id": "fleet-nodes",
        },
    ),
    (
        "07",
        "node-backups",
        {
            "display_name": "Node Backups",
            "sql_table": None,
            "table_kind": "table",
            "dom_table_id": "backups-table",
            "tab_id": "fleet-nodes",
            "route_path": "/api/v1/backup",
        },
    ),
    (
        "08",
        "pfsense-dns",
        {
            "display_name": "pfSense DNS",
            "sql_table": "pfsense_dns",
            "table_kind": "table",
            "dom_table_id": "dns-table",
            "tab_id": "pfsense-dns",
        },
    ),
    (
        "09",
        "proxmox-config",
        {
            "display_name": "Proxmox Config",
            "sql_table": "proxmox_config",
            "table_kind": "table",
            "dom_table_id": "pve-config-table",
            "tab_id": "proxmox-config",
        },
    ),
    (
        "0A",
        "dockge-stacks",
        {
            "display_name": "Dockge Stacks",
            "sql_table": "dockge_stacks",
            "table_kind": "table",
            "dom_table_id": "dockge-table",
            "tab_id": "dockge-stacks",
        },
    ),
    (
        "0B",
        "caddy-configs",
        {
            "display_name": "Caddy Configs",
            "sql_table": "caddy_configs",
            "table_kind": "table",
            "dom_table_id": "caddy-table",
            "tab_id": "caddy-configs",
        },
    ),
    (
        "0C",
        "pve-hosts",
        {
            "display_name": "PVE Hosts",
            "sql_table": "pve_hosts",
            "table_kind": "table",
            "dom_table_id": "pve-hosts-table",
            "tab_id": "pve-hosts",
        },
    ),
    (
        "0D",
        "vlans",
        {
            "display_name": "VLANs",
            "sql_table": "vlans",
            "table_kind": "table",
            "dom_table_id": "vlans-table",
            "tab_id": "vlans",
        },
    ),
    (
        "0E",
        "arp-manual",
        {
            "display_name": "Manual ARP",
            "sql_table": "arp_manual",
            "table_kind": "table",
            "dom_table_id": "arp-manual-table",
            "tab_id": "arp-manual",
        },
    ),
    (
        "0F",
        "keys-status",
        {
            "display_name": "SSH Keys Status",
            "sql_table": None,
            "table_kind": "table",
            "dom_table_id": "keys-status-table",
            "tab_id": "keys",
        },
    ),
    (
        "10",
        "certs-status",
        {
            "display_name": "Certificates Status",
            "sql_table": None,
            "table_kind": "table",
            "dom_table_id": "certs-status-table",
            "tab_id": "certs",
        },
    ),
    (
        "11",
        "ai-providers",
        {
            "display_name": "AI Providers",
            "sql_table": "ai_providers",
            "table_kind": "table",
            "dom_table_id": "ai-providers-table",
            "tab_id": "ai-providers",
        },
    ),
    (
        "12",
        "ai-project-assignments",
        {
            "display_name": "AI Project Assignments",
            "sql_table": "ai_project_assignments",
            "table_kind": "table",
            "dom_table_id": "ai-assignments-table",
            "tab_id": "ai-providers",
        },
    ),
    (
        "13",
        "ssh-targets",
        {
            "display_name": "SSH Targets",
            "sql_table": "ssh_targets",
            "table_kind": "table",
            "dom_table_id": "ssh-targets-table",
            "tab_id": "ssh-targets",
        },
    ),
    (
        "14",
        "bookmarks",
        {
            "display_name": "Bookmarks",
            "sql_table": "bookmarks",
            "table_kind": "table",
            "dom_table_id": "bm-table",
            "tab_id": "bookmarks-main",
        },
    ),
    (
        "15",
        "visits",
        {
            "display_name": "Visit History",
            "sql_table": "visits",
            "table_kind": "table",
            "dom_table_id": "vis-table",
            "tab_id": "bookmarks-history",
        },
    ),
    (
        "16",
        "nav-items",
        {
            "display_name": "Nav Items",
            "sql_table": "nav_items",
            "table_kind": "table",
            "dom_table_id": "ni-table",
            "tab_id": "nav-items",
        },
    ),
    (
        "17",
        "form-controls",
        {
            "display_name": "Form Controls",
            "sql_table": "form_controls",
            "table_kind": "table",
            "dom_table_id": "fc-table",
            "tab_id": "form-controls",
        },
    ),
    (
        "18",
        "embed-menu",
        {
            "display_name": "Embed Menu",
            "sql_table": "embed_menu_items",
            "table_kind": "table",
            "dom_table_id": "em-table",
            "tab_id": "embed-menu",
        },
    ),
    (
        "19",
        "bookmarks-search",
        {
            "display_name": "Bookmarks Search",
            "sql_table": "bookmarks",
            "table_kind": "table",
            "dom_table_id": "bm-table",
            "tab_id": "bookmarks-main",
        },
    ),
]

_SEED_SQL = """
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('gen',             '0');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('integrity_ok',   'true');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('last_write_at',  datetime('now'));
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('last_write_by',  'system');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('gui_version',    'initial');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('last_primary_node', '');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('last_primary_at',   '');
INSERT OR IGNORE INTO settings (key, value, description) VALUES ('fe.bm_fetch_limit', '50000', 'Max bookmarks fetched for browse/filter in JS client');
INSERT OR IGNORE INTO settings (key, value, description) VALUES ('fe.sound_enabled', 'false', 'Enable sound playback on menu item clicks');
"""

# ── Public API ────────────────────────────────────────────────────────────────


def _migrate_embed_menu_items_composite_unique(conn: sqlite3.Connection) -> None:
    """Replace per-column UNIQUE(item_key) with UNIQUE(item_key, menu_context).

    Existing nodes were created with 'item_key TEXT NOT NULL UNIQUE' which
    prevents the same action key appearing in multiple contexts.  This
    migration detects that and recreates the table with the composite index.

    Idempotent: detected by presence of idx_embed_menu_items_key_ctx.
    """
    idx = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='index' AND name='idx_embed_menu_items_key_ctx'"
    ).fetchone()
    if idx:
        return  # already on new schema

    log.info("migration: recreating embed_menu_items with composite unique(item_key, menu_context)")
    conn.executescript("""
        ALTER TABLE embed_menu_items RENAME TO _embed_menu_items_old;

        CREATE TABLE embed_menu_items (
            item_id       TEXT PRIMARY KEY,
            item_key      TEXT NOT NULL,
            label         TEXT NOT NULL,
            icon_emoji    TEXT,
            icon_asset    TEXT,
            sound_asset   TEXT,
            page_index    INTEGER NOT NULL DEFAULT 0,
            sort_order    INTEGER NOT NULL DEFAULT 0,
            enabled       INTEGER NOT NULL DEFAULT 1,
            menu_context  TEXT NOT NULL DEFAULT 'embed',
            created_at    TEXT DEFAULT (datetime('now')),
            updated_at    TEXT DEFAULT (datetime('now'))
        );

        INSERT INTO embed_menu_items
            SELECT item_id, item_key, label, icon_emoji, icon_asset, sound_asset,
                   page_index, sort_order, enabled,
                   COALESCE(menu_context, 'embed'),
                   created_at, updated_at
            FROM _embed_menu_items_old;

        DROP TABLE _embed_menu_items_old;

        CREATE UNIQUE INDEX idx_embed_menu_items_key_ctx
            ON embed_menu_items(item_key, menu_context);
        CREATE INDEX IF NOT EXISTS idx_embed_menu_items_page_sort
            ON embed_menu_items(menu_context, page_index, sort_order, item_key);
    """)
    log.info("migration: embed_menu_items composite unique applied")


_KANBAN_TABLE_RENAMES = (
    ("work_item_states", "kanban_item_states"),
    ("work_item_priorities", "kanban_item_priorities"),
    ("work_items", "kanban_items"),
    ("work_item_order_edges", "kanban_item_order_edges"),
    ("work_item_links", "kanban_item_links"),
    ("work_blockers", "kanban_blockers"),
    ("work_discussions", "kanban_discussions"),
    ("work_audit_log", "kanban_audit_log"),
)

_KANBAN_REF_REPLACEMENTS = (
    ("work_issues:", "kanban_items:"),
    ("work_todos:", "kanban_items:"),
    ("work_items:", "kanban_items:"),
    ("work_item_links:", "kanban_item_links:"),
    ("work_blockers:", "kanban_blockers:"),
    ("work_discussions:", "kanban_discussions:"),
    ("work_audit_log:", "kanban_audit_log:"),
    ('"table": "work_items"', '"table": "kanban_items"'),
    ('"table":"work_items"', '"table":"kanban_items"'),
    ('"table": "work_issues"', '"table": "kanban_items"'),
    ('"table":"work_issues"', '"table":"kanban_items"'),
    ('"table": "work_todos"', '"table": "kanban_items"'),
    ('"table":"work_todos"', '"table":"kanban_items"'),
    ('"table": "work_blockers"', '"table": "kanban_blockers"'),
    ('"table":"work_blockers"', '"table":"kanban_blockers"'),
    ('"table": "work_discussions"', '"table": "kanban_discussions"'),
    ('"table":"work_discussions"', '"table":"kanban_discussions"'),
    ('"source_table": "work_items"', '"source_table": "kanban_items"'),
    ('"source_table":"work_items"', '"source_table":"kanban_items"'),
    ('"source_table": "work_issues"', '"source_table": "kanban_items"'),
    ('"source_table":"work_issues"', '"source_table":"kanban_items"'),
    ('"source_table": "work_todos"', '"source_table": "kanban_items"'),
    ('"source_table":"work_todos"', '"source_table":"kanban_items"'),
    ('"source_table": "work_blockers"', '"source_table": "kanban_blockers"'),
    ('"source_table":"work_blockers"', '"source_table":"kanban_blockers"'),
    ("xarta.work.search_metadata.v1", "xarta.kanban.search_metadata.v1"),
    ("work-management", "kanban"),
    ("manual-work", "manual-kanban"),
    ("work-todo", "kanban-todo"),
    ("work_todo", "kanban_todo"),
    ("work_issue", "kanban_issue"),
    ("work_blocker", "kanban_blocker"),
    ("work_discussion", "kanban_discussion"),
    ('"kind": "work"', '"kind": "item"'),
    ('"kind":"work"', '"kind":"item"'),
)

_KANBAN_REF_REWRITE_MARKER = "kanban_ref_text_rewrite_2026_06_29"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
    )


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _copy_then_drop_table(conn: sqlite3.Connection, old_table: str, new_table: str) -> None:
    if not _table_exists(conn, old_table):
        return
    old_cols = _table_columns(conn, old_table)
    new_cols = _table_columns(conn, new_table)
    cols = [col for col in new_cols if col in old_cols]
    if cols:
        col_sql = ", ".join(cols)
        conn.execute(
            f"INSERT OR IGNORE INTO {new_table} ({col_sql}) SELECT {col_sql} FROM {old_table}"
        )
    conn.execute(f"DROP TABLE {old_table}")
    log.info("migration: renamed %s into %s", old_table, new_table)


def _migrate_kanban_priority_recommendations_scope(conn: sqlite3.Connection) -> None:
    table = "kanban_priority_recommendations"
    if not _table_exists(conn, table):
        return
    cols = set(_table_columns(conn, table))
    if "scope_id" in cols or "root_item_id" not in cols:
        return

    old_table = "_kanban_priority_recommendations_root_item_old"
    conn.execute(f"DROP TABLE IF EXISTS {old_table}")
    conn.execute(f"ALTER TABLE {table} RENAME TO {old_table}")
    conn.execute(
        """
        CREATE TABLE kanban_priority_recommendations (
            recommendation_id TEXT PRIMARY KEY,
            scope_id          TEXT NOT NULL DEFAULT 'kanban',
            rank              INTEGER NOT NULL DEFAULT 0,
            item_id           TEXT NOT NULL,
            title             TEXT NOT NULL DEFAULT '',
            summary           TEXT NOT NULL DEFAULT '',
            reason            TEXT NOT NULL DEFAULT '',
            priority_id       TEXT NOT NULL DEFAULT 'medium',
            state_id          TEXT NOT NULL DEFAULT '',
            score             REAL NOT NULL DEFAULT 0,
            strategy_version  TEXT NOT NULL DEFAULT 'skill-managed-v1',
            source_surface    TEXT NOT NULL DEFAULT '',
            source_hash       TEXT NOT NULL DEFAULT '',
            metadata_json     TEXT NOT NULL DEFAULT '{}',
            provenance_json   TEXT NOT NULL DEFAULT '{}',
            generated_at      TEXT NOT NULL DEFAULT '',
            created_at        TEXT DEFAULT (datetime('now')),
            updated_at        TEXT DEFAULT (datetime('now')),
            UNIQUE(scope_id, rank)
        )
        """
    )
    conn.execute(
        f"""
        INSERT OR IGNORE INTO {table} (
            recommendation_id, scope_id, rank, item_id, title, summary, reason,
            priority_id, state_id, score, strategy_version, source_surface,
            source_hash, metadata_json, provenance_json, generated_at,
            created_at, updated_at
        )
        SELECT
            recommendation_id,
            COALESCE(NULLIF(root_item_id, ''), 'kanban'),
            rank,
            item_id,
            title,
            summary,
            reason,
            priority_id,
            state_id,
            score,
            strategy_version,
            source_surface,
            source_hash,
            metadata_json,
            provenance_json,
            generated_at,
            created_at,
            updated_at
        FROM {old_table}
        """
    )
    conn.execute(f"DROP TABLE {old_table}")
    log.info("migration: rebuilt kanban_priority_recommendations with scope_id")


def _load_json_dict(value: str | None) -> dict[str, object]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _load_json_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if str(item).strip()]


def _kanban_leaf_state_status(source_status: str | None) -> tuple[str, str]:
    status = (source_status or "open").strip().lower()
    if status == "archived":
        return "todo", "archived"
    if status == "blocked":
        return "blocked", "blocked"
    if status in {"active", "in_progress"}:
        return "doing", "active"
    if status in {"done", "closed", "promoted", "resolved"}:
        return "done", "done"
    return "todo", "open"


def _kanban_leaf_tags(parent_tags_json: str | None, kind: str) -> list[str]:
    tags = [kind, "kanban"]
    parent_tags = _load_json_list(parent_tags_json)
    if "agent-working-out" in parent_tags:
        tags.append("agent-working-out")
    return tags


def _kanban_search_payload(
    *,
    row_id: str,
    kind: str,
    title: str,
    body: str,
    tags: list[str],
    related_refs: list[str],
) -> tuple[str, str, str]:
    search_text = "\n".join(part for part in [title, body, " ".join(tags)] if part)
    vector_key = f"kanban_items:{row_id}"
    metadata = {
        "schema": "xarta.kanban.search_metadata.v1",
        "table": "kanban_items",
        "row_id": row_id,
        "kind": kind,
        "related_refs": related_refs,
        "embedding": {"state": "pending", "ref": "", "model": ""},
        "vector": {"index": "kanban", "key": vector_key, "turbo_vec_ready": True},
    }
    return search_text, json.dumps(metadata, ensure_ascii=True, sort_keys=True), vector_key


def _migrate_leaf_table_to_kanban_items(
    conn: sqlite3.Connection,
    *,
    table: str,
    id_col: str,
    kind: str,
) -> None:
    if not _table_exists(conn, table):
        return
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    for row in rows:
        leaf_id = row[id_col]
        parent_id = row["item_id"]
        parent = conn.execute(
            "SELECT depth, tags_json FROM kanban_items WHERE item_id=?",
            (parent_id,),
        ).fetchone()
        depth = int(parent["depth"]) + 1 if parent and parent["depth"] is not None else 0
        parent_tags_json = parent["tags_json"] if parent else "[]"
        existing = conn.execute(
            "SELECT * FROM kanban_items WHERE item_id=?",
            (leaf_id,),
        ).fetchone()
        state_id, item_status = _kanban_leaf_state_status(row["status"])
        source_status = str(row["status"] or "")
        item_type = "issue" if kind == "issue" and source_status != "promoted" else "item"
        if existing and existing["item_type"] not in {"issue", "todo"}:
            item_type = existing["item_type"]
        tags = _kanban_leaf_tags(parent_tags_json, kind)
        related_tasks = [row["related_task_id"]] if row["related_task_id"] else []
        related_issues = [leaf_id] if kind == "issue" else []
        old_ref = f"{table}:{leaf_id}"
        related_refs = [
            f"kanban_items:{parent_id}",
            *([row["source_ref"]] if "source_ref" in row.keys() and row["source_ref"] else []),
            *([f"personal_time_tasks:{row['related_task_id']}"] if row["related_task_id"] else []),
        ]
        search_text, search_metadata, vector_key = _kanban_search_payload(
            row_id=leaf_id,
            kind=item_type,
            title=row["title"] or "",
            body=row["body_excerpt"] or "",
            tags=tags,
            related_refs=related_refs,
        )
        provenance = _load_json_dict(row["provenance_json"])
        kanban_meta = provenance.get("kanban") if isinstance(provenance.get("kanban"), dict) else {}
        provenance["kanban"] = {
            **kanban_meta,
            "typed_leaf_card": item_type == "issue",
            "leaf_kind": kind,
            "migrated_from_ref": old_ref,
            "parent_item_id": parent_id,
        }
        leaf_meta = provenance.get(kind) if isinstance(provenance.get(kind), dict) else {}
        provenance[kind] = {
            **leaf_meta,
            "item_id": parent_id,
            **({"typed_item_id": leaf_id} if kind == "issue" else {"kanban_item_id": leaf_id}),
            "migrated_from_ref": old_ref,
            "external_source_ref": row["source_ref"] if "source_ref" in row.keys() else "",
            "due_at": row["due_at"] if "due_at" in row.keys() and row["due_at"] else "",
        }
        created_at = row["created_at"] or (existing["created_at"] if existing else None)
        updated_at = row["updated_at"] or (existing["updated_at"] if existing else None)
        source_hash = hashlib.sha256(
            json.dumps(
                {
                    "item_id": leaf_id,
                    "parent_item_id": parent_id,
                    "title": row["title"] or "",
                    "body": row["body_excerpt"] or "",
                    "item_type": item_type,
                    "state_id": state_id,
                    "priority_id": row["priority_id"] or "medium",
                    "status": item_status,
                    "source_ref": f"kanban_items:{leaf_id}",
                    "related_task_ids": related_tasks,
                    "related_issue_ids": related_issues,
                },
                sort_keys=True,
                ensure_ascii=True,
            ).encode("utf-8")
        ).hexdigest()
        conn.execute(
            """
            INSERT INTO kanban_items (
                item_id, parent_item_id, title, body_excerpt, item_type, state_id,
                priority_id, depth, sort_order, status, archived_at, promoted_from_ref,
                source_type, source_ref, source_hash, tags_json, related_event_ids_json,
                related_task_ids_json, related_issue_ids_json, search_text,
                search_metadata_json, embedding_ref, embedding_model, embedding_updated_at,
                vector_index_key, provenance_json, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, '[]', ?, ?, ?,
                    ?, '', '', NULL, ?, ?, COALESCE(?, datetime('now')),
                    COALESCE(?, datetime('now')))
            ON CONFLICT(item_id) DO UPDATE SET
                parent_item_id=excluded.parent_item_id,
                title=excluded.title,
                body_excerpt=excluded.body_excerpt,
                item_type=excluded.item_type,
                state_id=excluded.state_id,
                priority_id=excluded.priority_id,
                depth=excluded.depth,
                status=excluded.status,
                archived_at=excluded.archived_at,
                promoted_from_ref=excluded.promoted_from_ref,
                source_type=excluded.source_type,
                source_ref=excluded.source_ref,
                source_hash=excluded.source_hash,
                tags_json=excluded.tags_json,
                related_task_ids_json=excluded.related_task_ids_json,
                related_issue_ids_json=excluded.related_issue_ids_json,
                search_text=excluded.search_text,
                search_metadata_json=excluded.search_metadata_json,
                vector_index_key=excluded.vector_index_key,
                provenance_json=excluded.provenance_json,
                updated_at=excluded.updated_at
            """,
            (
                leaf_id,
                parent_id,
                row["title"] or "",
                row["body_excerpt"] or "",
                item_type,
                state_id,
                row["priority_id"] or "medium",
                depth,
                item_status,
                row["updated_at"] if item_status == "archived" else None,
                f"kanban_items:{leaf_id}"
                if item_type == "item" and source_status == "promoted"
                else "",
                f"kanban-{kind}",
                f"kanban_items:{leaf_id}",
                source_hash,
                json.dumps(tags, ensure_ascii=True),
                json.dumps(related_tasks, ensure_ascii=True),
                json.dumps(related_issues, ensure_ascii=True),
                search_text,
                search_metadata,
                vector_key,
                json.dumps(provenance, ensure_ascii=True, sort_keys=True),
                created_at,
                updated_at,
            ),
        )
    conn.execute(f"DROP TABLE {table}")
    log.info("migration: folded %d %s rows into kanban_items", len(rows), table)


def _rewrite_kanban_ref_text(conn: sqlite3.Connection) -> None:
    done = conn.execute(
        "SELECT value FROM sync_meta WHERE key=?",
        (_KANBAN_REF_REWRITE_MARKER,),
    ).fetchone()
    if done and done[0] == "complete":
        return

    ref_columns = {
        "kanban_items": (
            "promoted_from_ref",
            "source_type",
            "source_ref",
            "search_metadata_json",
            "vector_index_key",
            "provenance_json",
        ),
        "kanban_item_links": ("metadata_json",),
        "kanban_blockers": (
            "blocked_by_ref",
            "search_metadata_json",
            "vector_index_key",
            "provenance_json",
        ),
        "kanban_discussions": ("search_metadata_json", "vector_index_key", "provenance_json"),
        "kanban_audit_log": ("target_ref", "source_surface", "metadata_json"),
        "personal_graph_links": (
            "source_ref",
            "source_table",
            "source_id",
            "target_ref",
            "target_table",
            "target_id",
            "title",
            "metadata_json",
            "provenance_json",
        ),
        "personal_search_documents": (
            "document_id",
            "record_type",
            "record_table",
            "record_id",
            "source_type",
            "source_ref",
            "search_text",
            "related_refs_json",
            "page_ref_json",
            "source_refs_json",
            "provenance_json",
            "vector_index_key",
        ),
        "personal_time_audit": ("target_ref", "metadata_json"),
        "table_layout_catalog": ("table_name", "table_meta"),
    }
    for table, wanted_columns in ref_columns.items():
        if not _table_exists(conn, table):
            continue
        existing_columns = set(_table_columns(conn, table))
        for column_name in wanted_columns:
            if column_name not in existing_columns:
                continue
            for old, new in _KANBAN_REF_REPLACEMENTS:
                conn.execute(
                    f"UPDATE {table} SET {column_name}=REPLACE({column_name}, ?, ?) "
                    f"WHERE {column_name} LIKE ?",
                    (old, new, f"%{old}%"),
                )
    if _table_exists(conn, "sync_queue"):
        for old, new in _KANBAN_TABLE_RENAMES:
            conn.execute("UPDATE sync_queue SET table_name=? WHERE table_name=?", (new, old))
        conn.execute("DELETE FROM sync_queue WHERE table_name IN ('work_issues', 'work_todos')")
    conn.execute(
        "INSERT OR REPLACE INTO sync_meta(key, value) VALUES (?, 'complete')",
        (_KANBAN_REF_REWRITE_MARKER,),
    )


def _migrate_kanban_storage(conn: sqlite3.Connection) -> None:
    """Move Kanban storage to canonical kanban_* tables and typed item cards."""
    conn.row_factory = sqlite3.Row
    for old_table, new_table in _KANBAN_TABLE_RENAMES:
        _copy_then_drop_table(conn, old_table, new_table)
    if _table_exists(conn, "kanban_items"):
        columns = set(_table_columns(conn, "kanban_items"))
        if "goal_flag" not in columns:
            conn.execute("ALTER TABLE kanban_items ADD COLUMN goal_flag INTEGER NOT NULL DEFAULT 0")
        if "automation_excluded" not in columns:
            conn.execute(
                "ALTER TABLE kanban_items ADD COLUMN automation_excluded INTEGER NOT NULL DEFAULT 0"
            )
    _migrate_leaf_table_to_kanban_items(conn, table="work_issues", id_col="issue_id", kind="issue")
    _migrate_leaf_table_to_kanban_items(conn, table="work_todos", id_col="todo_id", kind="todo")
    if _table_exists(conn, "kanban_items"):
        conn.execute("UPDATE kanban_items SET item_type='item' WHERE item_type='work'")
        conn.execute(
            """
            UPDATE kanban_items
            SET search_metadata_json=json_set(search_metadata_json, '$.kind', 'item')
            WHERE item_type='todo' AND json_valid(search_metadata_json)
            """
        )
        conn.execute(
            """
            UPDATE kanban_items
            SET provenance_json=json_remove(
                json_set(
                    json_set(
                        provenance_json,
                        '$.kanban.typed_leaf_card',
                        json('false')
                    ),
                    '$.todo.kanban_item_id',
                    COALESCE(json_extract(provenance_json, '$.todo.typed_item_id'), item_id)
                ),
                '$.todo.typed_item_id'
            )
            WHERE json_valid(provenance_json)
              AND (
                  item_type='todo'
                  OR json_extract(provenance_json, '$.kanban.leaf_kind')='todo'
              )
            """
        )
        conn.execute("UPDATE kanban_items SET item_type='item' WHERE item_type='todo'")
    _rewrite_kanban_ref_text(conn)


def _migrate_personal_kanban_refs(conn: sqlite3.Connection) -> None:
    """Rename cross-page Kanban reference columns and mode values."""
    conn.row_factory = sqlite3.Row
    for table in ("personal_events", "personal_time_tasks", "personal_git_daily_summaries"):
        if not _table_exists(conn, table):
            continue
        columns = _table_columns(conn, table)
        if "related_work_items_json" in columns and "related_kanban_items_json" not in columns:
            conn.execute(
                f"ALTER TABLE {table} RENAME COLUMN related_work_items_json TO related_kanban_items_json"
            )
        elif "related_work_items_json" in columns and "related_kanban_items_json" in columns:
            conn.execute(
                f"""
                UPDATE {table}
                SET related_kanban_items_json=related_work_items_json
                WHERE COALESCE(related_kanban_items_json, '') IN ('', '[]')
                  AND COALESCE(related_work_items_json, '') NOT IN ('', '[]')
                """
            )
            with suppress(sqlite3.OperationalError):
                conn.execute(f"ALTER TABLE {table} DROP COLUMN related_work_items_json")
        if "related_kanban_items_json" in _table_columns(conn, table):
            conn.execute(
                f"""
                UPDATE {table}
                SET related_kanban_items_json=replace(related_kanban_items_json, '"work:', '"')
                WHERE related_kanban_items_json LIKE '%"work:%'
                """
            )
    for table in ("personal_git_features", "personal_git_kanban_arcs"):
        if not _table_exists(conn, table):
            continue
        columns = _table_columns(conn, table)
        if "related_work_item_id" in columns and "related_kanban_item_id" not in columns:
            conn.execute(
                f"ALTER TABLE {table} RENAME COLUMN related_work_item_id TO related_kanban_item_id"
            )
        elif "related_work_item_id" in columns and "related_kanban_item_id" in columns:
            conn.execute(
                f"""
                UPDATE {table}
                SET related_kanban_item_id=related_work_item_id
                WHERE COALESCE(related_kanban_item_id, '') = ''
                  AND COALESCE(related_work_item_id, '') != ''
                """
            )
            with suppress(sqlite3.OperationalError):
                conn.execute(f"ALTER TABLE {table} DROP COLUMN related_work_item_id")
    if _table_exists(conn, "personal_time_tasks"):
        conn.execute("UPDATE personal_time_tasks SET mode='kanban' WHERE mode='work'")


def _run_migrations(conn: sqlite3.Connection) -> None:
    """Idempotent ALTER TABLE migrations for columns added after initial deploy."""
    _migrate_personal_kanban_refs(conn)
    migrations = [
        ("nodes", "ui_url", "TEXT"),
        # ── Phase-1 schema evolution (2026-03-11) ─────────────────────────
        # services: structured hosting, classification, health, flexibility
        ("services", "host_machine_id", "TEXT"),
        ("services", "service_kind", "TEXT DEFAULT 'app'"),
        ("services", "exposure_level", "TEXT DEFAULT 'internal'"),
        ("services", "health_path", "TEXT"),
        ("services", "health_expected_status", "INTEGER DEFAULT 200"),
        ("services", "runtime_notes_json", "TEXT"),
        # machines: richer type taxonomy, platform, status, extensibility
        ("machines", "machine_kind", "TEXT"),
        ("machines", "platform", "TEXT"),
        ("machines", "status", "TEXT DEFAULT 'active'"),
        ("machines", "labels", "TEXT"),
        ("machines", "properties_json", "TEXT"),
        # nodes: canonical machine mapping
        ("nodes", "machine_id", "TEXT"),
        # pfsense_dns: local ping sweep enrichment (2026-03-12)
        ("pfsense_dns", "ping_ms", "REAL"),
        ("pfsense_dns", "last_ping_check", "TEXT"),
        # proxmox_config: multi-VLAN + service detection (2026-03-12)
        ("proxmox_config", "vlans_json", "TEXT"),
        ("proxmox_config", "has_docker", "INTEGER DEFAULT 0"),
        ("proxmox_config", "dockge_stacks_dir", "TEXT"),
        ("proxmox_config", "has_portainer", "INTEGER DEFAULT 0"),
        ("proxmox_config", "portainer_method", "TEXT"),
        ("proxmox_config", "has_caddy", "INTEGER DEFAULT 0"),
        ("proxmox_config", "caddy_conf_path", "TEXT"),
        # proxmox_config: JSON service paths (2026-03-13)
        ("proxmox_config", "dockge_json", "TEXT"),
        ("proxmox_config", "portainer_json", "TEXT"),
        ("proxmox_config", "caddy_json", "TEXT"),
        # proxmox_nets: per-interface network rows (2026-03-12)
        # (table created in DDL above; no ALTER TABLE needed for it)
        # vlans: VLAN CIDR map (2026-03-12)
        # (table created in DDL above; no ALTER TABLE needed for it)
        # dockge_stacks: parentage + direct SSH metadata (2026-03-13)
        ("dockge_stacks", "vm_type", "TEXT"),
        ("dockge_stacks", "ip_address", "TEXT"),
        ("dockge_stacks", "parent_context", "TEXT"),
        ("dockge_stacks", "parent_stack_name", "TEXT"),
        # dockge_stacks: user-managed fields — obsolete flag + notes (2026-03-13)
        ("dockge_stacks", "obsolete", "INTEGER DEFAULT 0"),
        ("dockge_stacks", "notes", "TEXT"),
        # dockge_stack_services: relational per-container rows (2026-03-13)
        # (table created in DDL above; no ALTER TABLE needed for it)
        # pve_hosts: tailnet IP discovered during scan (2026-03-15)
        ("pve_hosts", "tailnet_ip", "TEXT"),
        # nodes: display order from .nodes.json (2026-03-15)
        ("nodes", "display_order", "INTEGER DEFAULT 0"),
        # nodes: HTTPS hostnames from .nodes.json (2026-03-15)
        ("nodes", "primary_hostname", "TEXT"),
        ("nodes", "tailnet_hostname", "TEXT"),
        # manual_links: physical/logical location label (2026-03-16)
        ("manual_links", "location", "TEXT"),
        # doc_images: user-defined tags for filtering (2026-03-17)
        ("doc_images", "tags", "TEXT"),
        # docs: group assignment (2026-03-17)
        ("docs", "group_id", "TEXT"),
        # sync_queue: GUID for dedup + forwarding (Phase 2, 2026-03-19)
        ("sync_queue", "guid", "TEXT DEFAULT ''"),
        # sync_queue: completion time for sent-row retention (2026-07-01)
        ("sync_queue", "sent_at", "TEXT DEFAULT ''"),
        # visits: count of times a URL has been visited (2026-03-21)
        ("visits", "visit_count", "INTEGER NOT NULL DEFAULT 1"),
        # form_controls: separate off-state sound for toggles/checkboxes (2026-03-24)
        ("form_controls", "sound_asset_off", "TEXT"),
        # embed_menu_items: multi-context support — embed / fallback-ui / db (2026-04-07)
        ("embed_menu_items", "menu_context", "TEXT NOT NULL DEFAULT 'embed'"),
        # manual_link_category_items: mapping-level hierarchy for Page 4 positions (2026-05-17)
        ("manual_link_category_items", "parent_mapping_id", "TEXT"),
        # manual_link_categories: optional Interface panel presentation (2026-05-17)
        ("manual_link_categories", "show_panel", "INTEGER DEFAULT 0"),
        ("manual_link_categories", "panel_color", "TEXT"),
        ("manual_link_categories", "panel_background", "TEXT"),
        # manual_link_categories: optional Manual Links page roots (2026-05-18)
        ("manual_link_categories", "is_page", "INTEGER DEFAULT 0"),
        ("manual_link_categories", "page_label", "TEXT"),
        ("manual_link_categories", "page_sort_order", "INTEGER DEFAULT 0"),
    ]
    existing_cols: dict[str, set[str]] = {}
    for table, column, col_type in migrations:
        if table not in existing_cols:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            existing_cols[table] = {r[1] for r in rows}
        if column not in existing_cols[table]:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            log.info("migration: added column %s.%s", table, column)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_manual_link_category_items_parent "
        "ON manual_link_category_items(parent_mapping_id)"
    )


def _backfill_manual_link_category_item_parents(conn: sqlite3.Connection) -> None:
    marker = "manual_link_category_item_parent_backfill_2026_05_17"
    done = conn.execute("SELECT value FROM sync_meta WHERE key=?", (marker,)).fetchone()
    if done:
        return
    rows = conn.execute(
        """
        SELECT m.mapping_id, m.category_id, l.parent_id
        FROM manual_link_category_items m
        JOIN manual_links l ON l.link_id = m.link_id
        WHERE m.parent_mapping_id IS NULL AND l.parent_id IS NOT NULL
        """
    ).fetchall()
    for row in rows:
        parent = conn.execute(
            """
            SELECT mapping_id FROM manual_link_category_items
            WHERE category_id=? AND link_id=?
            ORDER BY sort_order, mapping_id
            LIMIT 1
            """,
            (row[1], row[2]),
        ).fetchone()
        if parent:
            conn.execute(
                "UPDATE manual_link_category_items SET parent_mapping_id=? WHERE mapping_id=?",
                (parent[0], row[0]),
            )
    conn.execute(
        "INSERT OR REPLACE INTO sync_meta (key, value) VALUES (?, datetime('now'))", (marker,)
    )


def _backfill_visit_events(conn: sqlite3.Connection) -> None:
    """One-time backfill: seed visit_events from existing visits rows.

    For each visits row that has no corresponding event_id in visit_events,
    insert one event row using the visits row's visited_at.  This preserves
    the most-recent timestamp that survived the dedup run.  Idempotent — safe
    to call on every startup; skips rows that already have an event.
    """
    import uuid as _uuid

    rows = conn.execute(
        "SELECT v.normalized_url, v.visited_at "
        "FROM visits v "
        "WHERE NOT EXISTS ("
        "  SELECT 1 FROM visit_events e WHERE e.normalized_url = v.normalized_url"
        ")"
    ).fetchall()
    for row in rows:
        conn.execute(
            "INSERT OR IGNORE INTO visit_events (event_id, normalized_url, visited_at) VALUES (?,?,?)",
            (str(_uuid.uuid4()), row[0], row[1]),
        )
    if rows:
        log.info("_backfill_visit_events: seeded %d event row(s)", len(rows))


def _canonicalize_visit_history_urls(conn: sqlite3.Connection) -> None:
    """Apply current URL identity rules to stored visit history rows.

    This is intentionally scoped to history, not the whole bookmarks table:
    visits are an audit/use-frequency surface where cache-busting parameters
    should not fragment the logical page.  The follow-up dedup pass keeps the
    newest visit row if multiple rows collapse to the same identity.
    """
    visit_rows = conn.execute("SELECT visit_id, normalized_url FROM visits").fetchall()
    changed_visits = 0
    for row in visit_rows:
        canonical = normalize_url_identity(row[1])
        if canonical != row[1]:
            conn.execute(
                "UPDATE visits SET normalized_url=?, updated_at=datetime('now') WHERE visit_id=?",
                (canonical, row[0]),
            )
            changed_visits += 1

    event_rows = conn.execute("SELECT event_id, normalized_url FROM visit_events").fetchall()
    changed_events = 0
    for row in event_rows:
        canonical = normalize_url_identity(row[1])
        if canonical != row[1]:
            conn.execute(
                "UPDATE visit_events SET normalized_url=? WHERE event_id=?",
                (canonical, row[0]),
            )
            changed_events += 1

    if changed_visits or changed_events:
        log.info(
            "_canonicalize_visit_history_urls: updated %d visit row(s), %d event row(s)",
            changed_visits,
            changed_events,
        )


def _dedup_visits(conn: sqlite3.Connection) -> None:
    """Collapse existing duplicate visit rows by normalized_url.

    On each node, the visit-recorder may have inserted many rows for the same
    URL before the upsert logic was introduced.  This migration is idempotent:
    for each normalized_url with more than one row it keeps the most-recently-
    visited row, sets its visit_count to the total number of rows for that URL,
    and deletes the rest.
    """
    dups = conn.execute(
        "SELECT normalized_url, COUNT(*) as cnt FROM visits GROUP BY normalized_url HAVING cnt > 1"
    ).fetchall()
    if not dups:
        return
    collapsed = 0
    for dup in dups:
        nurl = dup[0]
        agg = conn.execute(
            "SELECT MAX(visited_at) as latest, SUM(visit_count) as total "
            "FROM visits WHERE normalized_url=?",
            (nurl,),
        ).fetchone()
        keeper = conn.execute(
            "SELECT visit_id FROM visits WHERE normalized_url=? ORDER BY visited_at DESC LIMIT 1",
            (nurl,),
        ).fetchone()
        conn.execute(
            "UPDATE visits SET visit_count=?, visited_at=?, updated_at=datetime('now') "
            "WHERE visit_id=?",
            (agg[1], agg[0], keeper[0]),
        )
        conn.execute(
            "DELETE FROM visits WHERE normalized_url=? AND visit_id != ?",
            (nurl, keeper[0]),
        )
        collapsed += 1
    log.info("_dedup_visits: collapsed duplicate rows for %d URLs", collapsed)


def _seed_vlans_from_proxmox_nets(conn: sqlite3.Connection) -> None:
    """
    Idempotent: ensure every vlan_tag in proxmox_nets has a row in vlans.
    Runs at startup so the vlans table survives a full-DB restore from a peer
    that had proxmox_nets data but not vlans.
    Also infers /24 CIDRs from known IPs and applies VLAN_CIDRS env var.
    """
    import ipaddress as _ip

    conn.execute(
        """
        INSERT OR IGNORE INTO vlans (vlan_id, cidr_inferred)
        SELECT DISTINCT vlan_tag, 0
        FROM proxmox_nets
        WHERE vlan_tag IS NOT NULL
        """
    )
    seeded = conn.execute("SELECT changes()").fetchone()[0]
    if seeded:
        log.info("startup: seeded %d vlans row(s) from proxmox_nets", seeded)

    # Infer /24 CIDR from first real IP seen for each vlan that still has no CIDR
    no_cidr = conn.execute("SELECT vlan_id FROM vlans WHERE cidr IS NULL OR cidr = ''").fetchall()
    for row in no_cidr:
        vid = row[0]
        ip_row = conn.execute(
            """SELECT ip_address FROM proxmox_nets
               WHERE vlan_tag=? AND ip_address IS NOT NULL AND ip_address != ''
                 AND ip_address != 'dhcp'
               LIMIT 1""",
            (vid,),
        ).fetchone()
        if not ip_row:
            continue
        try:
            net = _ip.ip_network(f"{ip_row[0]}/24", strict=False)
            cidr = str(net)
            conn.execute(
                "UPDATE vlans SET cidr=?, cidr_inferred=1 WHERE vlan_id=? AND (cidr IS NULL OR cidr='')",
                (cidr, vid),
            )
        except ValueError:
            pass

    # Apply VLAN_CIDRS env var to fill in or override any remaining NULL cidrs
    vlan_cidrs_raw = os.environ.get("VLAN_CIDRS", "")
    if vlan_cidrs_raw:
        for part in vlan_cidrs_raw.split(","):
            part = part.strip()
            if ":" not in part:
                continue
            tag_str, cidr = part.split(":", 1)
            try:
                tag = int(tag_str.strip())
            except ValueError:
                continue
            cidr = cidr.strip()
            conn.execute(
                "UPDATE vlans SET cidr=?, cidr_inferred=1 WHERE vlan_id=? AND (cidr IS NULL OR cidr='')",
                (cidr, tag),
            )


def _seed_table_layout_catalog(conn: sqlite3.Connection) -> None:
    """Ensure the table layout catalog covers the current GUI table surfaces."""
    for table_code, table_name, meta in _TABLE_LAYOUT_CATALOG_SEED:
        conn.execute(
            """
            INSERT OR IGNORE INTO table_layout_catalog (table_code, table_name, table_meta)
            VALUES (?, ?, ?)
            """,
            (table_code, table_name, json.dumps(meta, ensure_ascii=True, sort_keys=True)),
        )


def _seed_manual_links_ai_assignment(conn: sqlite3.Connection) -> None:
    """Ensure Manual Links URL intake has a DB-backed local LLM assignment.

    This is intentionally insert-only. If the DB already has any enabled
    manual-links LLM assignment, that database value wins.
    """
    existing = conn.execute(
        """
        SELECT assignment_id
        FROM ai_project_assignments
        WHERE project_name='manual-links'
          AND role='llm'
          AND enabled=1
        LIMIT 1
        """
    ).fetchone()
    if existing:
        return
    provider = conn.execute(
        """
        SELECT provider_id
        FROM ai_providers
        WHERE model_name='PRIMARY-LOCAL-NO-THINK-PRIVATE'
          AND model_type='llm'
          AND enabled=1
        LIMIT 1
        """
    ).fetchone()
    if not provider:
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO ai_project_assignments
            (assignment_id, project_name, provider_id, role, priority, enabled)
        VALUES ('manual-links-llm-primary-local-no-think-private',
                'manual-links', ?, 'llm', 0, 1)
        """,
        (provider[0],),
    )


def _seed_personal_search_ai_assignments(conn: sqlite3.Connection) -> None:
    """Ensure Personal Time Activity search has DB-backed embedding/reranker routes."""
    for role, model_name in (
        ("embedding", "EMBEDDINGS-LOCAL"),
        ("reranker", "RERANKER-LOCAL"),
    ):
        existing = conn.execute(
            """
            SELECT assignment_id
            FROM ai_project_assignments
            WHERE project_name='personal-time-activity'
              AND role=?
              AND enabled=1
            LIMIT 1
            """,
            (role,),
        ).fetchone()
        if existing:
            continue
        provider = conn.execute(
            """
            SELECT provider_id
            FROM ai_providers
            WHERE model_name=?
              AND model_type=?
              AND enabled=1
            LIMIT 1
            """,
            (model_name, role),
        ).fetchone()
        if not provider:
            continue
        conn.execute(
            """
            INSERT OR IGNORE INTO ai_project_assignments
                (assignment_id, project_name, provider_id, role, priority, enabled)
            VALUES (?, 'personal-time-activity', ?, ?, 0, 1)
            """,
            (f"personal-time-activity-{role}-{model_name.lower()}", provider[0], role),
        )


def _manual_link_category_id(label: str) -> str:
    slug = "".join(ch.lower() if ch.isalnum() else "-" for ch in label.strip())
    slug = "-".join(part for part in slug.split("-") if part)
    return f"group:{slug or 'uncategorized'}"


def _manual_link_icon_for_label(label: str) -> str:
    value = (label or "").lower()
    if "proxmox" in value:
        return "icons/proxmox-logo-stacked-color.svg"
    if "pfsense" in value:
        return "icons/pfSense.svg"
    return "icons/hieroglyphs/eye-of-horus-blue.svg"


def _seed_manual_link_categories_from_groups(conn: sqlite3.Connection) -> None:
    """One-time backfill of deterministic Interface categories from legacy groups."""
    marker = "manual_link_categories_group_seed_2026_05_17"
    if conn.execute("SELECT 1 FROM sync_meta WHERE key=?", (marker,)).fetchone():
        return

    existing = conn.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM manual_link_categories) AS categories,
            (SELECT COUNT(*) FROM manual_link_category_items) AS items
        """
    ).fetchone()
    if existing and (existing[0] or existing[1]):
        conn.execute(
            "INSERT OR REPLACE INTO sync_meta(key, value) VALUES (?, 'skipped_existing_mappings')",
            (marker,),
        )
        return

    rows = conn.execute(
        """
        SELECT link_id, COALESCE(NULLIF(TRIM(group_name), ''), 'Uncategorized') AS group_label,
               COALESCE(sort_order, 0) AS sort_order
        FROM manual_links
        ORDER BY group_label, sort_order, label
        """
    ).fetchall()
    category_order: dict[str, int] = {}
    for row in rows:
        link_id, label, sort_order = row
        category_id = _manual_link_category_id(label)
        if category_id not in category_order:
            category_order[category_id] = len(category_order)
        conn.execute(
            """
            INSERT OR IGNORE INTO manual_link_categories
                (category_id, label, icon, parent_category_id, sort_order, notes)
            VALUES (?, ?, NULL, NULL, ?, 'Seeded from manual_links.group_name')
            """,
            (category_id, label, category_order[category_id]),
        )
        conn.execute(
            """
            UPDATE manual_link_categories
            SET icon = COALESCE(icon, ?)
            WHERE category_id=?
            """,
            (_manual_link_icon_for_label(label), category_id),
        )
        conn.execute(
            """
            UPDATE manual_links
            SET icon = COALESCE(icon, ?)
            WHERE link_id=?
            """,
            (_manual_link_icon_for_label(label), link_id),
        )
        mapping_id = f"{category_id}:{link_id}"
        conn.execute(
            """
            INSERT OR IGNORE INTO manual_link_category_items
                (mapping_id, category_id, link_id, sort_order, label_override, notes)
            VALUES (?, ?, ?, ?, NULL, 'Seeded from manual_links.group_name')
            """,
            (mapping_id, category_id, link_id, sort_order),
        )
    for row in rows:
        link_id, label, _sort_order = row
        parent = conn.execute(
            "SELECT parent_id FROM manual_links WHERE link_id=?", (link_id,)
        ).fetchone()
        if not parent or not parent[0]:
            continue
        category_id = _manual_link_category_id(label)
        parent_mapping_id = f"{category_id}:{parent[0]}"
        mapping_id = f"{category_id}:{link_id}"
        conn.execute(
            """
            UPDATE manual_link_category_items
            SET parent_mapping_id = COALESCE(parent_mapping_id, ?)
            WHERE mapping_id=?
              AND EXISTS (SELECT 1 FROM manual_link_category_items WHERE mapping_id=?)
            """,
            (parent_mapping_id, mapping_id, parent_mapping_id),
        )
    conn.execute(
        "INSERT OR REPLACE INTO sync_meta(key, value) VALUES (?, 'seeded')",
        (marker,),
    )


def _repair_failed_kanban_marker_processed_hashes(conn: sqlite3.Connection) -> None:
    if not _table_exists(conn, "kanban_review_processor_markers"):
        return
    cur = conn.execute(
        """
        UPDATE kanban_review_processor_markers
        SET processed_document_updated_at='',
            processed_source_hash='',
            processed_at=''
        WHERE status='failed'
          AND COALESCE(processed_source_hash, '') != ''
          AND processed_source_hash=document_source_hash
        """
    )
    if cur.rowcount and cur.rowcount > 0:
        log.info(
            "migration: cleared legacy processed hashes from %s failed kanban marker(s)",
            cur.rowcount,
        )


def init_db() -> None:
    """Create schema, run migrations, and seed sync_meta on first use."""
    os.makedirs(cfg.DB_DIR, exist_ok=True)
    with sqlite3.connect(cfg.DB_PATH) as conn:
        conn.executescript(_SCHEMA_SQL)
        conn.executescript(_SEED_SQL)
        _run_migrations(conn)
        _migrate_embed_menu_items_composite_unique(conn)
        _backfill_manual_link_category_item_parents(conn)
        _canonicalize_visit_history_urls(conn)
        _dedup_visits(conn)
        _backfill_visit_events(conn)
        _seed_vlans_from_proxmox_nets(conn)
        _seed_table_layout_catalog(conn)
        _seed_manual_links_ai_assignment(conn)
        _seed_personal_search_ai_assignments(conn)
        _seed_manual_link_categories_from_groups(conn)
        _repair_failed_kanban_marker_processed_hashes(conn)
    log.info("database initialised at %s", cfg.DB_PATH)


def check_integrity() -> bool:
    """
    Run PRAGMA integrity_check against the main DB.
    Persists the result into sync_meta['integrity_ok'].
    Returns True if the DB is healthy.
    """
    try:
        with sqlite3.connect(cfg.DB_PATH) as conn:
            row = conn.execute("PRAGMA integrity_check").fetchone()
        ok = bool(row and row[0] == "ok")
    except Exception:
        log.exception("integrity_check failed with exception")
        ok = False

    flag = "true" if ok else "false"
    with get_conn() as conn:
        conn.execute("UPDATE sync_meta SET value=? WHERE key='integrity_ok'", (flag,))
    if not ok:
        log.error("DB integrity check FAILED — node will NOT sync out to peers")
    return ok


@contextmanager
def get_conn() -> Generator[sqlite3.Connection, None, None]:
    """
    Yield a WAL-mode SQLite connection with row_factory set.
    Commits on clean exit; rolls back on exception.
    """
    start_perf_ns = time.perf_counter_ns()
    start_time_ns = time.time_ns()
    conn = sqlite3.connect(cfg.DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    ok = True
    error_type = ""
    commit_start_ns = 0
    commit_end_ns = 0
    try:
        yield conn
        commit_start_ns = time.perf_counter_ns()
        conn.commit()
        commit_end_ns = time.perf_counter_ns()
    except Exception:
        ok = False
        error_type = "rollback"
        conn.rollback()
        raise
    finally:
        conn.close()
        timing.record_span(
            "sqlite_connection",
            start_perf_ns=start_perf_ns,
            end_perf_ns=time.perf_counter_ns(),
            start_time_ns=start_time_ns,
            end_time_ns=time.time_ns(),
            db_path=cfg.DB_PATH,
            ok=ok,
            error_type=error_type,
            commit_start_perf_ns=commit_start_ns,
            commit_end_perf_ns=commit_end_ns,
            commit_ms=round(max(0, commit_end_ns - commit_start_ns) / 1_000_000, 3)
            if commit_start_ns and commit_end_ns
            else None,
        )


def increment_gen(conn: sqlite3.Connection, source: str = "human") -> int:
    """
    Atomically increment the generation counter within an open transaction.
    Call this inside any write that should be replicated to peers.
    Returns the new gen value.
    """
    conn.execute(
        "UPDATE sync_meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = 'gen'"
    )
    conn.execute("UPDATE sync_meta SET value=datetime('now') WHERE key='last_write_at'")
    conn.execute("UPDATE sync_meta SET value=? WHERE key='last_write_by'", (source,))
    row = conn.execute("SELECT CAST(value AS INTEGER) FROM sync_meta WHERE key='gen'").fetchone()
    return int(row[0]) if row else 0


# ── Settings helpers ─────────────────────────────────────────────────────────


def get_setting(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    """Return the current value for *key*, or *default* if not set."""
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(
    conn: sqlite3.Connection, key: str, value: str, description: str | None = None
) -> None:
    """Upsert a setting.  Preserves existing description when none supplied."""
    conn.execute(
        """
        INSERT INTO settings (key, value, description, updated_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET
            value       = excluded.value,
            description = COALESCE(excluded.description, settings.description),
            updated_at  = datetime('now')
        """,
        (key, value, description),
    )


def get_setting_or_raise(conn: sqlite3.Connection, key: str, hint: str = "") -> str:
    """Return the value for *key* or raise ValueError with a helpful message."""
    val = get_setting(conn, key)
    if not val:
        extra = f" — {hint}" if hint else ""
        raise ValueError(f"Setting '{key}' is not configured{extra}")
    return val


def get_gen(conn: sqlite3.Connection) -> int:
    """Return current generation counter (read-only)."""
    row = conn.execute("SELECT CAST(value AS INTEGER) FROM sync_meta WHERE key='gen'").fetchone()
    return int(row[0]) if row else 0


def get_meta(conn: sqlite3.Connection, key: str) -> str:
    """Return a sync_meta value by key, or empty string if missing."""
    row = conn.execute("SELECT value FROM sync_meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else ""
