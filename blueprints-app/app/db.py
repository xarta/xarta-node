"""
db.py — SQLite initialisation, schema, integrity check, and connection helper.

WAL mode + synchronous=NORMAL gives crash-safe atomic commits with good
read concurrency. The generation counter in sync_meta provides a total
ordering across all committed writes — critical for the sync engine.
"""

import logging
import os
import sqlite3
from contextlib import contextmanager
from typing import Generator

from . import config as cfg

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
    sent           INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_sync_queue_target
    ON sync_queue(target_node_id, sent, queue_id);

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

CREATE TABLE IF NOT EXISTS docs (
    doc_id      TEXT PRIMARY KEY,   -- UUID
    label       TEXT NOT NULL,      -- display name shown in sidebar
    description TEXT,               -- short subtitle / explanation
    tags        TEXT,               -- comma-separated tags; "menu" = show in docs navbar
    path        TEXT NOT NULL,      -- path relative to REPO_INNER_PATH (e.g. "docs/ASSUMPTIONS.md")
    sort_order  INTEGER DEFAULT 0,  -- ordering within the sidebar
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
"""

_SEED_SQL = """
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('gen',             '0');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('integrity_ok',   'true');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('last_write_at',  datetime('now'));
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('last_write_by',  'system');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('gui_version',    'initial');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('last_primary_node', '');
INSERT OR IGNORE INTO sync_meta (key, value) VALUES ('last_primary_at',   '');
"""


# ── Public API ────────────────────────────────────────────────────────────────

def _run_migrations(conn: sqlite3.Connection) -> None:
    """Idempotent ALTER TABLE migrations for columns added after initial deploy."""
    migrations = [
        ("nodes",    "ui_url",                "TEXT"),
        # ── Phase-1 schema evolution (2026-03-11) ─────────────────────────
        # services: structured hosting, classification, health, flexibility
        ("services", "host_machine_id",       "TEXT"),
        ("services", "service_kind",          "TEXT DEFAULT 'app'"),
        ("services", "exposure_level",        "TEXT DEFAULT 'internal'"),
        ("services", "health_path",           "TEXT"),
        ("services", "health_expected_status", "INTEGER DEFAULT 200"),
        ("services", "runtime_notes_json",    "TEXT"),
        # machines: richer type taxonomy, platform, status, extensibility
        ("machines", "machine_kind",          "TEXT"),
        ("machines", "platform",              "TEXT"),
        ("machines", "status",                "TEXT DEFAULT 'active'"),
        ("machines", "labels",                "TEXT"),
        ("machines", "properties_json",       "TEXT"),
        # nodes: canonical machine mapping
        ("nodes",    "machine_id",            "TEXT"),
        # pfsense_dns: local ping sweep enrichment (2026-03-12)
        ("pfsense_dns", "ping_ms",                  "REAL"),
        ("pfsense_dns", "last_ping_check",           "TEXT"),
        # proxmox_config: multi-VLAN + service detection (2026-03-12)
        ("proxmox_config", "vlans_json",        "TEXT"),
        ("proxmox_config", "has_docker",        "INTEGER DEFAULT 0"),
        ("proxmox_config", "dockge_stacks_dir", "TEXT"),
        ("proxmox_config", "has_portainer",     "INTEGER DEFAULT 0"),
        ("proxmox_config", "portainer_method",  "TEXT"),
        ("proxmox_config", "has_caddy",         "INTEGER DEFAULT 0"),
        ("proxmox_config", "caddy_conf_path",   "TEXT"),
        # proxmox_config: JSON service paths (2026-03-13)
        ("proxmox_config", "dockge_json",       "TEXT"),
        ("proxmox_config", "portainer_json",    "TEXT"),
        ("proxmox_config", "caddy_json",        "TEXT"),
        # proxmox_nets: per-interface network rows (2026-03-12)
        # (table created in DDL above; no ALTER TABLE needed for it)
        # vlans: VLAN CIDR map (2026-03-12)
        # (table created in DDL above; no ALTER TABLE needed for it)
        # dockge_stacks: parentage + direct SSH metadata (2026-03-13)
        ("dockge_stacks", "vm_type",          "TEXT"),
        ("dockge_stacks", "ip_address",        "TEXT"),
        ("dockge_stacks", "parent_context",    "TEXT"),
        ("dockge_stacks", "parent_stack_name", "TEXT"),
        # dockge_stacks: user-managed fields — obsolete flag + notes (2026-03-13)
        ("dockge_stacks", "obsolete",          "INTEGER DEFAULT 0"),
        ("dockge_stacks", "notes",             "TEXT"),
        # dockge_stack_services: relational per-container rows (2026-03-13)
        # (table created in DDL above; no ALTER TABLE needed for it)
        # pve_hosts: tailnet IP discovered during scan (2026-03-15)
        ("pve_hosts", "tailnet_ip",           "TEXT"),
        # nodes: display order from .nodes.json (2026-03-15)
        ("nodes",    "display_order",          "INTEGER DEFAULT 0"),
        # nodes: HTTPS hostnames from .nodes.json (2026-03-15)
        ("nodes",    "primary_hostname",       "TEXT"),
        ("nodes",    "tailnet_hostname",       "TEXT"),
        # manual_links: physical/logical location label (2026-03-16)
        ("manual_links", "location",           "TEXT"),
        # doc_images: user-defined tags for filtering (2026-03-17)
        ("doc_images",   "tags",               "TEXT"),
    ]
    existing_cols: dict[str, set[str]] = {}
    for table, column, col_type in migrations:
        if table not in existing_cols:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            existing_cols[table] = {r[1] for r in rows}
        if column not in existing_cols[table]:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
            log.info("migration: added column %s.%s", table, column)


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
    no_cidr = conn.execute(
        "SELECT vlan_id FROM vlans WHERE cidr IS NULL OR cidr = ''"
    ).fetchall()
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


def init_db() -> None:
    """Create schema, run migrations, and seed sync_meta on first use."""
    os.makedirs(cfg.DB_DIR, exist_ok=True)
    with sqlite3.connect(cfg.DB_PATH) as conn:
        conn.executescript(_SCHEMA_SQL)
        conn.executescript(_SEED_SQL)
        _run_migrations(conn)
        _seed_vlans_from_proxmox_nets(conn)
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
        conn.execute(
            "UPDATE sync_meta SET value=? WHERE key='integrity_ok'", (flag,)
        )
    if not ok:
        log.error("DB integrity check FAILED — node will NOT sync out to peers")
    return ok


@contextmanager
def get_conn() -> Generator[sqlite3.Connection, None, None]:
    """
    Yield a WAL-mode SQLite connection with row_factory set.
    Commits on clean exit; rolls back on exception.
    """
    conn = sqlite3.connect(cfg.DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def increment_gen(conn: sqlite3.Connection, source: str = "human") -> int:
    """
    Atomically increment the generation counter within an open transaction.
    Call this inside any write that should be replicated to peers.
    Returns the new gen value.
    """
    conn.execute(
        "UPDATE sync_meta "
        "SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) "
        "WHERE key = 'gen'"
    )
    conn.execute(
        "UPDATE sync_meta SET value=datetime('now') WHERE key='last_write_at'"
    )
    conn.execute(
        "UPDATE sync_meta SET value=? WHERE key='last_write_by'", (source,)
    )
    row = conn.execute(
        "SELECT CAST(value AS INTEGER) FROM sync_meta WHERE key='gen'"
    ).fetchone()
    return int(row[0]) if row else 0


# ── Settings helpers ─────────────────────────────────────────────────────────

def get_setting(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    """Return the current value for *key*, or *default* if not set."""
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(conn: sqlite3.Connection, key: str, value: str,
                description: str | None = None) -> None:
    """Upsert a setting.  Preserves existing description when none supplied."""
    conn.execute(
        """
        INSERT INTO settings (key, value, description, updated_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(key) DO UPDATE SET
            value       = excluded.value,
            description = COALESCE(excluded.description, description),
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
    row = conn.execute(
        "SELECT CAST(value AS INTEGER) FROM sync_meta WHERE key='gen'"
    ).fetchone()
    return int(row[0]) if row else 0


def get_meta(conn: sqlite3.Connection, key: str) -> str:
    """Return a sync_meta value by key, or empty string if missing."""
    row = conn.execute(
        "SELECT value FROM sync_meta WHERE key=?", (key,)
    ).fetchone()
    return row[0] if row else ""
