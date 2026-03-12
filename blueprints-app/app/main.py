"""
main.py — FastAPI application factory.

Startup sequence:
  1. Ensure /data/db and /data/gui directories exist.
  2. Initialise SQLite schema (idempotent).
  3. Run PRAGMA integrity_check — mark node as degraded if it fails.
  4. Register all API routers.
  5. Start async queue-drain background task.
  6. Schedule boot catch-up task: compare gen with known peers; if behind or
     degraded, pull a full backup from the highest-gen available peer.
  7. Bootstrap new peers listed in BLUEPRINTS_PEERS (operator-introduced).
  8. Serve /data/gui as static files at /ui.
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from . import config as cfg
from . import db
from .cors import DynamicCORSMiddleware
from .routes_gui_sync import router as gui_sync_router
from .routes_health import router as health_router
from .routes_machines import router as machines_router
from .routes_nodes import router as nodes_router
from .routes_schema import router as schema_router
from .routes_services import router as services_router
from .routes_backup import router as backup_router
from .routes_pfsense_dns import router as pfsense_dns_router
from .routes_proxmox_config import router as proxmox_config_router
from .routes_proxmox_nets   import router as proxmox_nets_router
from .routes_vlans          import router as vlans_router
from .routes_dockge_stacks import router as dockge_stacks_router
from .routes_caddy_configs import router as caddy_configs_router
from .routes_settings   import router as settings_router
from .routes_pve_hosts   import router as pve_hosts_router
from .routes_arp_manual  import router as arp_manual_router
from .routes_sync import router as sync_router
from .sync.drain import start_drain_loop
from .sync.queue import enqueue_for_all_peers
from .sync.restore import apply_restore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncIterator[None]:
    log.info(
        "starting blueprints node=%s instance=%s",
        cfg.NODE_NAME,
        cfg.INSTANCE,
    )

    # Ensure data directories exist (may already exist via volume mounts)
    os.makedirs(cfg.DB_DIR, exist_ok=True)

    # Initialise schema (idempotent)
    db.init_db()

    # Integrity check — log warning but don't abort startup
    ok = db.check_integrity()
    if not ok:
        log.warning(
            "DB integrity check FAILED — sync-out is disabled until restored"
        )

    # Mount GUI static files (GUI dir must exist before mounting)
    if os.path.isdir(cfg.GUI_DIR):
        application.mount(
            "/ui", StaticFiles(directory=cfg.GUI_DIR, html=True, follow_symlink=True), name="gui"
        )
        log.info("serving GUI from %s at /ui", cfg.GUI_DIR)

    # Self-register this node's own identity from env vars — idempotent upsert
    # so that ui_url and addresses stay current every restart without manual
    # DB writes.  The /api/v1/nodes route rejects self-registration from peers
    # but we bypass that here via the DB directly.
    _self_register()

    # Start async drain loop
    await start_drain_loop()

    # Boot catch-up: compare gen with known peers; request backup if behind or degraded
    asyncio.create_task(_boot_catchup())

    # Enqueue any vlans rows that were seeded locally but not yet distributed
    asyncio.create_task(_enqueue_seeded_vlans())

    # Bootstrap: contact configured peers, exchange identities, trigger initial sync
    if cfg.PEER_URLS:
        asyncio.create_task(_bootstrap_peers())

    log.info("blueprints node ready — peers: %s", cfg.PEER_URLS or "(none)")

    yield  # application is running

    # Shutdown — nothing to clean up in Phase 1


def _self_register() -> None:
    """
    Upsert this node's own identity row in the local DB from environment
    variables.  Runs on every startup AND after any full restore so that this
    node's own record is always current regardless of the backup source.
    """
    # Use BLUEPRINTS_SELF_ADDRESS if set (bare-systemd nodes with a real TS IP);
    # fall back to localhost (Docker nodes where Caddy handles external routing).
    own_address = cfg.SELF_ADDRESS
    addresses_json = json.dumps([own_address])
    with db.get_conn() as conn:
        existing = conn.execute(
            "SELECT ui_url FROM nodes WHERE node_id=?", (cfg.NODE_ID,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE nodes SET display_name=?, host_machine=?, addresses=?, ui_url=?, last_seen=datetime('now')"
                " WHERE node_id=?",
                (cfg.NODE_NAME, cfg.HOST_MACHINE, addresses_json, cfg.UI_URL or None, cfg.NODE_ID),
            )
        else:
            conn.execute(
                "INSERT INTO nodes (node_id, display_name, host_machine, addresses, ui_url, last_seen)"
                " VALUES (?, ?, ?, ?, ?, datetime('now'))",
                (cfg.NODE_ID, cfg.NODE_NAME, cfg.HOST_MACHINE, addresses_json, cfg.UI_URL or None),
            )
    log.info("self-registered node=%s ui_url=%s", cfg.NODE_ID, cfg.UI_URL or "(none)")


async def _enqueue_seeded_vlans() -> None:
    """
    After startup (including after a boot-catchup restore), ensure any vlans
    rows that were seeded locally from proxmox_nets are distributed to peers.

    Waits 12 s to let boot-catchup complete first — if this node restored from
    a peer, the vlans it has are already theirs so nothing extra will be sent.
    Peers that are missing vlans rows (e.g. they have fewer proxmox_nets entries)
    will receive any extras.
    """
    await asyncio.sleep(12)
    with db.get_conn() as conn:
        gen = db.get_gen(conn)
        rows = conn.execute("SELECT * FROM vlans").fetchall()
        if not rows:
            return
        for row in rows:
            enqueue_for_all_peers(conn, "UPDATE", "vlans", str(row["vlan_id"]), dict(row), gen)
    if rows:
        log.info("startup: re-enqueued %d vlans rows for peers", len(rows))


async def _boot_catchup() -> None:
    """
    Boot-up catch-up task.

    Runs after startup to detect and heal two scenarios:
      1. DB corruption: integrity_ok == false → pull a full backup immediately
         from the first available peer, regardless of gen.
      2. Stale node: this node was offline while peers made changes → local gen
         is lower than the highest-gen peer → pull a full backup from that peer.

    This makes restarts safe: a node that missed writes while down will
    automatically re-sync from the most up-to-date peer rather than silently
    serving stale data.

    Runs inside a background task; does not block application startup.
    Waits 8 s to allow peers and the drain loop to initialise first.
    """
    await asyncio.sleep(8)

    with db.get_conn() as conn:
        my_gen = db.get_gen(conn)
        integrity_ok = db.get_meta(conn, "integrity_ok") == "true"
        peer_rows = conn.execute(
            "SELECT node_id, addresses FROM nodes WHERE node_id != ?",
            (cfg.NODE_ID,),
        ).fetchall()

    if not peer_rows:
        log.debug("boot_catchup: no peers known yet — nothing to check")
        return

    force_catchup = not integrity_ok
    if force_catchup:
        log.warning(
            "boot_catchup: integrity_ok=false — will request full backup "
            "from first available peer (my gen=%d)",
            my_gen,
        )

    best_peer_url: str | None = None
    best_peer_id: str | None = None
    best_gen = -1 if force_catchup else my_gen  # -1 means always catchup

    for row in peer_rows:
        addresses = json.loads(row["addresses"]) if row["addresses"] else []
        for addr in addresses:
            addr = addr.rstrip("/")
            try:
                async with httpx.AsyncClient(timeout=8.0) as client:
                    resp = await client.get(f"{addr}/health")
                if resp.status_code != 200:
                    continue
                peer_health = resp.json()
                peer_gen = int(peer_health.get("gen", 0))
                if peer_gen > best_gen:
                    best_gen = peer_gen
                    best_peer_url = addr
                    best_peer_id = row["node_id"]
                break  # use first reachable address for this peer
            except Exception:
                continue  # try next address

    if best_peer_url is None:
        if force_catchup:
            log.error(
                "boot_catchup: DB is degraded but no peers reachable — "
                "node will remain degraded until a peer sends a restore"
            )
        else:
            log.debug("boot_catchup: no peers reachable — skipping gen comparison")
        return

    if not force_catchup and best_gen <= my_gen:
        log.info(
            "boot_catchup: up-to-date (my gen=%d, best peer gen=%d) — no catchup needed",
            my_gen,
            best_gen,
        )
        return

    log.info(
        "boot_catchup: requesting full backup from %s "
        "(peer gen=%d, my gen=%d, force=%s)",
        best_peer_id,
        best_gen,
        my_gen,
        force_catchup,
    )

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(f"{best_peer_url}/api/v1/sync/export")
        if resp.status_code != 200:
            log.warning(
                "boot_catchup: peer %s returned HTTP %d for /sync/export",
                best_peer_id,
                resp.status_code,
            )
            return
        sha256_hex = resp.headers.get("x-blueprints-checksum", "")
        ok = await apply_restore(resp.content, sha256_hex)
        if ok:
            log.info(
                "boot_catchup: ✓ restored from %s — node is now up-to-date",
                best_peer_id,
            )
            # Re-register self after restore: the incoming DB is a copy of the
            # peer's DB which may not include this node's own record.
            _self_register()
            log.info("boot_catchup: re-self-registered after restore")
        else:
            log.error(
                "boot_catchup: restore from %s failed — checksum mismatch or "
                "corrupt zip; node remains in current state",
                best_peer_id,
            )
    except Exception:
        log.exception("boot_catchup: unexpected error fetching backup from %s", best_peer_id)


async def _bootstrap_peers() -> None:
    """
    Startup bootstrap — called when BLUEPRINTS_PEERS is set in the environment.

    BLUEPRINTS_PEERS is populated by the operator (in .xarta/deploy.env) to tell
    THIS node about one or more peers at deploy time.  It is always the operator
    who introduces nodes to each other — nodes never register themselves with
    other nodes.  Any node can act as temporary primary; there is no fixed primary.

    For each peer URL configured by the operator:
      1. GET /health to learn the peer's node_id and display name.
      2. Register the peer locally via our own POST /api/v1/nodes endpoint.
         This is identical to the operator calling that endpoint manually and
         triggers _send_initial_backup so this node pushes its full DB to the
         peer immediately (acting as temporary primary for this onboarding).

    Runs inside a background task so it does not block application startup.
    Retries once after 10 s on ConnectError per peer.
    """
    await asyncio.sleep(5)  # let the app finish startup before making requests

    for peer_url in cfg.PEER_URLS:
        peer_url = peer_url.rstrip("/")
        for attempt in range(2):
            try:
                # Step 1: learn peer identity
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.get(f"{peer_url}/health")
                if resp.status_code != 200:
                    log.warning(
                        "bootstrap: peer %s /health returned HTTP %d",
                        peer_url, resp.status_code,
                    )
                    break
                health = resp.json()
                peer_node_id = health.get("node_id")
                peer_node_name = health.get("node_name", peer_node_id or "unknown")
                if not peer_node_id:
                    log.warning("bootstrap: peer %s health missing node_id", peer_url)
                    break

                # Normalise display name: "lady-penelope" → "Lady Penelope"
                peer_display = peer_node_name.replace("-", " ").title()

                # Step 2: register peer locally — this triggers a full DB backup
                # push to the peer (this node acting as temporary primary).
                # We do NOT post our own identity to the peer — that would be
                # self-registration, which is explicitly not part of the design.
                # If the operator wants the peer to know about us, they will tell
                # that peer separately.
                peer_ui_url = health.get("ui_url") or None
                async with httpx.AsyncClient(timeout=10.0) as client:
                    r2 = await client.post(
                        "http://localhost:8080/api/v1/nodes",
                        json={
                            "node_id": peer_node_id,
                            "display_name": peer_display,
                            "addresses": [peer_url],
                            "ui_url": peer_ui_url,
                        },
                    )
                log.info(
                    "bootstrap: registered peer %s locally, full backup scheduled (HTTP %d)",
                    peer_node_id, r2.status_code,
                )
                break  # success — no retry needed

            except httpx.ConnectError:
                if attempt == 0:
                    log.warning(
                        "bootstrap: peer %s unreachable — retrying in 10 s", peer_url
                    )
                    await asyncio.sleep(10)
                else:
                    log.warning(
                        "bootstrap: peer %s still unreachable — giving up for now",
                        peer_url,
                    )
            except Exception:
                log.exception("bootstrap: unexpected error contacting peer %s", peer_url)
                break


def create_app() -> FastAPI:
    application = FastAPI(
        title="Blueprints",
        description="Distributed peer-to-peer service index",
        version="0.1.0",
        lifespan=lifespan,
    )

    # CORS — origin allowlist loaded from config + dynamic peer node ui_urls from DB.
    # Requests from unlisted origins are blocked by the browser (server still receives
    # the request but the browser withholds the response from the calling page).
    application.add_middleware(DynamicCORSMiddleware)

    # ── Routers ───────────────────────────────────────────────────────────────
    application.include_router(health_router)
    application.include_router(services_router, prefix="/api/v1")
    application.include_router(machines_router, prefix="/api/v1")
    application.include_router(nodes_router,    prefix="/api/v1")
    application.include_router(schema_router,   prefix="/api/v1")
    application.include_router(sync_router,     prefix="/api/v1")
    application.include_router(backup_router,   prefix="/api/v1")
    application.include_router(pfsense_dns_router,    prefix="/api/v1")
    application.include_router(proxmox_config_router, prefix="/api/v1")
    application.include_router(proxmox_nets_router,   prefix="/api/v1")
    application.include_router(vlans_router,          prefix="/api/v1")
    application.include_router(dockge_stacks_router,  prefix="/api/v1")
    application.include_router(caddy_configs_router,  prefix="/api/v1")
    application.include_router(settings_router,       prefix="/api/v1")
    application.include_router(pve_hosts_router,      prefix="/api/v1")
    application.include_router(arp_manual_router,     prefix="/api/v1")
    application.include_router(gui_sync_router,       prefix="/api/v1")

    return application


app = create_app()
