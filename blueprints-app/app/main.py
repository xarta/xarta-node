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
  7. Load nodes from .nodes.json into the DB.
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
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles

from . import config as cfg
from . import db
from .auth import compute_token
from .cors import DynamicCORSMiddleware
from .middleware_auth import AuthMiddleware
from .routes_gui_sync import router as gui_sync_router
from .routes_health import router as health_router
from .routes_machines import router as machines_router
from .routes_nodes import router as nodes_router, _upsert_nodes_from_config
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
from .routes_arp_manual    import router as arp_manual_router
from .routes_ssh_targets   import router as ssh_targets_router
from .routes_manual_links  import router as manual_links_router
from .routes_sync import router as sync_router
from .routes_keys import router as keys_router
from .routes_certs import router as certs_router
from .routes_assumptions import router as assumptions_router
from .routes_todo import router as todo_router
from .routes_docs import router as docs_router
from .routes_doc_groups import router as doc_groups_router
from .routes_doc_images import router as doc_images_router
from .routes_firewall import router as firewall_router
from .routes_ai_providers import router as ai_providers_router
from .routes_ai_project_assignments import router as ai_project_assignments_router
from .routes_bookmarks import router as bookmarks_router
from .routes_form_controls import router as form_controls_router
from .routes_nav_items import router as nav_items_router
from .routes_table_layouts import router as table_layouts_router
from .routes_ui_cache import router as ui_cache_router
from .seekdb_sync import start_seekdb_sync_loop
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

    # Load all nodes from .nodes.json into DB — idempotent upsert.
    # Populates peer records and self-record with current addresses from JSON.
    _load_nodes_from_json()

    # Start async drain loop
    await start_drain_loop()

    # Start background SQLite->SeekDB index sync loop
    start_seekdb_sync_loop()

    # Boot catch-up: if DB integrity failed, pull full backup from a trusted peer
    asyncio.create_task(_boot_catchup())

    # Enqueue any vlans rows that were seeded locally but not yet distributed
    asyncio.create_task(_enqueue_seeded_vlans())

    log.info("blueprints node ready — peers: %s", list(cfg.PEER_SYNC_URLS) or "(none)")

    yield  # application is running

    # Shutdown — nothing to clean up in Phase 1


def _load_nodes_from_json() -> None:
    """
    Upsert all active nodes from .nodes.json into the local DB.

    Called on startup and after a boot-catchup restore. Ensures the DB mirrors
    the JSON file on every start — this node's own record stays current and all
    fleet peers are known without needing peer-to-peer bootstrap.
    """
    count = _upsert_nodes_from_config()
    log.info("loaded %d active nodes from .nodes.json into DB", count)


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
    Boot-up corruption-recovery task.

    Runs after startup to heal one scenario only:
      DB corruption: integrity_ok == false → pull a full backup immediately
      from the first reachable trusted peer (PEER_URLS), regardless of gen.

    A node that was merely offline while peers made writes does NOT need a
    full restore — the drain queue handles incremental catch-up automatically
    once the node is back up.  Gen numbers are local write counters and are
    not comparable across nodes, so gen comparison is not a reliable signal
    for "who has better data".

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

    if integrity_ok:
        log.debug("boot_catchup: integrity_ok=true — no recovery needed")
        return

    log.warning(
        "boot_catchup: integrity_ok=false — will request full backup "
        "from first reachable trusted peer (my gen=%d)",
        my_gen,
    )

    if not peer_rows:
        log.error("boot_catchup: DB is degraded but no peers known — cannot recover")
        return

    # Only pull from nodes whose address is in our configured PEER_SYNC_URLS.
    # Ghost/retired nodes in the DB must never be used as restore sources.
    trusted_urls: set[str] = {
        url.rstrip("/")
        for urls in cfg.PEER_SYNC_URLS.values()
        for url in urls
    }

    best_peer_url: str | None = None
    best_peer_id: str | None = None

    for row in peer_rows:
        addresses = json.loads(row["addresses"]) if row["addresses"] else []
        for addr in addresses:
            addr = addr.rstrip("/")
            if addr not in trusted_urls:
                log.debug(
                    "boot_catchup: skipping %s (%s) — not in configured PEER_URLS",
                    row["node_id"],
                    addr,
                )
                break  # skip this peer entirely
            try:
                async with httpx.AsyncClient(timeout=8.0) as client:
                    resp = await client.get(f"{addr}/health")
                if resp.status_code == 200:
                    best_peer_url = addr
                    best_peer_id = row["node_id"]
                    break
            except Exception:
                continue
        if best_peer_url:
            break

    if best_peer_url is None:
        log.error(
            "boot_catchup: DB is degraded but no trusted peers reachable — "
            "node will remain degraded until a peer sends a restore"
        )
        return

    log.info("boot_catchup: requesting full backup from %s", best_peer_id)

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(
                f"{best_peer_url}/api/v1/sync/export",
                headers={"x-api-token": compute_token(cfg.SYNC_SECRET)} if cfg.SYNC_SECRET else {},
            )
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
            # Re-load nodes from JSON after restore: the incoming DB is a copy
            # of the peer's DB which may not have current addresses for this node.
            _load_nodes_from_json()
            log.info("boot_catchup: refreshed nodes from .nodes.json after restore")
        else:
            log.error(
                "boot_catchup: restore from %s failed — checksum mismatch or "
                "corrupt zip; node remains in current state",
                best_peer_id,
            )
    except Exception:
        log.exception("boot_catchup: unexpected error fetching backup from %s", best_peer_id)




def create_app() -> FastAPI:
    application = FastAPI(
        title="Blueprints",
        description="Distributed peer-to-peer service index",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Auth — IP allowlist + TOTP token check (secrets from .env).
    # Must be added BEFORE CORS so authenticated preflight requests pass through.
    application.add_middleware(AuthMiddleware)

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
    application.include_router(ssh_targets_router,    prefix="/api/v1")
    application.include_router(manual_links_router,   prefix="/api/v1")
    application.include_router(keys_router,           prefix="/api/v1")
    application.include_router(certs_router,          prefix="/api/v1")
    application.include_router(gui_sync_router,       prefix="/api/v1")
    application.include_router(assumptions_router,    prefix="/api/v1")
    application.include_router(todo_router,           prefix="/api/v1")
    application.include_router(docs_router,           prefix="/api/v1")
    application.include_router(doc_groups_router,     prefix="/api/v1")
    application.include_router(doc_images_router,      prefix="/api/v1")
    application.include_router(firewall_router,               prefix="/api/v1")
    application.include_router(ai_providers_router,            prefix="/api/v1")
    application.include_router(ai_project_assignments_router,  prefix="/api/v1")
    application.include_router(bookmarks_router,               prefix="/api/v1")
    application.include_router(form_controls_router,           prefix="/api/v1")
    application.include_router(nav_items_router,               prefix="/api/v1")
    application.include_router(table_layouts_router,           prefix="/api/v1")
    application.include_router(ui_cache_router,                prefix="/api/v1")

    @application.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> Response:
        svg = (
            '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32">'
            '<rect width="32" height="32" rx="4" fill="#1a5c30"/>'
            '<text x="16" y="22.5" text-anchor="middle"'
            ' font-family="\'Arial Black\',Arial,sans-serif"'
            ' font-weight="900" font-size="15" fill="#ffffff"'
            ' letter-spacing="0.5">FAB</text></svg>'
        )
        return Response(content=svg, media_type="image/svg+xml")

    return application


app = create_app()
