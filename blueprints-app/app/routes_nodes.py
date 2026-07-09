"""routes_nodes.py — node management endpoints.

/api/v1/nodes/self    — returns this node's identity (from config, not DB)
/api/v1/nodes         — list nodes registered in the DB
POST /api/v1/nodes/refresh — re-read .nodes.json and upsert DB (Refresh button)
DELETE /api/v1/nodes/{id}  — mark node inactive in .nodes.json and update DB
POST /api/v1/nodes/{id}/pct — start/stop/reboot the LXC for a fleet node via pct on its PVE host
GET  /api/v1/nodes/{id}/pct-status — return current pct status from proxmox_config DB
"""

import asyncio
import hashlib
import json
import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from starlette.responses import Response

from . import config as cfg
from . import timing
from .auth import compute_token
from .db import get_conn
from .models import NodeOut, RepoVersionsOut
from .sync.drain import _make_sync_client

log = logging.getLogger(__name__)

router = APIRouter(prefix="/nodes", tags=["nodes"])


# ── Helpers ───────────────────────────────────────────────────────────────────


async def _run_nodes_sync_work(label: str, func, *args, **kwargs):
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return func(*args, **kwargs)
    return await timing.to_thread(f"nodes.{label}", func, *args, **kwargs)


def _row_to_out(row) -> NodeOut:
    addrs = row["addresses"]
    keys = row.keys()
    addr_list: list[str] = json.loads(addrs) if addrs else []

    # A node is a fleet peer if it is self, or if any of its addresses appears
    # in the configured PEER_SYNC_URLS (these are the nodes this instance syncs to).
    _peer_set = {u.rstrip("/") for urls in cfg.PEER_SYNC_URLS.values() for u in urls}
    fleet_peer: bool = row["node_id"] == cfg.NODE_ID or any(
        a.rstrip("/") in _peer_set for a in addr_list
    )

    return NodeOut(
        node_id=row["node_id"],
        display_name=row["display_name"],
        display_order=row["display_order"] if "display_order" in keys else 0,
        host_machine=row["host_machine"],
        tailnet=row["tailnet"],
        primary_hostname=row["primary_hostname"] if "primary_hostname" in keys else None,
        tailnet_hostname=row["tailnet_hostname"] if "tailnet_hostname" in keys else None,
        addresses=addr_list or None,
        ui_url=row["ui_url"] if "ui_url" in keys else None,
        machine_id=row["machine_id"] if "machine_id" in keys else None,
        last_seen=row["last_seen"],
        created_at=row["created_at"],
        fleet_peer=fleet_peer,
        pending_count=row["pending_count"] if "pending_count" in keys else 0,
    )


# ── Routes ────────────────────────────────────────────────────────────────────


@router.get("/self", response_model=NodeOut)
async def get_self() -> NodeOut:
    """Return this node's identity as derived from .nodes.json."""
    return NodeOut(
        node_id=cfg.NODE_ID,
        display_name=cfg.NODE_NAME,
        host_machine=cfg.HOST_MACHINE,
        tailnet=None,
        addresses=[cfg.SELF_ADDRESS],
        last_seen=None,
        created_at="",
    )


@router.get("", response_model=list[NodeOut])
async def list_nodes() -> list[NodeOut]:
    """List all peer nodes registered in the local DB."""
    return await _run_nodes_sync_work("list_nodes", _list_nodes_sync)


def _list_nodes_sync() -> list[NodeOut]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT n.*,
                   (SELECT COUNT(*) FROM sync_queue
                    WHERE target_node_id = n.node_id AND sent = 0) AS pending_count
            FROM nodes n ORDER BY n.display_order, n.display_name
            """
        ).fetchall()
    return [_row_to_out(r) for r in rows]


@router.post("/refresh", status_code=200)
async def refresh_nodes() -> dict:
    """
    Re-read .nodes.json and upsert all active nodes into the local DB.
    Called by the Refresh button in the Nodes UI tab.
    """
    await _run_nodes_sync_work("upsert_nodes_from_config", _upsert_nodes_from_config)
    log.info("nodes refreshed from .nodes.json via API")
    return {
        "status": "ok",
        "active_nodes": len([n for n in cfg.NODES_DATA if n.get("active", False)]),
    }


def _upsert_nodes_from_config() -> int:
    """Upsert all active nodes from cfg.NODES_DATA into the local DB. Returns count."""
    count = 0
    with get_conn() as conn:
        for node in cfg.NODES_DATA:
            if not node.get("active", False):
                continue
            nid = node["node_id"]
            name = node["display_name"]
            order = node.get("display_order", 0)
            host = node["host_machine"]
            tailnet = node.get("tailnet", "")
            pip = node["primary_ip"]
            ph = node["primary_hostname"]
            tip = node["tailnet_ip"]
            th = node.get("tailnet_hostname", "")
            port = node["sync_port"]
            scheme = node.get("sync_scheme", "http")

            addresses = json.dumps(
                [
                    f"{scheme}://{pip}:{port}",
                    f"{scheme}://{tip}:{port}",
                ]
            )
            ui_url = f"https://{ph}"

            existing = conn.execute("SELECT node_id FROM nodes WHERE node_id=?", (nid,)).fetchone()
            if existing:
                conn.execute(
                    "UPDATE nodes SET display_name=?, display_order=?, host_machine=?, tailnet=?, "
                    "primary_hostname=?, tailnet_hostname=?, "
                    "addresses=?, ui_url=?, last_seen=datetime('now') WHERE node_id=?",
                    (name, order, host, tailnet, ph, th, addresses, ui_url, nid),
                )
            else:
                conn.execute(
                    "INSERT INTO nodes (node_id, display_name, display_order, host_machine, tailnet, "
                    "primary_hostname, tailnet_hostname, addresses, ui_url, last_seen) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))",
                    (nid, name, order, host, tailnet, ph, th, addresses, ui_url),
                )
            count += 1
    return count


@router.delete("/{node_id}", status_code=204)
async def delete_node(node_id: str) -> Response:
    """
    Mark a node inactive in .nodes.json (via bp-nodes-delete.sh) and remove
    its DB record from this node. Does not propagate via sync queue — nodes
    table is local-only, sourced from .nodes.json.
    """
    if node_id == cfg.NODE_ID:
        raise HTTPException(400, "cannot delete self")

    # Run bp-nodes-delete.sh to mark inactive in JSON and reload
    script = os.path.join(cfg.REPO_OUTER_PATH, "bp-nodes-delete.sh")
    if os.path.isfile(script):
        result = subprocess.run(
            ["bash", script, node_id],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            log.error("bp-nodes-delete.sh failed: %s", result.stderr)
            raise HTTPException(500, f"failed to update .nodes.json: {result.stderr.strip()}")
        log.info("bp-nodes-delete.sh marked %s inactive in .nodes.json", node_id)
    else:
        log.warning("bp-nodes-delete.sh not found at %s — deleting from DB only", script)

    with get_conn() as conn:
        deleted = conn.execute("DELETE FROM nodes WHERE node_id=?", (node_id,)).rowcount
        if not deleted:
            raise HTTPException(404, f"node '{node_id}' not found")

    log.info("deleted node %s from local DB", node_id)
    return Response(status_code=204)


@router.delete("/{node_id}/sync-queue", status_code=204)
async def purge_node_sync_queue(node_id: str) -> Response:
    """Purge all unsent sync queue entries targeting a specific node."""
    with get_conn() as conn:
        n = conn.execute(
            "DELETE FROM sync_queue WHERE target_node_id=? AND sent=0", (node_id,)
        ).rowcount
    log.info("purged %d unsent sync queue entries for node %s", n, node_id)
    return Response(status_code=204)


@router.post("/{node_id}/git-pull", status_code=204)
async def proxy_node_git_pull(node_id: str) -> Response:
    """Proxy a git-pull (scope=outer) request to the named peer node."""
    addrs = await _run_nodes_sync_work("node_addresses", _node_addresses_sync, node_id)
    if addrs is None:
        raise HTTPException(404, f"node '{node_id}' not found or has no addresses")
    if not addrs:
        raise HTTPException(422, f"node '{node_id}' has no addresses configured")
    target = addrs[0].rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                f"{target}/api/v1/sync/git-pull",
                json={"scope": "outer"},
                headers={"x-api-token": compute_token(cfg.SYNC_SECRET)} if cfg.SYNC_SECRET else {},
            )
    except Exception as exc:
        raise HTTPException(502, f"failed to reach {node_id} at {target}: {exc}") from exc
    if resp.status_code not in (200, 204):
        raise HTTPException(502, f"remote {node_id} returned HTTP {resp.status_code}")
    log.info("proxied git-pull to %s (%s)", node_id, target)
    return Response(status_code=204)


@router.post("/{node_id}/restart", status_code=204)
async def proxy_node_restart(node_id: str) -> Response:
    """Proxy a service restart request to the named peer node."""
    addrs = await _run_nodes_sync_work("node_addresses", _node_addresses_sync, node_id)
    if addrs is None:
        raise HTTPException(404, f"node '{node_id}' not found or has no addresses")
    if not addrs:
        raise HTTPException(422, f"node '{node_id}' has no addresses configured")
    target = addrs[0].rstrip("/")
    try:
        async with _make_sync_client(timeout=10.0) as client:
            resp = await client.post(
                f"{target}/api/v1/sync/restart",
                headers={"x-api-token": compute_token(cfg.SYNC_SECRET)} if cfg.SYNC_SECRET else {},
            )
    except Exception as exc:
        raise HTTPException(502, f"failed to reach {node_id} at {target}: {exc}") from exc
    if resp.status_code not in (200, 204):
        raise HTTPException(502, f"remote {node_id} returned HTTP {resp.status_code}")
    log.info("proxied restart to %s (%s)", node_id, target)
    return Response(status_code=204)


@router.get("/{node_id}/repo-versions", response_model=RepoVersionsOut)
async def proxy_node_repo_versions(node_id: str) -> RepoVersionsOut:
    """Return outer/inner/non-root repo versions for the named node."""
    if node_id == cfg.NODE_ID:
        from .routes_health import repo_versions

        return await asyncio.to_thread(repo_versions)

    addrs = await _run_nodes_sync_work("node_addresses", _node_addresses_sync, node_id)
    if addrs is None:
        raise HTTPException(404, f"node '{node_id}' not found or has no addresses")
    if not addrs:
        raise HTTPException(422, f"node '{node_id}' has no addresses configured")
    target = addrs[0].rstrip("/")
    try:
        async with _make_sync_client(timeout=10.0) as client:
            resp = await client.get(
                f"{target}/health/repos",
                headers={"x-api-token": compute_token(cfg.SYNC_SECRET)} if cfg.SYNC_SECRET else {},
            )
    except Exception as exc:
        raise HTTPException(502, f"failed to reach {node_id} at {target}: {exc}") from exc
    if resp.status_code != 200:
        raise HTTPException(502, f"remote {node_id} returned HTTP {resp.status_code}")
    return RepoVersionsOut(**resp.json())


def _node_addresses_sync(node_id: str) -> list[str] | None:
    with get_conn() as conn:
        row = conn.execute("SELECT addresses FROM nodes WHERE node_id=?", (node_id,)).fetchone()
    if not row or not row["addresses"]:
        return None
    addrs: list[str] = json.loads(row["addresses"])
    return addrs


class PctAction(BaseModel):
    action: str  # "start" | "stop" | "reboot" | "force-cycle"


class FleetHealthCheckRequest(BaseModel):
    source: str = "manual"
    expected_versions: dict[str, object] | None = None
    only_node: str | None = None
    timeout_seconds: int = 190


class FleetHealthClearRequest(BaseModel):
    issues: list[dict[str, object]] | None = None


_FLEET_HEALTH_ACK_PATH = Path(
    os.environ.get(
        "XARTA_FLEET_HEALTH_ACK_PATH",
        "/xarta-node/.lone-wolf/state/fleet-health-checks/acknowledged-issues.json",
    )
)
_FLEET_HEALTH_OK_STATUSES = {"", "ok", "pass", "info", "skipped"}
_FLEET_HEALTH_REPO_LABELS = {
    "repo:/root/xarta-node": "outer",
    "repo:/root/xarta-node/.xarta": "inner",
    "repo:/xarta-node": "non_root",
}


def _fleet_health_ack_path() -> Path:
    return Path(os.environ.get("XARTA_FLEET_HEALTH_ACK_PATH", str(_FLEET_HEALTH_ACK_PATH)))


def _fleet_health_is_problem_check(check: dict) -> bool:
    return str(check.get("status") or "").lower() not in _FLEET_HEALTH_OK_STATUSES


def _fleet_health_issue_fingerprint(report: dict, check: dict) -> str:
    payload = {
        "node_id": report.get("node_id"),
        "target_ip": report.get("target_ip"),
        "check_name": check.get("name"),
        "check_status": check.get("status"),
        "check_detail": check.get("detail"),
        "check_metrics": check.get("metrics") or {},
    }
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _load_fleet_health_acknowledgements() -> dict[str, dict]:
    path = _fleet_health_ack_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except Exception as exc:
        log.warning("failed to load fleet health acknowledgements from %s: %s", path, exc)
        return {}
    if isinstance(data, dict) and isinstance(data.get("issues"), dict):
        return {str(k): v for k, v in data["issues"].items() if isinstance(v, dict)}
    if isinstance(data, dict):
        return {str(k): v for k, v in data.items() if isinstance(v, dict)}
    return {}


def _save_fleet_health_acknowledgements(issues: dict[str, dict]) -> None:
    path = _fleet_health_ack_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "updated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "issues": issues,
    }
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def _fleet_health_issue_snapshot(report: dict, check: dict, fingerprint: str) -> dict:
    return {
        "fingerprint": fingerprint,
        "node_id": report.get("node_id"),
        "target_ip": report.get("target_ip"),
        "check_name": check.get("name"),
        "status": check.get("status"),
        "detail": check.get("detail"),
        "metrics": check.get("metrics") or {},
    }


def _fleet_health_report_status(report: dict, active_problem_count: int) -> str:
    if active_problem_count <= 0 and int(report.get("checks_not_run") or 0) <= 0:
        return "ok"
    active_statuses = {
        str(check.get("status") or "").lower()
        for check in report.get("checks") or []
        if isinstance(check, dict)
        and _fleet_health_is_problem_check(check)
        and not check.get("acknowledged")
    }
    if "fail" in active_statuses or "error" in active_statuses:
        return "fail"
    return "warn"


def _fleet_health_report_commits(report: dict) -> str | None:
    commits: dict[str, str] = {}
    for check in report.get("checks") or []:
        if not isinstance(check, dict):
            continue
        label = _FLEET_HEALTH_REPO_LABELS.get(str(check.get("name") or ""))
        if not label:
            continue
        metrics = check.get("metrics") if isinstance(check.get("metrics"), dict) else {}
        head = metrics.get("head") or ""
        if head:
            commits[label] = str(head)
    if not commits:
        return None
    return (
        f"outer={commits.get('outer', '?')} "
        f"non_root={commits.get('non_root', '?')} "
        f"inner={commits.get('inner', '?')}"
    )


def _render_fleet_health_text_report(data: dict) -> str:
    summary = data.get("summary") or {}
    generated_at = data.get("generated_at") or data.get("completed_at") or "completed"
    source = data.get("source") or "manual"
    nodes_checked = int(summary.get("nodes_checked") or 0)
    nodes_targeted = int(summary.get("nodes_targeted") or 0)
    nodes_not_checked = summary.get("nodes_not_checked") or []
    if isinstance(nodes_not_checked, list):
        nodes_not_checked_text = (
            ", ".join(map(str, nodes_not_checked)) if nodes_not_checked else "none"
        )
    else:
        nodes_not_checked_text = str(nodes_not_checked or "none")
    problems = int(summary.get("problems_found") or 0)
    acknowledged = int(summary.get("acknowledged_problems_hidden") or 0)
    checks_not_run = int(summary.get("checks_not_run") or 0)

    lines = [
        f"Fleet health check: {generated_at}",
        f"Source: {source}",
        f"Nodes checked: {nodes_checked}/{nodes_targeted}",
        f"Nodes not checked: {nodes_not_checked_text}",
        f"Problems found: {problems}",
    ]
    if acknowledged:
        lines.append(f"Acknowledged old issues hidden: {acknowledged}")
    lines.extend(
        [
            f"Checks not run: {checks_not_run}",
            f"Harness return code: {data.get('helper_returncode', (data.get('harness') or {}).get('returncode', 0))}",
            "",
        ]
    )

    for report in data.get("reports") or []:
        if not isinstance(report, dict):
            continue
        node_id = report.get("node_id") or "unknown-node"
        status = str(report.get("status") or "ok").upper()
        problem_count = int(report.get("problem_count") or 0)
        blocked = int(report.get("checks_not_run") or 0)
        lines.append(f"{node_id}: {status} problems={problem_count} blocked_checks={blocked}")
        commits = _fleet_health_report_commits(report)
        if commits:
            lines.append(f"  Commits: {commits}")
        for check in report.get("checks") or []:
            if not isinstance(check, dict):
                continue
            if not _fleet_health_is_problem_check(check) or check.get("acknowledged"):
                continue
            lines.append(
                f"  {str(check.get('status') or 'WARN').upper()} "
                f"{check.get('name') or 'unknown'}: {check.get('detail') or ''}"
            )
        hidden = int(report.get("acknowledged_problem_count") or 0)
        if hidden:
            lines.append(f"  Acknowledged old issues hidden: {hidden}")
        lines.append("")

    if data.get("log_path"):
        lines.append(f"Saved text log: {data['log_path']}")
    if data.get("json_log_path"):
        lines.append(f"Saved JSON log: {data['json_log_path']}")
    if "logs_pruned" in data:
        lines.append(f"Pruned old logs: {data.get('logs_pruned')}")
    return "\n".join(lines).rstrip() + "\n"


def _apply_fleet_health_acknowledgements(data: dict) -> dict:
    acknowledged = _load_fleet_health_acknowledgements()
    summary = data.setdefault("summary", {})
    data["raw_ok"] = data.get("ok")
    data["raw_summary"] = dict(summary)
    if "text_report" in data:
        data["raw_text_report"] = data.get("text_report")

    active_issues: list[dict] = []
    acknowledged_issues: list[dict] = []
    total_active = 0
    total_acknowledged = 0

    for report in data.get("reports") or []:
        if not isinstance(report, dict):
            continue
        raw_problem_count = int(report.get("problem_count") or 0)
        report["raw_problem_count"] = raw_problem_count
        active_count = 0
        acknowledged_count = 0
        for check in report.get("checks") or []:
            if not isinstance(check, dict) or not _fleet_health_is_problem_check(check):
                continue
            fingerprint = _fleet_health_issue_fingerprint(report, check)
            check["issue_fingerprint"] = fingerprint
            snapshot = _fleet_health_issue_snapshot(report, check, fingerprint)
            if fingerprint in acknowledged:
                check["acknowledged"] = True
                acknowledged_count += 1
                acknowledged_issues.append(snapshot)
            else:
                check["acknowledged"] = False
                active_count += 1
                active_issues.append(snapshot)
        report["problem_count"] = active_count
        report["acknowledged_problem_count"] = acknowledged_count
        report["status"] = _fleet_health_report_status(report, active_count)
        total_active += active_count
        total_acknowledged += acknowledged_count

    raw_problems_found = int(summary.get("problems_found") or 0)
    summary["raw_problems_found"] = raw_problems_found
    unmapped_problem_count = max(0, raw_problems_found - total_active - total_acknowledged)
    if unmapped_problem_count:
        data["fleet_health_unmapped_problem_count"] = unmapped_problem_count
        total_active += unmapped_problem_count
    summary["problems_found"] = total_active
    summary["acknowledged_problems_hidden"] = total_acknowledged
    checks_not_run = int(summary.get("checks_not_run") or 0)
    data["ok"] = total_active == 0 and checks_not_run == 0
    data["fleet_health_issues"] = active_issues
    data["fleet_health_acknowledged_issues"] = acknowledged_issues
    data["text_report"] = _render_fleet_health_text_report(data)
    return data


@router.post("/fleet-health-clear", status_code=200)
async def clear_fleet_health_issues(body: FleetHealthClearRequest | None = None) -> dict:
    """Acknowledge exact current fleet-health issue fingerprints."""
    issues = (body.issues if body else None) or []
    acknowledgements = await _run_nodes_sync_work(
        "clear_fleet_health_issues", _clear_fleet_health_issues_sync, issues
    )
    return acknowledgements


def _clear_fleet_health_issues_sync(issues: list[dict[str, object]]) -> dict:
    stored = _load_fleet_health_acknowledgements()
    acknowledged_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    count = 0
    for issue in issues:
        if not isinstance(issue, dict):
            continue
        fingerprint = str(issue.get("fingerprint") or issue.get("issue_fingerprint") or "")
        if not fingerprint:
            continue
        stored[fingerprint] = {
            "acknowledged_at": acknowledged_at,
            "node_id": issue.get("node_id"),
            "target_ip": issue.get("target_ip"),
            "check_name": issue.get("check_name") or issue.get("name"),
            "status": issue.get("status"),
            "detail": issue.get("detail"),
            "metrics": issue.get("metrics") or {},
        }
        count += 1
    _save_fleet_health_acknowledgements(stored)
    return {
        "ok": True,
        "acknowledged": count,
        "total_acknowledgements": len(stored),
        "ack_path": str(_fleet_health_ack_path()),
    }


def _fleet_health_blocked_report(reason: str, *, source: str = "manual") -> dict:
    log_dir = "/xarta-node/.lone-wolf/state/fleet-health-checks"
    text = (
        "Fleet health check: not run\n"
        f"Source: {source}\n"
        "Nodes checked: 0/0\n"
        "Nodes not checked: unknown\n"
        "Problems found: 1\n"
        "Checks not run: 1\n"
        f"Blocked: {reason}\n"
        f"Log directory: {log_dir}\n"
    )
    return _apply_fleet_health_acknowledgements(
        {
            "ok": False,
            "source": source,
            "summary": {
                "nodes_targeted": 0,
                "nodes_checked": 0,
                "nodes_not_checked": [],
                "problems_found": 1,
                "checks_not_run": 1,
            },
            "reports": [],
            "text_report": text,
            "log_dir": log_dir,
            "log_path": None,
            "json_log_path": None,
            "harness": {"returncode": 127, "stdout": "", "stderr": reason},
        }
    )


@router.post("/fleet-health-check", status_code=200)
async def run_fleet_health_check(body: FleetHealthCheckRequest | None = None) -> dict:
    """Run the shared read-only fleet health helper and return its report."""
    body = body or FleetHealthCheckRequest()
    source = (body.source or "manual").strip()[:80] or "manual"
    timeout = max(15, min(int(body.timeout_seconds or 190), 300))
    helper = os.environ.get("XARTA_FLEET_HEALTH_HELPER")
    if not helper:
        inner = cfg.REPO_INNER_PATH or os.path.join(
            cfg.REPO_OUTER_PATH or "/root/xarta-node", ".xarta"
        )
        helper = os.path.join(inner, ".agents", "bin", "fleet-health-check")
    if not os.path.isfile(helper):
        return _fleet_health_blocked_report(f"helper missing: {helper}", source=source)
    if not os.access(helper, os.X_OK):
        return _fleet_health_blocked_report(f"helper is not executable: {helper}", source=source)

    cmd = [
        helper,
        "--json",
        "--source",
        source,
        "--timeout-seconds",
        str(timeout),
    ]
    if body.only_node:
        cmd.extend(["--only-node", body.only_node])
    if body.expected_versions:
        cmd.extend(
            ["--expected-repos-json", json.dumps(body.expected_versions, separators=(",", ":"))]
        )

    try:
        result = await asyncio.to_thread(
            subprocess.run,
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout + 15,
        )
    except subprocess.TimeoutExpired as exc:
        return _fleet_health_blocked_report(
            f"helper timed out after {timeout + 15}s: {(exc.stderr or exc.stdout or '').strip()}",
            source=source,
        )
    except Exception as exc:
        return _fleet_health_blocked_report(f"{type(exc).__name__}: {exc}", source=source)

    try:
        data = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        text = (
            "Fleet health check: helper output was not JSON\n"
            f"Source: {source}\n"
            "Nodes checked: 0/0\n"
            "Problems found: 1\n"
            "Checks not run: 1\n"
            f"Return code: {result.returncode}\n"
            f"stdout: {(result.stdout or '').strip()[:1200]}\n"
            f"stderr: {(result.stderr or '').strip()[:1200]}\n"
        )
        return _apply_fleet_health_acknowledgements(
            {
                "ok": False,
                "source": source,
                "summary": {
                    "nodes_targeted": 0,
                    "nodes_checked": 0,
                    "nodes_not_checked": [],
                    "problems_found": 1,
                    "checks_not_run": 1,
                },
                "reports": [],
                "text_report": text,
                "log_dir": "/xarta-node/.lone-wolf/state/fleet-health-checks",
                "log_path": None,
                "json_log_path": None,
                "harness": {
                    "returncode": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                },
            }
        )
    data.setdefault("ok", result.returncode == 0)
    data.setdefault("source", source)
    data.setdefault("harness", {})
    data["helper_returncode"] = result.returncode
    if result.stderr:
        data["helper_stderr"] = result.stderr
    return _apply_fleet_health_acknowledgements(data)


def _get_node_lxc_target(node_id: str) -> tuple[str, str, int]:
    """Return host_machine, pve_host, vmid for a fleet node's LXC."""
    with get_conn() as conn:
        node_row = conn.execute(
            "SELECT host_machine FROM nodes WHERE node_id=?", (node_id,)
        ).fetchone()
    if not node_row:
        raise HTTPException(404, f"node '{node_id}' not found")

    host_machine = node_row["host_machine"]
    with get_conn() as conn:
        pve_row = conn.execute(
            "SELECT pve_host, vmid FROM proxmox_config WHERE name=? AND vm_type='lxc' LIMIT 1",
            (host_machine,),
        ).fetchone()
    if not pve_row:
        raise HTTPException(
            404,
            f"no proxmox_config entry for '{host_machine}' — run a Proxmox probe first",
        )
    return host_machine, pve_row["pve_host"], pve_row["vmid"]


def _make_pve_ssh_args(pve_host: str, connect_timeout: int = 10) -> list[str]:
    from .ssh import SshKeyMissing, SshTargetNotFound, make_ssh_args, resolve_env_key

    try:
        return make_ssh_args(pve_host, connect_timeout=connect_timeout)
    except SshTargetNotFound:
        # Fallback: use PROXMOX_SSH_KEY directly (no source-IP binding)
        try:
            key_path = resolve_env_key("PROXMOX_SSH_KEY")
        except SshKeyMissing as exc:
            raise HTTPException(503, f"SSH key not available: {exc}") from exc
        return [
            "-i",
            key_path,
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={connect_timeout}",
        ]
    except SshKeyMissing as exc:
        raise HTTPException(503, f"SSH key not available: {exc}") from exc


def _run_pct_command_sync(
    pve_host: str,
    vmid: int,
    pct_args: str,
    *,
    connect_timeout: int = 10,
    command_timeout: int = 30,
) -> subprocess.CompletedProcess[str]:
    ssh_args = _make_pve_ssh_args(pve_host, connect_timeout=connect_timeout)
    cmd = ["ssh"] + ssh_args + [f"root@{pve_host}", f"pct {pct_args} {vmid}"]
    try:
        return subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=command_timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(504, "SSH command timed out") from exc


async def _run_pct_command(
    pve_host: str,
    vmid: int,
    pct_args: str,
    *,
    connect_timeout: int = 10,
    command_timeout: int = 30,
) -> subprocess.CompletedProcess[str]:
    return await _run_nodes_sync_work(
        "pct_command",
        _run_pct_command_sync,
        pve_host,
        vmid,
        pct_args,
        connect_timeout=connect_timeout,
        command_timeout=command_timeout,
    )


@router.get("/{node_id}/pct-status", status_code=200)
async def get_node_pct_status(node_id: str) -> dict:
    """Return the live pct status of the LXC for this node via SSH to its PVE host."""
    try:
        _, pve_host, vmid = await _run_nodes_sync_work(
            "get_lxc_target",
            _get_node_lxc_target,
            node_id,
        )
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        if str(exc.detail).startswith("node "):
            raise
        return {"node_id": node_id, "status": "unknown", "vmid": None, "pve_host": None}

    try:
        result = await _run_pct_command(
            pve_host,
            vmid,
            "status",
            connect_timeout=6,
            command_timeout=8,
        )
    except HTTPException as exc:
        if exc.status_code == 503:
            return {
                "node_id": node_id,
                "status": "unknown",
                "vmid": vmid,
                "pve_host": pve_host,
                "error": str(exc.detail),
            }
        if exc.status_code == 504:
            return {
                "node_id": node_id,
                "status": "unknown",
                "vmid": vmid,
                "pve_host": pve_host,
                "error": "SSH timed out",
            }
        raise
    if result.returncode != 0:
        return {
            "node_id": node_id,
            "status": "unknown",
            "vmid": vmid,
            "pve_host": pve_host,
            "error": result.stderr.strip() or result.stdout.strip() or "pct status failed",
        }

    output = result.stdout.strip().lower()
    if "running" in output:
        status = "running"
    elif "stopped" in output:
        status = "stopped"
    else:
        status = "unknown"

    return {"node_id": node_id, "status": status, "vmid": vmid, "pve_host": pve_host}


@router.post("/{node_id}/pct", status_code=200)
async def node_pct_action(node_id: str, body: PctAction) -> dict:
    """Start, stop, reboot, or force stop/start the named node's LXC."""
    action = body.action.strip().lower()
    if action not in ("start", "stop", "reboot", "force-cycle"):
        raise HTTPException(
            400, f"invalid action '{action}'; must be start, stop, reboot, or force-cycle"
        )

    _, pve_host, vmid = _get_node_lxc_target(node_id)

    outputs: list[str] = []
    if action == "force-cycle":
        stop_result = await _run_pct_command(pve_host, vmid, "stop", command_timeout=45)
        stop_output = stop_result.stderr.strip() or stop_result.stdout.strip()
        outputs.append(stop_output)
        if (
            stop_result.returncode != 0
            and "not running" not in stop_output.lower()
            and "already stopped" not in stop_output.lower()
        ):
            raise HTTPException(
                500,
                f"pct stop {vmid} on {pve_host} failed: {stop_output or '(no output)'}",
            )
        result = await _run_pct_command(pve_host, vmid, "start", command_timeout=45)
        outputs.append(result.stderr.strip() or result.stdout.strip())
    else:
        result = await _run_pct_command(pve_host, vmid, action, command_timeout=45)

    if result.returncode != 0:
        raise HTTPException(
            500,
            f"pct {action} {vmid} on {pve_host} failed: {result.stderr.strip() or '(no output)'}",
        )

    log.info("pct %s %s on %s (node %s) succeeded", action, vmid, pve_host, node_id)
    final_output = "\n".join(line for line in outputs + [result.stdout.strip()] if line)
    return {
        "status": "ok",
        "action": action,
        "vmid": vmid,
        "pve_host": pve_host,
        "output": final_output,
    }


@router.post("", status_code=405)
async def register_node_rejected() -> dict:
    """
    Nodes are now managed via .nodes.json (single source of truth).

    To add or remove a node: edit .nodes.json, then distribute it with
    bp-nodes-push.sh and press Refresh in Settings > Nodes (or restart
    the app).  Programmatic registration via this endpoint is no longer
    supported.
    """
    raise HTTPException(
        405,
        detail=(
            "Node registration via the API is no longer supported. "
            "Edit .nodes.json and distribute via bp-nodes-push.sh."
        ),
    )
