"""routes_disks.py — on-demand storage topology for Probes > Disks.

This page is intentionally refresh-only. It does not start a background poller.
Each request gathers one bounded read-only SSH snapshot per host and overlays
the existing AI Control storage feeds where they add nested-guest context.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import os
import re
import subprocess
import textwrap
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query

from .ssh import SshKeyMissing, resolve_env_key

router = APIRouter(prefix="/disks", tags=["disks"])


def _env_list(name: str, fallback: str = "") -> tuple[str, ...]:
    return tuple(item.strip() for item in os.environ.get(name, fallback).split(",") if item.strip())


_AI_CONTROL_BASE_URL = os.environ.get("DISKS_AI_CONTROL_BASE_URL", "").strip().rstrip("/")
_AI_CONTROL_VERIFY = os.environ.get("DISKS_AI_CONTROL_VERIFY", "false").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_DISK_HOSTS = _env_list("DISKS_HOSTS")
_SMART_HOSTS = set(_env_list("DISKS_SMART_HOSTS")) or set(_DISK_HOSTS)
_NESTED_GUEST_HOST = os.environ.get("DISKS_NESTED_GUEST_HOST", "").strip()
_NESTED_GUEST_PARENT = os.environ.get("DISKS_NESTED_GUEST_PARENT", "").strip()
_HTTP_TIMEOUT = httpx.Timeout(10.0, connect=4.0)
_SSH_CONNECT_TIMEOUT = int(os.environ.get("DISKS_SSH_CONNECT_TIMEOUT_SECONDS", "5"))
_SSH_INVENTORY_TIMEOUT = int(os.environ.get("DISKS_SSH_INVENTORY_TIMEOUT_SECONDS", "18"))
_SSH_SMART_TIMEOUT = int(os.environ.get("DISKS_SSH_SMART_TIMEOUT_SECONDS", "24"))
_SAFE_HOST_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_SAFE_DEVICE_RE = re.compile(r"^/dev/[A-Za-z0-9_./:-]+$")

_REMOTE_INVENTORY_SCRIPT = textwrap.dedent(
    """
    import glob
    import json
    import os
    import re
    import shutil
    import socket
    import subprocess

    PATH_ENV = {"PATH": "/usr/sbin:/sbin:/usr/bin:/bin"}
    GROUP_RE = re.compile(
        r"^(mirror|raidz\\d*|draid\\d*|spare|spares|logs?|cache|special|dedup|replacing)(-|$)"
    )
    DEVICE_SLOT_RE = re.compile(r"^(ide|sata|scsi|virtio|efidisk|tpmstate)\\d+$")


    def run(cmd, timeout=8):
        exe = cmd[0]
        if shutil.which(exe) is None:
            return {
                "ok": False,
                "missing": True,
                "timeout": False,
                "rc": 127,
                "stdout": "",
                "stderr": f"{exe} not found",
            }
        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=PATH_ENV,
            )
            return {
                "ok": proc.returncode == 0,
                "missing": False,
                "timeout": False,
                "rc": proc.returncode,
                "stdout": proc.stdout,
                "stderr": proc.stderr,
            }
        except subprocess.TimeoutExpired as exc:
            return {
                "ok": False,
                "missing": False,
                "timeout": True,
                "rc": 124,
                "stdout": exc.stdout or "",
                "stderr": exc.stderr or "command timed out",
            }


    def parse_zpool_list(text):
        rows = []
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            parts = line.split("\\t")
            if len(parts) != 7:
                continue
            name, size, alloc, free, cap, health, frag = parts

            def to_int(value):
                try:
                    return int(value)
                except Exception:
                    return None

            rows.append(
                {
                    "name": name,
                    "size_bytes": to_int(size),
                    "allocated_bytes": to_int(alloc),
                    "free_bytes": to_int(free),
                    "capacity_pct": to_int(cap.rstrip("%")),
                    "health": health,
                    "fragmentation_pct": to_int(frag.rstrip("%")),
                }
            )
        return rows


    def parse_zfs_list(text):
        rows = []
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            parts = line.split("\\t")
            if len(parts) != 12:
                continue
            (
                name,
                used,
                avail,
                refer,
                ds_type,
                mountpoint,
                volsize,
                encryption,
                keystatus,
                encryptionroot,
                logicalused,
                usedbysnapshots,
            ) = parts

            def to_int(value):
                try:
                    return int(value)
                except Exception:
                    return None

            rows.append(
                {
                    "name": name,
                    "used_bytes": to_int(used),
                    "available_bytes": to_int(avail),
                    "referenced_bytes": to_int(refer),
                    "type": ds_type,
                    "mountpoint": mountpoint,
                    "volsize_bytes": to_int(volsize),
                    "encryption": encryption,
                    "keystatus": keystatus,
                    "encryptionroot": encryptionroot,
                    "logical_used_bytes": to_int(logicalused),
                    "used_by_snapshots_bytes": to_int(usedbysnapshots),
                }
            )
        return rows


    def parse_zpool_members(text):
        pools = {}
        current_pool = None
        in_config = False
        for raw in text.splitlines():
            line = raw.rstrip("\\n")
            stripped = line.strip()
            if stripped.startswith("pool: "):
                current_pool = stripped.split(":", 1)[1].strip()
                pools.setdefault(current_pool, [])
                in_config = False
                continue
            if stripped == "config:":
                in_config = True
                continue
            if not in_config or not current_pool:
                continue
            if not stripped or stripped.startswith("NAME"):
                continue
            if stripped.startswith("errors:"):
                in_config = False
                continue
            name = stripped.split()[0]
            if name == current_pool or GROUP_RE.match(name):
                continue
            if not name.startswith("/"):
                continue
            resolved = os.path.realpath(name)
            pools[current_pool].append(
                {
                    "path": name,
                    "resolved_path": resolved,
                    "leaf_name": os.path.basename(resolved) or os.path.basename(name) or name,
                }
            )
        return pools


    def parse_findmnt(text):
        try:
            body = json.loads(text)
        except Exception:
            return []
        rows = body.get("filesystems")
        return rows if isinstance(rows, list) else []


    def parse_guest_disk_assignments():
        assignments = []
        for path in sorted(glob.glob("/etc/pve/qemu-server/*.conf")):
            vmid = os.path.basename(path).split(".", 1)[0]
            name = ""
            try:
                with open(path, "r", encoding="utf-8", errors="replace") as handle:
                    lines = handle.readlines()
            except Exception:
                continue

            pending = []
            for raw in lines:
                line = raw.strip()
                if not line or line.startswith("#") or ":" not in line:
                    continue
                key, value = line.split(":", 1)
                key = key.strip()
                value = value.strip()
                if key == "name":
                    name = value
                    continue
                if not DEVICE_SLOT_RE.match(key):
                    continue
                source = value.split(",", 1)[0].strip()
                if not source.startswith("/dev/"):
                    continue
                pending.append(
                    {
                        "vmid": vmid,
                        "name": name,
                        "slot": key,
                        "source_path": source,
                        "resolved_path": os.path.realpath(source),
                    }
                )

            if pending and not name:
                for entry in pending:
                    entry["name"] = f"vm-{vmid}"
            assignments.extend(pending)
        return assignments


    lsblk = run(
        [
            "lsblk",
            "--json",
            "-b",
            "-o",
            "NAME,KNAME,PKNAME,TYPE,SIZE,MODEL,SERIAL,VENDOR,ROTA,TRAN,FSTYPE,MOUNTPOINT,PARTLABEL,PARTUUID,UUID,PATH",
        ],
        timeout=8,
    )
    findmnt = run(
        ["findmnt", "-J", "-b", "-o", "SOURCE,TARGET,FSTYPE,SIZE,USED,AVAIL"],
        timeout=8,
    )
    zpool_list = run(["zpool", "list", "-Hp", "-o", "name,size,alloc,free,cap,health,frag"], timeout=8)
    zpool_status = run(["zpool", "status", "-P"], timeout=10)
    zfs_list = run(
        [
            "zfs",
            "list",
            "-Hp",
            "-o",
            "name,used,avail,refer,type,mountpoint,volsize,encryption,keystatus,encryptionroot,logicalused,usedbysnapshots",
        ],
        timeout=10,
    )

    payload = {
        "host": socket.gethostname(),
        "lsblk": json.loads(lsblk["stdout"]) if lsblk["ok"] and lsblk["stdout"].strip() else {"blockdevices": []},
        "mounts": parse_findmnt(findmnt["stdout"]) if findmnt["ok"] else [],
        "zpool_list": parse_zpool_list(zpool_list["stdout"]) if zpool_list["ok"] else [],
        "zpool_members": parse_zpool_members(zpool_status["stdout"]) if zpool_status["ok"] else {},
        "zpool_status_text": zpool_status["stdout"] if zpool_status["ok"] else "",
        "zfs_list": parse_zfs_list(zfs_list["stdout"]) if zfs_list["ok"] else [],
        "guest_disk_assignments": parse_guest_disk_assignments(),
        "commands": {
            "lsblk": lsblk,
            "findmnt": findmnt,
            "zpool_list": zpool_list,
            "zpool_status": zpool_status,
            "zfs_list": zfs_list,
        },
    }
    print(json.dumps(payload))
    """
).strip()


def _safe_host(host: str) -> str:
    value = str(host or "").strip()
    if not value or not _SAFE_HOST_RE.fullmatch(value):
        raise HTTPException(400, "invalid host")
    return value


def _safe_device_path(device_path: str) -> str:
    value = str(device_path or "").strip()
    if not value or not _SAFE_DEVICE_RE.fullmatch(value):
        raise HTTPException(400, "invalid device path")
    return value


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _bytes_from_gb(value: Any) -> int | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(round(number * 1024 * 1024 * 1024))


def _pct(used_bytes: int | None, total_bytes: int | None) -> float | None:
    if used_bytes is None or total_bytes in (None, 0):
        return None
    return round((used_bytes / total_bytes) * 100.0, 1)


def _fact(label: str, value: Any) -> dict[str, str] | None:
    text = str(value or "").strip()
    if not text or text == "-":
        return None
    return {"label": label, "value": text}


def _non_null_facts(*facts: dict[str, str] | None) -> list[dict[str, str]]:
    return [fact for fact in facts if fact is not None]


def _format_bytes(value: int | None) -> str:
    if value is None or value <= 0:
        return "—"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(value)
    unit_idx = 0
    while size >= 1024 and unit_idx < len(units) - 1:
        size /= 1024
        unit_idx += 1
    decimals = 0 if size >= 100 or unit_idx == 0 else 1
    return f"{size:.{decimals}f}{units[unit_idx]}"


def _usage_fields(total_bytes: int | None, used_bytes: int | None) -> dict[str, Any]:
    free_bytes = None
    if total_bytes is not None and used_bytes is not None:
        free_bytes = max(0, total_bytes - used_bytes)
    return {
        "total_bytes": total_bytes,
        "used_bytes": used_bytes,
        "free_bytes": free_bytes,
        "usage_pct": _pct(used_bytes, total_bytes),
    }


def _node(
    node_id: str,
    kind: str,
    label: str,
    *,
    subtitle: str = "",
    status: str = "info",
    note: str = "",
    group: str = "",
    facts: list[dict[str, str]] | None = None,
    children: list[dict[str, Any]] | None = None,
    smart: dict[str, str] | None = None,
    total_bytes: int | None = None,
    used_bytes: int | None = None,
    usage_text: str = "",
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "id": node_id,
        "kind": kind,
        "label": label,
        "subtitle": subtitle,
        "status": status,
        "note": note,
        "group": group,
        "facts": facts or [],
        "children": children or [],
        "smart": smart,
        **_usage_fields(total_bytes, used_bytes),
    }
    if usage_text:
        payload["usage_text"] = usage_text
    if isinstance(meta, dict):
        payload.update(meta)
    return payload


async def _fetch_json(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    response = await client.get(url, headers={"accept": "application/json"})
    response.raise_for_status()
    return response.json()


async def _maybe_fetch_json(client: httpx.AsyncClient, url: str) -> dict[str, Any]:
    try:
        return {"ok": True, "data": await _fetch_json(client, url), "error": ""}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "data": None, "error": str(exc)}


def _ssh_base_command(host: str) -> list[str]:
    try:
        key_path = resolve_env_key("PROXMOX_SSH_KEY")
    except SshKeyMissing as exc:
        raise RuntimeError(f"PROXMOX_SSH_KEY unavailable: {exc}") from exc
    return [
        "ssh",
        "-i",
        key_path,
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        f"ConnectTimeout={_SSH_CONNECT_TIMEOUT}",
        f"root@{host}",
    ]


def _run_inventory_snapshot(host: str) -> dict[str, Any]:
    command = _ssh_base_command(host) + ["python3", "-"]
    try:
        proc = subprocess.run(
            command,
            input=_REMOTE_INVENTORY_SCRIPT,
            capture_output=True,
            text=True,
            timeout=_SSH_INVENTORY_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        return {"ok": False, "host": host, "error": f"inventory timed out: {exc}", "data": None}
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "host": host, "error": str(exc), "data": None}

    if proc.returncode != 0:
        stderr = (proc.stderr or "").strip()
        stdout = (proc.stdout or "").strip()
        detail = stderr or stdout or f"ssh exited {proc.returncode}"
        return {"ok": False, "host": host, "error": detail[:400], "data": None}

    try:
        data = json.loads(proc.stdout)
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "host": host, "error": f"invalid inventory json: {exc}", "data": None}
    return {"ok": True, "host": host, "error": "", "data": data}


async def _inventory_host(host: str) -> dict[str, Any]:
    return await asyncio.to_thread(_run_inventory_snapshot, host)


def _run_smart_snapshot(host: str, device_path: str) -> dict[str, Any]:
    command = _ssh_base_command(host) + ["smartctl", "-a", "-j", device_path]
    try:
        proc = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=_SSH_SMART_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        raise HTTPException(504, f"S.M.A.R.T. timed out for {host}:{device_path}") from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, f"S.M.A.R.T. probe failed: {exc}") from exc

    if proc.returncode not in {0, 4}:
        detail = (proc.stderr or proc.stdout or "").strip()[:500]
        raise HTTPException(502, detail or f"smartctl exited {proc.returncode}")

    try:
        body = json.loads(proc.stdout or "{}")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, f"smartctl returned invalid json: {exc}") from exc
    return body


def _flatten_block_devices(
    blockdevices: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    top_disks: list[dict[str, Any]] = []
    by_path: dict[str, dict[str, Any]] = {}

    def walk(node: dict[str, Any], *, parent_path: str = "", root_disk_path: str = "") -> None:
        path = str(node.get("path") or "").strip()
        node["_parent_path"] = parent_path
        node["_root_disk_path"] = root_disk_path or path
        by_path[path] = node
        children = node.get("children")
        if not isinstance(children, list):
            children = []
            node["children"] = children
        next_root = root_disk_path or path
        for child in children:
            if isinstance(child, dict):
                walk(child, parent_path=path, root_disk_path=next_root)

    for node in blockdevices:
        if not isinstance(node, dict):
            continue
        if str(node.get("type") or "").strip() != "disk":
            continue
        name = str(node.get("name") or "")
        if name.startswith(("loop", "ram", "zram", "sr", "zd", "dm-")):
            continue
        top_disks.append(node)
        walk(node)

    return top_disks, by_path


def _mount_index(mounts: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for mount in mounts:
        if not isinstance(mount, dict):
            continue
        source = str(mount.get("source") or "").strip()
        if source:
            index[source] = mount
    return index


def _pool_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("name") or "").strip(): row
        for row in rows
        if isinstance(row, dict) and str(row.get("name") or "").strip()
    }


def _datasets_by_pool(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        pool_name = name.split("/", 1)[0]
        grouped.setdefault(pool_name, []).append(row)
    return grouped


def _build_pool_member_lookup(snapshot: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    raw = snapshot.get("zpool_members")
    if not isinstance(raw, dict):
        return {}
    return {
        str(pool_name): members
        for pool_name, members in raw.items()
        if isinstance(pool_name, str) and isinstance(members, list)
    }


def _pool_member_used_bytes(pool_row: dict[str, Any], member_size: int | None) -> int | None:
    if member_size is None:
        return None
    cap = _to_int(pool_row.get("capacity_pct"))
    if cap is not None:
        return int(round(member_size * (cap / 100.0)))
    allocated = _to_int(pool_row.get("allocated_bytes"))
    total = _to_int(pool_row.get("size_bytes"))
    if allocated is None or total in (None, 0):
        return None
    return int(round(member_size * (allocated / total)))


def _smart_payload(host: str, device_path: str | None) -> dict[str, str] | None:
    if not device_path:
        return None
    return {"host": host, "device_path": device_path}


def _build_guest_assignment_lookup(snapshot: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    rows = snapshot.get("guest_disk_assignments")
    if not isinstance(rows, list):
        return {}

    lookup: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        source_path = str(row.get("source_path") or "").strip()
        resolved_path = str(row.get("resolved_path") or "").strip()
        for key in {source_path, resolved_path}:
            if not key:
                continue
            lookup.setdefault(key, []).append(row)
    return lookup


def _guest_assignment_target(row: dict[str, Any]) -> str:
    vmid = str(row.get("vmid") or "").strip()
    name = str(row.get("name") or "").strip()
    label = f"VM {vmid}" if vmid else "VM"
    if name and name != f"vm-{vmid}":
        label = f"{label} ({name})"
    return label


def _guest_assignment_display(row: dict[str, Any]) -> str:
    target = _guest_assignment_target(row)
    slot = str(row.get("slot") or "").strip()
    return f"{target} via {slot}" if slot else target


def _guest_assignment_note(assignments: list[dict[str, Any]], *, scope: str = "drive") -> str:
    if not assignments:
        return ""
    labels = [_guest_assignment_display(item) for item in assignments if isinstance(item, dict)]
    if not labels:
        labels = ["a guest"]
    unique_labels = list(dict.fromkeys(labels))
    joined = ", ".join(unique_labels)
    return (
        f"This {scope} is assigned directly to {joined}. "
        "Usage inside the guest is not measured in this host view."
    )


def _guest_assignment_facts(assignments: list[dict[str, Any]]) -> list[dict[str, str]]:
    if not assignments:
        return []
    targets = list(
        dict.fromkeys(
            _guest_assignment_target(item) for item in assignments if isinstance(item, dict)
        )
    )
    slots = list(
        dict.fromkeys(
            str(item.get("slot") or "").strip()
            for item in assignments
            if isinstance(item, dict) and str(item.get("slot") or "").strip()
        )
    )
    return _non_null_facts(
        _fact("Assigned to", ", ".join(targets)),
        _fact("Guest slot", ", ".join(slots)),
        _fact("Assignment", "Raw disk passthrough"),
    )


def _partial_usage_text(
    total_bytes: int | None,
    *,
    known_used_bytes: int | None = None,
    guest_assigned_bytes: int | None = None,
) -> str:
    if (
        known_used_bytes is None
        and guest_assigned_bytes is not None
        and total_bytes is not None
        and guest_assigned_bytes == total_bytes
    ):
        return f"Guest-assigned · {_format_bytes(total_bytes)} total"

    parts: list[str] = []
    if known_used_bytes is not None:
        parts.append(f"{_format_bytes(known_used_bytes)} known here")
    if guest_assigned_bytes is not None:
        parts.append(f"{_format_bytes(guest_assigned_bytes)} guest-assigned")
    if total_bytes is not None:
        parts.append(f"{_format_bytes(total_bytes)} total")
    return " · ".join(parts) if parts else "Usage unavailable"


def _build_guest_overlay_nodes(storage_payload: dict[str, Any]) -> dict[str, Any] | None:
    if not _NESTED_GUEST_HOST:
        return None
    disks = storage_payload.get("disks")
    if not isinstance(disks, list):
        return None

    role_nodes: list[dict[str, Any]] = []
    dataset_roles: dict[str, list[dict[str, Any]]] = {}
    total_bytes = 0
    used_bytes = 0

    for item in disks:
        if not isinstance(item, dict):
            continue
        detail = item.get("detail")
        if not isinstance(detail, dict):
            continue
        source_host = str(detail.get("source_host") or "").strip()
        if source_host != _NESTED_GUEST_HOST:
            continue

        label = str(item.get("name") or "storage").strip() or "storage"
        total = _bytes_from_gb(item.get("total_gb"))
        used = _bytes_from_gb(item.get("used_gb"))
        if total:
            total_bytes += total
        if used:
            used_bytes += used

        local_device = (
            detail.get("local_device") if isinstance(detail.get("local_device"), dict) else {}
        )
        local_model = str(local_device.get("model") or "").strip()
        local_path = str(local_device.get("path") or "").strip()
        local_name = str(local_device.get("name") or "").strip()
        smart = None
        if local_path and "QEMU" not in local_model.upper():
            smart = _smart_payload(_NESTED_GUEST_HOST, local_path)

        facts = _non_null_facts(
            _fact("Guest host", source_host),
            _fact("Mount", detail.get("mount_path")),
            _fact("Guest pool", detail.get("local_pool")),
            _fact("Guest vdev", detail.get("local_vdev")),
            _fact("Device", local_name or local_path),
            _fact("Model", local_model),
        )

        parent_detail = (
            detail.get(_NESTED_GUEST_PARENT)
            if _NESTED_GUEST_PARENT and isinstance(detail.get(_NESTED_GUEST_PARENT), dict)
            else {}
        )
        dataset = (
            parent_detail.get("dataset") if isinstance(parent_detail.get("dataset"), dict) else {}
        )
        dataset_name = str(dataset.get("name") or "").strip()
        if dataset_name:
            facts.append({"label": "Backed by", "value": dataset_name})

        node = _node(
            f"guest:{_NESTED_GUEST_HOST}:{label}",
            "guest-storage",
            label,
            subtitle="Nested guest role",
            status="info",
            group="Roles",
            facts=facts,
            smart=smart,
            total_bytes=total,
            used_bytes=used,
        )
        role_nodes.append(node)
        if dataset_name:
            dataset_roles.setdefault(dataset_name, []).append(node)

    if not role_nodes:
        return None

    guest_node = _node(
        f"guest:{_NESTED_GUEST_HOST}",
        "nested-host",
        _NESTED_GUEST_HOST,
        subtitle="Nested guest view from the existing AI Control storage lane",
        status="info",
        note="Guest totals can overlap the host pool view because this section shows a nested guest perspective.",
        group="Nested guests",
        facts=[{"label": "Source", "value": "AI Control storage overlay"}],
        children=role_nodes,
        total_bytes=total_bytes or None,
        used_bytes=used_bytes or None,
    )
    return {"guest_node": guest_node, "dataset_roles": dataset_roles}


def _build_thunderbolt_overlay(thunderbolt_payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    pools = thunderbolt_payload.get("pools")
    if not isinstance(pools, list):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for pool in pools:
        if not isinstance(pool, dict):
            continue
        name = str(pool.get("name") or pool.get("label") or "").strip()
        if name:
            result[name] = pool
    return result


def _build_dataset_tree(
    host: str,
    pool_name: str,
    datasets: list[dict[str, Any]],
    *,
    dataset_roles: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    children_by_parent: dict[str, list[str]] = {}
    entries: dict[str, dict[str, Any]] = {}

    for row in datasets:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        entries[name] = row
        parent = name.rsplit("/", 1)[0] if "/" in name else ""
        children_by_parent.setdefault(parent, []).append(name)

    def make_dataset_node(name: str) -> dict[str, Any]:
        row = entries[name]
        ds_type = str(row.get("type") or "").strip() or "dataset"
        volsize = _to_int(row.get("volsize_bytes"))
        used = _to_int(row.get("used_bytes"))
        available = _to_int(row.get("available_bytes"))
        total = (
            volsize
            if volsize
            else (used + available if used is not None and available is not None else None)
        )
        facts = _non_null_facts(
            _fact("Type", ds_type),
            _fact("Mount", row.get("mountpoint")),
            _fact("Encryption", row.get("encryption")),
            _fact("Key status", row.get("keystatus")),
            _fact("Encryption root", row.get("encryptionroot")),
        )
        guest_roles = dataset_roles.get(name) or []
        if guest_roles:
            facts.append(
                {"label": "Guest roles", "value": ", ".join(role["label"] for role in guest_roles)}
            )
        child_names = sorted(children_by_parent.get(name, []))
        child_nodes = [make_dataset_node(child_name) for child_name in child_names]
        node = _node(
            f"{host}:dataset:{name}",
            "dataset" if ds_type == "filesystem" else "volume",
            name.split("/")[-1],
            subtitle=name,
            status="info",
            group="Datasets",
            facts=facts,
            children=child_nodes,
            total_bytes=total,
            used_bytes=used,
        )
        return node

    top_level_names = sorted(children_by_parent.get(pool_name, []))
    return [make_dataset_node(name) for name in top_level_names]


def _build_pool_nodes(
    host: str,
    snapshot: dict[str, Any],
    *,
    by_path: dict[str, dict[str, Any]],
    thunderbolt_pools: dict[str, dict[str, Any]],
    dataset_roles: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    pool_rows = snapshot.get("zpool_list")
    if not isinstance(pool_rows, list):
        return []
    dataset_lookup = _datasets_by_pool(
        snapshot.get("zfs_list") if isinstance(snapshot.get("zfs_list"), list) else []
    )
    member_lookup = _build_pool_member_lookup(snapshot)
    pool_nodes: list[dict[str, Any]] = []

    for row in pool_rows:
        if not isinstance(row, dict):
            continue
        pool_name = str(row.get("name") or "").strip()
        if not pool_name:
            continue

        used = _to_int(row.get("allocated_bytes"))
        total = _to_int(row.get("size_bytes"))
        members = member_lookup.get(pool_name) or []
        member_nodes: list[dict[str, Any]] = []
        for member in members:
            if not isinstance(member, dict):
                continue
            resolved_path = str(member.get("resolved_path") or "").strip()
            member_path = str(member.get("path") or "").strip()
            part = by_path.get(resolved_path)
            if part:
                disk = by_path.get(str(part.get("_root_disk_path") or "").strip())
            else:
                disk = None
            member_size = _to_int(part.get("size")) if part else None
            member_used = _pool_member_used_bytes(row, member_size)
            disk_path = str(disk.get("path") or "").strip() if isinstance(disk, dict) else ""
            member_nodes.append(
                _node(
                    f"{host}:pool-member:{pool_name}:{resolved_path or member_path}",
                    "pool-member",
                    str(part.get("name") or member.get("leaf_name") or member_path)
                    if part
                    else str(member.get("leaf_name") or member_path),
                    subtitle=member_path,
                    status="info",
                    group="Members",
                    facts=_non_null_facts(
                        _fact("Host path", member_path),
                        _fact("Resolved", resolved_path),
                        _fact("Disk", disk.get("name") if isinstance(disk, dict) else ""),
                        _fact("Model", disk.get("model") if isinstance(disk, dict) else ""),
                        _fact("Serial", disk.get("serial") if isinstance(disk, dict) else ""),
                    ),
                    smart=_smart_payload(host, disk_path or resolved_path),
                    total_bytes=member_size,
                    used_bytes=member_used,
                )
            )

        tb = thunderbolt_pools.get(pool_name) or {}
        dataset_nodes = _build_dataset_tree(
            host,
            pool_name,
            dataset_lookup.get(pool_name) or [],
            dataset_roles=dataset_roles,
        )
        guest_refs = (
            tb.get("guest_references") if isinstance(tb.get("guest_references"), list) else []
        )
        guest_ref_nodes = [
            _node(
                f"{host}:guest-ref:{pool_name}:{idx}",
                "guest-reference",
                str(ref.get("id") or "guest"),
                subtitle=str(ref.get("line") or "").strip(),
                status="info",
                group="Guest references",
                facts=_non_null_facts(
                    _fact("Kind", ref.get("kind")),
                    _fact("Status", ref.get("status")),
                ),
            )
            for idx, ref in enumerate(guest_refs)
            if isinstance(ref, dict)
        ]

        facts = _non_null_facts(
            _fact("Health", row.get("health")),
            _fact(
                "Fragmentation",
                f"{row.get('fragmentation_pct')}%"
                if row.get("fragmentation_pct") is not None
                else "",
            ),
            _fact("Description", tb.get("description")),
            _fact("Hardware", tb.get("hardware")),
            _fact("Mounted datasets", tb.get("mounted_count")),
        )
        note = ""
        if tb:
            note = str(tb.get("description") or "").strip()
        elif not member_nodes and not dataset_nodes:
            note = "Pool summary is available, but this host did not expose a deeper dataset or member breakdown in the current snapshot."

        pool_nodes.append(
            _node(
                f"{host}:pool:{pool_name}",
                "pool",
                pool_name,
                subtitle="ZFS pool",
                status="ok" if str(row.get("health") or "").upper() == "ONLINE" else "warn",
                note=note,
                group="Logical systems",
                facts=facts,
                children=member_nodes + dataset_nodes + guest_ref_nodes,
                total_bytes=total,
                used_bytes=used,
            )
        )
    return pool_nodes


def _partition_used_bytes(
    part: dict[str, Any],
    *,
    pool_rows: dict[str, dict[str, Any]],
    partition_pools: dict[str, list[str]],
    mount_by_source: dict[str, dict[str, Any]],
) -> int | None:
    path = str(part.get("path") or "").strip()
    mount = mount_by_source.get(path)
    if mount:
        used = _to_int(mount.get("used"))
        if used is not None:
            return used
    pools = partition_pools.get(path) or []
    if len(pools) == 1:
        size = _to_int(part.get("size"))
        pool_row = pool_rows.get(pools[0]) or {}
        return _pool_member_used_bytes(pool_row, size)
    return None


def _build_drive_nodes(
    host: str,
    top_disks: list[dict[str, Any]],
    *,
    by_path: dict[str, dict[str, Any]],
    pool_rows: dict[str, dict[str, Any]],
    partition_pools: dict[str, list[str]],
    mount_by_source: dict[str, dict[str, Any]],
    guest_assignment_lookup: dict[str, list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], dict[str, int | bool]]:
    drive_nodes: list[dict[str, Any]] = []
    rollup = {
        "known_used_bytes": 0,
        "guest_assigned_bytes": 0,
        "has_guest_assigned_usage": False,
    }

    for disk in top_disks:
        disk_path = str(disk.get("path") or "").strip()
        disk_name = str(disk.get("name") or disk_path or "disk")
        disk_total = _to_int(disk.get("size"))
        disk_assignments = guest_assignment_lookup.get(disk_path) or []
        partition_nodes: list[dict[str, Any]] = []
        guest_partition_bytes = 0
        has_guest_partition = False
        children = disk.get("children") if isinstance(disk.get("children"), list) else []
        for part in children:
            if not isinstance(part, dict):
                continue
            part_path = str(part.get("path") or "").strip()
            part_total = _to_int(part.get("size"))
            part_assignments = guest_assignment_lookup.get(part_path) or []
            pools = partition_pools.get(part_path) or []
            mount = mount_by_source.get(part_path) or {}
            used = _partition_used_bytes(
                part,
                pool_rows=pool_rows,
                partition_pools=partition_pools,
                mount_by_source=mount_by_source,
            )
            if part_assignments:
                used = None
                has_guest_partition = True
                guest_partition_bytes += part_total or 0
            facts = _non_null_facts(
                _fact("Path", part_path),
                _fact("Filesystem", part.get("fstype")),
                _fact("Mount", part.get("mountpoint") or mount.get("target")),
                _fact("Part label", part.get("partlabel")),
                _fact("UUID", part.get("uuid")),
                _fact("Pools", ", ".join(pools)),
            )
            facts.extend(_guest_assignment_facts(part_assignments))
            pool_children = [
                _node(
                    f"{host}:partition-pool-link:{part_path}:{pool_name}",
                    "pool-link",
                    pool_name,
                    subtitle="Logical system",
                    status="info",
                    group="Allocations",
                    facts=_non_null_facts(
                        _fact("Path", part_path),
                        _fact("Pool", pool_name),
                    ),
                    total_bytes=_to_int(part.get("size")),
                    used_bytes=used,
                )
                for pool_name in pools
            ]
            partition_nodes.append(
                _node(
                    f"{host}:partition:{part_path}",
                    "partition",
                    str(part.get("name") or part_path),
                    subtitle=str(part.get("mountpoint") or mount.get("target") or "").strip(),
                    status="warn" if part_assignments else "info",
                    note=_guest_assignment_note(part_assignments, scope="partition"),
                    group="Partitions",
                    facts=facts,
                    children=pool_children,
                    total_bytes=part_total,
                    used_bytes=used,
                    usage_text=_partial_usage_text(part_total, guest_assigned_bytes=part_total)
                    if part_assignments
                    else "",
                )
            )

        used = None
        usage_text = ""
        status = "warn" if disk_assignments or has_guest_partition else "info"
        note = _guest_assignment_note(disk_assignments, scope="drive") if disk_assignments else ""
        if partition_nodes:
            part_used = [
                node.get("used_bytes")
                for node in partition_nodes
                if node.get("used_bytes") is not None
            ]
            if has_guest_partition:
                known_used = int(sum(part_used)) if part_used else None
                usage_text = _partial_usage_text(
                    disk_total,
                    known_used_bytes=known_used,
                    guest_assigned_bytes=guest_partition_bytes or None,
                )
            elif part_used:
                used = int(sum(part_used))
        elif mount_by_source.get(disk_path):
            used = _to_int(mount_by_source[disk_path].get("used"))
        if disk_assignments:
            used = None
            usage_text = _partial_usage_text(disk_total, guest_assigned_bytes=disk_total)
        if disk_assignments or has_guest_partition:
            rollup["has_guest_assigned_usage"] = True
            rollup["guest_assigned_bytes"] += (
                (disk_total or 0) if disk_assignments else guest_partition_bytes
            )
        elif used is not None:
            rollup["known_used_bytes"] += used

        transport = str(disk.get("tran") or "").strip()
        subtitle_bits = [
            bit
            for bit in [
                transport.upper() if transport else "",
                str(disk.get("model") or "").strip(),
            ]
            if bit
        ]
        facts = _non_null_facts(
            _fact("Path", disk_path),
            _fact("Model", disk.get("model")),
            _fact("Serial", disk.get("serial")),
            _fact("Vendor", disk.get("vendor")),
            _fact("Transport", transport),
            _fact("Rotational", "yes" if _to_int(disk.get("rota")) == 1 else "no"),
        )
        facts.extend(_guest_assignment_facts(disk_assignments))
        if has_guest_partition and not disk_assignments:
            facts.append(
                {
                    "label": "Guest-assigned partitions",
                    "value": str(
                        sum(1 for node in partition_nodes if node.get("status") == "warn")
                    ),
                }
            )
            note = (
                "One or more partitions are assigned directly to a guest. "
                "Usage inside those guests is not measured in this host view."
            )
        drive_nodes.append(
            _node(
                f"{host}:drive:{disk_path}",
                "drive",
                disk_name,
                subtitle=" · ".join(subtitle_bits),
                status=status,
                note=note,
                group="Physical drives",
                facts=facts,
                children=partition_nodes,
                smart=_smart_payload(host, disk_path),
                total_bytes=disk_total,
                used_bytes=used,
                usage_text=usage_text,
            )
        )

    return drive_nodes, rollup


def _build_host_node(
    host: str,
    snapshot_result: dict[str, Any],
    *,
    guest_overlay: dict[str, Any] | None,
    thunderbolt_pools: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not snapshot_result.get("ok"):
        return _node(
            f"host:{host}",
            "host",
            host,
            subtitle="Inventory unavailable",
            status="fail",
            note=str(snapshot_result.get("error") or "Live host inventory failed."),
            facts=[{"label": "Source", "value": "Live SSH snapshot"}],
        )

    snapshot = snapshot_result.get("data") if isinstance(snapshot_result.get("data"), dict) else {}
    blockdevices = snapshot.get("lsblk", {}).get("blockdevices")
    if not isinstance(blockdevices, list):
        blockdevices = []
    top_disks, by_path = _flatten_block_devices(blockdevices)
    mount_by_source = _mount_index(
        snapshot.get("mounts") if isinstance(snapshot.get("mounts"), list) else []
    )
    member_lookup = _build_pool_member_lookup(snapshot)
    guest_assignment_lookup = _build_guest_assignment_lookup(snapshot)
    partition_pools: dict[str, list[str]] = {}
    for pool_name, members in member_lookup.items():
        for member in members:
            if not isinstance(member, dict):
                continue
            resolved = str(member.get("resolved_path") or "").strip()
            if not resolved:
                continue
            partition_pools.setdefault(resolved, []).append(pool_name)

    pool_rows = _pool_index(
        snapshot.get("zpool_list") if isinstance(snapshot.get("zpool_list"), list) else []
    )
    dataset_roles = guest_overlay.get("dataset_roles") if isinstance(guest_overlay, dict) else {}
    if not isinstance(dataset_roles, dict):
        dataset_roles = {}
    pool_nodes = _build_pool_nodes(
        host,
        snapshot,
        by_path=by_path,
        thunderbolt_pools=thunderbolt_pools,
        dataset_roles=dataset_roles,
    )
    drive_nodes, drive_rollup = _build_drive_nodes(
        host,
        top_disks,
        by_path=by_path,
        pool_rows=pool_rows,
        partition_pools=partition_pools,
        mount_by_source=mount_by_source,
        guest_assignment_lookup=guest_assignment_lookup,
    )

    children = drive_nodes + pool_nodes
    if _NESTED_GUEST_PARENT and host == _NESTED_GUEST_PARENT and isinstance(guest_overlay, dict):
        guest_node = guest_overlay.get("guest_node")
        if isinstance(guest_node, dict):
            children.append(guest_node)

    total = None
    used = None
    usage_text = ""
    if drive_nodes:
        total = int(sum(node.get("total_bytes") or 0 for node in drive_nodes)) or None
        if drive_rollup.get("has_guest_assigned_usage"):
            used = None
            usage_text = _partial_usage_text(
                total,
                known_used_bytes=drive_rollup.get("known_used_bytes") or None,
                guest_assigned_bytes=drive_rollup.get("guest_assigned_bytes") or None,
            )
        elif any(node.get("used_bytes") is not None for node in drive_nodes):
            used = int(sum(node.get("used_bytes") or 0 for node in drive_nodes))
    elif pool_nodes:
        total = int(sum(node.get("total_bytes") or 0 for node in pool_nodes)) or None
        used = int(sum(node.get("used_bytes") or 0 for node in pool_nodes)) or None

    subtitle_bits = []
    if drive_nodes:
        subtitle_bits.append(
            f"{len(drive_nodes)} physical drive{'s' if len(drive_nodes) != 1 else ''}"
        )
    if pool_nodes:
        subtitle_bits.append(
            f"{len(pool_nodes)} logical system{'s' if len(pool_nodes) != 1 else ''}"
        )

    note = "Refreshes only on page open and manual refresh."
    if _NESTED_GUEST_PARENT and host == _NESTED_GUEST_PARENT:
        note = "Host view combines a live SSH storage snapshot with the existing AI Control overlay for nested guest context."
    if drive_rollup.get("has_guest_assigned_usage"):
        note = f"{note} One or more drives are assigned directly to a guest, so usage on that capacity is shown as partial here."

    facts = [{"label": "Source", "value": "Live SSH snapshot"}]
    guest_assigned_total = drive_rollup.get("guest_assigned_bytes") or 0
    if guest_assigned_total:
        facts.append({"label": "Guest-assigned", "value": _format_bytes(int(guest_assigned_total))})

    return _node(
        f"host:{host}",
        "host",
        host,
        subtitle=" · ".join(subtitle_bits),
        status="ok",
        note=note,
        facts=facts,
        children=children,
        total_bytes=total,
        used_bytes=used,
        usage_text=usage_text,
        meta={
            "guest_assigned_bytes": int(guest_assigned_total) or None,
            "known_used_bytes": drive_rollup.get("known_used_bytes") or None,
        },
    )


def _build_topology(
    inventories: list[dict[str, Any]],
    storage_payload: dict[str, Any] | None,
    thunderbolt_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    guest_overlay = _build_guest_overlay_nodes(storage_payload or {})
    thunderbolt_pools = _build_thunderbolt_overlay(thunderbolt_payload or {})
    host_nodes = [
        _build_host_node(
            snapshot_result.get("host") or "host",
            snapshot_result,
            guest_overlay=guest_overlay,
            thunderbolt_pools=thunderbolt_pools,
        )
        for snapshot_result in inventories
    ]

    total = None
    used = None
    usage_text = ""
    if host_nodes and any(node.get("total_bytes") is not None for node in host_nodes):
        total = int(sum(node.get("total_bytes") or 0 for node in host_nodes)) or None
        guest_assigned_total = (
            int(sum(node.get("guest_assigned_bytes") or 0 for node in host_nodes)) or None
        )
        known_used_total = (
            int(sum(node.get("known_used_bytes") or 0 for node in host_nodes)) or None
        )
        if guest_assigned_total:
            used = None
            usage_text = _partial_usage_text(
                total,
                known_used_bytes=known_used_total,
                guest_assigned_bytes=guest_assigned_total,
            )
        else:
            used = (
                int(
                    sum(
                        node.get("used_bytes") or 0
                        for node in host_nodes
                        if node.get("used_bytes") is not None
                    )
                )
                or None
            )

    root = _node(
        "fleet:disks",
        "fleet",
        "Disks",
        subtitle=", ".join(node.get("label") or "host" for node in host_nodes),
        status="info",
        note=(
            "This page is refresh-only. No background disk polling is started here."
            + (
                " Some capacity is passed directly to guests, so fleet usage is shown as partial where needed."
                if usage_text
                else ""
            )
        ),
        children=host_nodes,
        total_bytes=total,
        used_bytes=used,
        usage_text=usage_text,
    )
    return root


@router.get("/topology")
async def disks_topology() -> dict[str, Any]:
    if not _DISK_HOSTS:
        raise HTTPException(503, "DISKS_HOSTS is not configured")
    inventories = await asyncio.gather(*[_inventory_host(host) for host in _DISK_HOSTS])

    if _AI_CONTROL_BASE_URL:
        async with httpx.AsyncClient(
            timeout=_HTTP_TIMEOUT,
            follow_redirects=True,
            verify=_AI_CONTROL_VERIFY,
        ) as client:
            storage_task = _maybe_fetch_json(client, f"{_AI_CONTROL_BASE_URL}/storage")
            thunderbolt_task = _maybe_fetch_json(
                client, f"{_AI_CONTROL_BASE_URL}/thunderbolt/status"
            )
            storage_result, thunderbolt_result = await asyncio.gather(
                storage_task, thunderbolt_task
            )
    else:
        storage_result = {"ok": False, "error": "DISKS_AI_CONTROL_BASE_URL is not configured"}
        thunderbolt_result = {"ok": False, "error": "DISKS_AI_CONTROL_BASE_URL is not configured"}

    root = _build_topology(
        inventories,
        storage_result.get("data") if storage_result.get("ok") else None,
        thunderbolt_result.get("data") if thunderbolt_result.get("ok") else None,
    )
    return {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "root": root,
        "sources": {
            "hosts": inventories,
            "storage_overlay": {
                "ok": bool(storage_result.get("ok")),
                "error": str(storage_result.get("error") or ""),
            },
            "thunderbolt_overlay": {
                "ok": bool(thunderbolt_result.get("ok")),
                "error": str(thunderbolt_result.get("error") or ""),
            },
        },
    }


@router.get("/smart")
async def disks_smart(
    host: str = Query(..., description="Target host label"),
    device_path: str = Query(..., description="Host device path"),
) -> dict[str, Any]:
    clean_host = _safe_host(host)
    clean_device = _safe_device_path(device_path)
    if clean_host not in _SMART_HOSTS:
        raise HTTPException(403, f"S.M.A.R.T. is not allowed for host {clean_host}")

    body = await asyncio.to_thread(_run_smart_snapshot, clean_host, clean_device)
    return {
        "host": clean_host,
        "device_path": clean_device,
        "body": body,
        "_via": {"type": "ssh-smartctl"},
    }
