"""routes_disks.py — on-demand storage topology for Probes > Disks.

This page is intentionally refresh-only. It does not start a background poller.
Each request gathers one bounded read-only SSH snapshot per host and overlays
the existing AI Control storage feeds where they add nested-guest context.
"""

from __future__ import annotations

import asyncio
import base64
import copy
import datetime as dt
import json
import os
import re
import secrets
import subprocess
import textwrap
import threading
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from . import db
from .ssh import SshKeyMissing, resolve_env_key

router = APIRouter(prefix="/disks", tags=["disks"])


class DisksFilesystemTreeBody(BaseModel):
    host: str = Field(min_length=1, max_length=255)
    root_path: str = Field(min_length=1, max_length=2000)
    browse_mode: str | None = Field(default=None, max_length=32)
    source_path: str | None = Field(default=None, max_length=2000)
    path: str | None = Field(default=None, max_length=4000)
    limit: int = Field(default=400, ge=1, le=1000)


class DisksNoteBody(BaseModel):
    node_id: str = Field(min_length=1, max_length=255)
    note: str = Field(default="", max_length=4000)


class DisksOfflineBrowseOpenBody(BaseModel):
    host: str = Field(min_length=1, max_length=255)
    guest_id: str = Field(min_length=1, max_length=32)
    guest_name: str | None = Field(default=None, max_length=255)
    volume_ref: str = Field(min_length=1, max_length=1024)
    volume_label: str | None = Field(default=None, max_length=255)


class DisksOfflineBrowseHeartbeatBody(BaseModel):
    session_id: str = Field(min_length=1, max_length=128)


class DisksOfflineBrowseCloseBody(BaseModel):
    session_id: str = Field(min_length=1, max_length=128)


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
_BROWSE_HOSTS = frozenset(
    {
        host
        for host in (*_DISK_HOSTS, *_SMART_HOSTS, _NESTED_GUEST_HOST, _NESTED_GUEST_PARENT)
        if host
    }
)
_HTTP_TIMEOUT = httpx.Timeout(10.0, connect=4.0)
_SSH_CONNECT_TIMEOUT = int(os.environ.get("DISKS_SSH_CONNECT_TIMEOUT_SECONDS", "5"))
_SSH_INVENTORY_TIMEOUT = int(os.environ.get("DISKS_SSH_INVENTORY_TIMEOUT_SECONDS", "18"))
_SSH_SMART_TIMEOUT = int(os.environ.get("DISKS_SSH_SMART_TIMEOUT_SECONDS", "24"))
_BROWSE_MOUNT_PREFIXES = ("/tmp/disks-browse-", "/run/disks-browse-")
_SAFE_HOST_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
_SAFE_DEVICE_RE = re.compile(r"^/dev/[A-Za-z0-9_./:-]+$")
_SAFE_VOLUME_REF_RE = re.compile(r"^[A-Za-z0-9_./:@+-]+$")
_FILESYSTEM_MEMBER_TYPES = {"swap", "zfs_member", "linux_raid_member", "lvm2_member", "crypto_luks"}
_GENERIC_PARTLABELS = {"basic data partition", "microsoft reserved partition", "primary"}
_STANDALONE_LOGICAL_MIN_BYTES = 2 * 1024**3
_GUEST_VOLUME_LABEL_RE = re.compile(
    r"^(?P<prefix>vm|base|subvol)-(?P<guest_id>\d+)-(?P<suffix>.+)$",
    re.IGNORECASE,
)
_DISKS_INVENTORY_MEMORY_PATH = Path(
    os.environ.get(
        "DISKS_INVENTORY_MEMORY_PATH",
        "/xarta-node/.lone-wolf/runtime/disks-inventory-memory.json",
    )
).expanduser()
_DISKS_LAYOUT_HINTS_PATH = Path(
    os.environ.get(
        "DISKS_LAYOUT_HINTS_PATH",
        "/xarta-node/.lone-wolf/config/disks-layout-hints.json",
    )
).expanduser()
_DISKS_NOTES_MAX_LENGTH = int(os.environ.get("DISKS_NOTES_MAX_LENGTH", "4000"))
_DISKS_OFFLINE_BROWSE_STATE_PATH = Path(
    os.environ.get(
        "DISKS_OFFLINE_BROWSE_STATE_PATH",
        "/xarta-node/.lone-wolf/runtime/disks-offline-browse-sessions.json",
    )
).expanduser()
_DISKS_OFFLINE_BROWSE_TIMEOUT_SECONDS = max(
    10,
    int(os.environ.get("DISKS_OFFLINE_BROWSE_TIMEOUT_SECONDS", "20")),
)
_DISKS_OFFLINE_BROWSE_REAPER_INTERVAL_SECONDS = max(
    3,
    int(os.environ.get("DISKS_OFFLINE_BROWSE_REAPER_INTERVAL_SECONDS", "5")),
)
_DISKS_OFFLINE_BROWSE_LOCK = threading.Lock()
_DISKS_INVENTORY_MEMORY_GROUPS = ("Physical drives", "Logical systems")
_DISKS_INVENTORY_MEMORY_LOCK = threading.Lock()

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
    BROWSEABLE_VOLUME_SLOT_RE = re.compile(r"^(ide|sata|scsi|virtio)\\d+$")
    HOSTPCI_SLOT_RE = re.compile(r"^hostpci\\d+$")
    NUMBER_RE = re.compile(r"(-?\\d+)")


    def parse_guest_identities():
        identities = []

        def collect(pattern, guest_type):
            for path in sorted(glob.glob(pattern)):
                guest_id = os.path.basename(path).split(".", 1)[0]
                name = ""
                template = False
                try:
                    with open(path, "r", encoding="utf-8", errors="replace") as handle:
                        lines = handle.readlines()
                except Exception:
                    continue

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
                    if guest_type == "ct" and key == "hostname" and not name:
                        name = value
                        continue
                    if guest_type == "vm" and key == "template":
                        template = value.lower() in {"1", "yes", "true", "on"}

                if not name:
                    name = f"{guest_type}-{guest_id}"
                identities.append(
                    {
                        "vmid": guest_id,
                        "name": name,
                        "guest_type": "template" if guest_type == "vm" and template else guest_type,
                    }
                )

        collect("/etc/pve/qemu-server/*.conf", "vm")
        collect("/etc/pve/lxc/*.conf", "ct")
        return identities


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


    def annotate_mount_rows(rows):
        annotated = []
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict):
                continue
            source = str(row.get("source") or "").strip()
            clone = dict(row)
            if source.startswith("/dev/"):
                clone["resolved_source"] = os.path.realpath(source)
            children = row.get("children")
            if isinstance(children, list):
                clone["children"] = annotate_mount_rows(children)
            annotated.append(clone)
        return annotated


    def iter_lsblk_nodes(nodes):
        for node in nodes if isinstance(nodes, list) else []:
            if not isinstance(node, dict):
                continue
            yield node
            children = node.get("children")
            if isinstance(children, list):
                yield from iter_lsblk_nodes(children)


    def parse_first_int(text):
        match = NUMBER_RE.search(str(text or ""))
        if not match:
            return None
        try:
            return int(match.group(1))
        except Exception:
            return None


    def normalize_pci_base(value):
        token = str(value or "").strip()
        if not token:
            return ""
        token = token.split(",", 1)[0].split(";", 1)[0].strip()
        if token.startswith("host="):
            token = token[5:].strip()
        if not token:
            return ""
        if token.count(":") == 1:
            token = f"0000:{token}"
        elif token.count(":") == 2 and not token.startswith("0000:"):
            token = f"0000:{token}"
        if "." in token:
            token = token.rsplit(".", 1)[0]
        return token


    def pci_attr(bdf, attr):
        path = f"/sys/bus/pci/devices/{bdf}/{attr}"
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as handle:
                return handle.read().strip().removeprefix("0x")
        except Exception:
            return ""


    def pci_identity(base):
        bdf = f"{base}.0"
        if not os.path.isdir(f"/sys/bus/pci/devices/{bdf}"):
            return {
                "pci_base": base,
                "pci_bdf": bdf,
                "vendor_id": "",
                "device_id": "",
                "class_id": "",
                "subsystem_vendor_id": "",
                "subsystem_device_id": "",
                "driver": "",
                "description": "",
            }
        driver = ""
        driver_link = f"/sys/bus/pci/devices/{bdf}/driver"
        if os.path.islink(driver_link):
            driver = os.path.basename(os.path.realpath(driver_link))
        description = ""
        lspci = run(["lspci", "-nnD", "-s", base], timeout=3)
        if lspci["ok"]:
            description = (lspci["stdout"].splitlines() or [""])[0].strip()
        return {
            "pci_base": base,
            "pci_bdf": bdf,
            "vendor_id": pci_attr(bdf, "vendor"),
            "device_id": pci_attr(bdf, "device"),
            "class_id": pci_attr(bdf, "class"),
            "subsystem_vendor_id": pci_attr(bdf, "subsystem_vendor"),
            "subsystem_device_id": pci_attr(bdf, "subsystem_device"),
            "driver": driver,
            "description": description,
        }


    def probe_ext_usage(path):
        result = run(["tune2fs", "-l", path], timeout=3)
        if not result["ok"]:
            return None
        fields = {}
        for raw in result["stdout"].splitlines():
            if ":" not in raw:
                continue
            key, value = raw.split(":", 1)
            fields[key.strip().lower()] = parse_first_int(value)
        block_count = fields.get("block count")
        free_blocks = fields.get("free blocks")
        block_size = fields.get("block size")
        if None in {block_count, free_blocks, block_size}:
            return None
        total_bytes = int(block_count * block_size)
        used_bytes = int(max(0, block_count - free_blocks) * block_size)
        return {
            "strategy": "tune2fs",
            "total_bytes": total_bytes,
            "used_bytes": used_bytes,
        }


    def probe_ntfs_usage(path):
        result = run(["ntfsinfo", "-m", path], timeout=3)
        if not result["ok"]:
            return None
        fields = {}
        for raw in result["stdout"].splitlines():
            if ":" not in raw:
                continue
            key, value = raw.split(":", 1)
            fields[key.strip().lower()] = parse_first_int(value)
        cluster_size = fields.get("cluster size")
        total_clusters = fields.get("volume size in clusters")
        free_clusters = fields.get("free clusters")
        if None in {cluster_size, total_clusters, free_clusters}:
            return None
        total_bytes = int(total_clusters * cluster_size)
        used_bytes = int(max(0, total_clusters - free_clusters) * cluster_size)
        return {
            "strategy": "ntfsinfo",
            "total_bytes": total_bytes,
            "used_bytes": used_bytes,
        }


    def probe_exfat_usage(path):
        result = run(["dump.exfat", path], timeout=3)
        if not result["ok"]:
            return None
        fields = {}
        volume_label = ""
        for raw in result["stdout"].splitlines():
            if ":" not in raw:
                continue
            key, value = raw.split(":", 1)
            key_name = key.strip().lower()
            fields[key_name] = parse_first_int(value)
            if key_name == "volume label":
                volume_label = value.strip()
        cluster_size = fields.get("cluster size")
        total_clusters = fields.get("total clusters")
        free_clusters = fields.get("free clusters")
        if None in {cluster_size, total_clusters, free_clusters}:
            return None
        total_bytes = int(total_clusters * cluster_size)
        used_bytes = int(max(0, total_clusters - free_clusters) * cluster_size)
        return {
            "strategy": "dump.exfat",
            "total_bytes": total_bytes,
            "used_bytes": used_bytes,
            "volume_label": volume_label,
        }


    def collect_filesystem_usage_probes(blockdevices, mounts):
        mounted_sources = set()

        def walk_mounts(rows):
            for row in rows if isinstance(rows, list) else []:
                if not isinstance(row, dict):
                    continue
                for key in ("source", "resolved_source"):
                    value = str(row.get(key) or "").strip()
                    if value:
                        mounted_sources.add(value)
                children = row.get("children")
                if isinstance(children, list):
                    walk_mounts(children)

        walk_mounts(mounts)
        probes = {}
        for node in iter_lsblk_nodes(blockdevices):
            path = str(node.get("path") or "").strip()
            if not path:
                continue
            fstype = str(node.get("fstype") or "").strip().lower()
            if not fstype or fstype in {"swap", "zfs_member", "linux_raid_member", "lvm2_member", "crypto_luks"}:
                continue
            if path in mounted_sources:
                continue
            resolved_path = os.path.realpath(path)
            if resolved_path and resolved_path in mounted_sources:
                continue
            probe = None
            if fstype in {"ext2", "ext3", "ext4"}:
                probe = probe_ext_usage(path)
            elif fstype in {"ntfs", "ntfs3"}:
                probe = probe_ntfs_usage(path)
            elif fstype == "exfat":
                probe = probe_exfat_usage(path)
            if not isinstance(probe, dict):
                continue
            probes[path] = {
                "path": path,
                "fstype": fstype,
                "total_bytes": probe.get("total_bytes"),
                "used_bytes": probe.get("used_bytes"),
                "strategy": probe.get("strategy"),
                "volume_label": probe.get("volume_label"),
            }
        return probes


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
                        "guest_type": "vm",
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


    def parse_guest_volume_assignments():
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
                if not BROWSEABLE_VOLUME_SLOT_RE.match(key):
                    continue
                source = value.split(",", 1)[0].strip()
                if not source or source.startswith("/dev/"):
                    continue
                lower_source = source.lower()
                lower_value = value.lower()
                if source in {"none", "cdrom"}:
                    continue
                if "media=cdrom" in lower_value or lower_source.endswith(".iso"):
                    continue
                storage = ""
                volume_name = ""
                if ":" in source and not source.startswith("/"):
                    storage, volume_name = source.split(":", 1)
                else:
                    volume_name = os.path.basename(source)
                pending.append(
                    {
                        "vmid": vmid,
                        "name": name,
                        "guest_type": "vm",
                        "slot": key,
                        "volume_ref": source,
                        "storage": storage,
                        "volume_name": volume_name,
                        "volume_leaf": os.path.basename(volume_name.rstrip("/")) or volume_name,
                    }
                )

            if pending and not name:
                for entry in pending:
                    entry["name"] = f"vm-{vmid}"
            assignments.extend(pending)
        return assignments


    def parse_hostpci_assignments():
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
                if not HOSTPCI_SLOT_RE.match(key):
                    continue
                base = normalize_pci_base(value)
                if not base:
                    continue
                pending.append(
                    {
                        "vmid": vmid,
                        "name": name,
                        "slot": key,
                        "raw": value,
                        **pci_identity(base),
                    }
                )

            if pending and not name:
                for entry in pending:
                    entry["name"] = f"vm-{vmid}"
            assignments.extend(pending)
        return assignments


    def parse_storage_cfg_pbs():
        try:
            with open("/etc/pve/storage.cfg", "r", encoding="utf-8", errors="replace") as handle:
                lines = handle.readlines()
        except Exception:
            return []

        rows = []
        current = None
        for raw in lines:
            line = raw.rstrip("\\n")
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if not line[:1].isspace():
                kind, _, name = stripped.partition(":")
                kind = kind.strip().lower()
                name = name.strip()
                current = {"type": kind, "name": name} if kind and name else None
                if current and kind == "pbs":
                    rows.append(current)
                continue
            if not current or current.get("type") != "pbs":
                continue
            key, _, value = stripped.partition(" ")
            key = key.strip().lower()
            value = value.strip()
            if key:
                current[key] = value
        return rows


    def parse_pvesm_status(text):
        rows = []
        for raw in text.splitlines():
            line = raw.strip()
            if not line or line.lower().startswith("name "):
                continue
            parts = line.split()
            if len(parts) < 7:
                continue
            name, storage_type, status = parts[:3]
            total, used, avail = parts[3:6]
            pct = parts[6]

            def to_int(value):
                try:
                    return int(value)
                except Exception:
                    return None

            def to_float(value):
                text = str(value or "").strip().rstrip("%")
                try:
                    return float(text)
                except Exception:
                    return None

            rows.append(
                {
                    "name": name,
                    "type": storage_type,
                    "status": status,
                    "total_kib": to_int(total),
                    "used_kib": to_int(used),
                    "available_kib": to_int(avail),
                    "pct": to_float(pct),
                }
            )
        return rows


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
    pvesm_status = run(["pvesm", "status", "--enabled", "1"], timeout=8)

    mount_rows = annotate_mount_rows(parse_findmnt(findmnt["stdout"])) if findmnt["ok"] else []
    lsblk_payload = (
        json.loads(lsblk["stdout"]) if lsblk["ok"] and lsblk["stdout"].strip() else {"blockdevices": []}
    )
    blockdevices = lsblk_payload.get("blockdevices") if isinstance(lsblk_payload, dict) else []

    payload = {
        "host": socket.gethostname(),
        "lsblk": lsblk_payload,
        "mounts": mount_rows,
        "filesystem_usage_probes": collect_filesystem_usage_probes(blockdevices, mount_rows),
        "zpool_list": parse_zpool_list(zpool_list["stdout"]) if zpool_list["ok"] else [],
        "zpool_members": parse_zpool_members(zpool_status["stdout"]) if zpool_status["ok"] else {},
        "zpool_status_text": zpool_status["stdout"] if zpool_status["ok"] else "",
        "zfs_list": parse_zfs_list(zfs_list["stdout"]) if zfs_list["ok"] else [],
        "guest_identities": parse_guest_identities(),
        "guest_disk_assignments": parse_guest_disk_assignments(),
        "guest_volume_assignments": parse_guest_volume_assignments(),
        "hostpci_assignments": parse_hostpci_assignments(),
        "pbs_storages": parse_storage_cfg_pbs(),
        "pvesm_status": parse_pvesm_status(pvesm_status["stdout"]) if pvesm_status["ok"] else [],
        "commands": {
            "lsblk": lsblk,
            "findmnt": findmnt,
            "zpool_list": zpool_list,
            "zpool_status": zpool_status,
            "zfs_list": zfs_list,
            "pvesm_status": pvesm_status,
        },
    }
    print(json.dumps(payload))
    """
).strip()

_REMOTE_NESTED_GUEST_FS_SCRIPT = textwrap.dedent(
    """
    import json
    import shutil
    import subprocess

    PATH_ENV = {"PATH": "/usr/sbin:/sbin:/usr/bin:/bin"}


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


    lsblk = run(
        [
            "lsblk",
            "--json",
            "-b",
            "-o",
            "NAME,PKNAME,TYPE,SIZE,MODEL,SERIAL,ROTA,TRAN,FSTYPE,LABEL,PARTLABEL,UUID,MOUNTPOINT,PATH",
        ],
        timeout=8,
    )
    lsblk_payload = (
        json.loads(lsblk["stdout"])
        if lsblk["ok"] and lsblk["stdout"].strip()
        else {"blockdevices": []}
    )
    print(
        json.dumps(
            {
                "lsblk": lsblk_payload,
                "commands": {"lsblk": lsblk},
            }
        )
    )
    """
).strip()

_REMOTE_FILESYSTEM_TREE_SCRIPT = textwrap.dedent(
    """
    import base64
    import datetime as dt
    import json
    import os
    from pathlib import Path
    import subprocess
    import sys
    import tempfile

    PAYLOAD_ARG = (sys.argv[1] if len(sys.argv) > 1 else "").strip()


    def fail(message, code=1):
        print(json.dumps({"ok": False, "error": message}))
        raise SystemExit(code)


    def decode_payload(value):
        text = str(value or "").strip()
        if not text:
            fail("Filesystem tree payload is missing")
        try:
            padding = "=" * (-len(text) % 4)
            raw = base64.urlsafe_b64decode((text + padding).encode("ascii"))
            body = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            fail(f"Filesystem tree payload is invalid: {exc}")
        if not isinstance(body, dict):
            fail("Filesystem tree payload is invalid")
        return body


    def normalize_mode(value):
        text = str(value or "").strip().lower()
        return "device_ro" if text == "device_ro" else "mounted"


    def normalize_relative(path):
        text = str(path or "").strip().replace("\\\\", "/")
        if not text or text == ".":
            return "."
        if text.startswith("/"):
            fail("Path must be relative to the filesystem root")
        parts = []
        for part in text.split("/"):
            if part in {"", "."}:
                continue
            if part == "..":
                fail("Path escapes filesystem root")
            parts.append(part)
        return "/".join(parts) if parts else "."


    def within_root(candidate, root):
        try:
            candidate.relative_to(root)
            return True
        except Exception:
            return False


    def rel_path(root, target):
        if target == root:
            return "."
        return target.relative_to(root).as_posix()


    def breadcrumbs(relative_path):
        items = [{"label": "root", "path": "."}]
        if relative_path == ".":
            return items
        running = []
        for part in Path(relative_path).parts:
            running.append(part)
            items.append({"label": part, "path": "/".join(running)})
        return items


    def display_absolute(root_display, relative_path):
        if relative_path == ".":
            return root_display
        if root_display == "/":
            return f"/{relative_path}"
        return f"{root_display.rstrip('/')}/{relative_path}"


    payload = decode_payload(PAYLOAD_ARG)
    mode = normalize_mode(payload.get("browse_mode"))
    SOURCE_ARG = str(payload.get("source_path") or "").strip()
    ROOT_ARG = str(payload.get("root_path") or "").strip()
    REL_ARG = str(payload.get("relative_path") or "").strip()

    try:
        limit = int(payload.get("limit"))
    except Exception:
        limit = 400
    limit = max(1, min(limit, 1000))

    relative_path = normalize_relative(REL_ARG)
    mount_root = None
    mounted = False

    try:
        if mode == "device_ro":
            if not SOURCE_ARG.startswith("/dev/"):
                fail("Filesystem source must be a device path")
            mount_root = Path(tempfile.mkdtemp(prefix="disks-browse-"))
            mount_proc = subprocess.run(
                ["mount", "-o", "ro", SOURCE_ARG, str(mount_root)],
                capture_output=True,
                text=True,
            )
            if mount_proc.returncode != 0:
                detail = (
                    (mount_proc.stderr or "").strip()
                    or (mount_proc.stdout or "").strip()
                    or f"mount exited {mount_proc.returncode}"
                )
                fail(f"Could not mount filesystem read-only: {detail}")
            mounted = True
            root_input = "/"
            root_display = "/"
            root = mount_root.resolve()
        else:
            if not ROOT_ARG.startswith("/"):
                fail("Filesystem root must be an absolute path")
            root_input = ROOT_ARG.rstrip("/") or "/"
            root_display = root_input
            root = Path(root_input).resolve()
            if not root.exists():
                fail(f"Filesystem root does not exist: {ROOT_ARG}")
            if not root.is_dir():
                fail(f"Filesystem root is not a directory: {ROOT_ARG}")
            if not os.path.ismount(root):
                fail(f"Filesystem root is not a mounted path: {ROOT_ARG}")

        current = root if relative_path == "." else (root / relative_path).resolve()
        if not within_root(current, root):
            fail("Path escapes filesystem root")
        if not current.exists():
            fail(f"Folder does not exist: {relative_path}")
        if not current.is_dir():
            fail(f"Not a directory: {relative_path}")

        entries = []
        total_entries = 0
        with os.scandir(current) as iterator:
            for entry in iterator:
                total_entries += 1
                if len(entries) >= limit:
                    continue
                lexical_path = current / entry.name
                entry_rel = rel_path(root, lexical_path)
                try:
                    is_symlink = entry.is_symlink()
                except OSError:
                    is_symlink = False
                try:
                    entry_type = "other"
                    browseable = False
                    symlink_target = ""
                    stat_obj = None
                    if is_symlink:
                        resolved = lexical_path.resolve()
                        if within_root(resolved, root):
                            symlink_target = rel_path(root, resolved)
                            if resolved.is_dir():
                                entry_type = "folder"
                                browseable = True
                            elif resolved.is_file():
                                entry_type = "file"
                            else:
                                entry_type = "link"
                        else:
                            entry_type = "link"
                        stat_obj = entry.stat(follow_symlinks=True)
                    elif entry.is_dir(follow_symlinks=False):
                        entry_type = "folder"
                        browseable = True
                        stat_obj = entry.stat(follow_symlinks=False)
                    elif entry.is_file(follow_symlinks=False):
                        entry_type = "file"
                        stat_obj = entry.stat(follow_symlinks=False)
                    else:
                        stat_obj = entry.stat(follow_symlinks=False)
                    entries.append(
                        {
                            "name": entry.name,
                            "path": entry_rel,
                            "type": entry_type,
                            "browseable": browseable,
                            "symlink": is_symlink,
                            "symlink_target": symlink_target,
                            "size_bytes": (
                                int(stat_obj.st_size) if stat_obj and entry_type == "file" else None
                            ),
                            "modified_at": (
                                dt.datetime.fromtimestamp(
                                    stat_obj.st_mtime,
                                    dt.timezone.utc,
                                ).isoformat()
                                if stat_obj
                                else ""
                            ),
                        }
                    )
                except OSError as exc:
                    entries.append(
                        {
                            "name": entry.name,
                            "path": entry_rel,
                            "type": "other",
                            "browseable": False,
                            "symlink": is_symlink,
                            "symlink_target": "",
                            "size_bytes": None,
                            "modified_at": "",
                            "error": str(exc),
                        }
                    )

        entries.sort(
            key=lambda item: (
                0
                if item["type"] == "folder"
                else 1
                if item["type"] == "file"
                else 2,
                item["name"].lower(),
            )
        )
        parent_path = None
        if relative_path != ".":
            parent = Path(relative_path).parent.as_posix()
            parent_path = "." if parent in {"", "."} else parent

        print(
            json.dumps(
                {
                    "ok": True,
                    "root_path": root_input,
                    "current_path": relative_path,
                    "current_absolute_path": display_absolute(root_display, relative_path),
                    "parent_path": parent_path,
                    "breadcrumbs": breadcrumbs(relative_path),
                    "entries": entries,
                    "entry_count": total_entries,
                    "returned_count": len(entries),
                    "truncated": total_entries > len(entries),
                }
            )
        )
    finally:
        if mounted and mount_root is not None:
            subprocess.run(
                ["umount", str(mount_root)],
                capture_output=True,
                text=True,
                check=False,
            )
        if mount_root is not None:
            try:
                mount_root.rmdir()
            except OSError:
                pass
    """
).strip()

_REMOTE_OFFLINE_VM_PREPARE_SCRIPT = textwrap.dedent(
    """
    import base64
    import json
    import os
    import shutil
    import subprocess
    import sys

    PATH_ENV = {"PATH": "/usr/sbin:/sbin:/usr/bin:/bin"}


    def run(cmd, timeout=12):
        exe = cmd[0]
        if shutil.which(exe) is None:
            return {
                "ok": False,
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
                "rc": proc.returncode,
                "stdout": proc.stdout or "",
                "stderr": proc.stderr or "",
            }
        except subprocess.TimeoutExpired as exc:
            return {
                "ok": False,
                "rc": 124,
                "stdout": exc.stdout or "",
                "stderr": exc.stderr or f"timed out after {timeout}s",
            }


    def fail(message):
        print(json.dumps({"ok": False, "error": str(message)}))
        raise SystemExit(0)


    def decode_payload(value):
        text = str(value or "").strip()
        if not text:
            fail("Offline browse payload is missing")
        try:
            padding = "=" * (-len(text) % 4)
            raw = base64.urlsafe_b64decode((text + padding).encode("ascii"))
            payload = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            fail(f"Offline browse payload is invalid: {exc}")
        if not isinstance(payload, dict):
            fail("Offline browse payload is invalid")
        return payload


    def next_free_nbd():
        for idx in range(32):
            name = f"nbd{idx}"
            if not os.path.exists(f"/sys/class/block/{name}"):
                continue
            if os.path.exists(f"/sys/class/block/{name}/pid"):
                continue
            return f"/dev/{name}"
        return ""


    payload = decode_payload(sys.argv[1] if len(sys.argv) > 1 else "")
    guest_id = str(payload.get("guest_id") or "").strip()
    volume_ref = str(payload.get("volume_ref") or "").strip()
    if not guest_id.isdigit():
        fail("guest_id must be numeric")
    if not volume_ref:
        fail("volume_ref is required")

    status = run(["qm", "status", guest_id], timeout=8)
    status_text = (status.get("stdout") or "").strip()
    if not status["ok"]:
        detail = (status.get("stderr") or "").strip() or status_text or "qm status failed"
        fail(f"Could not inspect VM {guest_id}: {detail}")
    if "status: stopped" not in status_text.lower():
        fail(f"VM {guest_id} must be stopped before offline browse")

    if volume_ref.startswith("/"):
        resolved_path = volume_ref
    else:
        path_result = run(["pvesm", "path", volume_ref], timeout=12)
        if not path_result["ok"]:
            detail = (path_result.get("stderr") or "").strip() or (path_result.get("stdout") or "").strip()
            fail(f"Could not resolve {volume_ref}: {detail or 'pvesm path failed'}")
        resolved_path = (path_result.get("stdout") or "").strip()

    if not resolved_path or not os.path.exists(resolved_path):
        fail(f"Resolved volume path does not exist: {resolved_path or volume_ref}")

    modprobe = run(["modprobe", "nbd", "max_part=16"], timeout=8)
    if not modprobe["ok"]:
        detail = (modprobe.get("stderr") or "").strip() or (modprobe.get("stdout") or "").strip()
        fail(f"Could not load nbd module: {detail or 'modprobe failed'}")

    nbd_device = next_free_nbd()
    if not nbd_device:
        fail("No free /dev/nbd device is available on the host")

    print(
        json.dumps(
            {
                "ok": True,
                "guest_id": guest_id,
                "volume_ref": volume_ref,
                "resolved_path": resolved_path,
                "nbd_device": nbd_device,
                "vm_status": status_text,
            }
        )
    )
    """
).strip()

_REMOTE_OFFLINE_VM_ATTACH_SCRIPT = textwrap.dedent(
    """
    import base64
    import json
    import os
    import shutil
    import subprocess
    import sys
    import time

    PATH_ENV = {"PATH": "/usr/sbin:/sbin:/usr/bin:/bin"}
    MEMBER_TYPES = {"swap", "zfs_member", "linux_raid_member", "lvm2_member", "crypto_luks"}


    def run(cmd, timeout=12):
        exe = cmd[0]
        if shutil.which(exe) is None:
            return {
                "ok": False,
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
                "rc": proc.returncode,
                "stdout": proc.stdout or "",
                "stderr": proc.stderr or "",
            }
        except subprocess.TimeoutExpired as exc:
            return {
                "ok": False,
                "rc": 124,
                "stdout": exc.stdout or "",
                "stderr": exc.stderr or f"timed out after {timeout}s",
            }


    def fail(message):
        print(json.dumps({"ok": False, "error": str(message)}))
        raise SystemExit(0)


    def decode_payload(value):
        text = str(value or "").strip()
        if not text:
            fail("Offline browse payload is missing")
        try:
            padding = "=" * (-len(text) % 4)
            raw = base64.urlsafe_b64decode((text + padding).encode("ascii"))
            payload = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            fail(f"Offline browse payload is invalid: {exc}")
        if not isinstance(payload, dict):
            fail("Offline browse payload is invalid")
        return payload


    def cleanup(device):
        if not device:
            return
        disconnect = run(["qemu-nbd", "--disconnect", device], timeout=10)
        if not disconnect["ok"] and "not connected" not in (disconnect.get("stderr") or "").lower():
            pass


    def iter_nodes(nodes):
        for node in nodes if isinstance(nodes, list) else []:
            if not isinstance(node, dict):
                continue
            yield node
            children = node.get("children")
            if isinstance(children, list):
                yield from iter_nodes(children)


    payload = decode_payload(sys.argv[1] if len(sys.argv) > 1 else "")
    resolved_path = str(payload.get("resolved_path") or "").strip()
    nbd_device = str(payload.get("nbd_device") or "").strip()
    if not resolved_path or not os.path.exists(resolved_path):
        fail("Resolved volume path is missing")
    if not nbd_device.startswith("/dev/nbd"):
        fail("nbd_device is invalid")
    if os.path.exists(f"/sys/class/block/{os.path.basename(nbd_device)}/pid"):
        fail(f"{nbd_device} is already in use")

    try:
        connect = run(["qemu-nbd", "--read-only", "--connect", nbd_device, resolved_path], timeout=16)
        if not connect["ok"]:
            detail = (connect.get("stderr") or "").strip() or (connect.get("stdout") or "").strip()
            fail(f"Could not attach {resolved_path}: {detail or 'qemu-nbd failed'}")

        settle = run(["udevadm", "settle"], timeout=8)
        if not settle["ok"]:
            time.sleep(1.0)

        lsblk = run(
            [
                "lsblk",
                "--json",
                "-b",
                "-o",
                "NAME,KNAME,PATH,TYPE,SIZE,FSTYPE,LABEL,PARTLABEL,UUID,MOUNTPOINT",
                nbd_device,
            ],
            timeout=10,
        )
        if not lsblk["ok"] or not (lsblk.get("stdout") or "").strip():
            detail = (lsblk.get("stderr") or "").strip() or (lsblk.get("stdout") or "").strip()
            fail(f"Could not inspect attached disk: {detail or 'lsblk failed'}")
        try:
            payload = json.loads(lsblk["stdout"])
        except Exception as exc:
            fail(f"Attached disk inventory was invalid JSON: {exc}")

        blockdevices = payload.get("blockdevices") if isinstance(payload, dict) else []
        sources = []
        for node in iter_nodes(blockdevices):
            node_type = str(node.get("type") or "").strip().lower()
            device_path = str(node.get("path") or "").strip()
            fstype = str(node.get("fstype") or "").strip().lower()
            if node_type not in {"disk", "part"} or not device_path or not fstype or fstype in MEMBER_TYPES:
                continue
            label = (
                str(node.get("label") or "").strip()
                or str(node.get("partlabel") or "").strip()
                or os.path.basename(device_path)
            )
            sources.append(
                {
                    "path": device_path,
                    "label": label,
                    "filesystem": fstype,
                    "volume_label": str(node.get("label") or "").strip(),
                    "part_label": str(node.get("partlabel") or "").strip(),
                    "uuid": str(node.get("uuid") or "").strip(),
                    "size_bytes": int(node.get("size")) if str(node.get("size") or "").strip().isdigit() else None,
                    "default": node_type == "part",
                }
            )
        if not sources:
            fail("No mountable filesystem was detected on this offline VM disk")
        if len(sources) == 1:
            sources[0]["default"] = True
        print(json.dumps({"ok": True, "nbd_device": nbd_device, "sources": sources}))
    except SystemExit:
        raise
    except Exception as exc:
        cleanup(nbd_device)
        fail(str(exc))
    """
).strip()

_REMOTE_OFFLINE_VM_CLEANUP_SCRIPT = textwrap.dedent(
    """
    import base64
    import json
    import os
    import shutil
    import subprocess
    import sys

    PATH_ENV = {"PATH": "/usr/sbin:/sbin:/usr/bin:/bin"}


    def run(cmd, timeout=12):
        exe = cmd[0]
        if shutil.which(exe) is None:
            return {
                "ok": False,
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
                "rc": proc.returncode,
                "stdout": proc.stdout or "",
                "stderr": proc.stderr or "",
            }
        except subprocess.TimeoutExpired as exc:
            return {
                "ok": False,
                "rc": 124,
                "stdout": exc.stdout or "",
                "stderr": exc.stderr or f"timed out after {timeout}s",
            }


    def decode_payload(value):
        padding = "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode((value + padding).encode("ascii"))
        payload = json.loads(raw.decode("utf-8"))
        return payload if isinstance(payload, dict) else {}


    payload = decode_payload(sys.argv[1] if len(sys.argv) > 1 else "")
    nbd_device = str(payload.get("nbd_device") or "").strip()
    prefixes = payload.get("mount_prefixes")
    prefixes = tuple(item for item in prefixes if isinstance(item, str) and item) if isinstance(prefixes, list) else ()
    cleaned = []

    if nbd_device.startswith("/dev/nbd"):
        lsblk = run(["lsblk", "-nr", "-o", "PATH", nbd_device], timeout=8)
        devices = []
        for raw in (lsblk.get("stdout") or "").splitlines():
            path = raw.strip()
            if path.startswith("/dev/"):
                devices.append(path)
        if nbd_device not in devices:
            devices.insert(0, nbd_device)
        for device in devices:
            mounts = run(["findmnt", "-rn", "-S", device, "-o", "TARGET"], timeout=8)
            for raw_target in (mounts.get("stdout") or "").splitlines():
                target = raw_target.strip()
                if not target or (prefixes and not any(target.startswith(prefix) for prefix in prefixes)):
                    continue
                lazy = run(["umount", target], timeout=10)
                if not lazy["ok"]:
                    run(["umount", "-lf", target], timeout=10)
                cleaned.append(target)
        disconnect = run(["qemu-nbd", "--disconnect", nbd_device], timeout=10)
        detail = (disconnect.get("stderr") or "").strip() or (disconnect.get("stdout") or "").strip()
        print(
            json.dumps(
                {
                    "ok": disconnect["ok"] or "not connected" in detail.lower(),
                    "nbd_device": nbd_device,
                    "cleaned_mounts": cleaned,
                    "detail": detail,
                }
            )
        )
    else:
        print(json.dumps({"ok": True, "nbd_device": nbd_device, "cleaned_mounts": cleaned}))
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


def _safe_volume_ref(volume_ref: str) -> str:
    value = str(volume_ref or "").strip()
    if not value or not _SAFE_VOLUME_REF_RE.fullmatch(value):
        raise HTTPException(400, "invalid volume ref")
    return value


def _normalize_browser_mode(mode: Any) -> str:
    return "device_ro" if str(mode or "").strip().lower() == "device_ro" else "mounted"


def _visible_mount_target(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return "" if text.startswith(_BROWSE_MOUNT_PREFIXES) else text


def _safe_node_id(node_id: str) -> str:
    value = str(node_id or "").strip()
    if not value or len(value) > 400:
        raise HTTPException(400, "invalid node id")
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


def _inventory_memory_empty() -> dict[str, Any]:
    return {"version": 1, "hosts": {}}


def _inventory_memory_read_unlocked() -> dict[str, Any]:
    try:
        raw = _DISKS_INVENTORY_MEMORY_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        return _inventory_memory_empty()
    except Exception:
        return _inventory_memory_empty()
    try:
        payload = json.loads(raw)
    except Exception:
        return _inventory_memory_empty()
    if not isinstance(payload, dict):
        return _inventory_memory_empty()
    hosts = payload.get("hosts")
    if not isinstance(hosts, dict):
        payload["hosts"] = {}
    payload.setdefault("version", 1)
    return payload


def _inventory_memory_write_unlocked(payload: dict[str, Any]) -> None:
    _DISKS_INVENTORY_MEMORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = _DISKS_INVENTORY_MEMORY_PATH.with_suffix(".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temp_path, _DISKS_INVENTORY_MEMORY_PATH)


def _utcnow_iso() -> str:
    return dt.datetime.now(dt.UTC).isoformat()


def _disks_notes_lookup() -> dict[str, str]:
    with db.get_conn() as conn:
        rows = conn.execute("SELECT node_id, note FROM disks_notes").fetchall()
    return {
        str(row["node_id"]): str(row["note"] or "")
        for row in rows
        if str(row["node_id"] or "").strip() and str(row["note"] or "").strip()
    }


def _persist_disks_note(node_id: str, note: str) -> None:
    clean_node_id = _safe_node_id(node_id)
    clean_note = str(note or "").strip()
    if len(clean_note) > _DISKS_NOTES_MAX_LENGTH:
        raise HTTPException(400, f"Note exceeds {_DISKS_NOTES_MAX_LENGTH} characters")
    with db.get_conn() as conn:
        if clean_note:
            conn.execute(
                """
                INSERT INTO disks_notes (node_id, note, updated_at)
                VALUES (?, ?, datetime('now'))
                ON CONFLICT(node_id) DO UPDATE SET
                    note = excluded.note,
                    updated_at = datetime('now')
                """,
                (clean_node_id, clean_note),
            )
        else:
            conn.execute("DELETE FROM disks_notes WHERE node_id = ?", (clean_node_id,))
        conn.commit()


def _apply_disks_notes(node: dict[str, Any], notes_by_id: dict[str, str]) -> None:
    node_id = str(node.get("id") or "").strip()
    note = notes_by_id.get(node_id, "").strip()
    if note:
        node["user_note"] = note
    else:
        node.pop("user_note", None)
    for child in node.get("children") or []:
        if isinstance(child, dict):
            _apply_disks_notes(child, notes_by_id)


def _offline_browse_state_empty() -> dict[str, Any]:
    return {"version": 1, "sessions": {}}


def _offline_browse_state_read_unlocked() -> dict[str, Any]:
    try:
        raw = _DISKS_OFFLINE_BROWSE_STATE_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        return _offline_browse_state_empty()
    except Exception:
        return _offline_browse_state_empty()
    try:
        payload = json.loads(raw)
    except Exception:
        return _offline_browse_state_empty()
    if not isinstance(payload, dict):
        return _offline_browse_state_empty()
    sessions = payload.get("sessions")
    if not isinstance(sessions, dict):
        payload["sessions"] = {}
    payload.setdefault("version", 1)
    return payload


def _offline_browse_state_write_unlocked(payload: dict[str, Any]) -> None:
    _DISKS_OFFLINE_BROWSE_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    temp_path = _DISKS_OFFLINE_BROWSE_STATE_PATH.with_suffix(".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temp_path, _DISKS_OFFLINE_BROWSE_STATE_PATH)


def _offline_browse_upsert_session(session: dict[str, Any]) -> None:
    session_id = str(session.get("session_id") or "").strip()
    if not session_id:
        raise ValueError("offline browse session_id is required")
    with _DISKS_OFFLINE_BROWSE_LOCK:
        payload = _offline_browse_state_read_unlocked()
        sessions = payload.setdefault("sessions", {})
        sessions[session_id] = session
        _offline_browse_state_write_unlocked(payload)


def _offline_browse_get_session(session_id: str) -> dict[str, Any] | None:
    clean_session_id = str(session_id or "").strip()
    if not clean_session_id:
        return None
    with _DISKS_OFFLINE_BROWSE_LOCK:
        payload = _offline_browse_state_read_unlocked()
        session = payload.get("sessions", {}).get(clean_session_id)
        return copy.deepcopy(session) if isinstance(session, dict) else None


def _offline_browse_delete_session(session_id: str) -> dict[str, Any] | None:
    clean_session_id = str(session_id or "").strip()
    if not clean_session_id:
        return None
    with _DISKS_OFFLINE_BROWSE_LOCK:
        payload = _offline_browse_state_read_unlocked()
        sessions = payload.setdefault("sessions", {})
        session = sessions.pop(clean_session_id, None)
        _offline_browse_state_write_unlocked(payload)
        return session if isinstance(session, dict) else None


def _offline_browse_update_session(
    session_id: str,
    updater,
) -> dict[str, Any]:
    clean_session_id = str(session_id or "").strip()
    if not clean_session_id:
        raise HTTPException(400, "session_id is required")
    with _DISKS_OFFLINE_BROWSE_LOCK:
        payload = _offline_browse_state_read_unlocked()
        sessions = payload.setdefault("sessions", {})
        session = sessions.get(clean_session_id)
        if not isinstance(session, dict):
            raise HTTPException(404, "offline browse session not found")
        updated = updater(copy.deepcopy(session))
        if not isinstance(updated, dict):
            raise HTTPException(500, "offline browse session update failed")
        sessions[clean_session_id] = updated
        _offline_browse_state_write_unlocked(payload)
        return copy.deepcopy(updated)


def _offline_browse_all_sessions() -> list[dict[str, Any]]:
    with _DISKS_OFFLINE_BROWSE_LOCK:
        payload = _offline_browse_state_read_unlocked()
        sessions = payload.get("sessions", {})
        return [copy.deepcopy(value) for value in sessions.values() if isinstance(value, dict)]


def _layout_hints_payload() -> dict[str, Any]:
    try:
        raw = _DISKS_LAYOUT_HINTS_PATH.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    try:
        payload = json.loads(raw)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _layout_hints_for_host(payload: dict[str, Any], host: str) -> dict[str, Any]:
    hosts = payload.get("hosts")
    if not isinstance(hosts, dict):
        return {}
    hints = hosts.get(host)
    return copy.deepcopy(hints) if isinstance(hints, dict) else {}


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


def _run_nested_guest_filesystem_snapshot(host: str) -> dict[str, Any]:
    command = _ssh_base_command(host) + ["python3", "-"]
    try:
        proc = subprocess.run(
            command,
            input=_REMOTE_NESTED_GUEST_FS_SCRIPT,
            capture_output=True,
            text=True,
            timeout=_SSH_INVENTORY_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "host": host,
            "error": f"nested filesystem probe timed out: {exc}",
            "data": None,
        }
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
        return {
            "ok": False,
            "host": host,
            "error": f"invalid nested filesystem json: {exc}",
            "data": None,
        }
    return {"ok": True, "host": host, "error": "", "data": data}


async def _nested_guest_filesystem_host(host: str) -> dict[str, Any]:
    return await asyncio.to_thread(_run_nested_guest_filesystem_snapshot, host)


def _run_filesystem_tree_snapshot(
    host: str,
    *,
    browse_mode: str,
    source_path: str,
    root_path: str,
    relative_path: str,
    limit: int,
) -> dict[str, Any]:
    payload = {
        "browse_mode": browse_mode,
        "source_path": source_path,
        "root_path": root_path,
        "relative_path": relative_path,
        "limit": int(limit),
    }
    payload_arg = base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("ascii")
    payload_arg = payload_arg.rstrip("=")
    command = _ssh_base_command(host) + ["python3", "-", payload_arg]
    try:
        proc = subprocess.run(
            command,
            input=_REMOTE_FILESYSTEM_TREE_SCRIPT,
            capture_output=True,
            text=True,
            timeout=_SSH_INVENTORY_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "host": host,
            "error": f"filesystem tree probe timed out: {exc}",
            "data": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "host": host, "error": str(exc), "data": None}

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    if stdout:
        try:
            payload = json.loads(stdout)
        except Exception:
            payload = None
        if isinstance(payload, dict):
            if payload.get("ok"):
                return {"ok": True, "host": host, "error": "", "data": payload}
            detail = str(payload.get("error") or "").strip()
            return {
                "ok": False,
                "host": host,
                "error": detail or stderr or f"ssh exited {proc.returncode}",
                "data": None,
            }

    if proc.returncode != 0:
        detail = stderr or stdout or f"ssh exited {proc.returncode}"
        return {"ok": False, "host": host, "error": detail[:500], "data": None}
    return {
        "ok": False,
        "host": host,
        "error": "filesystem tree probe returned no JSON payload",
        "data": None,
    }


async def _filesystem_tree_host(
    host: str,
    *,
    browse_mode: str,
    source_path: str,
    root_path: str,
    relative_path: str,
    limit: int,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        _run_filesystem_tree_snapshot,
        host,
        browse_mode=browse_mode,
        source_path=source_path,
        root_path=root_path,
        relative_path=relative_path,
        limit=limit,
    )


def _payload_arg(payload: dict[str, Any]) -> str:
    text = json.dumps(payload, separators=(",", ":"))
    encoded = base64.urlsafe_b64encode(text.encode("utf-8")).decode("ascii")
    return encoded.rstrip("=")


def _run_offline_vm_prepare(
    host: str,
    *,
    guest_id: str,
    volume_ref: str,
) -> dict[str, Any]:
    command = _ssh_base_command(host) + [
        "python3",
        "-",
        _payload_arg({"guest_id": guest_id, "volume_ref": volume_ref}),
    ]
    try:
        proc = subprocess.run(
            command,
            input=_REMOTE_OFFLINE_VM_PREPARE_SCRIPT,
            capture_output=True,
            text=True,
            timeout=_SSH_INVENTORY_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "host": host,
            "error": f"offline browse prepare timed out: {exc}",
            "data": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "host": host, "error": str(exc), "data": None}
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    try:
        payload = json.loads(stdout) if stdout else None
    except Exception:
        payload = None
    if isinstance(payload, dict):
        if payload.get("ok"):
            return {"ok": True, "host": host, "error": "", "data": payload}
        return {
            "ok": False,
            "host": host,
            "error": str(payload.get("error") or stderr or "offline browse prepare failed"),
            "data": None,
        }
    detail = stderr or stdout or f"ssh exited {proc.returncode}"
    return {"ok": False, "host": host, "error": detail[:500], "data": None}


async def _offline_vm_prepare_host(
    host: str,
    *,
    guest_id: str,
    volume_ref: str,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        _run_offline_vm_prepare,
        host,
        guest_id=guest_id,
        volume_ref=volume_ref,
    )


def _run_offline_vm_attach(
    host: str,
    *,
    resolved_path: str,
    nbd_device: str,
) -> dict[str, Any]:
    payload_arg = _payload_arg({"resolved_path": resolved_path, "nbd_device": nbd_device})
    command = _ssh_base_command(host) + ["python3", "-", payload_arg]
    try:
        proc = subprocess.run(
            command,
            input=_REMOTE_OFFLINE_VM_ATTACH_SCRIPT,
            capture_output=True,
            text=True,
            timeout=_SSH_SMART_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "host": host,
            "error": f"offline browse attach timed out: {exc}",
            "data": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "host": host, "error": str(exc), "data": None}
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    try:
        payload = json.loads(stdout) if stdout else None
    except Exception:
        payload = None
    if isinstance(payload, dict):
        if payload.get("ok"):
            return {"ok": True, "host": host, "error": "", "data": payload}
        return {
            "ok": False,
            "host": host,
            "error": str(payload.get("error") or stderr or "offline browse attach failed"),
            "data": None,
        }
    detail = stderr or stdout or f"ssh exited {proc.returncode}"
    return {"ok": False, "host": host, "error": detail[:500], "data": None}


async def _offline_vm_attach_host(
    host: str,
    *,
    resolved_path: str,
    nbd_device: str,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        _run_offline_vm_attach,
        host,
        resolved_path=resolved_path,
        nbd_device=nbd_device,
    )


def _run_offline_vm_cleanup(host: str, *, nbd_device: str) -> dict[str, Any]:
    payload_arg = _payload_arg(
        {"nbd_device": nbd_device, "mount_prefixes": list(_BROWSE_MOUNT_PREFIXES)}
    )
    command = _ssh_base_command(host) + ["python3", "-", payload_arg]
    try:
        proc = subprocess.run(
            command,
            input=_REMOTE_OFFLINE_VM_CLEANUP_SCRIPT,
            capture_output=True,
            text=True,
            timeout=_SSH_INVENTORY_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "host": host,
            "error": f"offline browse cleanup timed out: {exc}",
            "data": None,
        }
    except Exception as exc:  # noqa: BLE001
        return {"ok": False, "host": host, "error": str(exc), "data": None}
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    try:
        payload = json.loads(stdout) if stdout else None
    except Exception:
        payload = None
    if isinstance(payload, dict):
        if payload.get("ok"):
            return {"ok": True, "host": host, "error": "", "data": payload}
        return {
            "ok": False,
            "host": host,
            "error": str(payload.get("detail") or stderr or "offline browse cleanup failed"),
            "data": payload,
        }
    detail = stderr or stdout or f"ssh exited {proc.returncode}"
    return {"ok": False, "host": host, "error": detail[:500], "data": None}


def _offline_browse_parse_iso(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _offline_browse_public_session(session: dict[str, Any]) -> dict[str, Any]:
    return {
        "session_id": str(session.get("session_id") or "").strip(),
        "state": str(session.get("state") or "").strip(),
        "host": str(session.get("host") or "").strip(),
        "guest_id": str(session.get("guest_id") or "").strip(),
        "guest_name": str(session.get("guest_name") or "").strip(),
        "volume_ref": str(session.get("volume_ref") or "").strip(),
        "volume_label": str(session.get("volume_label") or "").strip(),
        "nbd_device": str(session.get("nbd_device") or "").strip(),
        "sources": copy.deepcopy(session.get("sources") or []),
        "opened_at": str(session.get("opened_at") or "").strip(),
        "last_heartbeat_at": str(session.get("last_heartbeat_at") or "").strip(),
        "timeout_seconds": int(
            session.get("timeout_seconds") or _DISKS_OFFLINE_BROWSE_TIMEOUT_SECONDS
        ),
    }


async def _offline_browse_cleanup_session(session: dict[str, Any]) -> dict[str, Any]:
    host = str(session.get("host") or "").strip()
    nbd_device = str(session.get("nbd_device") or "").strip()
    if not host or not nbd_device:
        return {"ok": True, "host": host, "error": "", "data": None}
    return await asyncio.to_thread(_run_offline_vm_cleanup, host, nbd_device=nbd_device)


def _offline_browse_session_is_stale(
    session: dict[str, Any],
    *,
    now: dt.datetime,
) -> bool:
    last_seen = _offline_browse_parse_iso(
        session.get("last_heartbeat_at")
    ) or _offline_browse_parse_iso(session.get("opened_at"))
    if last_seen is None:
        return True
    timeout_seconds = max(
        10,
        int(session.get("timeout_seconds") or _DISKS_OFFLINE_BROWSE_TIMEOUT_SECONDS),
    )
    return (now - last_seen).total_seconds() > timeout_seconds


async def _reap_stale_offline_browse_sessions() -> None:
    now = dt.datetime.now(dt.UTC)
    stale_sessions = [
        session
        for session in _offline_browse_all_sessions()
        if _offline_browse_session_is_stale(session, now=now)
    ]
    for session in stale_sessions:
        session_id = str(session.get("session_id") or "").strip()
        if not session_id:
            continue
        try:
            _offline_browse_update_session(
                session_id,
                lambda current: {
                    **current,
                    "state": "closing",
                    "close_reason": "timeout",
                    "closed_at": _utcnow_iso(),
                },
            )
        except HTTPException:
            continue
        await _offline_browse_cleanup_session(session)
        _offline_browse_delete_session(session_id)


async def run_disks_offline_browse_reaper() -> None:
    await _reap_stale_offline_browse_sessions()
    while True:
        await asyncio.sleep(_DISKS_OFFLINE_BROWSE_REAPER_INTERVAL_SECONDS)
        await _reap_stale_offline_browse_sessions()


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

    try:
        body = json.loads(proc.stdout or "{}")
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(502, f"smartctl returned invalid json: {exc}") from exc
    if proc.returncode not in {0, 4}:
        detail = (proc.stderr or "").strip() or f"smartctl exited {proc.returncode}"
        body.setdefault("_smartctl_exit_status", proc.returncode)
        body.setdefault("_smartctl_error", detail[:500])
    return body


def _flatten_block_devices(
    blockdevices: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    top_disks: list[dict[str, Any]] = []
    by_path: dict[str, dict[str, Any]] = {}

    def walk(node: dict[str, Any], *, parent_path: str = "", root_disk_path: str = "") -> None:
        path = str(node.get("path") or "").strip()
        node["mountpoint"] = _visible_mount_target(node.get("mountpoint"))
        mountpoints = node.get("mountpoints")
        if isinstance(mountpoints, list):
            node["mountpoints"] = [
                point for point in (_visible_mount_target(item) for item in mountpoints) if point
            ]
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

    def walk(rows: list[dict[str, Any]]) -> None:
        for mount in rows:
            if not isinstance(mount, dict):
                continue
            mount["target"] = _visible_mount_target(mount.get("target"))
            for key in ("source", "resolved_source"):
                source = str(mount.get(key) or "").strip()
                if source:
                    index[source] = mount
            children = mount.get("children")
            if isinstance(children, list):
                walk(children)

    walk(mounts)
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


def _hostpci_assignments(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows = snapshot.get("hostpci_assignments")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _guest_identity_lookup(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = snapshot.get("guest_identities")
    if not isinstance(rows, list):
        return {}
    lookup: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        guest_id = str(row.get("vmid") or "").strip()
        if not guest_id:
            continue
        lookup[guest_id] = row
    return lookup


def _guest_kind_label(kind: str) -> str:
    clean = str(kind or "").strip().lower()
    if clean == "ct":
        return "CT"
    if clean == "template":
        return "Template"
    return "VM"


def _guest_display_label(guest_id: str, guest_kind: str, guest_name: str = "") -> str:
    base = f"{_guest_kind_label(guest_kind)} {guest_id}".strip()
    clean_name = str(guest_name or "").strip()
    if clean_name and clean_name.lower() not in {
        f"vm-{guest_id}".lower(),
        f"ct-{guest_id}".lower(),
        f"template-{guest_id}".lower(),
    }:
        return f"{base} ({clean_name})"
    return base


def _guest_button_label(guest_id: str, guest_kind: str, guest_name: str = "") -> str:
    clean_name = str(guest_name or "").strip()
    if clean_name and clean_name.lower() not in {
        f"vm-{guest_id}".lower(),
        f"ct-{guest_id}".lower(),
        f"template-{guest_id}".lower(),
    }:
        return clean_name
    return f"{_guest_kind_label(guest_kind)} {guest_id}".strip()


def _guest_identity_meta(
    host: str,
    *,
    guest_id: str,
    guest_kind: str,
    guest_name: str = "",
) -> dict[str, Any]:
    clean_host = str(host or "").strip()
    clean_guest_id = str(guest_id or "").strip()
    clean_guest_kind = str(guest_kind or "").strip().lower() or "vm"
    if not clean_host or not clean_guest_id:
        return {}
    clean_name = str(guest_name or "").strip()
    return {
        "guest_identity": {
            "host": clean_host,
            "guest_id": clean_guest_id,
            "guest_kind": clean_guest_kind,
            "guest_kind_label": _guest_kind_label(clean_guest_kind),
            "guest_key": f"{clean_guest_kind}:{clean_guest_id}",
            "guest_name": clean_name,
            "guest_display": _guest_display_label(
                clean_guest_id,
                clean_guest_kind,
                clean_name,
            ),
            "guest_summary_label": clean_name
            or f"{_guest_kind_label(clean_guest_kind)} {clean_guest_id}",
            "guest_button_label": _guest_button_label(
                clean_guest_id,
                clean_guest_kind,
                clean_name,
            ),
        }
    }


def _guest_identity_meta_from_label(
    host: str,
    label: str,
    guest_identity_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    match = _GUEST_VOLUME_LABEL_RE.match(str(label or "").strip())
    if not match:
        return {}
    guest_id = str(match.group("guest_id") or "").strip()
    prefix = str(match.group("prefix") or "").strip().lower()
    row = guest_identity_lookup.get(guest_id) or {}
    guest_kind = "ct" if prefix == "subvol" else "template" if prefix == "base" else "vm"
    row_kind = str(row.get("guest_type") or "").strip().lower()
    if prefix == "vm" and row_kind in {"vm", "template"}:
        guest_kind = row_kind
    guest_name = str(row.get("name") or "").strip()
    return _guest_identity_meta(
        host,
        guest_id=guest_id,
        guest_kind=guest_kind,
        guest_name=guest_name,
    )


def _guest_identity_meta_from_assignment(host: str, row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        return {}
    guest_id = str(row.get("vmid") or "").strip()
    guest_kind = str(row.get("guest_type") or "").strip().lower() or "vm"
    guest_name = str(row.get("name") or "").strip()
    return _guest_identity_meta(
        host,
        guest_id=guest_id,
        guest_kind=guest_kind,
        guest_name=guest_name,
    )


def _dataset_total_bytes(row: dict[str, Any] | None) -> int | None:
    if not isinstance(row, dict):
        return None
    volsize = _to_int(row.get("volsize_bytes"))
    if volsize:
        return volsize
    used = _to_int(row.get("used_bytes"))
    available = _to_int(row.get("available_bytes"))
    if used is None or available is None:
        return None
    return max(0, used + available)


def _transport_display(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    mapping = {
        "nvme": "NVMe",
        "usb": "USB",
        "usb4": "USB4",
        "sata": "SATA",
        "sas": "SAS",
        "scsi": "SCSI",
        "ata": "ATA",
        "pcie": "PCIe",
        "hba": "HBA",
        "thunderbolt": "Thunderbolt",
    }
    return mapping.get(text, text.upper() if len(text) <= 4 else text.title())


def _transport_tags(*values: Any) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip().lower()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        tags.append(clean)
    return tags


def _thunderbolt_connection_tags(
    pool_meta: dict[str, Any] | None, *, raw_transport: str = ""
) -> list[str]:
    if not isinstance(pool_meta, dict):
        return []
    text = " ".join(
        str(pool_meta.get(key) or "").strip()
        for key in ("hardware", "description", "label", "name")
    ).lower()
    if not text:
        return []
    tags: list[str] = []
    raw = str(raw_transport or "").strip().lower()
    if raw in {"sata", "sas"} and ("hb sata" in text or "sas" in text or "controller" in text):
        tags.append("hba")
    if "thunderbolt" in text:
        tags.append("thunderbolt")
    elif "usb4" in text:
        tags.append("usb4")
    return _transport_tags(*tags)


def _transport_label(raw_transport: Any, *extra_tags: Any) -> str:
    tags = _transport_tags(raw_transport, *extra_tags)
    return " · ".join(part for part in (_transport_display(tag) for tag in tags) if part)


def _pool_member_used_bytes(
    pool_row: dict[str, Any],
    member_size: int | None,
    *,
    member_count: int | None = None,
) -> int | None:
    allocated = _to_int(pool_row.get("allocated_bytes"))
    total = _to_int(pool_row.get("size_bytes"))
    if member_count == 1 and allocated is not None:
        return max(0, allocated)
    if member_size is None:
        return None
    if allocated is not None and total not in (None, 0):
        return int(round(member_size * (allocated / total)))
    cap = _to_int(pool_row.get("capacity_pct"))
    if cap is not None:
        return int(round(member_size * (cap / 100.0)))
    if allocated is None or total in (None, 0):
        return None
    return int(round(member_size * (allocated / total)))


def _filesystem_probe_index(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw = snapshot.get("filesystem_usage_probes")
    if not isinstance(raw, dict):
        return {}
    return {
        str(path): probe
        for path, probe in raw.items()
        if isinstance(path, str) and isinstance(probe, dict)
    }


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


def _guest_volume_assignments(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    rows = snapshot.get("guest_volume_assignments")
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _match_guest_volume_assignment(
    snapshot: dict[str, Any],
    *,
    guest_id: str,
    dataset_name: str,
    label: str,
) -> dict[str, Any] | None:
    clean_guest_id = str(guest_id or "").strip()
    clean_dataset_name = str(dataset_name or "").strip()
    clean_label = str(label or "").strip()
    if not clean_guest_id or not (clean_dataset_name or clean_label):
        return None
    candidates = {
        clean_dataset_name,
        clean_label,
        clean_dataset_name.split("/")[-1] if clean_dataset_name else "",
    }
    candidates = {item for item in candidates if item}
    for row in _guest_volume_assignments(snapshot):
        if str(row.get("vmid") or "").strip() != clean_guest_id:
            continue
        volume_ref = str(row.get("volume_ref") or "").strip()
        volume_name = str(row.get("volume_name") or "").strip()
        volume_leaf = str(row.get("volume_leaf") or "").strip()
        if (
            volume_ref in candidates
            or volume_name in candidates
            or volume_leaf in candidates
            or any(
                clean_dataset_name.endswith(f"/{item}")
                for item in (volume_name, volume_leaf)
                if item
            )
        ):
            return row
    return None


def _offline_browser_meta_from_assignment(host: str, assignment: dict[str, Any]) -> dict[str, Any]:
    guest_id = str(assignment.get("vmid") or "").strip()
    guest_name = str(assignment.get("name") or "").strip()
    volume_ref = str(assignment.get("volume_ref") or "").strip()
    volume_label = (
        str(assignment.get("volume_leaf") or "").strip()
        or str(assignment.get("volume_name") or "").strip()
        or volume_ref
    )
    if not host or not guest_id or not volume_ref:
        return {}
    return {
        "offline_browser": {
            "host": host,
            "guest_id": guest_id,
            "guest_name": guest_name,
            "volume_ref": volume_ref,
            "volume_label": volume_label,
            "slot": str(assignment.get("slot") or "").strip(),
        }
    }


def _normalize_backup_guest_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    head = text.split(".", 1)[0].split("/", 1)[0]
    match = re.match(r"^(pbs\d+)", head)
    if match:
        return match.group(1)
    match = re.match(r"^(rusty-backups|pbs[a-z0-9-]+)", head)
    return match.group(1) if match else ""


def _pbs_guest_usage_lookup(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
    storages = {
        str(row.get("name") or "").strip(): row
        for row in (snapshot.get("pbs_storages") or [])
        if isinstance(row, dict) and str(row.get("name") or "").strip()
    }
    grouped: dict[str, dict[str, Any]] = {}
    for row in snapshot.get("pvesm_status") or []:
        if not isinstance(row, dict):
            continue
        if str(row.get("type") or "").strip().lower() != "pbs":
            continue
        name = str(row.get("name") or "").strip()
        storage = storages.get(name, {})
        guest_name = _normalize_backup_guest_name(
            storage.get("server") or name or storage.get("datastore")
        )
        if not guest_name:
            continue
        total_kib = _to_int(row.get("total_kib"))
        used_kib = _to_int(row.get("used_kib"))
        available_kib = _to_int(row.get("available_kib"))
        total_bytes = total_kib * 1024 if total_kib is not None else None
        used_bytes = used_kib * 1024 if used_kib is not None else None
        available_bytes = available_kib * 1024 if available_kib is not None else None
        if total_bytes in (None, 0):
            continue
        signature = (
            total_bytes,
            used_bytes,
            available_bytes,
            str(storage.get("datastore") or "").strip(),
            guest_name,
        )
        record = grouped.get(guest_name)
        if record and record.get("signature") == signature:
            names = record.setdefault("storage_names", [])
            if name and name not in names:
                names.append(name)
            continue
        candidate = {
            "guest_name": guest_name,
            "server": str(storage.get("server") or "").strip(),
            "datastore": str(storage.get("datastore") or "").strip(),
            "storage_names": [name] if name else [],
            "status": str(row.get("status") or "").strip(),
            "total_bytes": total_bytes,
            "used_bytes": used_bytes,
            "available_bytes": available_bytes,
            "usage_pct": _pct(used_bytes, total_bytes),
            "signature": signature,
        }
        if not record or (candidate.get("total_bytes") or 0) > (record.get("total_bytes") or 0):
            grouped[guest_name] = candidate
    for record in grouped.values():
        record.pop("signature", None)
    return grouped


def _guest_assignment_name_counts(snapshot: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    seen: set[tuple[str, str]] = set()
    for row in snapshot.get("guest_disk_assignments") or []:
        if not isinstance(row, dict):
            continue
        guest_name = _normalize_backup_guest_name(row.get("name"))
        source_key = str(row.get("resolved_path") or row.get("source_path") or "").strip()
        if not guest_name or not source_key:
            continue
        identity = (guest_name, source_key)
        if identity in seen:
            continue
        seen.add(identity)
        counts[guest_name] = counts.get(guest_name, 0) + 1
    return counts


def _pbs_usage_for_assignments(
    snapshot: dict[str, Any],
    assignments: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not assignments:
        return None
    usage_lookup = _pbs_guest_usage_lookup(snapshot)
    if not usage_lookup:
        return None
    assignment_counts = _guest_assignment_name_counts(snapshot)
    names = []
    for row in assignments:
        guest_name = _normalize_backup_guest_name(row.get("name"))
        if guest_name and guest_name not in names:
            names.append(guest_name)
    for guest_name in names:
        if assignment_counts.get(guest_name) != 1:
            continue
        usage = usage_lookup.get(guest_name)
        if usage:
            return usage
    return None


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


def _probe_strategy_label(probe: dict[str, Any] | None) -> str:
    if not isinstance(probe, dict):
        return ""
    strategy = str(probe.get("strategy") or "").strip().lower()
    if strategy == "tune2fs":
        return "read-only ext filesystem metadata"
    if strategy == "ntfsinfo":
        return "read-only NTFS metadata"
    if strategy == "dump.exfat":
        return "read-only exFAT metadata"
    return "read-only filesystem metadata"


def _meaningful_part_label(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.lower() in _GENERIC_PARTLABELS:
        return ""
    return text


def _is_data_filesystem(fstype: Any) -> bool:
    clean = str(fstype or "").strip().lower()
    return bool(clean) and clean not in _FILESYSTEM_MEMBER_TYPES


def _standalone_volume_label(
    node: dict[str, Any],
    *,
    mount_target: str,
    probe: dict[str, Any],
) -> str:
    volume_label = str(probe.get("volume_label") or "").strip()
    if volume_label:
        return volume_label
    if mount_target and mount_target != "/":
        base = mount_target.rstrip("/").rsplit("/", 1)[-1].strip()
        if base:
            return base
    part_label = _meaningful_part_label(node.get("partlabel"))
    if part_label:
        return part_label
    return str(node.get("name") or node.get("path") or "filesystem").strip() or "filesystem"


def _should_surface_standalone_logical(
    *,
    total_bytes: int | None,
    used_bytes: int | None,
    mount_target: str,
) -> bool:
    if mount_target:
        return True
    if used_bytes not in (None, 0):
        return True
    return total_bytes is not None and total_bytes >= _STANDALONE_LOGICAL_MIN_BYTES


def _standalone_logical_note(
    *,
    mount_target: str,
    probe: dict[str, Any],
    used_bytes: int | None,
) -> str:
    if used_bytes is None:
        return "Filesystem detected, but usage could not be measured in the current host snapshot."
    if mount_target:
        return ""
    if probe:
        return f"Usage estimated from {_probe_strategy_label(probe)}."
    return ""


def _guest_assignment_note(
    assignments: list[dict[str, Any]],
    *,
    scope: str = "drive",
    usage_probe: dict[str, Any] | None = None,
    guest_usage: dict[str, Any] | None = None,
) -> str:
    if not assignments:
        return ""
    labels = [_guest_assignment_display(item) for item in assignments if isinstance(item, dict)]
    if not labels:
        labels = ["a guest"]
    unique_labels = list(dict.fromkeys(labels))
    joined = ", ".join(unique_labels)
    if isinstance(guest_usage, dict) and guest_usage.get("total_bytes"):
        datastore = str(guest_usage.get("datastore") or "").strip()
        source_label = datastore or "the guest datastore"
        return (
            f"This {scope} is assigned directly to {joined}. "
            f"Usage is measured from {source_label} over the configured PBS storage path."
        )
    probe_label = _probe_strategy_label(usage_probe)
    if probe_label:
        return (
            f"This {scope} is assigned directly to {joined}. "
            f"Usage is estimated from {probe_label} on the host at refresh time."
        )
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


def _guest_usage_facts(guest_usage: dict[str, Any] | None) -> list[dict[str, str]]:
    if not isinstance(guest_usage, dict) or not guest_usage.get("total_bytes"):
        return []
    datastore = str(guest_usage.get("datastore") or "").strip()
    names = (
        guest_usage.get("storage_names")
        if isinstance(guest_usage.get("storage_names"), list)
        else []
    )
    return _non_null_facts(
        _fact("Guest store", f"PBS {datastore}" if datastore else "Proxmox Backup"),
        _fact("Guest free", _format_bytes(_to_int(guest_usage.get("available_bytes")))),
        _fact(
            "Guest usage source",
            ", ".join(str(name).strip() for name in names if str(name).strip()),
        ),
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


def _display_count(items: list[dict[str, Any]], *, group: str) -> int:
    return sum(
        1 for item in items if isinstance(item, dict) and str(item.get("group") or "") == group
    )


def _refresh_host_subtitle(host_node: dict[str, Any]) -> None:
    children = host_node.get("children")
    if not isinstance(children, list):
        return
    physical_count = _display_count(children, group="Physical drives")
    logical_count = _display_count(children, group="Logical systems")
    subtitle_bits: list[str] = []
    if physical_count:
        subtitle_bits.append(f"{physical_count} physical drive{'s' if physical_count != 1 else ''}")
    if logical_count:
        subtitle_bits.append(f"{logical_count} logical system{'s' if logical_count != 1 else ''}")
    host_node["subtitle"] = " · ".join(subtitle_bits)


def _cacheable_host_child(node: dict[str, Any]) -> bool:
    if not isinstance(node, dict):
        return False
    node_id = str(node.get("id") or "").strip()
    group = str(node.get("group") or "").strip()
    return bool(node_id) and group in _DISKS_INVENTORY_MEMORY_GROUPS


def _snapshot_cacheable_node(node: dict[str, Any]) -> dict[str, Any]:
    snapshot = copy.deepcopy(node)
    for key in (
        "cached_missing",
        "cached_missing_ancestor",
        "cache_host",
        "cache_last_seen_at",
        "excluded_from_totals",
    ):
        snapshot.pop(key, None)
    return snapshot


def _mark_cached_missing_subtree(
    node: dict[str, Any],
    *,
    host: str,
    last_seen_at: str,
    is_root: bool,
) -> dict[str, Any]:
    cached = copy.deepcopy(node)
    cached["status"] = "stale"
    cached["smart"] = None
    if is_root:
        cached["cached_missing"] = True
        cached["cache_host"] = host
        cached["cache_last_seen_at"] = last_seen_at
        cached["excluded_from_totals"] = True
        last_usage = ""
        total = _to_int(cached.get("total_bytes"))
        used = _to_int(cached.get("used_bytes"))
        if total is not None:
            if used is not None:
                last_usage = f"Last seen {_format_bytes(used)} / {_format_bytes(total)}"
            else:
                last_usage = f"Last seen total {_format_bytes(total)}"
        if last_usage:
            cached["usage_text"] = last_usage
        stale_note = "Cached from a previous inventory snapshot. Not seen in the latest refresh."
        note = str(cached.get("note") or "").strip()
        cached["note"] = f"{stale_note} {note}".strip() if note else stale_note
    else:
        cached["cached_missing_ancestor"] = True
    children = cached.get("children")
    if isinstance(children, list):
        cached["children"] = [
            _mark_cached_missing_subtree(
                child,
                host=host,
                last_seen_at=last_seen_at,
                is_root=False,
            )
            if isinstance(child, dict)
            else child
            for child in children
        ]
    return cached


def _inventory_record_sort_key(record: dict[str, Any]) -> tuple[int, str, str]:
    snapshot = record.get("snapshot") if isinstance(record.get("snapshot"), dict) else {}
    return (
        int(record.get("order") or 0),
        str(snapshot.get("label") or "").lower(),
        str(record.get("id") or ""),
    )


def _merge_inventory_memory(host_nodes: list[dict[str, Any]]) -> None:
    now_iso = dt.datetime.now(dt.UTC).isoformat()
    with _DISKS_INVENTORY_MEMORY_LOCK:
        memory = _inventory_memory_read_unlocked()
        hosts_store = memory.setdefault("hosts", {})
        changed = False

        for host_node in host_nodes:
            if not isinstance(host_node, dict) or str(host_node.get("kind") or "") != "host":
                continue
            host = str(host_node.get("label") or "").strip()
            if not host:
                continue

            host_store = hosts_store.setdefault(host, {})
            node_store = host_store.setdefault("nodes", {})
            if not isinstance(node_store, dict):
                host_store["nodes"] = {}
                node_store = host_store["nodes"]

            live_children = host_node.get("children")
            if not isinstance(live_children, list):
                live_children = []

            grouped_live: dict[str, list[dict[str, Any]]] = {
                group: [] for group in _DISKS_INVENTORY_MEMORY_GROUPS
            }
            other_children: list[dict[str, Any]] = []
            for child in live_children:
                if _cacheable_host_child(child):
                    grouped_live[str(child.get("group"))].append(child)
                else:
                    other_children.append(child)

            for group_name in _DISKS_INVENTORY_MEMORY_GROUPS:
                for order, child in enumerate(grouped_live[group_name]):
                    record = {
                        "id": str(child.get("id") or ""),
                        "group": group_name,
                        "order": order,
                        "last_seen_at": now_iso,
                        "snapshot": _snapshot_cacheable_node(child),
                    }
                    if node_store.get(record["id"]) != record:
                        node_store[record["id"]] = record
                        changed = True

            merged_children: list[dict[str, Any]] = []
            cached_missing_count = 0
            for group_name in _DISKS_INVENTORY_MEMORY_GROUPS:
                live_by_id = {
                    str(child.get("id") or ""): child
                    for child in grouped_live[group_name]
                    if isinstance(child, dict)
                }
                records = [
                    record
                    for record in node_store.values()
                    if isinstance(record, dict) and str(record.get("group") or "") == group_name
                ]
                records.sort(key=_inventory_record_sort_key)
                seen_ids: set[str] = set()
                for record in records:
                    node_id = str(record.get("id") or "")
                    if not node_id or node_id in seen_ids:
                        continue
                    seen_ids.add(node_id)
                    live_node = live_by_id.get(node_id)
                    if live_node is not None:
                        merged_children.append(live_node)
                        continue
                    snapshot = record.get("snapshot")
                    if not isinstance(snapshot, dict):
                        continue
                    cached_missing_count += 1
                    merged_children.append(
                        _mark_cached_missing_subtree(
                            snapshot,
                            host=host,
                            last_seen_at=str(record.get("last_seen_at") or ""),
                            is_root=True,
                        )
                    )
                for node_id, live_node in live_by_id.items():
                    if node_id in seen_ids:
                        continue
                    merged_children.append(live_node)

            host_node["children"] = merged_children + other_children
            _refresh_host_subtitle(host_node)
            if cached_missing_count:
                note = str(host_node.get("note") or "").strip()
                addition = "Missing inventory cached from earlier refreshes is shown grey and excluded from totals."
                if addition not in note:
                    host_node["note"] = f"{note} {addition}".strip() if note else addition
                host_node["cached_missing_count"] = cached_missing_count
            else:
                host_node.pop("cached_missing_count", None)

        if changed:
            _inventory_memory_write_unlocked(memory)


def _inventory_memory_forget(host: str, node_id: str) -> bool:
    with _DISKS_INVENTORY_MEMORY_LOCK:
        memory = _inventory_memory_read_unlocked()
        hosts_store = memory.setdefault("hosts", {})
        host_store = hosts_store.get(host)
        if not isinstance(host_store, dict):
            return False
        node_store = host_store.get("nodes")
        if not isinstance(node_store, dict) or node_id not in node_store:
            return False
        del node_store[node_id]
        _inventory_memory_write_unlocked(memory)
        return True


def _mount_leaf(path: Any) -> str:
    text = str(path or "").strip().rstrip("/")
    if not text:
        return ""
    if "/" not in text:
        return text
    return text.rsplit("/", 1)[-1].strip()


def _browseable_mount_path(path: Any) -> str:
    text = str(path or "").strip().replace("\\", "/")
    if not text:
        return ""
    if text.lower() in {"-", "none", "legacy"}:
        return ""
    if not text.startswith("/"):
        return ""
    clean = re.sub(r"/{2,}", "/", text)
    clean = clean.rstrip("/") or "/"
    return clean if clean.startswith("/") else ""


def _filesystem_browser_meta(
    *,
    host: str,
    root_path: Any,
    filesystem: Any = "",
    source_path: Any = "",
    dataset_name: Any = "",
    allow_device_fallback: bool = False,
) -> dict[str, Any] | None:
    try:
        clean_host = _safe_host(host)
    except HTTPException:
        return None
    filesystem_name = str(filesystem or "").strip().lower()
    if filesystem_name and filesystem_name != "zfs" and not _is_data_filesystem(filesystem_name):
        return None
    clean_root = _browseable_mount_path(root_path)
    browse_mode = "mounted"
    root_value = clean_root
    root_display = clean_root
    clean_source = str(source_path or "").strip()
    if not clean_root:
        if not allow_device_fallback:
            return None
        try:
            clean_source = _safe_device_path(clean_source)
        except HTTPException:
            return None
        browse_mode = "device_ro"
        root_value = "/"
        root_display = clean_source
    return {
        "filesystem_browser": {
            "host": clean_host,
            "root_path": root_value,
            "root_display": root_display,
            "browse_mode": browse_mode,
            "filesystem": filesystem_name or "filesystem",
            "source_path": clean_source,
            "dataset_name": str(dataset_name or "").strip(),
            "download_available": False,
        }
    }


def _safe_browse_host(host: str) -> str:
    clean_host = _safe_host(host)
    if _BROWSE_HOSTS and clean_host not in _BROWSE_HOSTS:
        raise HTTPException(400, f"Host is not enabled for filesystem browsing: {clean_host}")
    return clean_host


def _normalize_browser_root_path(path: str) -> str:
    text = str(path or "").strip().replace("\\", "/")
    if not text.startswith("/"):
        raise HTTPException(400, "Filesystem root must be an absolute path")
    clean = re.sub(r"/{2,}", "/", text)
    clean = clean.rstrip("/") or "/"
    if not clean.startswith("/"):
        raise HTTPException(400, "Filesystem root must be an absolute path")
    return clean


def _normalize_browser_relative_path(path: str | None) -> str:
    text = str(path or "").strip().replace("\\", "/")
    if not text or text == ".":
        return "."
    if text.startswith("/"):
        raise HTTPException(400, "Path must be relative to the filesystem root")
    parts: list[str] = []
    for part in text.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            raise HTTPException(400, "Path escapes filesystem root")
        parts.append(part)
    return "/".join(parts) if parts else "."


def _needs_nested_guest_filesystem_snapshot(storage_payload: dict[str, Any] | None) -> bool:
    if not _NESTED_GUEST_HOST or not isinstance(storage_payload, dict):
        return False
    disks = storage_payload.get("disks")
    if not isinstance(disks, list):
        return False
    for item in disks:
        if not isinstance(item, dict):
            continue
        detail = item.get("detail")
        if not isinstance(detail, dict):
            continue
        if str(detail.get("source_host") or "").strip() != _NESTED_GUEST_HOST:
            continue
        local_device = (
            detail.get("local_device") if isinstance(detail.get("local_device"), dict) else {}
        )
        if not (
            str(detail.get("mount_path") or "").strip()
            or str(local_device.get("path") or "").strip()
        ):
            continue
        if all(str(local_device.get(key) or "").strip() for key in ("fstype", "label", "uuid")):
            continue
        return True
    return False


def _nested_guest_filesystem_lookup(
    snapshot: dict[str, Any] | None,
) -> dict[str, dict[str, dict[str, Any]]]:
    if not isinstance(snapshot, dict):
        return {"by_path": {}, "mount_by_target": {}}
    lsblk = snapshot.get("lsblk")
    blockdevices = lsblk.get("blockdevices") if isinstance(lsblk, dict) else None
    if not isinstance(blockdevices, list):
        return {"by_path": {}, "mount_by_target": {}}
    _, by_path = _flatten_block_devices(blockdevices)
    mount_by_target: dict[str, dict[str, Any]] = {}
    for node in by_path.values():
        if not isinstance(node, dict):
            continue
        mount_target = str(node.get("mountpoint") or "").strip()
        if mount_target and mount_target not in mount_by_target:
            mount_by_target[mount_target] = node
    return {"by_path": by_path, "mount_by_target": mount_by_target}


def _nested_guest_filesystem_meta(
    detail: dict[str, Any],
    local_device: dict[str, Any],
    lookup: dict[str, dict[str, dict[str, Any]]] | None,
) -> dict[str, str]:
    local_path = str(local_device.get("path") or "").strip()
    mount_path = str(detail.get("mount_path") or "").strip()
    meta = {
        "path": local_path,
        "mount_path": mount_path,
        "fstype": str(local_device.get("fstype") or "").strip(),
        "volume_label": str(local_device.get("label") or "").strip(),
        "uuid": str(local_device.get("uuid") or "").strip(),
        "part_label": _meaningful_part_label(local_device.get("partlabel")),
    }
    if not isinstance(lookup, dict):
        return meta

    by_path = lookup.get("by_path") if isinstance(lookup.get("by_path"), dict) else {}
    mount_by_target = (
        lookup.get("mount_by_target") if isinstance(lookup.get("mount_by_target"), dict) else {}
    )
    candidates: dict[str, dict[str, Any]] = {}
    if mount_path:
        mount_node = mount_by_target.get(mount_path)
        if isinstance(mount_node, dict):
            candidates[str(mount_node.get("path") or mount_path)] = mount_node
    if local_path:
        exact = by_path.get(local_path)
        if isinstance(exact, dict):
            candidates[str(exact.get("path") or local_path)] = exact
        for node in by_path.values():
            if not isinstance(node, dict):
                continue
            node_path = str(node.get("path") or "").strip()
            if not node_path:
                continue
            if str(node.get("_root_disk_path") or "").strip() == local_path:
                candidates[node_path] = node
            elif str(node.get("_parent_path") or "").strip() == local_path:
                candidates[node_path] = node

    if not candidates:
        return meta

    def score(node: dict[str, Any]) -> tuple[int, int]:
        node_path = str(node.get("path") or "").strip()
        node_mount = str(node.get("mountpoint") or "").strip()
        node_type = str(node.get("type") or "").strip().lower()
        node_root = str(node.get("_root_disk_path") or "").strip()
        rank = 0
        if mount_path and node_mount == mount_path:
            rank += 100
        if local_path and node_path == local_path:
            rank += 40
        if local_path and node_root == local_path and node_path != local_path:
            rank += 70
        if str(node.get("fstype") or "").strip():
            rank += 25
        if str(node.get("label") or "").strip():
            rank += 12
        if str(node.get("uuid") or "").strip():
            rank += 8
        if node_type in {"part", "crypt", "lvm"}:
            rank += 6
        return rank, -len(node_path)

    selected = max(candidates.values(), key=score)
    meta["path"] = str(selected.get("path") or "").strip() or meta["path"]
    meta["mount_path"] = str(selected.get("mountpoint") or "").strip() or meta["mount_path"]
    meta["fstype"] = str(selected.get("fstype") or "").strip() or meta["fstype"]
    meta["volume_label"] = str(selected.get("label") or "").strip() or meta["volume_label"]
    meta["uuid"] = str(selected.get("uuid") or "").strip() or meta["uuid"]
    meta["part_label"] = _meaningful_part_label(selected.get("partlabel")) or meta["part_label"]
    return meta


def _local_device_transport(local_device: dict[str, Any] | None, *, fallback: str = "") -> str:
    if not isinstance(local_device, dict):
        return str(fallback or "").strip().lower()
    text = " ".join(
        str(local_device.get(key) or "").strip()
        for key in ("name", "path", "model", "serial", "size", "fstype")
    ).lower()
    if "nvme" in text:
        return "nvme"
    if "usb4" in text:
        return "usb4"
    if "thunderbolt" in text:
        return "thunderbolt"
    if re.search(r"\b(sata|ata)\b", text):
        return "sata"
    if "sas" in text:
        return "sas"
    if "scsi" in text:
        return "scsi"
    if "usb" in text or "portable" in text or "datatraveler" in text:
        return "usb"
    return str(fallback or "").strip().lower()


def _local_device_rotational(local_device: dict[str, Any] | None, *, transport: str = "") -> str:
    model = (
        str(local_device.get("model") or "").strip().lower()
        if isinstance(local_device, dict)
        else ""
    )
    clean_transport = str(transport or "").strip().lower()
    if clean_transport == "nvme" or "ssd" in model:
        return "no"
    return ""


def _is_storage_hostpci_assignment(row: dict[str, Any]) -> bool:
    class_id = str(row.get("class_id") or "").strip().lower().removeprefix("0x")
    description = str(row.get("description") or "").strip().lower()
    if class_id.startswith(("0108", "0106", "0104", "0107", "0100", "0101", "0102", "0180")):
        return True
    return any(
        marker in description
        for marker in (
            "non-volatile memory controller",
            "nvme",
            "sata controller",
            "raid bus controller",
            "serial attached scsi",
            "storage controller",
            "mass storage",
        )
    )


def _match_nested_hostpci_assignment(
    assignments: list[dict[str, Any]],
    local_device: dict[str, Any] | None,
    *,
    vmid: str = "",
) -> dict[str, Any] | None:
    candidates = [row for row in assignments if _is_storage_hostpci_assignment(row)]
    if vmid:
        vmid_matches = [row for row in candidates if str(row.get("vmid") or "").strip() == vmid]
        if vmid_matches:
            candidates = vmid_matches
    if not candidates:
        return None

    transport = _local_device_transport(local_device)
    if transport == "nvme":
        nvme_matches = [
            row
            for row in candidates
            if str(row.get("class_id") or "").strip().lower().removeprefix("0x").startswith("0108")
            or "non-volatile memory controller" in str(row.get("description") or "").strip().lower()
            or "nvme" in str(row.get("description") or "").strip().lower()
        ]
        if len(nvme_matches) == 1:
            return nvme_matches[0]
        if nvme_matches:
            candidates = nvme_matches

    return candidates[0] if len(candidates) == 1 else None


def _root_dataset_row(
    dataset_lookup: dict[str, list[dict[str, Any]]],
    pool_name: str,
) -> dict[str, Any] | None:
    for row in dataset_lookup.get(pool_name) or []:
        if str(row.get("name") or "").strip() == pool_name:
            return row
    return None


def _build_guest_overlay_nodes(
    storage_payload: dict[str, Any],
    *,
    nested_guest_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not _NESTED_GUEST_HOST:
        return None
    disks = storage_payload.get("disks")
    if not isinstance(disks, list):
        return None

    nested_lookup = _nested_guest_filesystem_lookup(nested_guest_snapshot)
    role_nodes: list[dict[str, Any]] = []
    role_records: list[dict[str, Any]] = []
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
        filesystem_meta = _nested_guest_filesystem_meta(detail, local_device, nested_lookup)
        local_model = str(local_device.get("model") or "").strip()
        local_path = str(local_device.get("path") or "").strip()
        local_name = str(local_device.get("name") or "").strip()
        smart = None
        if local_path and "QEMU" not in local_model.upper():
            smart = _smart_payload(_NESTED_GUEST_HOST, local_path)

        facts = _non_null_facts(
            _fact("Guest host", source_host),
            _fact("Filesystem", filesystem_meta.get("fstype")),
            _fact("Mount", filesystem_meta.get("mount_path")),
            _fact("Path", filesystem_meta.get("path")),
            _fact("Volume label", filesystem_meta.get("volume_label")),
            _fact("UUID", filesystem_meta.get("uuid")),
            _fact("Guest pool", detail.get("local_pool")),
            _fact("Guest vdev", detail.get("local_vdev")),
            _fact("Part label", filesystem_meta.get("part_label")),
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
        browse_meta = _filesystem_browser_meta(
            host=source_host,
            root_path=filesystem_meta.get("mount_path"),
            filesystem=filesystem_meta.get("fstype"),
            source_path=filesystem_meta.get("path") or local_path,
            dataset_name=dataset_name,
        )

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
            meta=browse_meta,
        )
        role_nodes.append(node)
        if dataset_name:
            dataset_roles.setdefault(dataset_name, []).append(node)
        role_records.append(
            {
                "name": label,
                "detail": detail,
                "local_device": local_device,
                "parent_detail": parent_detail,
                "dataset_name": dataset_name,
                "total_bytes": total,
                "used_bytes": used,
                "smart": smart,
                "filesystem_meta": filesystem_meta,
            }
        )

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
    return {"guest_node": guest_node, "dataset_roles": dataset_roles, "roles": role_records}


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
    snapshot: dict[str, Any],
    datasets: list[dict[str, Any]],
    *,
    dataset_roles: dict[str, list[dict[str, Any]]],
    guest_identity_lookup: dict[str, dict[str, Any]],
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
        browse_meta = _filesystem_browser_meta(
            host=host,
            root_path=row.get("mountpoint"),
            filesystem="zfs" if ds_type == "filesystem" else ds_type,
            source_path=row.get("mountpoint"),
            dataset_name=name,
        )
        guest_meta = _guest_identity_meta_from_label(
            host,
            name.split("/")[-1],
            guest_identity_lookup,
        )
        guest_identity = (
            guest_meta.get("guest_identity")
            if isinstance(guest_meta.get("guest_identity"), dict)
            else {}
        )
        offline_browser_meta = {}
        if guest_identity:
            facts.append(
                {
                    "label": "Assigned to",
                    "value": str(guest_identity.get("guest_display") or "").strip(),
                }
            )
            if str(guest_identity.get("guest_kind") or "").strip().lower() == "vm":
                assignment = _match_guest_volume_assignment(
                    snapshot,
                    guest_id=str(guest_identity.get("guest_id") or "").strip(),
                    dataset_name=name,
                    label=name.split("/")[-1],
                )
                if assignment:
                    offline_browser_meta = _offline_browser_meta_from_assignment(host, assignment)
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
            meta={**(browse_meta or {}), **guest_meta, **offline_browser_meta},
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
    guest_identity_lookup = _guest_identity_lookup(snapshot)
    member_lookup = _build_pool_member_lookup(snapshot)
    pool_nodes: list[dict[str, Any]] = []

    for row in pool_rows:
        if not isinstance(row, dict):
            continue
        pool_name = str(row.get("name") or "").strip()
        if not pool_name:
            continue

        root_dataset = _root_dataset_row(dataset_lookup, pool_name)
        used = _to_int(root_dataset.get("used_bytes")) if isinstance(root_dataset, dict) else None
        total = _dataset_total_bytes(root_dataset)
        if total is None:
            total = _to_int(row.get("size_bytes"))
        if used is None:
            used = _to_int(row.get("allocated_bytes"))
        members = member_lookup.get(pool_name) or []
        member_count = sum(1 for member in members if isinstance(member, dict))
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
            member_used = _pool_member_used_bytes(
                row,
                member_size,
                member_count=member_count,
            )
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
            snapshot,
            dataset_lookup.get(pool_name) or [],
            dataset_roles=dataset_roles,
            guest_identity_lookup=guest_identity_lookup,
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


def _build_standalone_logical_nodes(
    host: str,
    top_disks: list[dict[str, Any]],
    *,
    pool_rows: dict[str, dict[str, Any]],
    member_lookup: dict[str, list[dict[str, Any]]],
    partition_pools: dict[str, list[str]],
    mount_by_source: dict[str, dict[str, Any]],
    filesystem_probe_by_path: dict[str, dict[str, Any]],
    guest_assignment_lookup: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    logical_nodes: list[dict[str, Any]] = []

    for disk in top_disks:
        if not isinstance(disk, dict):
            continue
        disk_path = str(disk.get("path") or "").strip()
        if guest_assignment_lookup.get(disk_path):
            continue
        disk_name = str(disk.get("name") or disk_path or "disk").strip() or "disk"
        disk_model = str(disk.get("model") or "").strip()
        disk_transport = str(disk.get("tran") or "").strip()
        children = [child for child in (disk.get("children") or []) if isinstance(child, dict)]
        candidates = children if children else [disk]

        for node in candidates:
            path = str(node.get("path") or "").strip()
            if not path or guest_assignment_lookup.get(path):
                continue
            if node is not disk and (partition_pools.get(path) or []):
                continue
            fstype = str(node.get("fstype") or "").strip().lower()
            if not _is_data_filesystem(fstype):
                continue

            mount = mount_by_source.get(path) or {}
            probe = filesystem_probe_by_path.get(path) or {}
            mount_target = str(node.get("mountpoint") or mount.get("target") or "").strip()
            if node is disk:
                used = _to_int(mount.get("used"))
                if used is None:
                    used = _to_int(probe.get("used_bytes"))
            else:
                used = _partition_used_bytes(
                    node,
                    pool_rows=pool_rows,
                    member_lookup=member_lookup,
                    partition_pools=partition_pools,
                    mount_by_source=mount_by_source,
                    filesystem_probe_by_path=filesystem_probe_by_path,
                )
            total = _to_int(node.get("size"))
            if not _should_surface_standalone_logical(
                total_bytes=total,
                used_bytes=used,
                mount_target=mount_target,
            ):
                continue

            part_label = _meaningful_part_label(node.get("partlabel"))
            label = _standalone_volume_label(
                node,
                mount_target=mount_target,
                probe=probe,
            )
            subtitle_bits = [f"{fstype.upper()} filesystem"]
            if mount_target:
                subtitle_bits.append(mount_target)
            elif disk_model:
                subtitle_bits.append(disk_model)

            logical_nodes.append(
                _node(
                    f"{host}:logical-fs:{path}",
                    "volume",
                    label,
                    subtitle=" · ".join(bit for bit in subtitle_bits if bit),
                    status="warn" if used is None else "ok",
                    note=_standalone_logical_note(
                        mount_target=mount_target,
                        probe=probe,
                        used_bytes=used,
                    ),
                    group="Logical systems",
                    facts=_non_null_facts(
                        _fact("Filesystem", fstype),
                        _fact("Mount", mount_target),
                        _fact("Path", path),
                        _fact("Volume label", probe.get("volume_label")),
                        _fact("UUID", node.get("uuid")),
                        _fact("Part label", part_label),
                        _fact("Backing drive", disk_name),
                        _fact("Drive model", disk_model),
                        _fact("Transport", disk_transport),
                        _fact(
                            "Source",
                            "Mounted filesystem" if mount_target else _probe_strategy_label(probe),
                        ),
                    ),
                    total_bytes=total,
                    used_bytes=used,
                    meta=_filesystem_browser_meta(
                        host=host,
                        root_path=mount_target,
                        filesystem=fstype,
                        source_path=path,
                        allow_device_fallback=True,
                    ),
                )
            )

    return logical_nodes


def _partition_used_bytes(
    part: dict[str, Any],
    *,
    pool_rows: dict[str, dict[str, Any]],
    member_lookup: dict[str, list[dict[str, Any]]],
    partition_pools: dict[str, list[str]],
    mount_by_source: dict[str, dict[str, Any]],
    filesystem_probe_by_path: dict[str, dict[str, Any]],
) -> int | None:
    path = str(part.get("path") or "").strip()
    mount = mount_by_source.get(path)
    if mount:
        used = _to_int(mount.get("used"))
        if used is not None:
            return used
    probe = filesystem_probe_by_path.get(path) or {}
    probed_used = _to_int(probe.get("used_bytes"))
    if probed_used is not None:
        return probed_used
    pools = partition_pools.get(path) or []
    if len(pools) == 1:
        size = _to_int(part.get("size"))
        pool_name = pools[0]
        pool_row = pool_rows.get(pool_name) or {}
        member_count = sum(
            1 for member in member_lookup.get(pool_name) or [] if isinstance(member, dict)
        )
        return _pool_member_used_bytes(
            pool_row,
            size,
            member_count=member_count or None,
        )
    return None


def _build_nested_passthrough_nodes(
    parent_host: str,
    snapshot: dict[str, Any],
    guest_overlay: dict[str, Any] | None,
    *,
    top_disks: list[dict[str, Any]],
) -> dict[str, Any]:
    if not _NESTED_GUEST_PARENT or parent_host != _NESTED_GUEST_PARENT:
        return {"drive_nodes": [], "logical_nodes": [], "guest_assigned_bytes": 0}
    if not isinstance(guest_overlay, dict):
        return {"drive_nodes": [], "logical_nodes": [], "guest_assigned_bytes": 0}

    role_records = guest_overlay.get("roles")
    if not isinstance(role_records, list):
        return {"drive_nodes": [], "logical_nodes": [], "guest_assigned_bytes": 0}

    parent_vmids = {
        str(parent_detail.get("vmid") or "").strip()
        for role in role_records
        if isinstance(role, dict)
        for parent_detail in [role.get("parent_detail")]
        if isinstance(parent_detail, dict)
        and str(parent_detail.get("host") or "").strip() == parent_host
        and str(parent_detail.get("vmid") or "").strip()
    }
    parent_vmid = next(iter(parent_vmids)) if len(parent_vmids) == 1 else ""
    hostpci_assignments = _hostpci_assignments(snapshot)
    existing_serials = {
        str(disk.get("serial") or "").strip().upper()
        for disk in top_disks
        if isinstance(disk, dict) and str(disk.get("serial") or "").strip()
    }

    drive_nodes: list[dict[str, Any]] = []
    logical_nodes: list[dict[str, Any]] = []
    guest_assigned_bytes = 0
    nested_guest_meta = (
        _guest_identity_meta(
            parent_host,
            guest_id=parent_vmid,
            guest_kind="vm",
            guest_name=_NESTED_GUEST_HOST,
        )
        if parent_vmid and _NESTED_GUEST_HOST
        else {}
    )

    for role in role_records:
        if not isinstance(role, dict):
            continue
        detail = role.get("detail")
        if not isinstance(detail, dict):
            continue
        source_host = str(detail.get("source_host") or "").strip()
        if not source_host or source_host != _NESTED_GUEST_HOST:
            continue
        parent_detail = role.get("parent_detail")
        if isinstance(parent_detail, dict) and parent_detail:
            continue

        local_device = role.get("local_device")
        if not isinstance(local_device, dict):
            continue
        local_model = str(local_device.get("model") or "").strip()
        if not local_model or "QEMU" in local_model.upper():
            continue

        serial = str(local_device.get("serial") or "").strip()
        if serial and serial.upper() in existing_serials:
            continue

        total = _to_int(role.get("total_bytes"))
        used = _to_int(role.get("used_bytes"))
        mount_path = str(detail.get("mount_path") or "").strip()
        role_name = str(role.get("name") or "storage").strip() or "storage"
        mount_leaf = _mount_leaf(mount_path)
        logical_label = mount_leaf or role_name
        physical_label = f"{source_host}-{logical_label}"
        raw_transport = _local_device_transport(local_device)
        transport_label = _transport_label(raw_transport)
        rotational = _local_device_rotational(local_device, transport=raw_transport)
        assignment = _match_nested_hostpci_assignment(
            hostpci_assignments,
            local_device,
            vmid=parent_vmid,
        )
        assigned_to = ""
        if parent_vmid:
            assigned_to = f"VM {parent_vmid} ({source_host})"
        elif source_host:
            assigned_to = source_host

        smart = role.get("smart") if isinstance(role.get("smart"), dict) else None
        filesystem_meta = (
            role.get("filesystem_meta") if isinstance(role.get("filesystem_meta"), dict) else {}
        )
        usage_note = (
            f"Whole-controller passthrough. Usage is measured from {source_host} at refresh time."
        )
        transport_bits = [bit for bit in [transport_label, local_model] if bit]

        drive_nodes.append(
            _node(
                f"{parent_host}:drive:nested:{source_host}:{logical_label}",
                "drive",
                physical_label,
                subtitle=" · ".join(transport_bits),
                status="info" if used is not None else "warn",
                note=usage_note,
                group="Physical drives",
                facts=_non_null_facts(
                    _fact("Filesystem", filesystem_meta.get("fstype")),
                    _fact("Mount", filesystem_meta.get("mount_path")),
                    _fact("Volume label", filesystem_meta.get("volume_label")),
                    _fact("UUID", filesystem_meta.get("uuid")),
                    _fact("Model", local_model),
                    _fact("Serial", serial),
                    _fact("Transport", transport_label),
                    _fact("Rotational", rotational),
                    _fact("Assigned to", assigned_to),
                    _fact("Assignment", "PCIe / whole-controller passthrough"),
                    _fact("Guest host", source_host),
                    _fact("Guest path", local_device.get("path")),
                    _fact("Parent PCI", assignment.get("pci_base") if assignment else ""),
                    _fact("Parent slot", assignment.get("slot") if assignment else ""),
                    _fact("PCI device", assignment.get("description") if assignment else ""),
                ),
                smart=smart,
                total_bytes=total,
                used_bytes=used,
                meta=nested_guest_meta or None,
            )
        )

        logical_nodes.append(
            _node(
                f"{parent_host}:logical:nested:{source_host}:{logical_label}",
                "volume",
                logical_label,
                subtitle=f"Nested guest filesystem · {source_host}",
                status="ok" if used is not None else "warn",
                note=usage_note,
                group="Logical systems",
                facts=_non_null_facts(
                    _fact("Filesystem", filesystem_meta.get("fstype")),
                    _fact("Mount", filesystem_meta.get("mount_path") or mount_path),
                    _fact("Path", filesystem_meta.get("path")),
                    _fact("Volume label", filesystem_meta.get("volume_label")),
                    _fact("UUID", filesystem_meta.get("uuid")),
                    _fact("Assigned to", assigned_to),
                    _fact("Model", local_model),
                    _fact("Serial", serial),
                    _fact("Transport", transport_label),
                    _fact("Guest path", local_device.get("path")),
                    _fact("Parent PCI", assignment.get("pci_base") if assignment else ""),
                    _fact("Source", f"Measured from {source_host}"),
                ),
                total_bytes=total,
                used_bytes=used,
                meta={
                    **(
                        _filesystem_browser_meta(
                            host=source_host,
                            root_path=filesystem_meta.get("mount_path") or mount_path,
                            filesystem=filesystem_meta.get("fstype"),
                            source_path=filesystem_meta.get("path") or local_device.get("path"),
                        )
                        or {}
                    ),
                    **nested_guest_meta,
                },
            )
        )
        guest_assigned_bytes += total or 0

    return {
        "drive_nodes": drive_nodes,
        "logical_nodes": logical_nodes,
        "guest_assigned_bytes": guest_assigned_bytes,
    }


def _build_drive_nodes(
    host: str,
    snapshot: dict[str, Any],
    top_disks: list[dict[str, Any]],
    *,
    by_path: dict[str, dict[str, Any]],
    pool_rows: dict[str, dict[str, Any]],
    member_lookup: dict[str, list[dict[str, Any]]],
    partition_pools: dict[str, list[str]],
    mount_by_source: dict[str, dict[str, Any]],
    filesystem_probe_by_path: dict[str, dict[str, Any]],
    guest_assignment_lookup: dict[str, list[dict[str, Any]]],
    thunderbolt_pools: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int | bool]]:
    drive_nodes: list[dict[str, Any]] = []
    rollup = {
        "known_used_bytes": 0,
        "guest_assigned_bytes": 0,
        "guest_assigned_known_total_bytes": 0,
        "guest_assigned_known_used_bytes": 0,
        "guest_assigned_unknown_bytes": 0,
    }

    for disk in top_disks:
        disk_path = str(disk.get("path") or "").strip()
        disk_name = str(disk.get("name") or disk_path or "disk")
        disk_total = _to_int(disk.get("size"))
        disk_assignments = guest_assignment_lookup.get(disk_path) or []
        disk_guest_meta = (
            _guest_identity_meta_from_assignment(host, disk_assignments[0])
            if disk_assignments
            else {}
        )
        disk_guest_usage = _pbs_usage_for_assignments(snapshot, disk_assignments)
        disk_probe = filesystem_probe_by_path.get(disk_path) or {}
        partition_nodes: list[dict[str, Any]] = []
        guest_partition_total_bytes = 0
        guest_partition_unknown_bytes = 0
        has_guest_partition = False
        children = disk.get("children") if isinstance(disk.get("children"), list) else []
        for part in children:
            if not isinstance(part, dict):
                continue
            part_path = str(part.get("path") or "").strip()
            part_total = _to_int(part.get("size"))
            part_assignments = guest_assignment_lookup.get(part_path) or []
            part_probe = filesystem_probe_by_path.get(part_path) or {}
            pools = partition_pools.get(part_path) or []
            mount = mount_by_source.get(part_path) or {}
            used = _partition_used_bytes(
                part,
                pool_rows=pool_rows,
                member_lookup=member_lookup,
                partition_pools=partition_pools,
                mount_by_source=mount_by_source,
                filesystem_probe_by_path=filesystem_probe_by_path,
            )
            part_note = ""
            if part_assignments:
                has_guest_partition = True
                guest_partition_total_bytes += part_total or 0
                if used is None:
                    guest_partition_unknown_bytes += part_total or 0
                    part_note = _guest_assignment_note(part_assignments, scope="partition")
                else:
                    part_note = _guest_assignment_note(
                        part_assignments,
                        scope="partition",
                        usage_probe=part_probe,
                    )
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
                    note=part_note,
                    group="Partitions",
                    facts=facts,
                    children=pool_children,
                    total_bytes=part_total,
                    used_bytes=used,
                    usage_text=_partial_usage_text(part_total, guest_assigned_bytes=part_total)
                    if part_assignments and used is None
                    else "",
                    meta=_guest_identity_meta_from_assignment(host, part_assignments[0])
                    if part_assignments
                    else None,
                )
            )

        used = None
        usage_text = ""
        status = "warn" if disk_assignments or has_guest_partition else "info"
        note = ""
        if partition_nodes:
            part_used = [
                node.get("used_bytes")
                for node in partition_nodes
                if node.get("used_bytes") is not None
            ]
            if guest_partition_unknown_bytes:
                known_used = int(sum(part_used)) if part_used else None
                usage_text = _partial_usage_text(
                    disk_total,
                    known_used_bytes=known_used,
                    guest_assigned_bytes=guest_partition_unknown_bytes or None,
                )
            elif part_used:
                used = int(sum(part_used))
        elif mount_by_source.get(disk_path):
            used = _to_int(mount_by_source[disk_path].get("used"))
        else:
            used = _to_int(disk_probe.get("used_bytes"))
        if disk_assignments:
            rollup["guest_assigned_bytes"] += disk_total or 0
            guest_total = _to_int(disk_guest_usage.get("total_bytes")) if disk_guest_usage else None
            guest_used = _to_int(disk_guest_usage.get("used_bytes")) if disk_guest_usage else None
            if guest_total:
                rollup["guest_assigned_known_total_bytes"] += guest_total
                if guest_used is not None:
                    rollup["guest_assigned_known_used_bytes"] += guest_used
                datastore = (
                    str(disk_guest_usage.get("datastore") or "").strip() if disk_guest_usage else ""
                )
                scope_label = f"PBS {datastore}" if datastore else "the guest datastore"
                usage_text = (
                    f"{_format_bytes(guest_used)} / {_format_bytes(guest_total)} in {scope_label}"
                )
                note = _guest_assignment_note(
                    disk_assignments,
                    scope="drive",
                    guest_usage=disk_guest_usage,
                )
            elif used is None:
                rollup["guest_assigned_unknown_bytes"] += disk_total or 0
                usage_text = _partial_usage_text(disk_total, guest_assigned_bytes=disk_total)
                note = _guest_assignment_note(disk_assignments, scope="drive")
            else:
                note = _guest_assignment_note(
                    disk_assignments,
                    scope="drive",
                    usage_probe=disk_probe,
                )
        elif has_guest_partition:
            rollup["guest_assigned_bytes"] += guest_partition_total_bytes
            if guest_partition_unknown_bytes:
                rollup["guest_assigned_unknown_bytes"] += guest_partition_unknown_bytes
        if used is not None:
            rollup["known_used_bytes"] += used

        transport = str(disk.get("tran") or "").strip()
        disk_pool_names = list(partition_pools.get(disk_path) or [])
        for part in children:
            if not isinstance(part, dict):
                continue
            part_path = str(part.get("path") or "").strip()
            for pool_name in partition_pools.get(part_path) or []:
                if pool_name not in disk_pool_names:
                    disk_pool_names.append(pool_name)
        transport_extras: list[str] = []
        for pool_name in disk_pool_names:
            transport_extras.extend(
                _thunderbolt_connection_tags(
                    thunderbolt_pools.get(pool_name),
                    raw_transport=transport,
                )
            )
        transport_label = _transport_label(transport, *transport_extras)
        subtitle_bits = [
            bit
            for bit in [
                transport_label,
                str(disk.get("model") or "").strip(),
            ]
            if bit
        ]
        facts = _non_null_facts(
            _fact("Path", disk_path),
            _fact("Model", disk.get("model")),
            _fact("Serial", disk.get("serial")),
            _fact("Vendor", disk.get("vendor")),
            _fact("Transport", transport_label),
            _fact("Rotational", "yes" if _to_int(disk.get("rota")) == 1 else "no"),
        )
        facts.extend(_guest_assignment_facts(disk_assignments))
        facts.extend(_guest_usage_facts(disk_guest_usage))
        if has_guest_partition and not disk_assignments:
            facts.append(
                {
                    "label": "Guest-assigned partitions",
                    "value": str(
                        sum(1 for node in partition_nodes if node.get("status") == "warn")
                    ),
                }
            )
            note = "One or more partitions are assigned directly to a guest. " + (
                "Usage on some of that capacity is still unknown in this host view."
                if guest_partition_unknown_bytes
                else "Usage is estimated from read-only filesystem metadata where possible."
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
                meta={
                    **disk_guest_meta,
                    **(
                        {"usage_pct": disk_guest_usage.get("usage_pct")}
                        if isinstance(disk_guest_usage, dict)
                        and disk_guest_usage.get("total_bytes")
                        else {}
                    ),
                }
                or None,
            )
        )

    return drive_nodes, rollup


def _build_host_node(
    host: str,
    snapshot_result: dict[str, Any],
    *,
    guest_overlay: dict[str, Any] | None,
    thunderbolt_pools: dict[str, dict[str, Any]],
    layout_hints: dict[str, Any] | None = None,
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
    filesystem_probe_by_path = _filesystem_probe_index(snapshot)
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
    standalone_logical_nodes = _build_standalone_logical_nodes(
        host,
        top_disks,
        pool_rows=pool_rows,
        member_lookup=member_lookup,
        partition_pools=partition_pools,
        mount_by_source=mount_by_source,
        filesystem_probe_by_path=filesystem_probe_by_path,
        guest_assignment_lookup=guest_assignment_lookup,
    )
    logical_nodes = pool_nodes + standalone_logical_nodes
    drive_nodes, drive_rollup = _build_drive_nodes(
        host,
        snapshot,
        top_disks,
        by_path=by_path,
        pool_rows=pool_rows,
        member_lookup=member_lookup,
        partition_pools=partition_pools,
        mount_by_source=mount_by_source,
        filesystem_probe_by_path=filesystem_probe_by_path,
        guest_assignment_lookup=guest_assignment_lookup,
        thunderbolt_pools=thunderbolt_pools,
    )
    nested_passthrough = _build_nested_passthrough_nodes(
        host,
        snapshot,
        guest_overlay,
        top_disks=top_disks,
    )
    if nested_passthrough.get("drive_nodes"):
        drive_nodes.extend(
            node for node in nested_passthrough.get("drive_nodes") or [] if isinstance(node, dict)
        )
    if nested_passthrough.get("logical_nodes"):
        logical_nodes.extend(
            node for node in nested_passthrough.get("logical_nodes") or [] if isinstance(node, dict)
        )

    children = drive_nodes + logical_nodes
    if _NESTED_GUEST_PARENT and host == _NESTED_GUEST_PARENT and isinstance(guest_overlay, dict):
        guest_node = guest_overlay.get("guest_node")
        if isinstance(guest_node, dict):
            children.append(guest_node)

    installed_total = int(sum(node.get("total_bytes") or 0 for node in drive_nodes)) or None
    known_total = int(sum(node.get("total_bytes") or 0 for node in logical_nodes)) or None
    known_used = (
        int(
            sum(
                node.get("used_bytes") or 0
                for node in logical_nodes
                if node.get("used_bytes") is not None
            )
        )
        or None
    )
    guest_assigned_total = int(drive_rollup.get("guest_assigned_bytes") or 0) + int(
        nested_passthrough.get("guest_assigned_bytes") or 0
    )
    guest_assigned_known_total = int(drive_rollup.get("guest_assigned_known_total_bytes") or 0)
    guest_assigned_known_used = int(drive_rollup.get("guest_assigned_known_used_bytes") or 0)
    guest_assigned_unknown = int(drive_rollup.get("guest_assigned_unknown_bytes") or 0)
    total = None
    used = None
    usage_text = ""
    if logical_nodes:
        total = ((known_total or 0) + guest_assigned_known_total) or None
        known_visible_used = ((known_used or 0) + guest_assigned_known_used) or None
        if guest_assigned_unknown:
            used = None
            usage_text = _partial_usage_text(
                ((known_total or 0) + guest_assigned_known_total + guest_assigned_unknown) or None,
                known_used_bytes=known_visible_used,
                guest_assigned_bytes=guest_assigned_unknown or None,
            )
            total = (
                (known_total or 0) + guest_assigned_known_total + guest_assigned_unknown
            ) or None
        else:
            used = known_visible_used
    elif drive_nodes:
        total = installed_total
        if guest_assigned_unknown:
            used = None
            usage_text = _partial_usage_text(
                total,
                known_used_bytes=(
                    (drive_rollup.get("known_used_bytes") or 0) + guest_assigned_known_used
                )
                or None,
                guest_assigned_bytes=guest_assigned_unknown or None,
            )
        elif any(node.get("used_bytes") is not None for node in drive_nodes):
            used = (
                int(sum(node.get("used_bytes") or 0 for node in drive_nodes))
                + guest_assigned_known_used
            ) or None

    subtitle_bits = []
    if drive_nodes:
        subtitle_bits.append(
            f"{len(drive_nodes)} physical drive{'s' if len(drive_nodes) != 1 else ''}"
        )
    if logical_nodes:
        subtitle_bits.append(
            f"{len(logical_nodes)} logical system{'s' if len(logical_nodes) != 1 else ''}"
        )

    note = "Refreshes only on page open and manual refresh."
    if _NESTED_GUEST_PARENT and host == _NESTED_GUEST_PARENT:
        note = "Host view combines a live SSH storage snapshot with the existing AI Control overlay for nested guest context."
    if guest_assigned_unknown:
        note = f"{note} One or more drives are assigned directly to a guest, so usage on some of that capacity is still partial here."

    facts = [{"label": "Source", "value": "Live SSH snapshot"}]
    if guest_assigned_total:
        facts.append({"label": "Guest-assigned", "value": _format_bytes(int(guest_assigned_total))})
    if guest_assigned_known_total:
        facts.append(
            {
                "label": "Guest-known",
                "value": _format_bytes(int(guest_assigned_known_total)),
            }
        )

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
            "installed_total_bytes": installed_total,
            "known_total_bytes": known_total,
            "guest_assigned_bytes": int(guest_assigned_total) or None,
            "guest_assigned_known_total_bytes": guest_assigned_known_total or None,
            "guest_assigned_known_used_bytes": guest_assigned_known_used or None,
            "guest_assigned_unknown_bytes": guest_assigned_unknown or None,
            "known_used_bytes": known_used,
            "layout_hints": copy.deepcopy(layout_hints)
            if isinstance(layout_hints, dict) and layout_hints
            else None,
        },
    )


def _build_topology(
    inventories: list[dict[str, Any]],
    storage_payload: dict[str, Any] | None,
    thunderbolt_payload: dict[str, Any] | None,
    nested_guest_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    guest_overlay = _build_guest_overlay_nodes(
        storage_payload or {},
        nested_guest_snapshot=nested_guest_snapshot,
    )
    thunderbolt_pools = _build_thunderbolt_overlay(thunderbolt_payload or {})
    layout_hints = _layout_hints_payload()
    host_nodes = [
        _build_host_node(
            snapshot_result.get("host") or "host",
            snapshot_result,
            guest_overlay=guest_overlay,
            thunderbolt_pools=thunderbolt_pools,
            layout_hints=_layout_hints_for_host(
                layout_hints,
                str(snapshot_result.get("host") or "host"),
            ),
        )
        for snapshot_result in inventories
    ]
    _merge_inventory_memory(host_nodes)

    total = None
    used = None
    usage_text = ""
    installed_total = (
        int(
            sum(
                node.get("installed_total_bytes") or node.get("total_bytes") or 0
                for node in host_nodes
            )
        )
        or None
    )
    known_total = (
        int(
            sum(
                node.get("known_total_bytes") or node.get("total_bytes") or 0 for node in host_nodes
            )
        )
        or None
    )
    guest_assigned_known_total = (
        int(sum(node.get("guest_assigned_known_total_bytes") or 0 for node in host_nodes)) or None
    )
    guest_assigned_known_used = (
        int(sum(node.get("guest_assigned_known_used_bytes") or 0 for node in host_nodes)) or None
    )
    guest_assigned_unknown_total = None
    known_used_total = None
    if host_nodes and any(node.get("total_bytes") is not None for node in host_nodes):
        total = int(sum(node.get("total_bytes") or 0 for node in host_nodes)) or None
        guest_assigned_unknown_total = (
            int(sum(node.get("guest_assigned_unknown_bytes") or 0 for node in host_nodes)) or None
        )
        known_used_total = (
            int(
                sum(node.get("known_used_bytes") or 0 for node in host_nodes)
                + sum(node.get("guest_assigned_known_used_bytes") or 0 for node in host_nodes)
            )
            or None
        )
        if guest_assigned_unknown_total:
            used = None
            usage_text = _partial_usage_text(
                total,
                known_used_bytes=known_used_total,
                guest_assigned_bytes=guest_assigned_unknown_total,
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
        meta={
            "installed_total_bytes": installed_total,
            "known_total_bytes": known_total,
            "guest_assigned_known_total_bytes": guest_assigned_known_total,
            "guest_assigned_known_used_bytes": guest_assigned_known_used,
            "known_used_bytes": known_used_total,
            "guest_assigned_unknown_bytes": guest_assigned_unknown_total,
        },
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

    nested_guest_snapshot = None
    if _NESTED_GUEST_HOST:
        for inventory in inventories:
            if (
                isinstance(inventory, dict)
                and inventory.get("ok")
                and str(inventory.get("host") or "").strip() == _NESTED_GUEST_HOST
                and isinstance(inventory.get("data"), dict)
            ):
                nested_guest_snapshot = inventory.get("data")
                break
        if (
            nested_guest_snapshot is None
            and storage_result.get("ok")
            and _needs_nested_guest_filesystem_snapshot(storage_result.get("data"))
        ):
            nested_guest_result = await _nested_guest_filesystem_host(_NESTED_GUEST_HOST)
            if nested_guest_result.get("ok") and isinstance(nested_guest_result.get("data"), dict):
                nested_guest_snapshot = nested_guest_result.get("data")

    root = _build_topology(
        inventories,
        storage_result.get("data") if storage_result.get("ok") else None,
        thunderbolt_result.get("data") if thunderbolt_result.get("ok") else None,
        nested_guest_snapshot=nested_guest_snapshot,
    )
    _apply_disks_notes(root, _disks_notes_lookup())
    return {
        "generated_at": _utcnow_iso(),
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


@router.post("/memory/forget")
async def disks_memory_forget(
    host: str = Query(..., description="Host label"),
    node_id: str = Query(..., description="Cached node id"),
) -> dict[str, Any]:
    clean_host = _safe_host(host)
    clean_node_id = _safe_node_id(node_id)
    removed = await asyncio.to_thread(_inventory_memory_forget, clean_host, clean_node_id)
    if not removed:
        raise HTTPException(404, "cached inventory item not found")
    return {"ok": True, "host": clean_host, "node_id": clean_node_id}


@router.post("/note")
async def disks_note(body: DisksNoteBody) -> dict[str, Any]:
    _persist_disks_note(body.node_id, body.note)
    return {
        "ok": True,
        "node_id": _safe_node_id(body.node_id),
        "note": str(body.note or "").strip(),
    }


@router.post("/offline-browse/open")
async def disks_offline_browse_open(body: DisksOfflineBrowseOpenBody) -> dict[str, Any]:
    clean_host = _safe_browse_host(body.host)
    clean_guest_id = str(body.guest_id or "").strip()
    if not clean_guest_id.isdigit():
        raise HTTPException(400, "guest_id must be numeric")
    clean_volume_ref = _safe_volume_ref(body.volume_ref)
    guest_name = str(body.guest_name or "").strip()
    volume_label = str(body.volume_label or "").strip() or clean_volume_ref

    prepare = await _offline_vm_prepare_host(
        clean_host,
        guest_id=clean_guest_id,
        volume_ref=clean_volume_ref,
    )
    if not prepare.get("ok") or not isinstance(prepare.get("data"), dict):
        detail = str(prepare.get("error") or "offline browse prepare failed").strip()
        lowered = detail.lower()
        if "must be stopped" in lowered:
            raise HTTPException(409, detail)
        raise HTTPException(502, detail)

    prepared = prepare["data"]
    session_id = secrets.token_hex(16)
    now_iso = _utcnow_iso()
    session = {
        "session_id": session_id,
        "state": "intent",
        "host": clean_host,
        "guest_id": clean_guest_id,
        "guest_name": guest_name,
        "volume_ref": clean_volume_ref,
        "volume_label": volume_label,
        "resolved_path": str(prepared.get("resolved_path") or "").strip(),
        "nbd_device": str(prepared.get("nbd_device") or "").strip(),
        "opened_at": now_iso,
        "last_heartbeat_at": now_iso,
        "timeout_seconds": _DISKS_OFFLINE_BROWSE_TIMEOUT_SECONDS,
    }
    _offline_browse_upsert_session(session)

    attach = await _offline_vm_attach_host(
        clean_host,
        resolved_path=str(prepared.get("resolved_path") or "").strip(),
        nbd_device=str(prepared.get("nbd_device") or "").strip(),
    )
    if not attach.get("ok") or not isinstance(attach.get("data"), dict):
        await _offline_browse_cleanup_session(session)
        _offline_browse_delete_session(session_id)
        detail = str(attach.get("error") or "offline browse attach failed").strip()
        lowered = detail.lower()
        if "no mountable filesystem" in lowered:
            raise HTTPException(400, detail)
        raise HTTPException(502, detail)

    attached = attach["data"]
    active_session = _offline_browse_update_session(
        session_id,
        lambda current: {
            **current,
            "state": "active",
            "sources": copy.deepcopy(attached.get("sources") or []),
            "last_heartbeat_at": _utcnow_iso(),
        },
    )
    return {"ok": True, **_offline_browse_public_session(active_session)}


@router.post("/offline-browse/heartbeat")
async def disks_offline_browse_heartbeat(body: DisksOfflineBrowseHeartbeatBody) -> dict[str, Any]:
    session = _offline_browse_update_session(
        body.session_id,
        lambda current: {**current, "last_heartbeat_at": _utcnow_iso()},
    )
    return {"ok": True, **_offline_browse_public_session(session)}


@router.post("/offline-browse/close")
async def disks_offline_browse_close(body: DisksOfflineBrowseCloseBody) -> dict[str, Any]:
    session = _offline_browse_get_session(body.session_id)
    if not isinstance(session, dict):
        return {
            "ok": True,
            "already_closed": True,
            "session_id": str(body.session_id or "").strip(),
        }
    _offline_browse_update_session(
        body.session_id,
        lambda current: {
            **current,
            "state": "closing",
            "closed_at": _utcnow_iso(),
            "close_reason": "client_closed",
        },
    )
    cleanup = await _offline_browse_cleanup_session(session)
    _offline_browse_delete_session(body.session_id)
    if not cleanup.get("ok"):
        raise HTTPException(502, str(cleanup.get("error") or "offline browse cleanup failed"))
    return {
        "ok": True,
        "session_id": str(body.session_id or "").strip(),
        "cleanup": cleanup.get("data") if isinstance(cleanup.get("data"), dict) else {},
    }


@router.post("/filesystem/tree")
async def disks_filesystem_tree(body: DisksFilesystemTreeBody) -> dict[str, Any]:
    clean_host = _safe_browse_host(body.host)
    browse_mode = _normalize_browser_mode(body.browse_mode)
    clean_source = _safe_device_path(body.source_path or "") if browse_mode == "device_ro" else ""
    clean_root = _normalize_browser_root_path(body.root_path)
    clean_path = _normalize_browser_relative_path(body.path)
    result = await _filesystem_tree_host(
        clean_host,
        browse_mode=browse_mode,
        source_path=clean_source,
        root_path=clean_root,
        relative_path=clean_path,
        limit=int(body.limit),
    )
    if result.get("ok") and isinstance(result.get("data"), dict):
        payload = result["data"]
        payload["host"] = clean_host
        payload["root_path"] = clean_root
        payload["browse_mode"] = browse_mode
        if clean_source:
            payload["source_path"] = clean_source
        return payload
    detail = str(result.get("error") or "Filesystem tree unavailable").strip()
    lowered = detail.lower()
    if "timed out" in lowered:
        raise HTTPException(504, detail)
    if any(
        marker in lowered
        for marker in (
            "filesystem root",
            "path escapes filesystem root",
            "path must be relative",
            "folder does not exist",
            "not a directory",
        )
    ):
        raise HTTPException(400, detail)
    raise HTTPException(502, detail)


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
