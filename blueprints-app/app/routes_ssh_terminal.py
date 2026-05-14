"""Browser terminal bridge for tightly scoped operational targets.

The browser never sends an arbitrary command.  It selects one target id from
this module, then Blueprints spawns that fixed command under a PTY and shuttles
bytes over a websocket.
"""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import ipaddress
import json
import logging
import os
import pty
import re
import shlex
import signal
import struct
import subprocess
import termios
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect

from . import config as cfg
from .auth import verify_token

log = logging.getLogger(__name__)
router = APIRouter(prefix="/ssh-terminal", tags=["ssh-terminal"])

_LOOPBACK = frozenset({"127.0.0.1", "::1"})
_MAX_COLS = 240
_MAX_ROWS = 80
_ACTIVE_PROCESSES: dict[str, set[subprocess.Popen[bytes]]] = {}


@dataclass(frozen=True)
class TerminalTarget:
    target_id: str
    label: str
    kind: str
    stack: str
    command: tuple[str, ...]
    cwd: str = "/root"
    enabled: bool = True
    menu_order: int = 100
    show_in_menu: bool = True


_TARGET_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{1,80}$")
_PRIVATE_TARGETS_FILE_ENV = "SSH_TERMINAL_TARGETS_FILE"


_STATIC_TARGETS: dict[str, TerminalTarget] = {
    "local-hermes": TerminalTarget(
        target_id="local-hermes",
        label="Hermes Local Agent",
        kind="docker-exec",
        stack="hermes-local",
        command=(
            "docker",
            "exec",
            "-it",
            "-e",
            "TERM=xterm-256color",
            "hermes-local",
            "/opt/hermes/.venv/bin/hermes",
        ),
        cwd="/root/xarta-node",
        menu_order=1,
        show_in_menu=False,
    ),
    "local-hermes-container": TerminalTarget(
        target_id="local-hermes-container",
        label="Hermes Local",
        kind="docker-exec",
        stack="hermes-local",
        command=(
            "docker",
            "exec",
            "-it",
            "-e",
            "TERM=xterm-256color",
            "-e",
            "PS1=hermes-local:\\w\\$ ",
            "-w",
            "/opt/hermes",
            "hermes-local",
            "/bin/bash",
            "--noprofile",
            "--norc",
            "-i",
        ),
        cwd="/root/xarta-node",
        menu_order=0,
    ),
    "local-hermes-setup": TerminalTarget(
        target_id="local-hermes-setup",
        label="Local Hermes Setup",
        kind="docker-exec",
        stack="hermes-local",
        command=(
            "docker",
            "exec",
            "-it",
            "-e",
            "TERM=xterm-256color",
            "hermes-local",
            "/opt/hermes/.venv/bin/hermes",
            "setup",
        ),
        cwd="/root/xarta-node",
        menu_order=2,
        show_in_menu=False,
    ),
}


def _require_str(spec: dict[str, Any], key: str) -> str:
    value = str(spec.get(key, "")).strip()
    if not value:
        raise RuntimeError(f"terminal target {spec.get('target_id')!r} is missing {key!r}")
    return value


def _safe_target_id(spec: dict[str, Any]) -> str:
    target_id = _require_str(spec, "target_id")
    if not _TARGET_ID_RE.fullmatch(target_id):
        raise RuntimeError(f"invalid terminal target id: {target_id!r}")
    return target_id


def _target_bool(spec: dict[str, Any], key: str, default: bool) -> bool:
    value = spec.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _target_int(spec: dict[str, Any], key: str, default: int) -> int:
    try:
        return int(spec.get(key, default))
    except (TypeError, ValueError):
        return default


def _identity_file_from_spec(spec: dict[str, Any]) -> str:
    identity_file = str(spec.get("identity_file", "")).strip()
    identity_file_env = str(spec.get("identity_file_env", "")).strip()
    if identity_file and identity_file_env:
        raise RuntimeError(
            f"terminal target {spec.get('target_id')!r} sets both identity_file and identity_file_env"
        )
    if identity_file_env:
        identity_file = os.environ.get(identity_file_env, "").strip()
        if not identity_file:
            raise RuntimeError(
                f"terminal target {spec.get('target_id')!r} requires unset env var {identity_file_env!r}"
            )
    if not identity_file:
        raise RuntimeError(f"terminal target {spec.get('target_id')!r} has no SSH identity")
    if not os.path.isfile(identity_file):
        raise RuntimeError(
            f"terminal target {spec.get('target_id')!r} SSH identity not found: {identity_file}"
        )
    return identity_file


def _ssh_command_from_spec(spec: dict[str, Any]) -> tuple[str, ...]:
    host = _require_str(spec, "host")
    user = _require_str(spec, "user")
    identity_file = _identity_file_from_spec(spec)
    connect_timeout = _target_int(spec, "connect_timeout", 10)

    command = [
        "ssh",
        "-tt",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        f"ConnectTimeout={connect_timeout}",
        "-o",
        "IdentitiesOnly=yes",
        "-i",
        identity_file,
    ]

    source_ip = str(spec.get("source_ip", "")).strip()
    if source_ip:
        command += ["-b", source_ip]

    host_key_alias = str(spec.get("host_key_alias", "")).strip()
    if host_key_alias:
        command += ["-o", f"HostKeyAlias={host_key_alias}"]

    command.append("-A" if _target_bool(spec, "forward_agent", False) else "-a")
    command.append(f"{user}@{host}")

    remote_command = str(spec.get("remote_command", "")).strip()
    if remote_command:
        command.append(remote_command)

    return tuple(command)


def _vps_dockge_hosts() -> list[str]:
    hosts: list[str] = []
    for raw in cfg.VPS_DOCKGE_SSH_HOSTS.split(","):
        host = raw.strip()
        if host and host not in hosts:
            hosts.append(host)
    if not hosts:
        raise RuntimeError("VPS_DOCKGE_SSH_HOSTS is empty")
    return hosts


def _vps_dockge_ssh_command_from_spec(spec: dict[str, Any]) -> tuple[str, ...]:
    """Build an interactive VPS SSH command using the Dockge failover settings."""

    remote_command = _require_str(spec, "remote_command")
    user = cfg.VPS_DOCKGE_SSH_USER.strip()
    if not user:
        raise RuntimeError("VPS_DOCKGE_SSH_USER is empty")

    key = cfg.VPS_DOCKGE_SSH_KEY.strip()
    if not key:
        raise RuntimeError("VPS_DOCKGE_SSH_KEY is empty")
    if not os.path.isfile(key):
        raise RuntimeError(f"VPS_DOCKGE_SSH_KEY not found: {key}")

    connect_timeout = _target_int(spec, "connect_timeout", 8)
    host_array = " ".join(shlex.quote(host) for host in _vps_dockge_hosts())
    source_ip = str(spec.get("source_ip", "")).strip()
    source_bind = f" -b {shlex.quote(source_ip)}" if source_ip else ""
    script = (
        "set -u\n"
        f"hosts=({host_array})\n"
        "last_rc=255\n"
        "for host in \"${hosts[@]}\"; do\n"
        "  ssh -tt"
        f" -i {shlex.quote(key)}"
        " -o BatchMode=yes"
        " -o StrictHostKeyChecking=accept-new"
        " -o ServerAliveInterval=30"
        " -o ServerAliveCountMax=3"
        " -o IdentitiesOnly=yes"
        f" -o ConnectTimeout={connect_timeout}"
        f"{source_bind}"
        f" -a {shlex.quote(user)}@\"$host\" {shlex.quote(remote_command)}\n"
        "  rc=$?\n"
        "  if [ \"$rc\" -ne 255 ]; then exit \"$rc\"; fi\n"
        "  last_rc=$rc\n"
        "done\n"
        "exit \"$last_rc\"\n"
    )
    return ("bash", "-lc", script)


def _target_from_spec(spec: dict[str, Any]) -> TerminalTarget:
    target_id = _safe_target_id(spec)
    kind = str(spec.get("kind", "ssh")).strip().lower()
    if kind not in {"ssh", "vps-dockge-ssh"}:
        raise RuntimeError(f"unsupported terminal target kind for {target_id!r}: {kind!r}")
    enabled = _target_bool(spec, "enabled", True)
    command: tuple[str, ...] = ()
    if enabled:
        if kind == "vps-dockge-ssh":
            command = _vps_dockge_ssh_command_from_spec(spec)
        else:
            command = _ssh_command_from_spec(spec)
    return TerminalTarget(
        target_id=target_id,
        label=_require_str(spec, "label"),
        kind=kind,
        stack=str(spec.get("stack", "")).strip(),
        command=command,
        cwd=str(spec.get("cwd", "/root")).strip() or "/root",
        enabled=enabled,
        menu_order=_target_int(spec, "menu_order", 100),
        show_in_menu=_target_bool(spec, "show_in_menu", True),
    )


def _load_private_targets() -> dict[str, TerminalTarget]:
    path = os.environ.get(_PRIVATE_TARGETS_FILE_ENV, "").strip()
    if not path:
        return {}
    if not os.path.isfile(path):
        raise RuntimeError(f"{_PRIVATE_TARGETS_FILE_ENV} points to missing file: {path}")
    with open(path, encoding="utf-8") as handle:
        raw = json.load(handle)
    specs = raw.get("targets") if isinstance(raw, dict) else raw
    if not isinstance(specs, list):
        raise RuntimeError(f"{_PRIVATE_TARGETS_FILE_ENV} must contain a list or {{'targets': [...]}}")

    targets: dict[str, TerminalTarget] = {}
    for spec in specs:
        if not isinstance(spec, dict):
            raise RuntimeError(f"{_PRIVATE_TARGETS_FILE_ENV} contains a non-object target")
        target = _target_from_spec(spec)
        targets[target.target_id] = target
    return targets


def _targets() -> dict[str, TerminalTarget]:
    targets = dict(_STATIC_TARGETS)
    targets.update(_load_private_targets())
    return targets


def _allowed_networks() -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for raw in cfg.ALLOWED_NETWORKS_RAW.split(","):
        raw = raw.strip()
        if not raw:
            continue
        try:
            networks.append(ipaddress.ip_network(raw, strict=False))
        except ValueError:
            log.warning("ssh-terminal: ignoring invalid CIDR %r", raw)
    return networks


def _client_ip(websocket: WebSocket) -> str:
    xff = websocket.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()
    return websocket.client.host if websocket.client else "unknown"


def _client_ip_allowed(ip_str: str) -> bool:
    if ip_str in _LOOPBACK:
        return True
    networks = _allowed_networks()
    if not networks:
        return True
    try:
        ip_obj = ipaddress.ip_address(ip_str)
    except ValueError:
        return False
    return any(ip_obj in network for network in networks)


def _token_allowed(token: str) -> bool:
    if not (cfg.API_SECRET or cfg.SYNC_SECRET):
        return True
    return bool(
        (cfg.API_SECRET and verify_token(cfg.API_SECRET, token))
        or (cfg.SYNC_SECRET and verify_token(cfg.SYNC_SECRET, token))
    )


def _target_payload(target: TerminalTarget) -> dict[str, str | bool | int]:
    return {
        "target_id": target.target_id,
        "label": target.label,
        "kind": target.kind,
        "stack": target.stack,
        "enabled": target.enabled,
        "menu_order": target.menu_order,
        "show_in_menu": target.show_in_menu,
    }


def _resize_pty(fd: int, cols: Any, rows: Any) -> None:
    try:
        clean_cols = max(20, min(_MAX_COLS, int(cols)))
        clean_rows = max(5, min(_MAX_ROWS, int(rows)))
    except (TypeError, ValueError):
        return
    packed = struct.pack("HHHH", clean_rows, clean_cols, 0, 0)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, packed)


async def _send_output(websocket: WebSocket, fd: int, process: subprocess.Popen[bytes]) -> None:
    loop = asyncio.get_running_loop()
    while True:
        if process.poll() is not None:
            break
        try:
            data = await loop.run_in_executor(None, os.read, fd, 8192)
        except OSError:
            break
        if not data:
            break
        await websocket.send_text(data.decode("utf-8", errors="replace"))


async def _receive_input(websocket: WebSocket, fd: int) -> None:
    while True:
        message = await websocket.receive_text()
        try:
            payload = json.loads(message)
        except json.JSONDecodeError:
            continue
        kind = payload.get("type")
        if kind == "input":
            data = str(payload.get("data", ""))
            if data:
                os.write(fd, data.encode("utf-8", errors="ignore"))
        elif kind == "resize":
            _resize_pty(fd, payload.get("cols"), payload.get("rows"))


def _terminate_process(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=2)


def _register_process(target_id: str, process: subprocess.Popen[bytes]) -> None:
    _ACTIVE_PROCESSES.setdefault(target_id, set()).add(process)


def _unregister_process(target_id: str, process: subprocess.Popen[bytes]) -> None:
    processes = _ACTIVE_PROCESSES.get(target_id)
    if not processes:
        return
    processes.discard(process)
    if not processes:
        _ACTIVE_PROCESSES.pop(target_id, None)


def _terminate_target_processes(target_id: str) -> int:
    processes = list(_ACTIVE_PROCESSES.get(target_id, set()))
    stopped = 0
    for process in processes:
        if process.poll() is None:
            _terminate_process(process)
            stopped += 1
        _unregister_process(target_id, process)
    return stopped


@router.get("/targets")
async def list_terminal_targets() -> list[dict[str, str | bool | int]]:
    try:
        targets = _targets()
    except RuntimeError as exc:
        log.error("ssh-terminal: unable to load targets: %s", exc)
        raise HTTPException(500, str(exc)) from exc
    return [
        _target_payload(target)
        for target in sorted(targets.values(), key=lambda item: (item.menu_order, item.label))
    ]


@router.post("/targets/{target_id}/disconnect")
async def disconnect_terminal_target(target_id: str) -> dict[str, int | str | bool]:
    try:
        target = _targets().get(target_id)
    except RuntimeError as exc:
        log.error("ssh-terminal: unable to load targets: %s", exc)
        raise HTTPException(500, str(exc)) from exc
    if not target:
        raise HTTPException(404, f"Unknown terminal target: {target_id}")
    stopped = _terminate_target_processes(target_id)
    return {"ok": True, "target_id": target_id, "stopped": stopped}


@router.websocket("/ws")
async def terminal_websocket(websocket: WebSocket) -> None:
    target_id = websocket.query_params.get("target", "")
    token = websocket.query_params.get("token", "")
    client_ip = _client_ip(websocket)

    if not _client_ip_allowed(client_ip):
        log.warning("ssh-terminal: blocked websocket from %s outside allowlist", client_ip)
        await websocket.close(code=1008, reason="Forbidden")
        return
    if not _token_allowed(token):
        log.warning("ssh-terminal: blocked websocket from %s with invalid token", client_ip)
        await websocket.close(code=1008, reason="Unauthorized")
        return

    try:
        target = _targets().get(target_id)
    except RuntimeError as exc:
        log.error("ssh-terminal: unable to load targets: %s", exc)
        await websocket.close(code=1011, reason="Target config unavailable")
        return
    if not target or not target.enabled:
        log.warning("ssh-terminal: blocked websocket from %s for unknown target %r", client_ip, target_id)
        await websocket.close(code=1008, reason="Unknown target")
        return

    if not os.path.isdir(target.cwd):
        log.error("ssh-terminal: target %s cwd unavailable: %s", target.target_id, target.cwd)
        await websocket.close(code=1011, reason="Target cwd unavailable")
        return

    await websocket.accept()

    env = os.environ.copy()
    env.update({"TERM": "xterm-256color", "COLORTERM": "truecolor"})
    master_fd, slave_fd = pty.openpty()
    _resize_pty(master_fd, websocket.query_params.get("cols", 100), websocket.query_params.get("rows", 28))

    def _prepare_child() -> None:
        os.setsid()
        fcntl.ioctl(slave_fd, termios.TIOCSCTTY, 0)

    process = subprocess.Popen(
        target.command,
        cwd=target.cwd,
        env=env,
        stdin=slave_fd,
        stdout=slave_fd,
        stderr=slave_fd,
        close_fds=True,
        preexec_fn=_prepare_child,
    )
    _register_process(target.target_id, process)
    os.close(slave_fd)

    output_task = asyncio.create_task(_send_output(websocket, master_fd, process))
    input_task = asyncio.create_task(_receive_input(websocket, master_fd))
    try:
        done, pending = await asyncio.wait(
            {output_task, input_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            with contextlib.suppress(WebSocketDisconnect, OSError):
                await task
        for task in pending:
            task.cancel()
    except WebSocketDisconnect:
        pass
    except OSError:
        pass
    finally:
        with contextlib.suppress(OSError):
            os.close(master_fd)
        for task in (output_task, input_task):
            if not task.done():
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError, WebSocketDisconnect, OSError):
                await task
        _terminate_process(process)
        _unregister_process(target.target_id, process)
