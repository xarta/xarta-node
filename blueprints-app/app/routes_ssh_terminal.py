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


_TARGETS: dict[str, TerminalTarget] = {
    "local-hermes": TerminalTarget(
        target_id="local-hermes",
        label="Local Hermes",
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
    ),
}


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


def _target_payload(target: TerminalTarget) -> dict[str, str | bool]:
    return {
        "target_id": target.target_id,
        "label": target.label,
        "kind": target.kind,
        "stack": target.stack,
        "enabled": target.enabled,
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
async def list_terminal_targets() -> list[dict[str, str | bool]]:
    return [_target_payload(target) for target in _TARGETS.values()]


@router.post("/targets/{target_id}/disconnect")
async def disconnect_terminal_target(target_id: str) -> dict[str, int | str | bool]:
    target = _TARGETS.get(target_id)
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

    target = _TARGETS.get(target_id)
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
