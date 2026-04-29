"""ssh.py — Single-point SSH parameter lookup from the ssh_targets table.

Every route or script that needs to open an SSH connection to any host must
use this module instead of reading env vars directly.  The ssh_targets table
is the authoritative source; it is populated by POST /api/v1/ssh-targets/rebuild.

Public surface:

  get_ssh_params(ip)              → {key_path, source_ip, key_env}
  make_ssh_args(ip, ...)          → ["-i", key, "-b", src, "-o", ...]
  probe_status_for_host_type(ht)  → {configured, ssh_key_present, reason}
  resolve_env_key(key_env)        → key_path string

Exceptions (both subclass RuntimeError):
  SshTargetNotFound   — ip is absent from ssh_targets
  SshKeyMissing       — key env var not set or file missing on this node
"""

from __future__ import annotations

import os

from .db import get_conn

# ── Exceptions ────────────────────────────────────────────────────────────────

class SshTargetNotFound(RuntimeError):
    """Raised when no ssh_targets entry exists for the given IP."""


class SshKeyMissing(RuntimeError):
    """Raised when the key env var is not set or the key file does not exist."""


# ── Core helpers ──────────────────────────────────────────────────────────────

def get_ssh_params(ip: str) -> dict:
    """Look up ssh_targets by IP and return SSH connection parameters.

    Returns a dict:
        key_path  (str):        resolved absolute path to private key file
        source_ip (str | None): local interface IP for -b VLAN binding
        key_env   (str):        env var name used (e.g. PROXMOX_SSH_KEY)

    Raises SshTargetNotFound if ip is not in ssh_targets (run /rebuild first).
    Raises SshKeyMissing if the env var resolves to a missing or empty path.
    """
    with get_conn() as conn:
        row = conn.execute(
            "SELECT key_env_var, source_ip FROM ssh_targets WHERE ip_address=?",
            (ip,),
        ).fetchone()

    if row is None:
        raise SshTargetNotFound(
            f"No ssh_targets entry for {ip!r} — "
            "run POST /api/v1/ssh-targets/rebuild to populate the table"
        )

    key_env = row["key_env_var"]
    key_path = os.environ.get(key_env, "").strip()
    if not key_path:
        raise SshKeyMissing(
            f"Env var {key_env!r} is not set (needed to SSH to {ip})"
        )
    if not os.path.isfile(key_path):
        raise SshKeyMissing(
            f"Key file {key_path!r} not found "
            f"(env var {key_env!r}, target {ip}) — "
            "this node may not be the probe node for this host"
        )

    return {
        "key_path":  key_path,
        "source_ip": row["source_ip"],
        "key_env":   key_env,
    }


def make_ssh_args(ip: str, *, connect_timeout: int = 8) -> list[str]:
    """Build the ssh argv fragment for connecting to ip.

    Returns e.g.::

        ["-i", "/root/.ssh/<key-file>",
         "-b", "10.0.0.1",   # source_ip from ssh_targets row (omitted when NULL)
         "-o", "StrictHostKeyChecking=no",
         "-o", "BatchMode=yes",
         "-o", "ConnectTimeout=8"]

    The key path is resolved from the env var stored in ssh_targets.
    source_ip / -b is omitted when not present in ssh_targets.
    Raises SshTargetNotFound or SshKeyMissing on lookup failure.
    """
    p = get_ssh_params(ip)
    args: list[str] = [
        "-i", p["key_path"],
        "-o", "StrictHostKeyChecking=no",
        "-o", "BatchMode=yes",
        "-o", f"ConnectTimeout={connect_timeout}",
    ]
    if p["source_ip"]:
        args += ["-b", p["source_ip"]]
    return args


def probe_status_for_host_type(host_type: str) -> dict:
    """Check whether this node has a resolvable SSH key for host_type targets.

    Returns:
        {"configured": bool, "ssh_key_present": bool, "reason": str}

    Used by /probe/status endpoints (proxmox_config, dockge_stacks, etc.)
    so they all share the same lookup logic.
    """
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT key_env_var FROM ssh_targets WHERE host_type=? LIMIT 5",
            (host_type,),
        ).fetchall()

    if not rows:
        return {
            "configured":     False,
            "ssh_key_present": False,
            "reason": (
                f"No ssh_targets entries with host_type='{host_type}' — "
                "run POST /api/v1/ssh-targets/rebuild first"
            ),
        }

    for row in rows:
        key_path = os.environ.get(row["key_env_var"], "").strip()
        if key_path and os.path.isfile(key_path):
            return {"configured": True, "ssh_key_present": True, "reason": ""}

    # Entries exist but key not resolvable on this node
    env_var  = rows[0]["key_env_var"]
    key_path = os.environ.get(env_var, "").strip()
    reason = (
        f"{env_var} is not set"
        if not key_path
        else f"key file not found: {key_path} (this node may not be the probe node)"
    )
    return {"configured": False, "ssh_key_present": False, "reason": reason}


def resolve_env_key(key_env: str) -> str:
    """Resolve a specific key env-var name to its absolute path.

    Use when you already know which env var to use (e.g. passing PROXMOX_SSH_KEY
    to a shell subprocess that expects it) but want consistent error handling.

    Raises SshKeyMissing if not set or the file does not exist.
    """
    path = os.environ.get(key_env, "").strip()
    if not path:
        raise SshKeyMissing(f"Env var {key_env!r} is not set")
    if not os.path.isfile(path):
        raise SshKeyMissing(f"Key file {path!r} not found (env var {key_env!r})")
    return path
