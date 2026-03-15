"""routes_firewall.py — GET /api/v1/firewall/status  +  POST /api/v1/firewall/probe

Two read-only endpoints — no DB writes, no sync queue touched.

GET /api/v1/firewall/status
    Reports the local iptables state: INPUT default policy, whether the
    XARTA_INPUT chain exists, and which of the expected ports are represented
    by rules in that chain.

POST /api/v1/firewall/probe
    Accepts {"target": "<scheme://host:port>", "target_node_id": "<id>"}
    and probes that address for each well-known port, returning open/blocked
    per port.

    Target is validated against the nodes table — arbitrary IPs are rejected
    to prevent SSRF abuse.
"""
from __future__ import annotations

import json
import re
import socket
import subprocess
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from . import config as cfg
from .db import get_conn
from .models import FirewallPortCheck, FirewallProbePort, FirewallProbeOut, FirewallStatusOut

router = APIRouter(prefix="/firewall", tags=["firewall"])

# ── Port catalogue ─────────────────────────────────────────────────────────────
# expected: "open"    → firewall should allow; probe pass = we can connect
# expected: "blocked" → firewall should drop;  probe pass = connection refused/timeout

_PORT_CATALOGUE: list[dict] = [
    # Expected OPEN
    {"port": 22,    "proto": "tcp", "label": "SSH",                    "expected": "open"},
    {"port": 80,    "proto": "tcp", "label": "HTTP (Caddy redirect)",  "expected": "open"},
    {"port": 443,   "proto": "tcp", "label": "HTTPS (Caddy/API)",      "expected": "open"},
    {"port": 41641, "proto": "udp", "label": "Tailscale/WireGuard",    "expected": "open"},
    # Expected BLOCKED (should not be reachable from outside)
    {"port": 8080,  "proto": "tcp", "label": "uvicorn (direct)",       "expected": "blocked"},
    {"port": 3000,  "proto": "tcp", "label": "Common dev port",        "expected": "blocked"},
    {"port": 5000,  "proto": "tcp", "label": "Common dev port",        "expected": "blocked"},
    {"port": 8000,  "proto": "tcp", "label": "Common dev port",        "expected": "blocked"},
    {"port": 9000,  "proto": "tcp", "label": "Common admin port",      "expected": "blocked"},
]

# Ports that XARTA_INPUT explicitly allows (used by status endpoint).
_XARTA_ALLOWED_PORTS = {22, 80, 443, 41641}

_TCP_TIMEOUT = 3   # seconds for TCP connect probes
_UDP_TIMEOUT = 3   # seconds for nmap UDP scan if available


# ── Helpers ───────────────────────────────────────────────────────────────────

def _iptables_available() -> bool:
    try:
        subprocess.run(
            ["iptables", "--version"],
            capture_output=True, timeout=5, check=True,
        )
        return True
    except Exception:
        return False


def _get_input_policy() -> str:
    try:
        result = subprocess.run(
            ["iptables", "-L", "INPUT", "--line-numbers", "-n"],
            capture_output=True, text=True, timeout=5,
        )
        for line in result.stdout.splitlines():
            m = re.match(r"Chain INPUT \(policy (\w+)\)", line)
            if m:
                return m.group(1)
    except Exception:
        pass
    return "unknown"


def _xarta_chain_exists() -> bool:
    try:
        result = subprocess.run(
            ["iptables", "-L", "XARTA_INPUT", "-n"],
            capture_output=True, text=True, timeout=5,
        )
        return result.returncode == 0
    except Exception:
        return False


def _ports_in_xarta_chain() -> set[int]:
    """Return set of destination ports explicitly mentioned in XARTA_INPUT."""
    found: set[int] = set()
    try:
        result = subprocess.run(
            ["iptables", "-L", "XARTA_INPUT", "-n"],
            capture_output=True, text=True, timeout=5,
        )
        for line in result.stdout.splitlines():
            m = re.search(r"dpt:(\d+)", line)
            if m:
                found.add(int(m.group(1)))
    except Exception:
        pass
    return found


def _probe_tcp(host: str, port: int, timeout: int = _TCP_TIMEOUT) -> str:
    """Returns 'open', 'blocked', or 'timeout'."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return "open"
    except ConnectionRefusedError:
        return "blocked"
    except (socket.timeout, TimeoutError):
        return "timeout"
    except OSError:
        # e.g. "Network unreachable" — treat as blocked
        return "blocked"


def _probe_udp_nmap(host: str, port: int) -> str:
    """
    Best-effort UDP probe via nmap -sU.  Requires nmap to be installed and
    the process to have CAP_NET_RAW (root or nmap with setuid).
    Returns 'open', 'blocked', 'timeout', or 'skipped'.
    """
    try:
        result = subprocess.run(
            ["nmap", "-sU", "-p", str(port), "--open", "-oG", "-", host],
            capture_output=True, text=True, timeout=15,
        )
        output = result.stdout + result.stderr
        if "open" in output.lower():
            return "open"
        if "filtered" in output.lower() or "closed" in output.lower():
            return "blocked"
        return "timeout"
    except FileNotFoundError:
        return "skipped"
    except Exception:
        return "error"


def _extract_host(address: str) -> str:
    """Extract bare hostname/IP from a URL like http://10.0.0.1:8080."""
    parsed = urlparse(address)
    return parsed.hostname or address


def _known_node_addresses(target_node_id: str) -> list[str]:
    """Return all stored addresses for a fleet node."""
    with get_conn() as conn:
        row = conn.execute(
            "SELECT addresses FROM nodes WHERE node_id = ?",
            (target_node_id,),
        ).fetchone()
    if not row or not row["addresses"]:
        return []
    try:
        return json.loads(row["addresses"])
    except Exception:
        return []


def _all_known_hosts() -> set[str]:
    """Return all hosts from all fleet node addresses (for SSRF check)."""
    with get_conn() as conn:
        rows = conn.execute("SELECT addresses FROM nodes").fetchall()
    hosts: set[str] = set()
    for row in rows:
        if not row["addresses"]:
            continue
        try:
            addrs = json.loads(row["addresses"])
        except Exception:
            continue
        for addr in addrs:
            h = _extract_host(addr)
            if h:
                hosts.add(h)
    return hosts


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/status", response_model=FirewallStatusOut)
async def firewall_status() -> FirewallStatusOut:
    """
    Return local iptables state: INPUT policy, XARTA_INPUT chain presence,
    and which expected ports are represented in the chain.
    """
    avail = _iptables_available()
    policy = _get_input_policy() if avail else "unknown"
    chain_exists = _xarta_chain_exists() if avail else False
    in_chain = _ports_in_xarta_chain() if chain_exists else set()

    ports = [
        FirewallPortCheck(
            port=p["port"],
            proto=p["proto"],
            label=p["label"],
            expected=p["expected"],
            in_ruleset=(p["port"] in in_chain),
        )
        for p in _PORT_CATALOGUE
        if p["port"] in _XARTA_ALLOWED_PORTS
    ]

    return FirewallStatusOut(
        iptables_available=avail,
        input_policy=policy,
        xarta_input_chain=chain_exists,
        ports=ports,
    )


class ProbeRequest(BaseModel):
    target: str          # scheme://host:port base URL of the target node
    target_node_id: str  # must match a known node in the nodes table


@router.post("/probe", response_model=FirewallProbeOut)
async def firewall_probe(req: ProbeRequest) -> FirewallProbeOut:
    """
    Probe a target fleet node for open/blocked ports and return per-port results.

    The target is validated against the nodes table — requests targeting
    addresses not associated with any known fleet node are rejected (SSRF guard).
    """
    # ── SSRF guard: target host must belong to a known fleet node ─────────────
    target_host = _extract_host(req.target)
    if not target_host:
        raise HTTPException(status_code=400, detail="Could not parse target host")

    known_hosts = _all_known_hosts()
    if target_host not in known_hosts:
        raise HTTPException(
            status_code=403,
            detail=f"Target host '{target_host}' is not a known fleet node address. "
                   "Probing arbitrary hosts is not permitted.",
        )

    results: list[FirewallProbePort] = []
    for p in _PORT_CATALOGUE:
        port = p["port"]
        proto = p["proto"]
        expected = p["expected"]

        if proto == "tcp":
            result = _probe_tcp(target_host, port)
        elif proto == "udp":
            result = _probe_udp_nmap(target_host, port)
        else:
            result = "skipped"

        # Pass logic:
        #   expected "open"    → pass if result is "open"
        #   expected "blocked" → pass if result is "blocked" or "timeout"
        #                        (a DROP firewall gives timeout; REJECT gives blocked)
        if expected == "open":
            passed = result == "open"
        else:
            passed = result in ("blocked", "timeout")

        results.append(
            FirewallProbePort(
                port=port,
                proto=proto,
                label=p["label"],
                expected=expected,
                result=result,
                **{"pass": passed},
            )
        )

    all_pass = all(
        r.pass_ for r in results
        if r.result != "skipped"
    )

    return FirewallProbeOut(
        prober_node=cfg.NODE_NAME or cfg.NODE_ID,
        target=req.target,
        ports=results,
        all_pass=all_pass,
    )
