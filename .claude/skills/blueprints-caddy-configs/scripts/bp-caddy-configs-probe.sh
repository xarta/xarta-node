#!/usr/bin/env bash
# bp-caddy-configs-probe.sh — Discover Caddy configs across all LXCs on all
# Proxmox hosts, producing records for the caddy_configs table.
#
# Dynamically enumerates all running LXCs on each PVE host, then uses
# pct exec to check for /etc/caddy/Caddyfile (and fragments).
#
# stdout: ##ENTRIES## [...] and ##STATS## {...}
# stderr: progress messages
#
# Requires: PROXMOX_SSH_KEY env var (ed25519 key for root@pve)

set -uo pipefail

TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

# ── Discover PVE hosts from Blueprints API ────────────────────────────────────
# Run POST /api/v1/pve-hosts/scan first if this list is empty.
API_BASE="${BLUEPRINTS_API:-http://localhost:8080}"
echo "[hosts] Fetching PVE hosts from Blueprints API ..." >&2
PVE_HOSTS_RAW=$(curl -sf "${API_BASE}/api/v1/pve-hosts" 2>/dev/null || echo "[]")
if [[ "$PVE_HOSTS_RAW" == "[]" ]] || [[ -z "$PVE_HOSTS_RAW" ]]; then
    echo "ERROR: No PVE hosts in Blueprints — run the PVE Hosts scan first" >&2
    exit 1
fi
mapfile -t PVE_HOSTS < <(python3 -c "
import json, sys
for h in json.loads(sys.stdin.read()):
    ip   = h['ip_address']
    name = h.get('pve_name') or h['pve_id']
    print(f'{ip}:{name}')
" <<< "$PVE_HOSTS_RAW")
echo "[hosts] ${#PVE_HOSTS[@]} PVE host(s) from API" >&2

KEY="${PROXMOX_SSH_KEY:-}"
if [[ -z "$KEY" || ! -f "$KEY" ]]; then
    echo "ERROR: PROXMOX_SSH_KEY not set or key file missing: ${KEY:-<unset>}" >&2
    exit 1
fi

SSH_OPTS=(-i "$KEY" -o ConnectTimeout=10 -o BatchMode=yes -o StrictHostKeyChecking=accept-new)

echo "=== Caddy Configs Probe ===" >&2
echo "Timestamp: ${TIMESTAMP}" >&2

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

# Python script that runs on the PVE host, iterates running LXCs, finds Caddy
# Args: pve_host pve_name timestamp
REMOTE_PY="$WORK_DIR/remote_probe.py"
cat > "$REMOTE_PY" << 'PYEOF'
import sys, os, json, re, subprocess

pve_host = sys.argv[1]
pve_name = sys.argv[2]
ts       = sys.argv[3]

def pct_exec(vmid, cmd_list, timeout=15):
    try:
        r = subprocess.run(
            ["pct", "exec", str(vmid), "--"] + cmd_list,
            capture_output=True, text=True, timeout=timeout
        )
        return r.stdout if r.returncode == 0 else None
    except Exception:
        return None

def lxc_name(vmid):
    try:
        r = subprocess.run(["pct", "config", str(vmid)],
                           capture_output=True, text=True, timeout=8)
        for line in r.stdout.splitlines():
            if line.startswith("hostname:"):
                return line.split(":", 1)[1].strip()
    except Exception:
        pass
    return ""

# List running LXCs
try:
    pct_out = subprocess.run(["pct", "list"], capture_output=True, text=True, timeout=10).stdout
except Exception:
    print(json.dumps([]))
    sys.exit(0)

running_vmids = []
for line in pct_out.splitlines()[1:]:  # skip header
    parts = line.split()
    if len(parts) >= 2 and parts[1] == "running":
        running_vmids.append(parts[0])

entries = []
for vmid in running_vmids:
    # Check for Caddyfile
    caddy_content = None
    caddyfile_path = None
    for candidate in ["/etc/caddy/Caddyfile", "/etc/caddy/caddy.conf"]:
        content = pct_exec(vmid, ["cat", candidate])
        if content is not None:
            caddy_content = content
            caddyfile_path = candidate
            break

    if caddy_content is None:
        continue  # no Caddy on this LXC

    # Also collect fragment files from /etc/caddy/*.caddy or /etc/caddy/conf.d/
    fragments = {}
    for frag_dir in ["/etc/caddy", "/etc/caddy/conf.d"]:
        listing = pct_exec(vmid, ["ls", "-1", frag_dir])
        if listing:
            for fname in listing.strip().splitlines():
                fname = fname.strip()
                if fname in ("Caddyfile", "caddy.conf") or not fname:
                    continue
                if fname.endswith((".caddy", ".conf")):
                    frag = pct_exec(vmid, ["cat", f"{frag_dir}/{fname}"])
                    if frag:
                        fragments[f"{frag_dir}/{fname}"] = frag

    # Combine main + fragments for domain/upstream extraction
    all_content = caddy_content + "\n" + "\n".join(fragments.values())

    # Extract domains: lines like "hostname.example.com {" or "hostname.example.com, www.host.com {"
    domain_matches = re.findall(
        r'^\s*((?:[a-zA-Z0-9][-a-zA-Z0-9]*\.)+[a-zA-Z]{2,}(?::\d+)?'
        r'(?:\s*,\s*(?:[a-zA-Z0-9][-a-zA-Z0-9]*\.)+[a-zA-Z]{2,}(?::\d+)?)*)\s*\{',
        all_content, re.MULTILINE
    )
    domains = []
    for m in domain_matches:
        for d in m.split(','):
            d = d.strip()
            if d:
                domains.append(d)
    domains = list(dict.fromkeys(domains))  # deduplicate while preserving order

    # Extract upstreams after reverse_proxy directive
    upstreams = re.findall(
        r'reverse_proxy\s+([^\s{#\n]+)',
        all_content
    )
    upstreams = list(dict.fromkeys(upstreams))

    lxc_hostname = lxc_name(vmid)

    entries.append({
        "caddy_id":          f"{pve_name}_{vmid}",
        "pve_host":          pve_host,
        "source_vmid":       vmid,
        "source_lxc_name":   lxc_hostname,
        "caddyfile_path":    caddyfile_path,
        "caddyfile_content": caddy_content,
        "domains_json":      json.dumps(domains) if domains else None,
        "upstreams_json":    json.dumps(upstreams) if upstreams else None,
        "last_probed":       ts,
    })

print(json.dumps(entries))
PYEOF

TOTAL_HOSTS=0
TOTAL_CADDY=0

for host_entry in "${PVE_HOSTS[@]}"; do
    PVE_IP="${host_entry%%:*}"
    PVE_NAME="${host_entry##*:}"

    echo "[probe] root@${PVE_IP} (${PVE_NAME}) — scanning running LXCs for Caddy" >&2
    OUTFILE="${WORK_DIR}/${PVE_NAME}.json"

    ssh "${SSH_OPTS[@]}" "root@${PVE_IP}" \
        python3 /dev/stdin "$PVE_IP" "$PVE_NAME" "$TIMESTAMP" \
        < "$REMOTE_PY" > "$OUTFILE" \
        || { echo "  WARNING: SSH/python3 failed for ${PVE_IP} — skipping" >&2; rm -f "$OUTFILE"; continue; }

    COUNT=$(python3 -c "import json; print(len(json.load(open('${OUTFILE}'))))" 2>/dev/null || echo 0)
    echo "  -> ${COUNT} Caddy LXC(s) on ${PVE_NAME}" >&2
    (( TOTAL_HOSTS += 1 )) || true
    (( TOTAL_CADDY += COUNT )) || true
done

echo "" >&2
echo "[merge] ${TOTAL_HOSTS} host(s) probed — combining..." >&2

FINAL_JSON=$(python3 - "$WORK_DIR" << 'PYEOF'
import json, os, sys
tmpdir   = sys.argv[1]
combined = []
for fname in sorted(os.listdir(tmpdir)):
    if not fname.endswith('.json') or 'remote_probe' in fname:
        continue
    fpath = os.path.join(tmpdir, fname)
    try:
        combined.extend(json.load(open(fpath)))
    except Exception:
        pass
print(json.dumps(combined))
PYEOF
)

TOTAL=$(python3 -c "import json,sys; print(len(json.loads(sys.stdin.read())))" <<< "$FINAL_JSON" 2>/dev/null || echo 0)
echo "  Total: ${TOTAL} Caddy configs" >&2

echo "##ENTRIES## ${FINAL_JSON}"
printf '##STATS## {"pve_hosts_probed":%d,"lxcs_with_caddy":%d}\n' "$TOTAL_HOSTS" "$TOTAL_CADDY"
