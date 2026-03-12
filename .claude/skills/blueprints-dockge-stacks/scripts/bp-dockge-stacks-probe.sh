#!/usr/bin/env bash
# bp-dockge-stacks-probe.sh — Probe Dockge instances via pct exec and collect
# compose stacks for the dockge_stacks table.
#
# stdout: ##ENTRIES## [...] and ##STATS## {...}
# stderr: progress messages
#
# Requires: PROXMOX_SSH_KEY env var (ed25519 key for root@pve)
# LXC inventory is read from Blueprints proxmox_config table (run bp-proxmox-config-probe first)

set -uo pipefail

TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")

# ── Discover LXC instances from Blueprints API ────────────────────────────────
# Reads all LXC VMIDs from proxmox_config table (populated by bp-proxmox-config-probe).
# Each entry is PVE_HOST:VMID:PVE_NAME — the remote python checks for Dockge stacks
# in each LXC and returns [] when none are found, so this is safe to run exhaustively.
API_BASE="${BLUEPRINTS_API:-http://localhost:8080}"
echo "[instances] Fetching LXC inventory from Blueprints API ..." >&2
PVE_CONFIG_RAW=$(curl -sf "${API_BASE}/api/v1/proxmox-config?vm_type=lxc" 2>/dev/null || echo "[]")
if [[ "$PVE_CONFIG_RAW" == "[]" ]] || [[ -z "$PVE_CONFIG_RAW" ]]; then
    echo "ERROR: No LXC records in proxmox_config — run bp-proxmox-config-probe first" >&2
    exit 1
fi
# PVE_HOST:VMID:PVE_NAME format
mapfile -t DOCKGE_INSTANCES < <(python3 -c "
import json, sys
seen = set()
for r in json.loads(sys.stdin.read()):
    key = (r['pve_host'], r['vmid'])
    if key in seen:
        continue
    seen.add(key)
    name = r.get('pve_name') or r['pve_host']
    print(f\"{r['pve_host']}:{r['vmid']}:{name}\")
" <<< "$PVE_CONFIG_RAW")
echo "[instances] ${#DOCKGE_INSTANCES[@]} LXC(s) to check for Dockge" >&2

KEY="${PROXMOX_SSH_KEY:-}"
if [[ -z "$KEY" || ! -f "$KEY" ]]; then
    echo "ERROR: PROXMOX_SSH_KEY not set or key file missing: ${KEY:-<unset>}" >&2
    exit 1
fi

SSH_OPTS=(-i "$KEY" -o ConnectTimeout=10 -o BatchMode=yes -o StrictHostKeyChecking=accept-new)

echo "=== Dockge Stacks Probe ===" >&2
echo "Timestamp: ${TIMESTAMP}" >&2

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

# Python script to run on the PVE host via SSH (uses pct exec to reach the LXC)
# Args: pve_host vmid pve_name timestamp
REMOTE_PY="$WORK_DIR/remote_probe.py"
cat > "$REMOTE_PY" << 'PYEOF'
import sys, os, json, re, subprocess

pve_host = sys.argv[1]
vmid     = sys.argv[2]
pve_name = sys.argv[3]
ts       = sys.argv[4]

def pct_exec(cmd_list, vmid=vmid):
    """Run a command inside the LXC via pct exec. Returns stdout string or None."""
    try:
        result = subprocess.run(
            ["pct", "exec", vmid, "--"] + cmd_list,
            capture_output=True, text=True, timeout=15
        )
        if result.returncode == 0:
            return result.stdout
        return None
    except Exception:
        return None

# Try common stacks directories
STACKS_CANDIDATES = ["/opt/stacks", "/opt/dockge/data/stacks"]
stacks_dir = None
for candidate in STACKS_CANDIDATES:
    out = pct_exec(["ls", candidate])
    if out is not None:
        stacks_dir = candidate
        break

if stacks_dir is None:
    # Output empty list — no stacks found
    print(json.dumps([]))
    sys.exit(0)

stacks_listing = pct_exec(["ls", "-1", stacks_dir]) or ""
stack_names = [s.strip() for s in stacks_listing.splitlines() if s.strip()]

entries = []
for stack_name in stack_names:
    stack_path = f"{stacks_dir}/{stack_name}"

    # Try compose.yaml or compose.yml or docker-compose.yaml
    compose_content = None
    for fname in ["compose.yaml", "compose.yml", "docker-compose.yaml", "docker-compose.yml"]:
        content = pct_exec(["cat", f"{stack_path}/{fname}"])
        if content is not None:
            compose_content = content
            break

    if compose_content is None:
        continue

    # Check for .env file
    env_check = pct_exec(["sh", "-c", f"test -f {stack_path}/.env && echo yes || echo no"])
    env_file_exists = (env_check or "").strip() == "yes"

    # Get docker compose status
    status = ""
    ps_json = pct_exec(["docker", "compose", "-f", f"{stack_path}/compose.yaml", "ps", "--format", "json"])
    if ps_json:
        try:
            ps_data = [json.loads(line) for line in ps_json.strip().splitlines() if line.strip()]
            statuses = list(set(c.get("State", "") or c.get("Status", "") for c in ps_data))
            status = statuses[0] if len(statuses) == 1 else ("running" if "running" in statuses else ", ".join(statuses))
        except Exception:
            status = "unknown"

    # Parse compose for services, ports, volumes (regex based — no PyYAML needed)
    services = re.findall(r'^  ([a-zA-Z0-9_-]+):\s*$', compose_content, re.MULTILINE)
    ports    = re.findall(r'["\']?(\d+:\d+)["\']?', compose_content)
    volumes  = re.findall(r'["\']?([./~][^:\s"\']*:[^:\s"\']+)["\']?', compose_content)

    entries.append({
        "stack_id":        f"{vmid}_{stack_name}",
        "pve_host":        pve_host,
        "source_vmid":     vmid,
        "source_lxc_name": "",
        "stack_name":      stack_name,
        "status":          status,
        "compose_content": compose_content,
        "services_json":   json.dumps(services) if services else None,
        "ports_json":      json.dumps(list(set(ports))) if ports else None,
        "volumes_json":    json.dumps(list(set(volumes))) if volumes else None,
        "env_file_exists": env_file_exists,
        "stacks_dir":      stacks_dir,
        "last_probed":     ts,
    })

print(json.dumps(entries))
PYEOF

TOTAL_INSTANCES=0
TOTAL_STACKS=0

for inst in "${DOCKGE_INSTANCES[@]}"; do
    PVE_IP="${inst%%:*}"
    rest="${inst#*:}"
    VMID="${rest%%:*}"
    PVE_NAME="${rest##*:}"

    echo "[probe] pct exec ${VMID} on ${PVE_IP} (${PVE_NAME})" >&2
    OUTFILE="${WORK_DIR}/${PVE_NAME}_${VMID}.json"

    ssh "${SSH_OPTS[@]}" "root@${PVE_IP}" \
        python3 /dev/stdin "$PVE_IP" "$VMID" "$PVE_NAME" "$TIMESTAMP" \
        < "$REMOTE_PY" > "$OUTFILE" \
        || { echo "  WARNING: failed for ${PVE_IP} vmid=${VMID} — skipping" >&2; rm -f "$OUTFILE"; continue; }

    COUNT=$(python3 -c "import json; print(len(json.load(open('${OUTFILE}'))))" 2>/dev/null || echo 0)
    echo "  -> ${COUNT} stacks from vmid=${VMID}" >&2
    (( TOTAL_INSTANCES += 1 )) || true
    (( TOTAL_STACKS += COUNT )) || true
done

echo "" >&2
echo "[merge] ${TOTAL_INSTANCES} instance(s) probed — combining..." >&2

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
echo "  Total: ${TOTAL} stacks" >&2

echo "##ENTRIES## ${FINAL_JSON}"
printf '##STATS## {"dockge_instances_probed":%d,"stacks_found":%d}\n' "$TOTAL_INSTANCES" "$TOTAL_STACKS"
