#!/usr/bin/env bash
# bp-proxmox-config-probe.sh — Read /etc/pve/{lxc,qemu-server}/*.conf from
# all Proxmox hosts, parse them, and output records for proxmox_config table.
#
# stdout: ##ENTRIES## [...] and ##STATS## {...}
# stderr: progress messages
#
# Requires: PROXMOX_SSH_KEY env var pointing to ed25519 private key for root@pve

set -euo pipefail

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

echo "=== Proxmox Config Probe ===" >&2
echo "Timestamp: ${TIMESTAMP}" >&2

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

# Write the remote Python probe script to a local temp file.
# Args: pve_ip pve_name timestamp
# Receives via stdin when called as: ssh host python3 /dev/stdin ARGS < script
REMOTE_PY="$WORK_DIR/remote_probe.py"
cat > "$REMOTE_PY" << 'PYEOF'
import sys, os, json, re

pve_ip   = sys.argv[1]
pve_name = sys.argv[2]
ts       = sys.argv[3]

entries = []
for vm_type, conf_dir in [("lxc", "/etc/pve/lxc"), ("qemu", "/etc/pve/qemu-server")]:
    if not os.path.isdir(conf_dir):
        continue
    for fname in sorted(os.listdir(conf_dir)):
        if not fname.endswith('.conf'):
            continue
        vmid = fname.replace('.conf', '')
        fpath = os.path.join(conf_dir, fname)
        try:
            raw_conf = open(fpath).read()
        except Exception:
            continue

        # Parse key: value lines
        p = {}
        for line in raw_conf.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if ':' in line:
                k, _, v = line.partition(':')
                p[k.strip()] = v.strip()

        def g(key):
            return p.get(key) or ""

        name = g("hostname") or g("name")

        # net0: hwaddr=MAC,ip=x.x.x.x/24,gw=x.x.x.x,tag=N,bridge=vmbrN,...
        net0_str    = g("net0")
        ip_config   = net0_str
        ip_address  = ""
        gateway     = ""
        mac_address = ""
        vlan_tag    = ""
        if net0_str:
            for part in net0_str.split(','):
                part = part.strip()
                if part.startswith("ip=") and not part.startswith("ip6="):
                    ip_config  = part[3:]
                    ip_address = ip_config.split('/')[0]
                elif part.startswith("gw="):
                    gateway = part[3:]
                elif re.match(r'^(hwaddr|virtio|e1000|rtl8139|vmxnet3)=', part):
                    mac_address = part.split('=', 1)[1].split(',')[0].upper()
                elif part.startswith("tag="):
                    vlan_tag = part[4:]

        try:
            cores = int(g("cores") or g("sockets") or 0) or None
        except ValueError:
            cores = None
        try:
            memory_mb = int(g("memory") or 0) or None
        except ValueError:
            memory_mb = None

        rootfs = g("rootfs") or g("scsi0") or g("virtio0") or g("ide0") or ""
        tags   = g("tags") or None

        mountpoints = {k: v for k, v in p.items() if re.match(r'^mp\d+$', k)}
        mountpoints_json = json.dumps(mountpoints) if mountpoints else None

        entries.append({
            "config_id":        f"{pve_name}_{vmid}",
            "pve_host":         pve_ip,
            "pve_name":         pve_name,
            "vmid":             vmid,
            "vm_type":          vm_type,
            "name":             name,
            "status":           "",
            "cores":            cores,
            "memory_mb":        memory_mb,
            "rootfs":           rootfs,
            "ip_config":        ip_config,
            "ip_address":       ip_address,
            "gateway":          gateway,
            "mac_address":      mac_address,
            "vlan_tag":         vlan_tag,
            "tags":             tags,
            "mountpoints_json": mountpoints_json,
            "raw_conf":         raw_conf,
            "last_probed":      ts,
        })

print(json.dumps(entries))
PYEOF

TOTAL_HOSTS=0
TOTAL_FILES=0

for host_entry in "${PVE_HOSTS[@]}"; do
    PVE_IP="${host_entry%%:*}"
    PVE_NAME="${host_entry##*:}"

    echo "[probe] root@${PVE_IP} (${PVE_NAME})" >&2
    OUTFILE="${WORK_DIR}/${PVE_NAME}.json"

    # python3 /dev/stdin reads the script from stdin; argv goes after --
    ssh "${SSH_OPTS[@]}" "root@${PVE_IP}" \
        python3 /dev/stdin "$PVE_IP" "$PVE_NAME" "$TIMESTAMP" \
        < "$REMOTE_PY" > "$OUTFILE" \
        || { echo "  WARNING: SSH/python3 failed for ${PVE_IP} — skipping" >&2; rm -f "$OUTFILE"; continue; }

    # ── Patch pve_hosts record now that we know SSH works ────────────────
    # Fetch pveversion and hostname in one ssh call; gracefully skip on error
    PVE_META=$(ssh "${SSH_OPTS[@]}" "root@${PVE_IP}" \
        'printf "%s\t%s\n" "$(pveversion --verbose 2>/dev/null | head -1 || pveversion 2>/dev/null | head -1)" "$(hostname -s 2>/dev/null)"' \
        2>/dev/null || echo "")
    if [[ -n "$PVE_META" ]]; then
        PVE_VER=$(echo "$PVE_META" | cut -f1 | sed 's/^proxmox-ve: //' | xargs)
        PVE_HOSTNAME=$(echo "$PVE_META" | cut -f2 | xargs)
        PATCH_BODY=$(python3 -c "import json; print(json.dumps({'ssh_reachable':1,'version':'''${PVE_VER}''','hostname':'''${PVE_HOSTNAME}'''}))")
        curl -sf -X PUT "${API_BASE}/api/v1/pve-hosts/${PVE_IP}" \
            -H "Content-Type: application/json" \
            -d "$PATCH_BODY" > /dev/null \
            && echo "  [pve-hosts] patched ${PVE_IP}: version=${PVE_VER} hostname=${PVE_HOSTNAME} ssh=1" >&2 \
            || echo "  [pve-hosts] warn: could not PATCH ${PVE_IP} — API unreachable?" >&2
    fi

    COUNT=$(python3 -c "import json; print(len(json.load(open('${OUTFILE}'))))" 2>/dev/null || echo 0)
    echo "  -> ${COUNT} VM/LXC configs from ${PVE_NAME}" >&2
    (( TOTAL_HOSTS += 1 )) || true
    (( TOTAL_FILES += COUNT )) || true
done

echo "" >&2
echo "[merge] ${TOTAL_HOSTS} host(s) probed — combining..." >&2

FINAL_JSON=$(python3 - "$WORK_DIR" << 'PYEOF'
import json, os, sys
tmpdir   = sys.argv[1]
combined = []
for fname in sorted(os.listdir(tmpdir)):
    if not fname.endswith('.json'):
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
echo "  Total: ${TOTAL} entries" >&2

echo "##ENTRIES## ${FINAL_JSON}"
printf '##STATS## {"pve_hosts_probed":%d,"conf_files_read":%d}\n' "$TOTAL_HOSTS" "$TOTAL_FILES"
