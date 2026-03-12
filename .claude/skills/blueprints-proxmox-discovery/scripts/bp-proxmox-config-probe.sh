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
import sys, os, json, re, subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

pve_ip   = sys.argv[1]
pve_name = sys.argv[2]
ts       = sys.argv[3]

# ── Get VM/LXC statuses from local PVE API ───────────────────────────────────
def _pvesh_statuses(vm_type_arg):
    try:
        r = subprocess.run(
            ['pvesh', 'get', f'/nodes/localhost/{vm_type_arg}', '--output-format=json'],
            capture_output=True, text=True, timeout=10
        )
        return {str(e['vmid']): e.get('status', '') for e in json.loads(r.stdout)}
    except Exception:
        return {}

lxc_statuses  = _pvesh_statuses('lxc')
qemu_statuses = _pvesh_statuses('qemu')

# ── Service detection inside a running LXC via pct exec ─────────────────────
_DETECT_SH = (
    "HAS_DOCKER=0; HAS_DOCKGE=0; DOCKGE_DIR=''; HAS_PORTAINER=0; PORTAINER_METHOD=''; HAS_CADDY=0; CADDY_PATH=''\n"
    "which docker >/dev/null 2>&1 && HAS_DOCKER=1 || true\n"
    "if [ \"$HAS_DOCKER\" = '1' ]; then\n"
    "  docker ps --format '{{.Names}}' 2>/dev/null | grep -qi portainer && HAS_PORTAINER=1 && PORTAINER_METHOD='docker' || true\n"
    "fi\n"
    "for _d in /opt/stacks /opt/dockge/data/stacks /home/dockge/stacks; do\n"
    "  [ -d \"$_d\" ] && HAS_DOCKGE=1 && DOCKGE_DIR=\"$_d\" && break\n"
    "done\n"
    "which caddy >/dev/null 2>&1 && HAS_CADDY=1 && CADDY_PATH=$(which caddy) || true\n"
    "printf '%s|%s|%s|%s|%s|%s|%s' \"$HAS_DOCKER\" \"$HAS_DOCKGE\" \"$DOCKGE_DIR\" "
    "\"$HAS_PORTAINER\" \"$PORTAINER_METHOD\" \"$HAS_CADDY\" \"$CADDY_PATH\"\n"
)

def _detect_lxc(vmid_str):
    """Run service detection inside a running LXC. Returns dict or {}."""
    try:
        r = subprocess.run(
            ['pct', 'exec', vmid_str, '--', 'bash', '-c', _DETECT_SH],
            capture_output=True, text=True, timeout=15
        )
        if r.returncode == 0 and '|' in r.stdout:
            parts = r.stdout.strip().split('|')
            if len(parts) >= 7:
                return {
                    'has_docker':        1 if parts[0] == '1' else 0,
                    'dockge_stacks_dir': parts[2] or None,
                    'has_portainer':     1 if parts[3] == '1' else 0,
                    'portainer_method':  parts[4] or None,
                    'has_caddy':         1 if parts[5] == '1' else 0,
                    'caddy_conf_path':   parts[6] or None,
                }
    except Exception:
        pass
    return {}

entries = []
running_lxc_vmids = []

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

        # Parse key: value lines (skip snapshot sections after [snapshot] headers)
        p = {}
        in_snapshot = False
        for line in raw_conf.splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if line.startswith('['):
                in_snapshot = True
                continue
            if in_snapshot:
                continue
            if ':' in line:
                k, _, v = line.partition(':')
                p[k.strip()] = v.strip()

        def g(key):
            return p.get(key) or ""

        name   = g("hostname") or g("name")
        status = (lxc_statuses if vm_type == "lxc" else qemu_statuses).get(vmid, "")

        # ── Parse ALL netN for IPs, MACs, and ALL VLAN tags ──────────────────
        net_keys = sorted([k for k in p if re.match(r'^net\d+$', k)])
        all_vlan_tags = []
        ip_address = gateway = mac_address = ip_config = ""
        for netkey in net_keys:
            net_str = p.get(netkey, "")
            for part in net_str.split(','):
                part = part.strip()
                if part.startswith("ip=") and not part.startswith("ip6=") and not ip_address:
                    ip_config  = part[3:]
                    ip_address = ip_config.split('/')[0]
                elif part.startswith("gw=") and not gateway:
                    gateway = part[3:]
                elif re.match(r'^(hwaddr|virtio|e1000|e1000e|rtl8139|vmxnet3|ne2k_pci)=', part) and not mac_address:
                    mac_address = part.split('=', 1)[1].split(',')[0].upper()
                elif part.startswith("tag="):
                    tag = part[4:].strip()
                    if tag and tag not in all_vlan_tags:
                        all_vlan_tags.append(tag)

        vlan_tag   = all_vlan_tags[0] if all_vlan_tags else ""
        vlans_json = json.dumps([int(t) for t in all_vlan_tags if t.isdigit()]) if all_vlan_tags else None

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

        entry = {
            "config_id":        f"{pve_name}_{vmid}",
            "pve_host":         pve_ip,
            "pve_name":         pve_name,
            "vmid":             vmid,
            "vm_type":          vm_type,
            "name":             name,
            "status":           status,
            "cores":            cores,
            "memory_mb":        memory_mb,
            "rootfs":           rootfs,
            "ip_config":        ip_config,
            "ip_address":       ip_address,
            "gateway":          gateway,
            "mac_address":      mac_address,
            "vlan_tag":         vlan_tag,
            "vlans_json":       vlans_json,
            "tags":             tags,
            "mountpoints_json": mountpoints_json,
            "raw_conf":         raw_conf,
            "has_docker":       0,
            "dockge_stacks_dir": None,
            "has_portainer":    0,
            "portainer_method": None,
            "has_caddy":        0,
            "caddy_conf_path":  None,
            "last_probed":      ts,
        }
        entries.append(entry)
        if vm_type == "lxc" and status == "running":
            running_lxc_vmids.append(vmid)

# ── Build per-interface nets records ─────────────────────────────────────────
nets = []
for e in entries:
    e_id   = e["config_id"]
    e_host = e["pve_host"]
    e_vmid = e["vmid"]
    raw    = e.get("raw_conf", "")
    # Re-parse net lines from raw_conf (already in p but we rebuild per-entry)
    net_p = {}
    in_snap = False
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith('#'): continue
        if line.startswith('['): in_snap = True; continue
        if in_snap: continue
        if ':' in line:
            k, _, v = line.partition(':')
            net_p[k.strip()] = v.strip()
    net_keys = sorted([k for k in net_p if re.match(r'^net\d+$', k)])
    for nk in net_keys:
        raw_str  = net_p[nk]
        n_mac = n_ip = n_cidr = n_gw = n_bridge = n_model = ""
        n_vlan = None
        for part in raw_str.split(','):
            part = part.strip()
            if part.startswith("ip=") and not part.startswith("ip6="):
                n_cidr = part[3:]
                n_ip   = n_cidr.split('/')[0]
            elif part.startswith("gw="):
                n_gw = part[3:]
            elif re.match(r'^(hwaddr|virtio|e1000|e1000e|rtl8139|vmxnet3|ne2k_pci)=', part):
                n_model = part.split('=',1)[0]
                n_mac   = part.split('=',1)[1].split(',')[0].upper()
            elif part.startswith("bridge="):
                n_bridge = part[7:]
            elif part.startswith("tag="):
                try: n_vlan = int(part[4:])
                except ValueError: pass
        nets.append({
            "net_id":      f"{e_id}_{nk}",
            "config_id":   e_id,
            "pve_host":    e_host,
            "vmid":        e_vmid,
            "net_key":     nk,
            "mac_address": n_mac or None,
            "ip_address":  n_ip or None,
            "ip_cidr":     n_cidr or None,
            "gateway":     n_gw or None,
            "vlan_tag":    n_vlan,
            "bridge":      n_bridge or None,
            "model":       n_model or None,
            "raw_str":     raw_str,
            "ip_source":   "conf",
        })

# ── Parallel service detection for running LXCs ───────────────────────────────
if running_lxc_vmids:
    sys.stderr.write(f"  [detect] checking {len(running_lxc_vmids)} running LXC(s) for Docker/Dockge/Portainer/Caddy\n")
    idx = {e["vmid"]: e for e in entries if e["vm_type"] == "lxc"}
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = {pool.submit(_detect_lxc, vmid): vmid for vmid in running_lxc_vmids}
        for fut in as_completed(futures):
            vmid   = futures[fut]
            result = fut.result()
            if result and vmid in idx:
                e = idx[vmid]
                e["has_docker"]        = result.get("has_docker", 0)
                e["dockge_stacks_dir"] = result.get("dockge_stacks_dir")
                e["has_portainer"]     = result.get("has_portainer", 0)
                e["portainer_method"]  = result.get("portainer_method")
                e["has_caddy"]         = result.get("has_caddy", 0)
                e["caddy_conf_path"]   = result.get("caddy_conf_path")

print(json.dumps({"entries": entries, "nets": nets}))
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

    COUNT=$(python3 -c "import json; d=json.load(open('${OUTFILE}')); print(len(d.get('entries',d) if isinstance(d,dict) else d))" 2>/dev/null || echo 0)
    echo "  -> ${COUNT} VM/LXC configs from ${PVE_NAME}" >&2
    (( TOTAL_HOSTS += 1 )) || true
    (( TOTAL_FILES += COUNT )) || true
done

echo "" >&2
echo "[merge] ${TOTAL_HOSTS} host(s) probed — combining..." >&2

COMBINED=$(python3 - "$WORK_DIR" << 'PYEOF'
import json, os, sys
tmpdir   = sys.argv[1]
entries  = []
nets     = []
for fname in sorted(os.listdir(tmpdir)):
    if not fname.endswith('.json'):
        continue
    fpath = os.path.join(tmpdir, fname)
    try:
        d = json.load(open(fpath))
        if isinstance(d, dict):
            entries.extend(d.get('entries', []))
            nets.extend(d.get('nets', []))
        else:
            entries.extend(d)  # legacy plain list
    except Exception:
        pass
print(json.dumps({'entries': entries, 'nets': nets}))
PYEOF
)

FINAL_JSON=$(python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(json.dumps(d['entries']))" <<< "$COMBINED")
FINAL_NETS=$(python3 -c "import json,sys; d=json.loads(sys.stdin.read()); print(json.dumps(d['nets']))"   <<< "$COMBINED")

TOTAL=$(python3 -c "import json,sys; print(len(json.loads(sys.stdin.read())))" <<< "$FINAL_JSON" 2>/dev/null || echo 0)
TOTAL_NETS=$(python3 -c "import json,sys; print(len(json.loads(sys.stdin.read())))" <<< "$FINAL_NETS" 2>/dev/null || echo 0)
echo "  Total: ${TOTAL} entries, ${TOTAL_NETS} net interfaces" >&2

echo "##ENTRIES## ${FINAL_JSON}"
echo "##NETS## ${FINAL_NETS}"
printf '##STATS## {"pve_hosts_probed":%d,"conf_files_read":%d,"net_interfaces":%d}\n' "$TOTAL_HOSTS" "$TOTAL_FILES" "$TOTAL_NETS"
