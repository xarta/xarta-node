#!/usr/bin/env bash
# bp-dockge-stacks-probe.sh — v2
#
# Probes Dockge stacks across the fleet using:
#   - proxmox_config.dockge_json  to know which machines have Dockge + stacks paths
#   - proxmox_nets + ssh_targets  to find the right IP and SSH key per machine
#   - Direct SSH into each machine (not pct exec via PVE, except as LXC fallback)
#
# stdout: ##ENTRIES## [...stacks] ##SERVICES## [...services] ##STATS## {...}
# stderr: progress messages
#
# Required env vars (pass whichever keys you have):
#   PROXMOX_SSH_KEY, VM_SSH_KEY, LXC_SSH_KEY, CITADEL_SSH_KEY, XARTA_NODE_SSH_KEY
set -uo pipefail

TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%S")
DB="${BLUEPRINTS_DB:-/opt/blueprints/data/db/blueprints.db}"

if [[ ! -f "$DB" ]]; then
    echo "ERROR: Blueprints DB not found: $DB" >&2
    exit 1
fi

# ── Build machine inventory from DB ──────────────────────────────────────────
echo "[inventory] Building Dockge machine list from DB..." >&2

MACHINES_JSON=$(python3 - "$DB" << 'INNERPY'
import json, sqlite3, sys

conn = sqlite3.connect(sys.argv[1])
conn.row_factory = sqlite3.Row

rows = conn.execute("""
    SELECT
        pc.config_id, pc.pve_host, pc.vmid, pc.name, pc.vm_type,
        pc.dockge_json,
        pn.ip_address, pn.vlan_tag,
        COALESCE(st.key_env_var, '') AS key_env_var,
        COALESCE(st.source_ip,   '') AS source_ip
    FROM proxmox_config pc
    JOIN proxmox_nets pn ON pn.config_id = pc.config_id AND pn.ip_address IS NOT NULL
    LEFT JOIN ssh_targets st ON st.ip_address = pn.ip_address
    WHERE pc.dockge_json IS NOT NULL
      AND pc.dockge_json NOT IN ('[]', 'null', '')
    ORDER BY
        pc.config_id,
        CASE WHEN st.key_env_var IS NOT NULL AND st.key_env_var != '' THEN 0 ELSE 1 END,
        CASE pn.vlan_tag WHEN 42 THEN 0 WHEN 33 THEN 1 ELSE 2 END
""").fetchall()

best = {}
for r in rows:
    cid = r['config_id']
    try:
        dj = json.loads(r['dockge_json'] or '[]')
    except Exception:
        dj = []
    if not dj:
        continue
    if cid not in best:
        best[cid] = dict(r)
        best[cid]['dockge_instances'] = dj

print(json.dumps(list(best.values())))
INNERPY
)

MACHINE_COUNT=$(python3 -c "import json,sys; print(len(json.loads(sys.stdin.read())))" <<< "$MACHINES_JSON" 2>/dev/null || echo 0)
echo "[inventory] ${MACHINE_COUNT} machine(s) with Dockge to probe" >&2

if [[ "$MACHINE_COUNT" -eq 0 ]]; then
    echo "ERROR: No machines with dockge_json found — run Proxmox Config probe first" >&2
    exit 1
fi

# ── Helpers ───────────────────────────────────────────────────────────────────

_resolve_key() {
    # Return path if env var is set and file exists, else empty string
    local varname="$1"
    local path="${!varname:-}"
    [[ -n "$path" && -f "$path" ]] && echo "$path" || echo ""
}

_direct_ssh() {
    # _direct_ssh <key_path> <source_ip> <target_ip> [cmd...]
    local key="$1"; local src="$2"; local tgt="$3"; shift 3
    local bind_args=()
    [[ -n "$src" ]] && bind_args=(-b "$src")
    ssh -i "$key" "${bind_args[@]+"${bind_args[@]}"}" \
        -o ConnectTimeout=10 -o BatchMode=yes \
        -o StrictHostKeyChecking=accept-new \
        "root@${tgt}" "$@"
}

_pve_key_for() {
    # Get key path for a PVE host (falls back to PROXMOX_SSH_KEY)
    local pve_ip="$1"
    local kv
    kv=$(sqlite3 "$DB" "SELECT COALESCE(key_env_var,'PROXMOX_SSH_KEY') FROM ssh_targets WHERE ip_address='${pve_ip}' LIMIT 1;" 2>/dev/null) || kv="PROXMOX_SSH_KEY"
    [[ -z "$kv" ]] && kv="PROXMOX_SSH_KEY"
    _resolve_key "$kv"
}

_pve_src_for() {
    local pve_ip="$1"
    sqlite3 "$DB" "SELECT COALESCE(source_ip,'') FROM ssh_targets WHERE ip_address='${pve_ip}' LIMIT 1;" 2>/dev/null || echo ""
}

# ── Work directory ────────────────────────────────────────────────────────────

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

# ── Remote probe Python script ────────────────────────────────────────────────
# This single script runs on the target machine (via SSH stdin or pct exec).
# It receives its argument as env var PROBE_ARG_B64 (base64-encoded JSON).

cat > "$WORK_DIR/remote_probe.py" << 'REMPY'
#!/usr/bin/env python3
"""
Remote Dockge probe. Receives config via env var PROBE_ARG_B64 (base64 JSON).
Arg schema: {vmid, pve_host, vm_type, ip, timestamp, name, instances:[{container,stacks_dir}]}
Output: JSON {stacks:[...], services:[...]}
"""
import base64, json, os, re, subprocess, sys

raw = os.environ.get("PROBE_ARG_B64", "")
if not raw:
    print(json.dumps({"stacks": [], "services": []})); sys.exit(0)

arg       = json.loads(base64.b64decode(raw).decode())
vmid      = str(arg.get("vmid", ""))
pve_host  = arg.get("pve_host", "")
vm_type   = arg.get("vm_type", "")
ip        = arg.get("ip", "")
ts        = arg.get("timestamp", "")
vm_name   = arg.get("name", "")
instances = arg.get("instances", [])


def run(cmd, timeout=15):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout if r.returncode == 0 else None
    except Exception:
        return None


def detect_parent_context(container_name, all_stacks_dirs):
    """Inspect Docker container labels to determine how Dockge was started."""
    out = run(["docker", "inspect", container_name, "--format", "{{json .Config.Labels}}"])
    if out is None:
        return "unknown", None
    try:
        labels = json.loads(out.strip())
    except Exception:
        return "unknown", None
    if not labels:
        return "docker-run", None
    working_dir = labels.get("com.docker.compose.project.working_dir", "")
    project     = labels.get("com.docker.compose.project", "")
    if working_dir:
        for sdir in all_stacks_dirs:
            sdir_n = sdir.rstrip("/")
            if working_dir.startswith(sdir_n + "/"):
                subdir = working_dir[len(sdir_n)+1:].split("/")[0]
                return "dockge-stack", subdir or project or None
        return "docker-compose", project or None
    return "docker-run", None


def parse_compose(content):
    """Extract services with image, ports, volumes from compose YAML text."""
    services = {}
    in_services = False
    cur_svc = None
    for line in content.splitlines():
        s = line.rstrip()
        if re.match(r'^services\s*:', s):
            in_services = True; continue
        if in_services:
            if s and not s.startswith((" ", "\t")) and re.match(r'^[a-zA-Z0-9_-]', s):
                in_services = False; continue
            m = re.match(r'^  ([a-zA-Z0-9_][a-zA-Z0-9_.:-]*):\s*$', s)
            if m:
                cur_svc = m.group(1)
                services[cur_svc] = {"image": None, "ports": [], "volumes": []}
                continue
            if cur_svc and s.strip():
                im = re.match(r'^\s+image\s*:\s*(.+)', s)
                if im:
                    services[cur_svc]["image"] = im.group(1).strip().strip('"\'')
                pm = re.match(r'^\s+-\s+["\']?(\d+:\d+(?:/\w+)?)["\']?\s*$', s)
                if pm:
                    services[cur_svc]["ports"].append(pm.group(1))
                vm = re.match(r'^\s+-\s+["\']?([./~][^:\s"\']+:[^:\s"\']+)["\']?\s*$', s)
                if vm:
                    services[cur_svc]["volumes"].append(vm.group(1))
    return services


def probe_stack(stack_path, sname, stack_id, ts):
    compose_content = None
    for fname in ["compose.yaml", "compose.yml", "docker-compose.yaml", "docker-compose.yml"]:
        c = run(["cat", f"{stack_path}/{fname}"])
        if c is not None:
            compose_content = c; break
    if not compose_content:
        return None, []

    env_file = (run(["sh", "-c", f"test -f {stack_path}/.env && echo yes || echo no"]) or "").strip() == "yes"

    services_defined = parse_compose(compose_content)

    # Live container state from docker compose ps
    ps_out = run(["docker", "compose", "-f", f"{stack_path}/compose.yaml", "ps", "--format", "json"], timeout=20)
    container_data = {}
    if ps_out:
        for pline in ps_out.strip().splitlines():
            pline = pline.strip()
            if not pline: continue
            try:
                c = json.loads(pline)
                svc   = c.get("Service") or c.get("service") or ""
                state = (c.get("State") or c.get("Status") or "").lower()
                cid   = (c.get("ID") or c.get("Id") or "")[:12]
                if svc:
                    container_data[svc] = {"state": state, "id": cid}
            except Exception:
                pass

    states = [v["state"] for v in container_data.values()]
    if not states:              stack_status = "unknown"
    elif all(s == "running" for s in states):   stack_status = "running"
    elif all(s in ("exited","stopped","") for s in states): stack_status = "stopped"
    elif any(s == "running" for s in states):   stack_status = "partial"
    else:                       stack_status = states[0]

    svc_rows = []
    all_ports = []
    for svc_n, svc_def in services_defined.items():
        cd = container_data.get(svc_n, {})
        svc_rows.append({
            "service_id":      f"{stack_id}_{svc_n}",
            "stack_id":        stack_id,
            "service_name":    svc_n,
            "image":           svc_def["image"],
            "ports_json":      json.dumps(svc_def["ports"])   if svc_def["ports"]   else None,
            "volumes_json":    json.dumps(svc_def["volumes"]) if svc_def["volumes"] else None,
            "container_state": cd.get("state"),
            "container_id":    cd.get("id"),
            "last_probed":     ts,
        })
        all_ports.extend(svc_def["ports"])

    stack_entry = {
        "stack_status":    stack_status,
        "compose_content": compose_content,
        "services_json":   json.dumps(list(services_defined.keys())) if services_defined else None,
        "ports_json":      json.dumps(list(set(all_ports))) if all_ports else None,
        "env_file_exists": 1 if env_file else 0,
    }
    return stack_entry, svc_rows


# ── Main ──────────────────────────────────────────────────────────────────────

all_stacks_dirs = [inst.get("stacks_dir","") for inst in instances if inst.get("stacks_dir")]
stacks_out = []; services_out = []

for inst in instances:
    container_name = inst.get("container", "")
    stacks_dir     = inst.get("stacks_dir", "")
    if not stacks_dir: continue

    dir_slug        = stacks_dir.strip("/").replace("/", "_")
    pve_safe        = pve_host.replace(".", "_")
    instance_prefix = f"{pve_safe}_{vmid}_{dir_slug}"

    parent_ctx, parent_stack = detect_parent_context(container_name, all_stacks_dirs)

    listing = run(["ls", "-1", stacks_dir])
    if listing is None: continue
    stack_names = [s.strip() for s in listing.splitlines() if s.strip()]

    for sname in stack_names:
        stack_id = f"{instance_prefix}_{sname}"
        entry, svcs = probe_stack(f"{stacks_dir}/{sname}", sname, stack_id, ts)
        if entry is None: continue
        stacks_out.append({
            "stack_id":          stack_id,
            "pve_host":          pve_host,
            "source_vmid":       vmid,
            "source_lxc_name":   vm_name,
            "stack_name":        sname,
            "status":            entry["stack_status"],
            "compose_content":   entry["compose_content"],
            "services_json":     entry["services_json"],
            "ports_json":        entry["ports_json"],
            "volumes_json":      None,
            "env_file_exists":   entry["env_file_exists"],
            "stacks_dir":        stacks_dir,
            "vm_type":           vm_type,
            "ip_address":        ip,
            "parent_context":    parent_ctx,
            "parent_stack_name": parent_stack,
            "last_probed":       ts,
        })
        services_out.extend(svcs)

print(json.dumps({"stacks": stacks_out, "services": services_out}))
REMPY

# ── PVE fallback for LXCs without direct SSH ─────────────────────────────────

_fallback_lxc() {
    # Run remote_probe.py inside an LXC via pct exec piped through PVE SSH
    local pve_ip="$1" vmid_in="$2" arg_b64="$3"
    local pve_key pve_src pve_bind=()
    pve_key=$(_pve_key_for "$pve_ip")
    [[ -z "$pve_key" ]] && { echo "  SKIP: no PVE key for ${pve_ip}" >&2; return 1; }
    pve_src=$(_pve_src_for "$pve_ip")
    [[ -n "$pve_src" ]] && pve_bind=(-b "$pve_src")

    local py_b64
    py_b64=$(base64 -w0 "$WORK_DIR/remote_probe.py")

    ssh -i "$pve_key" "${pve_bind[@]+"${pve_bind[@]}"}" \
        -o ConnectTimeout=10 -o BatchMode=yes -o StrictHostKeyChecking=accept-new \
        "root@${pve_ip}" \
        "echo '${py_b64}' | base64 -d > /tmp/_dp_${vmid_in}.py && \
         pct exec ${vmid_in} -- bash -c 'PROBE_ARG_B64=${arg_b64} python3 /dev/stdin' \
             < /tmp/_dp_${vmid_in}.py ; \
         rm -f /tmp/_dp_${vmid_in}.py" 2>/dev/null
}

# ── Main probe loop ───────────────────────────────────────────────────────────

TOTAL_MACHINES=0
TOTAL_INSTANCES=0
ALL_STACKS='[]'
ALL_SVCS='[]'

while IFS= read -r M; do
    PVE_HOST=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['pve_host'])" "$M")
    VMID=$(python3 -c "import json,sys; print(json.loads(sys.argv[1])['vmid'])" "$M")
    VM_NAME=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('name',''))" "$M")
    VM_TYPE=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('vm_type',''))" "$M")
    IP=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('ip_address',''))" "$M")
    KEY_VAR=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('key_env_var',''))" "$M")
    SRC_IP=$(python3 -c "import json,sys; print(json.loads(sys.argv[1]).get('source_ip',''))" "$M")
    INST_JSON=$(python3 -c "import json,sys; print(json.dumps(json.loads(sys.argv[1]).get('dockge_instances',[])))" "$M")

    echo "[probe] vmid=${VMID} (${VM_NAME}) type=${VM_TYPE} ip=${IP} key=${KEY_VAR}" >&2
    (( TOTAL_MACHINES+=1 )) || true
    (( TOTAL_INSTANCES+=$(python3 -c "import json,sys; print(len(json.loads(sys.argv[1])))" "$INST_JSON") )) || true

    # Build probe arg and base64-encode it
    PROBE_ARG=$(python3 -c "
import base64, json, sys
d = {
    'vmid':      sys.argv[1],
    'pve_host':  sys.argv[2],
    'vm_type':   sys.argv[3],
    'ip':        sys.argv[4],
    'timestamp': sys.argv[5],
    'name':      sys.argv[6],
    'instances': json.loads(sys.argv[7]),
}
# Print as base64 to avoid shell quoting issues
print(base64.b64encode(json.dumps(d).encode()).decode())
" "$VMID" "$PVE_HOST" "$VM_TYPE" "$IP" "$TIMESTAMP" "$VM_NAME" "$INST_JSON")

    OUTFILE="$WORK_DIR/${VMID}_result.json"
    PROBED=0

    # Try direct SSH
    if [[ -n "$IP" && -n "$KEY_VAR" ]]; then
        KEY_PATH=$(_resolve_key "$KEY_VAR")
        if [[ -n "$KEY_PATH" ]]; then
            _direct_ssh "$KEY_PATH" "$SRC_IP" "$IP" \
                "PROBE_ARG_B64=${PROBE_ARG} python3 /dev/stdin" \
                < "$WORK_DIR/remote_probe.py" > "$OUTFILE" 2>/dev/null \
                && PROBED=1 \
                || echo "  WARNING: direct SSH failed for ${IP} (${KEY_VAR})" >&2
        else
            echo "  WARNING: key env var '${KEY_VAR}' not set or file missing — skipping direct SSH" >&2
        fi
    fi

    # Fallback: pct exec via PVE (LXCs only)
    if [[ "$PROBED" -eq 0 && "$VM_TYPE" == "lxc" ]]; then
        echo "  FALLBACK: pct exec for LXC ${VMID} via ${PVE_HOST}" >&2
        _fallback_lxc "$PVE_HOST" "$VMID" "$PROBE_ARG" > "$OUTFILE" 2>/dev/null || true
        [[ -s "$OUTFILE" ]] && PROBED=1
    elif [[ "$PROBED" -eq 0 && "$VM_TYPE" == "qemu" ]]; then
        echo "  SKIP: QEMU vmid=${VMID} has no accessible SSH target — add to ssh_targets table" >&2
    fi

    if [[ "$PROBED" -eq 0 || ! -s "$OUTFILE" ]]; then
        echo "  WARNING: no output from vmid=${VMID} — skipping" >&2
        continue
    fi

    ALL_STACKS=$(python3 -c "
import json, sys
a  = json.loads(sys.argv[1])
nd = json.load(open(sys.argv[2]))
a.extend(nd.get('stacks',[]))
print(json.dumps(a))
" "$ALL_STACKS" "$OUTFILE" 2>/dev/null || echo "$ALL_STACKS")

    ALL_SVCS=$(python3 -c "
import json, sys
a  = json.loads(sys.argv[1])
nd = json.load(open(sys.argv[2]))
a.extend(nd.get('services',[]))
print(json.dumps(a))
" "$ALL_SVCS" "$OUTFILE" 2>/dev/null || echo "$ALL_SVCS")

    SC=$(python3 -c "import json,sys; print(len(json.load(open(sys.argv[1])).get('stacks',[])))" "$OUTFILE" 2>/dev/null || echo 0)
    echo "  -> ${SC} stacks from vmid=${VMID}" >&2

done < <(python3 -c "
import json, sys
for m in json.loads(sys.stdin.read()):
    print(json.dumps(m))
" <<< "$MACHINES_JSON")

echo "" >&2
FS=$(python3 -c "import json,sys; print(len(json.loads(sys.stdin.read())))" <<< "$ALL_STACKS" 2>/dev/null || echo 0)
SS=$(python3 -c "import json,sys; print(len(json.loads(sys.stdin.read())))" <<< "$ALL_SVCS" 2>/dev/null || echo 0)
echo "[done] ${TOTAL_MACHINES} machine(s), ${TOTAL_INSTANCES} Dockge instance(s), ${FS} stacks, ${SS} services" >&2

echo "##ENTRIES## ${ALL_STACKS}"
echo "##SERVICES## ${ALL_SVCS}"
printf '##STATS## {"machines_probed":%d,"dockge_instances_probed":%d,"stacks_found":%d,"services_found":%d}\n' \
    "$TOTAL_MACHINES" "$TOTAL_INSTANCES" "$FS" "$SS"
