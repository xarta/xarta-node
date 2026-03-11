#!/usr/bin/env bash
# bp-pve-discover.sh — SSH into a Proxmox host and enumerate LXCs + VMs.
# Usage: bp-pve-discover.sh <proxmox-ip-or-hostname>
#
# Outputs JSON lines, one per LXC/VM:
#   {"vmid":"100","name":"mycontainer","status":"running","type":"lxc","ips":["192.168.1.10"],"mem_mb":"","disk_gb":""}
#
# Prerequisites:
#   - SSH access to root@<proxmox-host> (agent forwarding recommended)
#   - jq not required (pure bash + awk)

set -euo pipefail

PVE_HOST="${1:?Usage: bp-pve-discover.sh <proxmox-ip>}"
SSH_OPTS="-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o BatchMode=yes"

# --- Discover LXCs ---
ssh $SSH_OPTS root@"$PVE_HOST" bash -s <<'REMOTE_LXC'
for vmid in $(pct list 2>/dev/null | tail -n+2 | awk '{print $1}'); do
    status=$(pct status "$vmid" 2>/dev/null | awk '{print $2}')
    name=$(pct list | grep "^[[:space:]]*${vmid} " | awk '{print $NF}')
    ips=""
    if [ "$status" = "running" ]; then
        raw=$(pct exec "$vmid" -- hostname -I 2>/dev/null || true)
        # Build JSON array of IPs, filtering docker bridge addresses
        ips_arr=""
        for ip in $raw; do
            case "$ip" in
                172.1[6-9].*|172.2[0-9].*|172.3[0-1].*) continue ;;  # skip docker bridges
                192.168.0.*|192.168.16.*|192.168.32.*|192.168.48.*) continue ;; # skip docker overlay
                192.168.80.*|192.168.96.*|192.168.112.*|192.168.128.*) continue ;;
                192.168.144.*|192.168.160.*|192.168.176.*|192.168.192.*) continue ;;
                192.168.208.*|192.168.224.*|192.168.240.*) continue ;;
                fe80:*|fd*) continue ;; # skip link-local and ULA IPv6 (keep Tailscale fd7a)
                2a00:*|2a10:*) continue ;; # skip public IPv6
            esac
            [ -n "$ips_arr" ] && ips_arr="${ips_arr},"
            ips_arr="${ips_arr}\"${ip}\""
        done
        ips="[${ips_arr}]"
    else
        ips="[]"
    fi
    printf '{"vmid":"%s","name":"%s","status":"%s","type":"lxc","ips":%s,"mem_mb":"","disk_gb":""}\n' \
        "$vmid" "$name" "$status" "$ips"
done
REMOTE_LXC

# --- Discover VMs ---
ssh $SSH_OPTS root@"$PVE_HOST" bash -s <<'REMOTE_VM'
qm list 2>/dev/null | tail -n+2 | while read -r line; do
    vmid=$(echo "$line" | awk '{print $1}')
    name=$(echo "$line" | awk '{print $2}')
    status=$(echo "$line" | awk '{print $3}')
    mem=$(echo "$line" | awk '{print $4}')
    disk=$(echo "$line" | awk '{print $5}')
    # VM IPs are harder to get without qemu-guest-agent; output empty array
    printf '{"vmid":"%s","name":"%s","status":"%s","type":"vm","ips":[],"mem_mb":"%s","disk_gb":"%s"}\n' \
        "$vmid" "$name" "$status" "$mem" "$disk"
done
REMOTE_VM
