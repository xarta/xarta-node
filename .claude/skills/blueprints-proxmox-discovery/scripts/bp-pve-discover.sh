#!/usr/bin/env bash
# bp-pve-discover.sh — SSH into a Proxmox host and enumerate LXCs + VMs.
# Usage: bp-pve-discover.sh <proxmox-ip-or-hostname>
#
# Outputs JSON lines, one per LXC/VM:
#   {"vmid":"100","name":"mycontainer","status":"running","type":"lxc",
#    "ips":["192.168.1.10"],"conf_ips":["192.168.1.10"],
#    "gw":"192.168.1.1","mac":"AA:BB:CC:DD:EE:FF","vlan":"42",
#    "mem_mb":"","disk_gb":""}
#
# Enhanced fields from config file parsing:
#   conf_ips  - Static IPs from /etc/pve/lxc/*.conf (works for stopped LXCs)
#   gw        - Gateway IP from config
#   mac       - MAC address from config (hwaddr= for LXC, virtio= for VM)
#   vlan      - VLAN tag from config
#
# Prerequisites:
#   - SSH access to root@<proxmox-host> (agent forwarding recommended)
#   - jq not required (pure bash + awk + grep)

set -euo pipefail

PVE_HOST="${1:?Usage: bp-pve-discover.sh <proxmox-ip>}"
SSH_OPTS="-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o BatchMode=yes"

# --- Discover LXCs ---
ssh $SSH_OPTS root@"$PVE_HOST" bash -s <<'REMOTE_LXC'
for vmid in $(pct list 2>/dev/null | tail -n+2 | awk '{print $1}'); do
    status=$(pct status "$vmid" 2>/dev/null | awk '{print $2}')
    name=$(pct list | grep "^[[:space:]]*${vmid} " | awk '{print $NF}')

    # --- Live IPs from running containers ---
    ips=""
    if [ "$status" = "running" ]; then
        raw=$(pct exec "$vmid" -- hostname -I 2>/dev/null || true)
        ips_arr=""
        for ip in $raw; do
            case "$ip" in
                172.1[6-9].*|172.2[0-9].*|172.3[0-1].*) continue ;;  # docker bridges
                192.168.0.*|192.168.16.*|192.168.32.*|192.168.48.*) continue ;; # docker overlay
                192.168.80.*|192.168.96.*|192.168.112.*|192.168.128.*) continue ;;
                192.168.144.*|192.168.160.*|192.168.176.*|192.168.192.*) continue ;;
                192.168.208.*|192.168.224.*|192.168.240.*) continue ;;
                fe80:*|fd*) continue ;; # skip link-local and ULA IPv6
                2a00:*|2a10:*) continue ;; # skip public IPv6
            esac
            [ -n "$ips_arr" ] && ips_arr="${ips_arr},"
            ips_arr="${ips_arr}\"${ip}\""
        done
        ips="[${ips_arr}]"
    else
        ips="[]"
    fi

    # --- Config file parsing for static IPs, gateway, MAC, VLAN ---
    conf_file="/etc/pve/lxc/${vmid}.conf"
    conf_ips="[]"
    gw=""
    mac=""
    vlan=""
    if [ -f "$conf_file" ]; then
        # Extract IPs from net lines (ip=x.x.x.x/y)
        conf_ip_arr=""
        for cip in $(grep -oP 'ip=\K[0-9.]+' "$conf_file" 2>/dev/null); do
            [ -n "$conf_ip_arr" ] && conf_ip_arr="${conf_ip_arr},"
            conf_ip_arr="${conf_ip_arr}\"${cip}\""
        done
        conf_ips="[${conf_ip_arr}]"

        # Extract gateway
        gw=$(grep -oP 'gw=\K[0-9.]+' "$conf_file" 2>/dev/null | head -1 || true)

        # Extract MAC address
        mac=$(grep -oP 'hwaddr=\K[A-Fa-f0-9:]+' "$conf_file" 2>/dev/null | head -1 || true)

        # Extract VLAN tag
        vlan=$(grep -oP 'tag=\K[0-9]+' "$conf_file" 2>/dev/null | head -1 || true)
    fi

    printf '{"vmid":"%s","name":"%s","status":"%s","type":"lxc","ips":%s,"conf_ips":%s,"gw":"%s","mac":"%s","vlan":"%s","mem_mb":"","disk_gb":""}\n' \
        "$vmid" "$name" "$status" "$ips" "$conf_ips" "$gw" "$mac" "$vlan"
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

    # --- Config file parsing for MAC, VLAN ---
    conf_file="/etc/pve/qemu-server/${vmid}.conf"
    mac=""
    vlan=""
    if [ -f "$conf_file" ]; then
        # Extract MAC from virtio= or net0: lines
        mac=$(grep -oP 'virtio=\K[A-Fa-f0-9:]+' "$conf_file" 2>/dev/null | head -1 || true)
        if [ -z "$mac" ]; then
            mac=$(grep -oP 'macaddr=\K[A-Fa-f0-9:]+' "$conf_file" 2>/dev/null | head -1 || true)
        fi

        # Extract VLAN tag
        vlan=$(grep -oP 'tag=\K[0-9]+' "$conf_file" 2>/dev/null | head -1 || true)
    fi

    # VM IPs are harder without guest agent — output empty array
    # Use Phase 4 (MAC cross-referencing) or Phase 9 (DNS) to resolve
    printf '{"vmid":"%s","name":"%s","status":"%s","type":"vm","ips":[],"conf_ips":[],"gw":"","mac":"%s","vlan":"%s","mem_mb":"%s","disk_gb":"%s"}\n' \
        "$vmid" "$name" "$status" "$mac" "$vlan" "$mem" "$disk"
done
REMOTE_VM
