#!/usr/bin/env bash
# setup-inventory.sh — read-only setup capability inventory for xarta-node.

set -euo pipefail

ROOT="/root/xarta-node"

if [[ ! -d "$ROOT" ]]; then
    echo "ERROR: expected repo root at $ROOT" >&2
    exit 1
fi

# capability|script|probe-command
catalog=(
  "lxc-failover|setup-lxc-failover.sh|command -v tailscale >/dev/null 2>&1 && command -v iptables >/dev/null 2>&1"
  "ssh-and-git|setup-ssh-and-git.sh|command -v git >/dev/null 2>&1"
  "certificates|setup-certificates.sh|command -v openssl >/dev/null 2>&1"
  "blueprints|setup-blueprints.sh|[[ -x /opt/blueprints/venv/bin/python ]] && systemctl is-active --quiet blueprints-app"
  "caddy|setup-caddy.sh|command -v caddy >/dev/null 2>&1 && systemctl is-active --quiet caddy"
  "tailscale-up|setup-tailscale-up.sh|command -v tailscale >/dev/null 2>&1 && tailscale status >/dev/null 2>&1"
  "firewall|setup-firewall.sh|iptables -S XARTA_INPUT >/dev/null 2>&1"
  "python-dev-tools|setup-python-dev-tools.sh|command -v uv >/dev/null 2>&1 && command -v ruff >/dev/null 2>&1"
  "shellcheck|setup-shellcheck.sh|command -v shellcheck >/dev/null 2>&1"
  "rsync|setup-rsync.sh|command -v rsync >/dev/null 2>&1"
  "github-cli|setup-github-cli.sh|command -v gh >/dev/null 2>&1"
  "docker|setup-docker.sh|command -v docker >/dev/null 2>&1 && systemctl is-active --quiet docker"
  "dockge|setup-dockge.sh|docker ps --format '{{.Names}}' 2>/dev/null | grep -qx dockge"
  "user-xarta|setup-user-xarta.sh|id xarta >/dev/null 2>&1"
  "ssh-and-git-xarta|setup-ssh-and-git-xarta.sh|[[ -f /home/xarta/.ssh/authorized_keys ]]"
  "xfce-xrdp|setup-xfce-xrdp.sh|systemctl is-active --quiet xrdp"
  "desktop-apps|setup-desktop-apps.sh|command -v code >/dev/null 2>&1 && command -v microsoft-edge >/dev/null 2>&1"
  "syncthing|setup-syncthing.sh|systemctl is-active --quiet syncthing@xarta.service || systemctl is-active --quiet syncthing@syncthing.service"
  "hosts|setup-hosts.sh|grep -q 'BEGIN blueprints-fleet-hosts' /etc/hosts"
  "seekdb|setup-seekdb.sh|systemctl is-active --quiet seekdb"
  "lone-wolf|setup-lone-wolf.sh|[[ -d /xarta-node/.lone-wolf ]]"
)

ok=0
missing_script=0
not_installed=0

printf "%-20s %-8s %-10s %s\n" "CAPABILITY" "SCRIPT" "STATUS" "DETAIL"
printf "%-20s %-8s %-10s %s\n" "----------" "------" "------" "------"

for row in "${catalog[@]}"; do
    cap="${row%%|*}"
    rest="${row#*|}"
    script="${rest%%|*}"
    probe="${rest#*|}"

    script_path="$ROOT/$script"
    if [[ ! -f "$script_path" ]]; then
        printf "%-20s %-8s %-10s %s\n" "$cap" "missing" "unknown" "$script_path"
        missing_script=$((missing_script + 1))
        continue
    fi

    if bash -c "$probe" >/dev/null 2>&1; then
        printf "%-20s %-8s %-10s %s\n" "$cap" "present" "installed" "$script"
        ok=$((ok + 1))
    else
        printf "%-20s %-8s %-10s %s\n" "$cap" "present" "missing" "$script"
        not_installed=$((not_installed + 1))
    fi
done

echo ""
echo "Summary: installed=$ok missing_state=$not_installed missing_script=$missing_script total=${#catalog[@]}"
