#!/usr/bin/env bash
# setup-remediate.sh — controlled setup remediation runner for xarta-node.

set -euo pipefail

ROOT="/root/xarta-node"
INVENTORY_SCRIPT="$ROOT/.claude/skills/setup-visibility-control/scripts/setup-inventory.sh"
APPLY=false
PROFILE="baseline"
INCLUDE=""
EXCLUDE=""

if [[ ! -d "$ROOT" ]]; then
    echo "ERROR: expected repo root at $ROOT" >&2
    exit 1
fi

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
    echo "ERROR: run as root" >&2
    exit 1
fi

while [[ $# -gt 0 ]]; do
    case "$1" in
        --apply) APPLY=true; shift ;;
        --profile) PROFILE="$2"; shift 2 ;;
        --include) INCLUDE="$2"; shift 2 ;;
        --exclude) EXCLUDE="$2"; shift 2 ;;
        --help|-h)
            cat <<'EOF'
Usage:
  bash setup-remediate.sh [--profile <name>] [--include a,b] [--exclude a,b] [--apply]

Default: dry-run (no changes) unless --apply is provided.
Profiles: baseline, network, ops, desktop, containers, sync
EOF
            exit 0
            ;;
        *) echo "ERROR: unknown option: $1" >&2; exit 1 ;;
    esac
done

# capability|script
catalog=(
  "lxc-failover|setup-lxc-failover.sh"
  "ssh-and-git|setup-ssh-and-git.sh"
  "certificates|setup-certificates.sh"
  "blueprints|setup-blueprints.sh"
  "caddy|setup-caddy.sh"
  "tailscale-up|setup-tailscale-up.sh"
  "firewall|setup-firewall.sh"
  "python-dev-tools|setup-python-dev-tools.sh"
  "shellcheck|setup-shellcheck.sh"
  "rsync|setup-rsync.sh"
  "github-cli|setup-github-cli.sh"
  "docker|setup-docker.sh"
  "dockge|setup-dockge.sh"
  "user-xarta|setup-user-xarta.sh"
  "ssh-and-git-xarta|setup-ssh-and-git-xarta.sh"
  "xfce-xrdp|setup-xfce-xrdp.sh"
  "desktop-apps|setup-desktop-apps.sh"
  "syncthing|setup-syncthing.sh"
)

profile_baseline="ssh-and-git,certificates,blueprints,caddy,python-dev-tools"
profile_network="lxc-failover,tailscale-up,firewall"
profile_ops="shellcheck,rsync,github-cli"
profile_desktop="user-xarta,ssh-and-git-xarta,xfce-xrdp,desktop-apps"
profile_containers="docker,dockge"
profile_sync="syncthing"

case "$PROFILE" in
    baseline) target_csv="$profile_baseline" ;;
    network) target_csv="$profile_network" ;;
    ops) target_csv="$profile_ops" ;;
    desktop) target_csv="$profile_desktop" ;;
    containers) target_csv="$profile_containers" ;;
    sync) target_csv="$profile_sync" ;;
    *) echo "ERROR: unknown profile: $PROFILE" >&2; exit 1 ;;
esac

if [[ -n "$INCLUDE" ]]; then
    if [[ -n "$target_csv" ]]; then
        target_csv="$target_csv,$INCLUDE"
    else
        target_csv="$INCLUDE"
    fi
fi

IFS=',' read -r -a target_caps <<< "$target_csv"
IFS=',' read -r -a exclude_caps <<< "$EXCLUDE"

contains_cap() {
    local needle="$1"; shift
    local item
    for item in "$@"; do
        [[ "$item" == "$needle" ]] && return 0
    done
    return 1
}

echo "=== setup remediation ==="
echo "root:     $ROOT"
echo "profile:  $PROFILE"
[[ -n "$INCLUDE" ]] && echo "include:  $INCLUDE"
[[ -n "$EXCLUDE" ]] && echo "exclude:  $EXCLUDE"
if [[ "$APPLY" == "true" ]]; then
    echo "mode:     apply"
else
    echo "mode:     dry-run"
fi
echo ""

# Build quick status map from inventory output.
declare -A cap_status
while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    [[ "$line" == CAPABILITY* ]] && continue
    [[ "$line" == ----------* ]] && continue
    cap="$(awk '{print $1}' <<<"$line")"
    status="$(awk '{print $3}' <<<"$line")"
    cap_status["$cap"]="$status"
done < <(bash "$INVENTORY_SCRIPT")

ran=0
skipped_installed=0
skipped_excluded=0
missing_script=0

for row in "${catalog[@]}"; do
    cap="${row%%|*}"
    script="${row#*|}"

    if ! contains_cap "$cap" "${target_caps[@]}"; then
        continue
    fi

    if [[ -n "$EXCLUDE" ]] && contains_cap "$cap" "${exclude_caps[@]}"; then
        echo "SKIP  $cap  (excluded)"
        skipped_excluded=$((skipped_excluded + 1))
        continue
    fi

    script_path="$ROOT/$script"
    if [[ ! -f "$script_path" ]]; then
        echo "MISS  $cap  (script not found: $script_path)"
        missing_script=$((missing_script + 1))
        continue
    fi

    if [[ "${cap_status[$cap]:-unknown}" == "installed" ]]; then
        echo "SKIP  $cap  (already installed)"
        skipped_installed=$((skipped_installed + 1))
        continue
    fi

    if [[ "$APPLY" == "true" ]]; then
        echo "RUN   $cap  -> $script"
        bash "$script_path"
        ran=$((ran + 1))
    else
        echo "PLAN  $cap  -> bash $script_path"
    fi
done

echo ""
echo "Summary: ran=$ran skipped_installed=$skipped_installed skipped_excluded=$skipped_excluded missing_script=$missing_script"
if [[ "$APPLY" != "true" ]]; then
    echo "Dry-run only. Re-run with --apply to execute planned actions."
fi
