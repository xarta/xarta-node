#!/usr/bin/env bash
# setup-docker.sh — install Docker Engine on Debian 12 and prepare the xarta workflow.
#
# What this script does (idempotent):
#   1. Installs Docker's official apt repository and packages.
#   2. Ensures the docker group exists.
#   3. Ensures the xarta user is in that group when present.
#   4. Optionally configures an HTTP/insecure registry from .env.
#   5. Enables and starts Docker.
#
# The registry endpoint is optional. If not configured or not reachable, Docker
# installation still succeeds.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [[ -f "$ENV_FILE" ]]; then
    # shellcheck source=/dev/null
    source "$ENV_FILE"
fi

XARTA_USER="${XARTA_USER:-xarta}"
DOCKER_REGISTRY_HOST="${DOCKER_REGISTRY_HOST:-}"
DOCKER_REGISTRY_PORT="${DOCKER_REGISTRY_PORT:-5000}"
DOCKER_REGISTRY_SCHEME="${DOCKER_REGISTRY_SCHEME:-http}"
DOCKER_REGISTRY_INSECURE="${DOCKER_REGISTRY_INSECURE:-true}"
PROFILE_FILE="/etc/profile.d/xarta-docker-registry.sh"
DOCKER_DAEMON_JSON="/etc/docker/daemon.json"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}Error:${NC} must be run as root." >&2
    exit 1
fi

echo "=== Docker setup ==="
echo ""

apt-get update
apt-get install -y ca-certificates curl gnupg

install -d -m 0755 /etc/apt/keyrings
if [[ ! -f /etc/apt/keyrings/docker.asc ]]; then
    curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
fi

if [[ ! -f /etc/apt/sources.list.d/docker.list ]]; then
    . /etc/os-release
    echo \
        "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $VERSION_CODENAME stable" \
        > /etc/apt/sources.list.d/docker.list
fi

apt-get update
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

groupadd -f docker
if id "$XARTA_USER" >/dev/null 2>&1; then
    usermod -aG docker "$XARTA_USER"
    echo -e "${GREEN}ensured${NC}: $XARTA_USER is in docker group"
else
    echo -e "${YELLOW}warning${NC}: user $XARTA_USER not present yet; docker group membership skipped"
fi

if [[ -n "$DOCKER_REGISTRY_HOST" ]]; then
    registry="${DOCKER_REGISTRY_HOST}:${DOCKER_REGISTRY_PORT}"
    python3 - "$DOCKER_DAEMON_JSON" "$registry" "$DOCKER_REGISTRY_SCHEME" "$DOCKER_REGISTRY_INSECURE" <<'PY'
import json
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
registry = sys.argv[2]
scheme = sys.argv[3]
insecure = sys.argv[4].lower() == "true"

data = {}
if path.exists():
    text = path.read_text().strip()
    if text:
        data = json.loads(text)

data.setdefault("features", {})
data["features"]["buildkit"] = True

if scheme == "http" or insecure:
    entries = data.setdefault("insecure-registries", [])
    if registry not in entries:
        entries.append(registry)

path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
PY
    cat > "$PROFILE_FILE" <<EOF
export XARTA_DOCKER_REGISTRY=${registry}
export XARTA_DOCKER_REGISTRY_URL=${DOCKER_REGISTRY_SCHEME}://${registry}
EOF
    chmod 644 "$PROFILE_FILE"
    echo -e "${GREEN}configured${NC}: registry settings for $registry"
else
    echo -e "${CYAN}skipped${NC}: no DOCKER_REGISTRY_HOST set in .env"
fi

systemctl enable --now docker
systemctl is-active --quiet docker

echo ""
echo -e "${GREEN}Done.${NC}"
docker --version