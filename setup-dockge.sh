#!/bin/bash

# setup-dockge.sh
# Installs Dockge as a Docker Compose stack backed by the node-local
# .lone-wolf repo under /xarta-node/.lone-wolf by default.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Error: .env not found at $ENV_FILE" >&2
    exit 1
fi

source "$ENV_FILE"

DOCKGE_STACKS_DIR="${DOCKGE_STACKS_DIR:-/xarta-node/.lone-wolf/stacks}"
DOCKGE_DATA_DIR="${DOCKGE_DATA_DIR:-$DOCKGE_STACKS_DIR/dockge/data}"
DOCKGE_STACK_DIR="${DOCKGE_STACK_DIR:-$DOCKGE_STACKS_DIR/dockge}"
DOCKGE_PORT="${DOCKGE_PORT:-5001}"
DOCKGE_BIND_IP="${DOCKGE_BIND_IP:-127.0.0.1}"
XARTA_USER="${XARTA_USER:-xarta}"

echo "=== Dockge setup ==="
echo "Stacks dir : $DOCKGE_STACKS_DIR"
echo "Stack dir  : $DOCKGE_STACK_DIR"
echo "Data dir   : $DOCKGE_DATA_DIR"
echo "Bind       : $DOCKGE_BIND_IP:$DOCKGE_PORT"
echo "User       : $XARTA_USER"
echo ""

if ! command -v docker >/dev/null 2>&1; then
    echo "Error: docker is not installed. Run setup-docker.sh first." >&2
    exit 1
fi

if ! docker info >/dev/null 2>&1; then
    echo "Error: docker daemon is not reachable." >&2
    exit 1
fi

mkdir -p "$DOCKGE_STACKS_DIR" "$DOCKGE_STACK_DIR" "$DOCKGE_DATA_DIR"

cat > "$DOCKGE_STACK_DIR/compose.yaml" <<EOF
services:
  dockge:
    image: louislam/dockge:1
    container_name: dockge
    restart: unless-stopped
    ports:
      - ${DOCKGE_BIND_IP}:${DOCKGE_PORT}:5001
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ${DOCKGE_DATA_DIR}:/app/data
      - ${DOCKGE_STACKS_DIR}:${DOCKGE_STACKS_DIR}
    environment:
      - DOCKGE_STACKS_DIR=${DOCKGE_STACKS_DIR}
EOF

chown -R "$XARTA_USER:$XARTA_USER" \
    "$(dirname "$DOCKGE_STACKS_DIR")" \
    "$DOCKGE_STACKS_DIR"

docker compose -f "$DOCKGE_STACK_DIR/compose.yaml" up -d

echo ""
echo "Dockge is running."
echo "URL: http://${DOCKGE_BIND_IP}:${DOCKGE_PORT}"
