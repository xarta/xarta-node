#!/usr/bin/env bash
# Normalize Syncthing asset subtree ownership back to the fallback GUI owner.

set -euo pipefail

# shellcheck disable=SC1091
source /root/xarta-node/.env 2>/dev/null || true

ASSETS_DIR="${BLUEPRINTS_ASSETS_DIR:-/xarta-node/gui-fallback/assets}"
ICONS_DIR="$ASSETS_DIR/icons"
SOUNDS_DIR="$ASSETS_DIR/sounds"

[[ -d "$ICONS_DIR" ]] || exit 0
[[ -d "$SOUNDS_DIR" ]] || exit 0

REFERENCE_ROOT="$(dirname "$ASSETS_DIR")"
[[ -d "$REFERENCE_ROOT" ]] || exit 0

OWNER_UID="$(stat -c '%u' "$REFERENCE_ROOT")"
OWNER_GID="$(stat -c '%g' "$REFERENCE_ROOT")"

# Only touch paths that have drifted; avoid unnecessary inode metadata churn.
if ! find "$ICONS_DIR" "$SOUNDS_DIR" \
    \( ! -uid "$OWNER_UID" -o ! -gid "$OWNER_GID" \) \
    -print -quit 2>/dev/null | grep -q .; then
    exit 0
fi

find "$ICONS_DIR" "$SOUNDS_DIR" \
    \( ! -uid "$OWNER_UID" -o ! -gid "$OWNER_GID" \) \
    -exec chown "$OWNER_UID:$OWNER_GID" {} +
