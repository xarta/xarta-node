#!/usr/bin/env bash
# Normalize docs subtree ownership back to the docs-root owner.

set -euo pipefail

# shellcheck disable=SC1091
source /root/xarta-node/.env 2>/dev/null || true

DOCS_ROOT="${DOCS_ROOT:-${REPO_INNER_PATH:-}}"
[[ -n "$DOCS_ROOT" ]] || exit 0

DOCS_TREE="${DOCS_ROOT}/docs"
[[ -d "$DOCS_TREE" ]] || exit 0

OWNER_UID="$(stat -c '%u' "$DOCS_ROOT")"
OWNER_GID="$(stat -c '%g' "$DOCS_ROOT")"

# Only touch paths that have actually drifted. A blanket chown -R every minute
# would generate unnecessary inode metadata churn across the docs tree.
if ! find "$DOCS_TREE" \( ! -uid "$OWNER_UID" -o ! -gid "$OWNER_GID" \) -print -quit 2>/dev/null | grep -q .; then
	exit 0
fi

find "$DOCS_TREE" \( ! -uid "$OWNER_UID" -o ! -gid "$OWNER_GID" \) -exec chown "$OWNER_UID:$OWNER_GID" {} +