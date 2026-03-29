#!/usr/bin/env bash
# Normalize docs subtree ownership back to the docs-root owner.

set -euo pipefail

# shellcheck disable=SC1091
source /root/xarta-node/.env 2>/dev/null || true

DOCS_ROOT="${DOCS_ROOT:-${REPO_INNER_PATH:-}}"
[[ -n "$DOCS_ROOT" ]] || exit 0

DOCS_TREE="${DOCS_ROOT}/docs"
[[ -d "$DOCS_TREE" ]] || exit 0

OWNER="$(stat -c '%u:%g' "$DOCS_ROOT")"
chown -R "$OWNER" "$DOCS_TREE"