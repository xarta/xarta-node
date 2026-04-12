#!/usr/bin/env bash
# lint-public-python.sh — run ruff check and ruff format check over blueprints-app Python source.
#
# Usage:
#   bash lint-public-python.sh              # check only (no changes, exit 1 if issues found)
#   bash lint-public-python.sh --fix        # auto-fix safe lint issues (no format changes)
#   bash lint-public-python.sh --format     # also show format diff (no changes)
#   bash lint-public-python.sh --fix-all    # both --fix and apply ruff format (MODIFIES files)
#
# Run from: /root/xarta-node (or any dir in xarta-node repo)
#
# Config: ruff.toml in the repo root (auto-discovered by ruff).
# Scope: blueprints-app/ (public Python source only).

set -euo pipefail

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
CYAN='\033[0;36m'
NC='\033[0m'

FIX=false
FORMAT=false
FORMAT_APPLY=false

for arg in "$@"; do
    case "$arg" in
        --fix)      FIX=true ;;
        --format)   FORMAT=true ;;
        --fix-all)  FIX=true; FORMAT=true; FORMAT_APPLY=true ;;
        *) echo -e "${RED}Unknown option:${NC} $arg" >&2; exit 1 ;;
    esac
done

if ! command -v ruff >/dev/null 2>&1; then
    echo -e "${RED}Error:${NC} ruff not found. Run: bash setup-python-dev-tools.sh" >&2
    exit 1
fi

# Resolve repo root — works whether called from repo root or skill scripts/ dir
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
TARGET="$REPO_ROOT/blueprints-app"

if [[ ! -d "$TARGET" ]]; then
    echo -e "${RED}Error:${NC} blueprints-app/ not found at $TARGET" >&2
    exit 1
fi

echo "=== Python lint check (ruff) ==="
echo -e "${CYAN}repo:${NC}   $REPO_ROOT"
echo -e "${CYAN}target:${NC} $TARGET"
echo -e "${CYAN}ruff:${NC}   $(ruff --version)"
echo ""

LINT_EXIT=0
FORMAT_EXIT=0

# ── Lint ──────────────────────────────────────────────────────────────────────
echo "--- ruff check (lint)..."
if [[ "$FIX" == "true" ]]; then
    ruff check --fix "$TARGET" || LINT_EXIT=$?
else
    ruff check "$TARGET" || LINT_EXIT=$?
fi

if [[ $LINT_EXIT -eq 0 ]]; then
    echo -e "${GREEN}lint: clean${NC}"
else
    echo -e "${YELLOW}lint: issues found (see above)${NC}"
fi
echo ""

# ── Format ────────────────────────────────────────────────────────────────────
if [[ "$FORMAT" == "true" || "$FORMAT_APPLY" == "true" ]]; then
    echo "--- ruff format..."
    if [[ "$FORMAT_APPLY" == "true" ]]; then
        ruff format "$TARGET" || FORMAT_EXIT=$?
        echo -e "${GREEN}format: applied${NC}"
    else
        ruff format --check "$TARGET" || FORMAT_EXIT=$?
        if [[ $FORMAT_EXIT -eq 0 ]]; then
            echo -e "${GREEN}format: all files already formatted${NC}"
        else
            echo -e "${YELLOW}format: files above would be reformatted (run with --fix-all to apply)${NC}"
        fi
    fi
    echo ""
fi

# ── Summary ───────────────────────────────────────────────────────────────────
if [[ $LINT_EXIT -ne 0 ]] || [[ $FORMAT_EXIT -ne 0 ]]; then
    echo -e "${YELLOW}Done — issues found. See above.${NC}"
    exit 1
fi

echo -e "${GREEN}Done — all checks passed.${NC}"
