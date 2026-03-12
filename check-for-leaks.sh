#!/bin/bash

# check-for-leaks.sh
# Checks that no values from .env (or private infra patterns) appear in
# publicly-committed files. Run from anywhere.
#
# Private patterns are loaded from .xarta/infra-leaks.txt (inner/private repo).
# If that file is absent the extra-patterns pass is skipped with a warning.

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
XARTA_DIR="$REPO_DIR/.xarta"
ENV_FILE="$REPO_DIR/.env"
INFRA_LEAKS_FILE="$XARTA_DIR/infra-leaks.txt"

# Minimum value length to bother checking — avoids false positives on
# short/common strings like "5" or short interface names.
MIN_LEN=5

# Keys whose values are intentionally referenced in committed files and should
# not be treated as leaks (e.g. paths baked into templates, service names in scripts).
SKIP_KEYS=(
    "REPO_OUTER_PATH"
    "REPO_INNER_PATH"       # paths appear in docs/templates — not secrets
    "SERVICE_RESTART_CMD"
    "BLUEPRINTS_DB_DIR"
    "GIT_USER_NAME"         # value matches the public repo name — not a secret leak
    "TAILSCALE_ACCEPT_DNS"  # value is "false" — too generic to scan for
    "TAILSCALE_EXIT_NODE"   # value is "true" — too generic to scan for
    "PROXMOX_SSH_KEY"       # standard path convention, present in onboarding templates
)

# Colours
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env not found at $ENV_FILE" >&2
    exit 1
fi

# Get the list of files committed (or staged) in the outer repo,
# excluding .env itself and anything inside .xarta/.
mapfile -t SCAN_FILES < <(
    git -C "$REPO_DIR" ls-files \
    | grep -v '^\(\.env\|\.xarta/\)' \
    | sed "s|^|$REPO_DIR/|"
)

if [ "${#SCAN_FILES[@]}" -eq 0 ]; then
    echo "No tracked files found to scan in $REPO_DIR"
    exit 1
fi

echo "Scanning ${#SCAN_FILES[@]} tracked file(s) for leaks..."
echo "Source .env:         $ENV_FILE"
echo "Source infra-leaks:  ${INFRA_LEAKS_FILE}"
echo ""

LEAKS=0
SKIPPED=0

# ---------------------------------------------------------------------------
# Pass 1: .env values
# ---------------------------------------------------------------------------
echo "=== Checking .env values ==="

while IFS= read -r line; do
    # Skip blank lines and comments
    [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue

    # Extract key and value (handles VAR=value and VAR="value")
    key="${line%%=*}"
    raw_value="${line#*=}"

    # Skip keys whose values are intentionally present in committed files
    skip=0
    for skip_key in "${SKIP_KEYS[@]}"; do
        [[ "$key" == "$skip_key" ]] && skip=1 && break
    done
    [ "$skip" -eq 1 ] && continue
    # Strip surrounding quotes
    value="${raw_value#\"}"
    value="${value%\"}"
    value="${value%%[[:space:]]*#*}"  # strip inline comments
    value="${value%"${value##*[![:space:]]}"}"  # rtrim whitespace

    # Skip empty values
    [ -z "$value" ] && continue

    # Skip values that are too short to search meaningfully
    if [ "${#value}" -lt "$MIN_LEN" ]; then
        (( SKIPPED++ ))
        continue
    fi

    # Search all tracked files for the value
    matches=$(grep -rn --fixed-strings -- "$value" "${SCAN_FILES[@]}" 2>/dev/null)

    if [ -n "$matches" ]; then
        echo -e "${RED}LEAK${NC}: $key=\"$value\""
        echo "$matches" | while IFS= read -r match; do
            # Print path relative to repo root
            rel="${match/$REPO_DIR\//}"
            echo "       $rel"
        done
        echo ""
        (( LEAKS++ ))
    fi

done < "$ENV_FILE"

# ---------------------------------------------------------------------------
# Pass 2: private infra patterns from infra-leaks.txt
# ---------------------------------------------------------------------------
echo ""
echo "=== Checking infrastructure patterns ==="

if [ ! -f "$INFRA_LEAKS_FILE" ]; then
    echo -e "${YELLOW}Warning:${NC} $INFRA_LEAKS_FILE not found — skipping infrastructure pattern check."
    echo "         (Clone the inner repo alongside this one to enable this check.)"
else
    echo "Loading patterns from: $INFRA_LEAKS_FILE"
    echo ""

    while IFS= read -r pattern; do
        # Skip blank lines and comments
        [[ -z "$pattern" || "$pattern" =~ ^[[:space:]]*# ]] && continue

        [ "${#pattern}" -lt "$MIN_LEN" ] && continue

        # Lines starting with ~ are treated as extended regex patterns
        if [[ "$pattern" == ~* ]]; then
            regex="${pattern:1}"
            matches=$(grep -rn -E -- "$regex" "${SCAN_FILES[@]}" 2>/dev/null)
            label="~${regex}"
        else
            matches=$(grep -rn --fixed-strings -- "$pattern" "${SCAN_FILES[@]}" 2>/dev/null)
            label="$pattern"
        fi
        if [ -n "$matches" ]; then
            echo -e "${RED}LEAK${NC}: \"$label\""
            echo "$matches" | while IFS= read -r match; do
                rel="${match/$REPO_DIR\//}"
                echo "       $rel"
            done
            echo ""
            (( LEAKS++ ))
        fi
    done < "$INFRA_LEAKS_FILE"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo "---"
if [ "$LEAKS" -gt 0 ]; then
    echo -e "${RED}${LEAKS} leak(s) found.${NC} Review the files above before pushing."
    exit 1
else
    echo -e "${GREEN}No leaks found.${NC}"
    if [ "$SKIPPED" -gt 0 ]; then
        echo -e "${YELLOW}Note:${NC} $SKIPPED value(s) skipped (shorter than ${MIN_LEN} chars — increase MIN_LEN if needed)."
    fi
    exit 0
fi
