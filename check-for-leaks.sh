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

# Well-known public values that should never be flagged as leaks regardless of
# which .env key they appear under (e.g. public DNS IPs used in health checks,
# connectivity probes, and documentation examples).
SKIP_VALUES=(
    "1.1.1.1"       # Cloudflare public DNS
    "1.0.0.1"       # Cloudflare public DNS (secondary)
    "8.8.8.8"       # Google public DNS
    "8.8.4.4"       # Google public DNS (secondary)
    "9.9.9.9"       # Quad9 public DNS
    "149.112.112.112" # Quad9 secondary
    "208.67.222.222"  # OpenDNS
    "208.67.220.220"  # OpenDNS secondary
    "0.0.0.0"       # placeholder / unspecified address
    "127.0.0.1"     # loopback — not a real infra address
    "::1"           # IPv6 loopback
)

# Keys whose values are intentionally referenced in committed files and should
# not be treated as leaks (e.g. paths baked into templates, service names in scripts).
SKIP_KEYS=(
    "REPO_OUTER_PATH"
    "REPO_INNER_PATH"           # paths appear in docs/templates — not secrets
    "SERVICE_RESTART_CMD"
    "BLUEPRINTS_DB_DIR"
    "GIT_USER_NAME"             # value matches the public repo name — not a secret leak
    "TAILSCALE_ACCEPT_DNS"      # value is "false" — too generic to scan for
    "TAILSCALE_EXIT_NODE"       # value is "true" — too generic to scan for
    "PROXMOX_SSH_KEY"           # standard path convention, present in onboarding templates
    "NODES_JSON_PATH"           # default path is a public convention — not a secret
    "BLUEPRINTS_GUI_DIR"        # derived from REPO_INNER_PATH — documented public structure
    "BLUEPRINTS_BACKUP_DIR"     # derived from REPO_INNER_PATH — documented public structure
    "CERTS_DIR"                 # derived from REPO_INNER_PATH — documented public structure
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

    # Skip values that are well-known public addresses / not real infra secrets
    skip_val=0
    for sv in "${SKIP_VALUES[@]}"; do
        [[ "$value" == "$sv" ]] && skip_val=1 && break
    done
    [ "$skip_val" -eq 1 ] && continue

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
# Pass 3: .nodes.json values (IPs and hostnames)
# ---------------------------------------------------------------------------
echo ""
echo "=== Checking .nodes.json values ==="

NODES_JSON=""
if [ -f "$ENV_FILE" ]; then
    NODES_JSON="$(grep -E '^NODES_JSON_PATH=' "$ENV_FILE" 2>/dev/null \
        | head -1 | sed 's/^NODES_JSON_PATH=//' | tr -d '"' | tr -d "'" || true)"
fi
: "${NODES_JSON:=$REPO_DIR/.nodes.json}"

if [ ! -f "$NODES_JSON" ]; then
    echo -e "${YELLOW}Warning:${NC} $NODES_JSON not found — skipping .nodes.json value check."
else
    echo "Loading values from: $NODES_JSON"
    echo ""

    # Extract every IP and hostname from .nodes.json
    mapfile -t JSON_VALUES < <(python3 - "$NODES_JSON" <<'PYEOF'
import json, sys
data = json.load(open(sys.argv[1]))
seen = set()
for n in data.get("nodes", []):
    for field in ("primary_ip", "primary_hostname", "tailnet_ip", "tailnet_hostname"):
        v = n.get(field, "").strip()
        if v and v not in seen:
            seen.add(v)
            print(v)
PYEOF
    )

    for jval in "${JSON_VALUES[@]}"; do
        [ "${#jval}" -lt "$MIN_LEN" ] && continue
        matches=$(grep -rn --fixed-strings -- "$jval" "${SCAN_FILES[@]}" 2>/dev/null)
        if [ -n "$matches" ]; then
            echo -e "${RED}LEAK${NC}: .nodes.json value \"$jval\""
            echo "$matches" | while IFS= read -r match; do
                rel="${match/$REPO_DIR\//}"
                echo "       $rel"
            done
            echo ""
            (( LEAKS++ ))
        fi
    done
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
