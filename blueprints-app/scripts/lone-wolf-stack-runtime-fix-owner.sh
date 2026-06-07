#!/usr/bin/env bash
# Repair known container-runtime ownership exceptions under node-local stacks.

set -euo pipefail

LONE_WOLF_ROOT="${LONE_WOLF_ROOT:-/xarta-node/.lone-wolf}"
STACKS_DIR="${STACKS_DIR:-$LONE_WOLF_ROOT/stacks}"
MODE="apply"
VERBOSE="${VERBOSE:-0}"

usage() {
    cat <<'EOF'
Usage: lone-wolf-stack-runtime-fix-owner.sh [--check] [--apply] [--verbose]

Repairs known Postgres and Redis runtime mount ownership under /xarta-node/.lone-wolf/stacks.
It intentionally does not normalize source/docs ownership.
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --check)
            MODE="check"
            ;;
        --apply)
            MODE="apply"
            ;;
        --verbose)
            VERBOSE="1"
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "ERROR: unknown argument: $1" >&2
            usage >&2
            exit 2
            ;;
    esac
    shift
done

[[ -d "$STACKS_DIR" ]] || exit 0

drift=0
changed=0

repair_path() {
    local path="$1"
    local owner="$2"
    local label="$3"

    [[ -e "$path" ]] || return 0

    if ! find "$path" -xdev \( ! -uid "${owner%:*}" -o ! -gid "${owner#*:}" \) -print -quit 2>/dev/null | grep -q .; then
        [[ "$VERBOSE" == "1" ]] && echo "OK: $label $path owner=$owner"
        return 0
    fi

    drift=$((drift + 1))
    if [[ "$MODE" == "check" ]]; then
        echo "DRIFT: $label $path expected_owner=$owner"
        return 0
    fi

    find "$path" -xdev \( ! -uid "${owner%:*}" -o ! -gid "${owner#*:}" \) -exec chown "$owner" {} +
    changed=$((changed + 1))
    [[ "$VERBOSE" == "1" ]] && echo "FIXED: $label $path owner=$owner"
}

for compose in "$STACKS_DIR"/*/compose.yaml; do
    [[ -f "$compose" ]] || continue
    stack_dir="$(dirname "$compose")"
    stack_name="$(basename "$stack_dir")"

    if grep -Eq 'image:[[:space:]]*"?([^"[:space:]]*/)?postgres:[^"[:space:]]*-alpine' "$compose"; then
        if grep -Eq '\./data/postgres:/var/lib/postgresql/data' "$compose"; then
            repair_path "$stack_dir/data/postgres" "70:70" "$stack_name postgres-alpine"
        fi
        if grep -Eq '\./db:/var/lib/postgresql(:|[[:space:]]|$)' "$compose"; then
            repair_path "$stack_dir/db" "70:70" "$stack_name postgres-alpine"
        fi
    fi

    if grep -Eq 'image:[[:space:]]*"?pgvector/pgvector:pg16' "$compose"; then
        if grep -Eq '\./data/postgres:/var/lib/postgresql/data' "$compose"; then
            repair_path "$stack_dir/data/postgres" "999:999" "$stack_name pgvector-pg16"
        fi
    fi

    if grep -Eq 'image:[[:space:]]*"?([^"[:space:]]*/)?redis:[^"[:space:]]*-alpine' "$compose"; then
        if grep -Eq '\./data/redis:/data' "$compose"; then
            repair_path "$stack_dir/data/redis" "999:1000" "$stack_name redis-alpine"
        fi
    fi
done

if [[ "$MODE" == "check" && "$drift" -gt 0 ]]; then
    exit 1
fi

[[ "$VERBOSE" == "1" ]] && echo "runtime_owner_guard mode=$MODE drift=$drift changed=$changed"
