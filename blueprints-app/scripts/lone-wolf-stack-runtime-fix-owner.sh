#!/usr/bin/env bash
# Audit or repair known ownership sentinels under node-local stacks.

set -euo pipefail

LONE_WOLF_ROOT="${LONE_WOLF_ROOT:-/xarta-node/.lone-wolf}"
STACKS_DIR="${STACKS_DIR:-$LONE_WOLF_ROOT/stacks}"
MODE="check"
AUDIT_LEVEL="default"
VERBOSE="${VERBOSE:-0}"

usage() {
    cat <<'EOF'
Usage: lone-wolf-stack-runtime-fix-owner.sh [--check] [--apply] [--expanded] [--verbose]

The default is a bounded, read-only check of known service ownership sentinels.
--expanded recursively checks those service paths without crossing filesystems;
it is read-only and can be expensive.  --apply repairs only the known path
itself, never all of its descendants.  Source, docs, Syncthing trees, caches,
backups, databases, and other runtime state are not generically normalized.
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
        --expanded)
            AUDIT_LEVEL="expanded"
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

if [[ "$MODE" == "apply" && "$AUDIT_LEVEL" == "expanded" ]]; then
    echo "ERROR: expanded audit is read-only; run checking and repair separately" >&2
    exit 2
fi

[[ -d "$STACKS_DIR" ]] || exit 0

drift=0
changed=0

repair_path() {
    local path="$1"
    local owner="$2"
    local label="$3"
    local mode="${4:-}"

    [[ -e "$path" ]] || return 0

    local owner_drift=0
    local mode_drift=0

    if [[ "$AUDIT_LEVEL" == "expanded" ]]; then
        if find -P "$path" -xdev \( ! -uid "${owner%:*}" -o ! -gid "${owner#*:}" \) -print -quit 2>/dev/null | grep -q .; then
            owner_drift=1
        fi
        if [[ -n "$mode" ]]; then
            while IFS= read -r -d '' item; do
                [[ "$(stat -c '%a' -- "$item")" == "$mode" ]] || {
                    mode_drift=1
                    break
                }
            done < <(find -P "$path" -xdev -print0 2>/dev/null)
        fi
    else
        if [[ "$(stat -c '%u:%g' -- "$path")" != "$owner" ]]; then
            owner_drift=1
        fi
        if [[ -n "$mode" && "$(stat -c '%a' -- "$path")" != "$mode" ]]; then
            mode_drift=1
        fi
    fi

    if [[ "$owner_drift" == "0" && "$mode_drift" == "0" ]]; then
        [[ "$VERBOSE" == "1" ]] && echo "OK: $label $path owner=$owner${mode:+ mode=$mode}"
        return 0
    fi

    drift=$((drift + 1))
    if [[ "$MODE" == "check" ]]; then
        echo "DRIFT: $label $path expected_owner=$owner${mode:+ expected_mode=$mode}"
        return 0
    fi

    # Apply is intentionally sentinel-only.  Distribution workflows must use
    # their exact touched-path manifest for copied/replaced descendants.
    [[ "$owner_drift" == "1" ]] && chown --no-dereference "$owner" "$path"
    if [[ "$mode_drift" == "1" ]]; then
        chmod "$mode" "$path"
    fi
    changed=$((changed + 1))
    [[ "$VERBOSE" == "1" ]] && echo "FIXED: $label $path owner=$owner${mode:+ mode=$mode}"
}

repair_path_self() {
    local path="$1"
    local owner="$2"
    local label="$3"
    local mode="${4:-}"

    [[ -e "$path" ]] || return 0

    local owner_drift=0
    local mode_drift=0
    local uid="${owner%:*}"
    local gid="${owner#*:}"

    if [[ "$(stat -c '%u:%g' "$path")" != "$uid:$gid" ]]; then
        owner_drift=1
    fi

    if [[ -n "$mode" && "$(stat -c '%a' "$path")" != "$mode" ]]; then
        mode_drift=1
    fi

    if [[ "$owner_drift" == "0" && "$mode_drift" == "0" ]]; then
        [[ "$VERBOSE" == "1" ]] && echo "OK: $label $path owner=$owner${mode:+ mode=$mode}"
        return 0
    fi

    drift=$((drift + 1))
    if [[ "$MODE" == "check" ]]; then
        echo "DRIFT: $label $path expected_owner=$owner${mode:+ expected_mode=$mode}"
        return 0
    fi

    if [[ "$owner_drift" == "1" ]]; then
        chown --no-dereference "$owner" "$path"
    fi
    if [[ "$mode_drift" == "1" ]]; then
        chmod "$mode" "$path"
    fi
    changed=$((changed + 1))
    [[ "$VERBOSE" == "1" ]] && echo "FIXED: $label $path owner=$owner${mode:+ mode=$mode}"
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

    if grep -Eq 'image:[[:space:]]*"?([^"[:space:]]*/)?valkey/valkey:[^"[:space:]]*-alpine' "$compose"; then
        if grep -Eq '\./data/valkey:/data' "$compose"; then
            repair_path "$stack_dir/data/valkey" "999:1000" "$stack_name valkey-alpine"
        fi
    fi

    if grep -Eq 'image:[[:space:]]*"?unclecode/crawl4ai:' "$compose"; then
        if grep -Eq '\./data/output:/app/output' "$compose"; then
            repair_path "$stack_dir/data/output" "999:999" "$stack_name crawl4ai-output"
        fi
    fi

    if grep -Eq 'user:[[:space:]]*"?65534:65534"?' "$compose"; then
        if grep -Eq '\./data:/data' "$compose"; then
            repair_path "$stack_dir/data" "65534:65534" "$stack_name nobody-data"
        fi
    fi

    if grep -Eq 'image:[[:space:]]*"?xarta/system-bridge-notifier:' "$compose"; then
        if grep -Eq '\./data:/data' "$compose"; then
            repair_path "$stack_dir/data" "65534:65534" "$stack_name system-bridge-notifier-data"
        fi
    fi

    if grep -Eq 'image:[[:space:]]*"?([^"[:space:]]*/)?matrixdotorg/synapse([:@][^"[:space:]]*)?' "$compose"; then
        if grep -Eq '\./data:/data' "$compose"; then
            synapse_config="$stack_dir/data/homeserver.yaml"
            repair_path "$synapse_config" "991:991" "$stack_name synapse-config" "600"
            media_store_path=""
            signing_key_path=""
            if [[ -r "$synapse_config" ]]; then
                media_store_path="$(sed -nE 's/^[[:space:]]*media_store_path:[[:space:]]*"?([^"#]+)"?[[:space:]]*(#.*)?$/\1/p' "$synapse_config" | head -n 1)"
                media_store_path="${media_store_path%\"}"
                signing_key_path="$(sed -nE 's/^[[:space:]]*signing_key_path:[[:space:]]*"?([^"#]+)"?[[:space:]]*(#.*)?$/\1/p' "$synapse_config" | head -n 1)"
                signing_key_path="${signing_key_path%\"}"
            fi
            if [[ "$media_store_path" == /data/* ]]; then
                repair_path_self "$stack_dir/data/${media_store_path#/data/}" "991:991" "$stack_name synapse-media-store" "750"
            else
                repair_path_self "$stack_dir/data/media_store" "991:991" "$stack_name synapse-media-store" "750"
            fi
            if [[ "$signing_key_path" == /data/* ]]; then
                repair_path "$stack_dir/data/${signing_key_path#/data/}" "991:991" "$stack_name synapse-signing-key" "600"
            else
                for signing_key in "$stack_dir"/data/*.signing.key; do
                    [[ -e "$signing_key" ]] || continue
                    repair_path "$signing_key" "991:991" "$stack_name synapse-signing-key" "600"
                done
            fi
        fi
    fi
done

if [[ "$MODE" == "check" && "$drift" -gt 0 ]]; then
    exit 1
fi

[[ "$VERBOSE" == "1" ]] && echo "runtime_owner_guard mode=$MODE audit_level=$AUDIT_LEVEL drift=$drift changed=$changed"
