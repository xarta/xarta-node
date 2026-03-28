#!/usr/bin/env bash

set -euo pipefail

PUBLIC_ROOT="${PUBLIC_ROOT:-/root/xarta-node}"
PRIVATE_ROOT="${PRIVATE_ROOT:-/root/xarta-node/.xarta}"

OLD_REPO_PATH="${OLD_REPO_PATH:-/root/xarta-node/.lone-wolf}"
OLD_CADDYFILE_PATH="${OLD_CADDYFILE_PATH:-/root/xarta-node/.lone-wolf/Caddyfile}"

have_rg=0
if command -v rg >/dev/null 2>&1; then
    have_rg=1
fi

scan_with_rg() {
    local label="$1"
    local root="$2"
    shift 2

    if [[ ! -d "$root" ]]; then
        echo "[$label] missing: $root"
        return 0
    fi

    rg \
        --color=never \
        --line-number \
        --with-filename \
        --hidden \
        --glob '!**/.git/**' \
        --glob '!check-old-lone-wolf-paths.sh' \
        --glob '!**/*.png' \
        --glob '!**/*.jpg' \
        --glob '!**/*.jpeg' \
        --glob '!**/*.gif' \
        --glob '!**/*.webp' \
        --glob '!**/*.svg' \
        --glob '!**/*.ico' \
        --glob '!**/*.pdf' \
        --glob '!**/*.db' \
        --glob '!**/*.sqlite*' \
        --glob '!**/*.zip' \
        --glob '!**/*.tar' \
        --glob '!**/*.gz' \
        --glob '!**/*.xz' \
        "$@" "$root" || true
}

scan_with_grep() {
    local label="$1"
    local root="$2"
    local pattern="$3"
    local exclude_dir="${4:-}"

    if [[ ! -d "$root" ]]; then
        echo "[$label] missing: $root"
        return 0
    fi

    local -a grep_args=(
        -rInF
        --exclude-dir=.git
        --exclude=check-old-lone-wolf-paths.sh
        --exclude='*.png'
        --exclude='*.jpg'
        --exclude='*.jpeg'
        --exclude='*.gif'
        --exclude='*.webp'
        --exclude='*.svg'
        --exclude='*.ico'
        --exclude='*.pdf'
        --exclude='*.db'
        --exclude='*.sqlite*'
        --exclude='*.zip'
        --exclude='*.tar'
        --exclude='*.gz'
        --exclude='*.xz'
    )

    if [[ -n "$exclude_dir" ]]; then
        grep_args+=("--exclude-dir=$exclude_dir")
    fi

    grep "${grep_args[@]}" \
        "$pattern" "$root" || true
}

scan_one_pattern() {
    local pattern_label="$1"
    local pattern="$2"
    local combined_output=""
    local output=""

    echo
    echo "== $pattern_label =="

    if [[ "$have_rg" -eq 1 ]]; then
        output="$(scan_with_rg public "$PUBLIC_ROOT" --glob '!.xarta/**' -F "$pattern")"
        if [[ -n "$output" ]]; then
            echo "[public]"
            echo "$output"
            combined_output+="$output"
        fi

        output="$(scan_with_rg private "$PRIVATE_ROOT" -F "$pattern")"
        if [[ -n "$output" ]]; then
            echo "[private]"
            echo "$output"
            combined_output+="$output"
        fi
    else
        output="$(scan_with_grep public "$PUBLIC_ROOT" "$pattern" .xarta)"
        if [[ -n "$output" ]]; then
            echo "[public]"
            echo "$output"
            combined_output+="$output"
        fi

        output="$(scan_with_grep private "$PRIVATE_ROOT" "$pattern")"
        if [[ -n "$output" ]]; then
            echo "[private]"
            echo "$output"
            combined_output+="$output"
        fi
    fi

    if [[ -z "$combined_output" ]]; then
        echo "No matches."
        return 1
    fi

    return 0
}

echo "Scanning for stale node-local Caddy path references..."
echo "Public repo:  $PUBLIC_ROOT"
echo "Private repo: $PRIVATE_ROOT"

found_any=0

if scan_one_pattern "Old Caddyfile path" "$OLD_CADDYFILE_PATH"; then
    found_any=1
fi

if scan_one_pattern "Old node-local repo path" "$OLD_REPO_PATH"; then
    found_any=1
fi

echo
if [[ "$found_any" -eq 1 ]]; then
    echo "Stale path references found."
    exit 1
fi

echo "No stale path references found."