#!/usr/bin/env bash

set -euo pipefail

PUBLIC_ROOT="${PUBLIC_ROOT:-/root/xarta-node}"
PRIVATE_ROOT="${PRIVATE_ROOT:-/root/xarta-node/.xarta}"
NODE_LOCAL_PARENT="${NODE_LOCAL_PARENT:-/xarta-node}"
NODE_LOCAL_ROOT="${NODE_LOCAL_ROOT:-${NODE_LOCAL_PARENT}/.lone-wolf}"
AUDIT_ARCHIVE_ROOT="${AUDIT_ARCHIVE_ROOT:-${PRIVATE_ROOT}/.audit/gui-migration-paths}"
TMP_REPORT=""
PREVIOUS_LATEST=""

have_rg=0
if command -v rg >/dev/null 2>&1; then
    have_rg=1
fi

REPOS=(
    "public:${PUBLIC_ROOT}"
    "private:${PRIVATE_ROOT}"
    "node-local:${NODE_LOCAL_ROOT}"
)

PATTERNS=(
    "Old public gui-fallback root|/root/xarta-node/gui-fallback"
    "Old public gui-fallback assets root|/root/xarta-node/gui-fallback/assets"
    "Old public shared gui-db root|/root/xarta-node/gui-db"
    "Old public gui-embed root|/root/xarta-node/gui-embed"
    "Old private gui root|/root/xarta-node/.xarta/gui"
    "Old private gui db path|/root/xarta-node/.xarta/gui/db"
    "Future non-root gui-fallback root|/xarta-node/gui-fallback"
    "Future non-root gui root|/xarta-node/gui"
    "Future non-root gui db path|/xarta-node/gui/db"
    "Future non-root gui-fallback db path|/xarta-node/gui-fallback/db"
    "Future non-root gui-fallback assets path|/xarta-node/gui-fallback/assets"
    "Relative gui-fallback path|gui-fallback/"
    "Relative gui-fallback db path|gui-fallback/db"
    "Relative gui-fallback assets path|gui-fallback/assets"
    "Relative shared gui-db path|gui-db/"
    "Relative gui-embed path|gui-embed/"
    "Relative private gui path|.xarta/gui"
    "Relative private gui db path|.xarta/gui/db"
    "Relative future gui db path|gui/db"
)

rg_scan() {
    local pattern="$1"
    local root="$2"

    rg \
        --color=never \
        --line-number \
        --with-filename \
        --hidden \
        -uuu \
        --glob '!**/.git/**' \
        --glob '!**/.audit/**' \
        --glob '!check-gui-migration-paths.sh' \
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
        --fixed-strings \
        -- "$pattern" "$root" || true
}

grep_scan() {
    local pattern="$1"
    local root="$2"

    grep \
        -rInF \
        --binary-files=without-match \
        --exclude-dir=.git \
        --exclude-dir=.audit \
        --exclude=check-gui-migration-paths.sh \
        --exclude='*.png' \
        --exclude='*.jpg' \
        --exclude='*.jpeg' \
        --exclude='*.gif' \
        --exclude='*.webp' \
        --exclude='*.svg' \
        --exclude='*.ico' \
        --exclude='*.pdf' \
        --exclude='*.db' \
        --exclude='*.sqlite*' \
        --exclude='*.zip' \
        --exclude='*.tar' \
        --exclude='*.gz' \
        --exclude='*.xz' \
        -- "$pattern" "$root" || true
}

scan_repo_for_pattern() {
    local pattern="$1"
    local root="$2"

    if [[ ! -d "$root" ]]; then
        return 0
    fi

    if [[ "$have_rg" -eq 1 ]]; then
        rg_scan "$pattern" "$root"
    else
        grep_scan "$pattern" "$root"
    fi
}

report_symlink() {
    local path="$1"

    if [[ -L "$path" ]]; then
        printf '%s -> %s\n' "$path" "$(readlink -f "$path")"
        return
    fi

    if [[ -e "$path" ]]; then
        printf '%s (not a symlink)\n' "$path"
        return
    fi

    printf '%s (missing)\n' "$path"
}

cleanup() {
    rm -f "${TMP_REPORT:-}" "${PREVIOUS_LATEST:-}"
}

normalize_report_for_diff() {
    local src="$1"
    local dest="$2"

    grep -v '^Run timestamp (UTC): ' "$src" > "$dest"
}

generate_report() {
    local run_ts="$1"

    echo "GUI migration path audit"
    echo "Run timestamp (UTC): $run_ts"
    echo "Public repo:         $PUBLIC_ROOT"
    echo "Private repo:        $PRIVATE_ROOT"
    echo "Node-local repo:     $NODE_LOCAL_ROOT"
    echo "Audit archive root:  $AUDIT_ARCHIVE_ROOT"
    echo
    echo "This scan includes hidden and gitignored files, but excludes .git internals, the audit archive, and common binary blobs."
    echo
    echo "== Current live symlink/layout check =="
    report_symlink "$PUBLIC_ROOT/gui-fallback/db"
    report_symlink "$PUBLIC_ROOT/gui-fallback/embed"
    report_symlink "$PUBLIC_ROOT/gui-db"
    report_symlink "$PRIVATE_ROOT/gui/db"
    report_symlink "$PRIVATE_ROOT/gui/embed"

    found_patterns=0
    total_matches=0

    for entry in "${PATTERNS[@]}"; do
        label="${entry%%|*}"
        pattern="${entry#*|}"

        echo
        echo "== $label =="
        echo "Pattern: $pattern"

        pattern_matches=0
        pattern_found=0

        for repo in "${REPOS[@]}"; do
            repo_name="${repo%%:*}"
            repo_root="${repo#*:}"

            if [[ ! -d "$repo_root" ]]; then
                echo "[$repo_name] missing: $repo_root"
                continue
            fi

            output="$(scan_repo_for_pattern "$pattern" "$repo_root")"
            if [[ -z "$output" ]]; then
                echo "[$repo_name] matches: 0"
                continue
            fi

            repo_matches="$(printf '%s\n' "$output" | wc -l | tr -d ' ')"
            echo "[$repo_name] matches: $repo_matches"
            echo "$output"
            pattern_matches=$((pattern_matches + repo_matches))
            total_matches=$((total_matches + repo_matches))
            pattern_found=1
        done

        echo "Total matches for pattern: $pattern_matches"

        if [[ "$pattern_found" -eq 1 ]]; then
            found_patterns=$((found_patterns + 1))
        fi
    done

    echo
    echo "Patterns with matches: $found_patterns"
    echo "Total matches across all patterns: $total_matches"
    echo
    echo "Interpretation note:"
    echo "- /root/xarta-node matches are expected today for the active public repo."
    echo "- /xarta-node matches indicate places already prepared for the non-root public repo path."
    echo "- Relative path matches need manual review to decide whether they are operational, informational, or safe to leave unchanged."
}

main() {
    local run_ts
    local report_path
    local latest_path
    local previous_path
    local diff_path
    local latest_diff_path
    local latest_meta_path
    local diff_status
    local normalized_previous
    local normalized_current

    run_ts="$(date -u +%Y%m%dT%H%M%SZ)"
    TMP_REPORT="$(mktemp)"

    mkdir -p "$AUDIT_ARCHIVE_ROOT"

    latest_path="$AUDIT_ARCHIVE_ROOT/latest.txt"
    previous_path="$AUDIT_ARCHIVE_ROOT/previous.txt"
    report_path="$AUDIT_ARCHIVE_ROOT/report-$run_ts.txt"
    diff_path="$AUDIT_ARCHIVE_ROOT/diff-from-previous-$run_ts.diff"
    latest_diff_path="$AUDIT_ARCHIVE_ROOT/latest.diff"
    latest_meta_path="$AUDIT_ARCHIVE_ROOT/latest.meta"
    PREVIOUS_LATEST=""
    normalized_previous=""
    normalized_current=""

    if [[ -f "$latest_path" ]]; then
        PREVIOUS_LATEST="$(mktemp)"
        cp "$latest_path" "$PREVIOUS_LATEST"
    fi

    generate_report "$run_ts" | tee "$TMP_REPORT"

    cp "$TMP_REPORT" "$report_path"
    cp "$TMP_REPORT" "$latest_path"

    diff_status="first-run"
    if [[ -n "$PREVIOUS_LATEST" ]]; then
        cp "$PREVIOUS_LATEST" "$previous_path"
        normalized_previous="$(mktemp)"
        normalized_current="$(mktemp)"
        normalize_report_for_diff "$PREVIOUS_LATEST" "$normalized_previous"
        normalize_report_for_diff "$TMP_REPORT" "$normalized_current"

        if diff -u --label previous --label current "$normalized_previous" "$normalized_current" > "$diff_path"; then
            diff_status="unchanged"
        else
            diff_status="changed"
        fi
        cp "$diff_path" "$latest_diff_path"
        rm -f "$normalized_previous" "$normalized_current"
    else
        printf 'No previous report was available.\n' > "$diff_path"
        cp "$diff_path" "$latest_diff_path"
    fi

    cat > "$latest_meta_path" <<EOF
run_timestamp_utc=$run_ts
report_path=$report_path
latest_path=$latest_path
previous_path=$previous_path
latest_diff_path=$latest_diff_path
diff_status=$diff_status
EOF

    echo
    echo "== Audit archive =="
    echo "Saved report: $report_path"
    echo "Latest report: $latest_path"
    echo "Previous report: $previous_path"
    echo "Diff vs previous: $latest_diff_path"
    echo "Diff status: $diff_status"
    echo "Metadata: $latest_meta_path"
}

trap cleanup EXIT

main "$@"