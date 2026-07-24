#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: catalog-skills.sh [options]
       catalog-skills.sh [/root/xarta-node] [json|text]  # legacy form

List maintained common and overlay xarta-node skills through the shared p103 audit.

Options:
  --name NAME       Exact front-matter name filter; may return root variants
  --json            Print JSON (default; compatible with the former script)
  --text            Print a compact name/role/path table
  --output PATH     Full audit destination under /tmp
  -h, --help        Show this help

The command collects metadata only. It does not select a skill semantically.
EOF
}

AUDIT=/root/xarta-node/.xarta/.agents/bin/xarta-skill-audit
OUTPUT=/tmp/roo-skill-audit.json
FORMAT=json
NAME=

# Preserve the former no-argument and BASE_DIR/format calling shapes.
if [[ "${1:-}" == "/root/xarta-node" ]]; then
  shift
  case "${1:-}" in
    ""|json) FORMAT=json; [[ $# -gt 0 ]] && shift ;;
    text) FORMAT=text; shift ;;
    *) echo "Unsupported legacy format: $1" >&2; exit 2 ;;
  esac
fi

while (($#)); do
  case "$1" in
    --name) NAME=${2:-}; shift 2 ;;
    --json|json) FORMAT=json; shift ;;
    --text|text) FORMAT=text; shift ;;
    --output) OUTPUT=${2:-}; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if [[ ! -x "$AUDIT" ]]; then
  echo "Maintained skill audit helper is unavailable: $AUDIT" >&2
  exit 3
fi
if [[ "$OUTPUT" != /tmp/* ]]; then
  echo "--output must be under /tmp" >&2
  exit 2
fi

"$AUDIT" --maintained-workspace --output "$OUTPUT" --allow-findings >/dev/null

FILTER='[.skills[] | select(.path_role | endswith("canonical"))'
if [[ -n "$NAME" ]]; then
  FILTER+=" | select(.name == \$name)"
fi
FILTER+='] | sort_by(.name,.path_role,.path)'

if [[ "$FORMAT" == json ]]; then
  jq -c --arg name "$NAME" "$FILTER | map({name,path,resolved_path,description,repo:(if .path_role == \"shared-common-canonical\" then \"shared\" elif .path_role == \"private-canonical\" then \"p100\" elif .path_role == \"public-root-canonical\" then \"p200\" elif .path_role == \"public-nonroot-canonical\" then \"p300\" else \"p400\" end),type:.path_role})" "$OUTPUT"
else
  jq -r --arg name "$NAME" "$FILTER[] | [.name,.path_role,.path] | @tsv" "$OUTPUT"
  printf 'raw_audit\t%s\n' "$OUTPUT"
fi
