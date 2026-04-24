#!/usr/bin/env bash
# catalog-skills.sh — Scan all Claude skills and produce a JSON catalog
# Usage: bash catalog-skills.sh [base_dir]
#   base_dir defaults to /root/xarta-node
# Output: JSON array of {name, path, description, repo, type}

set -euo pipefail

BASE_DIR="${1:-/root/xarta-node}"
OUTPUT_FORMAT="${2:-json}"

declare -a SKILLS=()

scan_skills_dir() {
  local dir="$1"
  local repo="$2"
  local type="$3"

  for skill_dir in "$dir"/*/; do
    [ -d "$skill_dir" ] || continue
    local skill_md="$skill_dir/SKILL.md"
    [ -f "$skill_md" ] || continue

    local name
    name=$(grep -m1 '^name:' "$skill_md" 2>/dev/null | sed 's/^name:[[:space:]]*//' | tr -d '\r' || echo "unknown")
    local desc
    desc=$(grep -m1 '^description:' "$skill_md" 2>/dev/null | sed 's/^description:[[:space:]]*//' | tr -d '\r' || echo "")
    local rel_path
    rel_path=$(realpath --relative-to="$BASE_DIR" "$skill_dir" 2>/dev/null || echo "$skill_dir")

    SKILLS+=("{\"name\":\"$name\",\"path\":\"$rel_path\",\"description\":\"$desc\",\"repo\":\"$repo\",\"type\":\"$type\"}")
  done
}

# Scan private inner repo (p100)
scan_skills_dir "$BASE_DIR/.xarta/.claude/skills" "p100" "private-inner"

# Scan public root skills (p201)
scan_skills_dir "$BASE_DIR/.claude/skills" "p201" "public-root"

# Scan public non-root skills (p301) — may not exist
if [ -d "$BASE_DIR/.xarta-node/.claude/skills" ] 2>/dev/null || [ -d "/xarta-node/.claude/skills" ] 2>/dev/null; then
  local_path="/xarta-node/.claude/skills"
  if [ -d "$local_path" ]; then
    scan_skills_dir "$local_path" "p301" "public-non-root"
  fi
fi

# Scan node-local private skills (p401)
if [ -d "$BASE_DIR/.lone-wolf/.claude/skills" ]; then
  scan_skills_dir "$BASE_DIR/.lone-wolf/.claude/skills" "p401" "node-local"
fi

# Output JSON
if [ "$OUTPUT_FORMAT" = "json" ]; then
  echo "["
  for i in "${!SKILLS[@]}"; do
    if [ $i -lt $((${#SKILLS[@]} - 1)) ]; then
      echo "  ${SKILLS[$i]},"
    else
      echo "  ${SKILLS[$i]}"
    fi
  done
  echo "]"
else
  # Human-readable table
  printf "%-40s %-12s %-12s %s\n" "NAME" "PATH" "REPO" "TYPE"
  printf "%-40s %-12s %-12s %s\n" "----" "----" "----" "----"
  for s in "${SKILLS[@]}"; do
    name=$(echo "$s" | grep -oP '"name":"\K[^"]+')
    path=$(echo "$s" | grep -oP '"path":"\K[^"]+')
    repo=$(echo "$s" | grep -oP '"repo":"\K[^"]+')
    type=$(echo "$s" | grep -oP '"type":"\K[^"]+')
    printf "%-40s %-12s %-12s %s\n" "$name" "$path" "$repo" "$type"
  done
fi
