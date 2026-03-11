#!/usr/bin/env bash

set -euo pipefail

ROOT="${1:-/root/xarta-node/.claude/skills}"

if [[ ! -d "$ROOT" ]]; then
  echo "ERROR: root not found: $ROOT"
  exit 2
fi

status=0

while IFS= read -r skill_file; do
  skill_dir="$(dirname "$skill_file")"
  rel="${skill_file#$ROOT/}"
  lines="$(wc -l < "$skill_file")"

  first_line="$(head -n 1 "$skill_file")"
  if [[ "$first_line" != "---" ]] || ! grep -q '^name:' "$skill_file" || ! grep -q '^description:' "$skill_file"; then
    echo "FAIL_FRONTMATTER $rel"
    status=1
  else
    echo "OK_FRONTMATTER $rel"
  fi

  if (( lines > 500 )); then
    echo "WARN_LINES_GT_500 $rel lines=$lines"
  else
    echo "OK_LINES $rel lines=$lines"
  fi

  while IFS= read -r path; do
    [[ -z "$path" ]] && continue
    cleaned="${path%%[\)\]\`\"\'\,]*}"
    if [[ ! -e "$cleaned" ]]; then
      echo "WARN_MISSING_PATH $rel -> $cleaned"
    fi
  done < <(grep -oE '/root/[A-Za-z0-9._/-]+' "$skill_file" | sort -u)

  while IFS= read -r relpath; do
    [[ -z "$relpath" ]] && continue
    target="$skill_dir/$relpath"
    if [[ ! -e "$target" ]]; then
      echo "WARN_MISSING_RELATIVE_PATH $rel -> $relpath"
    fi
  done < <(grep -oE '(references|scripts|assets)/[A-Za-z0-9._/-]+' "$skill_file" | sort -u)

  for subdir in scripts references assets; do
    if [[ -d "$skill_dir/$subdir" ]]; then
      echo "OK_DIR $rel has $subdir/"
    fi
  done
done < <(find "$ROOT" -mindepth 2 -maxdepth 2 -name SKILL.md | sort)

exit "$status"
