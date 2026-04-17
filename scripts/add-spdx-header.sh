#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Ensures every Go source file has the SPDX license identifier at the top.
# - For plain files: SPDX is line 1, followed by a blank line.
# - For codegen output: the "// Code generated ... DO NOT EDIT." marker stays
#   on line 1 and SPDX is inserted on line 2.
#
# Usage:
#   scripts/add-spdx-header.sh [file1.go file2.go ...]
#     — adds headers to the given files (used by pre-commit).
#
#   scripts/add-spdx-header.sh
#     — scans the whole repo (excluding vendored and build-output paths)
#       and adds headers where missing.
#
# Exit code: 0 if all files already had (or now have) the header.

set -euo pipefail

SPDX_LINE="// SPDX-License-Identifier: Apache-2.0"

add_header() {
  local file="$1"

  # Skip if header already present anywhere in the first 5 lines.
  if head -5 "$file" 2>/dev/null | grep -q "SPDX-License-Identifier"; then
    return 0
  fi

  local first_line
  first_line=$(head -1 "$file" 2>/dev/null || true)
  local tmp
  tmp="$(mktemp)"

  if [[ "$first_line" == "// Code generated"* ]]; then
    # Preserve "Code generated" marker on line 1, insert SPDX on line 2.
    {
      printf '%s\n' "$first_line"
      printf '%s\n' "$SPDX_LINE"
      tail -n +2 "$file"
    } >"$tmp"
  else
    # Prepend SPDX + blank line.
    {
      printf '%s\n\n' "$SPDX_LINE"
      cat "$file"
    } >"$tmp"
  fi

  # Preserve the original file's mode so editors/CI don't see spurious
  # permission changes from mktemp's default 0600.
  if command -v stat >/dev/null 2>&1; then
    if mode=$(stat -f '%Lp' "$file" 2>/dev/null || stat -c '%a' "$file" 2>/dev/null); then
      chmod "$mode" "$tmp" 2>/dev/null || true
    fi
  fi
  mv "$tmp" "$file"
  echo "added SPDX header: $file"
}

if [[ $# -gt 0 ]]; then
  for f in "$@"; do
    [[ "$f" == *.go ]] || continue
    [[ -f "$f" ]] || continue
    add_header "$f"
  done
else
  # Whole-repo scan.
  while IFS= read -r f; do
    add_header "$f"
  done < <(
    find . -type f -name "*.go" \
      -not -path "./web/node_modules/*" \
      -not -path "./.git/*" \
      -not -path "./dist/*" \
      -not -path "./.worktrees/*"
  )
fi
