#!/usr/bin/env bash
# scripts/download-smithy-models.sh
# Downloads AWS Smithy JSON models from the aws-sdk-go-v2 repository.
#
# The models committed under smithy-models/ are the pin for `make codegen`:
# BASE_URL tracks aws-sdk-go-v2 *main*, so without them codegen output would
# depend on whatever upstream published that day. By default this script only
# fills in missing files, which keeps a local codegen run reproducible and
# offline. The weekly smithy-sync workflow passes --refresh so upstream changes
# land as a reviewable model diff next to the regenerated code.
#
# usage: download-smithy-models.sh [--refresh] [models-dir] [service...]
#   Service names are upstream model filenames without .json. With none given,
#   every model already in models-dir is processed — name a service explicitly
#   to add a new one.
set -euo pipefail

REFRESH=0
if [ "${1:-}" = "--refresh" ]; then
  REFRESH=1
  shift
fi

MODELS_DIR="${1:-./smithy-models}"
shift || true
BASE_URL="https://raw.githubusercontent.com/aws/aws-sdk-go-v2/main/codegen/sdk-codegen/aws-models"

mkdir -p "$MODELS_DIR"

# Deriving the service list from the directory keeps it from drifting out of
# sync with the models we actually generate from — a hand-maintained list grows
# entries whose upstream filename has since changed, and those 404 silently.
if [ "$#" -gt 0 ]; then
  SERVICES=("$@")
else
  SERVICES=()
  for f in "$MODELS_DIR"/*.json; do
    [ -e "$f" ] || continue
    SERVICES+=("$(basename "$f" .json)")
  done
fi

if [ "${#SERVICES[@]}" -eq 0 ]; then
  echo "No models in $MODELS_DIR and no service named; pass service names to seed it." >&2
  exit 1
fi

updated=0
failed=0
for service in "${SERVICES[@]}"; do
  dest="${MODELS_DIR}/${service}.json"
  if [ -f "$dest" ] && [ "$REFRESH" -eq 0 ]; then
    echo "SKIP $service (exists)"
    continue
  fi
  # Download to a temp file first: a failed refresh must never delete or
  # truncate a model that is already committed.
  tmp="${dest}.tmp"
  if curl -sfL "${BASE_URL}/${service}.json" -o "$tmp"; then
    if [ -f "$dest" ] && cmp -s "$tmp" "$dest"; then
      rm -f "$tmp"
    else
      mv "$tmp" "$dest"
      updated=$((updated + 1))
      echo "UPDATED $service"
    fi
  else
    rm -f "$tmp"
    failed=$((failed + 1))
    echo "WARN: failed to download $service, keeping any existing copy" >&2
  fi
done

# find, not `ls *.json`: under `set -e` + pipefail a glob that matches nothing
# makes ls exit 2 and kills the script right before it reports what happened.
total=$(find "$MODELS_DIR" -maxdepth 1 -name '*.json' | wc -l | tr -d ' ')
echo "Done. ${updated} updated, ${failed} failed, ${total} models total."

# Exit non-zero on any download failure. Keeping the existing copy is the right
# recovery, but staying green is not: the caller (the weekly sync workflow) would
# regenerate from stale models, find no diff, and report success having synced
# nothing — the exact silent no-op this script was fixed to stop doing.
if [ "$failed" -gt 0 ]; then
  echo "ERROR: ${failed} model(s) failed to download; models on disk are unchanged for those." >&2
  exit 1
fi
