#!/bin/sh
set -e

# Ensure the data directory exists and is writable by appuser.
mkdir -p /app/data
owner=$(stat -c %U /app/data 2>/dev/null || echo "")
if [ "$owner" != "appuser" ]; then
    chown appuser:appuser /app/data
fi

exec su-exec appuser "$@"
