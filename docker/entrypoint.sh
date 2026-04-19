#!/bin/sh
set -e

# Ensure the data directory exists and is writable by appuser.
mkdir -p /app/data
chown appuser:appuser /app/data

exec su-exec appuser "$@"
