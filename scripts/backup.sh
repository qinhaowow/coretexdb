#!/usr/bin/env bash
# Full backup via coretex-backup / coretex backup.
set -euo pipefail
PREFIX="${PREFIX:-$(cd "$(dirname "$0")/.." && pwd)}"
OUT="${1:-$PREFIX/data/backup/full}"
NAME="${2:-backup_$(date -u +%Y%m%d_%H%M%S)}"
BIN="$PREFIX/bin/coretex"
[ -x "$BIN" ] || BIN="$PREFIX/bin/coretex.exe"
exec "$BIN" backup --data-dir "$PREFIX" --output "$OUT" --name "$NAME"
