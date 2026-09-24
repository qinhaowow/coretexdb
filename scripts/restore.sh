#!/usr/bin/env bash
# Restore from a backup directory.
# Usage: restore.sh <backup_output_dir> <name>
set -euo pipefail
PREFIX="${PREFIX:-$(cd "$(dirname "$0")/.." && pwd)}"
OUT="${1:?backup output dir}"
NAME="${2:?backup name}"
BIN="$PREFIX/bin/coretex-backup"
[ -x "$BIN" ] || BIN="$PREFIX/bin/coretex"
exec "$BIN" restore --data-dir "$PREFIX" --output "$OUT" --name "$NAME"
