#!/usr/bin/env bash
# Restore from a backup directory.
# Usage: restore.sh <backup_parent_dir> <name>
#   <backup_parent_dir> is the directory that CONTAINS the snapshot named
#   <name> (default: <install>/data/backup/full).
# Destructive: existing data is moved to .pre-restore-<ts>/ first.
set -euo pipefail
PREFIX="${PREFIX:-$(cd "$(dirname "$0")/.." && pwd)}"
OUT="${1:?backup parent dir}"
NAME="${2:?backup name}"
BIN="$PREFIX/bin/coretex"
[ -x "$BIN" ] || BIN="$PREFIX/bin/coretex.exe"
exec "$BIN" restore --data-dir "$PREFIX" --input "$OUT" --name "$NAME" --force
