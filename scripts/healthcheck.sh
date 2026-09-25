#!/usr/bin/env bash
set -euo pipefail
PREFIX="${PREFIX:-$(cd "$(dirname "$0")/.." && pwd)}"
BIN="$PREFIX/bin/coretex"
[ -x "$BIN" ] || BIN="$PREFIX/bin/coretex.exe"
exec "$BIN" doctor --data-dir "$PREFIX" "$@"
