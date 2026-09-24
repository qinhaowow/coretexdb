#!/usr/bin/env bash
set -euo pipefail
PREFIX="${PREFIX:-$(cd "$(dirname "$0")/.." && pwd)}"
BIN="$PREFIX/bin/coretexd"
[ -x "$BIN" ] || BIN="$PREFIX/bin/coretexd.exe"
exec "$BIN" --data-dir "$PREFIX" "$@"
