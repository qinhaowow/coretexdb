#!/usr/bin/env bash
set -euo pipefail
PREFIX="${PREFIX:-$(cd "$(dirname "$0")/.." && pwd)}"
BIN="$PREFIX/bin/coretex-healthcheck"
[ -x "$BIN" ] || BIN="$PREFIX/bin/coretex"
exec "$BIN" doctor --data-dir "$PREFIX" "$@"
