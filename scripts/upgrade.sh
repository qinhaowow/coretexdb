#!/usr/bin/env bash
# Upgrade in place: stop, install new version over PREFIX, start.
set -euo pipefail
PREFIX="${PREFIX:-$(cd "$(dirname "$0")/.." && pwd)}"
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
"$PREFIX/scripts/stop.sh" || true
"$ROOT/scripts/install.sh" "$PREFIX"
"$PREFIX/scripts/start.sh"
echo "upgraded to $(cat "$PREFIX/VERSION")"
