#!/usr/bin/env bash
# Uninstall: stop service, remove install-root tree (keeps nothing under PREFIX unless -p).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# The version comes from the tree: the VERSION file that ships in the install
# root, or Cargo.toml when running from a checkout. It used to be a literal in
# this line, so every release had to edit three scripts to agree.
VERSION="$(cat "$ROOT/VERSION" 2>/dev/null || grep -m1 '^version = ' "$ROOT/Cargo.toml" 2>/dev/null | cut -d'"' -f2)"
[ -n "$VERSION" ] || { echo "cannot determine version from $ROOT" >&2; exit 1; }
PREFIX="${1:-${PREFIX:-/opt/CoreTexDB-V$VERSION}}"
PURGE_DATA=0
[ "${2:-}" = "-p" ] || [ "${1:-}" = "-p" ] && PURGE_DATA=1

if [ -x "$PREFIX/scripts/stop.sh" ]; then
  "$PREFIX/scripts/stop.sh" || true
fi
if command -v systemctl >/dev/null 2>&1; then
  systemctl disable --now coretexd.service 2>/dev/null || true
fi

if [ "$PURGE_DATA" -eq 1 ]; then
  rm -rf "$PREFIX"
  echo "removed $PREFIX (including data)"
else
  rm -rf "$PREFIX/bin" "$PREFIX/lib" "$PREFIX/include" "$PREFIX/config" \
         "$PREFIX/scripts" "$PREFIX/systemd" "$PREFIX/logrotate" "$PREFIX/share" \
         "$PREFIX/VERSION" "$PREFIX/RELEASE_NOTES.md" "$PREFIX/LICENSE"
  echo "removed program files under $PREFIX; data/ preserved"
fi
