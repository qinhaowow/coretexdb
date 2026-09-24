#!/usr/bin/env bash
# Uninstall: stop service, remove install-root tree (keeps nothing under PREFIX unless -p).
set -euo pipefail
PREFIX="${1:-${PREFIX:-/opt/CoreTexDB-V0.2.1}}"
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
