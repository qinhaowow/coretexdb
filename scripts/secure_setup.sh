#!/usr/bin/env bash
# Apply security permission bits on an install root (spec §五).
# Usage: sudo scripts/secure_setup.sh [PREFIX]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# The version comes from the tree: the VERSION file that ships in the install
# root, or Cargo.toml when running from a checkout. It used to be a literal in
# this line, so every release had to edit three scripts to agree.
VERSION="$(cat "$ROOT/VERSION" 2>/dev/null || grep -m1 '^version = ' "$ROOT/Cargo.toml" 2>/dev/null | cut -d'"' -f2)"
[ -n "$VERSION" ] || { echo "cannot determine version from $ROOT" >&2; exit 1; }
PREFIX="${1:-/opt/CoreTexDB-V$VERSION}"
[ -d "$PREFIX" ] || { echo "missing prefix: $PREFIX" >&2; exit 1; }

# Groups/users are best-effort; fall back to current user if missing.
GROUP="coretex"
if ! getent group "$GROUP" >/dev/null 2>&1; then
  if command -v groupadd >/dev/null 2>&1 && [ "$(id -u)" -eq 0 ]; then
    groupadd --system "$GROUP"
  else
    GROUP="$(id -gn)"
  fi
fi

chown -R root:"$GROUP" "$PREFIX/bin" "$PREFIX/lib" "$PREFIX/scripts" 2>/dev/null || true
chmod 750 "$PREFIX/bin" "$PREFIX/lib" "$PREFIX/scripts" 2>/dev/null || true

chown -R root:root "$PREFIX/include" "$PREFIX/share" "$PREFIX/systemd" "$PREFIX/logrotate" 2>/dev/null || true
chmod 755 "$PREFIX/include" "$PREFIX/share" "$PREFIX/systemd" "$PREFIX/logrotate" 2>/dev/null || true

chown -R "$GROUP":"$GROUP" "$PREFIX/config" 2>/dev/null || true
find "$PREFIX/config" -type f -exec chmod 640 {} + 2>/dev/null || true
find "$PREFIX/config" -type d -exec chmod 750 {} + 2>/dev/null || true

if id coretex >/dev/null 2>&1; then
  chown -R coretex:"$GROUP" "$PREFIX/data" 2>/dev/null || true
else
  chown -R "$GROUP":"$GROUP" "$PREFIX/data" 2>/dev/null || true
fi
chmod 700 "$PREFIX/data" 2>/dev/null || true
find "$PREFIX/data/logs" -type d -exec chmod 750 {} + 2>/dev/null || true

chown -R root:root "$PREFIX/.deploy" 2>/dev/null || true
chmod 700 "$PREFIX/.deploy" 2>/dev/null || true
chmod 640 "$PREFIX/.deploy"/* 2>/dev/null || true

echo "secure_setup applied under $PREFIX"
