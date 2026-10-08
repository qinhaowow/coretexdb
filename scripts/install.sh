#!/usr/bin/env bash
# Install CoreTexDB into an install root (PREFIX).
# Usage: scripts/install.sh [PREFIX]   (default: /opt/CoreTexDB-V<version>)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# The version comes from the tree: the VERSION file that ships in the install
# root, or Cargo.toml when running from a checkout. It used to be a literal in
# this line, so every release had to edit three scripts to agree.
VERSION="$(cat "$ROOT/VERSION" 2>/dev/null || grep -m1 '^version = ' "$ROOT/Cargo.toml" 2>/dev/null | cut -d'"' -f2)"
[ -n "$VERSION" ] || { echo "cannot determine version from $ROOT" >&2; exit 1; }
PREFIX="${1:-/opt/CoreTexDB-V$VERSION}"
BIN_SRC="${BIN_SRC:-$ROOT/target/release}"

echo "Installing CoreTexDB $VERSION → $PREFIX"

mkdir -p "$PREFIX"/{bin,lib,include,config/{dev,staging,prod},share/{doc,examples/{rust,cpp}},scripts,systemd,logrotate,data/{coretex/{collections,indexes/{vector,scalar},metadata,store},wal,backup/{full,incremental,snapshots},logs/audit,temp,versions},.deploy}

# Static assets from the source tree
cp -f "$ROOT/VERSION" "$PREFIX/VERSION"
cp -f "$ROOT/RELEASE_NOTES.md" "$PREFIX/RELEASE_NOTES.md" 2>/dev/null || true
cp -f "$ROOT/LICENSE" "$PREFIX/LICENSE"
cp -a "$ROOT/config/." "$PREFIX/config/"
cp -a "$ROOT/scripts/." "$PREFIX/scripts/"
cp -a "$ROOT/systemd/." "$PREFIX/systemd/"
cp -a "$ROOT/logrotate/." "$PREFIX/logrotate/"

# The units name the install root literally, and `ExecStart=` is executed by
# systemd rather than read: copied verbatim, a tree installed anywhere but the
# default produced a unit pointing at a directory that does not exist, and the
# service failed to start with no obvious cause. Rewriting them here is also
# what makes an in-place upgrade work — upgrade.sh reuses the old prefix, so the
# freshly copied units are renamed from the new default onto it.
DEFAULT_PREFIX="/opt/CoreTexDB-V$VERSION"
if [ "$PREFIX" != "$DEFAULT_PREFIX" ]; then
    echo "Rewriting unit paths: $DEFAULT_PREFIX -> $PREFIX"
    find "$PREFIX/systemd" "$PREFIX/logrotate" -type f \
        -exec sed -i "s|$DEFAULT_PREFIX|$PREFIX|g" {} +
fi
cp -a "$ROOT/share/." "$PREFIX/share/"
cp -a "$ROOT/include/." "$PREFIX/include/" 2>/dev/null || true
chmod 755 "$PREFIX/scripts/"*.sh 2>/dev/null || true
ln -sfn ../include/coretexdb.h "$PREFIX/lib/coretexdb.h"

# Binaries
for b in coretex; do
  if [ -x "$BIN_SRC/$b" ]; then
    install -m 750 "$BIN_SRC/$b" "$PREFIX/bin/$b"
  elif [ -x "$BIN_SRC/$b.exe" ]; then
    install -m 750 "$BIN_SRC/$b.exe" "$PREFIX/bin/$b.exe"
  else
    echo "warn: missing binary $b (build with cargo build --release)" >&2
  fi
done

# Libraries (if present)
for lib in libcoretexdb.so libcoretexdb.a; do
  if [ -f "$BIN_SRC/$lib" ]; then
    install -m 750 "$BIN_SRC/$lib" "$PREFIX/lib/$lib"
  fi
done

# .deploy state
date -u +%Y-%m-%dT%H:%M:%SZ > "$PREFIX/.deploy/installed_at"
id -un > "$PREFIX/.deploy/installed_by"
echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) install $VERSION to $PREFIX" >> "$PREFIX/.deploy/deployment.log"

# Permissions (best-effort; secure_setup.sh for full enforcement)
chmod 750 "$PREFIX/bin" "$PREFIX/lib" "$PREFIX/scripts" 2>/dev/null || true
chmod 755 "$PREFIX/include" "$PREFIX/share" "$PREFIX/systemd" "$PREFIX/logrotate" 2>/dev/null || true
chmod 640 "$PREFIX/config"/*.toml "$PREFIX/config"/*.yaml 2>/dev/null || true
chmod 700 "$PREFIX/data" 2>/dev/null || true

echo "Done. Add $PREFIX/bin to PATH."
echo "Optional: sudo $PREFIX/scripts/secure_setup.sh $PREFIX"
