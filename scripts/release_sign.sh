#!/usr/bin/env bash
# Generate .sha256 for release artifacts (spec §五 integrity).
# Usage: scripts/release_sign.sh <dir-with-binaries>
set -euo pipefail
DIR="${1:?usage: release_sign.sh <dir>}"
cd "$DIR"
shopt -s nullglob
files=(*)
if [ ${#files[@]} -eq 0 ]; then
  echo "no files in $DIR" >&2
  exit 1
fi
: > SHA256SUMS
for f in *; do
  case "$f" in
    SHA256SUMS|*.sha256|*.sig) continue ;;
  esac
  [ -f "$f" ] || continue
  sha256sum "$f" | tee -a SHA256SUMS
  sha256sum "$f" > "$f.sha256"
done
echo "wrote SHA256SUMS and per-file .sha256 in $DIR"
echo "Optional detached signature:"
echo "  openssl dgst -sha256 -sign <private.pem> -out SHA256SUMS.sig SHA256SUMS"
