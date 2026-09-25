#!/usr/bin/env bash
set -euo pipefail
PREFIX="${PREFIX:-$(cd "$(dirname "$0")/.." && pwd)}"
if command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet coretexd.service 2>/dev/null; then
  echo "active (systemd)"
  exit 0
fi
if pgrep -f "coretex server .*${PREFIX}" >/dev/null 2>&1; then
  echo "active (process)"
  exit 0
fi
echo "inactive"
exit 3
