#!/usr/bin/env bash
set -euo pipefail
PREFIX="${PREFIX:-$(cd "$(dirname "$0")/.." && pwd)}"
if command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files coretexd.service >/dev/null 2>&1; then
  systemctl stop coretexd.service
  exit 0
fi
pkill -TERM -f "coretex server.*--data-dir.*${PREFIX}" 2>/dev/null || true
echo "stop requested"
