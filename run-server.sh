#!/usr/bin/env bash
# ── 启动 CoreTexDB 服务（Linux / WSL）────────────────────────────
# 用法：./run-server.sh
# 可用环境变量覆盖：ADDR PORT GRPC_PORT WS_PORT DATA_DIR
set -euo pipefail
cd "$(dirname "$0")" || exit 1

EXE="${EXE:-target/release/coretex}"
if [ ! -x "$EXE" ]; then
   echo "[ERR] 未找到 $EXE，请先执行: cargo build --release" >&2
   exit 1
fi

ADDR="${ADDR:-0.0.0.0}"
PORT="${PORT:-5000}"
GRPC_PORT="${GRPC_PORT:-50051}"
WS_PORT="${WS_PORT:-8080}"
DATA_DIR="${DATA_DIR:-./coretex_data}"

echo "[RUN] $EXE server -a $ADDR -p $PORT --data-dir $DATA_DIR"
exec "$EXE" server -a "$ADDR" -p "$PORT" \
   --grpc-port "$GRPC_PORT" --ws-port "$WS_PORT" \
   --data-dir "$DATA_DIR"
