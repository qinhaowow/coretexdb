#!/usr/bin/env bash
# B1 验证：构建 libcoretexdb → 用 cc 按 include/coretexdb.h 编译 C 示例 → 运行。
# 与另一个会话并行开发时必须错峰：曾有并发 cargo 把测试 binary 删在执行半途。
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.cargo/bin:$PATH"

command -v cc >/dev/null || {
    echo "cc not found (install build-essential)" >&2
    exit 1
}

# 等别的 cargo 释放目标目录（最多 20 分钟）。
for _ in $(seq 1 240); do
    pgrep -x cargo >/dev/null || break
    sleep 5
done

cargo build --lib

OUT=target/ffi-example
mkdir -p "$OUT"
cc -std=c11 -Wall -Wextra -Iinclude share/examples/c/main.c \
    -Ltarget/debug -lcoretexdb \
    -Wl,-rpath,"$PWD/target/debug" \
    -o "$OUT/c_example"

DATA_DIR=$(mktemp -d "${TMPDIR:-/tmp}/coretexdb_c_example.XXXXXX")
trap 'rm -rf "$DATA_DIR"' EXIT
"$OUT/c_example" "$DATA_DIR"
