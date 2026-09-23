# syntax=docker/dockerfile:1
# Multi-stage build for CoreTexDB (Linux / x86_64-unknown-linux-gnu).
# NOTE: the repo's .cargo/config.toml is Windows/MSVC-specific; we replace it
# inside the build with a Linux-friendly config (keeps the Tuna crates mirror,
# drops the windows-msvc target + linker + Windows PROTOC path).

FROM rust:1.95-slim AS builder

# Build essentials (protoc is required by tonic-build for gRPC codegen)
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler clang pkg-config libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Replace the Windows-specific cargo config with a Linux one (mirror only).
RUN printf '[source.crates-io]\nreplace-with = "tuna"\n[source.tuna]\nregistry = "sparse+https://mirrors.tuna.tsinghua.edu.cn/crates.io-index/"\n' > .cargo/config.toml \
    && export PROTOC=/usr/bin/protoc \
    && cargo build --release

# ---- Runtime image ----
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/coretex /usr/local/bin/coretex

# REST (5000) · gRPC (50051) · WebSocket (8080)
EXPOSE 5000 50051 8080
VOLUME ["/app/data"]

# 注意：绑地址/端口是 `-a/--address` 与 `-p/--port`。曾写成 `--host`，
# 那是个不存在的参数，容器一启动就会以 clap 的 “unexpected argument” 退出。
ENTRYPOINT ["coretex", "server", "-a", "0.0.0.0", "-p", "5000", "--grpc-port", "50051", "--ws-port", "8080"]
