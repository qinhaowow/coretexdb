@echo off
REM 启动 CoreTexDB 服务（Windows）
REM 依赖：已通过 cargo build --release 编译出 target/release/coretex.exe
setlocal
cd /d "%~dp0"
if not exist "target\release\coretex.exe" (
  echo [ERR] 未找到 target\release\coretex.exe，请先执行: cargo build --release
  exit /b 1
)
echo [RUN] 启动 CoreTexDB REST(:5000) gRPC(:50051) WS(:8080) ...
target\release\coretex.exe server --host 0.0.0.0 --port 5000 --grpc-port 50051 --ws-port 8080
endlocal
