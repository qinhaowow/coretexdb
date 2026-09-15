@echo off
REM ── 启动 CoreTexDB 服务（Windows）──────────────────────────────
REM 产物路径：
REM   从 Linux/WSL 交叉编译 → target\x86_64-pc-windows-gnu\release\coretex.exe
REM   在 Windows 本机构建     → target\release\coretex.exe
setlocal
cd /d "%~dp0"

set "EXE=target\x86_64-pc-windows-gnu\release\coretex.exe"
if not exist "%EXE%" set "EXE=target\release\coretex.exe"

if not exist "%EXE%" (
  echo [ERR] 未找到 coretex.exe
  echo       交叉编译: cargo build --release --target x86_64-pc-windows-gnu
  echo       本机构建: cargo build --release
  exit /b 1
)

REM 注意：地址/端口是 -a / -p，不是 --host / --port
echo [RUN] %EXE% server -a 0.0.0.0 -p 5000
"%EXE%" server -a 0.0.0.0 -p 5000 --grpc-port 50051 --ws-port 8080
endlocal
