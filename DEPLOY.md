# CoreTexDB 部署说明（v1.1.0）

本项目是一个用 Rust 编写的**多模态向量数据库**，提供 REST / gRPC / GraphQL / WebSocket 接口。
本目录包含：

- `cloud-deploy/index.html` —— 部署到 CloudStudio 的**云端 Web 控制台**（本页）。
- `Dockerfile` —— 多阶段 Linux 镜像，用于容器化部署。
- `run-server.bat` —— Windows 下一键启动脚本。
- `target/release/coretex.exe`（编译产物）—— 服务端二进制。

---

## 1. 本地编译与运行

```bash
# 默认特性（tokio / serde / compression / metrics）
cargo build --release
# 产物：target/release/coretex.exe

# 启动（REST 默认 0.0.0.0:5000，默认不启用鉴权）
./target/release/coretex server --host 0.0.0.0 --port 5000

# 验证
curl http://localhost:5000/health
# => {"status":"ok","version":"1.1.0"}
```

## 2. Docker 部署（推荐生产环境）

```bash
docker build -t coretexdb:1.1.0 .
docker run -d --name coretexdb \
  -p 5000:5000 -p 50051:50051 -p 8080:8080 \
  -v coretex_data:/app/data \
  coretexdb:1.1.0
```

> 说明：仓库内的 `.cargo/config.toml` 为 Windows/MSVC 专用（固定了 windows-msvc 目标与链接器）。
> Dockerfile 在构建时会用仅含 Tuna 镜像源的 Linux 配置覆盖它，因此不影响 Linux 编译。

## 3. 云端 Web 控制台（CloudStudio）

`cloud-deploy/index.html` 已通过 CloudStudio 静态托管，作为 CoreTexDB 的云端入口：
概览、API 调试台、接口参考、部署说明。

> 注意：向量数据库是有状态后端进程，需运行在具备二进制的宿主（容器 / VM / 裸机）上。
> CloudStudio 仅提供静态站点托管，无法运行 Rust 服务进程本身；调试台通过浏览器直接请求你配置的
> 实例地址，跨域调用需在反向代理（Nginx / Caddy）上补充 `Access-Control-Allow-Origin`。

## 4. 鉴权与限流

- 默认启动 `server` 子命令 **不启用鉴权**。
- 启用鉴权：`coretex server --auth`
- 限流（每分钟请求数）：`coretex server --rate-limit 600`
- 默认 CORS 在未配置白名单时不放行跨域。

## 5. 常用 CLI

```bash
coretex collection create --name vectors --dimension 128
coretex vector insert --collection vectors --id v1 --vector "0.1,0.2,..."
coretex search --collection vectors --vector "0.1,0.2,..." --k 10
coretex admin health
coretex benchmark --count 1000 --dimension 128 --queries 100
```
