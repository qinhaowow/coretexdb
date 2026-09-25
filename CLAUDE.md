# CoreTexDB — Session Memory

> 下次干活先读本文件。路径：`/home/qh/CoreTexDB/CLAUDE.md`（WSL Ubuntu）

## 当前状态（2026-09-25）

| 项 | 值 |
|----|-----|
| 版本 | **0.2.3**（`VERSION` / `Cargo.toml` / `Cargo.lock` 一致） |
| 工作分支 | `release/v0.2.1-base` |
| 最新 commit | `836969d` — argv[0] 单文件分发（coretex.exe 多角色）；其后「单 bin 化」改动待提交 |
| tag | `v0.2.3` → `c14e486`（已在远端，release.yml `v*` 触发）；旧 `v0.2.2` → `e12b6f6` 保留 |
| 工作区 | 已推送至 `836969d`；本地有「删 5 壳 bin」未提交改动（等测试绿后提交） |

### 近期完成（2026-09-25，均已推送）
1. 核心引擎 P0/P1 修复 12 项：WAL fsync、sync_writes→fsync、写路径不吞错、WAL 恢复 `collection:id`、事务 active 泄漏、HNSW 锁序、purge_expired、update 补 storage、restore.sh 参数、gRPC include 生成码等（commit `4b508fc`）
2. Windows 验收发现并修复 2 bug（commit `7c68885`）：restore 前未建父目录致全平台从未工作；`search --with-metadata` 在 `--format json` 被忽略
3. 版本全线 0.2.2 → 0.2.3（18 文件，commit `c14e486`），tag `v0.2.3` 已打
4. **单文件分发**：`src/main.rs` argv[0] 分发——coretex.exe 改名/硬链接即变 coretexd/backup/healthcheck 角色（commit `836969d`）
5. **单 bin 化（本地上次改动，待提交）**：Cargo.toml 删 5 个壳 `[[bin]]`、删 `src/bin/*.rs`、scripts/systemd/release.yml/README 全改为 `coretex server/backup/doctor` 用法；编译只产一个 `coretex.exe`
- Windows 验收：`E:\Ubuntn24042\wintest` 脚本 `windows_acceptance.ps1 -Root <dir>`，**19/19 全绿**（0.2.3 单 exe 构建）
- Linux 测试历史基线：424~426 unit + 26 + 11 integration 全绿（P0/P1 12 项修复明细见 commit `4b508fc`）

### 之前完成（V0.2.2 发布线）
1. `release.yml`：多 bin + `--target` 正确产物路径 + sha256 + GitHub Release
2. 版本号全线 0.2.1 → 0.2.2（含 systemd/scripts/docs/install 默认 `/opt/CoreTexDB-V0.2.2`）
3. 完整安装根架构打进 Release：`bin/ lib/ include/ config/ share/ scripts/ systemd/ logrotate/ data/` 骨架
4. `include/coretexdb.h` PATCH 已改为 2
5. 本地编译缓存已清（`target/` 3.9G、`package/`）

### 用户偏好 / 约束
- **不要主动 push / 打 tag**，除非明确要求
- 中文沟通
- 清理过本地编译垃圾；`~/.cargo/registry`（~710M）可保留加速重编

## 环境要点

- **代码在 WSL**：`\\wsl.localhost\Ubuntu\home\qh\CoreTexDB` 或 WSL 内 `/home/qh/CoreTexDB`
- **GitHub HTTPS 被墙**：DNS 污染 `github.com→127.0.0.1`；只能 **SSH over 443**
- SSH 配置：`~/.ssh/config` → `HostName 20.205.243.160` Port 443（IP 会变，用 `refresh-github-ssh-ip.sh` 刷新）
- 推送命令需：`export GIT_SSH_COMMAND='ssh -p 443 -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes'`
- 偶发 `remote: Internal Server Error`：重试即可
- GitHub API/HTTPS 本机不可用，Actions 状态需用户在浏览器看
- PowerShell 调 WSL 时 **避免复杂引号/`$(...)`**，改写脚本文件执行

## 架构速查

### 安装根（V0.2.2）
```
CoreTexDB-V0.2.2/
  bin/     coretex, coretexd, coretex-cli, coretex-migrate, coretex-backup, coretex-healthcheck
  lib/     libcoretexdb.so|.dylib|.a, coretexdb.dll|.lib, coretexdb.h → include
  include/ coretexdb.h
  config/  coretex.toml, logging.yaml, backup/security/metrics + dev|staging|prod
  share/   doc/, examples/
  scripts/ start/stop/install/upgrade/backup/...
  systemd/ logrotate/
  data/    coretex/{collections,indexes,metadata,store}, wal, backup, logs, temp, versions
```

### Cargo bins
`coretex`, `coretexd`, `coretex-cli`, `coretex-migrate`, `coretex-backup`, `coretex-healthcheck`  
crate-type: `rlib, cdylib, staticlib`  
默认 features: `tokio, serde, compression, metrics`（`full` 含 rocksdb/onnx 等，CI 用默认）

### 版本号来源
- 运行时：`env!("CARGO_PKG_VERSION")` → 读 `Cargo.toml`
- 安装默认路径：scripts/systemd 里的 `/opt/CoreTexDB-V0.2.2`
- Python SDK：`python/coretexdb/version.py` = `1.0.12`（独立线，勿混改）

## 关键文件

- 发布：`.github/workflows/release.yml`（`workflow_dispatch` + tag `v*`）
- 安装：`scripts/install.sh`
- 文档：`README.md` §1.1 §10、`RELEASE_NOTES.md`、`share/doc/INSTALL.md`

## 下次可能任务

- [ ] **提交本轮核心引擎修复**（等用户明确说 commit/push；注意 `?? CLAUDE.md AGENTS.md`）
- [ ] 确认 Actions 是否 green / 修 release 失败（需用户贴日志或看网页）
- [ ] 是否把分支改名 `release/v0.2.2`（现仍叫 `v0.2.1-base`）
- [ ] 真实 `coretexdb.h` FFI（当前是 placeholder）
- [ ] 遗留 P2：`insert_vectors` 持 `data.write()` 跨 storage IO（锁内 IO）；事务 abort 无 undo 回滚；`FileStorage` TTL 是否持久化待查
- [ ] 用户若说「推送」：再 commit/push/tag，确认无 `??` 临时文件

## Git 身份

```
user.name=qinhaowow
user.email=qinhaowo@126.com   # commit 时显式 -c 指定
```
