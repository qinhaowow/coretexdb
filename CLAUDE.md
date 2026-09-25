# CoreTexDB — Session Memory

> 下次干活先读本文件。路径：`/home/qh/CoreTexDB/CLAUDE.md`（WSL Ubuntu）

## 当前状态（2026-09-25）

| 项 | 值 |
|----|-----|
| 版本 | **0.2.2**（`VERSION` / `Cargo.toml` / `Cargo.lock` 一致） |
| 工作分支 | `release/v0.2.1-base` |
| 最新 commit | `e12b6f6` — package full install-root into V0.2.2 |
| tag | `v0.2.2` → `e12b6f6`（已在远端） |
| 工作区 | **有未提交改动**（核心引擎持久化修复 + `CLAUDE.md`/`AGENTS.md`）；**用户要求：暂时不要再推送 GitHub** |

### 近期完成（2026-09-25 核心引擎 P0/P1 修复，未提交）
1. WAL fsync：`coretex_utils/wal.rs` append 后 `sync_all()`（原只 flush）
2. `DbConfig::sync_writes`（默认 true）→ `FileStorage::with_fsync`；`save_metadata` 改为 temp+fsync+rename+目录 fsync
3. 写路径不再吞错：`insert_vectors`/`delete_vectors`/`update_vector` 去掉 `let _ =`，改为 WAL→storage→内存→索引 顺序并 `?` 传播
4. WAL 恢复修复（`recover_from_wal`）：storage key 用 `collection:id`（原裸 id 对不上）；重放进内存 map + 索引（原只进 storage 不可见）；manifest 丢失时自动重建 collection
5. `RecoveryManager::recover` 同步改 `collection:id` 前缀
6. 事务：`commit`/`abort` 从 `active_transactions` 移除（原泄漏致 active_count 恒增）；`abort` 增加非 Active 状态校验；解掉 `tests/tests_integration_v2.rs` 的 `#[ignore]`
7. HNSW 锁序统一 `vectors → entry_point → graph`：`add`/`build` 原先持 graph 再取 vectors 与 search AB-BA 死锁
8. `purge_expired`：真正同步清理内存 map + 索引（原 `retain(|_,_| true)` 空操作）
9. `update_vector` 补 storage.store（原更新只进内存，重启丢失）
10. `scripts/restore.sh`：`--output`→`--input` 并补 `--force`（原参数 CLI 不认）
11. gRPC 生成代码改 `include!(concat!(env!("OUT_DIR"), "/coretex.rs"))`（原 `src/coretex_generated.rs` 无任何生成步骤，新 checkout 必挂）；`build.rs` protoc 缺失时快速报错
12. 新增回归测试 `wal_integration_tests::test_recover_from_wal_restores_memory_and_index`
- 测试：`cargo test` **424 unit + 26 + 11 integration 全绿，0 failed 0 ignored**（unit 全量约 467s）
- 本地缓存已清重编过；WSL 有 `/usr/bin/protoc`

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
