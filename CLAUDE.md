# CoreTexDB — Session Memory

> 下次干活先读本文件。路径：`/home/qh/CoreTexDB/CLAUDE.md`（WSL Ubuntu）

## 当前状态（2026-09-25）

| 项 | 值 |
|----|-----|
| 版本 | **0.2.4**（`VERSION` / `Cargo.toml` / `Cargo.lock` / `RELEASE_NOTES.md` 一致） |
| 工作分支 | `release/v0.2.1-base`（默认分支仍 `master`） |
| 最新 commit | `f0032b1` — `chore(release): bump version to V0.2.4`（已推送）；其后 A3 过滤搜索待提交 |
| tag | `v0.2.4` → `53292cd`（指向 `f0032b1`，已推远端，release.yml `v*` 触发）；历史 `v0.2.3`→`c14e486`、`v0.2.2`→`e12b6f6` |
| 工作区 | 推送至 `f0032b1`；本地 A3 改动（`src/coretex_data/mod.rs` + `tests/filtered_search.rs` + 本文件）待提交 |

### 近期完成（2026-09-25，均已推送）
1. **PQ 真量化索引**（`d3f78c7`）：clone_box 共享 Arc、`layout_for` 自适应维度、惰性 `maybe_train`、码本 `decode`、`IndexType::PQ` 接线
2. **开源项目整备**（`1881474` fix + `a2f19bb` chore(oss)）：
   - 修复 5 处未编译/未检查缺陷：`.gitignore` 根锚定 `/coretex_data/`、`benches/vector_index.rs` 从未编译、Python 包名/CLI 用法过时、5 个 deny 级 clippy、坏 doc 链接
   - 治理文档：`CONTRIBUTING.md` `SECURITY.md` `CODE_OF_CONDUCT.md` `CHANGELOG.md` `.editorconfig` `rustfmt.toml` issue 表单×3 PR 模板
   - CI 新增 `lint`（clippy deny 级 + rustdoc）与 `examples`（编译+实跑）job，PR 触发补 `release/*`
   - `examples/{quickstart,filter_search,persistence}.rs` 本地实跑验证；`Cargo.toml` repository 改 `github.com/qinhaowow/coretexdb`
3. **V0.2.4 发布**（`f0032b1` + tag `v0.2.4`）：17 文件 35 处版本号、`RELEASE_NOTES.md` 重写、`CHANGELOG.md` `[0.2.4] - 2026-09-25`、`SECURITY.md` 版本表
4. **A3 过滤搜索性能（本次，待提交）**：`search_filtered` 三路径——候选 ≤ `max(256, k*16)` 或无索引走精确扫描；宽过滤让 ANN 索引过采样提案再过滤+同一距离函数重算；存活提案 < k 回退精确扫描（**过滤永远不能让查询变短**）。锁序 data.read → 索引内部，候选借引用不 clone。测试 `tests/filtered_search.rs` 6 例（含"提案全被拒绝必须回退拿满 k"回归）
5. （0.2.3 时代）索引持久化接线：`VectorIndex::persist` + `IndexManager::load_index`、原子写+校验和防陈旧索引、`restore_from_storage` 两阶段、CLI `coretex index save|list`
- Windows 验收：`E:\Ubuntn24042\wintest` 脚本 `windows_acceptance.ps1 -Root <dir>`，**19/19 全绿**（0.2.3 单 exe 构建）
- Linux 测试基线：**491** 全绿（485 + A3 新增 6）；clippy `--all-targets` 0 error；rustdoc 0 warning

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

### 安装根（V0.2.4）
```
CoreTexDB-V0.2.4/
  bin/     coretex（单二进制，argv[0] 分发：改名/硬链接即变 coretexd|backup|healthcheck）
  lib/     libcoretexdb.so|.dylib|.a, coretexdb.dll|.lib（crate-type: rlib+cdylib+staticlib）
  include/ coretexdb.h（**占位**，真实 FFI 在路线图 B1）
  config/  coretex.toml, logging.yaml, backup/security/metrics + dev|staging|prod
  share/   doc/, examples/
  scripts/ start/stop/install/upgrade/backup/...
  systemd/ logrotate/
  data/    coretex/{collections,indexes,metadata,store}, wal, backup, logs, temp, versions
```

### Cargo bins
单 `[[bin]] coretex`（`src/main.rs` 按 argv[0] 分发子命令：`server/backup/doctor/...`）  
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

- [ ] **A3 提交推送**（等全量测试绿 → `bash /home/qh/commit_a3.sh` → SSH443 push）；roadmap 登记 A3 完成 → **阶段 A 收口**
- [ ] 阶段 B：完整 C FFI（`coretexdb.h` + cbindgen + C 示例 + 测试）、hybrid/BM25/rerank 接入 search 与 CLI/REST
- [ ] 确认 Actions 是否 green（tag `v0.2.4` 触发 release.yml；GitHub HTTPS 被墙，需用户看网页）
- [ ] 是否把分支改名 `release/v0.2.4`（现仍叫 `v0.2.1-base`）
- [ ] 遗留：约 100 个 clippy warning（D5）；`insert_vectors` 持 `data.write()` 跨 storage IO；事务 abort 无 undo；Python 无 `pyproject.toml`；Python 类名 `CortexDB*`→`CoreTexDB*`（B7，破坏性）
- [ ] 系统级能力空白：复制/分片/Pub-Sub/快照

## Git 身份

```
user.name=qinhaowow
user.email=qinhaowo@126.com   # commit 时显式 -c 指定
```
