# CoreTexDB — Session Memory

> 下次干活先读本文件。路径：`/home/qh/CoreTexDB/CLAUDE.md`（WSL Ubuntu）

## 当前状态（2026-10-04）

| 项 | 值 |
|----|-----|
| 版本 | **0.2.4**（`VERSION` / `Cargo.toml` / `Cargo.lock` / `RELEASE_NOTES.md` 一致） |
| 工作分支 | `release/v0.2.1-base`（默认分支仍 `master`） |
| 最新 commit | `15290ee` — `test(replication): C1 end-to-end coverage; mark the roadmap item done` |
| tag | `v0.2.4` → `53292cd`（已推远端）；历史 `v0.2.3`→`c14e486`、`v0.2.2`→`e12b6f6` |
| 工作区 | **ahead 3**：`603da0a` audit 测试、`b8c844a` graphql 端点（并行会话）+ `15290ee` C1 测试与 roadmap（我）。**13 文件未提交 WIP 属并行会话**：`CHANGELOG.md`、`README.md`、`src/coretex_api/rest/mod.rs`（error_http_status 中间件）、cli/compression/crypto/`distributed/http_rpc`/index/`lakehouse/s3_http`/persistence/security/sql、`coretex_grpc/server.rs`、`tests/rest_metrics.rs` |

### 近期完成（2026-10-04，C1 主从复制收口）

1. **C1 主从复制** ✅ 走**真实 WAL 数据面**（明确不复用 `coretex_failover` 的半接线 Raft KV 抽象）：
   - `wal.rs`：`last_sequence()`（读 append 用的同一 counter，init 后 `stats.last_sequence` 不恢复所以不能用它）+ `read_entries_since(since) -> (tail, truncated)`；连续性判定 = `oldest > since+1`（段丢弃留洞）或 `since > last`（日志在客户端脚下被重置）或 `空日志 && since>0`
   - `coretex_data`：**只读守卫** —— `write_data()` 拆出 `write_data_unchecked()`（启动恢复/复制回放豁免）+ 6 处 schema/TTL/清理入口显式检查；**CreateCollection/DeleteCollection 进 WAL 且在 collections 写锁内**（此前三处 `wal_log` 只覆盖 insert/delete/update，schema 变更不进日志 → 增量复制会丢 schema）；`replication_snapshot()` 读序 **位置 → schemas → records**（记录写持 data 写锁跨 WAL append、schema 写持 collections 锁跨 WAL append，两条纪律共同保证"快照 + 尾部无缺口、无丢失，且重放幂等"）；`apply_replication_snapshot`（清空 → 灌 storage → 走 `restore_from_storage` 正常恢复路径重建 schema/内存/索引）、`apply_replicated_entries`（幂等回放 + 回写副本**自己**的 WAL）
   - `src/coretex_replication.rs`：`ReplicationSnapshot`/`EntriesBatch`（`lsn` 由**已发出条目**推导，绝不信另采样的水位）/`ReplicationStatus` + `ReplicationTransport` trait（`HttpTransport` **由并行会话补了 HTTP status 检查**、`InProcessTransport` 同进程双实例）+ `ReplicaSync`（全量→增量→追平、`replica_state.json` 原子持久、`spawn_loop`、`persist_manifest` 保证重启后 schema 可恢复）
   - REST：`GET /replication/{status,snapshot,entries}`（auth skip 同 `/raft/*`；未 push 前属内部通道，部署方需网络层保护）
   - **测试**：`tests/replication.rs` 8 例（全量→增量→追平周期 / 只读拒绝 / schema 与删除传播 / 幂等重放 / 状态文件续传 / 无日志断尾回退 / 副本重启本地恢复 / 快照-尾部接缝）全绿
2. **并行会话的提交序列**（我的 6 个 B 线提交已被 push 并入历史）：`bc1a4a7` auth 用户持久化、`ddbddd8`+/`b8c844a` `/console` 与 `/graphql` 端点、`adc70ac` 限流器封顶 + gRPC 限流、`2f62bfe` `/metrics`（修了三处会撒谎的实现）、`462a506` **提交了我的复制模块**（lib.rs 已声明模块而文件未提交会让干净检出编译不过）并改进 `HttpTransport`、`59e4186`/`603da0a` 审计日志与其端到端测试、`e87c5c9` per-feature 编译门禁、`bd33d41`/`1dc9234` `--features full` 修复、`49a3430` 修我两个写不通的 WAL 测试
3. **B4 用户拍板暂缓**：三孤立模块（ann/graph/tantivy 3.8k 行）零调用点、文档零承诺——C/D 后再定

### 之前完成（2026-10-01，B 线主体收口）

1. **B1 完整 C FFI** ✅ `91353e9`：手写 `include/coretexdb.h`（13 `extern "C"` + 状态码宏 + 所有权/线程约定）+ `src/coretex_ffi.rs`（`CoreTexDbHandle{db, rt}` 2-worker runtime、`catch_unwind`、线程局部 last_error）+ `tests/ffi_api.rs` 7 例（头↔源符号一致性守护）+ `share/examples/c/main.c` 真编译真运行 + `scripts/build_ffi_example.sh`
2. **B2 rerank 收口** ✅ 同批：`TwoStageSearchPipeline::search_with_documents` 真回调 + `CoreTexDB::hybrid_search_reranked`（细排对真实 metadata 文本、每次新建 pipeline、无文本查询逐位透传 RRF）；`tests/rerank_search.rs` 3 例 + pipeline 单测
3. **B6/B7/B8 Python 三件** ✅ `663b295`：PEP 621 `pyproject.toml`（setuptools 后端 + dynamic version=1.0.12 单源）；类名 `CortexDB*`→`CoreTexDB*` 主名 + 旧名同对象别名至 1.0；示例品牌 16 处 + 方法签名核对 + 修过时启动命令；16 单测绿
4. **B5 过滤索引** ✅ `8a6e6ee`：`coretex_data/filter_index.rs` metadata 倒排，`data_version` 在 data.read 锁内校验；`scan` 出候选超集（等值/`$in`/单 `$ne`/`$exists` 精确、范围/`$regex` 收窄存在集、`$and`/`$or` 交并、`$not` 回退）；单测 38 种 filter 形状对拍 + 1000 条窄查询 100 候选断言；集成 6 例 + A3 回归 6 例绿

### 之前完成（2026-09-25）

1. **PQ 真量化索引**（`d3f78c7`）：clone_box 共享 Arc、`layout_for` 自适应维度、惰性 `maybe_train`、码本 `decode`、`IndexType::PQ` 接线
2. **开源项目整备**（`1881474` fix + `a2f19bb` chore(oss)）：修复 5 处未编译/未检查缺陷、治理文档（CONTRIBUTING/SECURITY/CODE_OF_CONDUCT/CHANGELOG/.editorconfig/rustfmt/issue 模板/PR 模板）、CI 新增 `lint` 与 `examples` job、`examples/{quickstart,filter_search,persistence}.rs` 本地实跑
3. **V0.2.4 发布**（`f0032b1` + tag `v0.2.4`）：17 文件 35 处版本号、`RELEASE_NOTES.md` 重写、`CHANGELOG.md` `[0.2.4] - 2026-09-25`、`SECURITY.md` 版本表
4. **A3 过滤搜索性能** ✅ `25899d9`：`search_filtered` 三路径（候选 ≤ `max(256, k*16)` 或无索引精确扫描；宽过滤让 ANN 过采样再同距离函数重算；存活提案 < k 回退精确扫描——**过滤永远不能让查询变短**）。`tests/filtered_search.rs` 6 例
5. **B2a hybrid 搜索接线**：`CoreTexDB::hybrid_search`（向量 + BM25 → RRF 融合、单侧可用、filter 两侧生效）；BM25 缓存按 `data_version` 失效；`add_documents` 批量建 O(n)（原循环单加是 O(n²)）；**0 分命中过滤**。`tests/hybrid_search.rs` 7 例
6. 索引持久化接线：`VectorIndex::persist` + `IndexManager::load_index`、原子写+校验和防陈旧索引、`restore_from_storage` 两阶段、CLI `coretex index save|list`
   - Windows 验收：`E:\Ubuntn24042\wintest` 脚本 `windows_acceptance.ps1 -Root <dir>`，**19/19 全绿**
   - Linux 测试基线：**491** 全绿（0.2.3 时代）；此后 +ffi 7 +rerank 6 +filter_index 10 +replication 8；`--lib` 单测上次全量 449 绿 + 并行会话修好的 2 个 = **451**（全量待确认）

### 用户偏好 / 约束
- **不要主动 push / 打 tag**，除非明确要求；push 前必须本地全量 `cargo test` 全绿（定向绿不算）
- 中文沟通；直接给结果，不空谈；代码量只作真实功能副产品，不注水
- 不 rewrite/amend 已推送历史；工作区不留 `??` 临时文件
- Python SDK `__version__ = "1.0.12"` 独立版本线，勿混改
- 清理过本地编译垃圾；`~/.cargo/registry`（~710M）可保留加速重编

## 协作纪律（与并行会话共享工作区）

- **跑通即提交**：并行会话会把共享工作区里未提交的内容替我提交（C1 的复制模块就是这样进的 `462a506`，他们还顺手改进为检查 HTTP status）。攒着不提交 = 失去提交时机与说明权。
- **文件归属清单**（我不动）：`CHANGELOG.md`、`README.md`、`src/coretex_api/rest/mod.rs`（当前 error_http_status WIP）、cli/compression/crypto/`distributed/http_rpc`/index/`lakehouse/s3_http`/persistence/security/sql、`coretex_grpc/server.rs`、`tests/rest_metrics.rs`。提交前用 `git status --short` 复核，只 add 自己那几项。
- **错峰 cargo**：并发 cargo 曾删掉测试 binary 导致全量假红。所有脚本内置 `pgrep -x cargo` 探针，命中就退出（`CARGO_BUSY`），稍后重试。
- **`/home/qh/*.sh` 会被清理**：验证/提交脚本要能随手重建（当前：`verify_c1.sh`）；PowerShell 调 WSL 一律写脚本文件再 `bash /home/qh/x.sh`，绝不内联复杂引号。

## 环境要点

- **代码在 WSL**：`\\wsl.localhost\Ubuntu\home\qh\CoreTexDB` 或 WSL 内 `/home/qh/CoreTexDB`
- **GitHub HTTPS 被墙**：DNS 污染 `github.com→127.0.0.1`；只能 **SSH over 443**
- SSH 配置：`~/.ssh/config` → `HostName 20.205.243.160` Port 443（IP 会变，用 `refresh-github-ssh-ip.sh` 刷新）
- 推送命令需：`export GIT_SSH_COMMAND='ssh -p 443 -o StrictHostKeyChecking=accept-new -o IdentitiesOnly=yes'`
- 偶发 `remote: Internal Server Error`：重试即可
- GitHub API/HTTPS 本机不可用，Actions 状态需用户在浏览器看
- **ruflo 智能体平台**：Windows npm 全局 3.49.0（PATH 直接 `ruflo`）；**WSL 内严禁跑 `/mnt/c` 下的 ruflo 包**（9p 跨文件系统加载卡死），用 `npx -y ruflo@3.49.0`；LLM provider 全部未配置，key 由**用户自己** `ruflo providers configure ...` 填（不进对话）

## 架构速查

### 安装根（V0.2.4）
```
CoreTexDB-V0.2.4/
  bin/     coretex（单二进制，argv[0] 分发）
  lib/     libcoretexdb.so|.dylib|.a, coretexdb.dll|.lib（crate-type: rlib+cdylib+staticlib）
  include/ coretexdb.h（手写 FFI 头，13 函数；`tests/ffi_api.rs` 守护一致）
  config/  coretex.toml, logging.yaml, backup/security/metrics + dev|staging|prod
  share/   doc/, examples/
  scripts/ start/stop/install/upgrade/backup/...
  systemd/ logrotate/
  data/    coretex/{collections,indexes,metadata,store}, wal, backup, logs, temp, versions
```

### 复制数据面（C1）
```
ReplicaSync ──(Transport: Http / InProcess)──> 主库
   fetch_snapshot()  = replication_snapshot()  {lsn, schemas, records}
   fetch_entries(s)  = read_entries_since(s)  (tail, truncated) + lsn=尾条序号
   apply  = apply_replication_snapshot / apply_replicated_entries（幂等，豁免只读守卫）
只读守卫 = write_data()（拒） / write_data_unchecked()（恢复·回放放行）+ 6 处入口检查
接缝定理 = 记录写持 data 写锁跨 WAL append；schema 写持 collections 写锁跨 WAL append
          ⇒ 快照(位置先读) + 尾部(位置后) 覆盖全部写入，重叠部分幂等无害
```

**已知限制（有意留待后续）**：事务写（`*_tx`）不进 WAL → 既不被复制也不被本地恢复；`rename_collection` 不进 WAL（副本需全量重同步才跟上）；复制端点无认证（部署方网络层保护）；`HttpTransport` 单次拉全量尾部（无分页/压缩）。

### Cargo bins
单 `[[bin]] coretex`（`src/main.rs` 按 argv[0] 分发子命令）  
默认 features: `tokio, serde, compression, metrics`（`full` 含 rocksdb/onnx 等，CI 用默认）

### 版本号来源
- 运行时：`env!("CARGO_PKG_VERSION")` → 读 `Cargo.toml`
- 安装默认路径：scripts/systemd 里的 `/opt/CoreTexDB-V0.2.2`
- Python SDK：`python/coretexdb/version.py` = `1.0.12`（独立线，勿混改）

## 关键文件

- 发布：`.github/workflows/release.yml`（`workflow_dispatch` + tag `v*`）
- 安装：`scripts/install.sh`
- 文档：`README.md` §1.1 §10、`RELEASE_NOTES.md`、`share/doc/INSTALL.md`、`docs/roadmap.md`（A/B/C/D 路线图状态）

## 下次可能任务

- [ ] **C2 分片/集群**（最大头，roadmap +4.0k）：slot 路由、节点发现、迁移。起点建议：`coretex_distributed`（`TwoPhaseCommit`/`DistributedLock` 已有半成品）+ `coretex_failover` 的概念对齐，但都未接线——先摸清无调用点再定设计
- [ ] **C3 Pub/Sub**（+1.0k）：`coretex_websocket` 已有 WebSocketServer/订阅消息骨架，可能直接接线
- [ ] **C4 快照与后台重写**（+2.0k）：`coretex_backup` + `coretex_persistence` 可复用；复制快照已是可导出的全量形态
- [ ] **C5 慢查询/命令统计/INFO**（+1.0k）：`SlowQueryLogger`/`PrometheusMetrics` 已存在，缺 INFO 汇总面
- [ ] **D1-D4 生产化**：观测/SIMD/测试/文档（**D5 clippy 归并行会话**，工作区 13 文件 WIP 中）
- [ ] **推送 ahead 3**（`603da0a`、`b8c844a`、`15290ee`）：等并行会话 13 文件 WIP 收口 → 本地全量 `cargo test` 全绿（基线 451 lib + 集成，逐项确认）→ SSH443 push
- [ ] **B3 余项**：分页参数、错误码统一（并行会话的 `error_http_status` 正是错误码方向，避免重复造）、CLI/REST `--rerank` 标志
- [ ] B4 孤立模块：**暂缓**（用户拍板，C/D 后再定）
- [ ] 遗留缺陷：`insert_vectors` 持 `data.write()` 跨 storage IO；事务 abort 无 undo；事务写不进 WAL（见上）
- [ ] 确认 Actions 是否 green（需用户看网页）；是否把分支改名 `release/v0.2.4`

## Git 身份

```
user.name=qinhaowow
user.email=qinhaowo@126.com   # commit 时显式 -c 指定
```