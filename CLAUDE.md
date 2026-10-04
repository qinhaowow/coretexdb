# CoreTexDB — Session Memory

> 下次干活先读本文件。路径：`/home/qh/CoreTexDB/CLAUDE.md`（WSL Ubuntu）

## 当前状态（2026-10-04）

| 项 | 值 |
|----|-----|
| 版本 | **0.2.4**（`VERSION` / `Cargo.toml` / `Cargo.lock` / `RELEASE_NOTES.md` 一致） |
| 工作分支 | `release/v0.2.1-base`（默认分支仍 `master`） |
| 最新 commit | `dd04926` — `feat(stats): C5 command statistics, slow-query logging and INFO` |
| tag | `v0.2.4` → `53292cd`（已推远端）；历史 `v0.2.3`→`c14e486`、`v0.2.2`→`e12b6f6` |
| 工作区 | **ahead 8**（我 5：`15290ee` C1 测试 / `671187d` 记忆 / `202b5c0` C2 / `c17208f` C3 / `568831c` C4 / `dd04926` C5；并行会话 2：`603da0a` audit 测试、`b8c844a` graphql）。**13 文件未提交 WIP 属并行会话**：`CHANGELOG.md`、`README.md`、`src/coretex_api/rest/mod.rs`（error_http_status 中间件）、cli/compression/crypto/`distributed/http_rpc`/index/`lakehouse/s3_http`/persistence/security/sql、`coretex_grpc/server.rs`、`tests/rest_metrics.rs` |

### 近期完成（2026-10-04，阶段 C「Redis 级系统能力」全收口 C1-C5）

1. **C1 主从复制** ✅ 走**真实 WAL 数据面**（不复用 `coretex_failover` 半接线 KV 抽象）：`read_entries_since(since) -> (tail, truncated)`（连续性覆盖段丢弃与日志重置）；**只读守卫**（`write_data`/`write_data_unchecked` 拆分 + 6 处入口检查）；**CreateCollection/DeleteCollection 进 WAL 且在 collections 锁内**（此前 schema 变更不进日志 → 增量复制丢 schema）；**快照读序定理**（位置→schemas→records，配合两条"锁内 WAL"纪律保证无缺口且幂等）；`ReplicaSync` 全量→增量→追平 + `replica_state.json` + `persist_manifest`。核心模块由并行会话在 `462a506` 提交（并改进 `HttpTransport` 查 status），测试与 roadmap 由我 `15290ee` 收口。REST `/replication/{status,snapshot,entries}`
2. **C2 分片/集群** ✅ `202b5c0` + `src/coretex_cluster.rs`：**集合级分片**（单集合跨节点要合并部分 ANN 结果，留后续）+ Redis 式 16384 槽（CRC16/XMODEM、`{hashtag}` 同槽、MOVED 语义错误含槽号）+ `ClusterRouter`（集合↔槽双向索引、区间分配、概览）+ `probe_all` 节点发现（无 transport 报 down 而非静默跳过）+ `CollectionChunk` 迁移（schema 逐字保真：维度/度量/索引类型）+ `ClusterMigrator` **先搬数据后切路由**（目标失败路由不动；源保留副本待显式清理）
3. **C3 Pub/Sub** ✅ `c17208f` + `src/coretex_pubsub.rs`：补上长期空缺的**发布端**（websocket 订阅机制齐全却零触发点）。`EventBus`（tokio broadcast，可选挂载；发布永不失败永不阻塞，慢订阅者收 `Lagged` 而非反压写路径）；`DataManager::set_event_bus` + 10 处写路径 emit（含 tx 变体；**只广播成功落地的变更**，delete 只列真正存在的 id，clear 作为 delete 广播）；`WebSocketServer::attach_event_bus` 桥接既有订阅表 + `subscribe_connection` 程序化入口
4. **C4 快照与后台重写** ✅ `568831c` + `src/coretex_snapshot.rs`：`coretex_backup` 是文件级拷贝（运行中拷 live 文件 ≠ 一致），改走 C1 同一条门取一致快照（锁内拷贝、锁外序列化）；容器 `CTSNAP01+长度+CRC32+payload`（与 WAL 同一套校验）+ 原子落盘 + 损坏/截断/非快照均拒绝并指明失败项；`SnapshotArchive`（save/load/list/latest/prune/restore_into）；`BackgroundSnapshotter` 定期 BGSAVE；**`compact_wal`** 折叠为每 key 最终状态写到**新目录**（绝不改活跃日志，遇缺口拒绝）。**演练抓出并修复真 bug**：`recover_from_wal` 丢弃 CreateCollection、靠首行向量猜 schema（恢复后度量/索引类型丢失）→ 现 schema 条目按序先于数据回放
5. **C5 慢查询/命令统计/INFO** ✅ `dd04926` + `src/coretex_stats.rs`：`SlowQueryLogger` 此前完备但零调用点，现由库入口直连。`OperationObserver`（逐命令 calls/errors/total/max + 可选慢查询日志；**未挂观察者时连参数描述都不求值**）；`search`/`insert_vectors`/`get_vector`/`delete_vectors` 经 `op_timer` 插桩；`collect_info` 分段报告（Server/Replication/Keyspace/Stats/Cluster）+ Redis 风格文本渲染，**只读标志取自 C1 复制守卫**
6. **并行会话动态**：他们替我提交过 C1 核心（发现 lib.rs 已声明模块而文件未提交会让干净检出编译不过），并修了我两个写不通的 WAL 测试（`49a3430`）；`DataChangeEvent` 根导出被他们改名为 `WsDataChangeEvent`（我走 `coretex_pubsub::DataChangeEvent`）；`WebSocketServer` 事件接收口叫 `event_receiver()`

### C 线已知限制（有意留后续，记录在此免得重犯）
- **HTTP 端点延后**：C2 节点端点/MOVED 响应、C3 WebSocket accept 路由、C5 INFO 端点与 CLI 命令——都因 `src/coretex_api/rest/mod.rs` 与 `src/coretex_cli/mod.rs` 属并行会话 WIP 而不混合改动。库层 API 全部齐备，接上端点即可
- 事务写（`*_tx`）**不进 WAL**（既有缺陷）→ 既不被复制也不被本地恢复；`rename_collection` 不进 WAL（副本需全量重同步才跟上）
- 复制端点与 `HttpTransport` **无认证**（部署方需网络层保护，同 Redis 复制默认做法）
- 副本回放与迁移导入**不写本地 WAL**（持久性来自 storage + manifest，`persist_manifest` 由 `ReplicaSync`/`LocalNodeTransport` 负责）
- `compact_wal` 的"日志有缺口则拒绝"分支**未直接测试**（缺口由 `read_entries_since` 的 wal 单测覆盖）

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
   - Linux 测试基线：**491** 全绿（0.2.3 时代）；C 线收口后全量实测 **lib 482 + 13 个集成套件全绿**（persistence 26 / filtered_search 6 / hybrid_search 7 / rerank 3 / ffi_api 7 / filter_index 6 / ttl 2 / index_persistence 3 / replication 8 / cluster 8 / snapshot 5 / pubsub 6 / stats 6）

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

- [ ] **D1 可观测性统一出口**（+1.5k）：并行会话已上 `/metrics`（`PrometheusMetrics`）+ 代码里有零散 `tracing::info!`——缺一个统一门面：一次初始化同时接管 Prometheus 文本导出与 tracing 订阅，并把 C 线新能力（复制 LSN/延迟、集群槽分布、快照/压实耗时、命令统计）纳入同一出口。注意 `/metrics` 端点在 `coretex_api/rest/mod.rs`（对方 WIP）——先做库层门面
- [ ] **D2 性能**（+2.0k）：`coretex_simd` 距离函数接线（现状：模块存在但搜索路径是否真走 SIMD 待查）、批量写入（`bulk_*` 已委托但无并行化）、并行扫描（storage 全量扫描）
- [ ] **D3 Redis 级测试**（+15k，工作量最大）：故障注入（WAL 截断/段丢失/存储损坏 → 恢复行为断言）、崩溃一致性（快照+尾部接缝已有 8 例可扩展）、对拍（快照/复制/压实三条恢复路径互相结果一致）
- [ ] **D4 文档**（+3.0k）：英文 README 完整版、故障恢复演练、运维手册、Python 文档。**注意 `README.md` 目前是对方 WIP**——先写新文件（`docs/OPERATIONS.md`/`docs/RECOVERY.md`）避免冲突
- [ ] **推送 ahead 8**（我 6 + 对方 2）：等并行会话 13 文件 WIP 收口 → 本地全量 `cargo test` 全绿（基线 lib 482 + 13 套件）→ SSH443 push
- [ ] **C 线延后项**（等 `rest/mod.rs`/`cli/mod.rs` 归属清晰后一并做）：C2 节点端点+MOVED 响应、C3 WebSocket accept 路由、C5 INFO 端点与 `coretex info` CLI
- [ ] **B3 余项**：分页参数、错误码统一（对方 `error_http_status` 正是此方向，避免重复造）、CLI/REST `--rerank` 标志
- [ ] B4 孤立模块：**暂缓**（用户拍板，C/D 后再定）
- [ ] 遗留缺陷：`insert_vectors` 持 `data.write()` 跨 storage IO；事务 abort 无 undo；事务写不进 WAL（见 C 线限制）
- [ ] 确认 Actions 是否 green（需用户看网页）；是否把分支改名 `release/v0.2.4`

## Git 身份

```
user.name=qinhaowow
user.email=qinhaowo@126.com   # commit 时显式 -c 指定
```