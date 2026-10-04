# Roadmap / 路线图

本文件记录 CoreTexDB 的**已完成**与**待完善**项，按优先级排列。
标 ✅ 的已落地并有测试；标 ⬜ 的还没做。想参与就挑一个 ⬜。

目标不是"把代码行数堆大"，而是**每一行都在提供真实功能、并且有对应测试**
—— 这也是本项目衡量进度的标准。

---

## 阶段 O：开源项目整备 ✅（本轮）

让它成为一个**能独立对外的开源项目**，而不是一个只有源码的仓库：

| 项 | 状态 |
| --- | --- |
| 仓库元数据（`Cargo.toml` 的 `repository`/`homepage` 曾指向不存在的 `cerebros/CoretexDB`，已改为真实地址） | ✅ |
| `CONTRIBUTING.md` / `SECURITY.md` / `CODE_OF_CONDUCT.md` / `CHANGELOG.md` | ✅ |
| Issue 表单（bug / feature / config）+ PR 模板 | ✅ |
| `.editorconfig`、`rustfmt.toml` | ✅ |
| `docs/architecture.md`（分层、数据流、锁顺序、TTL、持久化布局）、`docs/roadmap.md` | ✅ |
| `examples/`：`quickstart`、`filter_search`、`persistence`，**CI 里会真正跑** | ✅ |
| README：徽章、英文简介、快速入口、贡献/安全入口 | ✅ |
| CI：`examples` job + `lint` job（clippy **deny 级** + rustdoc），PR 触发补 `release/*` | ✅ |
| 修复 `.gitignore` 的 `coretex_data/` 误屏蔽源码目录 `src/coretex_data/`（新增文件会被静默漏提交） | ✅ |
| 修复从未编译过的 `benches/vector_index.rs`（crate 名笔误 + 过时构造签名） | ✅ |
| 修复 Python 绑定里 `import cortexdb`（模块实为 `coretexdb`，照文档写必然 ImportError） | ✅ |
| 修复 deny 级 clippy：死循环恒不循环、3 处恒真断言、rustdoc 坏链接 | ✅ |

---

## 阶段 A：正确性收口 ✅ 全部完成

| # | 项 | 状态 |
| --- | --- | --- |
| A1 | **PQ 索引做成真量化索引**：修 `clone_box` 返回空索引、工厂硬编码维度 128、`train()` 无调用点、`pq` 选不到；惰性训练 + 码本解码 + 压缩 | ✅ `d3f78c7`，10 个测试 |
| A2 | **死代码清理**：删 6 个旧 `save_to_file`/`load_from_file`、未跟踪的 `coretex_generated.rs` | ✅ `70e55c1` |
| A3 | **过滤搜索性能**：宽泛过滤退化为 O(n·d) 精算，改为"元数据预筛 → ANN 过采样提案 → 存活不足 k 自动回退精确扫描"；三路径选择、锁序 data.read → 索引内部、候选借引用不 clone | ✅ 本次，`tests/filtered_search.rs` 6 例 |
| A4 | **TTL 入口**：lib + CLI `coretex ttl set\|remove\|purge` + REST 3 路由 | ✅ `70e55c1` |
| A5 | 列表分页（`vector list --limit/--offset`、REST `offset/limit`） | ✅ 早前已实现（此前审计误报为"无分页"，已更正） |
| A6 | **`purge_expired` 真 bug**：`list()` 隐藏过期键导致差集恒为空，内存/索引从不清理 | ✅ `70e55c1` |

---

## 阶段 B：打通已宣称但未实现的能力 ⬜

| # | 项 | 预估 |
| --- | --- | ---: |
| B1 | **完整 C FFI**：`include/coretexdb.h` 目前是占位符，与 README 声称的能力不符。补齐 connect/collection/insert/search/backup 全套 + cbindgen + C 示例 + 测试 | ✅ 手写 `include/coretexdb.h`（13 个 `extern "C"` + 状态码宏 + 所有权/线程约定）+ `src/coretex_ffi.rs`（`CoreTexDbHandle` 2-worker runtime、`catch_unwind`、线程局部 last_error）+ `tests/ffi_api.rs` 7 例（含头文件↔源码符号一致性守护）+ `share/examples/c/main.c` 真编译真运行 + `scripts/build_ffi_example.sh`；重复建集合经死变体 `CollectionAlreadyExists` 源映射为 `-3` |
| B2 | **hybrid / BM25 / rerank 接线**：`coretex_hybrid`、`coretex_bm25`、`coretex_rerank` 已存在但没接进 `search` | ✅ hybrid/BM25 `CoreTexDB::hybrid_search`（RRF 融合 + BM25 物化缓存按 `data_version` 失效 + 0 分命中过滤，`tests/hybrid_search.rs` 7 例）；rerank `CoreTexDB::hybrid_search_reranked`（细排对真实 metadata 文本、每次新建 pipeline、无文本查询逐位透传 RRF；`tests/rerank_search.rs` 3 例 + pipeline 单测）——B2 收口。余项：CLI/REST `--rerank` 标志（涉另一会话 WIP 文件，错峰补） |
| B3 | **REST/GraphQL 补全**：TTL 已补；余下 hybrid、分页参数、错误码统一 | 🔄 hybrid ✅（`e122d80` REST/CLI 入口）；余分页参数、错误码统一 |
| B4 | **孤立模块处理**：`coretex_ann`、`coretex_graph`、`coretex_tantivy` 等 3.8k 行模块无调用点——要么接线，要么删除并说明取舍 | ⏸ 暂缓（用户决定）：三模块零调用点、零测试引用、README/docs 无承诺，删除不损失现有功能；接线则意味着图查询产品面/自动调参/第二套全文引擎三块全新能力。待 C/D 排期后再定 |
| B5 | **过滤索引**（对应 A3）：倒置索引让元数据预筛也变成次线性 | ✅ `coretex_data/filter_index.rs`：metadata 倒排（(字段, 规范值)→ids + 字段存在集），按 `data_version` 在 `data.read` 锁内校验缓存（写者在写锁内 bump，命中必对应当前快照）；`scan` 产出候选**超集**——等值/`$in`/单 `$ne`/`$exists` 精确集合代数、范围与 `$regex` 收窄到存在集、`$and`/`$or` 交并组合、`$not` 回退全表；`search_filtered` 与 `delete_vectors_where` 改为候选集迭代 + `matches_filter` 精筛（索引买规模、线性谓词保精确）。单测 38 种 filter 形状对拍 `matches_filter` 断言超集性质（含 1 vs 1.0 数字边界）+ 1000 条窄查询候选规模断言；`tests/filter_index_search.rs` 6 例（算子精确语义/缓存失效/回退/等价写法/删除路径） |
| B6 | **Python 打包元数据缺失**：`python/` 没有 `pyproject.toml`/`setup.cfg`，`setup()` 不带任何元数据，`pip install -e .` 拿不到包名与依赖（CI 里靠先删 `pyproject.toml` 绕开 maturin） | ✅ `python/pyproject.toml`（PEP 621，setuptools 后端，dynamic version 取自 `coretexdb.version = 1.0.12` 单一事实源；5 个运行时依赖 + 3 组 extras）；CI 的 rm-pyproject 步骤删除（针对不存在文件的空操作，setuptools 后端本就不会触发 maturin）；实测 `pip install -e .` 拿到 `coretexdb 1.0.12` |
| B7 | **Python 命名不一致待决策**：包名 `coretexdb` 但类名是 `CortexDB`/`CortexDBClient`——是加 `CoreTexDB` 别名，还是统一改名（破坏性） | ✅ 用户拍板"新名为主 + 旧名兼容别名"：`CoreTexDB`/`CoreTexDBClient`/`AsyncCoreTexDBClient`/`CoreTexDBGrpcClient`/`AsyncCoreTexDBGrpcClient`/`CoreTexDBVectorStore` 为正式类名，旧名 `CortexDB*` 保留为同对象别名至 1.0（`__all__` 双导出，测试断言 `alias is canonical`）；文档/示例统一用正式名 |
| B8 | **`python/examples/basic_usage.py` 等示例的品牌与用法核对**（部分文案仍写 CortexDB） | ✅ 示例 16 处品牌改 `CoreTexDB`、4 个客户端方法逐一对照真实签名（`health_check`/`create_collection`/`insert_vectors`/`search`）、修正过时的 `bin coretex-server` 启动命令为单二进制 `./target/release/coretex server`（与 `examples/README.md` 对齐）；`py_compile` 通过 |

---

## 阶段 C：Redis 级系统能力 🔄

| # | 项 | 预估 |
| --- | --- | ---: |
| C1 | **主从复制**：WAL 传输、全量 + 增量同步、只读副本 | ✅ 真实 WAL 数据面（不用 `coretex_failover` 的半接线 KV 抽象）：**wal** `last_sequence` + `read_entries_since(since) → (tail, truncated)`（连续性覆盖段丢弃与日志重置，+2 单测）；**data 层** 只读守卫（`write_data` 拆出 `write_data_unchecked` 供恢复/回放豁免 + schema 三入口检查）、Create/DeleteCollection **锁内进 WAL**（增量携带 schema 变更）、`replication_snapshot`（位置→schema→记录 的读序保证接缝无缺口）/ `apply_replication_snapshot` / `apply_replicated_entries`（幂等、副本回写本地 WAL）；**coretex_replication** `ReplicationSnapshot`/`EntriesBatch`/`ReplicationStatus` + Transport trait（`HttpTransport`/`InProcessTransport`）+ `ReplicaSync`（全量→增量→追平、`replica_state.json` 原子持久、`spawn_loop`、apply 后 `persist_manifest` 保重启恢复）；**REST** `GET /replication/{status,snapshot,entries}`（auth skip 同 `/raft/*`）；**tests** `tests/replication.rs` 8 例（周期/只读拒绝/schema 与删除传播/幂等重放/状态续传/断尾回退/副本重启恢复/快照-尾部接缝） |
| C2 | **分片/集群**：slot 路由、节点发现、迁移 | ✅ 集合级分片（单集合跨节点需合并部分 ANN 结果，留后续）：`src/coretex_cluster.rs` —— **slot 路由**（Redis 式 16384 槽 + CRC16/XMODEM + `{hashtag}` 同槽，未分配报 MOVED 语义含 slot 号；`ClusterRouter` 集合↔槽双向索引、区间批量分配、概览）；**节点发现**（`ClusterTransport` + `probe_all` 报存活/集合数/记录数/LSN，无 transport 的节点报 down 而非静默跳过）；**迁移**（`CollectionChunk` = schema+行+位置，`DataManager::export_collection`/`import_collection` 逐字保真 schema 与索引类型；`ClusterMigrator` **先搬数据后切路由**，目标失败则路由不动，源保留副本待显式清理）。同进程 `LocalNodeTransport` + 8 集成例（路由/保真/幂等/迁移/失败不切/探测/目标重启恢复/概览）+ 4 单测（slot 稳定性与 hashtag、分布跨度、分配反查、区间与重复 id 校验）。**HTTP 侧（节点端点 + MOVED 响应）延后**：`coretex_api/rest/mod.rs` 属并行会话 WIP，不混入改动 |
| C3 | **Pub/Sub**：频道订阅 + 推送 | ✅ 补上长期空缺的**发布端**：`coretex_websocket` 的订阅/推送机制齐全却零触发点。`src/coretex_pubsub.rs` 新增 `EventBus`（tokio broadcast，可选挂载——未挂载时写路径零开销；发布永不失败、永不阻塞，慢订阅者收 `Lagged` 而非反压写路径；事件类型复用 `DataChangeEvent`）；`DataManager::set_event_bus` + 10 处写路径 emit（insert/update/delete 各含 tx 变体、create/delete/rename/import collection——**只广播成功落地的变更**，失败写静默；delete 只列真正存在的 id；clear 作为 delete 广播）；`WebSocketServer::attach_event_bus` 把总线桥到既有订阅表（补 `subscribe_connection` 程序化入口，库内消费者不必伪造 WS 消息）。单测 3 + `tests/pubsub.rs` 6 例（类型化事件/失败静默/delete 精确/多订阅者/无总线照常/WS 桥按集合过滤）。**socket accept 端点延后**（rest/mod.rs 属并行会话 WIP） |
| C4 | **快照与后台重写**：AOF/RDB 式格式 + 崩溃恢复演练 | ✅ `src/coretex_snapshot.rs`：运行中的一致性快照走 C1 同一条门（`replication_snapshot` 读序保证一致，**锁内拷贝、锁外序列化**，写路径零阻塞）；容器格式 `CTSNAP01 + 长度 + CRC32 + payload`（与 WAL 同一套校验），**原子落盘**（tmp+rename），损坏/截断/非快照文件均拒绝并指明失败项；`SnapshotArchive`（save/load/list/latest/prune/delete + `restore_into` 走复制 apply 路径并持久化 manifest）；`BackgroundSnapshotter` 定期 BGSAVE（失败只记日志、保留最近 N 份）；**日志压实** `compact_wal` 折叠为每 key 最终状态写到**新目录**（绝不改活跃日志，遇日志缺口拒绝压实）。**演练抓出并修复一个真 bug**：`recover_from_wal` 此前丢弃 WAL 里的 `CreateCollection`、靠首行向量猜 schema（恢复后度量/索引类型丢失）；现 schema 条目按序先于数据回放，行与排序与原库一致。单测 2 + `tests/snapshot.rs` 5 例（恢复"那一刻"而非现在/损坏拒绝/归档管理/恢复后可重启/压实回放等价） |
| C5 | **慢查询日志 / 命令统计 / INFO** | ✅ `src/coretex_stats.rs`：`SlowQueryLogger` 此前完备但零调用点，现由库入口直连。`OperationObserver`（命令计数 calls/errors/total_ms/max_ms + 可选慢查询日志，**未挂观察者时连参数描述都不求值**——零时钟读取零锁）；CoreTexDB 的 `search`/`insert_vectors`/`get_vector`/`delete_vectors` 经 `op_timer` 插桩；`collect_info` 产出 `# Server`（版本/模式/uptime）`# Replication`（WAL 开关/LSN）`# Keyspace`（每集合行数）`# Stats`（逐命令统计与均值/峰值）`# Cluster`（C2 槽位分布，按需）分段报告 + Redis 风格文本渲染，**只读标志取自 C1 的复制守卫**而非配置猜测。单测 3 + `tests/stats.rs` 6 例（计数准确/失败分列/慢查询带上下文/INFO 真相/文本与集群段/副本 read_only）。**INFO 端点与 CLI 延后**（rest/mod.rs、cli/mod.rs 属并行会话 WIP） |

---

## 阶段 D：生产化 ⬜

| # | 项 | 预估 |
| --- | --- | ---: |
| D1 | 可观测性：Prometheus 指标 + OpenTelemetry tracing 统一出口 | +1.5k 行 |
| D2 | 性能：SIMD 距离、批量写入、并行扫描 | +2.0k 行 |
| D3 | **测试覆盖到 Redis 级别**：故障注入、崩溃一致性、对拍测试 | +15k 行 |
| D4 | 文档：英文 README 完整版、故障恢复演练、运维手册、Python 文档 | +3.0k 行 |
| D5 | CI 质量门升级：`cargo fmt --check`（需先全量格式化）、`clippy -D warnings`（当前仍有约 100 个 warning）、覆盖率上报 | 中量 |

> 说明：本项目当前 Rust 代码约 5.4 万行（`src` + `tests`）。Redis 核心约 11 万行 C
> （不含测试），要对齐量级，**测试与文档必须跟上**——否则只是把未接线的模块堆得更多，
> 那正是本项目已知的历史教训。

---

## Good first issue

- 补 `examples/` 里的用法示例并从 README 链过去；
- 为 A3 过滤搜索写一个可复现的性能基准（过滤命中率高/低两组，量化提案路径 vs 全精算）；
- 给 `docs/` 补一页「故障恢复演练」；
- 报告一个带最小复现的边界 bug。
