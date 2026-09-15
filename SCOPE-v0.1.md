# CoreTexDB v0.1 — 基座范围与契约

> 状态：**已验收**（2026-09-15）。所有「保证」条目都有可复现的测试或端到端证据。
> 本文只描述**已经能工作**的部分；未完成项集中在「已知问题」与「明确不在 v0.1 内」。

---

## 1. 一句话定义

v0.1 是一个**能真正落盘、重启不丢数据、检索结果精确**的向量数据库基座。

在此之前，这个仓库有 45k 行代码、16 个 CLI 子命令和 REST/gRPC/GraphQL/SQL/图/时序/湖仓等模块，
但**没有任何持久化**：所有数据都在内存里，进程退出即归零。v0.1 补上了这一层。

---

## 2. 保证（Guarantees）

每条都可验证，验证方式见 §5。

| # | 保证 | 证据 |
| --- | --- | --- |
| G1 | Collection 定义（维度 / 距离度量 / 索引类型）写入 `metadata.json`，重启后完整恢复 | `metadata_json_is_a_readable_manifest`、`index_type_is_recorded_and_survives_a_restart` |
| G2 | 向量（id / 向量 / 元数据）持久化，重启后完整恢复 | `collections_and_vectors_survive_a_restart`、`persists_across_many_restarts` |
| G3 | 进程崩溃留下的**半写记录不会**被当成有效数据读回 | `truncated_tail_is_discarded`、`corrupt_record_stops_replay` |
| G4 | 四种距离度量（cosine / euclidean / dotproduct / manhattan）**结果精确**，与独立 brute-force 参考实现逐位一致 | `indexed_search_matches_brute_force_for_every_metric`、`scan_path_matches_brute_force_for_every_metric` |
| G5 | 重启后（索引从日志重建）检索结果**依然正确** | `search_correctness_is_preserved_across_a_restart` |
| G6 | 默认索引是**精确**的；近似索引（HNSW/IVF）必须显式指定 | `an_unknown_index_type_falls_back_to_the_exact_index` |
| G7 | 元数据过滤**生效**，且匹配数足够时一定返回 k 条 | `filter_is_honoured_and_returns_k_matches_when_they_exist` |
| G8 | CLI 每条命令是独立进程，但共享同一个数据库 | §4 端到端：跨 7 个进程 create→insert→list→count→search→get→filter 全部正确 |
| G9 | 删除 collection 会清除其落盘向量，同名重建**不会复活**旧数据 | `deleting_a_collection_drops_its_vectors_for_good` |
| G10 | Server 重启后数据存活，REST 全链路正常 | §4 端到端：重启后 collections/count/search 均正确 |

---

## 3. 架构（v0.1 实际生效的部分）

```text
coretex_data/metadata.json          ← manifest：集合定义（原子写：临时文件 + rename）
coretex_data/store/store-NNNNNN.log ← 向量日志：追加写、分片、CRC32 校验、自动 compaction
```

**写入路径**：`CoreTexDB` → `DataManager`（内存态 + 索引）→ `FileStorage`（落盘）

### `FileStorage`（新增，`src/coretex_storage/file_store.rs`）

纯 Rust 实现，无 C/C++ 构建依赖（此前持久化依赖 rocksdb feature，而默认 feature 未开启，
所以 `with_config` 会直接 panic —— 这也是「默认不可持久化」的原因之一）。

- **记录格式**：`[magic u8][op u8][crc32 u32][key_len u32][payload_len u32][key][payload]`，CRC 覆盖 `op || key || payload`
- **读取**：初始化时重放日志重建 `key → 文件偏移` 索引，单次读取即一次 pread，无需全量扫描
- **崩溃恢复**：重放遇到截断或 CRC 不符即认为尾部不可信，**物理截断**到最后一个完整记录
- **耐久性**：每条记录 `flush` 到 OS（进程崩溃不丢已确认写）。整机断电级耐久需 `with_fsync(true)`
- **空间回收**：死字节超过阈值时自动 compaction（重写存活记录到新分片，再删除旧分片）；也可手动 `compact()`
- **TTL**：支持并跨重启保留

### 单一距离真源（`metric_distance` in `src/coretex_index/mod.rs`）

此前 BruteForce / HNSW / IVF / PQ 各有一份 `calculate_distance` 拷贝，且**只实现了 cosine 和 euclidean**，
`dotproduct` 与 `manhattan` 落入 `_ =>` 分支**静默变成 cosine**。
现在收敛为一个函数，四个索引共用；未知 metric 不再静默降级为 cosine 之外的语义。

---

## 4. 端到端验收记录

```text
CLI（7 个独立进程共享一个库）
  collection create docs -d 4 -m euclidean -i brute_force   ✓
  vector insert docs a/b/c                                    ✓
  collection list -v      → docs 4 Euclidean 3                 ✓
  vector count docs       → 3                                  ✓
  search '0,0,0,0' -k 3   → a c b      （euclidean 正确序）     ✓
  search --filter {"tag":"near"} → c                           ✓
  collection delete docs → 同名重建 → count = 0（未复活）        ✓

Server 重启存活
  create dp(dotproduct) + insert x=[1,0,0], y=[2,0,0]
  重启后 collections/count 正确
  search [1,0,0] → ['y','x']   （dotproduct 下 y 更近，正确）    ✓

release 构建（LTO + panic=abort，与 debug 是不同编译路径）
  cargo build --release --bin coretex → 1m24s，4.5MB          ✓
  release 二进制重跑上述 CLI + Server 重启链路，行为一致        ✓

CLI 默认值与回显（不得「报告请求值而非生效值」）
  不传 -i             → index=brute_force（与库/REST 默认一致）  ✓
  -i hnsww（打错）     → 回显 index=brute_force，manifest 同     ✓
  -m l2（打错）        → 回显 metric=cosine，manifest 同         ✓
  以上四种情况回显与 manifest 逐项一致                            ✓
```

---

## 5. 如何验证

```bash
cd /home/qh/CoreTexDB

# 基座对拍测试（15 项，应全过）
cargo test --test persistence_and_search

# FileStorage 单元测试（15 项，含崩溃恢复 / compaction / TTL）
cargo test --lib coretex_storage::file_store

# 常规构建与 release 构建
cargo build --bin coretex
cargo build --release --bin coretex
```

**对拍基线**：`tests/persistence_and_search.rs` 内的 `reference_distance` / `expected_order`
是**独立重写**的暴力实现，不共享引擎任何代码。两边同时错才可能漏过，因此构成真正的差分校验。

---

## 6. 已知问题（均**非 v0.1 引入**，已定位、未修）

> 这些是本次工作中发现的既有缺陷。v0.1 的交付不依赖它们被修复，但它们是后续工作的输入。

1. **SQL：`ORDER BY` 未生效 + 聚合结果错误**（`src/coretex_sql/`）
   证据：`test_e2e_join_with_aggregate_data` 期望 `Alice`，实得 `Bob`；
   `test_e2e_aggregate_after_inserts` 期望 `1.0`，实得 `2.0`。
   因依赖 HashMap 迭代顺序，**具体哪个测试失败每个进程都不同**（flaky）。

2. **`IVFIndex::search` 在未 `train()` 时返回空**（`src/coretex_index/mod.rs`）
   `centroids.is_empty()` 直接 `return Ok(vec![])`，而 `test_ivf_index` 从不调用 `train`。
   IVF 目前**不可用**，已从默认路径移除。

3. **`TransactionManager::commit` 不从活跃表移除事务**（`src/coretex_transaction.rs`）
   只把 state 设为 `Committed`，`active_count()` 永久留 1 → 事务泄漏。

4. **WAL 与 FileStorage 双重日志**
   v0.1 把 `wal_enabled` 默认改为 `false`：FileStorage 本身就是崩溃安全日志，
   再叠一层 WAL 只会产生第二个真源。WAL 代码保留，需要其事务账本时可重新开启。
   WAL 自身另有既有问题（`test_wal_garbage_collection` 失败）。

5. **REST 请求体未知字段被静默忽略**（既有 serde 默认行为）
   发 `{"metric":"dotproduct"}` 会被忽略并默认成 cosine，而字段名实为 `distance_metric`。
   与本次「拒绝静默降级」的原则相悖，建议后续加 `deny_unknown_fields`（需评估兼容性）。

6. **CLI/HTTP 的 `score` 语义只对 cosine 有意义**
   REST 用 `score = 1 - distance` 呈现结果。对 euclidean/dotproduct/manhattan，
   `distance` 是合法的「越小越近」，但 `score` 会出现负数或含义不清。
   **排序正确性不受影响**，仅呈现层问题。

其余既有失败（bm25、permissions、security、cache、websocket、graphql filter、cost model）
分布在本次**未改动**的模块中，见 §7 基线。

---

## 7. 测试基线（2026-09-15）

| 目标 | 结果 |
| --- | --- |
| `tests/persistence_and_search.rs`（本次新增） | **15 passed / 0 failed** |
| `coretex_storage::file_store`（本次新增） | **15 passed / 0 failed** |
| `cargo build --release` | **成功**，1m24s / 4.5MB |
| lib 单测总体 | 365 passed / 14–16 failed（既有失败，数量随 HashMap 种子波动） |
| `tests/tests_integration_v2.rs` | 10 passed / 1 failed（既有：`tx_aware_insert` 断言 `active_count()==0`，根因见 §6.3） |

---

## 8. 明确不在 v0.1 内

以下模块**存在但未经本次验证**，不应视为可用：时序、空间/GIS、图、湖仓（lakehouse）、
hybrid/BM25、rerank、embedding、分布式/HA、事务、SQL 优化器、gRPC、GraphQL、WebSocket、Python 绑定。

HNSW 已修复三处构造缺陷（取最近 m 个邻居 / 入口点取最高层 / 层级分布 `mL = 1/ln(M)`），
在小数据集上与精确结果一致（已加回归测试），但**仍是近似索引**，大规模下不保证召回率。
IVF 按 §6.2 不可用。
