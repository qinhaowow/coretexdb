# Architecture / 架构说明

本文说明 CoreTexDB 的分层、数据流与并发约定。快速上手请看
[`README.md`](../README.md)，开发流程请看
[`CONTRIBUTING.md`](../CONTRIBUTING.md)，路线图见 [`roadmap.md`](roadmap.md)。

---

## 1. 单二进制分发

编译只产出一个 `coretex` 可执行文件，角色由 `argv[0]` 与子命令分担：

```
coretex server      服务端（REST / GraphQL / gRPC / WebSocket）
coretex backup      备份与恢复
coretex doctor      健康检查
coretex init        初始化数据目录
coretex vector|index|ttl|search|dump …   数据与索引操作
```

`src/main.rs` 负责分发，具体实现全部在 `src/coretex_cli/`。
早期的 `coretexd`、`coretex-cli`、`coretex-migrate`、`coretex-backup`、
`coretex-healthcheck` 5 个壳 `[[bin]]` 已删除，统一走子命令。

---

## 2. 分层

```
┌─────────────────────────────────────────────────────────┐
│ 入口：main.rs（argv[0]/子命令分发）                      │
├──────────────┬──────────────┬───────────┬───────────────┤
│ coretex_cli  │ coretex_api  │coretex_grpc│ coretex_query │
│  CLI 命令    │ REST/GraphQL │  gRPC/Protobuf│ SQL       │
│              │ WebSocket    │           │               │
├──────────────┴──────────────┴───────────┴───────────────┤
│ lib.rs → CoreTexDB：配置、init()、对外 API、save_indexes │
├─────────────────────────────────────────────────────────┤
│ coretex_data：DataManager                               │
│   集合元数据 · 写入路径 · WAL · 恢复 · TTL · 过滤与检索  │
├───────────────────────┬─────────────────────────────────┤
│ coretex_storage       │ coretex_index                   │
│  StorageEngine        │  IndexManager                   │
│  Memory / File /      │  brute_force · hnsw · ivf · pq  │
│  RocksDB(可选)        │  持久化 persist/load            │
├───────────────────────┴─────────────────────────────────┤
│ coretex_core（类型/配置/错误） · coretex_utils · WAL    │
└─────────────────────────────────────────────────────────┘
```

模块的完整清单见 `src/lib.rs` 的 `pub mod` 列表。

---

## 3. 写入路径

一次 `insert` / `update` 的顺序固定为：

```
WAL → storage → memory → index
```

**为什么是这个顺序**：先落 WAL 才能在崩溃后重放；先写存储再改内存，
保证"内存里的一定持久化过"；最后更新索引，索引丢了也能重建。

失败回滚：任一步失败即返回错误，内存与索引不再推进。

---

## 4. 启动与恢复

`CoreTexDB::init()`：

1. 打开 `metadata/config.toml`、`auth.json`（原子创建：temp + rename）；
2. **两阶段恢复** `restore_from_storage()`：
   - 阶段一：把存储里的向量全部填进内存；
   - 阶段二：按存储内容计算**校验和**，与磁盘上的索引文件比对——
     匹配则**加载**索引（省掉重建），不匹配/缺失/损坏则**重建**。
3. 若启用 WAL，做 `recover_from_wal()`：**last-write-wins 聚合**，
   同一键多次出现时以最后一次写为准，顺序不再影响结果。

索引文件的校验和绑定的是**存储中的 `(id, vector)` 集合**，不是索引文件自身，
因此"索引陈旧"总能被检出，避免加载到会漏检/错排的旧索引。

---

## 5. 持久化布局

```
{base}/
  data/coretex/
    collections/          集合数据
    indexes/vector/        ANN 索引文件（*.json，原子写）
    metadata/              config.toml, auth.json, metadata.json
    store/                 存储段文件
  wal/                     wal-NNNNNN.log
  backup/{full,incremental,snapshots}
  logs/audit/
  temp/  versions/
```

索引文件是信封结构：

```json
{ "format": 1, "index_type": "hnsw", "metric": "cosine",
  "count": 42, "checksum": "…", "data": { … } }
```

写入流程：`temp 文件 → fsync → rename → 目录 fsync`，
崩溃只会留下旧文件或新文件，绝不会留下半个。

只有 `hnsw` / `ivf` / `pq` 支持落盘（`brute_force`、`scalar` 走重建）。
CLI 可以手动触发：`coretex index save`、`coretex index list`。

---

## 6. 索引类型

| 类型 | 说明 | 何时用 |
| --- | --- | --- |
| `brute_force` | 精确全扫 | 小集合、要求 100% 准确 |
| `hnsw` | 分层可导航小世界图 | 通用，大集合低延迟 |
| `ivf` | 倒排文件（聚类 + nprobe） | 内存敏感、可接受近似 |
| `pq` | 乘积量化：向量压成 `n_subquantizers` 字节 | **内存优先**；样本攒够后惰性训练，样本不足自动回退精确扫描 |
| `scalar` | 标量量化 | 占位实现 |

- 选择入口：`create_collection_with_index(name, dim, metric, type)`、
  CLI `--index-type`；映射逻辑在 `parse_index_type()`。
- 索引由 `IndexManager` 管理，`get_index()` 返回的是**共享句柄**
  （`clone_box` 共享 `Arc`），不是副本——历史 bug 就出在这里。

---

## 7. 并发与锁顺序

**必须遵守**（否则会死锁，新增锁请在注释里写明顺序）：

| 场景 | 顺序 |
| --- | --- |
| HNSW | `vectors → entry_point → graph` |
| PQ | `training → original_vectors → vectors`（与 `persist` 一致） |
| 索引管理器 | 先取索引读锁，再进入索引内部锁 |

原则：**同一把锁不要嵌套获取两次**；跨锁操作先各自短暂持锁、拷贝数据再计算。

---

## 8. TTL 生命周期

1. `set_vector_ttl(collection, id, secs)` → 存储层记录过期时间戳（可持久化）；
2. `purge_expired()`：
   - **先**取 `expired_keys()`（`list()` 会隐藏过期键，不能用来做差集）；
   - 再 `purge_expired()` 删存储；
   - 按**已知集合名的最长前缀**解析 `collection:id`（id 里可以含 `:`），
     同步从内存与索引中删除。
3. 入口：CLI `coretex ttl purge`、REST `POST /api/admin/purge-expired`。

---

## 9. 测试布局

```
tests/                          端到端集成测试
  persistence_and_search.rs     持久化 + 搜索
  index_persistence.rs          索引落盘/加载/陈旧重建
  pq_index.rs                   pq 集合端到端
  ttl.rs                        TTL 过期清理
src/*/tests.rs                  模块内单元测试（include! 进 mod tests）
src/lib.rs (mod tests)          门面层测试
benches/                        基准
```

**push 前 `cargo test` 必须全绿**（当前约 485 个用例）。
