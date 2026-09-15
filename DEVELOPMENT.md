# 开发交接文档（CoreTexDB）

> 目的：让下一次接手的人（或下一个会话）在 5 分钟内知道**什么能用、怎么跑、从哪儿继续**。
> 权威的状态契约在 [`SCOPE-v0.1.md`](SCOPE-v0.1.md)；本文是操作手册。

---

## 1. 三十秒速览

这个仓库有 **45k 行代码、47 个模块、16 个 CLI 子命令**，但**只有一小部分经过验证**。

**能用（v0.1 已验证）**：向量的增删改查、相似度检索、元数据过滤、持久化与重启恢复。
**不能用 / 未验证**：SQL、图、时序、空间、湖仓、分布式、事务、gRPC、GraphQL、WebSocket、Python 绑定、embedding、rerank、BM25。

判断标准很简单：**`SCOPE-v0.1.md` §2 的 10 条 Guarantees 里有测试证据的，才叫能用。**

---

## 2. 环境准备

```bash
# 基础依赖
sudo apt install protobuf-compiler        # build.rs 需要 protoc（tonic-build）
rustup target add x86_64-pc-windows-gnu   # 仅交叉编译 Windows 时需要
sudo apt install gcc-mingw-w64-x86-64 binutils-mingw-w64-x86-64
```

`.cargo/config.toml` 已配好：

- crates.io 走清华镜像
- `[target.x86_64-pc-windows-gnu]` 的 linker/ar

---

## 3. 构建

```bash
cargo build                          # 调试构建（含调试信息，约 229MB）
cargo build --release                # 原生产物 → target/release/coretex

# Windows 交叉编译（从 Linux/WSL）
cargo build --release --target x86_64-pc-windows-gnu
#   → target/x86_64-pc-windows-gnu/release/coretex.exe
```

产物格式对照（别搞混）：

| 路径 | 格式 | 用途 |
| --- | --- | --- |
| `target/release/coretex` | ELF 64-bit PIE | Linux 可执行 |
| `target/x86_64-pc-windows-gnu/release/coretex.exe` | PE32+ | Windows 可执行 |
| `target/release/libcoretexdb.rlib` | ar 归档 | 库，**不能运行** |

**`x86_64-pc-windows-msvc` 在 Linux 下无法构建** —— 需要微软的 `link.exe`，只存在于 Windows 的 Visual Studio。别再试了，走 `-gnu`。

---

## 4. 测试

```bash
# 基座对拍测试（15 项，应全过）—— 改检索/持久化后必跑
cargo test --test persistence_and_search

# 存储引擎单测（15 项：崩溃恢复 / compaction / TTL / 分段滚动）
cargo test --lib coretex_storage::file_store

# 全量（注意：有既有失败，见 §7）
cargo test --no-fail-fast
```

**当前基线**：

| 目标 | 结果 |
| --- | --- |
| `tests/persistence_and_search.rs` | 15 passed / 0 failed |
| `coretex_storage::file_store` | 15 passed / 0 failed |
| lib 全量 | 365+ passed / **14–16 failed（既有）** |

对拍测试里的 `reference_distance` / `expected_order` 是**独立重写**的暴力实现，
不共享引擎任何代码。改检索逻辑时**先看这个文件**，它能在你改坏时救你一命。

---

## 5. 运行

```bash
./run-server.sh                    # Linux/WSL：起 REST 服务（默认 :5000）
./run-server.bat                   # Windows：同上

# 或手动
./target/release/coretex server -a 0.0.0.0 -p 5000 --data-dir ./coretex_data
```

CLI 每条命令是**独立进程**，共享同一个 `--data-dir`（默认 `./coretex_data`）：

```bash
B=./target/release/coretex

$B collection create docs -d 4 -m euclidean -i brute_force
$B vector insert docs v1 "1,0,0,0" -m '{"tag":"red"}'
$B vector insert docs v2 "0,1,0,0"
$B vector count docs
$B search "1,0,0,0" -c docs -k 3
$B search "1,0,0,0" -c docs -k 3 --filter '{"tag":"red"}'
$B collection list -v
```

> **参数名易错**：地址/端口是 `-a` / `-p`，**不是** `--host` / `--port`；
> 集合名是**位置参数**，不是 `--name`。这些在旧 README 里全是错的，已修正。

---

## 6. 数据布局与代码地图

```text
coretex_data/
  data/metadata.json           ← manifest：集合的维度/度量/索引类型（原子写：临时文件+rename）
  data/store/store-NNNNNN.log  ← 向量日志：追加写 + CRC32 + 崩溃尾部截断 + compaction
```

真正需要看的源码只有 5 个文件：

| 文件 | 职责 |
| --- | --- |
| `src/coretex_storage/file_store.rs` | **持久化引擎**（v0.1 新增，最重要） |
| `src/lib.rs` | `CoreTexDB`：manifest 读写、恢复流程、配置 |
| `src/coretex_data/mod.rs` | `DataManager`：集合/向量内存态、检索、恢复 |
| `src/coretex_index/mod.rs` | 索引；`metric_distance()` 是**距离度量的唯一真源** |
| `src/coretex_cli/mod.rs` | CLI；`DEFAULT_INDEX_TYPE` 是**默认索引的唯一真源** |

其余 40+ 个 `coretex_*.rs` 目前是**装饰**：能编译，但未经端到端验证。

---

## 7. 已知问题（既有，非 v0.1 引入）

这些已定位但未修，改动前先知道它们的存在：

1. **SQL `ORDER BY` 未生效 + 聚合错误**（`src/coretex_sql/`）。依赖 HashMap 迭代顺序，
   因此**每轮失败的具体测试都不一样**（flaky）。
2. **`IVFIndex::search` 未 `train()` 时无条件返回空** —— IVF 实际不可用，已移出默认路径。
3. **`TransactionManager::commit` 不移除活跃事务** → 事务泄漏（`active_count()` 永不归零）。
4. **WAL 与 FileStorage 双重日志** → `wal_enabled` 默认 `false`，避免两个真源。

---

## 8. 下一步（按依赖排序）

1. **Python 绑定跑通** —— `python` feature 从未编译过，这是 v0.1 原始目标里唯一没验的一格。
   `pip install` → `pyo3`/`pyo3-asyncio` 需要 `--features python`。
2. **启动时做 manifest ↔ 索引一致性校验** —— 目前改 `dimension` 需手动重建。
3. **把 §7 的既有缺陷各开一条 issue**，别再让它们藏着。
4. 之后才碰：时序/空间/图 作为 filter 插件 → RAG hybrid → ROS2 适配器 → HNSW 调优。

---

## 9. 踩过的坑（省你几小时）

**依赖解析**：改 `Cargo.toml` 的 GraphQL 相关依赖时注意版本钉死 —— `async-graphql` 必须
留在 `7.0.13`，7.0.14 起改用 axum 0.8，与项目的 axum 0.7 冲突。

**跨平台**：`/dev/null` 在 Windows 上是 `NUL`。任何占位文件都会让 Windows 构建在运行时炸掉，
而 Linux 上完全看不出来。**新增平台相关代码后，必须在 Windows 产物上真跑一遍。**

**WSL 网络**：WSL2 里访问 Windows 上绑 `127.0.0.1` 的服务**不通**（不同网络命名空间）。
验证 Windows 侧服务要用 `/mnt/c/Windows/System32/curl.exe`。
`cmd.exe` 也不在 PATH（`appendWindowsPath=false`），需全路径调用。

**构建耗时**：全新目标三元组需要重编全部约 600 个依赖（Windows 目标首次约 32 分钟）；
同目标增量约 1 分钟。别在没睡好时跑它。

**磁盘**：`target/` 目前约 11GB（debug 缓存为主）。清理用：

```bash
rm -rf target/debug/incremental     # 3.5GB，纯增量缓存，自动重建
cargo clean                          # 全部清掉（下次全量重编）
```
