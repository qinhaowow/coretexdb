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
| B1 | **完整 C FFI**：`include/coretexdb.h` 目前是占位符，与 README 声称的能力不符。补齐 connect/collection/insert/search/backup 全套 + cbindgen + C 示例 + 测试 | +3.0k 行 |
| B2 | **hybrid / BM25 / rerank 接线**：`coretex_hybrid`、`coretex_bm25`、`coretex_rerank` 已存在但没接进 `search` | +1.5k 行 |
| B3 | **REST/GraphQL 补全**：TTL 已补；余下 hybrid、分页参数、错误码统一 | +1.0k 行 |
| B4 | **孤立模块处理**：`coretex_ann`、`coretex_graph`、`coretex_tantivy` 等 3.8k 行模块无调用点——要么接线，要么删除并说明取舍 | 视决定 |
| B5 | **过滤索引**（对应 A3）：倒置索引让元数据预筛也变成次线性 | +1.5k 行 |
| B6 | **Python 打包元数据缺失**：`python/` 没有 `pyproject.toml`/`setup.cfg`，`setup()` 不带任何元数据，`pip install -e .` 拿不到包名与依赖（CI 里靠先删 `pyproject.toml` 绕开 maturin） | +0.5k 行 |
| B7 | **Python 命名不一致待决策**：包名 `coretexdb` 但类名是 `CortexDB`/`CortexDBClient`——是加 `CoreTexDB` 别名，还是统一改名（破坏性） | 决策 |
| B8 | **`python/examples/basic_usage.py` 等示例的品牌与用法核对**（部分文案仍写 CortexDB） | 少量 |

---

## 阶段 C：Redis 级系统能力 ⬜

| # | 项 | 预估 |
| --- | --- | ---: |
| C1 | **主从复制**：WAL 传输、全量 + 增量同步、只读副本 | +3.0k 行 |
| C2 | **分片/集群**：slot 路由、节点发现、迁移 | +4.0k 行 |
| C3 | **Pub/Sub**：频道订阅 + 推送 | +1.0k 行 |
| C4 | **快照与后台重写**：AOF/RDB 式格式 + 崩溃恢复演练 | +2.0k 行 |
| C5 | **慢查询日志 / 命令统计 / INFO** | +1.0k 行 |

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
