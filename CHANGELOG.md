# Changelog

All notable changes to CoreTexDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

后续变更在此累积，发布时下移为新的版本段。

---

## [0.2.4] - 2026-09-25

Tag: `v0.2.4`

### Added

- **PQ（乘积量化）索引真正可用**：`pq` 之前存在但每条路径都是坏的——
  `clone_box` 返回空索引导致通过 `get_index()` 的写入不可见；工厂硬编码
  `dimension = 128`；`add`/`search` 都要求先 `train()` 而 `train()` 无调用点；
  `parse_index_type` 把未知类型映射为 `brute_force`，`pq` 根本选不到。
  现在训练是惰性的（攒够样本自动生成码本），缓冲向量被换成了
  `n_subquantizers` 字节的码（8 维下 `compression_ratio = 4.0`），
  检索改由码本解码；样本不足时回退精确扫描，结果与是否训练无关。
  子空间划分按维度自适应推导，`IndexType::PQ` / CLI 帮助均已接线。
- **ANN 索引持久化**：`save_indexes()` 原子写入（temp → fsync → rename → 目录 fsync）
  并带内容校验和；启动时两阶段恢复（先填内存，再按校验和决定"加载"还是"重建"），
  校验和不匹配一律回退重建，避免加载陈旧索引静默漏检。
  CLI：`coretex index save|list`。
- **TTL 入口**：`CoreTexDB::set_vector_ttl / remove_vector_ttl / purge_expired`，
  CLI `coretex ttl set|remove|purge`，
  REST `PUT/DELETE /api/collections/:name/vectors/:id/ttl`、
  `POST /api/admin/purge-expired`。
  存储层新增 `expired_keys()` 接口。
- **开源项目治理文件**：`CONTRIBUTING.md`、`SECURITY.md`、`CODE_OF_CONDUCT.md`、
  `CHANGELOG.md`、issue / PR 模板、`.editorconfig`、`rustfmt.toml`、`docs/`、`examples/`。

### Fixed

- **`purge_expired` 从未清理内存与索引**：原实现用 `list()` 做 purge 前后差集，
  但 `list()` 本身会过滤掉已过期的键，差集恒为空，于是只有存储被删、
  内存与索引留下"幽灵向量"。现在改为按 `expired_keys()` 精确清理。
- **HNSW**：`remove`/`clear`/持久化的锁序补齐为 `vectors → entry_point → graph`；
  `remove` 后把最高层存活节点提升为 entry point，避免指向已删除节点；
  `search_layer` 容忍指向孤儿的邻居。
- **WAL 重放**：改为 last-write-wins 聚合，避免 delete-only 重放顺序造成的错误结果。
- **备份/恢复**：`coretex-backup` 只看 `argv[1]`；恢复失败时回滚已移开的目录。
- **`.gitignore`**：`coretex_data/` 规则会误屏蔽源码目录 `src/coretex_data/`
  （该模块下任何新文件都会被静默漏提交），已改为根锚定 `/coretex_data/`。

### Changed

- **单二进制分发**：`coretex` 按 `argv[0]`/子命令承担 `server`、`backup`、
  `doctor` 等全部角色，删除 5 个壳 `[[bin]]` 与 `src/bin/*.rs`；
  scripts/systemd/release workflow/README 同步改为 `coretex <role>`。

### Removed

- 已被 `persist`/`load_index` 取代的旧 `save_to_file`/`load_from_file`（每种索引各一对）。
- 未被跟踪的 `src/coretex_generated.rs` 副本（gRPC 改用 `OUT_DIR` 生成）。

---

## [0.2.3] - 2026-09-25

Tag: `v0.2.3` → `c14e486`

### Added

- 完整安装根目录打包：`bin/`、`lib/`、`include/`、`config/`、`share/`、
  `scripts/`、`systemd/`、`logrotate/` 与 `data/` 骨架。
- B-C-D-D `.cdb` 的 `encrypt` / `decrypt` / `info` / `keygen` CLI。
- WAL 段命名 `wal-NNNNNN.log`，严格发现、max+1 轮转、锁序写入文档。
- `metadata/config.toml` 与 `metadata/auth.json` 原子创建（temp + rename，绝不覆盖）。
- `doctor` 按 `wal_enabled` 分支（Plan A）。
- 运行期不再创建安装根的 `bin/`、`include/`（仅安装布局才有）。

### Fixed

- 存储、WAL、事务与 HNSW 的耐久性与并发问题（`4b508fc`）。
- CLI：恢复被搁置的目录创建、搜索结果 JSON 元数据；新增 Windows 验收脚本（`7c68885`）。

> 本版本仍为**多二进制**发行（`coretex`、`coretexd`、`coretex-cli`、
> `coretex-migrate`、`coretex-backup`、`coretex-healthcheck`）；
> 单二进制分发见 [0.2.4]。

---

## [0.2.2] - 2026-09-24

Tag: `v0.2.2` → `e12b6f6`

- 打包完整安装根目录架构（`feat: package full install-root architecture into V0.2.2 release`）。

---

更早的 tag（`v0.1`、`v0.2.1`、`v1.0.0`、`v1.0.1`、`v1.0.12`、`v1.1.0`）
请用 `git tag -l` 与 `git log <tag>` 查看，其变更未在本文件中逐条整理。
