# Changelog

All notable changes to CoreTexDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

本段汇总 `v0.2.4` 之后的全部变更。主题是**清除会撒谎的代码**——多处实现
看起来在工作，实际不产生任何效果，或返回与事实相反的结果。

### 移除的假接口

- **`POST /raft/append_entries` 已删除**（`aba58f8`）。它硬编码返回
  `success: true`，且在认证白名单内——等于告诉 leader「日志已复制」而实际
  什么都没写。选择删除而非打桩：`coretex_failover` 提供了 `RaftLog` 与
  `HttpRaftRpc`，但没有任何地方构造 `FailoverManager`，`ApiState` 里也没有
  日志，handler 只能回显一个常量。`HttpRaftRpc` 请求的 `/raft/request_vote`
  与 `/raft/heartbeat` 从未注册，故 leader 选举与心跳同样 404。
  `tests/rest_metrics.rs::removed_raft_endpoint_stays_404` 守住这一点。

### CLI 中无法执行的子命令改为明确失败

- `157762d`：`cluster` 与 `user revoke` 此前打印成功信息却什么都没做。
  按项目既有的 `--compression` 约定，保留代码、以退出码 2 退出并说明原因。

### 安全修复

- **`RateLimiter` 无界内存增长**（无需认证即可触发）。`check_rate_limit` 为每个
  identifier 永久创建 map 条目，`retain` 只清理条目内部的时间戳、从不删键。
  REST 按客户端 IP 建键、gRPC 按 token 建键，**两者都是攻击者可控值**。
  改为每次检查回收全部过期条目，并加 10,000 标识符上限应对突发一次性标识符。
- **gRPC 限流形同虚设**：`_rate_limiter` 创建后即丢弃，启动横幅却照打
  `Rate limit: N req/min`。已接线；**限流键由 token 改为客户端地址**——按 token
  计费意味着换个 token 即可绕过。横幅与 CLI 仅在真正启用时打印数值。
- **`AuthInterceptor` 公共方法白名单恒不命中**：读取 `x-grpc-method` header，
  而 tonic 从不注入、客户端也从不发送，`path` 恒为空。开启 `--auth` 后连
  `HealthCheck` 都要 token。经查证 tonic 源码，其 `InterceptedService::call`
  在调用 interceptor **之前**就剥离了 HTTP URI（源码注释："Tonic requests do
  not preserve the URI"），故 interceptor 在结构上无法得知方法名。改由 tower
  `AuthLayer` 在剥离前解析，再经 header 传递。
- **JWT 强制 HS256**（`0049571`），并修复 gRPC 丢弃调用方身份的问题
  （`sub.parse::<u64>()` 永远匹配不上 `user_12345` 这类 id）。
- **认证用户持久化**（`bc1a4a7`）：`AuthService::new()` 仅存内存，每次重启
  抹掉全部账户。现统一写入 `{base}/data/coretex/metadata/auth.json`，原子保存。

### 可观测性

- **`GET /metrics` 此前不存在**：`DatabaseMetrics` 在 `lib.rs` 里被 `pub use`
  重导出，但生产代码零调用，没有任何地方构造实例。现由 `ApiState` 持有，
  经 axum middleware 为全部 REST 请求埋点（路由模板 + 状态码 + 延迟）。
  该路由**不在认证白名单**内——series 暴露集合与向量数量。
  metrics 层置于 auth **之外**，使 401/429 同样被计数；层序由
  `auth_rejections_are_counted_in_metrics` 固定（已在反向层序下验证其失败）。
- **histogram 永久内存泄漏**：`HashMap<String, Vec<f64>>` 只 push 从不移除，
  每 series 每请求增长 8 字节。改为固定容量环形缓冲（1024 样本/series）；
  `count`/`sum` 保持精确，仅 `_avg` 为蓄水池估计。
- **Prometheus 输出格式三处不合规**：标签原写作 `name_k=v`（Prometheus 会视为
  字面指标名，所有带标签 series 均为垃圾）；`_sum` 行携带两个数值（第二个被当作
  时间戳，样本被静默丢弃）；缺少 `# HELP` / `# TYPE`。
- **`AlertManager` 的错误告警全部失效**：`check_threshold` 按精确 key 查表，而
  `record_error` 存储为 `coretexdb_errors_total:type=io`，裸名规则恒读到 0.0。
  现按前缀聚合全部标签组合。原有测试专门锁定该错误行为，已替换。
- **`make_key` 标签顺序不确定**：`HashMap` 迭代顺序随机，两个标签的同一 series
  可能存为两个 key，计数器被劈成两半。现排序后拼接。

### 正确性修复

- `PersistentStorage` bincode 长度前缀偏移 8 字节（真实数据损坏）。
- `BatchedStreamEmbedder::flush()` 丢弃尾部数据。
- `RaftLog::commit_up_to` 会清空日志本身（已识别，未修——见 roadmap）。

### 构建与测试

- `--features full` 恢复可编译（`s3` 空 feature、rocksdb 0.20→0.22 适配 GCC 13、
  pyo3-asyncio 的 `tokio`→`tokio-runtime`）；移除从未编译过的 `onnx`/`tantivy`。
- CI 新增 per-feature 编译门禁（10 个 feature 逐一 + `full --all-targets`），
  并移除 `RUSTFLAGS=--cap-lints warn`。
- `examples/mvp.rs`：8 步带断言的 MVP 定义，已接入 CI。
- 替换 3 处同义反复断言（无论代码怎么改都通过）；修正 2 个写法上不可能通过的测试。
- `tests/rest_metrics.rs`：8 个端到端测试，经 `tower::ServiceExt::oneshot` 驱动
  **真实 router**（含认证与 metrics 中间件），不绑定端口。`build_app` 自
  `start_server_with_db` 抽出并公开，供嵌入式场景复用。

### 其它

- `GET /console` 浏览器控制台（`ddbddd8`），页面经 `include_str!` 内嵌，
  支持登录、集合/向量 CRUD、搜索与修复验收自检。
- 会话记忆文档刷新（`57c6db4`）；元数据倒排索引使带过滤的预选检索转为亚线性
  （`8a6e6ee`）；PEP 621 打包；C ABI 接口与漂移守卫；两阶段重排接线；
  混合检索（向量 + BM25 + RRF）经 REST 与 CLI 暴露。

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
