# CoreTexDB V0.2.5 Release Notes

## Highlights (V0.2.5)

本版主题是**清除会撒谎的代码**，并把「已存在但从未接线」的模块接到对外
接口上。`v0.2.4..HEAD` 共 39 个提交、84 文件、+17112 行。

### 修复：会让运维看到假象的地方

这些不是崩溃或性能问题，而是**代码在正常工作、但报告的不是真实情况**——
排查时会被直接带偏：

- **`/raft/append_entries` 恒返回 `success: true`**：告诉 leader「日志已复制」
  而实际什么都没写。已删除而非打桩（`aba58f8`）。
- **gRPC 指标结构性恒为 0**：`auth_failures` 与 `rate_limited` 两个字段从存在
  起就没有任何代码写入，服务器拒绝掉全部请求时仍报告 0 次认证失败——而那正是
  撞库时运维要看的数字（`ace4c20`）。
- **`GET /metrics` 三处会撒谎**：端点此前不存在，`DatabaseMetrics` 被重导出
  却无人填充（`2f62bfe`）。
- **审计日志从未接线**：`AuditLogger` 被重导出、`init_metadata` 也建了
  `logs/audit` 目录，但没有任何地方构造实例，安装树里那个目录一直是空的；
  首条写 `[{...}]`、其后写 `,{...}`，产出的东西既非 JSON 也非 JSONL，
  `serde_json` 会因第 2 行的前导逗号直接拒绝；事件 ID 是 `audit_{unix秒}`，
  同一秒内全部事件共用一个 ID（`59e4186`）。现在由 `ApiState` 持有，
  登录成功与失败、被拒的 token 都入账。
- **`HttpTransport` 从不检查 HTTP 状态码**：503 的错误页被报成
  "expected value"，指向 JSON 而非真正的原因；若错误体恰好是合法 JSON 形状
  则**被静默接受**——replica 会套用假快照、把位置跳到从未收到的数据之后、
  并报告成功。这是复制唯一无法自行恢复的失败模式（`462a506`）。
- **认证用户只存内存**：每次重启抹掉全部账户（`bc1a4a7`）；HS256 未强制、
  gRPC 调用方身份被丢弃（`0049571`）。
- **限流器无界**，且方法白名单里有一个死方法（`adc70ac`）。

### 修复：会产生错误结果的地方

- **恢复时丢弃 `CreateCollection`**：`recover_from_wal` 重放日志时不读集合
  schema，于是度量靠猜——恢复后 `euclidean` 可能变成 `cosine`（`568831c`）。
- **manhattan SIMD 恒返回 0**：`_mm_andnot_ps` 用法错误使结果恒为零；
  对拍测试首轮即抓出（`055d20f`）。
- **等距向量的返回顺序不确定**：只按距离排序，等距时顺序取决于 `HashMap`
  迭代序，重启后同一查询可能给出不同顺序、副本间排名可能不一致。
  8 处排序补 id tie-break（`b5ef5a5`）。
- **`SearchResult::Ord` 只比 distance**：而它正是 HNSW 搜索层
  `BinaryHeap<Reverse<SearchResult>>` 的排序依据——上一条只修了显式
  `sort_by`，堆路径漏了。另：派生的 `PartialEq` 比较 NaN 字段，与 `cmp`
  把 NaN 判 Equal 矛盾，堆会自相矛盾（`2c86ad1`）。
- **批量流式写入丢数据**、3 处同义反复断言、2 个不可能通过的 WAL 测试
  （`86c45d3`、`49a3430`）。
- **不可达的 CLI 子命令静默成功**：`cluster` / 用户吊销等未接线的子命令
  退出码为 0（`157762d`）。
- **`--features full` 编译不过**（`1dc9234`、`bd33d41`）。

### 新能力

| 领域 | 内容 |
| --- | --- |
| 检索 | 混合检索（BM25 + 向量，RRF 融合，`ee5b02e`）、二阶段 rerank（`91353e9`） |
| 索引 | 倒排元数据索引，过滤预筛从 O(n·d) 降到亚线性（`8a6e6ee`、`25899d9`） |
| 接口 | C ABI + 漂移守卫（`91353e9`）、浏览器控制台 `/console`（`ddbddd8`）、GraphQL `/graphql` + `/graphql/ws`（`b8c844a`） |
| 分布式 | 拉复制 + 状态码校验（`462a506`）、16384 槽路由 / 节点发现 / 集合迁移（`202b5c0`）、Pub/Sub + WebSocket 订阅（`c17208f`） |
| 运维 | 一致性快照 / 后台快照 / WAL 压实（`568831c`）、命令统计 + 慢查询日志 + INFO（`dd04926`）、审计日志接线（`59e4186`）、遥测一键开关（`9a89a8a`） |
| 打包 | Python PEP 621 + 品牌一致命名与 pre-1.0 别名（`663b295`） |

### 性能

- SIMD 距离内核（`#[target_feature]` 特化 + 一次探测分发）：**4.9x / 5.4x**。
- 批量 WAL 写入改为一次 fsync（原先每条一次）：**1048x**。
- 全量扫描改为借用 id + 定容 max-heap，`O(n log k)`。
- **并行扫描实测无收益，已回退**：数字写进了源码注释，以免日后有人再盲试一次。

### 测试

- 每 feature 独立编译门禁（`e87c5c9`）：`onnx` / `tantivy` 已知失败，
  标记为 `continue-on-error` 只记录现状，不掩盖。
- 故障注入（`tests/fault_injection.rs`，8 例）：按字节偏移截断、翻转校验和、
  删段、半行 WAL。
- 差分恢复对拍（`tests/differential.rs`）：五条恢复路径 × 四度量逐字段比对。
- 审计端到端（`603da0a`）、`examples/mvp.rs` 作为可执行 MVP 定义并在 CI 真跑
  （`7553826`）。

### 版本号单一真源（本次）

版本号此前散落 35 处，上一版发布改了 17 个文件才能对齐，且**没有任何机制
会在漏改时报错**——`release.yml` 里写死 `V0.2.4`，真发布出一个装着 0.2.5
二进制却叫 `CoreTexDB-V0.2.4-<target>` 的包，故障要等到装它的人手上才暴露。

现在：

- 权威源是 `Cargo.toml` 的 `version`；`release.yml` 从中派生归档名，
  三个安装脚本从 `VERSION`（或 `Cargo.toml`）派生默认安装根。
- `tests/version_consistency.rs` 把这条约定变成门禁：4 条断言覆盖
  `VERSION` 与 `Cargo.toml` 一致、`DB_VERSION` 跟随、打包文件不出现字面量
  版本、`VERSION` 只有一行。**四个负向用例逐一验证过会变红**——第一版实现
  里那个检查函数写错了（needle 与扫描起点错位，永不命中），是这个验证抓出来的。
- 顺带修掉 `release.yml` 的 SHA256 步骤静默失效：`$ASSET` 为空时原先跳过
  而不报错，产物可以没有校验和就发布出去。

> `Cargo.toml` 在本地以 CRLF 行尾检出，因此版本提取用 `cut` 而非依赖 `$`
> 锚点的 `sed`——后者会静默返回空串，归档名变成 `CoreTexDB-V-<target>`。

## What's changed since V0.2.4

39 个提交。分组摘要（完整清单见 `git log v0.2.4..v0.2.5`）：

| 领域 | 提交 |
| --- | --- |
| 清除会撒谎的代码 | `aba58f8` `ace4c20` `2f62bfe` `59e4186` `462a506` `bc1a4a7` `0049571` `adc70ac` |
| 结果正确性 | `568831c` `055d20f` `b5ef5a5` `2c86ad1` `86c45d3` `49a3430` `157762d` |
| 检索与索引 | `ee5b02e` `e122d80` `91353e9` `8a6e6ee` `25899d9` |
| 分布式与运维 | `202b5c0` `c17208f` `dd04926` `9a89a8a` `d4f9dc8` `ddbddd8` `b8c844a` |
| 构建与测试 | `1dc9234` `bd33d41` `e87c5c9` `7553826` `603da0a` |
| 文档 | `57c6db4` `671187d` `3a8f3a1` `6706a3f` `8ef8b36` |

测试：`cargo test` lib **489 passed / 0 failed**，17 个集成套件全绿。

## Install-root layout (V0.2.5)

```
CoreTexDB-V0.2.5/
  bin/          coretex                      # 单二进制（按 argv[0]/子命令分发）
  lib/          libcoretexdb.so|.dylib|.a, coretexdb.dll|.lib
  include/      coretexdb.h                  # C ABI（B1）
  config/       coretex.toml, logging.yaml, backup.toml, security.toml, metrics.toml, {dev,staging,prod}/
  share/        doc/, examples/
  scripts/      start/stop/status/install/upgrade/uninstall/backup/restore/...
  systemd/      coretexd.service + timers + tmpfiles
  logrotate/    coretexd
  data/         coretex/{collections,indexes,metadata,store}, wal, backup/{full,incremental,snapshots}, logs/audit, temp, versions
```

> 目录名里的版本由 `scripts/install.sh` 在运行时从 `VERSION` 派生，不再写死。

## Upgrade

Use `scripts/upgrade.sh` from the install root. See `share/doc/INSTALL.md`.

从 V0.2.4 升级：安装根目录名从 `CoreTexDB-V0.2.4` 变为 `CoreTexDB-V0.2.5`，
systemd 单元与 logrotate 路径已同步更新，升级后需 `systemctl daemon-reload`。

---

## 归档：V0.2.4 Release Notes

### Highlights (V0.2.4)

- **单二进制分发 / Single binary**：只剩 `coretex` 一个可执行文件，按
  `argv[0]` 与子命令承担 `server` / `backup` / `doctor` 等全部角色；
  5 个壳 `[[bin]]`（`coretexd`、`coretex-cli`、`coretex-migrate`、
  `coretex-backup`、`coretex-healthcheck`）已删除，scripts / systemd /
  release workflow / README 全部改为 `coretex <role>`。
- **ANN 索引持久化**：`hnsw` / `ivf` / `pq` 可落盘。原子写（temp → fsync →
  rename → 目录 fsync），文件带**基于存储内容**的校验和；启动时两阶段恢复，
  校验和匹配就加载、不匹配就重建，陈旧索引永远不会被静默使用。
  CLI：`coretex index save|list`。
- **PQ 真正可用**：此前 `pq` 每条路径都是坏的（`clone_box` 返回空索引、
  工厂硬编码维度 128、`train()` 无调用点、`pq` 选不到）。现在惰性训练 +
  码本解码（8 维下压缩比 4.0），样本不足自动回退精确扫描。
- **TTL 完整入口**：lib `set_vector_ttl` / `remove_vector_ttl` /
  `purge_expired`，CLI `coretex ttl set|remove|purge`，REST 3 条路由；
  并修掉 `purge_expired` **从不清理内存与索引**的真 bug
  （`list()` 会隐藏过期键，导致前后差集恒为空）。
- **正确性修复**：HNSW `remove`/`clear`/持久化锁序补齐并修正 entry point；
  WAL 重放改为 last-write-wins；恢复失败时回滚已移开的目录。
- **开源项目整备**：`CONTRIBUTING` / `SECURITY` / `CODE_OF_CONDUCT` /
  `CHANGELOG`、issue 与 PR 模板、`docs/architecture.md`、`docs/roadmap.md`、
  可运行的 `examples/`（CI 会真跑）、lint 与 examples 两道 CI 门禁。
- **修复从未被编译或检查的代码**：`.gitignore` 的 `coretex_data/` 误屏蔽
  源码目录 `src/coretex_data/`；`benches/vector_index.rs` 从未编译过；
  Python 文档与测试 `import cortexdb`（模块实为 `coretexdb`）；
  5 个 deny 级 clippy error（含 `commit_up_to` 里恒不循环的死循环）。

### What's changed since V0.2.3

| 提交 | 说明 |
| --- | --- |
| `836969d` / `72442bf` | 单二进制：`argv[0]` 分发 + 删除 5 个壳 bin |
| `ecb0ccf` | HNSW 锁序 / WAL 确定性重放 / 恢复回滚 |
| `a52f20e` | 修复 last-write-wins 断言 |
| `70e55c1` | ANN 索引持久化 + TTL 入口 + `purge_expired` 修复 + 删死代码 |
| `d3f78c7` | PQ 真量化索引（惰性训练 / 码本解码 / 可选择） |
| `1881474` | 修复 5 处从未被编译或检查的缺陷 |
| `a2f19bb` | 开源治理文档、示例、CI 门禁 |

测试：`cargo test` **485 passed / 0 failed**；`cargo clippy --all-targets`
**0 error**；`cargo doc --no-deps` 无 rustdoc 警告。

### Install-root layout (V0.2.4)

```
CoreTexDB-V0.2.4/
  bin/          coretex                      # 单二进制（按 argv[0]/子命令分发）
  lib/          libcoretexdb.so|.dylib|.a, coretexdb.dll|.lib
  include/      coretexdb.h                  # C API 占位，完整 FFI 见路线图 B1
  config/       coretex.toml, logging.yaml, backup.toml, security.toml, metrics.toml, {dev,staging,prod}/
  share/        doc/, examples/
  scripts/      start/stop/status/install/upgrade/uninstall/backup/restore/healthcheck/...
  systemd/      coretexd.service + timers + tmpfiles
  logrotate/    coretexd
  data/         coretex/{collections,indexes,metadata,store}, wal, backup/{full,incremental,snapshots}, logs/audit, temp, versions
```

### Data layout (runtime)

```
{base}/data/
  coretex/{collections,indexes/{vector,scalar},metadata/{metadata.json,config.toml,auth.json},store}
  wal/
  backup/{full,incremental,snapshots}
  logs/audit
  temp/
  versions/
```

### Upgrade

Use `scripts/upgrade.sh` from the install root. See `share/doc/INSTALL.md`.

从 V0.2.3 升级：安装根目录名从 `CoreTexDB-V0.2.3` 变为 `CoreTexDB-V0.2.4`，
systemd 单元与 logrotate 路径已同步更新，升级后需 `systemctl daemon-reload`。

---

## 归档：V0.2.3 Release Notes

> 以下是 `v0.2.3` tag 时的**多二进制**发行形态（`coretex`、`coretexd`、
> `coretex-cli`、`coretex-migrate`、`coretex-backup`、`coretex-healthcheck`）。
> 自 V0.2.4 起改为单二进制分发。当前状态见
> [`CHANGELOG.md`](CHANGELOG.md) 的 [0.2.4]，用法见 [`README.md`](README.md)。

### Highlights

- Full install-root package for V0.2.3: `bin/`, `lib/`, `include/`, `config/`, `share/`, `scripts/`, `systemd/`, `logrotate/`, and `data/` skeleton.
- Multi binaries: `coretex`, `coretexd`, `coretex-cli`, `coretex-migrate`, `coretex-backup`, `coretex-healthcheck`.
- Shared/static libs when built: `libcoretexdb.so` / `.dylib` / `.a` / `coretexdb.dll`.
- WAL segment naming `wal-NNNNNN.log`, strict discovery, max+1 rotation, documented lock order.
- Atomic create for `metadata/config.toml` and `metadata/auth.json` (temp + rename; never overwrite).
- `doctor` branches on `wal_enabled` (Plan A).
- B-C-D-D `.cdb` encrypt/decrypt/info/keygen CLI.
- Runtime never creates install-root `bin/` or `include/` (install layout only).

### Install-root layout (V0.2.3)

```
CoreTexDB-V0.2.3/
  bin/          coretex, coretexd, coretex-cli, coretex-migrate, coretex-backup, coretex-healthcheck
  lib/          libcoretexdb.so|.dylib|.a, coretexdb.dll|.lib
  include/      coretexdb.h
  config/       coretex.toml, logging.yaml, backup.toml, security.toml, metrics.toml, {dev,staging,prod}/
  share/        doc/, examples/
  scripts/      start/stop/status/install/upgrade/uninstall/backup/restore/healthcheck/...
  systemd/      coretexd.service + timers + tmpfiles
  logrotate/    coretexd
  data/         coretex/{collections,indexes,metadata,store}, wal, backup/{full,incremental,snapshots}, logs/audit, temp, versions
```
