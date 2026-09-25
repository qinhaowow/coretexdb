# CoreTexDB V0.2.4 Release Notes

## Highlights (V0.2.4)

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

## What's changed since V0.2.3

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

## Install-root layout (V0.2.4)

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

## Data layout (runtime)

```
{base}/data/
  coretex/{collections,indexes/{vector,scalar},metadata/{metadata.json,config.toml,auth.json},store}
  wal/
  backup/{full,incremental,snapshots}
  logs/audit
  temp/
  versions/
```

## Upgrade

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
