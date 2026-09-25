# CoreTexDB V0.2.3 Release Notes

> **注意 / Note**：本文描述的是 `v0.2.3` tag 时的**多二进制**发行形态
> （`coretex`、`coretexd`、`coretex-cli`、`coretex-migrate`、`coretex-backup`、
> `coretex-healthcheck`）。**此后仓库已改为单二进制分发**：只有 `coretex` 一个
> 可执行文件，按 `argv[0]`/子命令承担全部角色，5 个壳 `[[bin]]` 已删除。
> 当前状态见 [`CHANGELOG.md`](CHANGELOG.md) 的 [Unreleased]，用法见 [`README.md`](README.md)。

## Highlights

- Full install-root package for V0.2.3: `bin/`, `lib/`, `include/`, `config/`, `share/`, `scripts/`, `systemd/`, `logrotate/`, and `data/` skeleton.
- Multi binaries: `coretex`, `coretexd`, `coretex-cli`, `coretex-migrate`, `coretex-backup`, `coretex-healthcheck`.
- Shared/static libs when built: `libcoretexdb.so` / `.dylib` / `.a` / `coretexdb.dll`.
- WAL segment naming `wal-NNNNNN.log`, strict discovery, max+1 rotation, documented lock order.
- Atomic create for `metadata/config.toml` and `metadata/auth.json` (temp + rename; never overwrite).
- `doctor` branches on `wal_enabled` (Plan A).
- B-C-D-D `.cdb` encrypt/decrypt/info/keygen CLI.
- Runtime never creates install-root `bin/` or `include/` (install layout only).

## Install-root layout (V0.2.3)

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
