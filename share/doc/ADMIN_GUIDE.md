# CoreTexDB Administration Guide

## Service

```bash
sudo systemctl enable --now coretexd
sudo systemctl status coretexd
```

## Backup / Restore

```bash
scripts/backup.sh
scripts/restore.sh data/backup/full <name>
```

## Health

```bash
scripts/healthcheck.sh
# or
coretex doctor --data-dir /opt/CoreTexDB-V0.2.4
```

## WAL

Default: `wal_enabled = false` (FileStorage is already a log).
Enable in `config/coretex.toml` under `[wal]` when a separate WAL is required.

## Logs

- `data/logs/coretex.log`, `error.log`, `slow_query.log`
- Audit: `data/logs/audit/audit-YYYYMMDD.log`
- Rotation: `logrotate/coretexd`
