# CoreTexDB V0.2.2 Release Notes

## Highlights

- Install-root layout (`bin/`, `config/`, `data/coretex/{collections,indexes,metadata,store}`, `data/{wal,backup,logs,temp,versions}`).
- WAL segment naming `wal-NNNNNN.log`, strict discovery, max+1 rotation, documented lock order.
- Atomic create for `metadata/config.toml` and `metadata/auth.json` (temp + rename; never overwrite).
- `doctor` branches on `wal_enabled` (Plan A).
- B-C-D-D `.cdb` encrypt/decrypt/info/keygen CLI.
- Runtime never creates install-root `bin/` or `include/` (install layout only).

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
