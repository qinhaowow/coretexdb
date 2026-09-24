# CoreTexDB Security Notes

## File permissions (spec §五)

| Path | Mode | Owner |
|------|------|-------|
| bin/, lib/, scripts/ | 750 | root:coretex |
| include/, share/, systemd/, logrotate/ | 755 | root:root |
| config/ files | 640 | coretex:coretex |
| data/ | 700 | coretex:coretex |
| data/logs/ | 750 | coretex:adm |
| .deploy/ | 700 | root:root |

Enforce with `scripts/secure_setup.sh`.

## Encryption

- At-rest: AES-256-GCM for `.cdb` files (`coretex crypto encrypt`).
- Keys: never store keys in `config/`. Use OS keyring or external KMS (pending security module).

## Logging

- Never log passwords, tokens, or private keys.
- Redaction patterns: `config/logging.yaml` → `redact`.

## systemd

- `coretexd.service` uses `ProtectSystem=strict`, `NoNewPrivileges`, empty capability bounding set.

## Integrity

- Release artifacts: provide `.sha256` and `.sig` beside binaries (packaging step).
