# Install CoreTexDB

## Requirements

- Linux x86_64 or Windows x86_64
- Optional: systemd for service integration

## Install (Linux)

```bash
sudo scripts/install.sh /opt/CoreTexDB-V0.2.1
sudo /opt/CoreTexDB-V0.2.1/scripts/secure_setup.sh /opt/CoreTexDB-V0.2.1
export PATH=/opt/CoreTexDB-V0.2.1/bin:$PATH
```

## Layout

See the install-root tree in the project README §10 / RELEASE_NOTES.md.

## Verify

```bash
coretex-healthcheck --data-dir /opt/CoreTexDB-V0.2.1
```
