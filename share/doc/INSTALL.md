# Install CoreTexDB

## Requirements

- Linux x86_64 or Windows x86_64
- Optional: systemd for service integration

## Install (Linux)

```bash
sudo scripts/install.sh /opt/CoreTexDB-V0.2.2
sudo /opt/CoreTexDB-V0.2.2/scripts/secure_setup.sh /opt/CoreTexDB-V0.2.2
export PATH=/opt/CoreTexDB-V0.2.2/bin:$PATH
```

## Layout (V0.2.2)

```
/opt/CoreTexDB-V0.2.2/
  bin/ lib/ include/ config/ share/ scripts/ systemd/ logrotate/
  data/{coretex,wal,backup,logs,temp,versions}
```

See README §1.1 / §10 and RELEASE_NOTES.md for the full tree.

## Verify

```bash
coretex-healthcheck --data-dir /opt/CoreTexDB-V0.2.2
```
