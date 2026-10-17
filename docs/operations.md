# Running in production

- [How routing and failover work](#how-routing-and-failover-work)
- [Security](#security)
- [Run as a service](#run-as-a-service) (systemd, Docker, launchd, Windows)
- [Graceful shutdown](#graceful-shutdown)
- [Corrupted SHiP logs](#corrupted-ship-logs)
- [Tuning](#tuning)

## How routing and failover work

A client connects over WebSocket. The router selects an upstream, forwards that upstream's ABI to the client, and then proxies frames in both directions.

- **Selection.** The router prefers an upstream whose trace range covers the requested block; otherwise it routes to the least-loaded healthy upstream. Among equally suitable upstreams, the optional per-server [`priority` and `weight`](configuration.md#upstream-preference) pick a preferred tier and split load inside it. A `get_blocks_request_v1` with `fetch_finality_data` is sent only to an upstream whose finality-data range covers that block (learned from `get_status_result_v1`). If no upstream reports a covering range, routing stays on the trace-range rules.
- **Health.** Background loops poll each upstream's status and block progress. An upstream that stops advancing is marked stale and deprioritized, not hard-excluded. An upstream whose status reply misses the `upstream_status_ms` deadline is marked offline.
- **Failover.** If the active upstream drops, the router selects another suitable upstream, resends the in-flight `get_blocks` request resumed at the next block, de-duplicates blocks the client has already received, and replays any client frames buffered during the swap. The client connection stays open throughout. Repeated unclean drops at the same block are bounded; see [Corrupted SHiP logs](#corrupted-ship-logs).

fleet-router is written in Rust on [tokio](https://tokio.rs/) and [tokio-tungstenite](https://github.com/snapview/tokio-tungstenite), and uses [rs_abieos](https://github.com/eosrio/rs-abieos) (pure-Rust backend) to check that an upstream SHiP ABI parses. Each handshake, failover, and monitor builds a short-lived `AbiHandle` and drops it; connections do not share an abieos context. Upstream TLS uses rustls with the [aws-lc-rs](https://github.com/aws/aws-lc-rs) provider.

## Security

> **Do not expose `fleet-router` directly to the public internet.**

- The client listener is **plaintext `ws://`** and **unauthenticated**. Anyone who can reach the port can stream data.
- Upstream connections default to plaintext `ws://`. Set `endpoint` to `wss://host:port` to dial that node with TLS; the certificate is verified with rustls against the OS trust store (see [Upstream TLS](configuration.md#upstream-tls-wss)).
- `rs_abieos` (pure Rust) parses untrusted upstream data; treat your upstreams as part of the trust boundary.

Deploy on a trusted or internal network, and/or behind a TLS-terminating reverse proxy (nginx, Caddy, Envoy) that adds access control.

The optional [dashboard](api.md) is off by default. When you enable it, it is served on `metrics_port` with no login for reading, and it shows client IP addresses. Its control actions need a bearer token. Keep `metrics_address` on a private interface (for example `127.0.0.1` behind an SSH tunnel or an authenticating reverse proxy).

**Maintenance without dropping clients.** With the control API enabled, `POST /api/v1/listeners/<listener>/upstreams/<endpoint>/drain` (or the Drain button on the dashboard) stops new sessions on that upstream and moves its current sessions to other upstreams. They resume at the next block and their clients stay connected. Restart or resync the node, then enable it again.

To report a vulnerability, see [SECURITY.md](../SECURITY.md).

## Run as a service

Run fleet-router under your platform's service manager so it starts at boot and restarts if it exits. Before installing it, check the config with `fleet-router config test <path>`: a service that fails validation on start just restarts in a loop.

- [Linux (systemd)](#linux-systemd)
- [Docker](#docker)
- [macOS (launchd)](#macos-launchd)
- [Windows](#windows)

### Linux (systemd)

The repository ships two units:

- [`deploy/fleet-router.service`](../deploy/fleet-router.service): one process for one config file (single-port or multi-listener).
- [`deploy/fleet-router@.service`](../deploy/fleet-router@.service): a template that runs one isolated process per network, each with its own config.

1. Install the binary and create an unprivileged user:

   ```bash
   sudo install -m 0755 fleet-router /usr/local/bin/fleet-router   # or: cargo install fleet-router, then copy ~/.cargo/bin/fleet-router
   sudo useradd --system --no-create-home --shell /usr/sbin/nologin fleet
   ```

2. Write and check the config:

   ```bash
   sudo mkdir -p /etc/fleet-router
   sudo fleet-router config init /etc/fleet-router/config.json
   sudoedit /etc/fleet-router/config.json        # list your SHiP nodes
   fleet-router config test /etc/fleet-router/config.json
   ```

3. Install the unit, then enable and start it:

   ```bash
   sudo cp deploy/fleet-router.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now fleet-router
   ```

   With the template unit, name each config after its network (`/etc/fleet-router/eos.json`, `/etc/fleet-router/wax.json`, each with its own `listen_port` and `metrics_port`) and enable one instance per network:

   ```bash
   sudo cp deploy/fleet-router@.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable --now fleet-router@eos fleet-router@wax
   ```

4. Check that it is running and follow the logs:

   ```bash
   systemctl status fleet-router
   journalctl -u fleet-router -f
   ```

`enable` starts the service at boot, and `Restart=on-failure` restarts it if it crashes. After you edit the config, run `config test` again and then `sudo systemctl restart fleet-router`. A restart drains open sessions (see [Graceful shutdown](#graceful-shutdown)); clients reconnect to the new process.

Both units also:

- set `LimitNOFILE=65536`. Each client session holds two sockets, one to the client and one to the upstream, and systemd's default soft limit of 1024 open files would cap the router at about 500 clients. Raise it if `max_connections` is higher.
- set `TimeoutStopSec=10`. Keep it larger than `shutdown_grace_ms`.
- run as the `fleet` user with `NoNewPrivileges`, a read-only file system (`ProtectSystem=strict`) and no access to home directories. The router only reads its config and the system CA store, and logs to journald.

To change a setting without editing the shipped file, use a drop-in: `sudo systemctl edit fleet-router` (for example, `Environment=RUST_LOG=debug`).

### Docker

```bash
docker run -d --name fleet-router --restart unless-stopped \
  -p 17000:17000 \
  -v "$PWD/config.json:/etc/fleet-router.json:ro" \
  ghcr.io/eosrio/fleet-router run --config /etc/fleet-router.json
```

`--restart unless-stopped` starts the container again when the Docker daemon starts (at boot, if the daemon is enabled) and after a crash, until you `docker stop` it. The image runs as a non-root user. Its `HEALTHCHECK` runs `fleet-router check`, which dials `127.0.0.1:17000`. For one container per network, see [`deploy/docker-compose.multi-network.yml`](../deploy/docker-compose.multi-network.yml), which sets the same restart policy; start it with `docker compose up -d`.

### macOS (launchd)

Save this as `/Library/LaunchDaemons/io.eosrio.fleet-router.plist`. Adjust the paths and `UserName`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>io.eosrio.fleet-router</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/local/bin/fleet-router</string>
    <string>run</string>
    <string>--config</string>
    <string>/usr/local/etc/fleet-router/config.json</string>
  </array>
  <key>UserName</key>
  <string>_fleet</string>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>SoftResourceLimits</key>
  <dict>
    <key>NumberOfFiles</key>
    <integer>65536</integer>
  </dict>
  <key>StandardOutPath</key>
  <string>/usr/local/var/log/fleet-router.log</string>
  <key>StandardErrorPath</key>
  <string>/usr/local/var/log/fleet-router.log</string>
</dict>
</plist>
```

```bash
sudo launchctl bootstrap system /Library/LaunchDaemons/io.eosrio.fleet-router.plist
sudo launchctl kickstart -k system/io.eosrio.fleet-router   # restart after a config change
```

`RunAtLoad` starts it at boot and `KeepAlive` restarts it when it exits. The user named in `UserName` must exist and be able to write the log file. Stop and remove it with `sudo launchctl bootout system/io.eosrio.fleet-router`.

### Windows

fleet-router is a console program. It does not implement the Windows service control protocol, so registering the `.exe` directly with `sc.exe create` fails to start (error 1053). Run it under a service wrapper such as [NSSM](https://nssm.cc/):

```powershell
nssm install fleet-router C:\fleet-router\fleet-router.exe run --config C:\fleet-router\config.json
nssm set fleet-router AppStdout C:\fleet-router\fleet-router.log
nssm set fleet-router AppStderr C:\fleet-router\fleet-router.log
nssm start fleet-router
```

The service starts automatically at boot, and NSSM restarts the process if it exits. On stop, NSSM sends Ctrl+C first, which triggers the router's [graceful shutdown](#graceful-shutdown).

## Graceful shutdown

`SIGINT` and `SIGTERM` (sent by `systemctl stop`) trigger graceful shutdown: the router stops accepting new connections, drains in-flight ones for up to `shutdown_grace_ms`, and sends WebSocket close frames to clients. Keep systemd's `TimeoutStopSec` comfortably larger than `shutdown_grace_ms`.

## Corrupted SHiP logs

A damaged `trace_history.log` or `chain_state_history.log` makes nodeos drop the SHiP socket with no WebSocket close frame when a client reaches that block (`zlib error: iostream error`). Indexers then reconnect and request the same block forever. fleet-router treats that drop as a mid-stream failure:

- The session tries another suitable upstream first. Retries after an unclean drop at the same next block wait 100 ms, then double, up to 3.2 s, and stop after five such drops. The client is closed with WebSocket code 1011 and reason `block <K> unavailable on all upstreams`. One error line names the upstream and the block. An idle timeout or a clean close still fails over after 100 ms and does not count, so a client waiting at the chain tip is not closed.
- Each unclean drop increments `fleet_router_upstream_block_failures_total` for that upstream. The node stays online, so other block ranges are unaffected.
- Optional [`block_failure_quarantine`](configuration.md#failure-handling) remembers the block. After `strikes` drops at the same K inside `window_secs`, from any session, that upstream is skipped for requests starting in `K..K+range` for `ttl_secs`. Later sessions are routed to a healthy node and do not pay a failover on that range. When the TTL ends the upstream is eligible again. Repair or resync the log; the gauge `fleet_router_upstream_quarantine_block` shows which block to look at.

This bound is always on. Quarantine is off until `block_failure_quarantine` is set, at the top level or on a listener.

## Tuning

- **Intervals** (`upstream_reconnect_ms`, `upstream_monitoring_ms`, `upstream_status_ms`): lower values detect failures and staleness faster at the cost of more polling traffic to upstreams; raise them to reduce chatter.
- **Connections** (`max_connections`): size it for your expected concurrency. Excess connections are rejected, not queued.
- **Timeouts** (`handshake_timeout_ms`, `idle_timeout_ms`): set `idle_timeout_ms` to reap dead clients.
- **Message sizes** (`max_message_bytes`, `max_client_message_bytes`): lower `max_message_bytes` only if you are sure upstream blocks fit. `max_client_message_bytes` caps client frames (default 64 KiB). The per-session failover buffer is capped at 64 KiB, and the session is closed if a client exceeds it.

See the [configuration reference](configuration.md) for every field and its default.
