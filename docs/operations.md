# Running in production

- [How routing and failover work](#how-routing-and-failover-work)
- [Security](#security)
- [systemd](#systemd)
- [Docker](#docker)
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

To report a vulnerability, see [SECURITY.md](../SECURITY.md).

## systemd

For a single config file (either single-port or multi-listener):

```ini
[Unit]
Description=Fleet SHiP Router
After=network-online.target
Wants=network-online.target

[Service]
User=fleet
ExecStart=/usr/local/bin/fleet-router run --config /etc/fleet-router/config.json
Restart=on-failure
# SIGTERM triggers a graceful, bounded drain. Allow more than shutdown_grace_ms
# so systemd does not SIGKILL the process mid-drain.
TimeoutStopSec=10
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

To run **one isolated process per network** with systemd template units, copy [`deploy/fleet-router@.service`](../deploy/fleet-router@.service) to `/etc/systemd/system/fleet-router@.service`, place each chain's config at `/etc/fleet-router/<chain>.json`, and start each unit (`systemctl enable --now fleet-router@eos fleet-router@wax`).

## Docker

```bash
docker run -p 17000:17000 \
  -v "$PWD/config.json:/etc/fleet-router.json:ro" \
  ghcr.io/eosrio/fleet-router run --config /etc/fleet-router.json
```

The image runs as a non-root user. Its `HEALTHCHECK` runs `fleet-router check`, which dials `127.0.0.1:17000`. For multi-container deployments (one container per network), see [`deploy/docker-compose.multi-network.yml`](../deploy/docker-compose.multi-network.yml).

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
