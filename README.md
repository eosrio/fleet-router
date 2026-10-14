# fleet-router

A reverse proxy and load balancer for the Antelope **SHiP** (State History Plugin) WebSocket protocol.

[![CI](https://github.com/eosrio/fleet-router/actions/workflows/ci.yml/badge.svg)](https://github.com/eosrio/fleet-router/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/fleet-router.svg)](https://crates.io/crates/fleet-router)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`fleet-router` sits in front of a fleet of Antelope SHiP nodes. Clients open a single WebSocket connection to the router; the router picks a healthy upstream SHiP node, forwards its ABI, and proxies the WebSocket bidirectionally. If an upstream drops, the router transparently fails over to another suitable node, replays the in-flight `get_blocks` request from the next block, and de-duplicates already-delivered blocks — so the client connection persists without any manual reconnect.

Use it when you run more than one SHiP node and want clients (Hyperion, dfuse-style indexers, custom consumers) to see a single, resilient endpoint with load balancing and automatic failover, instead of pinning each consumer to one node.

It is written in Rust on [tokio](https://tokio.rs/) and [tokio-tungstenite](https://github.com/snapview/tokio-tungstenite), and uses [rs_abieos](https://github.com/eosrio/rs-abieos) (pure-Rust backend) to check that an upstream SHiP ABI parses. Each handshake, failover, and monitor builds a short-lived `AbiHandle` and drops it; connections do not share an abieos context.

## Features

- **Range-aware, least-connections load balancing** — prefers an upstream whose trace range covers the requested block; otherwise routes to the least-loaded healthy upstream. A `get_blocks_request_v1` with `fetch_finality_data` is sent only to an upstream whose finality-data range covers that block (learned from `get_status_result_v1`). If no upstream reports a covering range, routing stays on the trace-range rules.
- **Automatic failover with de-duplication** — on upstream loss, reconnects to another suitable node, resumes `get_blocks` at the next block, de-duplicates replayed blocks, and buffers/replays client frames sent during the swap.
- **Stale-upstream deprioritization** — upstreams that stop advancing their chain state are flagged stale and deprioritized (not hard-excluded) when routing.
- **Graceful shutdown** — handles `SIGINT`/`SIGTERM` with a bounded drain and WebSocket close frames to connected clients.
- **Structured logging** — `tracing`-based, controlled with `RUST_LOG`.
- **Optional health/metrics endpoint** — liveness, readiness, and Prometheus metrics over HTTP, enabled on demand.
- **Upstream `wss://`** — optional TLS to SHiP nodes. A bare `host:port` stays `ws://`. Certificates are verified with rustls against the OS trust store. The client listener remains plaintext.
- **Resource safety** — connection cap, handshake/idle timeouts, and a bounded maximum WebSocket message size.

## How it works

A client connects over WebSocket. The router selects an upstream (range-aware, then least-connections), forwards that upstream's ABI to the client, and then proxies frames in both directions. Background loops poll each upstream's status and block progress; an upstream that stops advancing is marked stale and deprioritized. If the active upstream drops, the router selects another suitable upstream, resends the in-flight `get_blocks` request resumed at the next block, de-duplicates blocks the client has already received, and replays any client frames buffered during the swap. The client connection stays open throughout.

```
                                    +-----------------------+
                                    |  upstream SHiP node A | (active)
                            +-----> |  ws://hostA:port      |
                            |       +-----------------------+
+--------+   WebSocket   +--+-----------+
| client | ============> | fleet-router |   range-aware / least-connections
+--------+               +--+-----------+   selection + health monitoring
                            |       +-----------------------+
                            +-----> |  upstream SHiP node B | (failover target:
       on upstream A failure,       |  ws://hostB:port      |  resume at next block,
       transparent swap to B  ----> +-----------------------+  de-duplicate blocks)
```

## Requirements and supported platforms

fleet-router builds on **Linux, macOS, and Windows** (x86_64 and arm64). ABI handling uses the pure-Rust `rs_abieos` backend, so `libclang` is not required. Upstream TLS links [aws-lc-rs](https://github.com/aws/aws-lc-rs) through rustls. Building that provider needs a C compiler and CMake on `PATH`.

| Requirement | Notes |
|---|---|
| Rust ≥ 1.99 (MSRV) | Toolchain used to build, test, and publish fleet-router |
| C compiler and CMake | Used to build `aws-lc-rs` for `wss://`. Not required at runtime. |

If you do not already have Rust, install it via [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Installation

### From crates.io

```bash
cargo install fleet-router
```

### From source

```bash
git clone https://github.com/eosrio/fleet-router.git
cd fleet-router
cargo install --path .
```

### Docker

A prebuilt image is published to the GitHub Container Registry on tagged releases:

```bash
docker pull ghcr.io/eosrio/fleet-router
```

The image runs as a non-root user and defines a `HEALTHCHECK` that runs `fleet-router check` against the proxy port (`17000`). See [Running in production](#running-in-production) for a full `docker run` example.

## Quick start

1. Write a sample config file:

   ```bash
   fleet-router config init ./config.json
   ```

2. Edit `config.json` to list your SHiP nodes. A bare `host:port` is dialed as `ws://`; use `wss://host:port` when the node serves TLS. Adjust the bind address/port and intervals.

3. Validate the config and test upstream connectivity:

   ```bash
   fleet-router config test ./config.json
   ```

4. Run the proxy:

   ```bash
   fleet-router run --config ./config.json
   ```

On startup, `run` validates the configuration before binding. Invalid configs fail fast with an actionable error; on success the router begins listening and logs its upstream monitors:

```
configuration is valid.
INFO starting upstream monitor name="SHiP Node 1" upstream=127.0.0.1:18080
INFO listening for clients address=0.0.0.0 port=17000
```

## Configuration

Configuration is a single JSON file (`config.json` by default). The three `*_ms` fields are in **milliseconds**. An upstream `endpoint` is `host:port`, `ws://…`, or `wss://…`. A bare `host:port` is dialed as `ws://`, which is what existing configs already do.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `listen_address` | string | yes (single-port) / no (multi-listener) | `"0.0.0.0"` (with `listeners`) | Bind address for client connections (e.g. `0.0.0.0`). Acts as the default bind address for `listeners` and `metrics_address`. |
| `listen_port` | u16 | yes (single-port, non-zero) | `17000` (sample) | Port for client connections in single-listener mode. Cannot be combined with `listeners`. |
| `upstream_reconnect_ms` | u64 | yes (> 0) | — | Milliseconds between upstream reconnection attempts. |
| `upstream_monitoring_ms` | u64 | yes (> 0) | — | Milliseconds between block-progress logging and staleness checks. |
| `upstream_status_ms` | u64 | yes (> 0) | — | Milliseconds between status requests sent to each upstream. Only one request is in flight; a reply that misses this deadline marks the upstream offline. |
| `servers` | array | yes in single-port mode (≥ 1 enabled) | — | List of upstream SHiP nodes (see below). Mutually exclusive with `listeners`. |
| `listeners` | array | yes in multi-listener mode (≥ 1 entry) | — | Named ingress listeners mapping distinct ports to isolated SHiP pools (see [Multi-network / multi-port routing](#multi-network--multi-port-routing)). |
| `max_connections` | usize | no | `10000` | Max concurrent client connections; excess are rejected (backpressure). Acts as the default per-listener cap when `listeners` is used. |
| `handshake_timeout_ms` | u64 | no | `10000` | Client WebSocket handshake timeout; `0` disables it. The upstream monitor and `config test` use this same bound for connect and the first read, falling back to 10s when it is `0`. |
| `idle_timeout_ms` | u64 | no | `0` (disabled) | Close a connection idle (no data in either direction) for this long. |
| `max_message_bytes` | usize | no | `268435456` (256 MiB) | Max WebSocket message size on proxied upstreams and monitor connections. |
| `max_client_message_bytes` | usize | no | `65536` (64 KiB) | Max WebSocket message and frame size accepted from a client. Omitting the field keeps existing configs valid and applies this default. |
| `shutdown_grace_ms` | u64 | no | `5000` | How long to wait for in-flight connections to drain on shutdown. |
| `metrics_address` | string | no | falls back to `listen_address` | Bind address for the health/metrics HTTP endpoint. |
| `metrics_port` | u16 | no | unset (endpoint disabled) | Port for the health/metrics HTTP endpoint. Setting it enables the endpoint. |

Each entry in `servers` is an object:

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Human-readable name used in logs. |
| `endpoint` | string | yes | Upstream as `host:port`, `ws://host:port`, or `wss://host:port`. A bare `host:port` is dialed as `ws://`. Endpoints must be unique (see below). |
| `enabled` | bool | yes | Whether the router may use this upstream. At least one enabled server is required. |

`wss://` is dialed with rustls (not OpenSSL). The router verifies the upstream certificate against the operating-system trust store. The published container image installs CA certificates, so public CAs work there. For a private CA, install it into that store, or set `SSL_CERT_FILE` / `SSL_CERT_DIR` to PEM certificates — when either variable is set, those paths replace the OS store. Unknown schemes (`https://`, `http://`, …) are rejected. Uniqueness is the dial target: `127.0.0.1:8080` and `ws://127.0.0.1:8080` are the same upstream, and so are `ws://host` and `ws://host:80`. `ws://` and `wss://` to the same host are different. The client-facing listener stays plaintext `ws://`.

Client requests are capped by `max_client_message_bytes` (default 64 KiB) on both the WebSocket message size and the frame size. Upstream block payloads still use `max_message_bytes`. While an upstream failover is in progress, client frames that cannot be forwarded are buffered up to **64 KiB** per session. Each buffered frame counts as at least one byte, so empty frames cannot grow the queue without a bound. Exceeding that cap closes the session with WebSocket close code 1008 (policy violation).

### Sample `config.json`

```json
{
  "listen_address": "0.0.0.0",
  "listen_port": 17000,
  "upstream_reconnect_ms": 3000,
  "upstream_monitoring_ms": 5000,
  "upstream_status_ms": 5000,
  "servers": [
    {
      "name": "SHiP Node 1",
      "endpoint": "127.0.0.1:18080",
      "enabled": true
    },
    {
      "name": "SHiP Node 2",
      "endpoint": "127.0.0.1:28080",
      "enabled": true
    }
  ]
}
```

### Multi-network / multi-port routing

To expose multiple ingress ports for different chains (each backed by its own redundant SHiP pool), you can either:

1. **Use `listeners` in a single `fleet-router` process** — each entry in `listeners` binds its own port and maintains an isolated upstream pool, monitor loop, and connection semaphore:

   | Field | Type | Required | Description |
   |---|---|---|---|
   | `name` | string | yes | Unique listener/network identifier (no `/` or whitespace). Used in logs, `/metrics` (`listener="<name>"`), and `/ready/<name>`. |
   | `listen_port` | u16 | yes (non-zero) | Ingress TCP port for this listener. Must not collide with other listeners or `metrics_port`. |
   | `listen_address` | string | no | Bind address override; defaults to top-level `listen_address` (or `"0.0.0.0"`). |
   | `max_connections` | usize | no | Per-listener concurrent client cap; defaults to top-level `max_connections`. |
   | `servers` | array | yes (≥ 1 enabled) | Redundant SHiP nodes belonging to this network. |

   ```json
   {
     "listen_address": "0.0.0.0",
     "upstream_reconnect_ms": 3000,
     "upstream_monitoring_ms": 5000,
     "upstream_status_ms": 5000,
     "metrics_port": 9090,
     "listeners": [
       {
         "name": "eos",
         "listen_port": 17000,
         "servers": [
           { "name": "EOS SHiP 1", "endpoint": "10.0.1.10:8080", "enabled": true },
           { "name": "EOS SHiP 2", "endpoint": "10.0.1.11:8080", "enabled": true }
         ]
       },
       {
         "name": "wax",
         "listen_port": 17001,
         "servers": [
           { "name": "WAX SHiP 1", "endpoint": "10.0.2.10:8080", "enabled": true },
           { "name": "WAX SHiP 2", "endpoint": "10.0.2.11:8080", "enabled": true }
         ]
       }
     ]
   }
   ```

2. **Run one `fleet-router` instance per network** for process/container-level isolation — see [`deploy/docker-compose.multi-network.yml`](deploy/docker-compose.multi-network.yml) and [`deploy/fleet-router@.service`](deploy/fleet-router@.service).

## Usage

```text
fleet-router config init <path>     Write a sample config file to <path>.
fleet-router config test <path>     Parse and validate a config, then test upstream connectivity.
fleet-router run [--config <path>]  Run the proxy.
fleet-router check [--port <port>]  Exit 0 if 127.0.0.1:<port> accepts TCP (default 17000).
fleet-router --version              Print the version.
fleet-router --help                 Print help.
```

The `--config` flag is global and defaults to `./config.json`, so `fleet-router run` with a `config.json` in the working directory is equivalent to passing `--config ./config.json`.

## Observability

### Logging

Logging uses `tracing`. Control verbosity with the `RUST_LOG` environment variable (default `info`):

```bash
# Everything at debug
RUST_LOG=debug fleet-router run --config config.json

# Debug for fleet-router, info for everything else
RUST_LOG=fleet_router=debug,info fleet-router run --config config.json
```

### Health and metrics endpoint

Set `metrics_port` (and optionally `metrics_address`) to enable an HTTP endpoint. It serves `GET` requests on:

| Route | Response |
|---|---|
| `/health`, `/healthz` | `200` while the process is running. |
| `/ready`, `/readyz` | `200` if at least one upstream is online (or, in `listeners` mode, if **every** listener has at least one upstream online), otherwise `503`. |
| `/ready/<listener>`, `/readyz/<listener>` | `200` if the named listener has at least one online upstream, `503` if none are online, or `404` if the listener name is unknown. |
| `/metrics` | Prometheus text exposition of router and upstream state. |

Exposed metrics include (in multi-listener mode, each upstream series also carries `listener="<name>"`):

```text
fleet_router_up
fleet_router_upstream_up{endpoint}
fleet_router_upstream_stale{endpoint}
fleet_router_active_connections{endpoint}
fleet_router_upstream_chain_state_end_block{endpoint}
fleet_router_upstream_finality_data_begin_block{endpoint}
fleet_router_upstream_finality_data_end_block{endpoint}
```

## Running in production

> **Do not expose `fleet-router` directly to the public internet.** See [Security and limitations](#security-and-limitations).

### systemd

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

To run **one isolated process per network** with systemd template units, copy [`deploy/fleet-router@.service`](deploy/fleet-router@.service) to `/etc/systemd/system/fleet-router@.service`, place each chain's config at `/etc/fleet-router/<chain>.json`, and start each unit (`systemctl enable --now fleet-router@eos fleet-router@wax`).

`SIGTERM` (sent by `systemctl stop`) triggers graceful shutdown: the router stops accepting new connections, drains in-flight ones for up to `shutdown_grace_ms`, and sends WebSocket close frames to clients. Keep `TimeoutStopSec` comfortably larger than `shutdown_grace_ms`.

### Docker

```bash
docker run -p 17000:17000 \
  -v "$PWD/config.json:/etc/fleet-router.json:ro" \
  ghcr.io/eosrio/fleet-router run --config /etc/fleet-router.json
```

The image runs as a non-root user. Its `HEALTHCHECK` runs `fleet-router check`, which dials `127.0.0.1:17000`. For multi-container deployments (one container per network), see [`deploy/docker-compose.multi-network.yml`](deploy/docker-compose.multi-network.yml).

### Tuning notes

- **Intervals** (`upstream_reconnect_ms`, `upstream_monitoring_ms`, `upstream_status_ms`): lower values detect failures and staleness faster at the cost of more polling traffic to upstreams; raise them to reduce chatter.
- **Limits** (`max_connections`, `handshake_timeout_ms`, `idle_timeout_ms`, `max_message_bytes`, `max_client_message_bytes`): size `max_connections` for your expected concurrency (excess connections are rejected, not queued); set `idle_timeout_ms` to reap dead clients; lower `max_message_bytes` only if you are sure upstream blocks fit. `max_client_message_bytes` caps client frames (default 64 KiB). The per-session failover buffer is capped at 64 KiB, and the session is closed if a client exceeds it.

## Security and limitations

- The client listener is **plaintext `ws://`** and **unauthenticated** — anyone who can reach the port can stream data. Put TLS and access control for clients on a reverse proxy in front of fleet-router.
- Upstream connections default to plaintext `ws://`. Set `endpoint` to `wss://host:port` to dial that node with TLS; the certificate is verified with rustls against the OS trust store.
- `rs_abieos` (pure Rust) parses untrusted upstream data; treat your upstreams as part of the trust boundary.

Because of the above:

- Deploy on a trusted or internal network, and/or behind a TLS-terminating reverse proxy (nginx, Caddy, Envoy) that adds access control.
- **Do not expose `fleet-router` directly to the public internet.**

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) for the development setup, the CI checks, and the pre-PR checklist (`cargo fmt --all`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace`, and a `CHANGELOG.md` entry). Tests run against an in-repo mock SHiP double and need no external services.

## Security policy

To report a vulnerability, see [SECURITY.md](SECURITY.md). Please do not open public issues for security reports.

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for release notes ([Keep a Changelog](https://keepachangelog.com/) format).

## Code of conduct

This project follows the [Code of Conduct](CODE_OF_CONDUCT.md).

## License

Licensed under the [MIT License](LICENSE). © EOS Rio.
