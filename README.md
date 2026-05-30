# fleet-router

A reverse proxy and load balancer for the Antelope **SHiP** (State History Plugin) WebSocket protocol.

[![CI](https://github.com/eosrio/fleet-router/actions/workflows/ci.yml/badge.svg)](https://github.com/eosrio/fleet-router/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/fleet-router.svg)](https://crates.io/crates/fleet-router)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`fleet-router` sits in front of a fleet of Antelope SHiP nodes. Clients open a single WebSocket connection to the router; the router picks a healthy upstream SHiP node, forwards its ABI, and proxies the WebSocket bidirectionally. If an upstream drops, the router transparently fails over to another suitable node, replays the in-flight `get_blocks` request from the next block, and de-duplicates already-delivered blocks — so the client connection persists without any manual reconnect.

Use it when you run more than one SHiP node and want clients (Hyperion, dfuse-style indexers, custom consumers) to see a single, resilient endpoint with load balancing and automatic failover, instead of pinning each consumer to one node.

It is written in Rust on [tokio](https://tokio.rs/) and [tokio-tungstenite](https://github.com/snapview/tokio-tungstenite), and uses [rs_abieos](https://github.com/eosrio/rs-abieos) (C++ FFI) for ABI handling.

## Features

- **Range-aware, least-connections load balancing** — prefers an upstream whose trace range covers the requested block; otherwise routes to the least-loaded healthy upstream.
- **Automatic failover with de-duplication** — on upstream loss, reconnects to another suitable node, resumes `get_blocks` at the next block, de-duplicates replayed blocks, and buffers/replays client frames sent during the swap.
- **Stale-upstream deprioritization** — upstreams that stop advancing their chain state are flagged stale and deprioritized (not hard-excluded) when routing.
- **Graceful shutdown** — handles `SIGINT`/`SIGTERM` with a bounded drain and WebSocket close frames to connected clients.
- **Structured logging** — `tracing`-based, controlled with `RUST_LOG`.
- **Optional health/metrics endpoint** — liveness, readiness, and Prometheus metrics over HTTP, enabled on demand.
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

**Linux x86_64 only.** The `rs_abieos` build script panics with *"Unsupported OS"* on macOS and Windows. On those platforms, use the [Docker image](#docker) instead of a native build.

A native build compiles vendored C++ via a build script and uses `bindgen`, so you need a C/C++ toolchain plus `clang`/`libclang`:

| Requirement | Notes |
|---|---|
| Linux x86_64 | Only supported native target |
| Rust ≥ 1.85 (MSRV) | A transitive dependency uses the Rust 2024 edition |
| `git` | To clone and build from source |
| C/C++ toolchain | Debian/Ubuntu: `build-essential` |
| `clang` + `libclang-dev` | Required by `bindgen` |

On Debian/Ubuntu, install the prerequisites with:

```bash
sudo apt-get install -y git clang libclang-dev build-essential
```

If you do not already have Rust:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Installation

> Native builds (crates.io or from source) require the [prerequisites above](#requirements-and-supported-platforms) to be installed first. macOS/Windows users should use the Docker image.

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

The image runs as a non-root user and defines a `HEALTHCHECK` on the proxy port (`17000`). See [Running in production](#running-in-production) for a full `docker run` example.

## Quick start

1. Write a sample config file:

   ```bash
   fleet-router config init ./config.json
   ```

2. Edit `config.json` to list your SHiP nodes (set each `endpoint` to `host:port`, no scheme) and adjust the bind address/port and intervals.

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

Configuration is a single JSON file (`config.json` by default). The three `*_ms` fields are in **milliseconds**. Each upstream `endpoint` is `host:port` with **no scheme** — the router prepends `ws://` itself.

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `listen_address` | string | yes | — | Bind address for client connections (e.g. `0.0.0.0`). |
| `listen_port` | u16 | yes (non-zero) | `17000` (sample) | Port for client connections. |
| `upstream_reconnect_ms` | u64 | yes (> 0) | — | Milliseconds between upstream reconnection attempts. |
| `upstream_monitoring_ms` | u64 | yes (> 0) | — | Milliseconds between block-progress logging and staleness checks. |
| `upstream_status_ms` | u64 | yes (> 0) | — | Milliseconds between status requests sent to each upstream. |
| `servers` | array | yes (≥ 1 enabled) | — | List of upstream SHiP nodes (see below). |
| `max_connections` | usize | no | `10000` | Max concurrent client connections; excess are rejected (backpressure). |
| `handshake_timeout_ms` | u64 | no | `10000` | Client WebSocket handshake timeout; `0` disables it. |
| `idle_timeout_ms` | u64 | no | `0` (disabled) | Close a connection idle (no data in either direction) for this long. |
| `max_message_bytes` | usize | no | `268435456` (256 MiB) | Max WebSocket message size on both client and upstream links. |
| `shutdown_grace_ms` | u64 | no | `5000` | How long to wait for in-flight connections to drain on shutdown. |
| `metrics_address` | string | no | falls back to `listen_address` | Bind address for the health/metrics HTTP endpoint. |
| `metrics_port` | u16 | no | unset (endpoint disabled) | Port for the health/metrics HTTP endpoint. Setting it enables the endpoint. |

Each entry in `servers` is an object:

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | yes | Human-readable name used in logs. |
| `endpoint` | string | yes | Upstream as `host:port` (no scheme; `ws://` is prepended). Endpoints must be unique. |
| `enabled` | bool | yes | Whether the router may use this upstream. At least one enabled server is required. |

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

## Usage

```text
fleet-router config init <path>     Write a sample config file to <path>.
fleet-router config test <path>     Parse and validate a config, then test upstream connectivity.
fleet-router run [--config <path>]  Run the proxy.
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
| `/health` | `200` while the process is running. |
| `/ready` | `200` if at least one upstream is online, otherwise `503`. |
| `/metrics` | Prometheus text exposition of router and upstream state. |

Exposed metrics include:

```text
fleet_router_up
fleet_router_upstream_up{endpoint}
fleet_router_upstream_stale{endpoint}
fleet_router_active_connections{endpoint}
fleet_router_upstream_chain_state_end_block{endpoint}
```

## Running in production

> **Do not expose `fleet-router` directly to the public internet.** See [Security and limitations](#security-and-limitations).

### systemd

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

`SIGTERM` (sent by `systemctl stop`) triggers graceful shutdown: the router stops accepting new connections, drains in-flight ones for up to `shutdown_grace_ms`, and sends WebSocket close frames to clients. Keep `TimeoutStopSec` comfortably larger than `shutdown_grace_ms`.

### Docker

```bash
docker run -p 17000:17000 \
  -v "$PWD/config.json:/etc/fleet-router.json:ro" \
  ghcr.io/eosrio/fleet-router run --config /etc/fleet-router.json
```

The image runs as a non-root user and defines a `HEALTHCHECK` against port `17000`.

### Tuning notes

- **Intervals** (`upstream_reconnect_ms`, `upstream_monitoring_ms`, `upstream_status_ms`): lower values detect failures and staleness faster at the cost of more polling traffic to upstreams; raise them to reduce chatter.
- **Limits** (`max_connections`, `handshake_timeout_ms`, `idle_timeout_ms`, `max_message_bytes`): size `max_connections` for your expected concurrency (excess connections are rejected, not queued); set `idle_timeout_ms` to reap dead clients; lower `max_message_bytes` only if you are sure your blocks fit, since oversized frames are rejected on both links.

## Security and limitations

- Transport is **plaintext `ws://`** on both the client listener and the upstream connections. No TLS / `wss://` is compiled in.
- The client listener is **unauthenticated** — anyone who can reach the port can stream data.
- `rs_abieos` parses untrusted upstream bytes through C++ FFI. Treat your upstreams as part of the trust boundary.

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
