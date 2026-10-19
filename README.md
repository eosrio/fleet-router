# fleet-router

A reverse proxy and load balancer for the Antelope **SHiP** (State History Plugin) WebSocket protocol.

[![CI](https://github.com/eosrio/fleet-router/actions/workflows/ci.yml/badge.svg)](https://github.com/eosrio/fleet-router/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/fleet-router.svg)](https://crates.io/crates/fleet-router)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

[Homepage](https://eosrio.github.io/fleet-router/) · [Frozen SHiP deployment](docs/frozen-ship.md) · [Configuration](docs/configuration.md) · [Operations](docs/operations.md) · [Observability](docs/observability.md) · [Chain API](docs/chain-api.md) · [Dashboard & API](docs/api.md) · [Changelog](CHANGELOG.md)

Run more than one SHiP node? Put fleet-router in front of them and give your clients (Hyperion, other indexers, custom consumers) **one resilient endpoint** instead of pinning each consumer to one node.

Clients open a single WebSocket connection to the router. The router picks a healthy upstream, forwards its ABI, and proxies the stream. If that upstream drops, the router fails over to another suitable node, resumes `get_blocks` at the next block, and de-duplicates blocks the client already has. The client connection stays open.

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

## Features

- **Failover without reconnecting.** On upstream loss, a silent socket or a stalled stream, the session moves to another node, resumes at the next block with the client's outstanding credit, and drops blocks the client already has.
- **Range-aware, least-connections routing.** Each `get_blocks` goes to an upstream that has the requested block for every log it asks for (traces, deltas, finality data), otherwise to the least-loaded healthy one. Optional per-node `priority` and `weight` mark preferred and last-resort nodes and split load.
- **Stale and lagging nodes are deprioritized, not excluded.** Upstreams that stop advancing, or fall behind the fleet head, drop down the list.
- **Range-split fleets.** Mark archive upstreams `"role": "frozen"` and the listener presents live nodes plus frozen range servers (fixed block ranges served from archived logs, no nodeos) as one SHiP node: the router answers status itself, routes each request by block range, and hands the stream from one upstream to the next. See [Range-split fleets](docs/operations.md#range-split-fleets).
- **Chain API (opt-in).** A nodeos-compatible HTTP port per listener: block lookups go to the frozen range server or live node that holds the block, `get_info` and state calls to live nodes, push and exec are off by default. See [Chain API](docs/chain-api.md).
- **Bounded failure on corrupted SHiP logs.** A block that breaks every upstream ends the session with close code 1011 and a clear reason instead of a silent stall. Opt-in quarantine skips one node for just the bad block range.
- **Several networks in one process.** Each listener has its own port, upstream pool, `chain_id`, connection cap, and `/ready/<name>` check.
- **`chain_id` verification.** Upstreams that report the wrong chain stay offline. Older nodes that omit `chain_id` (FIO and other EOSIO 2.0-based builds) stay usable unless you require it.
- **Operable.** Prometheus metrics, health and readiness endpoints, `tracing` logs, graceful shutdown, upstream `wss://`, and connection, timeout and message-size limits.
- **Live dashboard and control API (opt-in).** See every upstream's block range, load and health in real time, and drain a node for maintenance: its sessions move to other nodes without the clients reconnecting.

## Installation

```bash
cargo install fleet-router              # from crates.io
docker pull ghcr.io/eosrio/fleet-router  # container image
```

Prebuilt archives for Linux x86_64, macOS (arm64 and x86_64) and Windows x86_64 are attached to each [GitHub Release](https://github.com/eosrio/fleet-router/releases). To build from source:

```bash
git clone https://github.com/eosrio/fleet-router.git
cd fleet-router
cargo install --path .
```

### Requirements and supported platforms

fleet-router builds on **Linux, macOS, and Windows** (x86_64 and arm64).

- **Rust ≥ 1.99** (MSRV). Install it with [rustup](https://rustup.rs/) if needed.
- **A C compiler and CMake** on `PATH`, to build `aws-lc-rs` for upstream `wss://`. Not needed at runtime.

ABI handling uses the pure-Rust [rs_abieos](https://github.com/eosrio/rs-abieos) backend, so `libclang` is not required.

## Quick start

```bash
fleet-router config init ./config.json   # 1. write a sample config
#                                          2. list your SHiP nodes in config.json
fleet-router config test ./config.json   # 3. validate and test upstream connectivity
fleet-router run --config ./config.json  # 4. start the proxy
```

To start fleet-router at boot and restart it after a crash, run it as a service: [systemd, Docker, launchd or Windows](docs/operations.md#run-as-a-service).

A minimal config with two SHiP nodes:

```json
{
  "listen_address": "0.0.0.0",
  "listen_port": 17000,
  "upstream_reconnect_ms": 3000,
  "upstream_monitoring_ms": 5000,
  "upstream_status_ms": 5000,
  "servers": [
    { "name": "SHiP Node 1", "endpoint": "127.0.0.1:18080", "enabled": true },
    { "name": "SHiP Node 2", "endpoint": "127.0.0.1:28080", "enabled": true }
  ]
}
```

A bare `host:port` is dialed as `ws://`; use `wss://host:port` for nodes that serve TLS. Then point your client's SHiP URL at the router (`ws://<router-host>:17000`) instead of a single node.

On startup the router validates the config and logs its upstream monitors:

```
configuration is valid.
INFO starting upstream monitor name="SHiP Node 1" upstream=127.0.0.1:18080
INFO listening for clients address=0.0.0.0 port=17000
```

## Commands

```text
fleet-router config init <path>     Write a sample config file to <path>.
fleet-router config test <path>     Parse and validate a config, then test upstream connectivity.
             [--strict]             Also exit 1 on an invalid config, a coverage hole, an
                                    unreported archive, or archived blocks that no archive
                                    with api_endpoint covers on a chain API listener.
fleet-router run [--config <path>]  Run the proxy.
fleet-router check [--port <port>]  Exit 0 if 127.0.0.1:<port> accepts TCP (default 17000).
fleet-router --version              Print the version.
fleet-router --help                 Print help.
```

`--config` is global and defaults to `./config.json`.

## Documentation

- **[Frozen SHiP deployment](docs/frozen-ship.md)**: deploy Hyperion Tools archive servers and live nodes behind one stitched endpoint. Includes log preparation, configuration, snapshot cut-over and rollout checks. Also available as a [site guide](https://eosrio.github.io/fleet-router/frozen-ship.html).
- **[Configuration reference](docs/configuration.md)**: every field and default, topology and stitching, the chain API, upstream TLS, and multi-network `listeners`.
- **[Running in production](docs/operations.md)**: routing and failover, range-split fleets with frozen range servers, security, running as a service (systemd, Docker, launchd, Windows), graceful shutdown, corrupted SHiP logs, and tuning.
- **[Observability](docs/observability.md)**: logging, health and readiness endpoints, `/coverage`, the Prometheus metrics, and alerts.
- **[Chain API](docs/chain-api.md)**: the opt-in nodeos-compatible HTTP port and how it routes requests.
- **[Dashboard and control API](docs/api.md)**: the opt-in live dashboard, status page, JSON API, and token-protected control actions.
- **Example configs**: [`example.config.json`](example.config.json) (two full nodes) and [`example.stitched.config.json`](example.stitched.config.json) (archived ranges, live nodes and a chain API).
- **Deployment samples**: [`deploy/fleet-router.service`](deploy/fleet-router.service), [`deploy/fleet-router@.service`](deploy/fleet-router@.service), and [`deploy/docker-compose.multi-network.yml`](deploy/docker-compose.multi-network.yml).

## Security

The client listener is **plaintext `ws://` and unauthenticated**, and so is the optional chain API port (plain HTTP). Run fleet-router on a trusted network or behind a TLS-terminating reverse proxy that adds access control. **Do not expose it directly to the public internet.** See [Security](docs/operations.md#security) for details.

To report a vulnerability, follow [SECURITY.md](SECURITY.md). Please do not open public issues for security reports.

## Contributing

Contributions are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) covers the development setup, the CI checks, and the pre-PR checklist. Tests run against an in-repo mock SHiP double and need no external services. This project follows the [Code of Conduct](CODE_OF_CONDUCT.md).

Release notes are in [CHANGELOG.md](CHANGELOG.md) ([Keep a Changelog](https://keepachangelog.com/) format).

## License

Licensed under the [MIT License](LICENSE). © EOS Rio.
