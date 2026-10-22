# Getting started

fleet-router presents one SHiP WebSocket endpoint in front of a pool of upstream nodes. Start with nodes on the same chain and a trusted network. Use the [frozen SHiP deployment guide](frozen-ship.md) for a fleet split between archive ranges and live nodes.

## Install

Download the archive for your platform from [GitHub Releases](https://github.com/eosrio/fleet-router/releases). Releases include Linux x86_64, macOS arm64 and x86_64, and Windows x86_64 binaries. For v0.8.1, compare the archive's SHA-256 hash with `SHA256SUMS.txt` from the same release.

Or install the published crate. Building requires Rust 1.99 or newer, a C compiler and CMake:

```sh
cargo install fleet-router --version 0.8.1 --locked
fleet-router --version
```

The public GHCR image can be pulled without signing in:

```sh
docker pull ghcr.io/eosrio/fleet-router:0.8.1
docker run --rm ghcr.io/eosrio/fleet-router:0.8.1 --version
```

Pin a release tag or image digest for a controlled rollout. See [running as a service](operations.md#run-as-a-service) for persistent deployment and Docker volume and port settings.

## Configure upstreams

Create a sample file:

```sh
fleet-router config init ./config.json
```

Edit the upstream addresses before starting the router. This example keeps the listener on localhost; replace the upstream endpoints with reachable SHiP nodes on your chain:

```json
{
  "listen_address": "127.0.0.1",
  "listen_port": 17000,
  "metrics_address": "127.0.0.1",
  "metrics_port": 9090,
  "servers": [
    { "name": "SHiP 1", "endpoint": "10.0.1.10:8080", "enabled": true },
    { "name": "SHiP 2", "endpoint": "10.0.1.11:8080", "enabled": true }
  ]
}
```

Pin your chain's `chain_id` before production use. A bare `host:port` means `ws://`; use `wss://host:port` for an upstream serving TLS. See the [configuration reference](configuration.md) for all options.

## Validate and start

```sh
fleet-router config test ./config.json --strict
fleet-router run --config ./config.json
```

From another terminal, check the process and upstream readiness:

```sh
curl --fail http://127.0.0.1:9090/health
curl --fail http://127.0.0.1:9090/ready
```

`/health` confirms the process is running. `/ready` also requires an eligible online upstream; in a multi-listener deployment every listener must be ready. Point your consumer at `ws://127.0.0.1:17000` when it runs on the same host. A remote client needs a reachable listener address and appropriate network access controls. Test real block consumption and failover before rollout. See [readiness and coverage](observability.md#readiness-and-coverage).

## Enable operator tools

The dashboard is off by default. Enable `dashboard.enabled` on the metrics endpoint for `/ui/` and `/status`; configure a control token to unlock maintenance actions and the [configuration editor](configuration.md#reloading-configuration). Keep the dashboard and metrics endpoint private.

For encrypted client connections, configure [client TLS termination](configuration.md#client-tls-termination). TLS does not add authentication: the SHiP listener and optional chain API still require a trusted network or a reverse proxy with access control. Read the [production security guidance](operations.md#security).
