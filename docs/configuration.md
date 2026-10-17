# Configuration reference

fleet-router reads one JSON file (`./config.json` by default; pass another with `--config`). Run `fleet-router config init <path>` to write a sample and `fleet-router config test <path>` to validate it and test upstream connectivity.

A config uses one of two modes:

- **Single-port mode**: `listen_port` and `servers` at the top level. One port, one pool of SHiP nodes.
- **Multi-listener mode**: a `listeners` array. Each listener has its own port, pool, `chain_id` and readiness check. See [Multiple networks](#multiple-networks).

All `*_ms` fields are in **milliseconds**.

- [Minimal example](#minimal-example)
- [Top-level fields](#top-level-fields)
  - [Dashboard](#dashboard)
- [Upstream servers](#upstream-servers)
  - [Upstream preference](#upstream-preference)
- [Upstream TLS (`wss://`)](#upstream-tls-wss)
- [Client message limits](#client-message-limits)
- [Multiple networks](#multiple-networks)

## Minimal example

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

## Top-level fields

### Listener and upstreams

- **`listen_address`** *(string, required in single-port mode; default `"0.0.0.0"` with `listeners`)* — Bind address for client connections (e.g. `0.0.0.0`). Acts as the default bind address for `listeners` and `metrics_address`.
- **`listen_port`** *(u16, required in single-port mode, non-zero; sample `17000`)* — Port for client connections in single-listener mode. Cannot be combined with `listeners`.
- **`servers`** *(array, required in single-port mode, `≥ 1` enabled)* — List of upstream SHiP nodes (see [Upstream servers](#upstream-servers)). Mutually exclusive with `listeners`.
- **`listeners`** *(array, required in multi-listener mode, `≥ 1` entry)* — Named ingress listeners mapping distinct ports to isolated SHiP pools (see [Multiple networks](#multiple-networks)).

### Polling intervals

- **`upstream_reconnect_ms`** *(u64, required, `> 0`)* — Milliseconds between upstream reconnection attempts.
- **`upstream_monitoring_ms`** *(u64, required, `> 0`)* — Milliseconds between block-progress logging and staleness checks.
- **`upstream_status_ms`** *(u64, required, `> 0`)* — Milliseconds between status requests sent to each upstream. Only one request is in flight; a reply that misses this deadline marks the upstream offline.

Lower values detect failures and staleness faster at the cost of more polling traffic to upstreams; raise them to reduce chatter.

### Chain verification

- **`chain_id`** *(string, optional)* — 64-character hex `chain_id` in single-port mode; upstreams reporting a different `chain_id` are rejected and kept offline. Even when unset, any upstream whose reported `chain_id` mismatches the rest of its pool is automatically rejected.
- **`require_chain_id`** *(bool, optional, default `false`)* — Reject and keep offline any upstream whose status omits `chain_id`. Requires `chain_id`. Cannot be combined with `listeners`; set it on each listener instead.

A status that omits `chain_id` stays online unless `require_chain_id` is set. On `get_status_result_v0` that field is an optional extension, and older nodeos builds (FIO mainnet/testnet and other EOSIO 2.0-based nodes) never send it. In that case the router logs one warning per upstream that `chain_id` cannot be verified. The warning is cleared if a later status reports a `chain_id`, and logged again on the next omission.

### Failure handling

- **`block_failure_quarantine`** *(object, optional, off when omitted)* — After repeated unclean mid-`get_blocks` drops at the same block, skip that upstream for a short range. It stays online for every other block. Cannot be combined with `listeners`; set it on each listener instead. See [Corrupted SHiP logs](operations.md#corrupted-ship-logs).
  - **`strikes`** *(u32, default `2`, `> 0`)* — Unclean drops at one block, from any session, before the exclusion.
  - **`window_secs`** *(u64, default `300`, `> 0`)* — How long a strike counts toward `strikes`.
  - **`ttl_secs`** *(u64, default `600`, `> 0`)* — How long the exclusion lasts.
  - **`range`** *(u32, default `1`, `> 0`)* — Blocks skipped starting at the failed block K (`K..K+range`). `1` skips only K.

### Limits and timeouts

- **`max_connections`** *(usize, optional, default `10000`)* — Max concurrent client connections; excess are rejected (backpressure), not queued. Acts as the default per-listener cap when `listeners` is used.
- **`handshake_timeout_ms`** *(u64, optional, default `10000`)* — Client WebSocket handshake timeout; `0` disables it. The upstream monitor and `config test` use this same bound for connect and the first read, falling back to 10s when it is `0`.
- **`idle_timeout_ms`** *(u64, optional, default `0` / disabled)* — Close a connection idle (no data in either direction) for this long. Set it to reap dead clients.
- **`upstream_liveness_timeout_ms`** *(u64, optional, default `4000`, `0` disables)* — Detects an upstream that stops answering without closing the connection (cable pulled, VM frozen, firewall dropping packets, hung nodeos). After half this time with nothing from the upstream, the session sends it a WebSocket ping. If nothing at all arrives within the full time, the upstream is marked offline and the session fails over, resuming at the next block on the same client connection. When the monitor marks the upstream offline first, sessions on it ping right away and fail over after 1 second without a reply. An upstream that is quiet but answers pings (for example, waiting for a client to acknowledge blocks) is left alone. Raise it only if single blocks take seconds to transfer from your upstreams.
- **`max_message_bytes`** *(usize, optional, default `268435456` / 256 MiB)* — Max WebSocket message size on proxied upstreams and monitor connections. Lower it only if you are sure upstream blocks fit.
- **`max_client_message_bytes`** *(usize, optional, default `65536` / 64 KiB)* — Max WebSocket message and frame size accepted from a client. Omitting the field keeps existing configs valid and applies this default. See [Client message limits](#client-message-limits).
- **`shutdown_grace_ms`** *(u64, optional, default `5000`)* — How long to wait for in-flight connections to drain on shutdown.

### Health and metrics endpoint

- **`metrics_address`** *(string, optional, defaults to `listen_address`)* — Bind address for the health/metrics HTTP endpoint.
- **`metrics_port`** *(u16, optional, disabled when unset)* — Port for the health/metrics HTTP endpoint. Setting it enables the endpoint. See [Observability](observability.md).

### Dashboard

- **`dashboard`** *(object, optional, off when omitted)* — Live dashboard, status page, JSON API and control API on the `metrics_port` endpoint. See [Dashboard and control API](api.md).
  - **`enabled`** *(bool, default `false`)* — Serve `/ui/`, `/status` and `/api/v1`. Requires `metrics_port`. While `false`, the endpoint serves only health, readiness and metrics, as before.
  - **`control_token_file`** *(string, optional)* — Path to a file holding the control API bearer token. Surrounding whitespace is ignored.
  - **`control_token`** *(string, optional)* — The token inline. Prefer the file or the environment variable. Set at most one of `control_token` and `control_token_file`.
  - **`allowed_hosts`** *(array of strings, optional)* — Host names the dashboard answers to. Requests addressed by IP (`http://10.0.0.5:9090`) or `localhost` always work. Add a name here when you reach the dashboard by name, for example through a reverse proxy that keeps the original `Host` header. Requests for any other name get `403`; this blocks DNS-rebinding pages from reading the API from an operator's browser. Health, readiness and `/metrics` are not affected.

The control API (drain, enable/disable, live priority and weight, clear quarantine, move or close a session) is on only when a token is configured. The `FLEET_ROUTER_CONTROL_TOKEN` environment variable overrides both fields. A token must be at least 16 characters; generate one with `openssl rand -hex 32`. Without a token the dashboard is read-only.

```json
"metrics_address": "127.0.0.1",
"metrics_port": 9090,
"dashboard": {
  "enabled": true,
  "control_token_file": "/etc/fleet-router/control.token"
}
```

Read endpoints and the dashboard need no token, and they show client IP addresses and upstream addresses. Keep `metrics_address` on a private interface or behind a reverse proxy with access control. The router logs a warning when the control API is enabled on a non-loopback address. Changes made through the control API are not written back to this file.

## Upstream servers

Each entry in `servers` is an object:

- **`name`** *(string, required)* — Human-readable name used in logs and as the `server` label on each `/metrics` series for this upstream.
- **`endpoint`** *(string, required)* — Upstream as `host:port`, `ws://host:port`, or `wss://host:port`. A bare `host:port` is dialed as `ws://`. Endpoints must be unique within a listener's pool.
- **`enabled`** *(bool, required)* — Whether the router may use this upstream. At least one enabled server is required.
- **`priority`** *(u32, optional, default `0`)* — Preference tier. Lower is preferred. See [Upstream preference](#upstream-preference).
- **`weight`** *(u32, optional, default `1`, `≥ 1`)* — Relative share of sessions within the same priority tier. See [Upstream preference](#upstream-preference).

Uniqueness is the dial target: `127.0.0.1:8080` and `ws://127.0.0.1:8080` are the same upstream, and so are `ws://host` and `ws://host:80`. `ws://` and `wss://` to the same host are different. Unknown schemes (`https://`, `http://`, …) are rejected.

### Upstream preference

By default every upstream is equal and new sessions go to the one with the fewest active connections. Use `priority` and `weight` to change that.

- **`priority`** sorts upstreams into tiers. The router uses the lowest tier that can serve the request and moves to the next tier only when nothing in the better tier can. A large value (for example `100`) makes an upstream a **last resort**: it takes sessions only when every preferred upstream is offline, stale, quarantined for the block, or does not have the block.
- **`weight`** splits sessions inside one tier. The router picks the upstream with the fewest active connections per unit of weight, so a node with `"weight": 2` carries about twice the sessions of a node with `"weight": 1`. On a tie, the higher weight wins.

Preference never overrides whether an upstream can serve the request. For each new session and each failover, the router ranks candidates in this order:

1. Online, enabled, and not quarantined for the requested block (always required).
2. Trace range covers the requested block and the node is advancing, then covers but stale, then does not cover but advancing, then neither.
3. Lowest `priority`.
4. Fewest connections per unit of `weight`, then the higher `weight`.

So a preferred node that is stale or missing the requested block loses to a backup node that has it. When the upstream serving a session fails, failover moves to another upstream that covers the block as well, even if that upstream is in a lower-preference tier.

Priority is applied when a session is placed. Sessions already on a backup upstream stay there when a preferred upstream comes back; new sessions go to the preferred one.

```json
"servers": [
  { "name": "local-1",  "endpoint": "10.0.0.10:8080",         "enabled": true, "weight": 2 },
  { "name": "local-2",  "endpoint": "10.0.0.11:8080",         "enabled": true },
  { "name": "provider", "endpoint": "wss://ship.example:443", "enabled": true, "priority": 100 }
]
```

Here `local-1` and `local-2` share all traffic about 2:1, and `provider` is used only when neither local node can serve a request.

`/metrics` reports each upstream's settings as `fleet_router_upstream_priority` and `fleet_router_upstream_weight`.

## Upstream TLS (`wss://`)

`wss://` is dialed with rustls (not OpenSSL). The router verifies the upstream certificate against the operating-system trust store. The published container image installs CA certificates, so public CAs work there. For a private CA, install it into that store, or set `SSL_CERT_FILE` / `SSL_CERT_DIR` to PEM certificates. When either variable is set, those paths replace the OS store.

The client-facing listener stays plaintext `ws://`. Put TLS for clients on a reverse proxy in front of fleet-router (see [Security](operations.md#security)).

## Client message limits

Client requests are capped by `max_client_message_bytes` (default 64 KiB) on both the WebSocket message size and the frame size. Upstream block payloads still use `max_message_bytes`.

While an upstream failover is in progress, client frames that cannot be forwarded are buffered up to **64 KiB** per session. Each buffered frame counts as at least one byte, so empty frames cannot grow the queue without a bound. Exceeding that cap closes the session with WebSocket close code 1008 (policy violation).

## Multiple networks

To expose several ingress ports for different chains, each backed by its own redundant SHiP pool, you can either:

1. **Use `listeners` in one `fleet-router` process.** Each entry binds its own port and keeps an isolated upstream pool, monitor loop, and connection semaphore.
2. **Run one `fleet-router` instance per network** for process or container isolation. See [`deploy/docker-compose.multi-network.yml`](../deploy/docker-compose.multi-network.yml) and [`deploy/fleet-router@.service`](../deploy/fleet-router@.service).

### Listener fields

- **`name`** *(string, required)* — Unique listener/network identifier (no `/` or whitespace). Used in logs, `/metrics` (`listener="<name>"`), and `/ready/<name>`.
- **`listen_port`** *(u16, required, non-zero)* — Ingress TCP port for this listener. Must not collide with other listeners or `metrics_port`.
- **`listen_address`** *(string, optional)* — Bind address override; defaults to top-level `listen_address` (or `"0.0.0.0"`).
- **`max_connections`** *(usize, optional)* — Per-listener concurrent client cap; defaults to top-level `max_connections`.
- **`chain_id`** *(string, optional, recommended)* — 64-character hex `chain_id` for this network; upstreams reporting a different `chain_id` are rejected and kept offline. Behaves like the [top-level field](#chain-verification).
- **`require_chain_id`** *(bool, optional, default `false`)* — When `true`, a status that omits `chain_id` is rejected and the upstream stays offline. Requires this listener's `chain_id`.
- **`block_failure_quarantine`** *(object, optional)* — Same fields as the [top-level object](#failure-handling) (`strikes`, `window_secs`, `ttl_secs`, `range`). Applies only to this listener's pool.
- **`servers`** *(array, required, `≥ 1` enabled)* — Redundant SHiP nodes belonging to this network. Referencing the same `endpoint` across two different listeners is allowed (e.g. a dedicated VIP vs. a public port) and spawns an independent upstream monitor per listener.

> **Always pin `chain_id` in multi-network setups.** When a listener's `chain_id` is omitted, fleet-router logs a startup warning and trusts the first currently-online upstream in the pool as the reference `chain_id`. If a misconfigured wrong-chain node answers first, the remaining nodes are rejected until it goes offline.

### Example

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
      "chain_id": "aca376f206b8fc25a6ed44dbdc66547c36c6c33e3a119ffbeaef943642f0e906",
      "servers": [
        { "name": "EOS SHiP 1", "endpoint": "10.0.1.10:8080", "enabled": true },
        { "name": "EOS SHiP 2", "endpoint": "10.0.1.11:8080", "enabled": true }
      ]
    },
    {
      "name": "wax",
      "listen_port": 17001,
      "chain_id": "1064487b3cd1a897ce03ae5b6a865651747e2e152090f99c1d19d44e01aea5a4",
      "servers": [
        { "name": "WAX SHiP 1", "endpoint": "10.0.2.10:8080", "enabled": true },
        { "name": "WAX SHiP 2", "endpoint": "10.0.2.11:8080", "enabled": true }
      ]
    }
  ]
}
```
