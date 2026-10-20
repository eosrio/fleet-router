# Configuration reference

fleet-router reads one JSON file (`./config.json` by default; pass another with `--config`). Run `fleet-router config init <path>` to write a sample and `fleet-router config test <path>` to validate it and test upstream connectivity.

A config uses one of two modes:

- **Single-port mode**: `listen_port` and `servers` at the top level. One port, one pool of SHiP nodes.
- **Multi-listener mode**: a `listeners` array. Each listener has its own port, pool, `chain_id` and readiness check. See [Multiple networks](#multiple-networks).

Each listener presents its upstreams in one of two topologies: **pool** (identical full nodes, the default) or **stitched** (live nodes plus frozen range servers, presented to clients as one SHiP node). See [Topology and stitching](#topology-and-stitching).

All `*_ms` fields are in **milliseconds**.

- [Minimal example](#minimal-example)
- [Top-level fields](#top-level-fields)
  - [Dashboard](#dashboard)
- [Reloading configuration](#reloading-configuration)
- [Upstream servers](#upstream-servers)
  - [Upstream preference](#upstream-preference)
- [Topology and stitching](#topology-and-stitching)
- [Chain API](#chain-api)
- [Upstream TLS (`wss://`)](#upstream-tls-wss)
- [Client message limits](#client-message-limits)
- [Multiple networks](#multiple-networks)

## Reloading configuration

With the dashboard and a control token enabled, open **Configuration** and unlock controls:

- To apply an external file edit, choose **Reload file** and review the validation summary.
- To edit in the dashboard, change the JSON, choose **Validate**, then **Save and apply**. **Load file** replaces the draft without applying anything.

Both routes validate the whole configuration and reserve new listening ports before applying it. A validation error or occupied port leaves the running configuration unchanged; a failed save also leaves the file unchanged. Stale browser drafts and intervening file edits return a conflict instead of overwriting a newer revision. The configuration source is limited to 1 MiB. File edits are applied explicitly; there is no automatic file watcher.

Listeners and upstreams can be added, removed or replaced, including chain API and dashboard ports. New connections use the new configuration. Established SHiP streams retain their previous upstream pool, failover policy and limits until they disconnect; established chain API connections likewise retain their previous policy and pool. Removing a listener stops new connections and releases its port. Its monitors remain while established connections need them, then stop. Configuration and Sessions show these connections as **draining**. There is no forced drain deadline, so long-lived streams can retain an old pool indefinitely. Keep removed upstreams available if those streams still depend on them.

Unchanged listeners keep their running state. Connection caps on a listener include its draining generations; lowering a cap rejects new connections until enough old ones finish. For a retained SHiP endpoint with the same listener and chain policy, upstream connection counts and cumulative counters survive replacement, including removal and re-addition while old streams remain. A renamed listener is a new identity. Other request limits remain specific to each connection's generation.

The editor reads the file selected at startup by `--config` (resolved to an absolute path). Saving uses an atomic replacement in the same directory; the service account needs permission to create and replace files there. Existing file permissions are retained. In container deployments, mount a writable configuration directory rather than a single bind-mounted file if dashboard saving is needed. A read-only file can still be reloaded after an external edit. A conflicting address change, such as replacing a wildcard bind with a specific address on the same occupied port, is refused: use a free port or restart for that change.

Inline `dashboard.control_token` values appear as `__FLEET_ROUTER_KEEP_SECRET__`; leave this placeholder unchanged to retain the saved token. Token files and environment tokens are resolved again on apply. If you change the dashboard address or token, reconnect or unlock with the new settings. Disabling the dashboard or removing its metrics endpoint also removes the reload API; re-enable it through a process restart. Existing event streams close when the dashboard is disabled or their hostname is no longer allowed.

The [configuration API](api.md#configuration-endpoints) exposes the same validation, revision checks and apply path.

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
- **`topology`** *(string, optional, default `"auto"`)* — How the listener presents its upstreams. See [Topology and stitching](#topology-and-stitching). Cannot be combined with `listeners`; set it on each listener instead.
- **`stitching`** *(object, optional)* — Session policy of a stitched listener. See [Topology and stitching](#topology-and-stitching). Cannot be combined with `listeners`; set it on each listener instead.
- **`chain_api`** *(object, optional, off when omitted)* — An HTTP chain API port in front of the listener's servers. See [Chain API](#chain-api). Cannot be combined with `listeners`; set it on each listener instead.

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

- **`block_failure_quarantine`** *(object, optional, off when omitted)* — After repeated unclean mid-`get_blocks` drops at the same block (for a frozen upstream, also stalls and archive contract violations), skip that upstream for a short range. It stays online for every other block. Cannot be combined with `listeners`; set it on each listener instead. See [Corrupted SHiP logs](operations.md#corrupted-ship-logs).
  - **`strikes`** *(u32, default `2`, `> 0`)* — Unclean drops at one block, from any session, before the exclusion.
  - **`window_secs`** *(u64, default `300`, `> 0`)* — How long a strike counts toward `strikes`.
  - **`ttl_secs`** *(u64, default `600`, `> 0`)* — How long the exclusion lasts.
  - **`range`** *(u32, default `1`, `> 0`)* — Blocks skipped starting at the failed block K (`K..K+range`). `1` skips only K.

### Limits and timeouts

- **`max_connections`** *(usize, optional, default `10000`, `1..=1000000`)* — Max concurrent client connections; excess are rejected (backpressure), not queued. Acts as the default per-listener cap when `listeners` is used.
- **`handshake_timeout_ms`** *(u64, optional, default `10000`)* — Client WebSocket handshake timeout; `0` disables it. The upstream monitor and `config test` use this same bound for connect and the first read, falling back to 10s when it is `0`, and a session's upstream connect is bounded the same way. The [chain API](#chain-api) uses it (10s when `0`) for a client's request head and body.
- **`idle_timeout_ms`** *(u64, optional, default `0` / disabled)* — Close a client connection that sends no frame for this long. Set it to reap dead clients. An upstream link that sends nothing for this long while it owes the client blocks is failed over, even when it is the only live upstream (the session then redials it); an upstream that is quiet because the client is at the chain tip, or has no credit left, is not.
- **`upstream_liveness_timeout_ms`** *(u64, optional, default `4000`, `0` disables)* — Detects an upstream that stops answering without closing the connection (cable pulled, VM frozen, firewall dropping packets, hung nodeos). After half this time with nothing read from the upstream, the session sends it a WebSocket ping. If nothing at all arrives within the full time, the session fails over, resuming at the next block on the same client connection; a live upstream is also marked offline, a frozen one is not. When the monitor marks the upstream offline first, sessions on it ping right away and fail over after 1 second with no byte from it. Every byte read counts, including the parts of a large message still on its way, so a slow transfer is not taken for silence. An upstream that is quiet but answers pings (for example, waiting for a client to acknowledge blocks) is left alone.
- **`upstream_stall_timeout_ms`** *(u64, optional, default `60000`, `0` disables, otherwise `≥ 1000`)* — Fail over an upstream link that sends no bytes for this long while it owes the client blocks: the client has credit left, and the upstream has blocks to send below the request's end and at or below its head (or the fleet head). This catches a node that answers pings but stops streaming. A live upstream is failed over then only when another online upstream can serve the next block; otherwise it is kept, with one warning, until the silence reaches three times this timeout, and then failed over, which redials it. A live stall is not counted against the upstream. A frozen upstream is always failed over after this timeout, and it counts as a block failure (see [Failover and strikes](operations.md#failover-and-strikes)). `idle_timeout_ms` and the 5 s rule for an upstream the monitor marked offline still fail over a sole live upstream sooner.
- **`live_lag_blocks`** *(u32, optional, default `1200`, `0` disables)* — A live upstream more than this many blocks behind the fleet head is ranked like a stale one for blocks above its own head, so a closer node serves them. The [chain API](#chain-api) also skips it for state calls while a closer node is up.
- **`max_message_bytes`** *(usize, optional, default `268435456` / 256 MiB)* — Max WebSocket message size on proxied upstreams and monitor connections. Lower it only if you are sure upstream blocks fit. A larger message closes the client with code 1011 and reason `block <K> exceeds max_message_bytes`; the upstream is not counted against.
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
- **`role`** *(string, optional, default `"live"`, one of `"live"`, `"frozen"`)* — `"live"` is a node at the chain head. `"frozen"` is a frozen range server: an archive upstream that serves a fixed block range from archived logs and never advances. An enabled frozen server makes its listener stitched (see [Topology and stitching](#topology-and-stitching)). The role is never inferred, because a stuck live node looks like a frozen one. When a live upstream stops advancing with its head, LIB and last block equal, the router only logs a hint to set the role.
- **`range`** *(object, optional, `role: "frozen"` only)* — The frozen server's coverage, `{"begin": n, "end": m}` with `end` exclusive and `1 ≤ begin < end`. It stands in for the reported ranges until the server first reports them, and while it is down, so a listener can answer status at startup and its coverage does not shrink when an archive restarts. A reported trace or chain-state range that differs from it is logged once and exported as `fleet_router_upstream_range_mismatch`. A log that starts at block 2 when `begin` is 1, or ends one block before `end` while the other log reaches `end`, is not a mismatch.
- **`max_sessions`** *(u32, optional, `> 0`, no limit when omitted)* — Most connections the router's sessions hold to this upstream at once: session links, pre-dialed standby links and dials in progress. The upstream monitor's connection is not counted. A full upstream is skipped. Set it at or below a frozen range server's own session limit, so sessions wait in the router instead of being refused.
- **`api_endpoint`** *(string, optional)* — The server's HTTP chain API, as `http://host[:port]` (port 80 by default) or `host:port`. For a live server, its nodeos `http-server-address`; for a frozen range server, its chain API address. Used only by a listener with [`chain_api`](#chain-api), and ignored with a warning otherwise. `https://` is rejected (not supported yet), and so are a path (a trailing `/` is allowed), a query, a fragment, user info, port `0` and a port that does not parse. It must be unique among the listener's enabled servers; hosts are compared without case. The host is sent as written in the `Host` header, which nodeos checks by default.

Uniqueness is the dial target: `127.0.0.1:8080` and `ws://127.0.0.1:8080` are the same upstream, and so are `ws://host` and `ws://host:80`. `ws://` and `wss://` to the same host are different. Unknown schemes (`https://`, `http://`, …) are rejected.

### Upstream preference

By default every upstream is equal and new sessions go to the one with the fewest active connections. Use `priority` and `weight` to change that.

- **`priority`** sorts upstreams into tiers. The router uses the lowest tier that can serve the request and moves to the next tier only when nothing in the better tier can. A large value (for example `100`) makes an upstream a **last resort**: it takes sessions only when every preferred upstream is offline, full, stale, quarantined for the block, or does not have the block.
- **`weight`** splits sessions inside one tier. The router picks the upstream with the fewest active connections per unit of weight, so a node with `"weight": 2` carries about twice the sessions of a node with `"weight": 1`. On a tie, the higher weight wins.

Preference never overrides whether an upstream can serve the request. For each new session and each failover in a pool listener, the router ranks candidates in this order (a `get_blocks` stays on the session's upstream when it covers the start block and does not lag, and otherwise moves to the best of these that serves it better):

1. Enabled, not disabled by an operator, below `max_sessions`, not quarantined for the requested block (always required), and online (on failover, an offline upstream is a last resort).
2. Covers the requested block and is advancing, then covers but stale or lagging (see `live_lag_blocks`), then does not cover but advancing, then neither. Covering means having the block in every log the request asks for (traces, chain state for deltas, finality data) that some online upstream has at that block. A live node covers every block from its first one on, including the next block at the head.
3. Lowest `priority`.
4. Fewest connections per unit of `weight`, then the higher `weight`.
5. A hash of the session and the endpoint, so equal upstreams are picked evenly and the same way for a session.

So a preferred node that is stale or missing the requested block loses to a backup node that has it. When the upstream serving a session fails, failover moves to another upstream that covers the block as well, even if that upstream is in a lower-preference tier.

A stitched listener picks the upstream for each leg of a request (see [Range-split fleets](operations.md#range-split-fleets)) in this order: online; fresh and not lagging; below `max_sessions`; for a request with `fetch_block`, not known to lack a block log; the role `history_preference` prefers; not the upstream the session just failed over from; lowest `priority`; the longest run of blocks it can serve from the cursor; fewest connections per unit of `weight`, then the higher `weight`; then the hash. Priority and weight come after the role preference and capacity, so neither pulls history reads onto a live node or a session onto a full upstream. With the default priority and weight, the longest run decides, which saves handoffs, and weight splits sessions among replicas of one range.

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

## Topology and stitching

`topology` decides how a listener presents its upstreams to clients:

- **`"pool"`**: every upstream is a full node. A client session is on one upstream at a time and fails over to another. Configs without frozen servers behave this way.
- **`"stitched"`**: the listener presents its upstreams as one SHiP node whose history is the union of their ranges. Live nodes serve the chain tip and frozen range servers serve archived history. See [Range-split fleets](operations.md#range-split-fleets) for what the router does.
- **`"auto"`** (the default): `"stitched"` when any enabled server has `"role": "frozen"`, otherwise `"pool"`.

These fields go on each listener, or at the top level in single-port mode:

- **`topology`** *(string, optional, default `"auto"`, one of `"auto"`, `"pool"`, `"stitched"`)* — `"pool"` with an enabled frozen server is rejected. A stitched listener requires `chain_id`.
- **`stitching`** *(object, optional)* — Session policy of a stitched listener. A pool listener ignores it and logs a warning. Every field is optional:
  - **`history_preference`** *(string, default `"frozen"`, one of `"frozen"`, `"any"`)* — For blocks that both a frozen and a live upstream cover, `"frozen"` sends sessions to the frozen one, which keeps history reads off the live nodes. `"any"` has no role preference.
  - **`predial_blocks`** *(u32, default `2000`, `0` disables)* — When this many blocks or fewer remain before the end of a leg, connect to the next leg's upstream (WebSocket and ABI only), so the handoff does not wait for a connect. The standby link counts toward that upstream's `max_sessions` and is dropped after 60 s unused.
  - **`gap_wait_ms`** *(u64, default `30000`, `1..=3600000`)* — How long a session holds for archived blocks whose upstreams are all offline, quarantined, busy or full before it closes the client with 1011. Blocks that only live nodes serve are waited for without a limit. See [Holds, waits and gaps](operations.md#holds-waits-and-gaps).
  - **`patch_head_lib`** *(bool, default `true`)* — Replace head and LIB in frozen results with the fleet's head and LIB, and keep both from going below what the session already sent. With `false`, every result keeps the head and LIB its upstream wrote.
  - **`abi_file`** *(string, optional)* — Path to a SHiP ABI that the listener sends to every client instead of one taken from an upstream. Validation reads it and checks it loads like an upstream's ABI.
  - **`abi_mismatch`** *(string, default `"warn"`, one of `"warn"`, `"exclude"`)* — What to do with an upstream whose ABI differs from the one clients are sent. `"warn"` logs it once and keeps using it. `"exclude"` stops routing sessions to it.

Two global fields matter most in a stitched listener, although they apply to pool listeners too: [`upstream_stall_timeout_ms`](#limits-and-timeouts) and [`live_lag_blocks`](#limits-and-timeouts).

A minimal stitched listener, with two replicas of one archived range and two live nodes:

```json
{
  "name": "main",
  "listen_port": 17000,
  "chain_id": "aca376f206b8fc25a6ed44dbdc66547c36c6c33e3a119ffbeaef943642f0e906",
  "servers": [
    { "name": "archive-a", "endpoint": "10.0.3.10:8080", "enabled": true, "role": "frozen", "range": { "begin": 2, "end": 50000001 }, "max_sessions": 64 },
    { "name": "archive-b", "endpoint": "10.0.3.11:8080", "enabled": true, "role": "frozen", "range": { "begin": 2, "end": 50000001 }, "max_sessions": 64 },
    { "name": "live-1",    "endpoint": "10.0.1.10:8080", "enabled": true },
    { "name": "live-2",    "endpoint": "10.0.1.11:8080", "enabled": true }
  ]
}
```

[`example.stitched.config.json`](../example.stitched.config.json) is a complete file with two archived ranges and a [chain API](#chain-api).

Validation rejects:

- `range` on a server without `"role": "frozen"`, and a `range` without `1 ≤ begin < end`;
- `max_sessions` of `0`;
- `"topology": "pool"` with an enabled frozen server;
- a stitched listener without `chain_id`;
- `gap_wait_ms` outside `1..=3600000`, and an `abi_file` that cannot be read or is not a SHiP ABI;
- `upstream_stall_timeout_ms` between `1` and `999`.

These are warnings, logged at startup and printed by `config test`:

- `stitching` on a pool listener is ignored.
- A stitched listener without an enabled live server: status reports the newest archived block as head, and sessions wait at the end of history.
- A frozen server without `range`: the listener holds status requests while that server is down at startup (see [What clients see](operations.md#what-clients-see)).
- The declared archive ranges leave a hole: a block range no archive declares, below the highest declared end.

`fleet-router config test --strict <path>` also probes every server and prints the coverage each stitched listener adds up to, and exits 1 on a hole in the trace or chain-state coverage, a frozen server that did not report its ranges, or (with a [chain API](#chain-api)) archived blocks that no frozen server with an `api_endpoint` covers.

## Chain API

`chain_api` opens an HTTP port in front of a listener's servers that answers like a nodeos chain API: block endpoints are routed by block number to the frozen range server or live node that holds the block, and `get_info` and read-only state calls go to live nodes. It is off when omitted. Each server takes part through its [`api_endpoint`](#upstream-servers). See [Chain API](chain-api.md) for how requests are routed and answered.

```json
"chain_api": { "listen_port": 18888, "listen_address": "10.0.0.5" }
```

Set it on each listener, or at the top level in single-port mode. An older fleet-router ignores the field without an error.

- **`chain_api`** *(object, optional, off when omitted)*:
  - **`listen_port`** *(u16, required, non-zero)* — Port of the chain API. It must not collide with a SHiP listener, `metrics_port` or another listener's `chain_api` on an overlapping address.
  - **`listen_address`** *(string, optional, defaults to the listener's `listen_address`)* — Bind address.
  - **`allow`** *(array of strings, default `["block", "info", "state"]`)* — Endpoint classes the port serves, each at most once, not empty:
    - `"block"`: `get_block`, `get_raw_block`, `get_block_header`, `get_block_header_state` and `get_block_info`, routed by block number;
    - `"info"`: `get_info`, answered by one live node;
    - `"state"`: every other `/v1/chain/*` call (`get_account`, `get_table_rows`, `get_abi`, ...), answered by live nodes;
    - `"exec"`: `compute_transaction` and `send_read_only_transaction`;
    - `"push"`: `push_transaction`, `push_transactions`, `send_transaction` and `send_transaction2`. The port then relays transactions for anyone who can reach it.

    A request for a class that is not listed gets nodeos's `404 Unknown Endpoint`.
  - **`block_source`** *(string, default `"frozen_first"`, one of `"frozen_first"`, `"live_first"`)* — Who answers first for an irreversible block that both a frozen range server and a live node hold. `"frozen_first"` keeps history reads off the live nodes. `get_block` always goes to live nodes.
  - **`push_servers`** *(array of server names, optional)* — Live servers that receive pushed transactions, tried in this order. Requires `"push"` in `allow`. Each must be an enabled live server with `api_endpoint`, listed once. Empty: the live node that answers `get_info`.
  - **`max_body_bytes`** *(usize, default `2097152` / 2 MiB, `1024..=67108864`)* — Largest request body. A larger one gets `413` and the connection is closed.
  - **`max_response_bytes`** *(u64, default `0` / no limit, otherwise `≥ 65536`)* — Largest upstream answer body the port relays. An answer whose `Content-Length` is larger gets `502` (`Upstream answer exceeds <n> bytes`) and none of it is sent; an answer without a `Content-Length` (chunked) is cut off once it passes this, which closes the client connection. A push's answer is relayed whole whatever this says, since it reports a push the node may have applied. nodeos sends every answer with its `Content-Length` and limits none, so leave it at `0` unless the port must bound what clients can be sent.
  - **`max_connections`** *(usize, default `1024`, `1..=1000000`)* — Client connections open at once. Excess connections are closed when they are accepted.
  - **`max_in_flight`** *(usize, default `256`, `1..=1000000`)* — Requests processed at once on this port. More get `503 Busy` (`Too many requests in flight: <n>`).
  - **`max_in_flight_per_upstream`** *(usize, default `64`, `1..=1000000`)* — Requests in flight to one upstream API. For a frozen range server, the lower of this and the limit the server reports.
  - **`upstream_queue_ms`** *(u64, default `1000`, at most `request_deadline_ms`)* — How long a request waits for a free upstream slot when every candidate is full, before it gets `503 Busy`.
  - **`upstream_timeout_ms`** *(u64, default `10000`, `≥ 100`)* — Per attempt, until the upstream's response head. Also the longest gap between two parts of an upstream's answer body, and the timeout of each probe.
  - **`request_deadline_ms`** *(u64, default `30000`, at least `upstream_timeout_ms`)* — The whole request, every attempt included.
  - **`max_attempts`** *(u32, default `3`, `1..=8`)* — Upstreams tried for one request.
  - **`probe_ms`** *(u64, default `1000`, `100..=60000`)* — Interval between probes of each upstream API.
  - **`cors`** *(object, optional)* — CORS headers the router sets on every answer. Without it, the router answers like a nodeos with no CORS configured, and browser clients need it.
    - **`allow_origin`** *(string, required)* — `Access-Control-Allow-Origin`. Not empty, and a valid header value.
    - **`allow_headers`** *(string, optional)* — `Access-Control-Allow-Headers`. Not empty, and a valid header value.
    - **`max_age`** *(u32, optional)* — `Access-Control-Max-Age`, in seconds.
    - **`allow_credentials`** *(bool, default `false`)* — Also send `Access-Control-Allow-Credentials: true`.

A `chain_api` needs at least one enabled server with `api_endpoint`. Three more settings apply to it: `handshake_timeout_ms` bounds a client's request head and body (10 s when `0`) and closes idle keep-alive connections; `live_lag_blocks` drops laggards from state calls; and an upstream connect is bounded like a session's upstream connect.

These are warnings, logged at startup and printed by `config test`:

- `api_endpoint` is set on a listener without `chain_api`: it is ignored.
- An enabled frozen server on a `chain_api` listener has no `api_endpoint`: the chain API does not use it. Its blocks are still answered by another frozen server whose API covers them (its replica, say), else by live nodes only.
- No enabled live server has an `api_endpoint`: `get_info`, state calls, pushes and reversible blocks answer `503`.
- `allow` contains `"push"`: the port relays transactions for anyone who can reach it.
- The chain API listens on every interface (`0.0.0.0` or `::`): it has no authentication.

`config test` prints each chain API listener's servers with their role and `api_endpoint`, with a warning for each enabled frozen server that has none. With `--strict` it exits 1 when archived blocks of the listener's coverage (the trace and chain-state ranges of its enabled frozen servers) have no enabled frozen server with an `api_endpoint` covering them, so that only live nodes could answer them. A frozen server without a block log cannot serve a chain API at all; it passes `--strict` as long as a replica with an `api_endpoint` covers its range.

## Upstream TLS (`wss://`)

`wss://` is dialed with rustls (not OpenSSL). The router verifies the upstream certificate against the operating-system trust store. The published container image installs CA certificates, so public CAs work there. For a private CA, install it into that store, or set `SSL_CERT_FILE` / `SSL_CERT_DIR` to PEM certificates. When either variable is set, those paths replace the OS store.

The client-facing listener stays plaintext `ws://`. Put TLS for clients on a reverse proxy in front of fleet-router (see [Security](operations.md#security)).

## Client message limits

Client requests are capped by `max_client_message_bytes` (default 64 KiB) on both the WebSocket message size and the frame size. Upstream block payloads still use `max_message_bytes`.

A client frame that is not a valid SHiP request closes the session: a malformed request with WebSocket close code 1007 (`invalid request`). A request variant the router does not know is forwarded to the upstream in a pool listener, for nodes newer than the router; a stitched listener closes the session with code 1003 (`unsupported request variant <n>`).

While no upstream link is up (during a failover), acks are added to the client's credit and status requests are queued; neither is buffered. Only those unknown-variant frames are buffered, up to **64 KiB** per session. Each buffered frame counts as at least one byte, so empty frames cannot grow the queue without a bound. Exceeding that cap closes the session with WebSocket close code 1008 (policy violation). More than 1024 status requests waiting for an answer also close it with 1008.

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
- **`topology`**, **`stitching`** *(optional)* — This listener's topology and stitching policy. See [Topology and stitching](#topology-and-stitching).
- **`chain_api`** *(object, optional)* — This listener's chain API port. See [Chain API](#chain-api).
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
