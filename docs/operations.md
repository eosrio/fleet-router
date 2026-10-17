# Running in production

- [How routing and failover work](#how-routing-and-failover-work)
- [Range-split fleets](#range-split-fleets)
  - [What clients see](#what-clients-see)
  - [Deploying frozen range servers](#deploying-frozen-range-servers)
  - [Seams and handoffs](#seams-and-handoffs)
  - [Holds, waits and gaps](#holds-waits-and-gaps)
  - [Failover and strikes](#failover-and-strikes)
  - [fetch_block and block logs](#fetch_block-and-block-logs)
  - [Readiness and coverage](#readiness-and-coverage)
- [Security](#security)
- [Run as a service](#run-as-a-service) (systemd, Docker, launchd, Windows)
- [Graceful shutdown](#graceful-shutdown)
- [Corrupted SHiP logs](#corrupted-ship-logs)
- [Tuning](#tuning)

## How routing and failover work

A client connects over WebSocket. In a pool listener (identical full nodes, the default), the router selects an upstream, forwards that upstream's ABI to the client, and then proxies frames in both directions. A stitched listener presents live nodes and frozen range servers as one node; see [Range-split fleets](#range-split-fleets).

- **Selection.** Each `get_blocks` is checked against the session's upstream before it is forwarded. The upstream must have the start block in every log the request asks for (traces, chain state for deltas, finality data for `fetch_finality_data`) that some online upstream has at that block. A live node has every block from its first one on, so a request for the next block at the head is covered. If the upstream does not cover the request, or lags more than [`live_lag_blocks`](configuration.md#limits-and-timeouts) behind the fleet head for it, the request goes to an upstream that serves it better (a reroute). Among equally suitable upstreams, the optional per-server [`priority` and `weight`](configuration.md#upstream-preference) pick a preferred tier and split load inside it, and `max_sessions` caps the links to one upstream.
- **Health.** Background loops poll each upstream's status and block progress. An upstream that stops advancing is marked stale and deprioritized, not hard-excluded. An upstream whose status reply misses the `upstream_status_ms` deadline is marked offline. A session dial that cannot reach a live upstream, or gets no valid ABI from it, marks it offline too. An upstream that refuses the WebSocket upgrade with HTTP 503 is busy: that session avoids it for 2 s, and it stays online.
- **Failover.** If the active upstream drops, closes, goes silent without closing the connection (see [`upstream_liveness_timeout_ms`](configuration.md#limits-and-timeouts)), or stops sending blocks it owes (see [`upstream_stall_timeout_ms`](configuration.md#limits-and-timeouts)), the router selects another suitable upstream and sends it the request again, from the client's next block and with the client's outstanding credit. Acks the client sent while no link was up are added to that credit, not replayed. Blocks the new upstream sends again are dropped by block number and id, and each one is refunded to it with an ack; a block number delivered before with a new id (a fork) is forwarded. An upstream WebSocket Close frame is never forwarded to the client. The client connection stays open throughout. Repeated unclean drops at the same block are bounded; see [Corrupted SHiP logs](#corrupted-ship-logs).
- **Status.** The router answers `get_status_request_v0` and `_v1` itself, with the matching result version, in both topologies. Head and LIB are the fleet's: the highest of the fresh online live upstreams' status and of the blocks sessions stream from them, never lower than what that client was already sent, and with LIB at or below head. Each log's range is the top contiguous block range the upstreams cover. `chain_id` is the pinned one, else the one the upstreams report; with neither, a v0 reply leaves it out (as older nodes do) and a v1 reply carries zeros, with one warning. Until the fleet head and LIB are known, a pool session sends status requests to its upstream and forwards the replies in order. A status request that cannot be answered within `handshake_timeout_ms` (10 s when `0`) closes the client with 1011 `fleet status unavailable`. Status replies cost no credit.
- **Head and LIB in results.** Within a session, head and LIB never go back. A result from a live node whose head or LIB is below what the session already sent (right after a failover to a node that lags) is rewritten with the higher values. A stitched listener with `patch_head_lib: false` forwards results unchanged.

fleet-router is written in Rust on [tokio](https://tokio.rs/) and [tokio-tungstenite](https://github.com/snapview/tokio-tungstenite), and uses [rs_abieos](https://github.com/eosrio/rs-abieos) (pure-Rust backend) to check that an upstream SHiP ABI parses. Each handshake, failover, and monitor builds a short-lived `AbiHandle` and drops it; connections do not share an abieos context. Upstream TLS uses rustls with the [aws-lc-rs](https://github.com/aws/aws-lc-rs) provider.

## Range-split fleets

A range-split fleet serves old history and the chain tip from different upstreams:

- **Live nodes** (`"role": "live"`, the default) run nodeos with state history at the chain head. They serve the tip and the recent history they still hold.
- **Frozen range servers** (`"role": "frozen"`) are archive upstreams. Each serves one fixed block range from archived state-history logs (and block logs, where it has them), with no nodeos behind it. It speaks the SHiP WebSocket protocol, never advances, and reports the same ranges for as long as it runs.

A listener with an enabled frozen server is **stitched** ([`topology`](configuration.md#topology-and-stitching)). The router presents its upstreams to clients as one SHiP node whose history is the union of their ranges, so a client reads from the first archived block to the head through one connection. With the default [`history_preference`](configuration.md#topology-and-stitching), blocks that an archive covers are read from the archive, not from a live node. With live servers configured, tip readiness (`/ready`) does not depend on the archives.

[`example.stitched.config.json`](../example.stitched.config.json) is a complete config with two archived ranges, two replicas each, and two live nodes.

### What clients see

- **ABI.** The client gets the listener's ABI as soon as it connects, without a dial: the [`abi_file`](configuration.md#topology-and-stitching) when set, otherwise the ABI of the online live upstream with the highest head. That choice stays while an online live upstream has the same ABI. With no live upstream online, the most common ABI of the online frozen upstreams is used until a live one is seen. A session keeps the ABI it was sent. When no ABI and no online upstream are known within `handshake_timeout_ms` (10 s when `0`), the client is closed with 1011 `No upstream server available`.
- **Status.** Always answered by the router (see [How routing and failover work](#how-routing-and-failover-work)). Each log's range is the top contiguous range of its coverage; when live nodes are part of it, it ends at the fleet head + 1. Coverage is sticky: an upstream going offline does not shrink it, and a frozen server counts with its last reported ranges, or its declared `range` before it first reports. While an enabled frozen server has neither reported nor declared a range, status requests wait for it, for at most `handshake_timeout_ms`. A client that only asks for status opens no upstream link.
- **Blocks.** Each `get_blocks` is planned from the client's cursor: an upstream that has the cursor block in every log the request asks for that the fleet has there, and the end of its run: the first block it cannot serve as fully as the fleet can, such as the end of its range, or a block where another upstream starts to have finality data the request asks for. A frozen upstream's request also ends at the fleet head + 1, or the fleet LIB + 1 with `irreversible_only`. When the client's cursor reaches the end of a leg, the router hands the stream off to the next upstream (see [Seams and handoffs](#seams-and-handoffs)).
- **Head and LIB.** With [`patch_head_lib`](configuration.md#topology-and-stitching) (the default), head and LIB in a frozen result are replaced with the fleet head and LIB, and no result has a head or LIB below what the session already sent.
- **Requests.** A request variant the router does not know closes the client with 1003 `unsupported request variant <n>`.

The fleet head and LIB come from the online live nodes that are not stale or disabled by an operator (all online ones when every one is stale), including the blocks sessions stream from them, so they are fresher than the monitor's status interval. The fleet LIB never goes back. While every live node is offline, the last known values stay. A listener with no live server reports the newest archived block as both.

### Deploying frozen range servers

1. **Archive only irreversible blocks.** A range must end at or below the LIB of the node its logs came from, when they were copied. The router cannot check this inside a range; it checks it at seams (see [Seams and handoffs](#seams-and-handoffs)) and warns once per archive when an archive serves blocks above the LIB of the live nodes (`frozen upstream serves blocks above the fleet LIB`).
2. **Keep the block log** with the state-history logs, so requests with `fetch_block` get blocks from the archive (see [fetch_block and block logs](#fetch_block-and-block-logs)), and so its [chain API](chain-api.md) can serve blocks.
3. **Run two replicas of each range** on separate hosts. Sessions on a failed archive resume on its replica at the next block. Sessions spread over replicas by connections per `weight`.
4. **Declare each archive** with `"role": "frozen"`, its `range` and a `max_sessions`. The declared range lets the listener answer status and plan routes while the archive is down or still starting.
5. **Overlap the live tier.** The live nodes' first block should be at or below the highest archive end, so no block falls between the archives and the tip. `fleet_router_fleet_live_overlap_blocks` is that overlap; a negative value is a hole. An overlap of `0`, where the live node's first block follows the last archived one, is enough (see [Cut-over from a snapshot](#cut-over-from-a-snapshot)).
6. **Pin `chain_id`.** A stitched listener requires it.
7. **Check the coverage.** `fleet-router config test --strict <path>` probes every server and prints, per log, the coverage the listener adds up to and its holes, and each upstream's ranges with where they came from (`reported`, `declared` or `unknown`). It exits 1 on a hole in the trace or chain-state coverage, a frozen server that did not report its ranges, or, with a [chain API](chain-api.md), archived blocks that no frozen server with an `api_endpoint` covers.

**Sizing.** A session holds at most one link to an archive while it streams, plus, during the last `predial_blocks` blocks of a leg, a standby link to the next leg's upstream. Both count toward `max_sessions`, and so do dials in progress. Set `max_sessions` at or below the archive's own session limit: a full archive is skipped, and a session waits in the router for it (see [Holds, waits and gaps](#holds-waits-and-gaps)). An archive that answers the WebSocket upgrade with HTTP 503 is busy: that session avoids it for 2 s, and it is never marked offline. A finished link to an archive is kept for 30 s, so a client that asks for the next range on the same connection reuses the socket.

**What an archive upstream must do.** The router relies on an archive upstream to:

- send the SHiP ABI as its first message, and answer `get_status_request_v0` and `_v1` with the matching result: head and LIB both its last block, each log's range as `[begin, end)` (`0, 0` for an absent log), and the right `chain_id`;
- treat a new `get_blocks` as replacing the previous one, add acks to the credit, send while it has credit and the cursor is below the request's end and at or below its last block, and answer a v0 request with v0 results and a v1 request with v1 results;
- for each block, send `this_block`, `prev_block`, the traces, deltas and finality data asked for where it has them, and `block` where its block log has it;
- at the end of a range, stay open and silent: no Close frame, no head-only keep-alives, no skipped or repeated blocks without a request;
- refuse the WebSocket upgrade with HTTP 503 when it is at its session limit, instead of accepting and dropping;
- drop the TCP connection, with no Close frame, on a data or integrity error.

Toward an archive, the router requests only blocks inside its range (or below the fleet's first block, see below), up to the fleet head + 1 (LIB + 1 with `irreversible_only`), sends no `have_positions`, closes a finished link with Close 1000, and never sends status on a session link: its monitor connection carries status.

### Cut-over from a snapshot

A common way to split a node's history is to archive its logs, then start a live node from a snapshot of the same chain. With the snapshot at block `S`:

- **The last archive ends at `S + 1`.** Serve and declare a `range` for the last frozen range server that ends at `S + 1` (exclusive), so its last block is `S`. The logs copied from the old node may hold blocks after `S` that the live node does not follow: blocks of a fork it abandoned. They must not be served, and the router routes by range: an archive that reaches past `S` would have its blocks there served instead of the live node's.
- **The live node's first blocks.** A live node started from a snapshot at `S` holds chain-state data from `S`, and traces, finality data and blocks from `S + 1`. With the last archive ending at `S + 1`, the live tier overlaps the archives by `0` blocks of trace and `1` of chain state: no hole, and the normal shape, not a reason to alert (see [Alerts](observability.md#alerts)).

### Seams and handoffs

- **Handoff.** When the client's cursor reaches the end of a leg, the router sends the next leg to the next upstream with the client's outstanding credit and the same request version and flags. When the next leg is on the same upstream (the fleet head moved on while a frozen leg was clamped to it, say), the stream goes on over the same link with a new request; that is no handoff. A frozen link at the end of its leg is also reused for a following client request to the same archive. A handoff is never a failure: no strike, no backoff, no exclusion. `fleet_router_handoffs_total` counts them, and `fleet_router_handoff_latency_seconds` times the gap from the last frame of the old link to the first frame of the new one.
- **Pre-dial.** When [`predial_blocks`](configuration.md#topology-and-stitching) or fewer blocks remain in a leg, the next leg's upstream is connected ahead of time (WebSocket and ABI). A standby link unused for 60 s, or no longer part of the plan, is dropped. `fleet_router_predials_total{result}` counts them.
- **Continuity.** The first result after a seam must continue the last block the client got: its `prev_block` id must be that block's id. A frozen upstream that breaks this is a `discontinuity` contract violation (struck and failed over); a live one is forwarded and logged as an error. The first result of any leg on an archive that follows the last irreversible block the client got is checked the same way, including a new client request that goes on where the last one ended, so an archive of another chain is not served across a seam.
- **Missing `prev_block`.** An archive leaves `prev_block` out for the first block of its range when no log it has holds the block before. When a frozen result for block `n` has no `prev_block` and the session has just delivered block `n - 1`, which was irreversible when it was delivered, the router fills `prev_block` in from that block. `fleet_router_upstream_prev_blocks_filled_total` counts it.
- **Below the fleet's first block.** A request that starts below the first block any upstream holds goes to the upstream that holds that first block. It answers results without ids until its own range begins, as Spring does.

### Holds, waits and gaps

When no upstream can serve the cursor now, a stitched session does not close at once:

- **Hold in archived history.** When the block is in an archive's range but every upstream that has it is offline, quarantined, busy or full, the session holds. It retries after 100 ms, doubling to 3.2 s (±20 %); a retry may dial an offline upstream that has the block. When [`gap_wait_ms`](configuration.md#topology-and-stitching) runs out it tries once more, then closes the client with 1011: `block <K> unavailable on all upstreams` (offline, quarantined) or `upstream capacity exhausted for block <K>` (busy, full). `fleet_router_holds_total{why}` and `fleet_router_hold_closes_total{why}` count them.
- **Hold at the tip.** When only live nodes have the block, the hold has no limit: the session waits until a live node can serve it.
- **Wait above the gate.** When the cursor is above the fleet head (the fleet LIB with `irreversible_only`) for an archive, or past the last archived block while no live node has reported its ranges (an archive-only listener, say), the session waits without a limit. Holds and waits plan again on every change to the fleet and at least every 5 s.
- **Gap.** A block that no upstream covers, with covered blocks below and above it (a hole between declared or reported ranges), closes the client at once with 1011 `block <K> unavailable on all upstreams`. While an enabled frozen server has not reported or declared its range yet, such a block, or one below the fleet's first block, holds as offline instead, since that server may have it.

### Failover and strikes

This table applies to pool and stitched listeners alike. A **strike** counts against the upstream at the session's next block: it adds to `fleet_router_upstream_block_failures_total`, to the [quarantine](configuration.md#failure-handling) strikes, and to the session's budget of five such ends at one block (see [Corrupted SHiP logs](#corrupted-ship-logs)). Only unclean drops, frozen stalls and frozen contract violations strike.

| How a link ended | Strike | Pause before the next link | Afterwards |
| --- | --- | --- | --- |
| Unclean drop: the stream ended with no Close frame | yes | 100 ms, doubling to 3.2 s at the same block | prefers another upstream |
| Close frame | no | 100 ms | |
| `idle_timeout_ms` of silence while owed | no | 100 ms | |
| `upstream_stall_timeout_ms` of silence while owed, live upstream | no | 100 ms | prefers another upstream |
| The same, frozen upstream | yes | as for an unclean drop | prefers another upstream |
| No byte within `upstream_liveness_timeout_ms` (1 s after the monitor lost it) | no | 100 ms | prefers another upstream; a live one is marked offline |
| The monitor marked it offline and it sent nothing for 5 s while owed | no | 100 ms | prefers another upstream |
| Frozen contract violation | yes | as for an unclean drop | prefers another upstream |
| A live resume jumped past the cursor while another upstream covers it | no | none | avoids it for 30 s |
| The request could not be written to a new link | no | 100 ms | avoids it for 2 s |
| Moved by an operator | no | 100 ms | avoids it until the next link is up, and returns there if no other upstream can take the session |
| Moved off an archive without a block log | no | none | see [fetch_block and block logs](#fetch_block-and-block-logs) |
| A message above `max_message_bytes` | no | | the client is closed with 1011 `block <K> exceeds max_message_bytes` |

Each pause gets ±20 % jitter. In a pool listener, an unclean drop within 5 s after the router forwarded a request variant it does not know closes the client with 1003 `unsupported request` instead, with no strike: the client's frame, not the upstream, is the likely cause.

**Contract violations** are checked on frozen results only: a result without ids where the archive has ids or a head-only keep-alive (`idless`), a block above (`gap`) or below (`rewind`) the expected one, a requested field missing where the archive has that log (`missing_field`), a result while the client had no credit (`credit`), the wrong result version or a status result (`variant`), and a break at a seam (`discontinuity`). `fleet_router_upstream_contract_violations_total{kind}` counts them.

A failed dial makes the session avoid that upstream for 2 s, and marks a live upstream offline unless it answered 503. A session never marks a frozen upstream offline. A stitched session that cannot dial any upstream after three tries holds (see above); a pool session closes the client with 1011.

`fleet_router_failovers_total{reason}` counts failovers by reason; see [Observability](observability.md#sessions-and-failovers).

### fetch_block and block logs

An archive may run without a block log. It then sends no `block` in its results, although it has the traces and deltas. The router handles that without counting it against the archive:

- The first result without `block` for a block the archive covers, on a request with `fetch_block`, flags the archive as having no block log, with one warning (`frozen upstream sent no block for a block it covers`). `fleet_router_upstream_no_block_log` is 1 while the flag is set. Missing traces, deltas or finality data are still `missing_field` violations.
- The router learns this only from a result. So the first `fetch_block` session to reach such an archive (and the first again after the flag clears) pays one failover, `no_block_log`, to an upstream that holds blocks when one can take it; later sessions go to such an upstream from the start.
- For requests with `fetch_block`, a flagged archive ranks below every upstream that holds blocks, right after online, fresh and below `max_sessions`. A leg on a flagged archive ends where an upstream that holds blocks, is online and is below its `max_sessions` can take over: its range begins, or its quarantine ends. An offline or full holder does not end the leg; the session moves to it once it can take the stream (next item).
- When an upstream that holds blocks could serve the cursor now, the session moves to it at once, with no strike and no pause (failover reason `no_block_log`, logged at info). If that move ends on an archive without a block log again (the holder refused the session, say), the next move waits 1 s, doubling to 60 s, until a result arrives with every field the request asked for.
- Otherwise the result is forwarded without `block`, as a node with a pruned block log would send it, and counted in `fleet_router_upstream_results_without_block_total`. The session asks again on every fleet change, where the archive's leg ends, and at least every second.
- The flag clears when the monitor sees the archive come back after being offline, or report other ranges, head or LIB, with one info line.

The flag covers the whole archive and is kept per listener: an archive with a partial block log is flagged after its first missing block, until it reconnects or reports a new status. A live node that is stale or lags is not used while a fresh archive without a block log covers the cursor; the session moves once the node is fresh again.

### Readiness and coverage

`/ready` means the listener can serve the chain tip: a stitched listener needs an online live upstream (any online upstream when it has no live server). With live servers configured, an archive outage never makes it fail, so one dead archive cannot pull the router out of a load balancer. `/ready?scope=history` also requires the archived history to be complete and online. `GET /coverage` returns, per listener, the fleet tip, the canonical ABI and each log's coverage with the upstreams that serve each segment. See [Observability](observability.md#readiness-and-coverage) for both, and for [alerts](observability.md#alerts) on the fleet gauges.

## Security

> **Do not expose `fleet-router` directly to the public internet.**

- The client listener is **plaintext `ws://`** and **unauthenticated**. Anyone who can reach the port can stream data.
- Upstream connections default to plaintext `ws://`. Set `endpoint` to `wss://host:port` to dial that node with TLS; the certificate is verified with rustls against the OS trust store (see [Upstream TLS](configuration.md#upstream-tls-wss)).
- `rs_abieos` (pure Rust) parses untrusted upstream data; treat your upstreams as part of the trust boundary.
- The optional [chain API](chain-api.md) port is **plaintext HTTP** and **unauthenticated**, like a nodeos HTTP port. It serves read-only calls by default. With `"push"` in `chain_api.allow`, anyone who can reach it can relay transactions through your live nodes, and `"exec"` lets them run read-only transactions on them. The router warns at startup when push is allowed or the port listens on every interface.
- Upstream chain APIs (`api_endpoint`) are dialed over plain `http://`, with no authentication. Keep them on a private network.

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

`SIGINT` and `SIGTERM` (sent by `systemctl stop`) trigger graceful shutdown: the router stops accepting new connections, drains in-flight ones for up to `shutdown_grace_ms`, and sends WebSocket close frames (code 1001) to clients. A chain API port stops accepting too; a request in progress finishes and its connection closes, within the same grace period. Keep systemd's `TimeoutStopSec` comfortably larger than `shutdown_grace_ms`.

## Corrupted SHiP logs

A damaged `trace_history.log` or `chain_state_history.log` makes nodeos drop the SHiP socket with no WebSocket close frame when a client reaches that block (`zlib error: iostream error`). Indexers then reconnect and request the same block forever. fleet-router treats that drop as a mid-stream failure:

- The session tries another suitable upstream first. Retries after an unclean drop at the same next block wait 100 ms, then double, up to 3.2 s, and stop after five such drops. The client is closed with WebSocket code 1011 and reason `block <K> unavailable on all upstreams`. One error line names the upstream and the block. An idle timeout or a clean close still fails over after 100 ms and does not count, so a client waiting at the chain tip is not closed.
- Each unclean drop increments `fleet_router_upstream_block_failures_total` for that upstream. The node stays online, so other block ranges are unaffected. A frozen range server's stall and its archive contract violations count the same way (see [Failover and strikes](#failover-and-strikes)).
- Optional [`block_failure_quarantine`](configuration.md#failure-handling) remembers the block. After `strikes` drops at the same K inside `window_secs`, from any session, that upstream is skipped for requests starting in `K..K+range` for `ttl_secs`. Later sessions are routed to a healthy node and do not pay a failover on that range. When the TTL ends the upstream is eligible again. Repair or resync the log; the gauge `fleet_router_upstream_quarantine_block` shows which block to look at.

This bound is always on. Quarantine is off until `block_failure_quarantine` is set, at the top level or on a listener.

## Tuning

- **Intervals** (`upstream_reconnect_ms`, `upstream_monitoring_ms`, `upstream_status_ms`): lower values detect failures and staleness faster at the cost of more polling traffic to upstreams; raise them to reduce chatter.
- **Connections** (`max_connections`): size it for your expected concurrency. Excess connections are rejected, not queued.
- **Timeouts** (`handshake_timeout_ms`, `idle_timeout_ms`): set `idle_timeout_ms` to reap dead clients.
- **Upstream silence** (`upstream_liveness_timeout_ms`, `upstream_stall_timeout_ms`): the liveness check catches a socket that went dead (default 4 s), the stall check a node that stays connected but stops sending blocks it owes (default 60 s). Both count bytes as they arrive, so a large block on a slow link does not trip them.
- **Lagging nodes** (`live_lag_blocks`): a live node more than this many blocks behind the fleet head (default 1200) is not chosen for blocks above its own head while a closer node is up. Lower it to move tip readers off a lagging node sooner; `0` turns it off.
- **Message sizes** (`max_message_bytes`, `max_client_message_bytes`): lower `max_message_bytes` only if you are sure upstream blocks fit. `max_client_message_bytes` caps client frames (default 64 KiB). The per-session failover buffer is capped at 64 KiB, and the session is closed if a client exceeds it.
- **Stitched listeners** (`max_sessions`, `predial_blocks`, `gap_wait_ms`): see [Deploying frozen range servers](#deploying-frozen-range-servers). Raise `gap_wait_ms` when archives restart slowly and clients should wait rather than reconnect.
- **Chain API** (`chain_api.max_in_flight`, `max_in_flight_per_upstream`, `upstream_timeout_ms`, `max_attempts`): see [Chain API](chain-api.md#limits-and-timeouts).

See the [configuration reference](configuration.md) for every field and its default.
