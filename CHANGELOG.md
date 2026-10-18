# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Upgrade notes

- Pool listeners now answer `get_status_request_v0` and `_v1` themselves,
  with the fleet's head, LIB and block ranges, instead of relaying one
  upstream's reply. Until the fleet head and LIB are known, the request is
  still relayed to the session's upstream.
- Two new checks are on by default: `upstream_stall_timeout_ms` (60 s) fails
  over an upstream that stops sending blocks it owes, and `live_lag_blocks`
  (1200) ranks a live node that far behind the fleet head like a stale one
  for blocks above its own head. Set either to `0` to turn it off. The stall
  check fails a live upstream over only when another online upstream can
  serve the next block; a live upstream that none could replace is kept, with
  one warning, until it has been silent for three times the timeout, and is
  then redialed. `idle_timeout_ms` and the 5 s rule for an upstream the
  monitor marked offline still fail over such an upstream sooner.
- The upstream liveness check now counts bytes as they arrive, so the 0.6.0
  advice to raise `upstream_liveness_timeout_ms` for large blocks whose bytes
  are already flowing on a slow link no longer applies. A block whose first
  byte takes longer than `upstream_stall_timeout_ms` still triggers a stall
  failover: on a frozen upstream always (with a strike), on a live one when
  another online upstream can serve the next block, else after three times
  the timeout.
- `max_connections`, at the top level and per listener, must be at most
  1000000. A larger value fails validation.
- Existing config files stay valid, and a listener without frozen servers
  stays a pool listener.

### Added

- Range-split fleets. A server can be marked `"role": "frozen"`: a frozen
  range server, an archive upstream that serves a fixed block range from
  archived state-history and block logs with no nodeos behind it. A listener
  with an enabled frozen server (or `"topology": "stitched"`) presents its
  upstreams as one SHiP node whose history is the union of their ranges. The
  router sends the listener's ABI without dialing, answers `get_status`
  itself, plans each `get_blocks` by start block and requested data, hands
  the stream from one upstream to the next at range boundaries without
  counting it as a failure, connects to the next upstream ahead of the
  boundary, and replaces head and LIB in archive results so they never move
  backwards. Archived blocks whose upstreams are all down are held for up to
  `gap_wait_ms`; a hole in the configured coverage closes the client with
  1011 `block K unavailable on all upstreams`. See "Range-split fleets" in
  `docs/operations.md`.
- Per-server `role`, `range` (the archive's declared coverage, used until it
  reports and while it is down) and `max_sessions`; per-listener `topology`
  and `stitching` (`history_preference`, `predial_blocks`, `gap_wait_ms`,
  `patch_head_lib`, `abi_file`, `abi_mismatch`); global
  `upstream_stall_timeout_ms` and `live_lag_blocks`.
- Results from a frozen range server are checked against the archive
  contract (block sequence, ids, requested fields, credit, continuity at
  seams). A violation fails the session over to another upstream and counts
  as a block failure.
- A frozen range server without a block log is supported. Requests with
  `fetch_block` prefer an upstream that holds blocks and move to it with no
  strike; otherwise results are forwarded without `block`. A frozen result
  without `prev_block`, right after an irreversible block the session
  delivered (at or below the LIB already sent to the client), gets it filled
  in from that block.
- `example.stitched.config.json`, a stitched listener with two archived
  ranges, two live nodes and a chain API.
- `config test --strict`: exits 1 when the config is invalid, a stitched
  listener's coverage has a hole or a frozen server did not report its
  ranges, or a chain API listener has archived blocks that no frozen server
  with an `api_endpoint` covers (a replica without one passes while its twin
  has one). `config test` prints each stitched listener's coverage and each
  chain API listener's servers.
- `GET /coverage` and `/coverage/<listener>` on the metrics port (fleet tip,
  canonical ABI, per-log coverage and the upstreams of each segment), and
  `/ready?scope=history`, which also requires complete, online archive
  coverage. Plain `/ready` still means the tip can be served.
- `/metrics`: per-upstream `fleet_router_upstream_trace_end_block`,
  `_head_block`, `_lib_block`, `_info{role}`, `_ranges_known`,
  `_range_mismatch`, `_abi_info`, `_abi_compatible`, `_abi_match`,
  `_links_total`, `_dial_failures_total`, `_stalls_total`,
  `_sequence_jumps_total`, `_contract_violations_total`, `_no_block_log`,
  `_results_without_block_total` and `_prev_blocks_filled_total`; per
  listener the fleet head, LIB and head age, coverage, gap, offline and
  live-overlap gauges, sessions by state, and counters for status replies,
  handoffs, failovers by reason, reroutes, dropped duplicates, fork
  re-deliveries, keep-alives, credit overruns, patched results, holds,
  pre-dials, client protocol errors and too-large closes, with
  `fleet_router_handoff_latency_seconds`.
- An opt-in HTTP chain API listener per listener (`chain_api`), with a
  per-server `api_endpoint`. Block endpoints are routed by block number:
  irreversible blocks go to the frozen range server that holds them, falling
  back to live nodes (`block_source` picks who goes first), and reversible
  blocks go to live nodes only. `get_info` and read-only state calls are
  proxied unchanged to live nodes. Push and exec are off by default; a push
  is never resent. The router answers with nodeos-shaped errors, nodeos's
  404 for unknown endpoints, and its own `get_supported_apis`, and never
  invents a parameter error or an Unknown Block. See `docs/chain-api.md`.
- `chain_api.max_response_bytes` (off by default) bounds the upstream answers
  a chain API relays: a longer `Content-Length` gets `502`, and an answer
  without one is cut off once it passes the limit.
- New metrics `fleet_router_api_*` and `fleet_router_upstream_api_*`; new
  readiness scope `/ready?scope=api`; a `chain_api` block per listener in
  `/coverage`.
- `docs/chain-api.md`, and sections on range-split fleets (including the
  cut-over to a live node started from a snapshot), failover and strikes,
  readiness scopes, `/coverage`, every new metric and suggested alerts in
  `docs/operations.md` and `docs/observability.md`.

### Changed

- Pool routing checks each `get_blocks` against the session's upstream
  before it is forwarded, and moves it to an upstream that serves it better
  when needed. An upstream must have the start block in every log the
  request asks for (deltas-only requests use the chain-state range), a live
  node counts as covering the next block at its head, lagging live nodes
  rank like stale ones, `max_sessions` is honoured, and ties are broken by a
  per-session hash.
- An upstream WebSocket close frame is no longer forwarded to the client;
  the session fails over and the client connection stays open.
- `idle_timeout_ms` fails over a silent upstream only while it owes blocks,
  so a client waiting at the chain tip no longer causes reconnects. On the
  client side it still closes a connection that sends nothing.
- Malformed client requests close the client with WebSocket code 1007
  instead of being forwarded. A request variant the router does not know is
  still forwarded in a pool listener; a stitched listener closes the client
  with 1003.
- An upstream that answers the WebSocket upgrade with HTTP 503 is treated as
  busy: the session avoids it for 2 s and it is not marked offline.
- The liveness check and the 1 s probe after the monitor loses an upstream
  count bytes as they arrive, so a large message on its way is not taken for
  silence. A silent upstream is marked offline only when it is a live node.
- `fleet_router_active_connections` counts the connections sessions hold to
  an upstream, which in a stitched listener includes pre-dialed standby
  links; a stitched client that only asks for status holds none.
- A control API drain or move counts only an upstream below its
  `max_sessions` and not quarantined at the session's next block. A moved
  session returns to the upstream it left when the target cannot take it,
  and a dial that lands on an upstream disabled meanwhile goes to another
  upstream when one can take the session. Disable, drain, priority and weight
  also apply to the chain API.
- In a stitched listener, a leg's dial in progress is replaced as soon as a
  view shows its upstream can no longer serve the cursor (disabled,
  quarantined, or its range shrunk below it), and its request end follows the
  view. It is kept when another upstream only ranks higher by then, or the
  monitor misses a status of it.
- A live seam whose first result does not continue the last delivered block
  fails over when another upstream covers the block, with a warning that
  gives both ids and `fleet_router_failovers_total{reason="discontinuity"}`.
  When none does, the result is forwarded and logged as an error as before,
  and the upstreams left at that seam are no longer avoided.
- Dashboard and `/api/v1/status`: a listener's `ready` is what
  `/ready/<listener>` answers; a listener's `sessions` (and the total) count
  client sessions, while an upstream's `sessions` count the connections held
  to it. The session list shows no upstream while a session has no link.

### Fixed

- A client ack that takes the session's credit past `u32::MAX` leaves it at
  `u32::MAX`, and the upstream is acked only what the credit rose by, so its
  own counter can no longer wrap and part from the session's. Such an ack
  made the session wait for blocks the upstream would never send (a false
  stall, and a strike on an archive) or forward results counted as overruns.
  One Spring node wraps the sum, which leaves a window that depends on the
  results still in flight.
- Failover now resumes with the client's outstanding credit instead of its
  original `max_messages_in_flight`, and acks sent during the swap are no
  longer replayed on top of it.
- Replayed blocks are de-duplicated by block number and id, dropped
  duplicates are refunded upstream with an ack, and fork re-deliveries are
  forwarded.
- Head keep-alive results from EOSIO 2.0 and Leap 3 nodes no longer move the
  resume block.
- A new `get_blocks` mid-stream no longer inherits the previous request's
  resume point, a new request on a link that still has blocks in flight
  opens a fresh upstream link, and status requests are no longer lost during
  a failover.
- Head and LIB in results never move backwards within a session.
- An upstream that stays connected and answers pings but stops sending the
  blocks it owes is failed over after `upstream_stall_timeout_ms` (a live
  upstream that no other online upstream could replace, after three times
  that).
- A message larger than `max_message_bytes` closes the client with 1011 and
  the reason `block K exceeds max_message_bytes` instead of striking the
  upstream.
- Session upstream dials now time out: the TCP connect with the WebSocket
  upgrade, and then the wait for the upstream's ABI, are each bounded by
  `handshake_timeout_ms` (10 s when `0`). Before, a session waited for the
  ABI with no bound when `handshake_timeout_ms` was `0`.

## [0.6.0] - 2026-10-17

### Upgrade notes

- The new upstream liveness check is on by default. A session pings an
  upstream that has sent nothing for 2 s and fails over if nothing comes
  back within 4 s. Clients keep their connection either way, but if single
  blocks from your upstreams take seconds to transfer (very large blocks over
  a slow link), raise `upstream_liveness_timeout_ms`.
- The dashboard and control API are off unless `dashboard.enabled` is set.
  An existing `metrics_port` serves the same health, readiness and metrics
  as before.
- `/ready` no longer counts an upstream that an operator disabled through
  the control API.
- The health/metrics endpoint now runs on hyper. Paths and bodies are
  unchanged; a non-GET request to an unknown path returns `404` instead of
  `405`.

### Fixed

- A session on an upstream that went silent without closing the connection
  (cable pulled, VM frozen, firewall dropping packets) waited until the
  operating system gave up on the TCP connection, often around 15 minutes,
  and the client saw a stalled stream. Sessions now ping a quiet upstream and
  fail over when nothing comes back within `upstream_liveness_timeout_ms`
  (default 4 s), or within 1 s once the monitor has marked that upstream
  offline. The client keeps its connection and resumes at the next block.
  Reported by @Rossco99.
- The upstream monitor stopped for good when an upstream closed its WebSocket
  cleanly, as nodeos does on a normal shutdown or restart. That upstream then
  stayed offline until fleet-router restarted. The monitor now reconnects.

### Added

- `upstream_liveness_timeout_ms` (default `4000`, `0` disables). See Fixed.
- Opt-in live dashboard at `/ui/`, a plain `/status` page, and a JSON API
  under `/api/v1` (status, sessions, 15-minute history, events, and a
  server-sent events stream), served on `metrics_port` when
  `dashboard.enabled` is `true`. Off by default, so upgrading does not expose
  anything new on an existing metrics port. Requested by @Rossco99.
- Token-protected control API (`dashboard.control_token_file`,
  `dashboard.control_token`, or `FLEET_ROUTER_CONTROL_TOKEN`): disable,
  enable, or drain an upstream, change its priority and weight at runtime,
  clear its quarantines, and move or close a single session. A drain moves
  live sessions to other upstreams with the normal failover, so clients stay
  connected. A session moves only when another online upstream's range
  starts at or before its next block, so draining the only archive node
  leaves its history readers in place. Runtime changes are not written back
  to the config.
- `dashboard.allowed_hosts`: dashboard routes answer only requests addressed
  by IP, `localhost`, or a listed name, which blocks DNS rebinding. At most 32
  live streams are open at once.
- New gauge `fleet_router_upstream_operator_disabled`.

- Optional per-server `priority` (default `0`, lower is preferred) and
  `weight` (default `1`). Among upstreams that are equally able to serve a
  request (same block coverage and freshness), the router uses the lowest
  priority tier, then the fewest connections per unit of weight. A high
  priority makes an upstream a last resort. Failover still moves off a failed
  preferred upstream to a backup that covers the block. New gauges
  `fleet_router_upstream_priority` and `fleet_router_upstream_weight`.
  Requested by @Rossco99.
- Static landing page under `site/` for GitHub Pages.
- `deploy/fleet-router.service`, a systemd unit for one process and one config
  file, next to the per-network `fleet-router@.service` template.
- "Run as a service" section in `docs/operations.md`: step-by-step systemd
  install, Docker restart policy, a launchd plist for macOS, and running the
  Windows build under a service wrapper.

### Changed

- The health/metrics endpoint now runs on hyper (via axum) instead of a
  hand-written HTTP loop. Paths and response bodies are unchanged. Request
  headers must arrive within 5 s, at most 256 connections are served at
  once, and each connection serves one request (as before). A request to an unknown path with a method other than `GET` now
  returns `404` instead of `405`.
- `fleet_router_upstream_priority` and `fleet_router_upstream_weight` report
  the values in effect, which can differ from the config after a control API
  change.
- Shorten `README.md` to an overview, install, and quick start. The full
  configuration reference, production notes, and metrics moved to
  `docs/configuration.md`, `docs/operations.md`, and `docs/observability.md`.
- Both systemd units set `LimitNOFILE=65536` (the systemd default of 1024
  open files capped the router at about 500 clients), `RestartSec=2`, and
  basic sandboxing (`NoNewPrivileges`, `ProtectSystem=strict`, `ProtectHome`,
  `PrivateTmp`).

## [0.5.0] - 2026-10-15

### Fixed

- A mid-stream failover that keeps dying at the same next block is now bounded.
  The bound counts only unclean socket drops (no WebSocket close frame). The
  session prefers a different suitable upstream, waits 100 ms and then doubles
  that pause up to 3.2 s, and stops after five such drops. The client receives
  WebSocket close code 1011 with reason
  `block K unavailable on all upstreams` instead of a silent stall, and one
  error names the upstream and the block. An idle timeout or a clean close
  still fails over and does not count. Reported by @eosusa (EOSUSA).

### Added

- Opt-in `block_failure_quarantine` (`strikes`, `window_secs`, `ttl_secs`,
  `range`), at the top level in single-listener mode or on each listener.
  After the configured number of unclean mid-`get_blocks` drops at the same
  block, within the window and from any session, that upstream is skipped for
  `K..K+range` for the TTL. It is not marked offline. Defaults are 2 strikes,
  a 300 s window, a 600 s TTL, and a range of 1 block. Expired exclusions are
  removed when selection, a block check, or `/metrics` looks at them.
- `/metrics` exposes `fleet_router_upstream_block_failures_total`,
  `fleet_router_upstream_quarantined`, and
  `fleet_router_upstream_quarantine_block` (the quarantined block is the gauge
  value, not a label). Requested by @eosusa (EOSUSA).

## [0.4.0] - 2026-10-14

### Security

- Bump locked `rustls` from 0.23.45 to 0.23.46. This clears `RUSTSEC-2026-0346`,
  `RUSTSEC-2026-0347`, `RUSTSEC-2026-0348`, and `RUSTSEC-2026-0349`.

### Fixed

- Pinning `chain_id` no longer marks an upstream offline when its SHiP status
  omits `chain_id`. That field is an optional binary extension of
  `get_status_result_v0`; older nodeos builds (FIO mainnet/testnet and other
  EOSIO 2.0-based nodes) never send it, so the previous check kept every such
  node permanently offline. A reported `chain_id` that does not match the
  pinned value or the rest of the pool is still rejected. When a status omits
  `chain_id`, the router logs a warning once per upstream that `chain_id`
  cannot be verified, and logs it again only after a later status reports one.
  Reported by @eosusa (EOSUSA).

### Added

- `/metrics` labels every per-upstream series with `server="<name>"` from the
  configured server name, in both single-port and multi-listener mode.
  `endpoint` and `listener` labels are unchanged. Label values escape
  backslash, double quote, and newline for the Prometheus text format.
  Requested by @eosusa (EOSUSA).
- Optional `require_chain_id` (`false` by default), at the top level in
  single-listener mode or on each listener. Set it to restore the previous
  reject-when-missing behaviour. It is rejected by config validation unless
  `chain_id` is set on that same scope.
- `/metrics` exposes each upstream's SHiP range start as
  `fleet_router_upstream_trace_begin_block` and
  `fleet_router_upstream_chain_state_begin_block`, beside the existing
  chain-state end and finality block gauges. `finality_data_begin_block` was
  already exported. These gauges are always present. `0` means a status result
  has not arrived yet; `finality_data_begin_block` also stays `0` on
  pre-Savanna nodes that only report `get_status_result_v0`.
  Requested by @eosusa (EOSUSA).

## [0.3.1] - 2026-10-14

### Changed

- Reformat `README.md` configuration and route tables into bulleted definition
  lists so the documentation renders cleanly within crates.io's 575px layout
  without horizontal clipping or narrow multi-column wrapping.

## [0.3.0] - 2026-10-14

### Security

- Cap client WebSocket messages separately from upstream messages with
  `max_client_message_bytes` (default 64 KiB). The client limit applies to both
  the message size and the frame size, so an unauthenticated client can no
  longer force the router to accept upstream-sized frames.
- Replace the per-session 1024-frame failover buffer with a 64 KiB byte cap.
  Each buffered frame counts as at least one byte. A session that exceeds the
  cap is closed with WebSocket close code 1008 (policy violation).
- Locked dependency updates clear `RUSTSEC-2026-0258` (`h2` 0.4.20),
  `RUSTSEC-2026-0285` (`rustls` 0.23.45), and `RUSTSEC-2026-0190`
  (`anyhow` 1.0.104). Compatible updates also pick up tokio 1.53.2, serde
  1.0.229, serde_json 1.0.154, clap 4.6.7, chrono 0.4.45, and futures 0.3.34.

### Added

- Multi-ingress (`listeners`) configuration for routing multiple local ports to
  isolated pools of redundant SHiP nodes (e.g. one ingress port per blockchain
  network) within a single `fleet-router` process. Existing single-port configs
  (`listen_port` + `servers`) remain unchanged. Supports optional `chain_id`
  pinning (top-level or per-listener) and automatic pool-wide `chain_id`
  consistency checks so a misconfigured upstream on a different chain is kept
  offline. In multi-listener mode, `/metrics` labels each series with
  `listener="<name>"`, `/ready` requires at least one online upstream in every
  listener, and `/ready/<name>` probes a single listener. Also added
  `deploy/docker-compose.multi-network.yml` and `deploy/fleet-router@.service`
  for multi-instance deployments.
- Upstream `endpoint` values may include a `ws://` or `wss://` scheme. A bare
  `host:port` still dials plaintext `ws://`, so existing configs are unchanged.
  `wss://` uses rustls with the `aws-lc-rs` provider (not OpenSSL) and verifies the
  upstream certificate against the OS trust store. `SSL_CERT_FILE` and
  `SSL_CERT_DIR` replace that store when set. Unknown schemes are rejected.
  Endpoint uniqueness follows the dial target, so `host:port` and
  `ws://host:port` are the same upstream. The client listener stays plaintext.
- Optional configuration field `max_client_message_bytes`. Configs that omit it
  remain valid and use the default.
- Parse `get_status_result_v1` and track each upstream's finality-data block
  range. The single in-flight status probe asks for v1 and falls back to
  `get_status_request_v0` when an upstream does not answer it.
- Route `get_blocks_request_v1` with `fetch_finality_data` to an upstream whose
  finality range covers the requested block. Other requests, and fleets that
  never report a covering range, keep trace-range selection.

### Changed

- Building from source now requires a C compiler and CMake. `wss://` links
  rustls with `aws-lc-rs` (the same crypto provider as `reqwest` 0.13).
  `rs_abieos` stays on its pure-Rust backend, and `libclang` is still not
  required.
- Upgrade `rs_abieos` from 0.5.0 to 0.6.0, still with default features off and
  `rust-backend` enabled.
- Validate each upstream ABI with `AbiHandle::from_json` and drop the handle
  as soon as the result is known. Client handshakes, failovers, and upstream
  monitors no longer share one `Arc<Mutex<Abieos>>` or register the ABI under
  account `0`. `AbiHandle` runs the same parse as `set_abi` and does not keep
  a context. A per-connection `Abieos` would retain that context and the
  parsed model for the whole session; fleet-router only needs the parse to
  succeed, so the handle is not stored. An interior NUL is still rejected
  before parsing. Client and upstream message caps are unchanged.
- Minimum supported Rust version is now 1.99.
- The container image is built from digest-pinned `rust:1.99-trixie` and
  `debian:trixie-slim`. Its `HEALTHCHECK` runs `fleet-router check`, which
  probes `127.0.0.1:17000` without a shell.
- GitHub Actions are pinned by commit SHA. The release workflow uses
  `softprops/action-gh-release` v3 (Node 24). Dependabot tracks Actions, Cargo,
  and Docker updates.

### Fixed

- The zero-copy decoder no longer panics on a type mismatch or on an array
  field whose element count would overflow the computed span. `From<ZCDValues>`
  conversions are now `TryFrom`, `get_type` returns `Option`, and the array
  branch of `deserialize_with` uses checked arithmetic and bounds-checked
  slices. Valid frames decode as before.
- The hand-rolled SHiP status decoder rejects truncated and malformed upstream
  frames with an error instead of panicking on a short slice or a missing
  field. The monitoring loop logs the error and leaves that upstream's state
  unchanged. A rejected status frame does not count as the in-flight reply.
  The upstream ABI is applied without unwrapping the handshake buffer.
- Upstream monitors and `config test` bound WebSocket connect and the first read
  with a timeout (`handshake_timeout_ms`, or 10s when that is 0) and apply
  `max_message_bytes`. A read error, a connect failure, or a missing status
  reply marks that upstream offline. An existing session still retries that
  upstream when no other node is online, so a single-node drop can reconnect
  (or hit the pending-byte cap) instead of closing immediately.
- Status polling keeps a single `get_status_request` in flight and drops the
  session if the reply does not arrive within `upstream_status_ms`. A stalled
  nodeos no longer accumulates queued status requests (Spring 1.2.3 ends a
  session once more than 100 are outstanding).

## [0.2.0] - 2026-05-30

First public release on crates.io and GitHub.

### Added

- Range-aware backend selection: prefer upstreams whose trace range covers the
  requested block, falling back to least-connections.
- Automatic failover and reconnection with inline de-duplication of replayed
  blocks, so persistent client connections survive upstream outages.
- Structured logging via `tracing`, controllable with the `RUST_LOG` environment
  variable (defaults to `info`).
- Graceful shutdown on `SIGINT` and `SIGTERM`, with a bounded drain period
  (`shutdown_grace_ms`) and WebSocket close frames sent to connected clients.
- Configuration validation with clear, actionable errors (`config test` and at
  startup), including detection of zero intervals and duplicate endpoints.
- Connection backpressure (`max_connections`), client handshake timeout
  (`handshake_timeout_ms`), optional idle timeout (`idle_timeout_ms`), and a
  configurable maximum WebSocket message size (`max_message_bytes`).
- Optional HTTP health/metrics endpoint (`metrics_port`) exposing `/health`,
  `/ready`, and Prometheus `/metrics`.
- Staleness detection: upstreams that stop advancing their chain state are
  flagged and deprioritized for routing.
- Capped exponential backoff for upstream monitoring reconnects.
- `cargo-deny` supply-chain configuration and CI (advisories, bans, licenses,
  sources), an MSRV check, dependency caching, and a tag-triggered release
  workflow (crates.io publish, cross-platform binaries, GHCR image).
- Community health files: `CONTRIBUTING.md`, `SECURITY.md`,
  `CODE_OF_CONDUCT.md`, issue templates, and a pull-request template.

### Changed

- Build on the pure-Rust `rs_abieos` backend (`rust-backend`): fleet-router now
  compiles on **Linux, macOS, and Windows** (x86_64 and arm64) with no C/C++
  toolchain, `clang`, or `libclang`.
- The CLI `--version` now derives from `Cargo.toml` (`CARGO_PKG_VERSION`).
- The connection counter is now an atomic, decremented synchronously and
  underflow-safely when a connection ends.
- Block-number arithmetic uses saturating operations to avoid overflow on
  adversarial upstream data.
- During failover, only duplicate block frames are dropped; status results and
  head keep-alives are always forwarded so clients never stall.
- Client frames that cannot be forwarded during a failover window are buffered
  and replayed after reconnect instead of being silently dropped.
- The published crate now ships only the files needed to build and run the
  binary (`include` in `Cargo.toml`); `Cargo.lock` is committed for reproducible
  builds.
- Canonical default listen port reconciled to `17000` across the sample config,
  Docker assets, and documentation.
- The runtime Docker image runs as a non-root user and declares a `HEALTHCHECK`.

### Fixed

- `config init` no longer panics when the target path is not writable; it
  reports a clear error instead.
- Upstream handshake now skips leading control frames and retries against
  another upstream instead of dropping the client on an unexpected first frame.

[Unreleased]: https://github.com/eosrio/fleet-router/compare/v0.6.0...HEAD
[0.6.0]: https://github.com/eosrio/fleet-router/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/eosrio/fleet-router/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/eosrio/fleet-router/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/eosrio/fleet-router/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/eosrio/fleet-router/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/eosrio/fleet-router/releases/tag/v0.2.0
