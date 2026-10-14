# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Pinning `chain_id` no longer marks an upstream offline when its SHiP status
  omits `chain_id`. That field is an optional binary extension of
  `get_status_result_v0`; older nodeos builds (FIO mainnet/testnet and other
  EOSIO 2.0-based nodes) never send it, so the previous check kept every such
  node permanently offline. A reported `chain_id` that does not match the
  pinned value or the rest of the pool is still rejected. When a status omits
  `chain_id`, the router logs a warning once per upstream that `chain_id`
  cannot be verified, and logs it again only after a later status reports one.

### Added

- Optional `require_chain_id` (`false` by default), at the top level in
  single-listener mode or on each listener. Set it to restore the previous
  reject-when-missing behaviour. It is rejected by config validation unless
  `chain_id` is set on that same scope.

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

[Unreleased]: https://github.com/eosrio/fleet-router/compare/v0.3.1...HEAD
[0.3.1]: https://github.com/eosrio/fleet-router/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/eosrio/fleet-router/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/eosrio/fleet-router/releases/tag/v0.2.0
