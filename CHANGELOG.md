# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

- Optional configuration field `max_client_message_bytes`. Configs that omit it
  remain valid and use the default.
- Parse `get_status_result_v1` and track each upstream's finality-data block
  range. The single in-flight status probe asks for v1 and falls back to
  `get_status_request_v0` when an upstream does not answer it.
- Route `get_blocks_request_v1` with `fetch_finality_data` to an upstream whose
  finality range covers the requested block. Other requests, and fleets that
  never report a covering range, keep trace-range selection.

### Changed

- Minimum supported Rust version is now 1.99.
- The container image is built from digest-pinned `rust:1.99-trixie` and
  `debian:trixie-slim`. Its `HEALTHCHECK` runs `fleet-router check`, which
  probes `127.0.0.1:17000` without a shell.
- GitHub Actions are pinned by commit SHA. The release workflow uses
  `softprops/action-gh-release` v3 (Node 24). Dependabot tracks Actions, Cargo,
  and Docker updates.

### Fixed

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

[Unreleased]: https://github.com/eosrio/fleet-router/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/eosrio/fleet-router/releases/tag/v0.2.0
