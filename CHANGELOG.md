# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Minimum supported Rust version is now 1.99.
- The container image is built from digest-pinned `rust:1.99-trixie` and
  `debian:trixie-slim`. Its `HEALTHCHECK` runs `fleet-router check`, which
  probes `127.0.0.1:17000` without a shell.
- GitHub Actions are pinned by commit SHA. The release workflow uses
  `softprops/action-gh-release` v3 (Node 24). Dependabot tracks Actions, Cargo,
  and Docker updates.

### Security

- Locked dependency updates clear `RUSTSEC-2026-0258` (`h2` 0.4.20),
  `RUSTSEC-2026-0285` (`rustls` 0.23.45), and `RUSTSEC-2026-0190`
  (`anyhow` 1.0.104). Compatible updates also pick up tokio 1.53.2, serde
  1.0.229, serde_json 1.0.154, clap 4.6.7, chrono 0.4.45, and futures 0.3.34.

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
