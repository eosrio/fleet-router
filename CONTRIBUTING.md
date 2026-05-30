# Contributing to fleet-router

Thanks for your interest in improving **fleet-router**, EOS Rio's reverse proxy
and load balancer for the Antelope SHiP (State History Plugin) WebSocket
protocol. Contributions of all kinds are welcome: bug reports, documentation
fixes, and code.

This guide covers how to set up your environment, build and test the project,
and the checks your pull request must pass before it can be merged.

Please also read our [Code of Conduct](CODE_OF_CONDUCT.md) — it applies to all
project spaces and interactions.

## Prerequisites

fleet-router builds on **Linux x86_64 only**. The `rs_abieos` build script
panics with "Unsupported OS" on macOS and Windows. On those platforms, use the
published Docker image (`ghcr.io/eosrio/fleet-router`) instead of a native
build.

You need the following to build from source. See the
[Requirements section in the README](README.md#requirements-and-supported-platforms) for the full
rationale.

- Linux x86_64
- `git`
- A C/C++ toolchain plus `clang` and `libclang-dev` (the `rs_abieos` build
  script compiles vendored C++ and uses `bindgen`, which needs `libclang`)
- Rust **1.85+** (the Minimum Supported Rust Version — a transitive dependency
  uses the 2024 edition)

On Debian/Ubuntu, install the system packages with:

```bash
sudo apt-get install -y git clang libclang-dev build-essential
```

Install Rust via [rustup](https://rustup.rs/) if you do not already have a
toolchain:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

## Building

Clone the repository and build the workspace:

```bash
git clone https://github.com/eosrio/fleet-router.git
cd fleet-router
cargo build
```

The first build compiles the vendored C++ in `rs_abieos`, so expect it to take
longer than a typical Rust build. `Cargo.lock` is committed; CI and releases
build with `--locked`, so do not delete or regenerate it unless your change
intentionally updates dependencies.

## Testing

Run the workspace test suite:

```bash
cargo test --workspace
```

These tests use the in-repo **mock-ship** test double, so they need **no
external services** — no `nodeos`, no SHiP node, no network access.

A few integration tests are marked `#[ignore]` because they require a running
Docker stack (real `nodeos` containers and a load generator). They are not part
of the default run. To exercise them, bring up the compose stack under
`docker/` and run the ignored tests explicitly, for example:

```bash
docker compose -f docker/docker-compose.test.yml up --build -d
cargo test --workspace -- --ignored
```

You do **not** need these Docker tests to pass to contribute most changes; the
default `cargo test --workspace` run is sufficient for the great majority of
work.

## Mandatory pre-PR checks

CI enforces formatting, linting, and tests, and treats all Clippy warnings as
errors. Run the same checks locally before opening a pull request so your PR
passes on the first try:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```

Notes:

- `cargo fmt --all` reformats your changes. CI runs `cargo fmt --all -- --check`
  and fails if anything is unformatted.
- Clippy must be clean: `-D warnings` turns every warning into an error.
- CI runs these commands with `--locked` and also runs an MSRV check against
  Rust 1.85, a `cargo-deny` supply-chain scan (advisories, bans, licenses,
  sources), and a Docker image build. Keeping the three commands above green
  locally covers the parts you are most likely to break.

## Updating the CHANGELOG

This project keeps a [CHANGELOG.md](CHANGELOG.md) in the
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format.

Add an entry for any user-facing change under the `## [Unreleased]` section,
using the appropriate subheading (`Added`, `Changed`, `Fixed`, etc.). Keep
entries concise and written from the user's perspective. Purely internal
refactors with no observable effect do not need an entry.

## Branches and pull requests

- Branch off `main` for your work; do not push directly to `main`.
- Keep pull requests **small and focused** — one logical change per PR. Smaller
  PRs are easier to review and faster to merge.
- Write **clear, descriptive commit messages** that explain what changed and
  why.
- In the PR description, explain the motivation and summarize the change. Link
  any related issues.
- Confirm the pre-PR checklist below before requesting review.

### Pre-PR checklist

- [ ] `cargo fmt --all`
- [ ] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] Updated `CHANGELOG.md` under `[Unreleased]` (for user-facing changes)

## Reporting bugs and requesting features

Please use the GitHub issue tracker:

- [Open an issue](https://github.com/eosrio/fleet-router/issues/new/choose) and
  pick the appropriate template (bug report or feature request).

For bug reports, include your OS and architecture, the fleet-router version
(`fleet-router --version`), your configuration (with any secrets redacted), the
steps to reproduce, and the relevant log output. Setting `RUST_LOG=debug` often
makes a report far easier to diagnose.

## Security

Do **not** report security vulnerabilities through public issues. Follow the
process in [SECURITY.md](SECURITY.md) for responsible disclosure.

## License

By contributing, you agree that your contributions will be licensed under the
project's [MIT License](LICENSE).
