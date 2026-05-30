# Security Policy

This document describes how to report security vulnerabilities in
**fleet-router** and the security properties you should assume when deploying
it.

## Supported Versions

Security fixes are provided for the latest `0.2.x` release line. Older
pre-release versions are not maintained; please upgrade to the latest `0.2.x`
release before reporting an issue.

| Version | Supported          |
| ------- | ------------------ |
| 0.2.x   | :white_check_mark: |
| < 0.2   | :x:                |

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security vulnerabilities.**
Public issues disclose the problem before a fix is available and put other
users at risk.

Report vulnerabilities privately using GitHub's
[Private Vulnerability Reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability)
for this repository. This is the primary and preferred channel:

1. Go to the repository's **Security** tab:
   <https://github.com/eosrio/fleet-router/security>
2. Click **Report a vulnerability**.
3. Fill in the advisory form with as much detail as you can.

A helpful report typically includes:

- A description of the vulnerability and its potential impact.
- The affected version(s) and platform.
- Steps to reproduce, or a proof of concept.
- The relevant configuration (with any secrets redacted).
- Any suggested remediation, if you have one.

### What to expect

We handle reports on a best-effort basis. We aim to acknowledge new reports
within a few business days and will keep you updated on our assessment and any
fix as it progresses. We may contact you through the advisory thread for
additional details. Please give us a reasonable opportunity to investigate and
release a fix before any public disclosure.

## Security Model and Scope

fleet-router is designed to run inside a trusted network boundary. Before
deploying, you should understand the following known properties. **These are
documented design limitations, not vulnerabilities, and reports about them will
be treated as such.**

- **Unauthenticated listener.** The client-facing WebSocket listener performs no
  authentication or authorization. Anyone able to reach the listen address can
  open connections.
- **Plaintext transport, no TLS.** All transport is plaintext `ws://` on **both**
  the client listener and the upstream SHiP connections. No TLS / `wss://`
  support is compiled in, so traffic is neither encrypted nor integrity-protected
  in transit.
- **Optional metrics endpoint is unauthenticated.** When enabled
  (`metrics_port`), the HTTP `/health`, `/ready`, and `/metrics` endpoints are
  served over plaintext HTTP with no authentication.

Because of the above, fleet-router **must not be exposed directly to the public
internet**. Deploy it on a trusted/internal network and/or place it behind a
TLS-terminating reverse proxy (for example nginx, Caddy, or Envoy) that provides
encryption and access control.

### Trust boundary

Upstream SHiP nodes are part of the trust boundary. fleet-router parses bytes
received from upstreams through `rs_abieos`, a C++ library reached via FFI.
Only point fleet-router at upstream nodes you operate or otherwise trust.

### In scope

Reports that demonstrate behavior outside the documented design above are in
scope, including (non-exhaustively):

- Memory safety issues, crashes, or panics triggered by client or upstream
  input that is within the documented protocol.
- Resource-exhaustion vectors that bypass the configured safeguards
  (`max_connections`, `handshake_timeout_ms`, `idle_timeout_ms`,
  `max_message_bytes`).
- Logic flaws in failover, de-duplication, or shutdown that lead to data
  corruption or unexpected disclosure between client connections.

### Out of scope

- The absence of TLS, client authentication, or authorization on the listener
  and metrics endpoint (see the design limitations above).
- Issues that require a malicious or compromised upstream that you have
  explicitly configured and trusted, beyond the memory-safety expectations
  noted above.
- Vulnerabilities in third-party dependencies that are already tracked
  upstream; please report those to the relevant project (we monitor advisories
  via `cargo-deny` in CI).

## See Also

- [README.md](README.md) for deployment and configuration guidance.
- [CONTRIBUTING.md](CONTRIBUTING.md) for development and pull-request workflow.
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for community expectations.
- [CHANGELOG.md](CHANGELOG.md) for release history.
- [LICENSE](LICENSE) for licensing terms (MIT).
