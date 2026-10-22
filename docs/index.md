# Documentation

Run fleet-router in front of your SHiP nodes, serve archived history alongside live blocks, and manage the fleet from one place.

## Start here

- [Getting started](getting-started.md) — install a binary or public Docker image, create a configuration and connect your first client.
- [Wire Network](wire-network.md) — upgrade to 0.8.1 for the Wire SHiP ABI fix and verify the upstream handshake.
- [Changelog](../CHANGELOG.md) — release notes and upgrade behavior.

## Configure and deploy

- [Configuration reference](configuration.md) — fields and defaults, upstream selection, multiple networks, client and upstream TLS, certificate renewal and configuration reload.
- [Running in production](operations.md) — routing and failover, security, Linux, Docker, macOS and Windows services, shutdown and corrupted logs.
- [Frozen SHiP deployment](frozen-ship.md) — prepare archive logs and connect frozen ranges and live nodes through a stitched listener.

## Monitor and integrate

- [Observability](observability.md) — logging, health, readiness, coverage, Prometheus metrics and alerts.
- [Chain API](chain-api.md) — the optional HTTP chain API, supported endpoints, routing, limits and errors.
- [Dashboard and control API](api.md) — live fleet views, REST and streaming APIs, operator controls and the configuration editor.

## Example configurations

- [Two full nodes](../example.config.json)
- [Stitched archive and live nodes](../example.stitched.config.json)
- [Frozen SHiP deployment configuration](../deploy/frozen-ship.config.json)
- [Systemd service](../deploy/fleet-router.service) and [one service per network](../deploy/fleet-router@.service)
- [Docker Compose for multiple networks](../deploy/docker-compose.multi-network.yml)
