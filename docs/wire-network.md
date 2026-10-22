# Wire Network

Use **fleet-router 0.8.1 or later** with Wire Network SHiP upstreams. Version 0.8.1 uses the published `rs_abieos` 0.7.0 Rust backend, which accepts Wire's `sysio::abi/1.*` handshake. Earlier fleet-router releases rejected this ABI version before the upstream could become usable.

## Recognize the handshake failure

An affected router can connect to the WebSocket and immediately reconnect after an ABI warning:

```text
INFO monitoring connected to upstream upstream=10.0.0.10:8081
WARN error setting ABI for upstream upstream=10.0.0.10:8081
INFO reconnecting to upstream upstream=10.0.0.10:8081 retry_in_ms=3000
```

Reachability alone does not confirm compatibility. In 0.8.1 the warning also includes the ABI parser error, making other ABI problems easier to identify.

## Upgrade

Install the [0.8.1 release](https://github.com/eosrio/fleet-router/releases/tag/v0.8.1), update your service to that binary or image, and restart the router. An executable upgrade needs a process restart; configuration reload does not replace the running binary. Existing clients must reconnect from their checkpoints.

The public image requires no GitHub login:

```sh
docker pull ghcr.io/eosrio/fleet-router:0.8.1
docker run --rm ghcr.io/eosrio/fleet-router:0.8.1 --version
```

For a source install:

```sh
cargo install fleet-router --version 0.8.1 --locked
fleet-router --version
```

If your service uses an absolute executable path, confirm that it points to the upgraded binary. Read the [0.7 upgrade notes](../CHANGELOG.md#070---2026-10-18) when upgrading from an earlier release, and the [0.8.0 notes](../CHANGELOG.md#080---2026-10-21) for TLS and configuration reload behavior.

## Verify the deployment

1. Confirm the running service reports version 0.8.1 or later.
2. Run `fleet-router config test ./config.json --strict` against the Wire upstreams. Use the actual deployment configuration and pin the Wire chain ID rather than copying an ID from another network's example.
3. Check that upstream monitoring completes without repeating ABI errors and that [readiness](observability.md#readiness-and-coverage) reports an online upstream for the Wire listener.
4. Connect your actual SHiP consumer, read blocks through the required historical range into the live head, then exercise upstream failure and reconnection. Verify the payloads your application needs, including traces and deltas.

## Compatibility coverage

The regression suite loads Wire's SHiP ABI and exercises v0 and v1 status, block requests and acknowledgements through a running router with mock upstreams. It checks that opaque block, trace, delta and v1 finality payload bytes are preserved. The fixture records its upstream revision and checksum in [the test fixtures](https://github.com/eosrio/fleet-router/tree/main/tests/fixtures).

This validates the ABI fix and router protocol handling. Live Wire history and application decoding still need deployment-specific checks. The router does not translate incompatible historical payload encodings. For mixed archive and live fleets, review the [stitched ABI policy](configuration.md#topology-and-stitching).
