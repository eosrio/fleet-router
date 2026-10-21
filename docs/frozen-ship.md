# Deploy frozen SHiP history

Serve old, irreversible history from disk with [antelope-ship-archive](https://github.com/eosrio/hyperion-tools/tree/main/crates/ship-archive), and keep live SHiP nodes for recent blocks. A frozen server reads existing state-history and block logs without running nodeos, replaying the chain, or maintaining chain state.

With **fleet-router 0.7.0 or later**, one stitched listener presents those servers as a single SHiP endpoint. Clients can read from archived history into the live head through one WebSocket connection. The archive server is **preview**: pin and qualify a Hyperion Tools revision against your own logs. See its [wire contract](https://github.com/eosrio/hyperion-tools/blob/main/crates/ship-archive/CONTRACT.md) and [deployment companion](https://github.com/eosrio/hyperion-tools/blob/main/crates/ship-archive/FLEET_ROUTER.md).

## What you will deploy

**One client endpoint, two frozen ranges, and a live tier.** The router selects the server for each leg of a request and hands the stream to the next server at a range boundary. It preserves the client's request flags and outstanding credit; the client keeps its normal SHiP acknowledgement and checkpoint logic.

Use `role: "frozen"` for archive servers, declare their ranges, and pin the listener's `chain_id`. `topology: "stitched"` makes the deployment explicit. Do not use a 0.6 binary for this configuration: stitched history requires 0.7 or later. Check `fleet-router --version` before rollout.

## Topology and range plan

The private addresses and block ranges below are examples. Replace them with ranges verified from your copies and your live node. Every endpoint must belong to the same chain.

```text
consumer / Hyperion / custom SHiP client
  |
  +-- ws://10.0.0.10:17000 -- fleet-router (listener: main)
                               |
                               +-- 10.0.0.21:8080  archive-a [1000001,2000001)
                               +-- 10.0.0.22:8080  archive-b [2000001,3000001)
                               +-- 10.0.0.31:8080  live node, recent + head
```

Ranges are **half-open**: `[1000001,2000001)` includes block 1,000,001 through 2,000,000. SHiP `end_block_num`, the router's declared `range.end`, and the archive's `--range BEGIN-END` use an exclusive end. Retained filenames use an inclusive last block; trust reported coverage rather than inferring it from names.

| Upstream | Role | Required coverage | Work |
| --- | --- | --- | --- |
| `archive-a` | `frozen` | `[1000001,2000001)` | First backfill segment |
| `archive-b` | `frozen` | `[2000001,3000001)` | Second backfill segment |
| `live-1` | `live` | Starts at or before `3000001` | Recent history and advancing head |

Keep all requested log kinds contiguous across the plan: traces, chain state for deltas, finality data when needed, and blocks. Adjacent archive ranges are enough if their block IDs join correctly. Keep verified irreversible overlap where available for independent seam checks. At the archive-to-live boundary, the live tier's first block must be at or below the last archive's exclusive end.

The example has one server per range and no redundancy for that range. For production, add a second replica of **each same range** on a separate host, plus another live node. The [complete replicated example](../example.stitched.config.json) shows two archives with two replicas each and two live nodes. Priority and weight do not fill missing history.

## Prepare and validate the frozen logs

Copy only irreversible history: the producer's LIB must have passed the last block in every copied file, including boundary files. Use a stopped source node, a consistent filesystem snapshot, or closed retained files that are no longer being written; rotation alone does not establish finality. Copy matching `.log` and `.index` files together, and compare inventories and checksums after transfer. Do not point the archive server at active logs or replace its files while it runs.

Each archive needs:

- `trace_history` logs for traces.
- Matching `chain_state_history` logs if the consumer requests deltas.
- `finality_data_history` logs if the consumer requests finality data.
- Non-pruned block logs and their indexes covering the entire range, for `fetch_block`, chain identity, and boundary block IDs.

Use the default `--blocks required` for this deployment. A state-history-only archive cannot supply block payloads. Missing kinds are not synthesized: inspect every payload range your consumer needs.

An archive directory can contain retained files under separate subdirectories:

```text
/srv/ship/archive-a/
  state-history/
    trace_history-1000001-2000000.log
    trace_history-1000001-2000000.index
    chain_state_history-1000001-2000000.log
    chain_state_history-1000001-2000000.index
  blocks/
    blocks-1000001-2000000.log
    blocks-1000001-2000000.index
```

The filenames illustrate the layout, not a requirement to repackage existing files. **`--range` is optional:** `check` and `serve` automatically detect each log's coverage from the file contents. With the default `--blocks required`, state-history coverage is also limited to the available block logs. Inspect the reported coverage rather than assuming all payload types span the same blocks.

The commands below serve each directory's detected coverage. Use `--range BEGIN-END` only to serve a subset of a larger copy, for example when neighboring servers share files or a snapshot cut-over needs a precise end. Pass the same clipping option to both `check` and `serve`, and keep boundary files for predecessor verification. The catalogs still require contiguous, consistent history for each served log kind.

Build from a revision you have qualified, from the Hyperion Tools repository root:

```sh
cargo build --release --locked --package antelope-ship-archive
./target/release/antelope-ship-archive --help
```

Install that binary as `antelope-ship-archive` on each archive host. On archive A, validate the exact options you will serve:

```sh
antelope-ship-archive check \
  --dir /srv/ship/archive-a --recursive \
  --deep --hash --strict
```

Repeat on archive B with its directory. Confirm the detected ranges match your plan: the example expects `[1000001,2000001)` on A and `[2000001,3000001)` on B. `--hash` includes deep validation and recomputes block IDs. Exit `0` means clean, `1` means errors, and `2` with `--strict` means warnings. Resolve findings before rollout; do not disable ID checks to make startup pass. Compare the first block's `prev_block` with the previous range's last block ID, and compare IDs in any retained overlap.

If indexes are missing or invalid, follow the archive's [sidecar index workflow](https://github.com/eosrio/hyperion-tools/tree/main/crates/ship-archive#missing-or-wrong-indexes). Do not repair original evidence in place.

## Start each archive server

On archive A:

```sh
antelope-ship-archive serve \
  --dir /srv/ship/archive-a --recursive \
  --listen 10.0.0.21:8080 --http 10.0.0.21:9180 \
  --max-sessions 32 --memory-budget 268435456 --stream-threshold 1048576
```

On archive B:

```sh
antelope-ship-archive serve \
  --dir /srv/ship/archive-b --recursive \
  --listen 10.0.0.22:8080 --http 10.0.0.22:9180 \
  --max-sessions 32 --memory-budget 268435456 --stream-threshold 1048576
```

Run each command under your service manager with an unprivileged account and read-only access to the logs. The archive's [Docker instructions](https://github.com/eosrio/hyperion-tools/tree/main/crates/ship-archive#docker) show read-only mounts. The settings above bound result buffers to about 1.1 GiB per process; allow additional memory for the runtime and filesystem cache, and size concurrency for the disk.

Check both `/ready` and `/health` on port `9180`. Readiness says the archive is accepting sessions; `/health` reports chain identity, first/last block IDs, findings, and served ranges. Confirm full trace, delta, block and finality coverage as needed. The archive's head and LIB stay at the end of its range; that is expected.

Keep archive WebSocket and HTTP ports private. The services use plaintext connections without authentication. Restrict upstream access to the router and monitoring access to operators.

## Configure the stitched listener

Install fleet-router 0.7.0 or later using the [installation options](https://github.com/eosrio/fleet-router#installation). Confirm the binary's version before loading the config below.

Download the [complete example config](../deploy/frozen-ship.config.json). It uses the EOS chain ID for illustration. Replace it, the bind address and every upstream address. Verify each server's chain identity. `require_chain_id: true` rejects older nodes that omit it.

The router's JSON `range` is also optional, but recommended as a startup fallback before an archive reports its coverage. Fill it from the verified `check` output or `/health`. It does not limit what the archive serves; the archive's optional `--range` does that.

```json
{
  "listen_address": "10.0.0.10",
  "upstream_reconnect_ms": 3000,
  "upstream_monitoring_ms": 5000,
  "upstream_status_ms": 1000,
  "metrics_address": "127.0.0.1",
  "metrics_port": 9090,
  "dashboard": { "enabled": true },
  "listeners": [
    {
      "name": "main",
      "listen_port": 17000,
      "chain_id": "aca376f206b8fc25a6ed44dbdc66547c36c6c33e3a119ffbeaef943642f0e906",
      "require_chain_id": true,
      "topology": "stitched",
      "stitching": {
        "history_preference": "frozen",
        "predial_blocks": 2000,
        "gap_wait_ms": 30000,
        "patch_head_lib": true
      },
      "servers": [
        {
          "name": "archive-a", "endpoint": "10.0.0.21:8080", "enabled": true,
          "role": "frozen", "range": { "begin": 1000001, "end": 2000001 },
          "max_sessions": 24
        },
        {
          "name": "archive-b", "endpoint": "10.0.0.22:8080", "enabled": true,
          "role": "frozen", "range": { "begin": 2000001, "end": 3000001 },
          "max_sessions": 24
        },
        {
          "name": "live-1", "endpoint": "10.0.0.31:8080", "enabled": true,
          "role": "live"
        }
      ]
    }
  ]
}
```

```sh
fleet-router --version
fleet-router config test --strict ./frozen-ship.config.json
fleet-router run --config ./frozen-ship.config.json
```

The strict check probes every server, prints each payload's coverage and holes, and fails on trace/chain-state holes or a frozen server that did not report its ranges. Check the output against your plan, including finality and block availability; a declaration cannot create missing payloads. See [running as a service](operations.md#run-as-a-service) for persistent deployment.

All tiers and the consumer need compatible ABI and payload encodings. A stitched listener sends one canonical ABI to each session; `stitching.abi_mismatch` defaults to `"warn"`, which keeps mismatched upstreams eligible. Use `"exclude"` when mismatches should block routing, and qualify the actual historical payloads. `stitching.abi_file` and the archive's `--abi` select ABI text; they do not convert incompatible stored formats. See [ABI policy](configuration.md#topology-and-stitching).

`history_preference: "frozen"` keeps covered history reads off live nodes. `predial_blocks` prepares the next upstream before a boundary. `patch_head_lib` presents fleet head and LIB in archive results, so an indexer does not mistake a frozen server's last block for the chain tip. Frozen servers do not need an advancing head and must be declared with the right role.

The router's `max_sessions: 24` counts active, standby and in-progress session links. Its monitor connection is additional; leaving room below the archive's `--max-sessions 32` avoids exhausting that server with monitoring and operator checks. Size both limits together.

The default assembled-message limit is 256 MiB. Archive frame streaming does not bypass it: a large snapshot delta or the sum of requested payloads can exceed it. Check entry sizes, inspect representative combined results, and set `max_message_bytes` and client limits consistently before admitting that workload.

## Connect clients and verify the seams

Point the consumer's SHiP URL at **`ws://10.0.0.10:17000`**. Request the start block and fetch flags as usual, and keep normal SHiP credit/acks. A request spanning both archive ranges and the live tier stays on that client connection; the router plans and switches upstream legs. Your consumer still needs durable checkpoints, idempotent retry handling, and its normal live-fork handling.

At a seam, the first result must continue the previous block's ID. All frozen data must already be irreversible: predecessor checks at handoffs do not prove finality inside copied files. A discontinuous archive is failed over and counted as a contract violation; the router does not repair its logs. See [seams and handoffs](operations.md#seams-and-handoffs) for the full contract.

An archive stays connected and silent at its range end. The router knows the leg's end and initiates the handoff; it does not wait for the archive to close. Toward frozen upstreams it sends no `have_positions`, avoiding a rewind below that archive's range.

For a snapshot cut-over at block `S`, end the last archive at **`S + 1`**, so its last served block is `S`. If its files extend further, add `--range BEGIN-END` with `END = S + 1` to both its `check` and `serve` commands. Setting only the router's declared `range` does not clip the archive. A live node started from that snapshot normally holds traces/blocks from `S + 1` and chain state from `S`. Do not serve copied fork remnants after `S`. This zero-block trace overlap is contiguous; it is not a history hole. See [snapshot cut-over](operations.md#cut-over-from-a-snapshot).

## Check readiness and exercise failure

On the router host:

```sh
curl --fail http://127.0.0.1:9090/ready/main
curl --fail 'http://127.0.0.1:9090/ready/main?scope=history'
curl --fail http://127.0.0.1:9090/coverage/main
curl --fail http://127.0.0.1:9090/metrics
```

Plain readiness checks the **tip** when live servers are configured; an archive outage does not make it fail. History readiness also checks that archived trace/chain-state coverage is complete and online. `/coverage/main` reports gaps, offline-only segments and the servers covering each log. Neither readiness check replaces validation of actual logs, requested finality/block payloads, and client behavior.

With your real consumer, verify:

- The first and last block of each range, with every requested payload present.
- One uninterrupted read across archive A → archive B → live, with consecutive block IDs and no duplicate durable effects. Observe `fleet_router_handoffs_total` and handoff latency.
- Once same-range replicas are configured, stopping one archive resumes its readers on a replica. The minimal three-server example has no archive redundancy.
- Stopping all configured copies of an archived segment, while keeping their range declarations, holds readers and eventually closes them with 1011 after `gap_wait_ms` (30 seconds here), without silently skipping blocks. Tip-only readers can continue on live nodes. A known hole between ranges closes immediately instead.
- `/ready/main?scope=history` detects the archive outage while plain `/ready/main` remains healthy if the live tier is available.

The router waits for future live blocks separately from an unavailable archive segment. See [holds, waits and gaps](operations.md#holds-waits-and-gaps) and [fleet alerts](observability.md#alerts) before choosing alert thresholds or increasing timeouts.

## Optional HTTP chain API

SHiP uses the WebSocket port above. To route HTTP block lookups as well, enable the archive's separate API with `--api 10.0.0.21:8888` (and the corresponding address on B). Add each archive's HTTP address as its router `api_endpoint`, add the live node's nodeos HTTP endpoint, and set the listener's `chain_api` to `{"listen_address":"10.0.0.10","listen_port":18888}`.

Keep all these ports private. The archive answers selected block endpoints, while the router sends other allowed calls to live nodes. Run the strict config check again: with `chain_api` enabled it also checks archive API coverage. Follow the [chain API guide](chain-api.md) for supported endpoints, limits and read-only defaults; port `9180` is archive health/metrics, not its chain API.

## Add or replace a range

Prepare a new immutable copy, deep/hash-check it, start its server, and verify health, payload coverage and boundary IDs before routing readers to it. Archive catalogs are fixed at startup; adding files requires a restart. Keep a verified copy of the old range until its replacement has passed the same checks.

Add new ranges and same-range replicas to the stitched listener's `servers`. Config-file changes require a controlled router restart; follow [graceful shutdown](operations.md#graceful-shutdown) and expect clients to reconnect to the restarted router from their checkpoints. The control API can drain one replica during maintenance; it cannot substitute for a missing copy of history.

## References

- [Archive installation, inputs, operations and validation](https://github.com/eosrio/hyperion-tools/tree/main/crates/ship-archive)
- [Archive deployment companion](https://github.com/eosrio/hyperion-tools/blob/main/crates/ship-archive/FLEET_ROUTER.md)
- [Stitched routing contract](operations.md#range-split-fleets), [configuration](configuration.md#topology-and-stitching), and [readiness/coverage](observability.md#readiness-and-coverage)
- Current implementation: [routing planner](../src/fleet/planner.rs) and [session actor](../src/session/mod.rs)
