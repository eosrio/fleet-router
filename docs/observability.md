# Observability

fleet-router reports through structured logs and an optional HTTP endpoint for health checks, readiness, fleet coverage and Prometheus metrics. The same endpoint can also serve an opt-in live dashboard; see [Dashboard and control API](api.md).

- [Logging](#logging)
- [Health and metrics endpoint](#health-and-metrics-endpoint)
- [Readiness and coverage](#readiness-and-coverage)
- [Metrics](#metrics)
  - [Upstream series](#upstream-series)
  - [Listener series](#listener-series)
  - [Chain API series](#chain-api-series)
- [Alerts](#alerts)

## Logging

Logging uses `tracing`. Control verbosity with the `RUST_LOG` environment variable (default `info`):

```bash
# Everything at debug
RUST_LOG=debug fleet-router run --config config.json

# Debug for fleet-router, info for everything else
RUST_LOG=fleet_router=debug,info \
  fleet-router run --config config.json
```

Each client session ends with one info line, `session closed`, with the results it forwarded, its handoffs, failovers, dropped duplicates, keep-alives, reroutes and patched results, its duration and the reason it ended.

## Health and metrics endpoint

Set `metrics_port` (and optionally `metrics_address`) in the [config](configuration.md#health-and-metrics-endpoint) to enable an HTTP endpoint. It serves `GET` requests on:

- **`/health`**, **`/healthz`** — `200` while the process is running.
- **`/ready`**, **`/readyz`** — `200` if the listener can serve the chain tip (or, in `listeners` mode, if **every** listener can), otherwise `503`. See [Readiness and coverage](#readiness-and-coverage).
- **`/ready/<listener>`**, **`/readyz/<listener>`** — The same for one listener, or `404` if the listener name is unknown.
- **`/coverage`**, **`/coverage/<listener>`** — JSON fleet coverage of every listener, or of one (`404` if unknown).
- **`/metrics`** — Prometheus text exposition of router, upstream, listener and chain API state.

## Readiness and coverage

A listener can serve the chain tip when:

- **pool**: at least one upstream is online and not disabled by an operator;
- **stitched**: at least one live upstream is online and not disabled by an operator. A listener with no live server needs any online upstream instead.

In a listener with live servers, an archive outage never makes `/ready` fail, so one dead frozen range server cannot pull the router out of a load balancer. Two scopes ask for more, on `/ready`, `/readyz` and the per-listener forms:

- **`?scope=history`** — Tip readiness, and for a stitched listener also: no hole in the trace or chain-state coverage, no part of it without an online upstream, and no enabled frozen server that has neither reported nor declared its range. For a pool listener it is the same as tip readiness.
- **`?scope=api`** — Whether the [chain API](chain-api.md) can answer: it accepts connections, and a live upstream API on the listener's chain is up and not disabled by an operator. When no live server has an `api_endpoint`, a verified frozen range server API that is up stands in. On `/ready`, only the listeners with a chain API count. `/ready/<listener>?scope=api` for a listener without one, and `/ready?scope=api` when no listener has one, answer `503` with `no chain API configured`.

`?scope=tip` is the default. An unknown scope gets `400` (`unknown scope '<x>'; use tip, history or api`). Neither scope is ever part of plain `/ready`.

The body names the result. In single-port mode it is `ready: <n> upstream(s) online` (`ready: <n> chain API upstream(s) up` for the API scope), or `not ready: <reasons>`. In multi-listener mode it is `ready: <k> listener(s) ready, <n> ...`, or `not ready: <listener>: <reasons>, ...`. One listener: `ready (<name>): ...`. The reasons a listener is not ready:

| Reason | Scope |
| --- | --- |
| `no upstreams online` | tip (pool, or a stitched listener with no live server) |
| `no live upstreams online` | tip (stitched) |
| `gap [a,b) has no upstream (trace, chain_state)` | history |
| `segment [a,b) has no online upstream (trace)` | history |
| `<n> frozen upstream(s) have neither reported nor declared a range` | history |
| `chain API not listening` | api |
| `no live chain API upstream up` | api |
| `no verified frozen chain API upstream up` | api |
| `no chain API configured` | api |

When every unready listener is a pool listener with no online upstream, the multi-listener body keeps its older form: `not ready: listener(s) with no online upstreams: <names>`.

`GET /coverage` returns every listener (`/coverage/<listener>` one):

```json
{"listeners":[{
  "name": "main", "topology": "stitched", "generation": 812,
  "head": {"num": 412345678, "id": "<hex>"}, "lib": {"num": 412345330, "id": "<hex>"},
  "chain_id": "<hex>", "status_ready": true, "frozen_unreported": 0,
  "abi": {"fingerprint": "9c1e0f2a4b6d8e10", "source": "live-1", "provisional": false, "mismatched": []},
  "logs": {
    "trace": {
      "reported": {"begin": 2, "end": 412345679},
      "gaps": [], "offline_only": [],
      "segments": [
        {"begin": 2, "end": 50000001, "upstreams": [
          {"server": "archive-0a", "role": "frozen", "online": true, "sessions": 12},
          {"server": "archive-0b", "role": "frozen", "online": true, "sessions": 9, "operator_disabled": true}
        ]}
      ]
    },
    "chain_state": {"...": "..."},
    "finality": {"...": "..."}
  },
  "chain_api": {
    "listen": "10.0.0.5:18888",
    "live": [{"server": "live-1", "up": true, "head": 412345680, "lib": 412345330, "earliest": 400000000, "version": "v1.2.2"}],
    "frozen": [{"server": "archive-0a", "up": true, "verified": true, "blocks": {"begin": 1, "end": 50000001}, "get_block": "off"}]
  }
}]}
```

- `name` is `null` in single-port mode. `head`, `lib`, `chain_id` and `abi` are `null` until known.
- `status_ready` is whether the router can answer status now; `frozen_unreported` counts enabled frozen servers that have neither reported nor declared a range.
- `abi` is the ABI a stitched listener sends its clients: its `source` is `abi_file`, or the name of the upstream it came from; `provisional` is true while it came from a frozen upstream because no live one was seen yet; `mismatched` names the upstreams whose ABI differs.
- For each log, `reported` is the range a status reply reports, `gaps` the holes below it, and `offline_only` the parts that no online upstream covers. `segments` splits the coverage into ranges with the same set of upstreams; `sessions` is each upstream's connection count, and `operator_disabled` appears, set to `true`, only on an upstream an operator disabled or drained (`online` stays what the monitor sees).
- `chain_api` is `null` for a listener without one. For each upstream API, `up` means it answers its probes on the listener's chain and no request to it failed in transport since. Unknown values are `null`. `version` and `get_block` are what the upstream reported.

A pool listener is listed too, with `"topology": "pool"`.

## Metrics

Per-upstream series carry `endpoint` (the server's SHiP `endpoint`) and `server` (its name). Per-listener series carry no label in single-port mode. In multi-listener mode, every series except `fleet_router_up` also carries `listener="<name>"`. Label values escape `\`, `"`, and newlines per the Prometheus text format. No label value is text an upstream sent.

### Upstream series

```text
fleet_router_up
fleet_router_upstream_up{endpoint,server}
fleet_router_upstream_stale{endpoint,server}
fleet_router_upstream_operator_disabled{endpoint,server}
fleet_router_upstream_priority{endpoint,server}
fleet_router_upstream_weight{endpoint,server}
fleet_router_active_connections{endpoint,server}
fleet_router_upstream_trace_begin_block{endpoint,server}
fleet_router_upstream_trace_end_block{endpoint,server}
fleet_router_upstream_chain_state_begin_block{endpoint,server}
fleet_router_upstream_chain_state_end_block{endpoint,server}
fleet_router_upstream_finality_data_begin_block{endpoint,server}
fleet_router_upstream_finality_data_end_block{endpoint,server}
fleet_router_upstream_head_block{endpoint,server}
fleet_router_upstream_lib_block{endpoint,server}
fleet_router_upstream_block_failures_total{endpoint,server}
fleet_router_upstream_quarantined{endpoint,server}
fleet_router_upstream_quarantine_block{endpoint,server}
fleet_router_upstream_info{endpoint,server,role}
fleet_router_upstream_ranges_known{endpoint,server}
fleet_router_upstream_range_mismatch{endpoint,server}
fleet_router_upstream_abi_info{endpoint,server,fingerprint}
fleet_router_upstream_abi_compatible{endpoint,server}
fleet_router_upstream_abi_match{endpoint,server}
fleet_router_upstream_links_total{endpoint,server,purpose}
fleet_router_upstream_dial_failures_total{endpoint,server,reason}
fleet_router_upstream_stalls_total{endpoint,server}
fleet_router_upstream_sequence_jumps_total{endpoint,server}
fleet_router_upstream_contract_violations_total{endpoint,server,kind}
fleet_router_upstream_no_block_log{endpoint,server}
fleet_router_upstream_results_without_block_total{endpoint,server}
fleet_router_upstream_prev_blocks_filled_total{endpoint,server}
```

`fleet_router_up` is a process gauge and has no labels.

#### SHiP ranges and tip

`trace_begin_block`, `chain_state_begin_block`, and `finality_data_begin_block` are the first block of each SHiP range on that upstream, and `trace_end_block` and `finality_data_end_block` are one past its last block. `chain_state_end_block` is the chain-state end as the upstream reports it. `head_block` and `lib_block` are the head and LIB from its status. The gauges are always emitted. `0` is the value before a status result arrives. `finality_data_begin_block` (and `finality_data_end_block`) stay `0` on pre-Savanna nodes that only answer `get_status_result_v0` and never report a finality range.

#### Roles, declared ranges and ABIs

- `fleet_router_upstream_info{role}` is always `1`; `role` is `live` or `frozen`.
- `fleet_router_upstream_ranges_known` is `1` once the upstream reported its ranges, or, for a frozen server, when it declares a `range`.
- `fleet_router_upstream_range_mismatch` is `1` while a frozen server's reported trace or chain-state range differs from its declared `range`. A log that starts at block 2 when the range starts at 1, or that ends one block before the range's end while the other log reaches it, is no mismatch.
- `fleet_router_upstream_abi_info{fingerprint}` (always `1`) carries a 16-hex-digit fingerprint of the ABI the upstream sent its monitor. `fleet_router_upstream_abi_compatible` is `1` when that ABI's request and result variants are a prefix of Spring's. Both are absent until an ABI arrives.
- `fleet_router_upstream_abi_match` is `1` when the upstream's ABI is the one its stitched listener sends clients. It is absent for pool listeners and until both are known.

#### Session links

- `fleet_router_active_connections` counts the connections the router's sessions hold to that upstream: session links and dials in progress, and in a stitched listener also pre-dialed standby links. This is what `max_sessions` caps. The monitor's connection is not counted. A stitched client that only asks for status holds none.
- `fleet_router_upstream_links_total{purpose}` counts links that sessions opened: `initial` (a client's request), `handoff` (the next leg at a range boundary), `failover`, `reroute` (a pool request moved to an upstream that serves it better) and `predial` (a standby link).
- `fleet_router_upstream_dial_failures_total{reason}`: `busy` (HTTP 503 at the upgrade), `unreachable`, `handshake` and `abi`.
- `fleet_router_upstream_stalls_total` counts links failed over for `upstream_stall_timeout_ms` of silence while blocks were owed, and `fleet_router_upstream_sequence_jumps_total` forward block jumps from a live upstream that sessions adopted (Leap 4 and 5 nodes may jump to their first available block).

#### Block failures and quarantine

`fleet_router_upstream_block_failures_total` counts unclean mid-`get_blocks` drops (the socket died with no WebSocket close frame) while a blocks request was in flight, and, for a frozen upstream, its stalls and archive contract violations.

`fleet_router_upstream_quarantined` is `1` while that upstream is excluded for any block range. `fleet_router_upstream_quarantine_block` is the start block of that exclusion, or `0` when none is active; if several ranges are active it is the lowest start block. The block number is the gauge value, not a label.

See [Corrupted SHiP logs](operations.md#corrupted-ship-logs) and [Failover and strikes](operations.md#failover-and-strikes) for when these move.

#### Frozen range servers

These are rendered for frozen upstreams only:

- `fleet_router_upstream_contract_violations_total{kind}` counts archive contract violations: `idless`, `gap`, `rewind`, `missing_field`, `credit`, `variant` and `discontinuity` (see [Failover and strikes](operations.md#failover-and-strikes)).
- `fleet_router_upstream_no_block_log` is `1` while the archive is known to have no block log: it sent a result without the requested `block` for a block it covers. It clears when the archive reconnects or reports another status.
- `fleet_router_upstream_results_without_block_total` counts its results forwarded without the requested `block`, because no upstream that holds blocks could serve them.
- `fleet_router_upstream_prev_blocks_filled_total` counts its results that lacked `prev_block` right after a block the session delivered, filled in by the router.

See [fetch_block and block logs](operations.md#fetch_block-and-block-logs).

#### Operator overrides

`fleet_router_upstream_priority` and `fleet_router_upstream_weight` report the values in effect. They differ from the config file after an operator changes them through the [control API](api.md#control-endpoints). `fleet_router_upstream_operator_disabled` is `1` while an operator has disabled or drained that upstream. An operator-disabled upstream does not count toward `/ready`.

### Listener series

These are rendered for every listener, pool and stitched.

```text
fleet_router_fleet_head_block
fleet_router_fleet_lib_block
fleet_router_fleet_head_age_seconds
fleet_router_fleet_coverage_begin_block{log}
fleet_router_fleet_coverage_end_block{log}
fleet_router_fleet_coverage_gaps{log}
fleet_router_fleet_offline_coverage_blocks{log}
fleet_router_fleet_live_overlap_blocks{log}
fleet_router_fleet_frozen_unreported
fleet_router_sessions{state}
fleet_router_status_replies_total{version,source}
fleet_router_handoffs_total
fleet_router_handoff_latency_seconds (histogram)
fleet_router_failovers_total{reason}
fleet_router_reroutes_total
fleet_router_duplicates_dropped_total
fleet_router_fork_redeliveries_total
fleet_router_keepalives_total
fleet_router_credit_overruns_total
fleet_router_patched_results_total
fleet_router_holds_total{why}
fleet_router_hold_closes_total{why}
fleet_router_predials_total{result}
fleet_router_client_protocol_errors_total{kind}
fleet_router_too_large_closes_total
```

#### Fleet tip and coverage

- `fleet_router_fleet_head_block` and `fleet_router_fleet_lib_block` are the fleet head and LIB: what the router's status replies report (`0` until known). The LIB never decreases. `fleet_router_fleet_head_age_seconds` is the time since the fleet head last advanced (since startup while none is known).
- The coverage gauges carry `log` = `trace`, `chain_state` or `finality`. `coverage_begin_block` and `coverage_end_block` bound the top contiguous component of that log's coverage, the range status reports (`0` if none). `coverage_gaps` counts the holes below it. `offline_coverage_blocks` counts the blocks of the top component that no online upstream covers.
- `fleet_router_fleet_live_overlap_blocks{log}` (trace and chain state only) is informational: the highest frozen end (exclusive) minus the lowest begin of the online live upstreams, so how many archived blocks the live tier also holds. A small value is normal: a live node started from a snapshot at block `S`, after archives that end at `S + 1`, overlaps them by `0` blocks of trace and `1` of chain state (see [Cut-over from a snapshot](operations.md#cut-over-from-a-snapshot)). Negative means a hole between the archives and the live tier, which the gap and offline coverage gauges also show. It is absent while the listener has no frozen upstream or no online live upstream.
- `fleet_router_fleet_frozen_unreported` counts enabled frozen upstreams that have neither reported nor declared a range. While it is above `0`, a stitched listener holds status requests.

#### Sessions and failovers

- `fleet_router_sessions{state}` counts client sessions by what they are doing: `idle` (no request, or the request is satisfied), `streaming`, `dialing`, `backoff` (waiting before the next link after a failover), `holding` and `waiting` (see [Holds, waits and gaps](operations.md#holds-waits-and-gaps)).
- `fleet_router_status_replies_total{version,source}`: `version` is `v0` or `v1`; `source` is `router` for replies the router encoded and `upstream` for replies relayed from a pool session's upstream while the fleet tip was unknown.
- `fleet_router_handoffs_total` counts planned handoffs from one upstream to the next at a range boundary. `fleet_router_handoff_latency_seconds` is the time from the last frame of the old link to the first frame of the new one, with buckets from 1 ms to 10 s.
- `fleet_router_failovers_total{reason}` counts failovers to another upstream link: `unclean` (no close frame), `clean` (close frame), `idle` (`idle_timeout_ms` while owed), `stall` (`upstream_stall_timeout_ms` while owed), `offline` (the monitor lost the upstream and it stayed silent for 5 s), `violation` (archive contract), `uncovered` (a resume jumped past the cursor while another upstream covers it), `send` (the request could not be written), `silent` (`upstream_liveness_timeout_ms`), `operator` (moved through the control API) and `no_block_log` (moved off an archive without a block log). `fleet_router_reroutes_total` counts pool requests moved to an upstream that serves them better before they were forwarded.
- `fleet_router_duplicates_dropped_total` counts results an upstream sent again after a failover that were dropped; each was refunded to it with an ack. `fleet_router_fork_redeliveries_total` counts results for an already delivered block number with a new id, which are forwarded.
- `fleet_router_keepalives_total` counts head-only keep-alive results from EOSIO 2.0 and Leap 3 upstreams forwarded to clients.
- `fleet_router_credit_overruns_total` counts results forwarded while the client had no credit left. It stays `0` with Spring upstreams.
- `fleet_router_patched_results_total` counts results whose head or LIB the router rewrote.
- `fleet_router_holds_total{why}` and `fleet_router_hold_closes_total{why}` count sessions that started holding for a block no upstream could serve yet, and those closed with 1011 after `gap_wait_ms`. `why` is `offline`, `quarantined`, `at_capacity` or `busy`.
- `fleet_router_predials_total{result}` counts standby links opened ahead of a handoff: `used`, `failed` or `discarded`.
- `fleet_router_client_protocol_errors_total{kind}` counts client frames that were not a valid SHiP request: `malformed` or `unknown_variant`.
- `fleet_router_too_large_closes_total` counts clients closed because an upstream message exceeded `max_message_bytes`.

### Chain API series

These are rendered only when at least one listener has a [chain API](chain-api.md). The per-upstream ones use the server's usual `endpoint` and `server` labels, so they join every other upstream series; `endpoint` is the SHiP endpoint, not the `api_endpoint`.

```text
fleet_router_upstream_api_up{endpoint,server}
fleet_router_upstream_api_chain_id_mismatch{endpoint,server}
fleet_router_upstream_api_head_block{endpoint,server}            # live only
fleet_router_upstream_api_earliest_block{endpoint,server}        # live only
fleet_router_upstream_api_blocks_begin_block{endpoint,server}    # frozen only
fleet_router_upstream_api_blocks_end_block{endpoint,server}      # frozen only
fleet_router_upstream_api_verified{endpoint,server}              # frozen only
fleet_router_upstream_api_in_flight{endpoint,server}
fleet_router_upstream_api_max_in_flight{endpoint,server}
fleet_router_upstream_api_requests_total{endpoint,server,result}
fleet_router_api_requests_total{class,code}
fleet_router_api_request_duration_seconds{class} (histogram)
fleet_router_api_in_flight
fleet_router_api_rejected_total{reason}
fleet_router_api_retries_total{reason}
fleet_router_api_block_requests_total{region,role}
fleet_router_api_pushes_total{result}
```

Per upstream API:

- `fleet_router_upstream_api_up` is `1` while the API answers its probes on the listener's chain and no request to it failed in transport since. An operator disable is not reflected here; `fleet_router_upstream_operator_disabled` shows it.
- `fleet_router_upstream_api_chain_id_mismatch` is `1` while the API reports another `chain_id` than the listener's. Such an API is not used.
- `fleet_router_upstream_api_head_block` is the head a live API reported to its probes, and `fleet_router_upstream_api_earliest_block` its `earliest_available_block_num` (`0` until known, and for nodes that do not report it, such as EOSIO 2.0 and FIO).
- `fleet_router_upstream_api_blocks_begin_block` and `_blocks_end_block` are the half-open range of blocks a frozen range server's API serves (`0` until it described itself). `fleet_router_upstream_api_verified` is `1` once it was verified against its SHiP endpoint; an API never verified is not used.
- `fleet_router_upstream_api_in_flight` counts client requests in flight to the API (probes are not counted), and `fleet_router_upstream_api_max_in_flight` is its cap: `max_in_flight_per_upstream`, for a frozen range server at most the limit it describes.
- `fleet_router_upstream_api_requests_total{result}` counts attempts sent to the API: `ok` (2xx), `unknown_block`, `not_found`, `busy` (503), `error` (any other answer), `connect` (the connection failed, or was lost after the request was written) and `timeout`.

Per listener:

- `fleet_router_api_requests_total{class,code}` counts requests by endpoint class and HTTP status class. `class` is `block`, `info`, `state`, `exec`, `push`, `node` (OPTIONS and `get_supported_apis`), `denied` (the 404s) or `invalid` (Illegal request-target); `code` is `2xx`, `3xx`, `4xx` or `5xx`. Every combination is rendered.
- `fleet_router_api_request_duration_seconds{class}` is the time from a request's head to the head of its answer, body read included, with buckets from 1 ms to 10 s.
- `fleet_router_api_in_flight` counts requests holding one of the listener's `max_in_flight` slots.
- `fleet_router_api_rejected_total{reason}`: `connections` (closed at accept over `max_connections`), `in_flight` (503 at `max_in_flight`), `body_too_large` (413) and `timeout` (a body not received in time, or a client that took none of its answer).
- `fleet_router_api_retries_total{reason}` counts attempts whose answer passed the request on to the next candidate: `unknown_block`, `not_found`, `busy`, `connect`, `timeout` and `error` (a `get_info` 5xx other than 503).
- `fleet_router_api_block_requests_total{region,role}` counts block requests by region (`irreversible`, `reversible`, or `unhinted` for a body with no block number the router could read) and by who wrote the answer (`live`, `frozen`, or `none` when the router answered Busy). Only the combinations that can occur are rendered: a frozen range server answers only irreversible blocks.
- `fleet_router_api_pushes_total{result}`: `ok` (a 2xx), `error` (another answer, or no node reached) and `ambiguous` (the connection failed after the push was written; it is never resent).

Each chain API listener adds 173 series, and each upstream API 13 (live) or 14 (frozen).

## Alerts

Starting points for a stitched listener:

| Expression | What it means |
| --- | --- |
| `fleet_router_fleet_coverage_gaps > 0` | A block range no upstream covers, such as a hole between the archives and the live tier; clients that reach it are closed with 1011. |
| `fleet_router_fleet_offline_coverage_blocks > 0` for 2 minutes | History with no online upstream, such as the blocks between the archives and the live tier while the live node that holds them is down; sessions there hold, then close after `gap_wait_ms`. |
| `fleet_router_fleet_head_age_seconds > 30` | The fleet head stopped advancing. |
| `increase(fleet_router_upstream_contract_violations_total[10m]) > 0` | An archive broke the contract; check its data. |
| `increase(fleet_router_credit_overruns_total[10m]) > 0` | An upstream sent results beyond the client's credit. |
| `histogram_quantile(0.99, rate(fleet_router_handoff_latency_seconds_bucket[5m]))` | Slow handoffs between ranges. |
| `fleet_router_upstream_no_block_log == 1` | An archive serves without blocks; `fetch_block` clients get no `block` where no other upstream holds it. |

Alert on holes with the two coverage gauges, not on `fleet_router_fleet_live_overlap_blocks`: an overlap of `0` or `1` block is the normal shape right after a cut-over, and a hole between the archives and the live tier already shows as a gap or as offline coverage.

For a chain API: `fleet_router_upstream_api_up == 0` and `fleet_router_upstream_api_chain_id_mismatch == 1` per upstream, and a rising share of `fleet_router_api_requests_total{code="5xx"}`.
