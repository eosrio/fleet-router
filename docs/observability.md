# Observability

fleet-router reports through structured logs and an optional HTTP endpoint for health checks and Prometheus metrics. The same endpoint can also serve an opt-in live dashboard; see [Dashboard and control API](api.md).

## Logging

Logging uses `tracing`. Control verbosity with the `RUST_LOG` environment variable (default `info`):

```bash
# Everything at debug
RUST_LOG=debug fleet-router run --config config.json

# Debug for fleet-router, info for everything else
RUST_LOG=fleet_router=debug,info \
  fleet-router run --config config.json
```

## Health and metrics endpoint

Set `metrics_port` (and optionally `metrics_address`) in the [config](configuration.md#health-and-metrics-endpoint) to enable an HTTP endpoint. It serves `GET` requests on:

- **`/health`**, **`/healthz`** — `200` while the process is running.
- **`/ready`**, **`/readyz`** — `200` if at least one upstream is online and not disabled by an operator (or, in `listeners` mode, if **every** listener has at least one upstream online), otherwise `503`.
- **`/ready/<listener>`**, **`/readyz/<listener>`** — `200` if the named listener has at least one online upstream, `503` if none are online, or `404` if the listener name is unknown.
- **`/metrics`** — Prometheus text exposition of router and upstream state.

## Metrics

Each per-upstream series carries `server="<name>"` from the configured server name. In multi-listener mode, each upstream series also carries `listener="<name>"`.

```text
fleet_router_up
fleet_router_upstream_up{endpoint,server}
fleet_router_upstream_stale{endpoint,server}
fleet_router_upstream_operator_disabled{endpoint,server}
fleet_router_upstream_priority{endpoint,server}
fleet_router_upstream_weight{endpoint,server}
fleet_router_active_connections{endpoint,server}
fleet_router_upstream_trace_begin_block{endpoint,server}
fleet_router_upstream_chain_state_begin_block{endpoint,server}
fleet_router_upstream_chain_state_end_block{endpoint,server}
fleet_router_upstream_finality_data_begin_block{endpoint,server}
fleet_router_upstream_finality_data_end_block{endpoint,server}
fleet_router_upstream_block_failures_total{endpoint,server}
fleet_router_upstream_quarantined{endpoint,server}
fleet_router_upstream_quarantine_block{endpoint,server}
```

`fleet_router_up` is a process gauge and has no labels. Label values escape `\`, `"`, and newlines per the Prometheus text format.

### SHiP ranges

`trace_begin_block`, `chain_state_begin_block`, and `finality_data_begin_block` are the first block of each SHiP range on that upstream. The gauges are always emitted. `0` is the value before a status result arrives. `finality_data_begin_block` (and `finality_data_end_block`) stay `0` on pre-Savanna nodes that only answer `get_status_result_v0` and never report a finality range.

### Block failures and quarantine

`fleet_router_upstream_block_failures_total` counts unclean mid-`get_blocks` drops (the socket died with no WebSocket close frame) while a blocks request was in flight.

`fleet_router_upstream_quarantined` is `1` while that upstream is excluded for any block range. `fleet_router_upstream_quarantine_block` is the start block of that exclusion, or `0` when none is active; if several ranges are active it is the lowest start block. The block number is the gauge value, not a label.

See [Corrupted SHiP logs](operations.md#corrupted-ship-logs) for when these move.

### Operator overrides

`fleet_router_upstream_priority` and `fleet_router_upstream_weight` report the values in effect. They differ from the config file after an operator changes them through the [control API](api.md#control-endpoints). `fleet_router_upstream_operator_disabled` is `1` while an operator has disabled or drained that upstream. An operator-disabled upstream does not count toward `/ready`.
