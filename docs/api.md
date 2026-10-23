# Status, dashboard, and control API

The dashboard is **off by default**. Turn it on with [`dashboard.enabled`](configuration.md#dashboard), which needs `metrics_port`. The health/metrics endpoint then also serves:

| Path | What |
| --- | --- |
| `/ui/` | Live dashboard (browser). `/` redirects here. |
| `/status` | Plain status page: server-rendered HTML, no JavaScript. |
| `/api/v1/...` | JSON API used by both pages. Status reads are open; control and configuration endpoints need a token. |
| `/health`, `/ready`, `/ready/<listener>`, `/coverage`, `/metrics` | Unchanged. See [Observability](observability.md). |

The endpoint has the same exposure as `/metrics`: keep it on a trusted network. The API shows upstream addresses and client IP addresses. See [Security](#security).

- [Conventions](#conventions)
- [Read endpoints](#read-endpoints)
- [Live stream](#live-stream)
- [Control endpoints](#control-endpoints)
- [Configuration endpoints](#configuration-endpoints)
- [Security](#security)

## Conventions

- All responses are JSON (`application/json`) unless stated. Errors use `{"error": "<message>"}` with a 4xx or 5xx status.
- Timestamps are Unix milliseconds (`*_ms`). Durations are seconds (`*_secs`).
- **Listener name.** In single-port mode the only listener is named `default`. In multi-listener mode it is the configured `name`.
- **Upstream id.** An upstream is addressed by its listener name and its configured `endpoint`, percent-encoded as one path segment (`encodeURIComponent("wss://ship.example:443")`).
- **Block ranges** are `{"begin": n, "end": n}` where `end` is one past the last available block, exactly as SHiP reports it. A range is `null` when the upstream has not reported it.

## Read endpoints

### `GET /api/v1/status`

Full router state. The [dashboard stream](#get-apiv1dashboardstream) includes this object as `status`; the compatibility stream sends it as `snapshot`.

```json
{
  "version": "0.8.1",
  "now_ms": 1760700000000,
  "started_ms": 1760690000000,
  "uptime_secs": 10000,
  "control": { "enabled": true },
  "config": { "revision": 1, "draining": [] },
  "totals": { "sessions": 12, "upstreams": 3, "upstreams_up": 2 },
  "listeners": [
    {
      "name": "eos",
      "port": 17000,
      "chain_id": "aca376f2...",
      "ready": true,
      "chain_api": {
        "listen": "127.0.0.1:18888",
        "listening": true,
        "ready": true,
        "usable_upstreams": 1,
        "upstreams_total": 1,
        "upstreams": [
          { "ship_endpoint": "10.0.1.10:8080", "api_endpoint": "10.0.1.10:8888", "role": "live", "status": "up" }
        ]
      },
      "max_connections": 10000,
      "sessions": 12,
      "accepted_total": 340,
      "rejected_total": 0,
      "upstreams": [
        {
          "listener": "eos",
          "endpoint": "10.0.1.10:8080",
          "name": "EOS SHiP 1",
          "tls": false,
          "status": "up",
          "online": true,
          "stale": false,
          "operator_disabled": false,
          "priority": 0,
          "weight": 2,
          "config": { "priority": 0, "weight": 2 },
          "chain_id": "aca376f2...",
          "trace": { "begin": 2, "end": 412345678 },
          "chain_state": { "begin": 2, "end": 412345678 },
          "finality": null,
          "head_block": 412345677,
          "chain_head_block": 412345677,
          "head_sample_age_ms": 235,
          "head_lag_blocks": 0,
          "sessions": 8,
          "sessions_total": 210,
          "failovers_total": 3,
          "block_failures_total": 0,
          "blocks_total": 98765432,
          "bytes_total": 123456789012,
          "quarantines": [
            { "block": 400000000, "range": 1, "expires_in_secs": 540 }
          ]
        }
      ]
    }
  ]
}
```

`config.revision` increases on each configuration apply and starts at 1 on process startup. `config.draining` lists retired listener generations as `{listener, generation, sessions, chain_api_connections}` until their established connections finish. `totals.sessions` includes those SHiP sessions; `listeners` describes the currently accepting topology. Session objects add `config_generation` and `draining`; moving a draining session uses its retained upstream pool. Listener/upstream controls target the current generation. Health, readiness and Prometheus topology describe the current generation; use the draining list and session API to inspect retired connections.

`status` is one word for display, chosen in this order:

| `status` | Meaning |
| --- | --- |
| `disabled` | An operator disabled or drained it through the control API. Existing sessions stay unless it was drained. |
| `down` | Offline: unreachable, failed its status deadline, or rejected for `chain_id`. |
| `stale` | Online but its chain state stopped advancing. Still routable, ranked lower. |
| `quarantined` | Online, with at least one active block-range quarantine. Routable outside those ranges. |
| `up` | Online and advancing. |

`priority` and `weight` are the values in effect. `config` holds the values from the config file, so a client can show when an operator changed them at runtime. An upstream's `sessions` is the number of connections sessions hold to it now; in a stitched listener a session can also hold a pre-dialed standby link to the next range's upstream. A listener's `sessions` and `totals.sessions` count client sessions. `sessions_total` counts every session it has been assigned since start, including failovers to it. `failovers_total` counts sessions that left it through failover: the stream ended, a request was rerouted to an upstream that serves it better, or an operator moved or drained it. `blocks_total` and `bytes_total` count block frames and bytes forwarded from it to clients.

A listener's `ready` is what [`/ready/<listener>`](observability.md#readiness-and-coverage) answers: a stitched listener needs an online live upstream.

`chain_api` is `null` when the listener has no chain API. Otherwise, its `ready` matches [`/ready/<listener>?scope=api`](observability.md#readiness-and-coverage): the HTTP listener must be listening and have a usable upstream API. `usable_upstreams` counts live APIs that are up and verified frozen APIs that are up. `upstreams_total` counts configured upstream APIs. Each upstream entry links an API endpoint to its SHiP endpoint. Its `status` is `up`, `down` (including before the first successful probe), `disabled` (operator disabled), `chain_mismatch`, or `unverified` (a frozen API that has not been verified against SHiP). SHiP `ready` and chain API `ready` are independent.

`head_block` is the last block in its trace or chain-state range (`max(end) - 1`), or `null` before the first status reply.

`chain_head_block` is the chain head reported by SHiP, independent of the end of
its history logs. `head_sample_age_ms` is the age of that status sample in
milliseconds. Both are `null` before the first accepted status reply. The
dashboard's Head column displays this chain head; trace, chain-state and finality
ranges retain their exact reported bounds.

`head_lag_blocks` compares a fresh status reply with the highest online live
peer chain head already known before that request was sent. It is a conservative
lower bound: differences smaller than one status polling interval may not be
observed. It is `null` for offline or frozen upstreams and before a comparison
is available. The dashboard uses it for both the lag label and the right-hand
graph marker; subtracting independently
sampled `head_block` values can falsely alternate which synchronized node is
behind after a restart. The left-hand graph shows the sampled history ranges
(including finality and quarantines); the right-hand scale shows measured lag
from 60 blocks behind to "at head". An unavailable comparison has no head marker.

After upgrading the router, reload an already-open dashboard tab to load the new
JavaScript. Reconnecting its live data stream does not replace code already
loaded in the browser.

### `GET /api/v1/sessions`

Active client sessions, oldest first. Optional `?listener=<name>` and `?upstream=<endpoint>` filters.

```json
{
  "sessions": [
    {
      "id": 41,
      "listener": "eos",
      "client": "10.0.5.20:51234",
      "upstream": "10.0.1.10:8080",
      "upstream_name": "EOS SHiP 1",
      "started_ms": 1760699000000,
      "start_block": 412000000,
      "last_block": 412345670,
      "finality": false,
      "blocks": 345670,
      "bytes": 1234567890,
      "failovers": 1
    }
  ]
}
```

`start_block` and `last_block` are `null` until the client sends a `get_blocks` request or receives its first block. `finality` is true when the request asked for finality data. `upstream` and `upstream_name` are empty while the session has no upstream link: between a failover and the next link, and in a stitched listener while it holds, waits or has only asked for status.

### `GET /api/v1/history`

Samples taken every second for the last 15 minutes (at most 900 points), oldest first. Use them to draw charts as soon as the page loads; the live stream adds one sample per second after that.

```json
{
  "interval_ms": 1000,
  "samples": [
    {
      "t_ms": 1760699100000,
      "upstreams": {
        "eos|10.0.1.10:8080": { "sessions": 8, "head_block": 412345000, "blocks_total": 98760000, "bytes_total": 123400000000, "status": "up" }
      }
    }
  ]
}
```

Keys of `upstreams` are `<listener>|<endpoint>`. Rates (blocks/s, bytes/s) are the difference of two `*_total` values divided by the time between them.

### `GET /api/v1/events`

Recent events, newest last (up to 500 kept in memory). Optional `?after=<id>` returns only newer events.

```json
{
  "events": [
    {
      "id": 812,
      "t_ms": 1760699900000,
      "level": "warn",
      "kind": "upstream_down",
      "listener": "eos",
      "upstream": "10.0.1.11:8080",
      "session": null,
      "message": "EOS SHiP 2 went offline"
    }
  ]
}
```

`level` is `info`, `warn`, or `error`. `kind` is one of:

| `kind` | When |
| --- | --- |
| `started` | Router started. |
| `upstream_up`, `upstream_down` | Upstream came online or went offline. |
| `upstream_stale`, `upstream_fresh` | Upstream stopped or resumed advancing. |
| `upstream_quarantined` | A block-range quarantine was armed. |
| `session_failover` | A session moved to another upstream. `upstream` is the new upstream; `message` names both and says why. |
| `session_failed` | A session was closed because no upstream could serve it (for example close code 1011). |
| `control` | An operator action through the control API (`message` describes it). |

## Live stream

### `GET /api/v1/dashboard/stream`

The browser uses one persistent server-sent event connection for all dashboard
reads. It does not poll status or sessions, fetch history separately, or make
additional reads after control actions. Commands still use authenticated HTTP.

Each SSE event is named `dashboard`. Its `id` and JSON `revision` increase within
one router process. The first event on **every connection**, including reconnects,
is a complete bootstrap:

```text
event: dashboard
id: 42
data: {"revision":42,"bootstrap":true,"status":{...},"sessions":[...],"events":[...],"history":{"interval_ms":1000,"samples":[...]}}
```

Subsequent frames contain the full `status` and `sessions` views plus new `events`.
Once per second they also carry a `sample` in the same format as one history
entry. Lifecycle changes wake the shared publisher, coalesced to at most four
publications per second; extra lifecycle frames do not add chart samples.

Apply all fields before rendering any panel. Session counts in `status` and
rows in `sessions` use the same registry snapshot. Upstream connection-slot
counts retain their separate meaning. This is an atomic dashboard publication;
upstream SHiP observations still have their own sample times, exposed through
`head_sample_age_ms`.

The publisher retries capture when a lifecycle mutation overlaps it. Control
changes and session assignments are coordinated with their audit events, and
the event watermark is taken only after accepting the state capture. Forwarded
block/byte counters remain independently sampled; they do not acquire the
lifecycle guard. The browser validates event fields before replacing usable
state, so malformed frames trigger resync without consuming their revision.

The router builds and encodes live frames once for all viewers. A slow stream
that falls behind the eight-frame queue gets another bootstrap, containing up
to 900 history samples and 500 events, instead of an unbounded backlog. Bootstrap
replaces retained history and events. The browser skips duplicate or older
revisions; an unexpected revision gap triggers reconnect and resync. A changed
`status.started_ms` identifies a process restart and resets event IDs and rates.
`Last-Event-ID` does not request replay: every reconnect receives a bootstrap.

Both stream routes share a 32-connection limit. Further connections receive 503.
Keepalive comments are sent every 15 seconds. The UI marks the connection live
only after applying a complete frame. Reload an already-open tab after upgrading
to load the new client code.

### `GET /api/v1/stream`

Compatibility stream for existing API consumers and older dashboard tabs.
[Server-sent events](https://developer.mozilla.org/docs/Web/API/Server-sent_events). Open it with `new EventSource("/api/v1/stream")`. No token is needed.

| SSE `event:` | `data:` | When |
| --- | --- | --- |
| `snapshot` | The `GET /api/v1/status` object | Once on connect, then every second. |
| `event` | One event object from `GET /api/v1/events` | As it happens. |

A `: keepalive` comment is sent every 15 seconds. On reconnect, call `GET /api/v1/events?after=<last id>` to fill any gap.

## Control endpoints

These routing controls change runtime state. **They do not write the config file** and are lost on restart. Configuration save/reload is a separate operation below. Unchanged listener generations retain routing overrides on reload; replacements take configured priority and weight while retaining operator-disabled state for matching SHiP endpoints under the same chain policy.

Every control request needs `Authorization: Bearer <token>`. The token comes from `dashboard.control_token_file`, `dashboard.control_token`, or the `FLEET_ROUTER_CONTROL_TOKEN` environment variable (see [Dashboard](configuration.md#dashboard)). Without a configured token, the control API is off: these endpoints return `403` and `status.control.enabled` is `false`. A wrong or missing token returns `401`.

Every successful action is recorded as a `control` event and logged.

| Method and path | Body | Effect |
| --- | --- | --- |
| `GET /api/v1/auth` | | `204` if the token is valid. Lets a client check a token before showing controls. |
| `POST /api/v1/listeners/{listener}/upstreams/{endpoint}/disable` | | Stop routing **new** sessions to it. Current sessions stay. The monitor keeps polling it. |
| `POST /api/v1/listeners/{listener}/upstreams/{endpoint}/drain` | | Disable it and move its current sessions to other upstreams with the normal failover (resume at the next block, no client reconnect). A session moves only when another upstream can take it: online, below its `max_sessions`, not quarantined at the session's next block, and with a trace range that starts at or before that block (a frozen range server's range must contain it). Sessions that no other upstream can serve (for example readers of old history when draining the only archive node) stay where they are, and the `control` event says how many stayed. |
| `POST /api/v1/listeners/{listener}/upstreams/{endpoint}/enable` | | Route to it again. |
| `PATCH /api/v1/listeners/{listener}/upstreams/{endpoint}` | `{"priority": 5, "weight": 2}` (either field) | Change priority or weight. `weight` must be at least 1. Applies to new sessions and failovers. |
| `POST /api/v1/listeners/{listener}/upstreams/{endpoint}/reset` | | Restore `priority`, `weight`, and enabled state from the config. |
| `POST /api/v1/listeners/{listener}/upstreams/{endpoint}/clear-quarantine` | | Remove its active block-range quarantines. |
| `POST /api/v1/sessions/{id}/move` | | Move one session to another upstream with the normal failover. Returns `409` when no other upstream can take it, by the same rule as a drain. |
| `POST /api/v1/sessions/{id}/close` | | Close one session with WebSocket close code 1000 and reason `closed by operator`. |

Upstream actions return the updated upstream object (same shape as in `status`). Session actions return `202` with `{"ok": true}`; the move or close happens asynchronously and shows up as an event.

A moved session avoids the upstream it left until its next link is up. If no other upstream can take it after all (the target filled up or failed meanwhile), it returns to the upstream it left instead of closing. A session whose dial lands on an upstream an operator disabled meanwhile goes to another upstream when one can take it. Disabling, draining and changing `priority` or `weight` also apply to the listener's [chain API](chain-api.md).

Upstreams disabled in the config file (`"enabled": false`) are not monitored and cannot be enabled by the routing controls. Change the file or use the configuration editor and apply it to enable them.

## Configuration endpoints

All four endpoints require the same Bearer token as the routing controls, including the configuration read. They operate only on the file selected at startup; the client cannot choose a filesystem path.

| Method and path | Body | Effect |
| --- | --- | --- |
| `GET /api/v1/config` | | Read the file, with its inline control token redacted. |
| `POST /api/v1/config/validate` | Revision checks, optional `source` | Validate a draft or the current file, including reservation of new listening ports. Does not write or apply. |
| `POST /api/v1/config/reload` | Revision checks, no `source` | Apply the current file without writing it. |
| `PUT /api/v1/config` | Revision checks and `source` | Validate, atomically replace the file, and apply. |

The read response is `{path, revision, disk_revision, source, disk_error, disk_matches_running, warnings}`. `source` is a JSON string; `disk_revision` is an opaque file-content version. If the file is malformed JSON, `disk_error` explains the error and `source` shows the running configuration so the editor can repair it. The inline token is replaced with `__FLEET_ROUTER_KEEP_SECRET__`; submitting that marker preserves the token. Token files and environment values are never returned.

Send both versions from the read response with every mutation or validation:

```json
{
  "expected_revision": 3,
  "expected_disk_revision": "opaque value from GET",
  "source": "{\"listen_address\":\"127.0.0.1\", ...}"
}
```

The abbreviated `source` above represents the complete configuration JSON, not a patch. Omit it for reload. Validation returns `{listeners, upstreams, replaced_listeners, warnings}`. A successful apply returns `{applied: true, revision, disk_revision, summary, source, access_changed}`; `source` remains redacted and `summary` has the validation shape. Dashboard state then publishes the new configuration revision over the shared event stream.

Stale runtime/file versions return `409`. Invalid configuration or resource preparation returns `400`; drafts over 1 MiB are refused. A rejected transaction leaves runtime state unchanged, and a rejected save leaves the file unchanged. A successful save uses a synced temporary file and atomic rename in the same directory, retaining file permissions. Apply continues if the requesting HTTP client disconnects, so reconnect and read the revision after a lost response. External editors should use atomic replacement and avoid writing during an apply; revision checks cannot lock an uncooperative external writer.

Topology changes preserve established SHiP and chain API connections in their previous listener generations. They continue using the old pools, including failover, until they close. See [reload behavior and filesystem requirements](configuration.md#reloading-configuration). Changing access settings may require a new address or token. Open event streams recheck enabled/hostname policy on updates and close when access is revoked.

## Security

- Nothing here is served unless `dashboard.enabled` is `true`. Upgrading does not expose new data on an existing `metrics_port`.
- Once enabled, status read endpoints, the dashboard and the live stream need no token, like `/metrics`, and they show client IP addresses. The configuration read requires a token. Bind `metrics_address` to a private interface, or put the port behind a reverse proxy with access control.
- Dashboard routes only answer requests addressed by IP, `localhost`, or a name listed in `dashboard.allowed_hosts`, so a page on another domain that rebinds its DNS to this router cannot read the API.
- At most 32 live streams are open at once (`503` beyond that), and every HTTP connection serves one request, so the dashboard cannot crowd out health and readiness probes.
- Control endpoints accept only the `Authorization` header (no cookies or query strings), so a cross-site page cannot send them on a visitor's behalf.
- The token is compared in constant time and must be at least 16 characters. Prefer `dashboard.control_token_file` or the `FLEET_ROUTER_CONTROL_TOKEN` environment variable to keep it out of the config file.
- The router logs a warning at startup when the control API is enabled on a non-loopback address.
