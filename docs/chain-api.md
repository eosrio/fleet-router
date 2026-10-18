# Chain API

A listener can open an HTTP port that answers like a nodeos chain API, in front of the listener's servers. Clients use one URL for `/v1/chain`: a block lookup goes to the frozen range server or live node that holds the block, and every other call goes to live nodes. The router never rewrites an answer body, and never writes a parameter error or an Unknown Block itself.

The port is off unless the listener has [`chain_api`](configuration.md#chain-api). It is separate from the SHiP port and from the [dashboard and control API](api.md) on `metrics_port`.

- [Setup](#setup)
- [Endpoints](#endpoints)
- [Block requests](#block-requests)
- [get_info](#get_info)
- [State calls](#state-calls)
- [Exec and push](#exec-and-push)
- [get_supported_apis, OPTIONS and CORS](#get_supported_apis-options-and-cors)
- [How requests are forwarded](#how-requests-are-forwarded)
- [Upstream APIs](#upstream-apis)
- [What a frozen range server's API must provide](#what-a-frozen-range-servers-api-must-provide)
- [Limits and timeouts](#limits-and-timeouts)
- [Errors the router writes](#errors-the-router-writes)
- [Differences from one nodeos](#differences-from-one-nodeos)
- [Monitoring](#monitoring)

## Setup

1. Give each server its HTTP API address as [`api_endpoint`](configuration.md#upstream-servers): a live server's nodeos `http-server-address`, a frozen range server's chain API address.
2. Add `chain_api` to the listener:

```json
"chain_api": { "listen_port": 18888, "listen_address": "10.0.0.5" },
"servers": [
  { "name": "archive-0a", "endpoint": "10.0.3.10:8080", "enabled": true, "role": "frozen",
    "range": { "begin": 2, "end": 50000001 }, "api_endpoint": "http://10.0.3.10:8890" },
  { "name": "live-1", "endpoint": "10.0.1.10:8080", "enabled": true, "api_endpoint": "http://10.0.1.10:8888" }
]
```

3. Run `fleet-router config test <path>`. It prints each chain API listener's servers with their role and `api_endpoint`, and with `--strict` it exits 1 when archived blocks have no enabled frozen server with an `api_endpoint` covering them. A frozen server without a block log has no chain API; its replica's API covers its blocks.

A chain API works in pool and stitched listeners alike. Without a live server API, only blocks that a frozen range server holds can be answered.

## Endpoints

| Request | Answered by |
| --- | --- |
| `/v1/chain/get_block_info`, `/v1/chain/get_block_header`, `/v1/chain/get_raw_block` | Routed by block number to a frozen range server or a live node (`block` class). |
| `/v1/chain/get_block`, `/v1/chain/get_block_header_state` | Routed by block number (`block` class). `get_block` always goes to live nodes. `get_block_header_state` reaches a frozen range server only if the server lists that endpoint. |
| `/v1/chain/get_info` | One live node (`info` class). |
| Any other `/v1/chain/<name>` (lowercase letters, digits and `_`), such as `get_account`, `get_table_rows`, `get_abi` | Live nodes (`state` class). |
| `/v1/chain/compute_transaction`, `/v1/chain/send_read_only_transaction` | Live nodes, only with `exec` in `allow`. |
| `/v1/chain/push_transaction`, `push_transactions`, `send_transaction`, `send_transaction2` | Live nodes, only with `push` in `allow`. |
| `/v1/node/get_supported_apis` | The router. |
| `OPTIONS` on any target | The router: `200 {}`. |
| `/v1/chain/push_block`; a target with a query string; a class not in `allow`; anything else (`/v1/trace_api/*`, `/v1/history/*`, `/v1/producer/*`, `/v1/net/*`, `/v1/archive/*`, ...) | The router: nodeos's `404 Unknown Endpoint`. |
| A target that is not a path (`OPTIONS *`, `GET http://host/v1/...`), or that contains `..` | The router: `400 Illegal request-target`. |

As on nodeos, the target is matched exactly, query string included, and any method reaches the endpoint with its body. The method is forwarded unchanged.

## Block requests

### The block number

The router reads the block number from the body: `block_num_or_id`, or `block_num` for `get_block_info`. nodeos parses bodies with its own JSON reader, which reads some bodies differently from standard JSON (escapes, repeated keys, numbers with exponents). So the router reads a block number only from a body that both read the same way: a standard JSON object with no backslash, no repeated top-level key and no number other than an integer, whose value is a non-negative integer, a string of digits, or a block id as an even number of hex digits, at most 64 (the block number is its first four bytes). For `get_block_header_state` an id must be exactly 64 hex digits. As in nodeos, a number above `u32` is cut to its low 32 bits.

Any other body (empty, malformed, or with an unusual value) is sent unchanged to one live node, chosen as for a [state call](#state-calls), which writes its own answer or error. Such a body never goes to a frozen range server.

### Reversible and irreversible blocks

Block `n` is **irreversible** when it is at or below the fleet LIB, or when a verified frozen range server serves it (a block log holds only irreversible blocks). The fleet LIB is the higher of the listener's SHiP fleet LIB and the highest LIB that an up live node's `get_info` reported, so the region is known at startup before any SHiP link has reported. Every other block is **reversible**.

A **reversible** block goes to live nodes only, because an answer about a block that can still change must come from one node's fork database. Nodes whose head has reached `n` come first, by `priority`, highest head and load; then the others, by head. A node's head includes the blocks the listener's SHiP sessions streamed from it, so a block a client has just received over SHiP is routed at once. The first HTTP answer is final, whatever it says; only a failed connection, a timeout or a 503 moves on to the next node.

An **irreversible** block goes through candidate tiers:

- **F**: verified frozen range servers that are up, list the endpoint and serve `n`;
- **L**: live nodes whose block log holds `n`: `earliest_available_block_num ≤ n ≤ head`;
- **U**: live nodes that do not report `earliest_available_block_num` (EOSIO 2.0 and FIO nodes), with `n ≤ head`;
- **X**: live nodes whose block log begins above `n`. They answer a genuine Unknown Block.

With [`block_source`](configuration.md#chain-api) `"frozen_first"` (the default) the order is F, L, U, X; with `"live_first"` it is L, F, U, X. `get_block` uses L, U, X. Inside a tier, candidates with a free slot come first, then the lower `priority`, then fewer requests in flight per unit of `weight`, then a hash of the block's 4096-block chunk, so nearby blocks go to the same replica and its page cache stays warm. The list is cut to `max_attempts`. When it holds no live node but a live node is a candidate, and `max_attempts` is at least 2, the last place goes to a live node, so that a real nodeos writes the final error. For a request by id, when the list holds no frozen range server whose Unknown Block is the answer (see below) but one is a candidate, a place goes to one too: the last place, or the one before it when the last holds the list's only live node; with a single attempt, that attempt. Otherwise, with `"live_first"`, live nodes could take every place, and a wrong id would get `503` on every retry.

For `get_block_info`, `get_block_header` and `get_raw_block`, a block that a frozen range server claims (it is in its served blocks once verified, in its declared `range` before that) exists. Tier X is then left out, and an Unknown Block is not kept as the answer: when no attempt serves the block, the client gets another kept answer if there is one (a `404`), else `503 No chain API upstream available`, and can retry.

A request **by id** is the exception. A verified frozen range server that serves block `n`, below the end of its declared `range`, looks the id up as its own block `n`, and answers Unknown Block when the ids differ. Block `n` is irreversible, so no upstream holds another block `n`: the id names no block, and that Unknown Block is the answer, as on nodeos. Past the end of its declared `range`, a server may serve blocks of a fork the live nodes left (see [Cut-over from a snapshot](operations.md#cut-over-from-a-snapshot)), so its Unknown Block there is not the answer. By number, an Unknown Block from such a server for a block it serves breaks its contract, so the next candidate is tried as above.

Each attempt's answer decides what happens next:

| Answer | Reversible block | Irreversible block |
| --- | --- | --- |
| The connection failed, or no response head within `upstream_timeout_ms` | Next candidate; the API is down until its next good probe | The same |
| `503` | Next candidate; the API is skipped for 2 s | The same |
| `400` with nodeos's Unknown Block (`error.code` 3100002) | Final | Next candidate; the answer is kept. Final from a verified frozen range server that serves the block below the end of its declared `range`, for a request by id |
| `404` | Final | Next candidate; the answer is kept |
| Anything else | Final | Final |

A `400` or `404` body of up to 64 KiB is read to classify it; a larger one is final. When every attempt is used, the client gets the last kept answer from a live node, else the last kept answer, else `503 No chain API upstream available`. A candidate whose slots are all taken is skipped without using an attempt; when every remaining candidate is full, the request waits up to `upstream_queue_ms` for a slot on the first one. `request_deadline_ms` bounds all attempts together.

## get_info

`get_info` comes from one live node, the primary, so that head, LIB and TAPOS values come from the same node and a client does not see the head step back between calls. The primary is the up live node with the highest head (ties by `priority`, then config order). It stays primary while it is up, until another node's head reaches the primary's head + 2.

The request is forwarded unchanged, method and body included, so the node itself rejects a body that `get_info` does not take. A failed connection, a timeout or a 5xx moves on to the next live node by head while one is left; a 5xx other than 503 from the last one is the answer. With no live node up, or when the last node fails or answers 503, the client gets `503 No live chain API upstream available`. The answer is never changed.

## State calls

A state call, and a block request whose body gives no block number, goes to an up live node that lists the path (or whose list is unknown). Nodes more than [`live_lag_blocks`](configuration.md#limits-and-timeouts) behind the fleet head are left out while any closer node is up. The candidates are ordered by `priority`, then fewer requests in flight per unit of `weight`, then round-robin. A failed connection, a timeout before the response head or a 503 moves on to the next one; any other answer is final. With no candidate, the client gets `503 No live chain API upstream available`, and when every attempt fails, `503 No chain API upstream available`.

## Exec and push

Both classes are off by default (see [Security](operations.md#security)).

- **Exec** (`compute_transaction`, `send_read_only_transaction`) goes to a live node chosen as for a state call. Only a connection that could not be made moves on to the next node. Once the request may have been written, any answer is final, and a connection lost or a timeout gets `503 Upstream connection lost after the request was sent`: the request is never sent twice.
- **Push** goes to the first up server in [`push_servers`](configuration.md#chain-api), in the configured order, or to the `get_info` primary when the list is empty. It is written once. A connection that cannot be made, or a target whose slots stay full for `upstream_queue_ms`, moves on to the next target. A failure after the push may have been written gets `503 Upstream connection lost after the request was sent`, and the push is never resent. A push never goes to a frozen range server. With no target up, the client gets `503 No live chain API upstream available`.

## get_supported_apis, OPTIONS and CORS

`/v1/node/get_supported_apis` is answered by the router: `{"apis":[...]}`, the sorted union of the paths that the up upstream APIs (live, or verified frozen) list and that this port serves. It never lists itself, never lists `push_block`, and never lists a path no upstream lists; with none known it is `{"apis":[]}`.

`OPTIONS` on any legal target is answered `200 {}` by the router.

With [`cors`](configuration.md#chain-api), the router removes every `Access-Control-*` header an upstream sent and sets the configured ones on every answer, its own included. As in nodeos, the `400 Illegal request-target` answer carries none, and `Access-Control-Allow-Methods` is never set. Without `cors`, upstream `Access-Control-*` headers are removed and none are set, as on a nodeos with no CORS configured, so browser clients need `cors`.

## How requests are forwarded

- **Request.** The same method and target, and the body byte for byte. Only the client's `Content-Type`, `Accept` and `User-Agent` headers are forwarded. `Host` is the server's `api_endpoint` as configured (nodeos checks `Host` by default), and `Content-Length` is the body's. A `HEAD` request is forwarded as `HEAD` with `Connection: close`.
- **Answer.** The upstream's status and headers, without hop-by-hop headers and `Date`. The `Server` header passes through. Bodies are streamed as they arrive and never changed. The router's own answers are JSON, with no `Server` and no `Date` header.
- **Connections.** HTTP/1.1 only, with keep-alive on both sides. Up to 32 idle connections are kept to each upstream API, for 5 s each.

## Upstream APIs

The router probes each upstream API every `probe_ms`, with `upstream_timeout_ms` as the timeout. Probes do not take request slots.

- **Live nodes.** `POST /v1/chain/get_info`, for its head, LIB, `earliest_available_block_num` (where reported), `chain_id` and version. `/v1/node/get_supported_apis` when the node comes up and every 60 s; a node that does not answer it is assumed to serve every path. A node's head for routing is the highest of its probed head, its SHiP head and the head of the blocks sessions stream from it.
- **Frozen range servers.** `GET /v1/archive/describe` (see [below](#what-a-frozen-range-servers-api-must-provide)). A frozen range server's API is used only once it is **verified**: the last block it describes is the head its own SHiP endpoint reports, and its `get_block_info` for that block returns the same id. Verification survives SHiP and API outages. It is cleared, with one warning (`chain API serves different blocks than its SHiP endpoint`), when the API reports another chain, a last block that fails the check, or when the SHiP endpoint reports a head other than the verified block. An API never verified is never used. Its served blocks are its exact coverage, which may begin below its SHiP range (a block log from block 1).
- **Up and down.** An API is up after one good probe, and down after two failed probes in a row. A request that fails in transport (the connection, or no response head in time) also takes it down until its next good probe. An API that answers `503` is skipped for 2 s.
- **Chain.** An API that reports a `chain_id` other than the listener's is not used, with one warning, until it reports the right one. The listener's `chain_id` is the pinned one, else the one the server's own SHiP endpoint reported, else the listener's SHiP `chain_id`, else the first one any API reported.
- **Slots.** At most `max_in_flight_per_upstream` requests are in flight to one API; for a frozen range server, at most the limit it describes.
- **Operator changes.** Disabling or draining a server through the [control API](api.md#control-endpoints) also stops chain API requests to it, and its `priority` and `weight` in effect apply here too.

## What a frozen range server's API must provide

The router relies on this behaviour of an archive's chain API:

- `GET /v1/archive/describe` returns its identity and coverage:

  ```json
  {
    "chain_id": "<64 hex>",
    "last": { "num": 50000000, "id": "<64 hex>" },
    "blocks": { "begin": 1, "end": 50000001 },
    "apis": ["/v1/chain/get_block_header", "/v1/chain/get_block_info", "/v1/chain/get_raw_block"],
    "get_block": "off",
    "max_in_flight": 4
  }
  ```

  `last` is the head its SHiP endpoint reports. `blocks` is the half-open range of blocks it serves. `apis` lists the endpoints it serves; without it, none are used. `max_in_flight` caps the requests the router sends it at once.
- For every block in `blocks`, each listed endpoint answers byte for byte as a nodeos node that holds the block. A block outside `blocks` gets nodeos's Unknown Block (`400`, `error.code` 3100002), and so does an id whose first four bytes name a block in `blocks` but that is not that block's id. The router passes that answer on as final.
- An internal failure answers `503`, so the router tries another candidate. Apart from Unknown Block and `404`, any other status is passed to the client as the answer.

The router sends a frozen range server only block requests for blocks in its `blocks`, on endpoints it lists. It never sends it `get_info`, state, exec, push, or a block request without a block number, and never forwards client requests to `/v1/archive/*`.

## Limits and timeouts

| What | Limit | When it is reached |
| --- | --- | --- |
| Open client connections | `max_connections` | Further connections are closed when they are accepted. |
| Request head | `handshake_timeout_ms` (10 s when `0`); at most 64 headers and 16 KiB | Too slow: the connection is closed without an answer. Too large: the HTTP parser's plain error answer. |
| Request body | The same time after the head; at most `max_body_bytes` | Too slow: closed without an answer. Too large: `413`, then the connection is closed. |
| Idle keep-alive connection | `handshake_timeout_ms` | The connection is closed. |
| A client that takes none of its answer | A write that makes no progress for `handshake_timeout_ms` | The connection is closed and the request's slots are freed. |
| Requests at once | `max_in_flight` | `503 Too many requests in flight: <n>` |
| Requests to one upstream API | `max_in_flight_per_upstream` | That candidate is skipped; when every candidate is full, the request waits `upstream_queue_ms` for a slot. |
| Upstream connect | `handshake_timeout_ms` (10 s when `0`) | Next candidate. |
| Upstream response head | `upstream_timeout_ms` per attempt | Next candidate; for an exec or push that may have been written, `503`. |
| Gap inside an upstream's answer body | `upstream_timeout_ms` | The answer is cut off. |
| Whole request | `request_deadline_ms` | No further attempt. |

## Errors the router writes

The router's own answers are JSON in nodeos's error shape, with `Content-Type: application/json`, so a client parses them like any node's answer:

| Status | Body |
| --- | --- |
| `400` | `{"code":400,"message":"Illegal request-target","error":{"code":0,"name":"","what":"","details":[]}}` |
| `404` | `{"code":404,"message":"Not Found","error":{"code":0,"name":"exception","what":"unspecified","details":[{"message":"Unknown Endpoint","file":"beast_http_session.hpp","line_number":186,"method":"handle_request"}]}}` |
| `413` | `{"code":413,"message":"Payload Too Large","error":{"code":413,"name":"Payload Too Large","what":"Request body exceeds <max_body_bytes> bytes","details":[]}}` |
| `503` | `{"code":503,"message":"Busy","error":{"code":503,"name":"Busy","what":"<reason>","details":[]}}` |

The `503` reasons:

- `Too many requests in flight: <n>`: the port is at `max_in_flight` (nodeos's wording).
- `No chain API upstream available`: no upstream could answer. No API was a candidate for the block, or every attempt failed in transport or was busy.
- `No live chain API upstream available`: no live node is up for `get_info`, a state call, exec or push.
- `Upstream connection lost after the request was sent`: an exec or push may have reached the node; it is not sent again.

## Differences from one nodeos

- Any `Host` header is accepted, as with nodeos's `http-validate-host = false`.
- A body over `max_body_bytes` gets `413` with the body above; nodeos sends no such answer.
- `OPTIONS` is always answered by the router, and there are no CORS headers unless `cors` is set.
- The `Server` header comes from whichever upstream answered; the router's own answers have none.
- With no live node up, the router answers `503` where a stopped nodeos would refuse the connection.
- A `#fragment` in the request target is dropped by the HTTP parser before routing, so `/v1/chain/get_info#x` reaches `get_info` where nodeos answers `404`. Fragments never belong in a request target, so clients do not send them.
- `HEAD` gets the upstream's status and headers, `Content-Length` included, and no body. nodeos writes a body after its `HEAD` answer; the router's connection to the upstream is closed instead of reused.
- A request line the HTTP parser rejects gets its plain `400` or `431` answer, not a nodeos-shaped body.

## Monitoring

- **Readiness.** `/ready?scope=api` is `200` when every listener with a chain API accepts connections and has a live node API up on its chain (for a listener without a live server API, a verified frozen range server API that is up). Plain `/ready` never includes the chain API. See [Readiness and coverage](observability.md#readiness-and-coverage).
- **Coverage.** `GET /coverage` has a `chain_api` entry per listener with what the probes last learned about each upstream API.
- **Metrics.** `fleet_router_api_*` per listener and `fleet_router_upstream_api_*` per upstream API; see [Chain API series](observability.md#chain-api-series).
- **Logs.** `chain API listening` at startup; `chain API up` and `chain API down` on each change, and warnings for a wrong `chain_id` or a lost verification. Each request is logged at debug with its class, status, upstream, attempts and time. Request bodies are never logged.
