use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::sync::futures::Notified;
use tokio::time::{sleep, sleep_until, timeout, Instant};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{self, Message},
    MaybeTlsStream, WebSocketStream,
};
use tungstenite::protocol::WebSocketConfig;

use crate::abi::{accept_upstream_abi, AbiEntry};
use crate::fleet::FleetHub;
use crate::models::{Role, Server, ServerState, ServerStateDb, StaticConfig};
use crate::ship::BlockPos;
use crate::zcd;
use crate::zcd::ZCDValues;

/// Number of consecutive monitoring intervals without chain-state advancement
/// before an upstream is flagged stale (and deprioritized for routing).
const STALE_THRESHOLD: u32 = 12;

/// `get_status_request_v0` — empty body, variant index 0.
const STATUS_REQUEST_V0: u8 = 0;
/// `get_status_request_v1` — empty body, variant index 4.
/// Spring answers with `get_status_result_v1`, which carries the finality-data range.
const STATUS_REQUEST_V1: u8 = 4;
/// Unanswered v1 probes before the monitor sticks to v0 for this upstream.
const STATUS_V1_MISS_LIMIT: u32 = 2;

/// Periodically log per-upstream block progress and flag stale upstreams.
pub async fn state_monitoring_loop(
    hub: Arc<FleetHub>,
    interval_ms: u64,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut tracker = ProgressTracker::default();

    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = sleep(Duration::from_millis(interval_ms)) => {}
        }

        let mut db = hub.state.lock().await;
        let updated = tracker.tick(&mut db);
        // Publishes stale flips. An unchanged view is not republished.
        hub.rebuild(&db);

        if updated > 0 {
            tracing::debug!(last_block = ?tracker.last_block, "upstream block progress");
        }
    }
    tracing::debug!("state monitoring loop stopped");
}

/// Chain-state progress between monitoring intervals, per upstream.
#[derive(Debug, Default)]
struct ProgressTracker {
    last_block: HashMap<String, u32>,
    stale_counter: HashMap<String, u32>,
}

impl ProgressTracker {
    /// Run one monitoring interval over `db`. Returns how many upstreams
    /// advanced since the previous interval.
    ///
    /// A live upstream whose `chain_state_end_block` has not grown for
    /// [`STALE_THRESHOLD`] intervals is flagged stale. A frozen upstream never
    /// advances by design, so it is never stale.
    fn tick(&mut self, db: &mut HashMap<String, ServerState>) -> usize {
        let mut updated = 0;
        for (server, state) in db.iter_mut() {
            let last = self.last_block.entry(server.clone()).or_insert(0);
            let counter = self.stale_counter.entry(server.clone()).or_insert(0);

            if state.role == Role::Frozen {
                *last = state.chain_state_end_block;
                *counter = 0;
                state.stale = false;
                continue;
            }

            if state.chain_state_end_block > *last {
                *last = state.chain_state_end_block;
                *counter = 0;
                updated += 1;
                if state.stale {
                    state.stale = false;
                    tracing::info!(upstream = %server, "upstream resumed advancing");
                }
            } else {
                *counter = counter.saturating_add(1);
                if *counter == STALE_THRESHOLD && !state.stale {
                    state.stale = true;
                    tracing::warn!(
                        upstream = %server,
                        intervals = STALE_THRESHOLD,
                        "upstream not advancing; flagged stale and deprioritized"
                    );
                }
            }

            if state.stale && !state.warned.looks_frozen && looks_frozen(state) {
                state.warned.looks_frozen = true;
                warn_looks_frozen(server, state);
            }
        }
        updated
    }
}

/// A stale live upstream whose head equals its LIB and its last block, which
/// is what an archive upstream reports.
fn looks_frozen(state: &ServerState) -> bool {
    match (state.head, state.lib) {
        (Some(head), Some(lib)) => {
            head.num > 0
                && head.num == lib.num
                && head.num.checked_add(1) == Some(state.reported_end())
        }
        _ => false,
    }
}

#[cfg(test)]
thread_local! {
    static LOOKS_FROZEN_HINTS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

fn warn_looks_frozen(endpoint: &str, state: &ServerState) {
    #[cfg(test)]
    LOOKS_FROZEN_HINTS.with(|count| count.set(count.get().saturating_add(1)));
    tracing::warn!(
        name = %state.name,
        upstream = %endpoint,
        head = state.head.map_or(0, |head| head.num),
        "upstream looks frozen (not advancing, head == LIB == last block); if it serves a fixed archive range, set \"role\": \"frozen\""
    );
}

type WsSink = SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

/// What an inbound monitor frame means for the status-request state machine.
enum Inbound {
    /// ABI accepted; the caller sends the first status request.
    AbiAccepted,
    /// ABI text arrived but could not be loaded. Reconnect.
    AbiRejected,
    /// A status result was decoded (the in-flight request is done).
    /// The payload is the result variant: 0 = v0, 3 = v1.
    Status(u8),
    /// Upstream sent a close frame (nodeos does this when it shuts down
    /// cleanly). Mark it offline and reconnect, so it comes back online after
    /// a restart.
    Close,
    Ignore,
}

/// How [`run_monitor_session`] ended.
enum SessionEnd {
    /// Shutdown.
    Stop,
    /// Reconnect. `unanswered_v1` means a v1 request was sent and no status
    /// result arrived. `saw_status` resets the miss counter.
    Reconnect {
        unanswered_v1: bool,
        saw_status: bool,
    },
}

fn session_reconnect(sent_v1: bool, saw_status: bool) -> SessionEnd {
    SessionEnd::Reconnect {
        unanswered_v1: sent_v1 && !saw_status,
        saw_status,
    }
}

/// WebSocket limits for an upstream monitor (and `config test`) connection.
/// Without this, tungstenite defaults to a 64 MiB message cap and no caller-supplied bound.
pub(crate) fn upstream_ws_config(max_message_bytes: usize) -> WebSocketConfig {
    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(max_message_bytes);
    config.max_frame_size = Some(max_message_bytes);
    config
}

async fn mark_offline(server_state_db: &ServerStateDb, endpoint: &str) {
    if let Some(state) = server_state_db.lock().await.get_mut(endpoint) {
        state.mark_offline(std::time::Instant::now());
    }
}

/// [`mark_offline`], then publish the change to the listener's fleet view.
async fn mark_offline_published(hub: &FleetHub, endpoint: &str) {
    mark_offline(&hub.state, endpoint).await;
    hub.refresh().await;
}

/// Record that the monitor now polls this upstream with
/// `get_status_request_v0` only.
async fn mark_status_v0_only(server_state_db: &ServerStateDb, endpoint: &str) {
    if let Some(state) = server_state_db.lock().await.get_mut(endpoint) {
        state.status_v1 = Some(false);
    }
}

/// Store the handshake ABI the monitor just accepted.
///
/// Warns when the ABI's request or result variants do not line up with
/// Spring's, once per fingerprint. Warns when the fingerprint differs from
/// the one most other upstreams in this pool sent, once per fingerprint
/// change. Neither is a rejection.
async fn store_abi(server_state_db: &ServerStateDb, server: &Server, entry: AbiEntry) {
    let mut db = server_state_db.lock().await;
    let pool_fp = pool_abi_fingerprint(&db, &server.endpoint);
    let Some(state) = db.get_mut(&server.endpoint) else {
        return;
    };
    let changed = state.abi.as_ref().map(|abi| abi.fp) != Some(entry.fp);
    if changed && !entry.compatible {
        tracing::warn!(
            name = %server.name,
            upstream = %server.endpoint,
            fingerprint = %entry.fp_hex(),
            "upstream ABI request/result variants are not a prefix of Spring's; clients may misread its messages"
        );
    }
    match pool_fp {
        Some(pool) if pool != entry.fp => {
            if state.warned.abi_fp != Some(entry.fp) {
                state.warned.abi_fp = Some(entry.fp);
                tracing::warn!(
                    name = %server.name,
                    upstream = %server.endpoint,
                    fingerprint = %entry.fp_hex(),
                    pool_fingerprint = %format!("{pool:016x}"),
                    "upstream ABI differs from the ABI most of this pool sent"
                );
            }
        }
        _ => state.warned.abi_fp = None,
    }
    tracing::debug!(
        upstream = %server.endpoint,
        fingerprint = %entry.fp_hex(),
        compatible = entry.compatible,
        "upstream ABI recorded"
    );
    state.abi = Some(Arc::new(entry));
}

/// The ABI fingerprint most other upstreams of the pool sent. Ties go to the
/// lowest server index.
fn pool_abi_fingerprint(db: &HashMap<String, ServerState>, endpoint: &str) -> Option<u64> {
    let mut counts: HashMap<u64, (usize, usize)> = HashMap::new();
    for (other, state) in db {
        if other == endpoint {
            continue;
        }
        if let Some(abi) = &state.abi {
            let slot = counts.entry(abi.fp).or_insert((0, usize::MAX));
            slot.0 += 1;
            slot.1 = slot.1.min(state.index);
        }
    }
    counts
        .into_iter()
        .max_by(|(_, a), (_, b)| a.0.cmp(&b.0).then(b.1.cmp(&a.1)))
        .map(|(fp, _)| fp)
}

fn backoff_delay(reconnect_ms: u64, attempts: &mut u32) -> u64 {
    const MAX_BACKOFF_MS: u64 = 30_000;
    let backoff = reconnect_ms
        .saturating_mul(1u64 << (*attempts).min(5))
        .min(MAX_BACKOFF_MS);
    *attempts = attempts.saturating_add(1);
    backoff
}

/// Wait out a reconnect backoff of `delay` after a failed connect. A client
/// session that dials the upstream successfully meanwhile (`reachable`)
/// cuts it short: the monitor then reconnects `floor` (the base reconnect
/// interval) after the failure at the latest, so it never lags far behind
/// the sessions once the upstream is back. Returns `false` on shutdown.
async fn reconnect_pause(
    delay: Duration,
    floor: Duration,
    reachable: Pin<&mut Notified<'_>>,
    shutdown: &mut broadcast::Receiver<()>,
    endpoint: &str,
) -> bool {
    let failed = Instant::now();
    tokio::select! {
        _ = shutdown.recv() => return false,
        _ = sleep(delay) => return true,
        () = reachable => {}
    }
    tracing::info!(
        upstream = %endpoint,
        "a client session reached the upstream; reconnecting the monitor"
    );
    tokio::select! {
        _ = shutdown.recv() => false,
        _ = sleep_until(failed + floor.min(delay)) => true,
    }
}

async fn send_status(sender: &mut WsSink, endpoint: &str, variant: u8) -> bool {
    if let Err(e) = sender.send(Message::Binary(vec![variant].into())).await {
        tracing::warn!(upstream = %endpoint, error = %e, "failed to send status request");
        return false;
    }
    true
}

/// Maintain a monitoring connection to a single upstream: connect, poll status,
/// keep its `ServerState` up to date, and reconnect with capped backoff.
///
/// Spring 1.2.3 terminates a session that has more than 100 `get_status_request`s
/// queued. Exactly one status request is left in flight, and it must be answered
/// within `upstream_status_ms` or the upstream is marked offline and the session
/// is dropped. Connects and the wait for the ABI frame are bounded, and the
/// socket uses the configured message size.
///
/// The backoff after failed connects grows, but a client session that dials
/// the upstream successfully ends it (see [`reconnect_pause`]): sessions
/// re-dial a returning upstream directly, and the monitor follows within
/// the base reconnect interval.
///
/// Every state change is published to `hub`'s fleet view.
pub async fn monitoring_connection(
    server: Server,
    static_config: StaticConfig,
    hub: Arc<FleetHub>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut backoff_attempts: u32 = 0;
    let mut unanswered_v1_sessions: u32 = 0;
    // Prefer v1 so Savanna nodes report a finality-data range. Fall back to v0
    // when the upstream answers with v0, or never answers a v1 probe.
    let mut status_request = STATUS_REQUEST_V1;
    let connect_timeout = Duration::from_millis(static_config.connect_timeout_ms);
    let reconnect = Duration::from_millis(static_config.upstream_reconnect_ms);
    let counters = hub
        .state
        .lock()
        .await
        .get(&server.endpoint)
        .map(|state| state.counters.clone())
        .unwrap_or_default();

    loop {
        // Registered before the connect attempt, so a session dial that gets
        // through while it fails is not missed.
        let reachable = counters.dial_succeeded.notified();
        tokio::pin!(reachable);
        reachable.as_mut().enable();
        let connect = connect_async_with_config(
            server.ws_url(),
            Some(upstream_ws_config(static_config.max_message_bytes)),
            true,
        );
        let websocket = tokio::select! {
            _ = shutdown.recv() => break,
            res = timeout(connect_timeout, connect) => match res {
                Ok(Ok((websocket, _))) => websocket,
                Ok(Err(e)) => {
                    mark_offline_published(&hub, &server.endpoint).await;
                    let backoff = backoff_delay(static_config.upstream_reconnect_ms, &mut backoff_attempts);
                    tracing::warn!(upstream = %server.endpoint, error = %e, retry_in_ms = backoff, "upstream connect failed");
                    let delay = Duration::from_millis(backoff);
                    if reconnect_pause(delay, reconnect, reachable.as_mut(), &mut shutdown, &server.endpoint).await {
                        continue;
                    }
                    break;
                }
                Err(_) => {
                    mark_offline_published(&hub, &server.endpoint).await;
                    let backoff = backoff_delay(static_config.upstream_reconnect_ms, &mut backoff_attempts);
                    tracing::warn!(
                        upstream = %server.endpoint,
                        timeout_ms = static_config.connect_timeout_ms,
                        retry_in_ms = backoff,
                        "upstream connect timed out"
                    );
                    let delay = Duration::from_millis(backoff);
                    if reconnect_pause(delay, reconnect, reachable.as_mut(), &mut shutdown, &server.endpoint).await {
                        continue;
                    }
                    break;
                }
            }
        };

        backoff_attempts = 0;
        tracing::info!(upstream = %server.endpoint, "monitoring connected to upstream");

        let end = run_monitor_session(
            websocket,
            &server,
            &static_config,
            &hub,
            &mut shutdown,
            &mut status_request,
        )
        .await;
        match end {
            SessionEnd::Stop => break,
            SessionEnd::Reconnect {
                unanswered_v1,
                saw_status,
            } => {
                // A v1 probe that produced no status result usually means the node
                // does not implement `get_status_request_v1` (it closes or ignores
                // the socket). After a couple of misses, stick to v0 so the upstream
                // can still come online. Routing then uses trace ranges.
                if unanswered_v1 && status_request == STATUS_REQUEST_V1 {
                    unanswered_v1_sessions = unanswered_v1_sessions.saturating_add(1);
                    if unanswered_v1_sessions >= STATUS_V1_MISS_LIMIT {
                        status_request = STATUS_REQUEST_V0;
                        mark_status_v0_only(&hub.state, &server.endpoint).await;
                        hub.refresh().await;
                        tracing::info!(
                            upstream = %server.endpoint,
                            "no reply to get_status_request_v1; falling back to get_status_request_v0"
                        );
                    }
                } else if saw_status {
                    unanswered_v1_sessions = 0;
                }
            }
        }

        tracing::info!(
            upstream = %server.endpoint,
            retry_in_ms = static_config.upstream_reconnect_ms,
            "reconnecting to upstream"
        );
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = sleep(Duration::from_millis(static_config.upstream_reconnect_ms)) => {}
        }
    }
    tracing::debug!(upstream = %server.endpoint, "monitoring loop stopped");
}

/// Drive one connected monitor session.
///
/// [`SessionEnd::Stop`] means shutdown. [`SessionEnd::Reconnect`] asks the
/// outer loop to connect again, including after the upstream closed. `status_request` is the variant byte to send; a v0 result
/// switches it to [`STATUS_REQUEST_V0`] for the rest of this upstream.
async fn run_monitor_session(
    websocket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    server: &Server,
    static_config: &StaticConfig,
    hub: &FleetHub,
    shutdown: &mut broadcast::Receiver<()>,
    status_request: &mut u8,
) -> SessionEnd {
    let (mut sender, mut receiver) = websocket.split();
    let mut ship_abi: Option<String> = None;
    let mut abi_ready = false;
    let mut status_in_flight = false;
    let mut sent_v1 = false;
    let mut saw_status = false;
    let reply_timeout = Duration::from_millis(static_config.upstream_status_ms);
    let abi_deadline = Instant::now() + Duration::from_millis(static_config.connect_timeout_ms);
    // Placeholders until the first status request is sent.
    let mut status_deadline = abi_deadline;
    let mut next_status_at = abi_deadline;

    loop {
        let wake_at = if abi_ready {
            if status_in_flight {
                status_deadline
            } else {
                next_status_at
            }
        } else {
            abi_deadline
        };

        tokio::select! {
            biased;
            _ = shutdown.recv() => return SessionEnd::Stop,
            next = receiver.next() => match next {
                Some(Ok(msg)) => {
                    let inbound = handle_monitoring_msg(msg, &mut ship_abi, &hub.state, server).await;
                    // A stored ABI or status (or a chain_id rejection) changed
                    // the state; the other outcomes publish through
                    // mark_offline_published below.
                    if matches!(inbound, Inbound::AbiAccepted | Inbound::Status(_)) {
                        hub.refresh().await;
                    }
                    match inbound {
                        Inbound::AbiAccepted => {
                            abi_ready = true;
                            let variant = *status_request;
                            if !send_status(&mut sender, &server.endpoint, variant).await {
                                mark_offline_published(hub, &server.endpoint).await;
                                return session_reconnect(sent_v1, saw_status);
                            }
                            if variant == STATUS_REQUEST_V1 {
                                sent_v1 = true;
                            }
                            status_in_flight = true;
                            let sent = Instant::now();
                            status_deadline = sent + reply_timeout;
                            next_status_at = sent + reply_timeout;
                        }
                        Inbound::Status(variant) => {
                            status_in_flight = false;
                            saw_status = true;
                            *status_request = if variant == 0 {
                                STATUS_REQUEST_V0
                            } else {
                                STATUS_REQUEST_V1
                            };
                        }
                        Inbound::AbiRejected => {
                            mark_offline_published(hub, &server.endpoint).await;
                            return session_reconnect(sent_v1, saw_status);
                        }
                        Inbound::Close => {
                            mark_offline_published(hub, &server.endpoint).await;
                            // Always reconnect. A node that does not understand
                            // v1 often closes the socket, and the miss counter
                            // then falls back to v0. A node that is restarting
                            // closes it too; stopping here would keep it offline
                            // until the router restarts.
                            return session_reconnect(sent_v1, saw_status);
                        }
                        Inbound::Ignore => {}
                    }
                }
                None => {
                    tracing::info!(upstream = %server.endpoint, "upstream disconnected");
                    mark_offline_published(hub, &server.endpoint).await;
                    return session_reconnect(sent_v1, saw_status);
                }
                Some(Err(e)) => {
                    tracing::warn!(upstream = %server.endpoint, error = %e, "error reading from upstream");
                    mark_offline_published(hub, &server.endpoint).await;
                    return session_reconnect(sent_v1, saw_status);
                }
            },
            _ = sleep_until(wake_at) => {
                if !abi_ready {
                    tracing::warn!(
                        upstream = %server.endpoint,
                        timeout_ms = static_config.connect_timeout_ms,
                        "timed out waiting for upstream ABI; marking upstream offline"
                    );
                    mark_offline_published(hub, &server.endpoint).await;
                    return session_reconnect(sent_v1, saw_status);
                }
                if status_in_flight {
                    tracing::warn!(
                        upstream = %server.endpoint,
                        timeout_ms = static_config.upstream_status_ms,
                        "status reply timed out; marking upstream offline"
                    );
                    mark_offline_published(hub, &server.endpoint).await;
                    return session_reconnect(sent_v1, saw_status);
                }
                let variant = *status_request;
                if !send_status(&mut sender, &server.endpoint, variant).await {
                    mark_offline_published(hub, &server.endpoint).await;
                    return session_reconnect(sent_v1, saw_status);
                }
                if variant == STATUS_REQUEST_V1 {
                    sent_v1 = true;
                }
                status_in_flight = true;
                let sent = Instant::now();
                status_deadline = sent + reply_timeout;
                next_status_at = sent + reply_timeout;
            }
        }
    }
}

async fn handle_monitoring_msg(
    message: Message,
    ship_abi: &mut Option<String>,
    server_state_db: &ServerStateDb,
    server: &Server,
) -> Inbound {
    match message {
        Message::Text(msg) => {
            if ship_abi.is_some() {
                tracing::debug!(upstream = %server.endpoint, "unexpected text message from upstream");
                return Inbound::Ignore;
            }
            let text = msg.to_string();
            // Same check as a client handshake: parse, then drop the handle.
            // `ship_abi` marks the handshake as seen, so a later text frame is
            // ignored. An accepted ABI is also recorded on the upstream state.
            let loaded = accept_upstream_abi(&text);
            if loaded.is_ok() {
                store_abi(server_state_db, server, AbiEntry::new(&text)).await;
            }
            *ship_abi = Some(text);
            match loaded {
                Ok(()) => Inbound::AbiAccepted,
                Err(_) => {
                    tracing::warn!(upstream = %server.endpoint, "error setting ABI for upstream");
                    Inbound::AbiRejected
                }
            }
        }
        Message::Binary(bin_msg) => {
            match apply_status_update(&bin_msg, server_state_db, server).await {
                // `Ok(Some(variant))` is a decoded status result, including head 0.
                // Anything else is not the in-flight reply.
                Ok(Some(variant)) => Inbound::Status(variant),
                Ok(None) => Inbound::Ignore,
                Err(e) => {
                    tracing::warn!(
                        upstream = %server.endpoint,
                        error = %e,
                        "rejected upstream status frame"
                    );
                    Inbound::Ignore
                }
            }
        }
        Message::Close(_) => {
            tracing::info!(upstream = %server.endpoint, "received close from upstream");
            Inbound::Close
        }
        _ => Inbound::Ignore,
    }
}

/// Apply one upstream binary frame to `server`'s monitored state.
///
/// `Ok(Some(variant))` means the frame was `get_status_result_v0` (0) or
/// `get_status_result_v1` (3) and counts as the in-flight status reply. State
/// is updated only when `head_block_num > 0`: the ranges, head and LIB (number
/// and id), `ranges_known`, and which status version the upstream answers. A
/// v0 result clears any previously learned finality range. `Ok(None)` means
/// some other result and was ignored.
/// A truncated or malformed status body leaves the stored state unchanged and
/// returns the decode error; that error is not a valid reply.
///
/// A reported `chain_id` that differs from the pinned value, or from the
/// `chain_id` of another online peer in an unpinned pool, marks the upstream
/// offline. `chain_id` on `get_status_result_v0` is an optional extension that
/// older nodeos builds never send. Omitting it cannot be verified, so the
/// upstream stays online and a warning is logged once until a later status
/// reports a `chain_id`. `require_chain_id` restores the reject-when-missing
/// behaviour for a pinned pool.
pub(crate) async fn apply_status_update(
    bin_msg: &[u8],
    server_state_db: &ServerStateDb,
    server: &Server,
) -> Result<Option<u8>, zcd::ZcdError> {
    let result_message = zcd::deserialize_result(bin_msg);
    let variant = match result_message.get("variant")? {
        Some(ZCDValues::U8(variant)) => variant,
        _ => return Ok(None),
    };
    let Some(ZCDValues::Bytes(bytes)) = result_message.get("data")? else {
        return Err(zcd::ZcdError::MissingField);
    };
    let result = match variant {
        0 => zcd::deserialize_status_result(&bytes)?,
        3 => zcd::deserialize_status_result_v1(&bytes)?,
        _ => return Ok(None),
    };
    let head_block_num = require_u32(&result, "head_block_num")?;
    if head_block_num == 0 {
        return Ok(Some(variant));
    }
    let head = BlockPos {
        num: head_block_num,
        id: require_id(&result, "head_block_id")?,
    };
    let lib = BlockPos {
        num: require_u32(&result, "last_irreversible_block_num")?,
        id: require_id(&result, "last_irreversible_block_id")?,
    };
    let trace_begin = require_u32(&result, "trace_begin_block")?;
    let trace_end = require_u32(&result, "trace_end_block")?;
    let chain_begin = require_u32(&result, "chain_state_begin_block")?;
    let chain_end = require_u32(&result, "chain_state_end_block")?;
    let finality = if variant == 3 {
        Some((
            require_u32(&result, "finality_data_begin_block")?,
            require_u32(&result, "finality_data_end_block")?,
        ))
    } else {
        None
    };
    let reported_chain_id = match result.get("chain_id")? {
        Some(ZCDValues::Checksum256(bytes)) => Some(bytes),
        _ => None,
    };

    let now = std::time::Instant::now();
    let mut db = server_state_db.lock().await;
    // Only currently online peers anchor an unpinned pool's chain_id so an
    // offline node's last reported chain_id cannot lock out active peers.
    let peer_chain_id = db
        .iter()
        .filter(|(ep, s)| *ep != &server.endpoint && s.online)
        .find_map(|(_, s)| s.chain_id);

    if let Some(state) = db.get_mut(&server.endpoint) {
        match reported_chain_id {
            Some(actual) => {
                // A later status that includes chain_id may omit it again; warn once more then.
                state.chain_id_unverified_warned = false;
                if let Some(expected) = state.expected_chain_id {
                    if actual != expected {
                        tracing::warn!(
                            name = %server.name,
                            upstream = %server.endpoint,
                            expected_chain_id = %hex_32(&expected),
                            reported_chain_id = %hex_32(&actual),
                            "upstream chain_id does not match configured chain_id; marking offline"
                        );
                        state.mark_offline(now);
                        return Ok(Some(variant));
                    }
                } else if let Some(pool_cid) = peer_chain_id {
                    if actual != pool_cid {
                        tracing::warn!(
                            name = %server.name,
                            upstream = %server.endpoint,
                            pool_chain_id = %hex_32(&pool_cid),
                            reported_chain_id = %hex_32(&actual),
                            "upstream chain_id mismatches pool chain_id; marking offline"
                        );
                        state.mark_offline(now);
                        return Ok(Some(variant));
                    }
                }
            }
            None => {
                if let Some(expected) = state.expected_chain_id {
                    if state.require_chain_id {
                        tracing::warn!(
                            name = %server.name,
                            upstream = %server.endpoint,
                            expected_chain_id = %hex_32(&expected),
                            "upstream status omitted chain_id while chain_id is pinned; marking offline"
                        );
                        state.mark_offline(now);
                        return Ok(Some(variant));
                    }
                }
                // Older SHiP (EOSIO 2.0 / FIO) never sends the optional extension.
                // Keep the last reported chain_id, if any, so this peer can still
                // anchor an unpinned pool against a later mismatched report.
                if !state.chain_id_unverified_warned {
                    warn_unverified_chain_id(server, state.expected_chain_id);
                    state.chain_id_unverified_warned = true;
                }
            }
        }

        if let Some(cid) = reported_chain_id {
            state.chain_id = Some(cid);
        }
        // An upstream that comes back, or reports another status, may have
        // a block log now: sessions try it for blocks again.
        let reported = (
            (trace_begin, trace_end),
            (chain_begin, chain_end),
            finality.unwrap_or((0, 0)),
            (Some(head), Some(lib)),
        );
        let stored = (
            (state.trace_begin_block, state.trace_end_block),
            (state.chain_state_begin_block, state.chain_state_end_block),
            (
                state.finality_data_begin_block,
                state.finality_data_end_block,
            ),
            (state.head, state.lib),
        );
        if state.no_block_log && (!state.online || reported != stored) {
            state.no_block_log = false;
            tracing::info!(
                name = %server.name,
                upstream = %server.endpoint,
                "upstream reconnected or reported a new status; trying it for fetch_block requests again"
            );
        }
        let previous_end = state.ranges_known.then(|| state.reported_end());
        state.enabled = true;
        state.online = true;
        state.trace_begin_block = trace_begin;
        state.trace_end_block = trace_end;
        state.chain_state_begin_block = chain_begin;
        state.chain_state_end_block = chain_end;
        if let Some((begin, end)) = finality {
            state.finality_data_begin_block = begin;
            state.finality_data_end_block = end;
        } else {
            state.finality_data_begin_block = 0;
            state.finality_data_end_block = 0;
        }
        state.head = Some(head);
        state.lib = Some(lib);
        state.ranges_known = true;
        state.status_at = Some(now);
        // The monitor keeps polling with the variant the upstream answers.
        state.status_v1 = Some(variant == 3);
        state.offline_since = None;
        warn_role_hints(state, server, previous_end);
    }
    Ok(Some(variant))
}

/// One-time hints about a frozen upstream after its ranges were updated.
/// `previous_end` is the reported end before this status, if one was known.
fn warn_role_hints(state: &mut ServerState, server: &Server, previous_end: Option<u32>) {
    if state.role != Role::Frozen {
        return;
    }
    if !state.warned.frozen_advanced
        && previous_end.is_some_and(|previous| state.reported_end() > previous)
    {
        state.warned.frozen_advanced = true;
        tracing::warn!(
            name = %server.name,
            upstream = %server.endpoint,
            from = previous_end.unwrap_or(0),
            to = state.reported_end(),
            "frozen upstream advanced; an archive upstream should serve a fixed range (is it a live node?)"
        );
    }
    let mismatch = state.range_mismatch();
    if mismatch && !state.warned.range_mismatch {
        let declared = state.declared.unwrap_or_default();
        tracing::warn!(
            name = %server.name,
            upstream = %server.endpoint,
            declared_begin = declared.begin,
            declared_end = declared.end,
            trace_begin = state.trace_begin_block,
            trace_end = state.trace_end_block,
            chain_state_begin = state.chain_state_begin_block,
            chain_state_end = state.chain_state_end_block,
            "reported ranges differ from the declared range"
        );
    }
    state.warned.range_mismatch = mismatch;
}

#[cfg(test)]
thread_local! {
    static UNVERIFIED_CHAIN_ID_WARNS: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

fn warn_unverified_chain_id(server: &Server, expected: Option<[u8; 32]>) {
    #[cfg(test)]
    UNVERIFIED_CHAIN_ID_WARNS.with(|count| count.set(count.get().saturating_add(1)));
    match expected {
        Some(expected) => {
            tracing::warn!(
                name = %server.name,
                upstream = %server.endpoint,
                expected_chain_id = %hex_32(&expected),
                "chain_id cannot be verified for this upstream (older SHiP without chain_id extension)"
            );
        }
        None => {
            tracing::warn!(
                name = %server.name,
                upstream = %server.endpoint,
                "chain_id cannot be verified for this upstream (older SHiP without chain_id extension)"
            );
        }
    }
}

pub(crate) fn hex_32(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(64);
    for b in bytes {
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

fn require_u32(result: &zcd::ZCD<'_>, field: &'static str) -> Result<u32, zcd::ZcdError> {
    match result.get(field)? {
        Some(ZCDValues::U32(value)) => Ok(value),
        _ => Err(zcd::ZcdError::Truncated {
            field,
            expected: 4,
            remaining: 0,
        }),
    }
}

fn require_id(result: &zcd::ZCD<'_>, field: &'static str) -> Result<[u8; 32], zcd::ZcdError> {
    match result.get(field)? {
        Some(ZCDValues::Checksum256(value)) => Ok(value),
        _ => Err(zcd::ZcdError::Truncated {
            field,
            expected: 32,
            remaining: 0,
        }),
    }
}

#[cfg(test)]
mod tests {
    use tokio_tungstenite::tungstenite::Message;

    use super::{apply_status_update, handle_monitoring_msg, Inbound};
    use crate::models::{build_state_db, Role, Server};
    use crate::ship::{BlockPos, Range};
    use crate::zcd::ZcdError;

    fn fixture() -> (Server, crate::models::ServerStateDb) {
        let server = Server {
            name: "alpha".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            ..Default::default()
        };
        let db = build_state_db(vec![server.clone()]);
        (server, db)
    }

    fn status_body() -> Vec<u8> {
        let mut buf = Vec::with_capacity(120);
        buf.extend_from_slice(&1000u32.to_le_bytes());
        buf.extend_from_slice(&[0x11; 32]);
        buf.extend_from_slice(&990u32.to_le_bytes());
        buf.extend_from_slice(&[0x22; 32]);
        buf.extend_from_slice(&10u32.to_le_bytes());
        buf.extend_from_slice(&1001u32.to_le_bytes());
        buf.extend_from_slice(&5u32.to_le_bytes());
        buf.extend_from_slice(&1000u32.to_le_bytes());
        buf.extend_from_slice(&[0xAB; 32]);
        buf
    }

    /// Result variant 0 followed by a status body.
    fn status_message(body: &[u8]) -> Vec<u8> {
        let mut frame = vec![0u8];
        frame.extend_from_slice(body);
        frame
    }

    fn status_body_v1(begin: u32, end: u32) -> Vec<u8> {
        let mut buf = status_body();
        buf.extend_from_slice(&begin.to_le_bytes());
        buf.extend_from_slice(&end.to_le_bytes());
        buf
    }

    fn status_message_v1(body: &[u8]) -> Vec<u8> {
        let mut frame = vec![3u8];
        frame.extend_from_slice(body);
        frame
    }

    #[tokio::test]
    async fn valid_status_frame_updates_upstream_state() {
        let (server, db) = fixture();
        assert_eq!(
            apply_status_update(&status_message(&status_body()), &db, &server)
                .await
                .expect("valid status"),
            Some(0),
            "a decoded status result counts as the in-flight reply"
        );
        let guard = db.lock().await;
        let state = guard.get(&server.endpoint).expect("state");
        assert!(state.online);
        assert_eq!(state.trace_begin_block, 10);
        assert_eq!(state.trace_end_block, 1001);
        assert_eq!(state.chain_state_begin_block, 5);
        assert_eq!(state.chain_state_end_block, 1000);
        assert_eq!(state.finality_data_begin_block, 0);
        assert_eq!(state.finality_data_end_block, 0);
    }

    #[tokio::test]
    async fn truncated_status_frame_is_rejected_without_panic() {
        let (server, db) = fixture();
        // Variant byte only: the status body is missing.
        let err = apply_status_update(&[0], &db, &server)
            .await
            .expect_err("truncated");
        assert_eq!(
            err,
            ZcdError::Truncated {
                field: "head_block_num",
                expected: 4,
                remaining: 0,
            }
        );
        let guard = db.lock().await;
        let state = guard.get(&server.endpoint).expect("state");
        assert!(!state.online);
        assert_eq!(state.trace_end_block, 0);
    }

    #[tokio::test]
    async fn malformed_status_frame_is_rejected_without_panic() {
        let (server, db) = fixture();
        // Variant plus three bytes of a four-byte head block number.
        let err = apply_status_update(&[0, 1, 2, 3], &db, &server)
            .await
            .expect_err("malformed");
        assert_eq!(
            err,
            ZcdError::Malformed {
                field: "head_block_num",
                expected: 4,
                actual: 3,
            }
        );
        let guard = db.lock().await;
        let state = guard.get(&server.endpoint).expect("state");
        assert!(!state.online);
        assert_eq!(state.chain_state_end_block, 0);
    }

    #[tokio::test]
    async fn rejected_status_frame_is_not_a_reply() {
        let (server, db) = fixture();
        let mut ship_abi = None;
        let inbound = handle_monitoring_msg(
            Message::Binary(vec![0u8].into()),
            &mut ship_abi,
            &db,
            &server,
        )
        .await;
        assert!(
            matches!(inbound, Inbound::Ignore),
            "a rejected status frame must not clear the in-flight request"
        );
        let guard = db.lock().await;
        assert!(!guard.get(&server.endpoint).expect("state").online);
    }

    #[tokio::test]
    async fn non_status_result_is_not_a_reply() {
        let (server, db) = fixture();
        // Result variant 1 is get_blocks_result_v0, not a status reply.
        let reply = apply_status_update(&[1], &db, &server)
            .await
            .expect("non-status");
        assert_eq!(reply, None);
        let mut ship_abi = None;
        let inbound = handle_monitoring_msg(
            Message::Binary(vec![1u8].into()),
            &mut ship_abi,
            &db,
            &server,
        )
        .await;
        assert!(matches!(inbound, Inbound::Ignore));
    }

    #[tokio::test]
    async fn v1_status_records_finality_range_and_v0_clears_it() {
        let (server, db) = fixture();
        assert_eq!(
            apply_status_update(&status_message_v1(&status_body_v1(10, 800)), &db, &server,)
                .await
                .expect("v1 status"),
            Some(3)
        );
        {
            let guard = db.lock().await;
            let state = guard.get(&server.endpoint).expect("state");
            assert!(state.online);
            assert_eq!(state.trace_begin_block, 10);
            assert_eq!(state.trace_end_block, 1001);
            assert_eq!(state.finality_data_begin_block, 10);
            assert_eq!(state.finality_data_end_block, 800);
        }

        let mut v0 = status_body();
        // trace_end_block sits at bytes 76..80 of the v0 body (4+32+4+32+4).
        v0[76..80].copy_from_slice(&1100u32.to_le_bytes());
        assert_eq!(
            apply_status_update(&status_message(&v0), &db, &server)
                .await
                .expect("v0 status"),
            Some(0)
        );
        let guard = db.lock().await;
        let state = guard.get(&server.endpoint).expect("state");
        assert_eq!(state.trace_end_block, 1100);
        assert_eq!(state.finality_data_begin_block, 0);
        assert_eq!(state.finality_data_end_block, 0);
    }

    #[tokio::test]
    async fn truncated_v1_does_not_clobber_a_known_finality_range() {
        let (server, db) = fixture();
        apply_status_update(&status_message_v1(&status_body_v1(10, 800)), &db, &server)
            .await
            .expect("v1 status");
        let err = apply_status_update(&status_message_v1(&status_body()), &db, &server)
            .await
            .expect_err("truncated v1");
        assert_eq!(
            err,
            ZcdError::Truncated {
                field: "finality_data_begin_block",
                expected: 4,
                remaining: 0,
            }
        );
        let inbound = handle_monitoring_msg(
            Message::Binary(status_message_v1(&status_body()).into()),
            &mut None,
            &db,
            &server,
        )
        .await;
        assert!(
            matches!(inbound, Inbound::Ignore),
            "a truncated v1 frame must not clear the in-flight request"
        );
        let guard = db.lock().await;
        let state = guard.get(&server.endpoint).expect("state");
        assert_eq!(state.trace_end_block, 1001);
        assert_eq!(state.finality_data_begin_block, 10);
        assert_eq!(state.finality_data_end_block, 800);
    }

    fn tiny_abi(action: &str) -> String {
        format!(
            r#"{{"version":"eosio::abi/1.1","actions":[{{"name":"{action}","type":"uint32","ricardian_contract":""}}]}}"#
        )
    }

    /// Monitors used to take the process-wide abieos mutex. Distinct ABIs
    /// accepted at the same time must not change each other's result.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_monitors_accept_their_own_abi() {
        let (server, db) = fixture();
        let mut tasks = Vec::new();
        for i in 0..12 {
            let server = server.clone();
            let db = db.clone();
            let json = if i % 3 == 2 {
                "{".to_string()
            } else if i % 2 == 0 {
                tiny_abi("acta")
            } else {
                tiny_abi("actb")
            };
            let valid = json != "{";
            tasks.push(tokio::spawn(async move {
                let mut ship_abi = None;
                let inbound =
                    handle_monitoring_msg(Message::Text(json.into()), &mut ship_abi, &db, &server)
                        .await;
                (valid, inbound, ship_abi.is_some())
            }));
        }
        for task in tasks {
            let (valid, inbound, saw_text) = task.await.expect("task");
            assert!(saw_text);
            if valid {
                assert!(
                    matches!(inbound, Inbound::AbiAccepted),
                    "a valid ABI must be accepted on its own handle"
                );
            } else {
                assert!(matches!(inbound, Inbound::AbiRejected));
            }
        }
    }

    #[tokio::test]
    async fn pool_chain_id_mismatch_is_rejected_and_marked_offline() {
        let s1 = Server {
            name: "alpha".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            ..Default::default()
        };
        let s2 = Server {
            name: "beta".to_string(),
            endpoint: "127.0.0.1:18081".to_string(),
            enabled: true,
            ..Default::default()
        };
        let db = build_state_db(vec![s1.clone(), s2.clone()]);

        // First server comes online with chain_id [0xAB; 32].
        apply_status_update(&status_message(&status_body()), &db, &s1)
            .await
            .expect("s1 status");
        assert!(db.lock().await.get(&s1.endpoint).unwrap().online);

        // Second server reports a different chain_id ([0xCD; 32]).
        let mut wrong_body = status_body();
        let len = wrong_body.len();
        wrong_body[len - 32..].copy_from_slice(&[0xCD; 32]);
        apply_status_update(&status_message(&wrong_body), &db, &s2)
            .await
            .expect("s2 status");

        let guard = db.lock().await;
        let state2 = guard.get(&s2.endpoint).unwrap();
        assert!(
            !state2.online,
            "mismatched chain_id in same pool must stay offline"
        );
        assert_eq!(state2.chain_state_end_block, 0);
    }

    fn pinned_server(require_chain_id: bool) -> (Server, crate::models::ServerStateDb) {
        let server = Server {
            name: "alpha".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            ..Default::default()
        };
        let db = crate::models::build_state_db_with_chain_id(
            vec![server.clone()],
            Some([0xAB; 32]),
            require_chain_id,
        );
        (server, db)
    }

    /// `get_status_result_v0` without the optional `chain_id` extension.
    fn status_v0_without_chain_id() -> Vec<u8> {
        status_message(&status_body()[..88])
    }

    #[tokio::test]
    async fn pinned_v0_without_chain_id_stays_online() {
        let (server, db) = pinned_server(false);
        assert_eq!(
            apply_status_update(&status_v0_without_chain_id(), &db, &server)
                .await
                .expect("v0 without chain_id"),
            Some(0)
        );
        let guard = db.lock().await;
        let state = guard.get(&server.endpoint).unwrap();
        assert!(
            state.online,
            "older SHiP that omits chain_id must stay online when chain_id is pinned"
        );
        assert_eq!(state.chain_id, None);
        assert!(state.chain_id_unverified_warned);
        assert_eq!(state.trace_begin_block, 10);
        assert_eq!(state.trace_end_block, 1001);
        assert_eq!(state.chain_state_begin_block, 5);
        assert_eq!(state.chain_state_end_block, 1000);
    }

    #[tokio::test]
    async fn require_chain_id_marks_missing_chain_id_offline() {
        let (server, db) = pinned_server(true);
        apply_status_update(&status_v0_without_chain_id(), &db, &server)
            .await
            .expect("v0 without chain_id");
        {
            let guard = db.lock().await;
            let state = guard.get(&server.endpoint).unwrap();
            assert!(
                !state.online,
                "require_chain_id restores reject-when-missing"
            );
            assert_eq!(state.chain_id, None);
            assert_eq!(state.chain_state_end_block, 0);
            assert!(!state.chain_id_unverified_warned);
        }

        apply_status_update(&status_message(&status_body()), &db, &server)
            .await
            .expect("matching chain_id");
        {
            let guard = db.lock().await;
            let state = guard.get(&server.endpoint).unwrap();
            assert!(state.online);
            assert_eq!(state.chain_id, Some([0xAB; 32]));
        }

        apply_status_update(&status_v0_without_chain_id(), &db, &server)
            .await
            .expect("omitted again");
        let guard = db.lock().await;
        let state = guard.get(&server.endpoint).unwrap();
        assert!(!state.online);
        assert_eq!(state.chain_id, None);
    }

    #[tokio::test]
    async fn pinned_chain_id_mismatch_marks_upstream_offline() {
        let (server, db) = pinned_server(false);
        let mut wrong_body = status_body();
        let len = wrong_body.len();
        wrong_body[len - 32..].copy_from_slice(&[0xFF; 32]);
        apply_status_update(&status_message(&wrong_body), &db, &server)
            .await
            .expect("wrong chain_id");
        let guard = db.lock().await;
        let state = guard.get(&server.endpoint).unwrap();
        assert!(
            !state.online,
            "a reported chain_id that mismatches the pin must stay offline"
        );
        assert_eq!(state.chain_id, None);
        assert_eq!(state.chain_state_end_block, 0);
    }

    #[tokio::test]
    async fn omitted_chain_id_stays_online_when_pool_peer_reported_one() {
        let s1 = Server {
            name: "alpha".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            ..Default::default()
        };
        let s2 = Server {
            name: "beta".to_string(),
            endpoint: "127.0.0.1:18081".to_string(),
            enabled: true,
            ..Default::default()
        };
        let db = build_state_db(vec![s1.clone(), s2.clone()]);
        apply_status_update(&status_message(&status_body()), &db, &s1)
            .await
            .expect("s1 status");
        apply_status_update(&status_v0_without_chain_id(), &db, &s2)
            .await
            .expect("s2 omits chain_id");
        let guard = db.lock().await;
        let state2 = guard.get(&s2.endpoint).unwrap();
        assert!(state2.online, "omitting chain_id is not a pool mismatch");
        assert_eq!(state2.chain_id, None);
        assert!(state2.chain_id_unverified_warned);
        assert!(guard.get(&s1.endpoint).unwrap().online);
    }

    fn unverified_warns() -> u32 {
        super::UNVERIFIED_CHAIN_ID_WARNS.with(std::cell::Cell::get)
    }

    #[tokio::test]
    async fn omitted_chain_id_warning_is_once_per_upstream() {
        // Cargo reuses worker threads, so a previous test on this thread may
        // already have counted a warning. The counter is thread-local.
        super::UNVERIFIED_CHAIN_ID_WARNS.with(|count| count.set(0));

        let server = Server {
            name: "alpha".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            ..Default::default()
        };
        let other = Server {
            name: "beta".to_string(),
            endpoint: "127.0.0.1:18081".to_string(),
            enabled: true,
            ..Default::default()
        };
        let db = crate::models::build_state_db_with_chain_id(
            vec![server.clone(), other.clone()],
            Some([0xAB; 32]),
            false,
        );

        apply_status_update(&status_v0_without_chain_id(), &db, &server)
            .await
            .expect("first omission");
        apply_status_update(&status_v0_without_chain_id(), &db, &server)
            .await
            .expect("second omission");
        assert_eq!(
            unverified_warns(),
            1,
            "a pinned upstream that keeps omitting chain_id warns once"
        );
        assert!(db.lock().await.get(&server.endpoint).unwrap().online);
        assert!(
            db.lock()
                .await
                .get(&server.endpoint)
                .unwrap()
                .chain_id_unverified_warned
        );

        apply_status_update(&status_v0_without_chain_id(), &db, &other)
            .await
            .expect("other omission");
        apply_status_update(&status_v0_without_chain_id(), &db, &other)
            .await
            .expect("other omission again");
        assert_eq!(unverified_warns(), 2, "each upstream warns on its own");

        apply_status_update(&status_message(&status_body()), &db, &server)
            .await
            .expect("matching chain_id");
        {
            let guard = db.lock().await;
            let state = guard.get(&server.endpoint).unwrap();
            assert!(state.online);
            assert_eq!(state.chain_id, Some([0xAB; 32]));
            assert!(!state.chain_id_unverified_warned);
        }
        assert_eq!(unverified_warns(), 2);

        apply_status_update(&status_v0_without_chain_id(), &db, &server)
            .await
            .expect("omission after a report");
        apply_status_update(&status_v0_without_chain_id(), &db, &server)
            .await
            .expect("repeat omission after reset");
        assert_eq!(
            unverified_warns(),
            3,
            "reporting a chain_id arms the one-time warning again"
        );
        let guard = db.lock().await;
        let state = guard.get(&server.endpoint).unwrap();
        assert!(state.online);
        assert_eq!(
            state.chain_id,
            Some([0xAB; 32]),
            "a later omission keeps the last reported chain_id"
        );
        assert!(state.chain_id_unverified_warned);
    }

    #[tokio::test]
    async fn offline_peer_chain_id_does_not_poison_unpinned_pool() {
        let s1 = Server {
            name: "alpha".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            ..Default::default()
        };
        let s2 = Server {
            name: "beta".to_string(),
            endpoint: "127.0.0.1:18081".to_string(),
            enabled: true,
            ..Default::default()
        };
        let db = build_state_db(vec![s1.clone(), s2.clone()]);

        // First server comes online with chain_id [0xAB; 32], then goes offline.
        apply_status_update(&status_message(&status_body()), &db, &s1)
            .await
            .expect("s1 status");
        super::mark_offline(&db, &s1.endpoint).await;

        // Even if an offline peer still had a chain_id recorded, only online peers anchor the pool.
        db.lock().await.get_mut(&s1.endpoint).unwrap().chain_id = Some([0xAB; 32]);

        // Second server reporting [0xCD; 32] comes online cleanly because no online peer holds a conflicting chain_id.
        let mut s2_body = status_body();
        let len = s2_body.len();
        s2_body[len - 32..].copy_from_slice(&[0xCD; 32]);
        apply_status_update(&status_message(&s2_body), &db, &s2)
            .await
            .expect("s2 status");

        let guard = db.lock().await;
        let state2 = guard.get(&s2.endpoint).unwrap();
        assert!(
            state2.online,
            "an offline peer must not block an incoming upstream from establishing the pool chain_id"
        );
        assert_eq!(state2.chain_id, Some([0xCD; 32]));
    }

    const SHIP_ABI_JSON: &str = include_str!("../mock-ship/src/ship_abi.json");

    fn frozen_fixture(declared: Option<Range>) -> (Server, crate::models::ServerStateDb) {
        let server = Server {
            name: "archive".to_string(),
            endpoint: "127.0.0.1:18090".to_string(),
            enabled: true,
            role: Role::Frozen,
            range: declared,
            ..Default::default()
        };
        let db = build_state_db(vec![server.clone()]);
        (server, db)
    }

    // T1
    #[tokio::test]
    async fn status_replies_store_head_lib_ids_and_status_version() {
        let (server, db) = fixture();
        {
            let guard = db.lock().await;
            let state = guard.get(&server.endpoint).unwrap();
            assert!(!state.ranges_known);
            assert_eq!(state.head, None);
            assert_eq!(state.status_v1, None);
        }

        apply_status_update(&status_message_v1(&status_body_v1(10, 800)), &db, &server)
            .await
            .expect("v1 status");
        {
            let guard = db.lock().await;
            let state = guard.get(&server.endpoint).unwrap();
            assert_eq!(
                state.head,
                Some(BlockPos {
                    num: 1000,
                    id: [0x11; 32]
                })
            );
            assert_eq!(
                state.lib,
                Some(BlockPos {
                    num: 990,
                    id: [0x22; 32]
                })
            );
            assert!(state.ranges_known);
            assert_eq!(state.status_v1, Some(true));
            assert!(state.status_at.is_some());
            assert_eq!(state.offline_since, None);
        }

        let mut v0 = status_body();
        v0[0..4].copy_from_slice(&1005u32.to_le_bytes());
        v0[4..36].copy_from_slice(&[0x33; 32]);
        v0[36..40].copy_from_slice(&995u32.to_le_bytes());
        v0[40..72].copy_from_slice(&[0x44; 32]);
        apply_status_update(&status_message(&v0), &db, &server)
            .await
            .expect("v0 status");
        let guard = db.lock().await;
        let state = guard.get(&server.endpoint).unwrap();
        assert_eq!(
            state.head,
            Some(BlockPos {
                num: 1005,
                id: [0x33; 32]
            })
        );
        assert_eq!(
            state.lib,
            Some(BlockPos {
                num: 995,
                id: [0x44; 32]
            })
        );
        assert_eq!(state.status_v1, Some(false), "a v0 reply means v0 polling");
    }

    #[tokio::test]
    async fn head_zero_status_does_not_mark_ranges_known() {
        let (server, db) = fixture();
        let mut body = status_body();
        body[0..4].copy_from_slice(&0u32.to_le_bytes());
        assert_eq!(
            apply_status_update(&status_message(&body), &db, &server)
                .await
                .expect("head 0"),
            Some(0)
        );
        let guard = db.lock().await;
        let state = guard.get(&server.endpoint).unwrap();
        assert!(!state.ranges_known);
        assert_eq!(state.head, None);
        assert_eq!(state.status_at, None);
    }

    #[tokio::test]
    async fn v1_fallback_marks_status_v0_only() {
        let (server, db) = fixture();
        apply_status_update(&status_message_v1(&status_body_v1(10, 800)), &db, &server)
            .await
            .expect("v1 status");
        assert_eq!(db.lock().await[&server.endpoint].status_v1, Some(true));
        super::mark_status_v0_only(&db, &server.endpoint).await;
        assert_eq!(db.lock().await[&server.endpoint].status_v1, Some(false));
    }

    #[tokio::test]
    async fn monitor_records_the_accepted_abi_and_its_fingerprint() {
        let (server, db) = fixture();
        let mut ship_abi = None;
        let inbound = handle_monitoring_msg(
            Message::Text(SHIP_ABI_JSON.into()),
            &mut ship_abi,
            &db,
            &server,
        )
        .await;
        assert!(matches!(inbound, Inbound::AbiAccepted));
        {
            let guard = db.lock().await;
            let abi = guard[&server.endpoint].abi.clone().expect("abi stored");
            assert_eq!(&*abi.text, SHIP_ABI_JSON);
            assert_eq!(abi.fp, crate::abi::abi_fingerprint(SHIP_ABI_JSON));
            assert!(abi.compatible);
        }

        // A rejected ABI is not recorded.
        let (other, other_db) = fixture();
        let inbound =
            handle_monitoring_msg(Message::Text("{".into()), &mut None, &other_db, &other).await;
        assert!(matches!(inbound, Inbound::AbiRejected));
        assert!(other_db.lock().await[&other.endpoint].abi.is_none());
    }

    #[tokio::test]
    async fn an_abi_that_differs_from_the_pool_is_flagged_once() {
        let s1 = Server {
            name: "alpha".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            ..Default::default()
        };
        let s2 = Server {
            name: "beta".to_string(),
            endpoint: "127.0.0.1:18081".to_string(),
            enabled: true,
            ..Default::default()
        };
        let db = build_state_db(vec![s1.clone(), s2.clone()]);
        handle_monitoring_msg(Message::Text(SHIP_ABI_JSON.into()), &mut None, &db, &s1).await;
        let other_abi = tiny_abi("acta");
        handle_monitoring_msg(Message::Text(other_abi.clone().into()), &mut None, &db, &s2).await;
        {
            let guard = db.lock().await;
            assert_eq!(guard[&s1.endpoint].warned.abi_fp, None);
            let beta = &guard[&s2.endpoint];
            let fp = beta.abi.as_ref().unwrap().fp;
            assert_eq!(beta.warned.abi_fp, Some(fp));
            assert!(!beta.abi.as_ref().unwrap().compatible);
        }
        // Reconnecting with the matching ABI clears the flag.
        handle_monitoring_msg(Message::Text(SHIP_ABI_JSON.into()), &mut None, &db, &s2).await;
        assert_eq!(db.lock().await[&s2.endpoint].warned.abi_fp, None);
    }

    #[tokio::test]
    async fn monitor_offline_keeps_state_and_a_status_clears_offline_since() {
        let (server, db) = fixture();
        apply_status_update(&status_message(&status_body()), &db, &server)
            .await
            .expect("status");
        handle_monitoring_msg(Message::Text(SHIP_ABI_JSON.into()), &mut None, &db, &server).await;
        super::mark_offline(&db, &server.endpoint).await;
        {
            let guard = db.lock().await;
            let state = &guard[&server.endpoint];
            assert!(!state.online);
            assert!(state.offline_since.is_some());
            assert!(state.ranges_known);
            assert_eq!(state.head.map(|h| h.num), Some(1000));
            assert_eq!(state.trace_end_block, 1001);
            assert!(state.abi.is_some());
        }
        apply_status_update(&status_message(&status_body()), &db, &server)
            .await
            .expect("status");
        let guard = db.lock().await;
        assert!(guard[&server.endpoint].online);
        assert_eq!(guard[&server.endpoint].offline_since, None);
    }

    #[tokio::test]
    async fn chain_id_mismatch_sets_offline_since() {
        let (server, db) = pinned_server(false);
        let mut wrong_body = status_body();
        let len = wrong_body.len();
        wrong_body[len - 32..].copy_from_slice(&[0xFF; 32]);
        apply_status_update(&status_message(&wrong_body), &db, &server)
            .await
            .expect("wrong chain_id");
        let guard = db.lock().await;
        let state = &guard[&server.endpoint];
        assert!(!state.online);
        assert!(state.offline_since.is_some());
        assert!(!state.ranges_known, "a rejected status is not a report");
    }

    fn set_static_status(state: &mut crate::models::ServerState, head: u32, lib: u32, end: u32) {
        state.online = true;
        state.head = Some(BlockPos {
            num: head,
            id: [0x11; 32],
        });
        state.lib = Some(BlockPos {
            num: lib,
            id: [0x22; 32],
        });
        state.trace_begin_block = 2;
        state.trace_end_block = end;
        state.chain_state_begin_block = 2;
        state.chain_state_end_block = end;
        state.ranges_known = true;
    }

    // T2
    #[test]
    fn frozen_upstreams_are_never_stale_and_live_ones_are_after_twelve_intervals() {
        let (live_server, live_db) = fixture();
        let (frozen_server, frozen_db) = frozen_fixture(None);
        let mut live_states = live_db.try_lock().unwrap();
        let mut frozen_states = frozen_db.try_lock().unwrap();
        set_static_status(
            live_states.get_mut(&live_server.endpoint).unwrap(),
            1000,
            990,
            1001,
        );
        set_static_status(
            frozen_states.get_mut(&frozen_server.endpoint).unwrap(),
            1000,
            1000,
            1001,
        );
        // A frozen upstream someone flagged stale is cleared on the next tick.
        frozen_states
            .get_mut(&frozen_server.endpoint)
            .unwrap()
            .stale = true;

        let mut live_tracker = super::ProgressTracker::default();
        let mut frozen_tracker = super::ProgressTracker::default();
        // The first interval sees the ranges grow from nothing.
        assert_eq!(live_tracker.tick(&mut live_states), 1);
        assert_eq!(frozen_tracker.tick(&mut frozen_states), 0);
        for interval in 1..=super::STALE_THRESHOLD {
            live_tracker.tick(&mut live_states);
            frozen_tracker.tick(&mut frozen_states);
            assert_eq!(
                live_states[&live_server.endpoint].stale,
                interval == super::STALE_THRESHOLD,
                "live stale flag after {interval} static intervals"
            );
        }
        for _ in 0..50 {
            frozen_tracker.tick(&mut frozen_states);
        }
        assert!(!frozen_states[&frozen_server.endpoint].stale);

        // Progress clears the live flag again.
        live_states
            .get_mut(&live_server.endpoint)
            .unwrap()
            .chain_state_end_block = 1002;
        assert_eq!(live_tracker.tick(&mut live_states), 1);
        assert!(!live_states[&live_server.endpoint].stale);
    }

    fn looks_frozen_hints() -> u32 {
        super::LOOKS_FROZEN_HINTS.with(std::cell::Cell::get)
    }

    // T3
    #[test]
    fn looks_frozen_hint_is_logged_exactly_once() {
        super::LOOKS_FROZEN_HINTS.with(|count| count.set(0));

        let (server, db) = fixture();
        let mut states = db.try_lock().unwrap();
        // A live node stuck where an archive would sit: head == LIB == last block.
        set_static_status(states.get_mut(&server.endpoint).unwrap(), 1000, 1000, 1001);
        let mut tracker = super::ProgressTracker::default();
        for _ in 0..(super::STALE_THRESHOLD * 4) {
            tracker.tick(&mut states);
        }
        assert!(states[&server.endpoint].stale);
        assert!(states[&server.endpoint].warned.looks_frozen);
        assert_eq!(looks_frozen_hints(), 1);

        // A stale live node whose LIB trails its head is just stuck.
        let (stuck, stuck_db) = fixture();
        let mut stuck_states = stuck_db.try_lock().unwrap();
        set_static_status(
            stuck_states.get_mut(&stuck.endpoint).unwrap(),
            1000,
            990,
            1001,
        );
        let mut stuck_tracker = super::ProgressTracker::default();
        for _ in 0..(super::STALE_THRESHOLD * 2) {
            stuck_tracker.tick(&mut stuck_states);
        }
        assert!(stuck_states[&stuck.endpoint].stale);
        assert!(!stuck_states[&stuck.endpoint].warned.looks_frozen);

        // A declared frozen upstream is never hinted.
        let (archive, archive_db) = frozen_fixture(None);
        let mut archive_states = archive_db.try_lock().unwrap();
        set_static_status(
            archive_states.get_mut(&archive.endpoint).unwrap(),
            1000,
            1000,
            1001,
        );
        let mut archive_tracker = super::ProgressTracker::default();
        for _ in 0..(super::STALE_THRESHOLD * 2) {
            archive_tracker.tick(&mut archive_states);
        }
        assert_eq!(looks_frozen_hints(), 1);
    }

    #[tokio::test]
    async fn frozen_advance_and_declared_range_mismatch_are_flagged() {
        let (server, db) = frozen_fixture(Some(Range {
            begin: 10,
            end: 1001,
        }));
        // status_body reports trace [10, 1001) and chain_state [5, 1000).
        apply_status_update(&status_message(&status_body()), &db, &server)
            .await
            .expect("status");
        {
            let guard = db.lock().await;
            let state = &guard[&server.endpoint];
            assert!(state.range_mismatch());
            assert!(state.warned.range_mismatch);
            assert!(
                !state.warned.frozen_advanced,
                "the first report is not an advance"
            );
        }

        let mut grown = status_body();
        // trace_end_block sits at bytes 76..80 of the v0 body.
        grown[76..80].copy_from_slice(&1100u32.to_le_bytes());
        apply_status_update(&status_message(&grown), &db, &server)
            .await
            .expect("status");
        assert!(db.lock().await[&server.endpoint].warned.frozen_advanced);

        // Reports that match the declared range clear the mismatch.
        let mut matching = status_body();
        matching[76..80].copy_from_slice(&1001u32.to_le_bytes());
        matching[80..84].copy_from_slice(&10u32.to_le_bytes());
        matching[84..88].copy_from_slice(&1001u32.to_le_bytes());
        apply_status_update(&status_message(&matching), &db, &server)
            .await
            .expect("status");
        let guard = db.lock().await;
        assert!(!guard[&server.endpoint].range_mismatch());
        assert!(!guard[&server.endpoint].warned.range_mismatch);
    }

    #[tokio::test]
    async fn normal_log_shapes_are_not_warned_as_a_range_mismatch() {
        // Declared [1, 1001); trace starts at 2 (the first block has no
        // entry) and chain state ends a block early at the data tail.
        let (server, db) = frozen_fixture(Some(Range {
            begin: 1,
            end: 1001,
        }));
        let mut body = status_body();
        body[72..76].copy_from_slice(&2u32.to_le_bytes());
        body[80..84].copy_from_slice(&1u32.to_le_bytes());
        apply_status_update(&status_message(&body), &db, &server)
            .await
            .expect("status");
        let guard = db.lock().await;
        let state = &guard[&server.endpoint];
        assert_eq!(
            (state.trace_begin_block, state.chain_state_end_block),
            (2, 1000)
        );
        assert!(!state.range_mismatch());
        assert!(!state.warned.range_mismatch, "not warned");
    }

    /// A reconnect pause runs its whole delay, unless a session dial gets
    /// through: then it ends at the floor after the failure, even when the
    /// dial came before the pause began (the waiter was registered first).
    #[tokio::test]
    async fn a_session_dial_ends_a_reconnect_pause_at_its_floor() {
        use std::time::Duration;

        use tokio::sync::{broadcast, Notify};
        use tokio::time::Instant;

        let (_tx, mut shutdown) = broadcast::channel::<()>(1);
        let notify = Notify::new();
        let floor = Duration::from_millis(150);

        let started = Instant::now();
        let reachable = notify.notified();
        tokio::pin!(reachable);
        reachable.as_mut().enable();
        assert!(
            super::reconnect_pause(
                Duration::from_millis(300),
                floor,
                reachable.as_mut(),
                &mut shutdown,
                "a"
            )
            .await
        );
        assert!(started.elapsed() >= Duration::from_millis(300), "the delay");

        for early in [false, true] {
            let reachable = notify.notified();
            tokio::pin!(reachable);
            reachable.as_mut().enable();
            if early {
                notify.notify_waiters();
            }
            let started = Instant::now();
            let pause = super::reconnect_pause(
                Duration::from_secs(30),
                floor,
                reachable.as_mut(),
                &mut shutdown,
                "a",
            );
            let woke = async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                notify.notify_waiters();
            };
            let (resumed, ()) = tokio::join!(pause, woke);
            assert!(resumed);
            let took = started.elapsed();
            assert!(
                took >= floor - Duration::from_millis(20) && took < Duration::from_secs(2),
                "early = {early}: {took:?}"
            );
        }
    }

    /// A monitor deep in its reconnect backoff (3.2 s after six failed
    /// connects) reconnects right after a client session's dial gets
    /// through, instead of at the end of the backoff.
    #[tokio::test]
    async fn a_session_dial_reconnects_a_monitor_in_backoff() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        use tokio::net::TcpListener;
        use tokio::sync::broadcast;
        use tokio::time::{sleep, Instant};

        use crate::fleet::FleetHub;
        use crate::models::{
            build_config_db, ResolvedListener, ResolvedTopology, StaticConfig, StitchingPolicy,
        };

        // Drops every connection until `serving`, then serves an archive.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let attempts = Arc::new(AtomicUsize::new(0));
        let serving = Arc::new(AtomicBool::new(false));
        let upstream = {
            let (attempts, serving) = (attempts.clone(), serving.clone());
            tokio::spawn(async move {
                loop {
                    let Ok((tcp, _)) = listener.accept().await else {
                        return;
                    };
                    attempts.fetch_add(1, Ordering::SeqCst);
                    if serving.load(Ordering::SeqCst) {
                        tokio::spawn(async move {
                            if let Ok(ws) = tokio_tungstenite::accept_async(tcp).await {
                                let config = mock_ship::MockShipConfig::frozen(1, 1001);
                                mock_ship::serve_websocket(config, ws).await;
                            }
                        });
                    }
                }
            })
        };

        let server = Server {
            name: "archive".to_string(),
            endpoint: endpoint.clone(),
            enabled: true,
            role: Role::Frozen,
            ..Default::default()
        };
        let resolved = ResolvedListener {
            name: None,
            listen_address: "127.0.0.1".to_string(),
            listen_port: 0,
            max_connections: 10,
            expected_chain_id: None,
            require_chain_id: false,
            block_failure_quarantine: None,
            topology: ResolvedTopology::Stitched,
            stitching: StitchingPolicy::default(),
            abi_pin: None,
            servers: vec![server.clone()],
        };
        let hub = FleetHub::new(
            &resolved,
            build_state_db(vec![server.clone()]),
            build_config_db(vec![server.clone()]),
        );
        let counters = hub.state.lock().await[&endpoint].counters.clone();
        let static_config = StaticConfig {
            upstream_reconnect_ms: 100,
            upstream_monitoring_ms: 100,
            upstream_status_ms: 1_000,
            connect_timeout_ms: 1_000,
            max_message_bytes: 1 << 20,
        };
        let (stop, shutdown) = broadcast::channel(1);
        let monitor = tokio::spawn(super::monitoring_connection(
            server,
            static_config,
            hub.clone(),
            shutdown,
        ));

        // Backoffs of 0.1, 0.2, 0.4, 0.8 and 1.6 s: the sixth failed connect
        // is followed by 3.2 s.
        let deadline = Instant::now() + Duration::from_secs(10);
        while attempts.load(Ordering::SeqCst) < 6 {
            assert!(Instant::now() < deadline, "the monitor stopped retrying");
            sleep(Duration::from_millis(5)).await;
        }
        serving.store(true, Ordering::SeqCst);
        let dialed = Instant::now();
        // What a session's dial that gets through does.
        counters.dial_succeeded.notify_waiters();
        while !hub.state.lock().await[&endpoint].online {
            assert!(
                dialed.elapsed() < Duration::from_millis(1_500),
                "the monitor waited out its backoff"
            );
            sleep(Duration::from_millis(10)).await;
        }
        stop.send(()).unwrap();
        monitor.await.unwrap();
        upstream.abort();
    }
}
