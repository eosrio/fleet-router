use std::collections::HashMap;
use std::time::Duration;

use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::{sleep, sleep_until, timeout, Instant};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{self, Message},
    MaybeTlsStream, WebSocketStream,
};
use tungstenite::protocol::WebSocketConfig;

use crate::abi::accept_upstream_abi;
use crate::models::{Server, ServerStateDb, StaticConfig};
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
    server_state_db: ServerStateDb,
    interval_ms: u64,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut last_block: HashMap<String, u32> = HashMap::new();
    let mut stale_counter: HashMap<String, u32> = HashMap::new();

    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = sleep(Duration::from_millis(interval_ms)) => {}
        }

        let mut db = server_state_db.lock().await;
        let mut updated = 0;
        for (server, state) in db.iter_mut() {
            let last = last_block.entry(server.clone()).or_insert(0);
            let counter = stale_counter.entry(server.clone()).or_insert(0);

            if state.chain_state_end_block > *last {
                *last = state.chain_state_end_block;
                *counter = 0;
                updated += 1;
                if state.stale {
                    state.stale = false;
                    tracing::info!(upstream = %server, "upstream resumed advancing");
                }
            } else {
                *counter += 1;
                if *counter == STALE_THRESHOLD && !state.stale {
                    state.stale = true;
                    tracing::warn!(
                        upstream = %server,
                        intervals = STALE_THRESHOLD,
                        "upstream not advancing; flagged stale and deprioritized"
                    );
                }
            }
        }

        if updated > 0 {
            tracing::debug!(?last_block, "upstream block progress");
        }
    }
    tracing::debug!("state monitoring loop stopped");
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
    /// Upstream sent a close frame. Stop monitoring this server, unless this
    /// session's unanswered v1 probe should reconnect and fall back.
    Close,
    Ignore,
}

/// How [`run_monitor_session`] ended.
enum SessionEnd {
    /// Shutdown, or the upstream closed after this was not an unanswered v1 probe.
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
        state.online = false;
    }
}

fn backoff_delay(reconnect_ms: u64, attempts: &mut u32) -> u64 {
    const MAX_BACKOFF_MS: u64 = 30_000;
    let backoff = reconnect_ms
        .saturating_mul(1u64 << (*attempts).min(5))
        .min(MAX_BACKOFF_MS);
    *attempts = attempts.saturating_add(1);
    backoff
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
pub async fn monitoring_connection(
    server: Server,
    static_config: StaticConfig,
    server_state_db: ServerStateDb,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut backoff_attempts: u32 = 0;
    let mut unanswered_v1_sessions: u32 = 0;
    // Prefer v1 so Savanna nodes report a finality-data range. Fall back to v0
    // when the upstream answers with v0, or never answers a v1 probe.
    let mut status_request = STATUS_REQUEST_V1;
    let connect_timeout = Duration::from_millis(static_config.connect_timeout_ms);

    loop {
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
                    mark_offline(&server_state_db, &server.endpoint).await;
                    let backoff = backoff_delay(static_config.upstream_reconnect_ms, &mut backoff_attempts);
                    tracing::warn!(upstream = %server.endpoint, error = %e, retry_in_ms = backoff, "upstream connect failed");
                    tokio::select! {
                        _ = shutdown.recv() => break,
                        _ = sleep(Duration::from_millis(backoff)) => continue,
                    }
                }
                Err(_) => {
                    mark_offline(&server_state_db, &server.endpoint).await;
                    let backoff = backoff_delay(static_config.upstream_reconnect_ms, &mut backoff_attempts);
                    tracing::warn!(
                        upstream = %server.endpoint,
                        timeout_ms = static_config.connect_timeout_ms,
                        retry_in_ms = backoff,
                        "upstream connect timed out"
                    );
                    tokio::select! {
                        _ = shutdown.recv() => break,
                        _ = sleep(Duration::from_millis(backoff)) => continue,
                    }
                }
            }
        };

        backoff_attempts = 0;
        tracing::info!(upstream = %server.endpoint, "monitoring connected to upstream");

        let end = run_monitor_session(
            websocket,
            &server,
            &static_config,
            &server_state_db,
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
/// [`SessionEnd::Stop`] means shutdown, or the upstream closed after this was
/// not an unanswered v1 probe. [`SessionEnd::Reconnect`] asks the outer loop
/// to connect again. `status_request` is the variant byte to send; a v0 result
/// switches it to [`STATUS_REQUEST_V0`] for the rest of this upstream.
async fn run_monitor_session(
    websocket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    server: &Server,
    static_config: &StaticConfig,
    server_state_db: &ServerStateDb,
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
                    match handle_monitoring_msg(msg, &mut ship_abi, server_state_db, server).await {
                        Inbound::AbiAccepted => {
                            abi_ready = true;
                            let variant = *status_request;
                            if !send_status(&mut sender, &server.endpoint, variant).await {
                                mark_offline(server_state_db, &server.endpoint).await;
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
                            mark_offline(server_state_db, &server.endpoint).await;
                            return session_reconnect(sent_v1, saw_status);
                        }
                        Inbound::Close => {
                            mark_offline(server_state_db, &server.endpoint).await;
                            // A node that does not understand v1 often closes the
                            // socket. Reconnect so the miss counter can fall back
                            // to v0. A close after a real status result still stops.
                            if sent_v1 && !saw_status {
                                return session_reconnect(true, false);
                            }
                            return SessionEnd::Stop;
                        }
                        Inbound::Ignore => {}
                    }
                }
                None => {
                    tracing::info!(upstream = %server.endpoint, "upstream disconnected");
                    mark_offline(server_state_db, &server.endpoint).await;
                    return session_reconnect(sent_v1, saw_status);
                }
                Some(Err(e)) => {
                    tracing::warn!(upstream = %server.endpoint, error = %e, "error reading from upstream");
                    mark_offline(server_state_db, &server.endpoint).await;
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
                    mark_offline(server_state_db, &server.endpoint).await;
                    return session_reconnect(sent_v1, saw_status);
                }
                if status_in_flight {
                    tracing::warn!(
                        upstream = %server.endpoint,
                        timeout_ms = static_config.upstream_status_ms,
                        "status reply timed out; marking upstream offline"
                    );
                    mark_offline(server_state_db, &server.endpoint).await;
                    return session_reconnect(sent_v1, saw_status);
                }
                let variant = *status_request;
                if !send_status(&mut sender, &server.endpoint, variant).await {
                    mark_offline(server_state_db, &server.endpoint).await;
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
            // The text is kept only so a later text frame is ignored.
            let loaded = accept_upstream_abi(&text);
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
/// is updated only when `head_block_num > 0`. A v0 result clears any previously
/// learned finality range. `Ok(None)` means some other result and was ignored.
/// A truncated or malformed status body leaves the stored state unchanged and
/// returns the decode error; that error is not a valid reply.
async fn apply_status_update(
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

    let mut db = server_state_db.lock().await;
    let peer_chain_id = db
        .iter()
        .filter(|(ep, s)| *ep != &server.endpoint && s.online)
        .find_map(|(_, s)| s.chain_id)
        .or_else(|| {
            db.iter()
                .filter(|(ep, _)| *ep != &server.endpoint)
                .find_map(|(_, s)| s.chain_id)
        });

    if let Some(state) = db.get_mut(&server.endpoint) {
        if let Some(expected) = state.expected_chain_id {
            match reported_chain_id {
                Some(actual) if actual == expected => {}
                Some(actual) => {
                    tracing::warn!(
                        name = %server.name,
                        upstream = %server.endpoint,
                        expected_chain_id = %hex_32(&expected),
                        reported_chain_id = %hex_32(&actual),
                        "upstream chain_id does not match configured chain_id; marking offline"
                    );
                    state.online = false;
                    return Ok(Some(variant));
                }
                None => {
                    tracing::warn!(
                        name = %server.name,
                        upstream = %server.endpoint,
                        expected_chain_id = %hex_32(&expected),
                        "upstream status omitted chain_id while chain_id is pinned; marking offline"
                    );
                    state.online = false;
                    return Ok(Some(variant));
                }
            }
        } else if let (Some(pool_cid), Some(actual)) = (peer_chain_id, reported_chain_id) {
            if actual != pool_cid {
                tracing::warn!(
                    name = %server.name,
                    upstream = %server.endpoint,
                    pool_chain_id = %hex_32(&pool_cid),
                    reported_chain_id = %hex_32(&actual),
                    "upstream chain_id mismatches pool chain_id; marking offline"
                );
                state.online = false;
                return Ok(Some(variant));
            }
        }

        if let Some(cid) = reported_chain_id {
            state.chain_id = Some(cid);
        }
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
    }
    Ok(Some(variant))
}

fn hex_32(bytes: &[u8; 32]) -> String {
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

#[cfg(test)]
mod tests {
    use tokio_tungstenite::tungstenite::Message;

    use super::{apply_status_update, handle_monitoring_msg, Inbound};
    use crate::models::{build_state_db, Server};
    use crate::zcd::ZcdError;

    fn fixture() -> (Server, crate::models::ServerStateDb) {
        let server = Server {
            name: "alpha".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
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
        };
        let s2 = Server {
            name: "beta".to_string(),
            endpoint: "127.0.0.1:18081".to_string(),
            enabled: true,
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

    #[tokio::test]
    async fn pinned_expected_chain_id_rejects_mismatch_and_missing_extension() {
        let s1 = Server {
            name: "alpha".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
        };
        let db = crate::models::build_state_db_with_chain_id(vec![s1.clone()], Some([0xAB; 32]));

        // Status v0 without the optional chain_id extension (88 bytes instead of 120) must be rejected when pinned.
        let v0_no_chain_id = &status_body()[..88];
        apply_status_update(&status_message(v0_no_chain_id), &db, &s1)
            .await
            .expect("v0 without chain_id");
        assert!(!db.lock().await.get(&s1.endpoint).unwrap().online);

        // Status with wrong chain_id must be rejected.
        let mut wrong_body = status_body();
        let len = wrong_body.len();
        wrong_body[len - 32..].copy_from_slice(&[0xFF; 32]);
        apply_status_update(&status_message(&wrong_body), &db, &s1)
            .await
            .expect("wrong chain_id");
        assert!(!db.lock().await.get(&s1.endpoint).unwrap().online);

        // Status with matching chain_id [0xAB; 32] succeeds.
        apply_status_update(&status_message(&status_body()), &db, &s1)
            .await
            .expect("matching chain_id");
        let guard = db.lock().await;
        let state = guard.get(&s1.endpoint).unwrap();
        assert!(state.online);
        assert_eq!(state.chain_id, Some([0xAB; 32]));
    }
}
