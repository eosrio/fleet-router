use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use rs_abieos::Abieos;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex};
use tokio::time::{sleep, sleep_until, timeout, Instant};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{self, Message},
    MaybeTlsStream, WebSocketStream,
};
use tungstenite::protocol::WebSocketConfig;

use crate::models::{Server, ServerStateDb, StaticConfig};
use crate::zcd;
use crate::zcd::ZCDValues;

/// Number of consecutive monitoring intervals without chain-state advancement
/// before an upstream is flagged stale (and deprioritized for routing).
const STALE_THRESHOLD: u32 = 12;

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
    /// A `get_status_result_v0` was decoded (the in-flight request is done).
    Status,
    /// Upstream sent a close frame. Stop monitoring this server.
    Close,
    Ignore,
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

async fn send_status(sender: &mut WsSink, endpoint: &str) -> bool {
    if let Err(e) = sender.send(Message::Binary(vec![0u8].into())).await {
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
    abieos: Arc<Mutex<Abieos>>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut backoff_attempts: u32 = 0;
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

        let stop = run_monitor_session(
            websocket,
            &server,
            &static_config,
            &server_state_db,
            &abieos,
            &mut shutdown,
        )
        .await;
        if stop {
            break;
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
/// Returns `true` when the outer loop should stop (shutdown, or the upstream
/// closed the session). Returns `false` to reconnect.
async fn run_monitor_session(
    websocket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    server: &Server,
    static_config: &StaticConfig,
    server_state_db: &ServerStateDb,
    abieos: &Arc<Mutex<Abieos>>,
    shutdown: &mut broadcast::Receiver<()>,
) -> bool {
    let (mut sender, mut receiver) = websocket.split();
    let mut ship_abi: Option<String> = None;
    let mut abi_ready = false;
    let mut status_in_flight = false;
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
            _ = shutdown.recv() => return true,
            next = receiver.next() => match next {
                Some(Ok(msg)) => {
                    match handle_monitoring_msg(msg, &mut ship_abi, abieos, server_state_db, server).await {
                        Inbound::AbiAccepted => {
                            abi_ready = true;
                            if !send_status(&mut sender, &server.endpoint).await {
                                mark_offline(server_state_db, &server.endpoint).await;
                                return false;
                            }
                            status_in_flight = true;
                            let sent = Instant::now();
                            status_deadline = sent + reply_timeout;
                            next_status_at = sent + reply_timeout;
                        }
                        Inbound::Status => {
                            status_in_flight = false;
                        }
                        Inbound::AbiRejected => {
                            mark_offline(server_state_db, &server.endpoint).await;
                            return false;
                        }
                        Inbound::Close => {
                            mark_offline(server_state_db, &server.endpoint).await;
                            return true;
                        }
                        Inbound::Ignore => {}
                    }
                }
                None => {
                    tracing::info!(upstream = %server.endpoint, "upstream disconnected");
                    mark_offline(server_state_db, &server.endpoint).await;
                    return false;
                }
                Some(Err(e)) => {
                    tracing::warn!(upstream = %server.endpoint, error = %e, "error reading from upstream");
                    mark_offline(server_state_db, &server.endpoint).await;
                    return false;
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
                    return false;
                }
                if status_in_flight {
                    tracing::warn!(
                        upstream = %server.endpoint,
                        timeout_ms = static_config.upstream_status_ms,
                        "status reply timed out; marking upstream offline"
                    );
                    mark_offline(server_state_db, &server.endpoint).await;
                    return false;
                }
                if !send_status(&mut sender, &server.endpoint).await {
                    mark_offline(server_state_db, &server.endpoint).await;
                    return false;
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
    abieos: &Arc<Mutex<Abieos>>,
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
            let loaded = {
                let abieos = abieos.lock().await;
                abieos.set_abi_json_native(0u64, &text)
            };
            *ship_abi = Some(text);
            match loaded {
                Ok(true) => Inbound::AbiAccepted,
                Ok(false) => {
                    tracing::warn!(upstream = %server.endpoint, "abieos rejected upstream ABI");
                    Inbound::AbiRejected
                }
                Err(_) => {
                    tracing::warn!(upstream = %server.endpoint, "error setting ABI for upstream");
                    Inbound::AbiRejected
                }
            }
        }
        Message::Binary(bin_msg) => {
            match apply_status_update(&bin_msg, server_state_db, server).await {
                // `Ok(true)` is a decoded status result, including head 0.
                // Anything else is not the in-flight reply.
                Ok(true) => Inbound::Status,
                Ok(false) => Inbound::Ignore,
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
/// `Ok(true)` means the frame was a `get_status_result_v0` and counts as the
/// in-flight status reply (state is updated only when `head_block_num > 0`).
/// `Ok(false)` means the frame was some other result and was ignored.
/// A truncated or malformed status body leaves the stored state unchanged and
/// returns the decode error; that error is not a valid reply.
async fn apply_status_update(
    bin_msg: &[u8],
    server_state_db: &ServerStateDb,
    server: &Server,
) -> Result<bool, zcd::ZcdError> {
    let result_message = zcd::deserialize_result(bin_msg);
    match result_message.get("variant")? {
        Some(ZCDValues::U8(0)) => {}
        _ => return Ok(false),
    }
    let Some(ZCDValues::Bytes(bytes)) = result_message.get("data")? else {
        return Err(zcd::ZcdError::MissingField);
    };
    let result = zcd::deserialize_status_result(&bytes)?;
    let head_block_num = require_u32(&result, "head_block_num")?;
    if head_block_num == 0 {
        return Ok(true);
    }
    let trace_begin = require_u32(&result, "trace_begin_block")?;
    let trace_end = require_u32(&result, "trace_end_block")?;
    let chain_begin = require_u32(&result, "chain_state_begin_block")?;
    let chain_end = require_u32(&result, "chain_state_end_block")?;

    let mut db = server_state_db.lock().await;
    if let Some(state) = db.get_mut(&server.endpoint) {
        state.enabled = true;
        state.online = true;
        state.trace_begin_block = trace_begin;
        state.trace_end_block = trace_end;
        state.chain_state_begin_block = chain_begin;
        state.chain_state_end_block = chain_end;
    }
    Ok(true)
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
    use std::sync::Arc;

    use rs_abieos::Abieos;
    use tokio::sync::Mutex;
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

    #[tokio::test]
    async fn valid_status_frame_updates_upstream_state() {
        let (server, db) = fixture();
        assert!(
            apply_status_update(&status_message(&status_body()), &db, &server)
                .await
                .expect("valid status"),
            "a decoded status result counts as the in-flight reply"
        );
        let guard = db.lock().await;
        let state = guard.get(&server.endpoint).expect("state");
        assert!(state.online);
        assert_eq!(state.trace_begin_block, 10);
        assert_eq!(state.trace_end_block, 1001);
        assert_eq!(state.chain_state_begin_block, 5);
        assert_eq!(state.chain_state_end_block, 1000);
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
        let abieos = Arc::new(Mutex::new(Abieos::new()));
        let mut ship_abi = None;
        let inbound = handle_monitoring_msg(
            Message::Binary(vec![0u8].into()),
            &mut ship_abi,
            &abieos,
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
        assert!(!reply);
        let abieos = Arc::new(Mutex::new(Abieos::new()));
        let mut ship_abi = None;
        let inbound = handle_monitoring_msg(
            Message::Binary(vec![1u8].into()),
            &mut ship_abi,
            &abieos,
            &db,
            &server,
        )
        .await;
        assert!(matches!(inbound, Inbound::Ignore));
    }
}
