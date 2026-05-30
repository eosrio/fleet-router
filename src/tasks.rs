use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use rs_abieos::Abieos;
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Mutex};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use crate::models::{Server, ServerStateDb, StaticConfig};
use crate::zcd;
use crate::zcd::ZCDValues;

type WsSender = Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>;

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

async fn send_status_loop(sender: WsSender, interval_ms: u64) {
    loop {
        sleep(Duration::from_millis(interval_ms)).await;
        let message = Message::Binary(vec![0u8].into());
        let mut s = sender.lock().await;
        if let Err(e) = s.send(message).await {
            tracing::debug!(error = %e, "status-ping send failed; stopping ping loop");
            break;
        }
    }
}

/// Maintain a monitoring connection to a single upstream: connect, poll status,
/// keep its `ServerState` up to date, and reconnect with capped backoff.
pub async fn monitoring_connection(
    server: Server,
    static_config: StaticConfig,
    server_state_db: ServerStateDb,
    abieos: Arc<Mutex<Abieos>>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut ship_abi: Option<String> = None;
    let mut server_closed = false;
    let mut backoff_attempts: u32 = 0;
    const MAX_BACKOFF_MS: u64 = 30_000;

    loop {
        let websocket = tokio::select! {
            _ = shutdown.recv() => break,
            res = connect_async(server.ws_url()) => match res {
                Ok((websocket, _)) => websocket,
                Err(e) => {
                    let backoff = (static_config.upstream_reconnect_ms
                        .saturating_mul(1u64 << backoff_attempts.min(5)))
                        .min(MAX_BACKOFF_MS);
                    backoff_attempts = backoff_attempts.saturating_add(1);
                    tracing::warn!(upstream = %server.endpoint, error = %e, retry_in_ms = backoff, "upstream connect failed");
                    tokio::select! {
                        _ = shutdown.recv() => break,
                        _ = sleep(Duration::from_millis(backoff)) => continue,
                    }
                }
            }
        };

        backoff_attempts = 0;
        tracing::info!(upstream = %server.endpoint, "monitoring connected to upstream");
        let (sender, mut receiver) = websocket.split();
        let sender = Arc::new(Mutex::new(sender));

        let status_loop = tokio::spawn(send_status_loop(
            sender.clone(),
            static_config.upstream_status_ms,
        ));

        loop {
            let msg = tokio::select! {
                _ = shutdown.recv() => {
                    server_closed = true;
                    break;
                }
                next = receiver.next() => match next {
                    Some(Ok(msg)) => msg,
                    None => {
                        tracing::info!(upstream = %server.endpoint, "upstream disconnected");
                        ship_abi = None;
                        if let Some(state) = server_state_db.lock().await.get_mut(&server.endpoint) {
                            state.online = false;
                        }
                        break;
                    }
                    Some(Err(e)) => {
                        tracing::warn!(upstream = %server.endpoint, error = %e, "error reading from upstream");
                        break;
                    }
                }
            };

            handle_monitoring_msg(
                msg,
                &mut server_closed,
                &mut ship_abi,
                &abieos,
                &sender,
                &server_state_db,
                &server,
            )
            .await;

            if server_closed {
                break;
            }
        }

        // Stop this connection's status-ping task before reconnecting.
        status_loop.abort();

        if server_closed {
            break;
        }

        tracing::info!(upstream = %server.endpoint, retry_in_ms = static_config.upstream_reconnect_ms, "reconnecting to upstream");
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = sleep(Duration::from_millis(static_config.upstream_reconnect_ms)) => {}
        }
    }
    tracing::debug!(upstream = %server.endpoint, "monitoring loop stopped");
}

async fn handle_monitoring_msg(
    message: Message,
    server_closed: &mut bool,
    ship_abi: &mut Option<String>,
    abieos: &Arc<Mutex<Abieos>>,
    sender: &WsSender,
    server_state_db: &ServerStateDb,
    server: &Server,
) {
    match message {
        Message::Text(msg) => {
            if ship_abi.is_none() {
                *ship_abi = Some(msg.to_string());
                let abieos = abieos.lock().await;
                match abieos.set_abi_json_native(0u64, ship_abi.as_deref().unwrap()) {
                    Ok(true) => {
                        let mut s = sender.lock().await;
                        if let Err(e) = s.send(Message::Binary(vec![0u8].into())).await {
                            tracing::warn!(upstream = %server.endpoint, error = %e, "failed to send initial status request");
                        }
                    }
                    Ok(false) => {
                        tracing::warn!(upstream = %server.endpoint, "abieos rejected upstream ABI")
                    }
                    Err(_) => {
                        tracing::warn!(upstream = %server.endpoint, "error setting ABI for upstream")
                    }
                }
            } else {
                tracing::debug!(upstream = %server.endpoint, "unexpected text message from upstream");
            }
        }
        Message::Binary(bin_msg) => {
            let result_message = zcd::deserialize_result(&bin_msg);

            let Some(ZCDValues::U8(0)) = result_message.get("variant") else {
                return;
            };
            let Some(ZCDValues::Bytes(bytes)) = result_message.get("data") else {
                return;
            };
            let result = zcd::deserialize_status_result(&bytes);

            let Some(ZCDValues::U32(head_block_num)) = result.get("head_block_num") else {
                return;
            };
            if head_block_num > 0 {
                let mut db = server_state_db.lock().await;
                if let Some(state) = db.get_mut(&server.endpoint) {
                    state.enabled = true;
                    state.online = true;
                    if let Some(ZCDValues::U32(tb)) = result.get("trace_begin_block") {
                        state.trace_begin_block = tb;
                    }
                    if let Some(ZCDValues::U32(te)) = result.get("trace_end_block") {
                        state.trace_end_block = te;
                    }
                    if let Some(ZCDValues::U32(cb)) = result.get("chain_state_begin_block") {
                        state.chain_state_begin_block = cb;
                    }
                    if let Some(ZCDValues::U32(ce)) = result.get("chain_state_end_block") {
                        state.chain_state_end_block = ce;
                    }
                }
            }
        }
        Message::Close(_) => {
            tracing::info!(upstream = %server.endpoint, "received close from upstream");
            *server_closed = true;
        }
        _ => {}
    }
}
