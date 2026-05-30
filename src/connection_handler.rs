use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use rs_abieos::Abieos;
use tokio::net::TcpStream;
use tokio::spawn;
use tokio::sync::{broadcast, Mutex, Notify};
use tokio::time::{sleep, timeout};
use tokio_tungstenite::{
    accept_async_with_config, connect_async_with_config, tungstenite, MaybeTlsStream,
    WebSocketStream,
};
use tungstenite::protocol::frame::coding::CloseCode;
use tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tungstenite::Message;

use crate::functions::select_backend_server;
use crate::models::{ProxyLimits, ServerConfigDb, ServerStateDb};

type UpstreamStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Maximum client frames buffered while an upstream is mid-failover, after which
/// further frames during the (typically sub-second) swap window are dropped.
const MAX_PENDING_FRAMES: usize = 1024;

/// Holds one unit of an upstream's active-connection counter. Decrements
/// synchronously (and underflow-safely) when dropped — no spawn, no lock, no
/// dependency on a live runtime handle.
pub struct ConnectionGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        // Saturating decrement: never underflows below zero.
        let _ = self
            .counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| c.checked_sub(1));
    }
}

/// Aborts the wrapped task when dropped, so the client->server forwarding task
/// never outlives its session.
struct AbortOnDrop(tokio::task::JoinHandle<()>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn dec(counter: &AtomicUsize) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| c.checked_sub(1));
}

async fn mark_offline(backend_servers: &ServerStateDb, endpoint: &str) {
    if let Some(s) = backend_servers.lock().await.get_mut(endpoint) {
        s.online = false;
    }
}

/// Read the upstream's first meaningful frame, which must be the Text ABI.
/// Leading Ping/Pong control frames are skipped. Returns `None` (so the caller
/// can try another upstream) on timeout, Close, Binary-first, or error.
async fn read_first_abi(ws: &mut UpstreamStream, handshake_timeout_ms: u64) -> Option<String> {
    // Single overall deadline for the whole handshake: a peer that drips
    // Ping/Pong frames cannot keep it alive past the timeout.
    let read = async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => return Some(text.to_string()),
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                _ => return None,
            }
        }
    };
    if handshake_timeout_ms > 0 {
        // On timeout (Err), unwrap_or_default yields None — treat as a failed handshake.
        timeout(Duration::from_millis(handshake_timeout_ms), read)
            .await
            .unwrap_or_default()
    } else {
        read.await
    }
}

/// Select, connect to, and complete the ABI handshake with an upstream, retrying
/// against other upstreams (up to `max_attempts`) on failure. Returns the
/// connection guard, the live socket, and the upstream's ABI JSON.
async fn establish_upstream(
    backend_servers: &ServerStateDb,
    server_config_db: &ServerConfigDb,
    max_attempts: u32,
    requested_block: Option<u32>,
    limits: ProxyLimits,
) -> Option<(ConnectionGuard, UpstreamStream, String)> {
    for _ in 0..max_attempts {
        // Select a backend and optimistically increment its counter under the lock.
        let (cfg, counter) = {
            let state = backend_servers.lock().await;
            let selected = match select_backend_server(&state, requested_block) {
                Ok(ep) => ep,
                Err(_) => continue,
            };
            let Some(server_state) = state.get(&selected) else {
                continue;
            };
            let counter = server_state.connections.clone();
            let Some(cfg) = server_config_db.lock().await.get(&selected).cloned() else {
                continue;
            };
            counter.fetch_add(1, Ordering::Relaxed);
            (cfg, counter)
        };

        let mut config = WebSocketConfig::default();
        config.max_message_size = Some(limits.max_message_bytes);
        config.max_frame_size = Some(limits.max_message_bytes);

        let mut ws = match connect_async_with_config(cfg.ws_url(), Some(config), true).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                tracing::warn!(upstream = %cfg.endpoint, error = %e, "error connecting to upstream");
                dec(&counter);
                mark_offline(backend_servers, &cfg.endpoint).await;
                continue;
            }
        };

        match read_first_abi(&mut ws, limits.handshake_timeout_ms).await {
            Some(abi) => {
                tracing::debug!(upstream = %cfg.name, "connected to upstream");
                return Some((ConnectionGuard { counter }, ws, abi));
            }
            None => {
                tracing::warn!(upstream = %cfg.endpoint, "invalid or absent ABI handshake; trying another upstream");
                dec(&counter);
                mark_offline(backend_servers, &cfg.endpoint).await;
                let _ = ws.close(None).await;
                continue;
            }
        }
    }
    tracing::error!(
        attempts = max_attempts,
        "unable to establish an upstream connection"
    );
    None
}

/// Extract the `this_block.block_num` from a `get_blocks_result_v0/v1` frame, if
/// present. Layout: variant(1) + head(36) + lib(36) + this_block_flag(1) @73 +
/// block_num(4) @74. Returns `None` for any other frame (status, head keep-alive).
fn extract_block_num(msg: &Message) -> Option<u32> {
    if let Message::Binary(bin) = msg {
        if bin.len() >= 78 && (bin[0] == 1 || bin[0] == 2) && bin[73] == 1 {
            if let Ok(bytes) = bin[74..78].try_into() {
                return Some(u32::from_le_bytes(bytes));
            }
        }
    }
    None
}

pub async fn handle_client(
    client_stream: TcpStream,
    _client_address: SocketAddr,
    backend_servers: ServerStateDb,
    server_config_db: ServerConfigDb,
    shared_abieos: Arc<Mutex<Abieos>>,
    limits: ProxyLimits,
    mut shutdown: broadcast::Receiver<()>,
) {
    // 1. Complete the client WebSocket handshake (bounded in time and message size).
    let mut client_config = WebSocketConfig::default();
    client_config.max_message_size = Some(limits.max_message_bytes);
    client_config.max_frame_size = Some(limits.max_message_bytes);

    let accept_fut = accept_async_with_config(client_stream, Some(client_config));
    let mut client_websocket = if limits.handshake_timeout_ms > 0 {
        match timeout(
            Duration::from_millis(limits.handshake_timeout_ms),
            accept_fut,
        )
        .await
        {
            Ok(Ok(ws)) => ws,
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "client websocket handshake failed");
                return;
            }
            Err(_) => {
                tracing::warn!("client websocket handshake timed out");
                return;
            }
        }
    } else {
        match accept_fut.await {
            Ok(ws) => ws,
            Err(e) => {
                tracing::warn!(error = %e, "client websocket handshake failed");
                return;
            }
        }
    };

    tracing::info!("client connected");

    // 2. Establish the first upstream and read its ABI.
    let Some((mut active_conn, server_socket, server_abi)) =
        establish_upstream(&backend_servers, &server_config_db, 3, None, limits).await
    else {
        tracing::error!("no upstream available; closing client");
        let _ = client_websocket
            .close(Some(CloseFrame {
                code: CloseCode::Error,
                reason: "No upstream server available".into(),
            }))
            .await;
        return;
    };

    // Validate the ABI against the shared abieos context.
    {
        let abieos = shared_abieos.lock().await;
        if let Err(e) = abieos.set_abi_json("0", &server_abi) {
            tracing::error!(error = %e, "error setting ABI from upstream");
            return;
        }
    }

    let (client_writer, mut client_reader) = client_websocket.split();
    let client_writer = Arc::new(Mutex::new(client_writer));

    let (server_writer, server_reader_stream) = server_socket.split();
    let server_reader = Arc::new(Mutex::new(server_reader_stream));
    let server_writer = Arc::new(Mutex::new(server_writer));

    // Raw bytes of the latest get_blocks_request (v0/v1) for failover replay, and
    // client frames that failed to forward during a failover window.
    let last_request: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let pending_client_frames: Arc<Mutex<VecDeque<Message>>> =
        Arc::new(Mutex::new(VecDeque::new()));

    // Forward the ABI to the client.
    {
        let mut cw = client_writer.lock().await;
        if let Err(e) = cw.send(Message::Text(server_abi.as_str().into())).await {
            tracing::warn!(error = %e, "error sending ABI to client");
            return;
        }
    }
    tracing::debug!("sent ABI to client");

    // 3. Spawn the client -> server forwarding task.
    let client_disconnected = Arc::new(AtomicBool::new(false));
    let client_disconnect_notify = Arc::new(Notify::new());

    let _c2s = {
        let last_request = last_request.clone();
        let server_writer = server_writer.clone();
        let pending = pending_client_frames.clone();
        let disconnected = client_disconnected.clone();
        let notify = client_disconnect_notify.clone();
        let idle_ms = limits.idle_timeout_ms;
        AbortOnDrop(spawn(async move {
            loop {
                let next = if idle_ms > 0 {
                    match timeout(Duration::from_millis(idle_ms), client_reader.next()).await {
                        Ok(n) => n,
                        Err(_) => {
                            tracing::info!("client idle timeout");
                            break;
                        }
                    }
                } else {
                    client_reader.next().await
                };
                let Some(Ok(msg)) = next else { break };

                // Record the latest blocks-request (v0=1, v1=3) for failover replay.
                if let Message::Binary(ref bin) = msg {
                    if bin.len() >= 5 && (bin[0] == 1 || bin[0] == 3) {
                        *last_request.lock().await = Some(bin.to_vec());
                    }
                }

                let mut sw = server_writer.lock().await;
                if let Err(e) = sw.send(msg.clone()).await {
                    // Don't silently drop: buffer for replay after failover swaps the
                    // socket. Keep holding `server_writer` while pushing to `pending`
                    // so the failover loop cannot acquire the writer, swap, and flush
                    // `pending` before this frame is buffered (which would strand it).
                    tracing::debug!(error = %e, "forward to upstream failed; buffering for failover");
                    let mut p = pending.lock().await;
                    if p.len() < MAX_PENDING_FRAMES {
                        p.push_back(msg);
                    } else {
                        tracing::warn!("pending client-frame buffer full; dropping frame");
                    }
                }
            }
            disconnected.store(true, Ordering::Release);
            notify.notify_one();
            tracing::debug!("client stream ended");
        }))
    };

    // 4. Server -> client loop with inline de-duplication and failover.
    let mut cw_guard = client_writer.lock().await;
    let mut sr_guard = server_reader.lock().await;
    let idle_ms = limits.idle_timeout_ms;

    let mut restarting = false;
    let mut last_seen_block: Option<u32> = None;

    'session: loop {
        let mut client_down = false;
        loop {
            if client_disconnected.load(Ordering::Acquire) {
                client_down = true;
                break;
            }

            let next_msg: Option<Message> = if idle_ms > 0 {
                tokio::select! {
                    _ = shutdown.recv() => {
                        let _ = cw_guard.send(Message::Close(Some(CloseFrame {
                            code: CloseCode::Away,
                            reason: "server shutting down".into(),
                        }))).await;
                        return;
                    }
                    _ = client_disconnect_notify.notified() => { client_down = true; break; }
                    r = timeout(Duration::from_millis(idle_ms), sr_guard.next()) => match r {
                        Ok(Some(Ok(m))) => Some(m),
                        Ok(_) => None,
                        Err(_) => { tracing::debug!("upstream idle timeout; failing over"); None }
                    }
                }
            } else {
                tokio::select! {
                    _ = shutdown.recv() => {
                        let _ = cw_guard.send(Message::Close(Some(CloseFrame {
                            code: CloseCode::Away,
                            reason: "server shutting down".into(),
                        }))).await;
                        return;
                    }
                    _ = client_disconnect_notify.notified() => { client_down = true; break; }
                    r = sr_guard.next() => match r {
                        Some(Ok(m)) => Some(m),
                        _ => None,
                    }
                }
            };

            let Some(msg) = next_msg else { break };

            let current_block = extract_block_num(&msg);

            // During failover, drop only duplicate block frames; forward everything
            // else (status results, head keep-alives) so the client never stalls.
            if restarting {
                if let Some(b_num) = current_block {
                    let expected = last_seen_block.map_or(0, |b| b.saturating_add(1));
                    if b_num < expected {
                        continue; // duplicate replayed block
                    }
                    restarting = false;
                }
            }

            if let Some(b_num) = current_block {
                last_seen_block = Some(b_num);
            }

            if let Err(e) = cw_guard.send(msg).await {
                tracing::warn!(error = %e, "error sending message to client");
                client_down = true;
                break;
            }
        }

        if client_down {
            break 'session;
        }

        // Upstream ended — patch the replay request to resume at the next block.
        {
            let mut lr = last_request.lock().await;
            if let Some(req) = lr.as_mut() {
                if let Some(lb) = last_seen_block {
                    let next_block = lb.saturating_add(1);
                    if req.len() >= 5 {
                        req[1..5].copy_from_slice(&next_block.to_le_bytes());
                    }
                }
            }
        }
        tracing::warn!("upstream stream ended; attempting failover");

        let failover_block = {
            let lr = last_request.lock().await;
            lr.as_ref().and_then(|req| {
                if req.len() >= 5 {
                    req[1..5].try_into().ok().map(u32::from_le_bytes)
                } else {
                    None
                }
            })
        };

        let Some((new_conn, new_stream, new_abi)) = establish_upstream(
            &backend_servers,
            &server_config_db,
            3,
            failover_block,
            limits,
        )
        .await
        else {
            tracing::error!("no upstream available after failover; closing client");
            let _ = cw_guard
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Error,
                    reason: "no upstream available".into(),
                })))
                .await;
            break 'session;
        };
        active_conn = new_conn; // drops the old guard (synchronous decrement)

        {
            let abieos = shared_abieos.lock().await;
            if let Err(e) = abieos.set_abi_json("0", &new_abi) {
                tracing::error!(error = %e, "error setting ABI after failover");
                break 'session;
            }
        }

        // Swap in the new socket.
        {
            let (writer, reader) = new_stream.split();
            *sr_guard = reader;
            *server_writer.lock().await = writer;
        }

        // Brief pause, then replay the request and flush any buffered client frames.
        sleep(Duration::from_millis(100)).await;
        {
            let mut sw = server_writer.lock().await;
            let lr = last_request.lock().await;
            if let Some(req) = lr.as_ref() {
                if let Err(e) = sw.send(Message::Binary(req.clone().into())).await {
                    tracing::warn!(error = %e, "error replaying request after failover");
                    break 'session;
                }
            }
            drop(lr);
            let mut pending = pending_client_frames.lock().await;
            while let Some(frame) = pending.pop_front() {
                if let Err(e) = sw.send(frame).await {
                    tracing::warn!(error = %e, "error flushing buffered client frame after failover");
                    break;
                }
            }
        }

        tracing::info!("reconnected to upstream after failover");
        restarting = true;
    }

    let _ = active_conn; // keep the guard alive for the whole session
}
