use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
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

use crate::abi::accept_upstream_abi;
use crate::functions::{
    has_alternative_for, parse_blocks_route, select_backend_server, select_failover_server,
    should_reroute_for_finality, RouteRequest,
};
use crate::models::{
    BlockFailureQuarantine, ProxyLimits, ServerConfigDb, ServerStateDb, UpstreamCounters,
};
use crate::telemetry::{self, Event, EventKind, Level};

type UpstreamStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Maximum payload bytes of client frames buffered per session while an upstream
/// is mid-failover. A frame counts as at least one byte, so an empty-frame flood
/// cannot grow the queue without bound. Exceeding the cap closes the session
/// (close code 1008) instead of dropping frames or pinning unbounded memory.
const MAX_PENDING_CLIENT_BYTES: usize = 64 * 1024;

/// Per session, how many times the upstream stream may die with the same next
/// block before the client is closed. The drop that starts the loop counts, so
/// one bad block cannot be re-dialed without a limit.
const MAX_SAME_BLOCK_FAILURES: u32 = 5;

/// First retry waits 100 ms (the historical failover pause). Each further retry
/// at that same block doubles the wait, up to 3.2 s.
fn same_block_backoff(failures: u32) -> Duration {
    // 100, 200, 400, ... capped at 3.2 s. Shift is capped so `100 << shift` fits.
    let shift = failures.saturating_sub(1).min(5);
    Duration::from_millis((100u64 << shift).min(3_200))
}

fn block_unavailable_reason(block: u32) -> String {
    format!("block {block} unavailable on all upstreams")
}

/// Next block a replay should request: the block after the last one delivered,
/// or the in-flight request's start when nothing has been delivered yet.
fn next_block_after(last_seen: Option<u32>, request: Option<&[u8]>) -> Option<u32> {
    if let Some(seen) = last_seen {
        return Some(seen.saturating_add(1));
    }
    request
        .and_then(parse_blocks_route)
        .map(|parsed| parsed.start_block_num)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UpstreamStop {
    Idle,
    /// TCP dropped, or the websocket ended without a Close frame.
    Unclean,
    /// Upstream sent a WebSocket Close frame.
    Clean,
}

struct SameBlockPlan {
    /// Set when the unclean-drop budget at this block is exhausted.
    close_reason: Option<String>,
    delay: Duration,
    /// Prefer an upstream other than the one that just failed.
    exclude_failed: bool,
    failures: u32,
}

/// Decide the next failover step.
///
/// Only an unclean drop consumes the per-block budget. An idle timeout or a
/// clean close still fails over, with the historical 100 ms pause, and does
/// not move `stuck_at`. A client following the chain tip waits at head+1;
/// those waits must not close the session with 1011.
fn plan_same_block_failover(
    stuck_at: &mut Option<(u32, u32)>,
    stop: Option<UpstreamStop>,
    rerouted: bool,
    next_block: Option<u32>,
) -> SameBlockPlan {
    let passthrough = SameBlockPlan {
        close_reason: None,
        delay: Duration::from_millis(100),
        exclude_failed: false,
        failures: 0,
    };
    if rerouted || !matches!(stop, Some(UpstreamStop::Unclean)) {
        return passthrough;
    }
    let Some(block) = next_block else {
        return passthrough;
    };
    let failures = match *stuck_at {
        Some((stuck_block, seen)) if stuck_block == block => seen.saturating_add(1),
        _ => 1,
    };
    *stuck_at = Some((block, failures));
    if failures >= MAX_SAME_BLOCK_FAILURES {
        SameBlockPlan {
            close_reason: Some(block_unavailable_reason(block)),
            delay: Duration::from_millis(100),
            exclude_failed: false,
            failures,
        }
    } else {
        SameBlockPlan {
            close_reason: None,
            delay: same_block_backoff(failures),
            exclude_failed: true,
            failures,
        }
    }
}

/// Client frames held during a failover window, accounted in bytes.
struct PendingClientFrames {
    frames: VecDeque<Message>,
    bytes: usize,
}

impl PendingClientFrames {
    fn new() -> Self {
        Self {
            frames: VecDeque::new(),
            bytes: 0,
        }
    }

    /// Bytes this frame contributes toward [`MAX_PENDING_CLIENT_BYTES`].
    fn accounted(msg: &Message) -> usize {
        msg.len().max(1)
    }

    /// Queue `msg` when it fits. Returns `false` without modifying the queue
    /// when the byte cap would be exceeded (the message is dropped).
    fn try_push(&mut self, msg: Message) -> bool {
        let n = Self::accounted(&msg);
        if self.bytes.saturating_add(n) > MAX_PENDING_CLIENT_BYTES {
            return false;
        }
        self.bytes += n;
        self.frames.push_back(msg);
        true
    }

    fn pop_front(&mut self) -> Option<Message> {
        let msg = self.frames.pop_front()?;
        self.bytes = self.bytes.saturating_sub(Self::accounted(&msg));
        Some(msg)
    }
}

enum FailoverAttempt {
    Shutdown,
    ClientEnded,
    Unavailable,
    Ready(Box<(ConnectionGuard, UpstreamStream, String, String)>),
}

/// Holds one unit of an upstream's active-connection counter. Decrements
/// synchronously (and underflow-safely) when dropped — no spawn, no lock, no
/// dependency on a live runtime handle.
pub struct ConnectionGuard {
    counter: Arc<AtomicUsize>,
    /// Cumulative counters of the same upstream, for traffic accounting.
    counters: Arc<UpstreamCounters>,
}

impl ConnectionGuard {
    /// Count one frame forwarded from this upstream to the client.
    fn note_frame(&self, bytes: usize, is_block: bool) {
        self.counters
            .bytes_total
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
        if is_block {
            self.counters.blocks_total.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        // Saturating decrement: never underflows below zero.
        let _ = self
            .counter
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |c| c.checked_sub(1));
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

async fn mark_offline(backend_servers: &ServerStateDb, endpoint: &str) {
    if let Some(s) = backend_servers.lock().await.get_mut(endpoint) {
        s.online = false;
    }
}

/// Record an unclean mid-`get_blocks` drop. Does not mark the upstream offline.
async fn note_block_failure(
    backend_servers: &ServerStateDb,
    listener: &str,
    endpoint: &str,
    block: u32,
    policy: Option<BlockFailureQuarantine>,
) {
    let mut db = backend_servers.lock().await;
    let Some(state) = db.get_mut(endpoint) else {
        return;
    };
    if state.record_block_failure(block, Instant::now(), policy) {
        let range = policy.map(|item| item.range).unwrap_or(1);
        tracing::warn!(
            upstream = %endpoint,
            block,
            range,
            "upstream quarantined after repeated mid-stream block failures"
        );
        let last = block.saturating_add(range.saturating_sub(1));
        telemetry::global().emit(
            Event::new(
                Level::Warn,
                EventKind::UpstreamQuarantined,
                format!("{} quarantined for blocks {block}..={last}", state.name),
            )
            .listener(listener)
            .upstream(endpoint),
        );
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
///
/// `failover` lets an existing session retry an enabled upstream the monitor
/// has already marked offline. New client sessions pass `false` and still
/// require `online`.
async fn establish_upstream(
    backend_servers: &ServerStateDb,
    server_config_db: &ServerConfigDb,
    max_attempts: u32,
    route: RouteRequest,
    limits: ProxyLimits,
    failover: bool,
    exclude: Option<&str>,
) -> Option<(ConnectionGuard, UpstreamStream, String, String)> {
    for _ in 0..max_attempts {
        // Select a backend and optimistically increment its counter under the lock.
        // The guard decrements on drop, including when this attempt is cancelled
        // (for example because the client session is closing).
        let (cfg, guard) = {
            let mut state = backend_servers.lock().await;
            let selected = match if failover {
                select_failover_server(&mut state, route, exclude)
            } else {
                select_backend_server(&mut state, route)
            } {
                Ok(ep) => ep,
                Err(_) => continue,
            };
            let Some(server_state) = state.get(&selected) else {
                continue;
            };
            let counter = server_state.connections.clone();
            let counters = server_state.counters.clone();
            let Some(cfg) = server_config_db.lock().await.get(&selected).cloned() else {
                continue;
            };
            counter.fetch_add(1, Ordering::Relaxed);
            (cfg, ConnectionGuard { counter, counters })
        };

        let mut config = WebSocketConfig::default();
        config.max_message_size = Some(limits.max_message_bytes);
        config.max_frame_size = Some(limits.max_message_bytes);

        let mut ws = match connect_async_with_config(cfg.ws_url(), Some(config), true).await {
            Ok((ws, _)) => ws,
            Err(e) => {
                tracing::warn!(upstream = %cfg.endpoint, error = %e, "error connecting to upstream");
                drop(guard);
                mark_offline(backend_servers, &cfg.endpoint).await;
                continue;
            }
        };

        match read_first_abi(&mut ws, limits.handshake_timeout_ms).await {
            Some(abi) => {
                tracing::debug!(upstream = %cfg.name, "connected to upstream");
                guard
                    .counters
                    .sessions_total
                    .fetch_add(1, Ordering::Relaxed);
                return Some((guard, ws, abi, cfg.endpoint));
            }
            None => {
                tracing::warn!(upstream = %cfg.endpoint, "invalid or absent ABI handshake; trying another upstream");
                drop(guard);
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

#[allow(clippy::too_many_arguments)]
pub async fn handle_client(
    client_stream: TcpStream,
    client_address: SocketAddr,
    listener: Arc<str>,
    backend_servers: ServerStateDb,
    server_config_db: ServerConfigDb,
    limits: ProxyLimits,
    quarantine: Option<BlockFailureQuarantine>,
    mut shutdown: broadcast::Receiver<()>,
) {
    // 1. Complete the client WebSocket handshake (bounded in time and message size).
    //    Client frames use the smaller client cap; upstream links keep max_message_bytes.
    let mut client_config = WebSocketConfig::default();
    client_config.max_message_size = Some(limits.max_client_message_bytes);
    client_config.max_frame_size = Some(limits.max_client_message_bytes);

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
    // The client has not sent a blocks request yet, so this pick is least-connections.
    // A later `get_blocks_request_v1` with `fetch_finality_data` may reroute.
    let Some((mut active_conn, server_socket, server_abi, endpoint)) = establish_upstream(
        &backend_servers,
        &server_config_db,
        3,
        RouteRequest::default(),
        limits,
        false,
        None,
    )
    .await
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

    // Validate on a fresh handle and drop it. The parsed model is not kept
    // for the session, and it is not registered on a shared context.
    if let Err(e) = accept_upstream_abi(&server_abi) {
        tracing::error!(error = %e, "error setting ABI from upstream");
        return;
    }

    // Visible to the dashboard and the control API until this function returns.
    let registration =
        telemetry::global().register_session(listener.clone(), client_address, &endpoint);
    let session = registration.info.clone();

    let (client_writer, mut client_reader) = client_websocket.split();
    let client_writer = Arc::new(Mutex::new(client_writer));
    let current_endpoint = Arc::new(Mutex::new(endpoint));
    let reroute_notify = Arc::new(Notify::new());

    let (server_writer, server_reader_stream) = server_socket.split();
    let server_reader = Arc::new(Mutex::new(server_reader_stream));
    let server_writer = Arc::new(Mutex::new(server_writer));

    // Raw bytes of the latest get_blocks_request (v0/v1) for failover replay, and
    // client frames that failed to forward during a failover window.
    let last_request: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    let pending_client_frames: Arc<Mutex<PendingClientFrames>> =
        Arc::new(Mutex::new(PendingClientFrames::new()));
    let pending_limit_hit = Arc::new(AtomicBool::new(false));

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
        let pending_limit_hit = pending_limit_hit.clone();
        let disconnected = client_disconnected.clone();
        let notify = client_disconnect_notify.clone();
        let idle_ms = limits.idle_timeout_ms;
        let backend_servers = backend_servers.clone();
        let current_endpoint = current_endpoint.clone();
        let reroute_notify = reroute_notify.clone();
        let session = session.clone();
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
                // A v1 request that asks for finality data is not forwarded when
                // this upstream's range misses and another upstream covers it.
                if let Message::Binary(ref bin) = msg {
                    if let Some(parsed) = parse_blocks_route(bin) {
                        session.note_request(parsed.start_block_num, parsed.fetch_finality_data);
                        *last_request.lock().await = Some(bin.to_vec());
                        let endpoint = current_endpoint.lock().await.clone();
                        let start = parsed.start_block_num;
                        // Quarantine is opt-in. Skip the state lock on the common path.
                        let quarantined = quarantine.is_some() && {
                            let mut db = backend_servers.lock().await;
                            db.get_mut(&endpoint)
                                .is_some_and(|state| state.quarantined_for(start, Instant::now()))
                        };
                        let reroute_finality = parsed.fetch_finality_data && {
                            let db = backend_servers.lock().await;
                            should_reroute_for_finality(&db, &endpoint, start)
                        };
                        if quarantined || reroute_finality {
                            if quarantined {
                                tracing::info!(
                                    upstream = %endpoint,
                                    block = start,
                                    "upstream quarantined for requested block; rerouting"
                                );
                            } else {
                                tracing::info!(
                                    upstream = %endpoint,
                                    block = start,
                                    "upstream finality range does not cover request; rerouting"
                                );
                            }
                            reroute_notify.notify_one();
                            continue;
                        }
                    }
                }

                let exceeded = {
                    let mut sw = server_writer.lock().await;
                    if let Err(e) = sw.send(msg.clone()).await {
                        // Don't silently drop: buffer for replay after failover swaps the
                        // socket. Keep holding `server_writer` while pushing to `pending`
                        // so the failover loop cannot acquire the writer, swap, and flush
                        // `pending` before this frame is buffered (which would strand it).
                        tracing::debug!(error = %e, "forward to upstream failed; buffering for failover");
                        let frame_bytes = PendingClientFrames::accounted(&msg);
                        let mut p = pending.lock().await;
                        let buffered = p.bytes;
                        if p.try_push(msg) {
                            false
                        } else {
                            tracing::warn!(
                                buffered,
                                frame_bytes,
                                cap = MAX_PENDING_CLIENT_BYTES,
                                "pending client buffer exceeded; closing session"
                            );
                            true
                        }
                    } else {
                        false
                    }
                };
                if exceeded {
                    pending_limit_hit.store(true, Ordering::Release);
                    break;
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
    // (next block, failures observed at that block). Cleared once a block at
    // or past it is delivered.
    let mut stuck_at: Option<(u32, u32)> = None;
    let mut close_for_pending_limit = false;

    'session: loop {
        let mut client_down = false;
        let mut rerouted = false;
        let mut upstream_stop: Option<UpstreamStop> = None;
        let mut upstream_sent_close = false;
        let mut operator_close = false;
        let mut operator_move = false;
        // Kept across iterations so a permit is not lost when another branch wins.
        let reroute_fut = reroute_notify.notified();
        tokio::pin!(reroute_fut);
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
                    _ = &mut reroute_fut => { rerouted = true; break; }
                    _ = session.control.notified() => {
                        if session.close_requested() {
                            operator_close = true;
                            break;
                        }
                        if session.take_move_request() {
                            let current = current_endpoint.lock().await.clone();
                            let next_block = session.next_block();
                            if has_alternative_for(&*backend_servers.lock().await, &current, next_block) {
                                operator_move = true;
                                rerouted = true;
                                break;
                            }
                            tracing::info!(upstream = %current, ?next_block, "operator move skipped: no other upstream can serve the next block");
                        }
                        continue;
                    }
                    r = timeout(Duration::from_millis(idle_ms), sr_guard.next()) => match r {
                        Ok(Some(Ok(m))) => Some(m),
                        Ok(_) => {
                            upstream_stop = Some(if upstream_sent_close {
                                UpstreamStop::Clean
                            } else {
                                UpstreamStop::Unclean
                            });
                            None
                        }
                        Err(_) => {
                            tracing::debug!("upstream idle timeout; failing over");
                            upstream_stop = Some(UpstreamStop::Idle);
                            None
                        }
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
                    _ = &mut reroute_fut => { rerouted = true; break; }
                    _ = session.control.notified() => {
                        if session.close_requested() {
                            operator_close = true;
                            break;
                        }
                        if session.take_move_request() {
                            let current = current_endpoint.lock().await.clone();
                            let next_block = session.next_block();
                            if has_alternative_for(&*backend_servers.lock().await, &current, next_block) {
                                operator_move = true;
                                rerouted = true;
                                break;
                            }
                            tracing::info!(upstream = %current, ?next_block, "operator move skipped: no other upstream can serve the next block");
                        }
                        continue;
                    }
                    r = sr_guard.next() => match r {
                        Some(Ok(m)) => Some(m),
                        _ => {
                            upstream_stop = Some(if upstream_sent_close {
                                UpstreamStop::Clean
                            } else {
                                UpstreamStop::Unclean
                            });
                            None
                        }
                    }
                }
            };

            let Some(msg) = next_msg else { break };

            if matches!(msg, Message::Close(_)) {
                upstream_sent_close = true;
            }

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
                if stuck_at.is_some_and(|(block, _)| b_num >= block) {
                    stuck_at = None;
                }
                last_seen_block = Some(b_num);
            }

            let frame_len = msg.len();
            active_conn.note_frame(frame_len, current_block.is_some());
            session.note_frame(frame_len, current_block);

            if let Err(e) = cw_guard.send(msg).await {
                tracing::warn!(error = %e, "error sending message to client");
                client_down = true;
                break;
            }
        }

        if client_down || client_disconnected.load(Ordering::Acquire) {
            // `notified()` can lose its permit if another select branch wins, so
            // re-read the flag the client task sets before it notifies.
            close_for_pending_limit = pending_limit_hit.load(Ordering::Acquire);
            break 'session;
        }

        if operator_close {
            tracing::info!("session closed by operator");
            let _ = cw_guard
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Normal,
                    reason: "closed by operator".into(),
                })))
                .await;
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
        let failed_endpoint = current_endpoint.lock().await.clone();
        let next_block = {
            let lr = last_request.lock().await;
            next_block_after(last_seen_block, lr.as_deref())
        };
        // Strikes and the per-block budget count only an unclean drop. An idle
        // timeout or a clean close still fails over, but a session waiting at
        // the chain tip must not be closed with 1011.
        let budget = plan_same_block_failover(&mut stuck_at, upstream_stop, rerouted, next_block);
        if budget.failures > 0 {
            if let Some(block) = next_block {
                note_block_failure(
                    &backend_servers,
                    &listener,
                    &failed_endpoint,
                    block,
                    quarantine,
                )
                .await;
            }
        }
        if let Some(reason) = budget.close_reason {
            if let Some(block) = next_block {
                tracing::error!(
                    upstream = %failed_endpoint,
                    block,
                    attempts = budget.failures,
                    "block {block} unavailable on all upstreams"
                );
            }
            telemetry::global().emit(
                Event::new(Level::Error, EventKind::SessionFailed, reason.clone())
                    .listener(&listener)
                    .upstream(&failed_endpoint)
                    .session(session.id),
            );
            let _ = cw_guard
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Error,
                    reason: reason.into(),
                })))
                .await;
            break 'session;
        }

        let exclude = (budget.exclude_failed || operator_move).then_some(failed_endpoint.as_str());
        let retry_delay = budget.delay;
        let failover_reason = if operator_move {
            "operator move"
        } else if rerouted {
            "reroute"
        } else {
            match upstream_stop {
                Some(UpstreamStop::Unclean) => "stream dropped",
                Some(UpstreamStop::Clean) => "upstream closed the stream",
                Some(UpstreamStop::Idle) => "upstream idle",
                None => "stream ended",
            }
        };
        if operator_move {
            tracing::info!(upstream = %failed_endpoint, "moving session at operator request");
        } else if rerouted {
            tracing::info!("selecting an upstream whose finality range covers the request");
        } else if budget.failures > 0 {
            if let Some(block) = next_block {
                tracing::warn!(
                    upstream = %failed_endpoint,
                    block,
                    attempt = budget.failures,
                    max = MAX_SAME_BLOCK_FAILURES,
                    "upstream stream ended; attempting failover"
                );
            }
        } else {
            tracing::warn!(
                upstream = %failed_endpoint,
                "upstream stream ended; attempting failover"
            );
        }

        let route = {
            let lr = last_request.lock().await;
            lr.as_deref()
                .and_then(parse_blocks_route)
                .map(|parsed| RouteRequest {
                    block: Some(parsed.start_block_num),
                    fetch_finality_data: parsed.fetch_finality_data,
                })
                .unwrap_or_default()
        };

        if client_disconnected.load(Ordering::Acquire) {
            close_for_pending_limit = pending_limit_hit.load(Ordering::Acquire);
            break 'session;
        }

        // Pause before dialing. The first retry at a block waits 100 ms; further
        // retries at that same block back off. Shutdown and client disconnect
        // interrupt the wait. A reroute keeps the historical 100 ms pause.
        enum Pause {
            Shutdown,
            ClientEnded,
            Elapsed,
        }
        let paused = tokio::select! {
            _ = shutdown.recv() => Pause::Shutdown,
            _ = client_disconnect_notify.notified() => Pause::ClientEnded,
            _ = sleep(retry_delay) => Pause::Elapsed,
        };
        match paused {
            Pause::Shutdown => {
                let _ = cw_guard
                    .send(Message::Close(Some(CloseFrame {
                        code: CloseCode::Away,
                        reason: "server shutting down".into(),
                    })))
                    .await;
                return;
            }
            Pause::ClientEnded => {
                close_for_pending_limit = pending_limit_hit.load(Ordering::Acquire);
                break 'session;
            }
            Pause::Elapsed => {}
        }

        // Failover can block in the upstream handshake. Race it against client
        // disconnect and shutdown so a pending-buffer close is not stuck behind
        // a hung connect. `establish_upstream` drops its connection guard if
        // cancelled.
        let attempt = tokio::select! {
            _ = shutdown.recv() => FailoverAttempt::Shutdown,
            _ = client_disconnect_notify.notified() => FailoverAttempt::ClientEnded,
            result = establish_upstream(
                &backend_servers,
                &server_config_db,
                3,
                route,
                limits,
                true,
                exclude,
            ) => match result {
                Some(established) => FailoverAttempt::Ready(Box::new(established)),
                None => FailoverAttempt::Unavailable,
            },
        };
        let (new_conn, new_stream, new_abi, endpoint) = match attempt {
            FailoverAttempt::Shutdown => {
                let _ = cw_guard
                    .send(Message::Close(Some(CloseFrame {
                        code: CloseCode::Away,
                        reason: "server shutting down".into(),
                    })))
                    .await;
                return;
            }
            FailoverAttempt::ClientEnded => {
                close_for_pending_limit = pending_limit_hit.load(Ordering::Acquire);
                break 'session;
            }
            FailoverAttempt::Unavailable => {
                tracing::error!("no upstream available after failover; closing client");
                telemetry::global().emit(
                    Event::new(
                        Level::Error,
                        EventKind::SessionFailed,
                        "no upstream available after failover",
                    )
                    .listener(&listener)
                    .upstream(&failed_endpoint)
                    .session(session.id),
                );
                let _ = cw_guard
                    .send(Message::Close(Some(CloseFrame {
                        code: CloseCode::Error,
                        reason: "no upstream available".into(),
                    })))
                    .await;
                break 'session;
            }
            FailoverAttempt::Ready(established) => *established,
        };
        active_conn
            .counters
            .failovers_total
            .fetch_add(1, Ordering::Relaxed);
        active_conn = new_conn; // drops the old guard (synchronous decrement)
        session.note_failover();
        session.set_upstream(&endpoint);
        telemetry::global().emit(
            Event::new(
                if operator_move || rerouted {
                    Level::Info
                } else {
                    Level::Warn
                },
                EventKind::SessionFailover,
                format!("moved from {failed_endpoint} to {endpoint} ({failover_reason})"),
            )
            .listener(&listener)
            .upstream(&endpoint)
            .session(session.id),
        );
        *current_endpoint.lock().await = endpoint;

        if let Err(e) = accept_upstream_abi(&new_abi) {
            tracing::error!(error = %e, "error setting ABI after failover");
            break 'session;
        }

        // Swap in the new socket.
        {
            let (writer, reader) = new_stream.split();
            *sr_guard = reader;
            *server_writer.lock().await = writer;
        }

        // Replay the request and flush any buffered client frames. The backoff
        // already ran before the dial, so a healthy failover still waits 100 ms
        // and does not pause twice.
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

    if close_for_pending_limit {
        let _ = cw_guard
            .send(Message::Close(Some(CloseFrame {
                code: CloseCode::Policy,
                reason: "pending client buffer exceeded".into(),
            })))
            .await;
    }

    let _ = active_conn; // keep the guard alive for the whole session
    drop(registration);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binary(n: usize) -> Message {
        Message::binary(vec![0u8; n])
    }

    #[test]
    fn pending_buffer_accepts_frames_until_the_byte_cap() {
        let mut pending = PendingClientFrames::new();
        assert!(pending.try_push(binary(1024)));
        assert!(pending.try_push(binary(MAX_PENDING_CLIENT_BYTES - 1024)));
        assert_eq!(pending.bytes, MAX_PENDING_CLIENT_BYTES);
        assert_eq!(pending.frames.len(), 2);
        assert!(!pending.try_push(binary(1)));
        assert_eq!(pending.frames.len(), 2);
        assert_eq!(pending.bytes, MAX_PENDING_CLIENT_BYTES);
    }

    #[test]
    fn pending_buffer_rejects_a_single_oversized_frame() {
        let mut pending = PendingClientFrames::new();
        assert!(!pending.try_push(binary(MAX_PENDING_CLIENT_BYTES + 1)));
        assert!(pending.frames.is_empty());
        assert_eq!(pending.bytes, 0);
        assert!(pending.try_push(binary(1)));
    }

    #[test]
    fn pending_buffer_exact_cap_fits_and_pop_frees_budget() {
        let mut pending = PendingClientFrames::new();
        assert!(pending.try_push(binary(MAX_PENDING_CLIENT_BYTES)));
        assert!(!pending.try_push(binary(1)));
        let got = pending.pop_front().unwrap();
        assert_eq!(got.len(), MAX_PENDING_CLIENT_BYTES);
        assert_eq!(pending.bytes, 0);
        assert!(pending.pop_front().is_none());
        assert!(pending.try_push(binary(MAX_PENDING_CLIENT_BYTES)));
    }

    #[test]
    fn pending_buffer_counts_empty_frames_as_one_byte() {
        let mut pending = PendingClientFrames::new();
        assert!(pending.try_push(binary(MAX_PENDING_CLIENT_BYTES - 1)));
        assert!(pending.try_push(Message::binary(Vec::<u8>::new())));
        assert_eq!(pending.bytes, MAX_PENDING_CLIENT_BYTES);
        assert!(!pending.try_push(Message::text("")));
        assert_eq!(pending.frames.len(), 2);
    }

    #[test]
    fn pending_buffer_counts_text_and_binary_payloads() {
        let mut pending = PendingClientFrames::new();
        assert!(pending.try_push(Message::text("abcd")));
        assert!(pending.try_push(Message::binary(vec![1, 2, 3])));
        assert_eq!(pending.bytes, 7);
        let first = pending.pop_front().unwrap();
        assert_eq!(first.len(), 4);
        assert_eq!(pending.bytes, 3);
    }

    #[test]
    fn same_block_backoff_doubles_from_100ms_and_caps_at_3_2s() {
        assert_eq!(same_block_backoff(1), Duration::from_millis(100));
        assert_eq!(same_block_backoff(2), Duration::from_millis(200));
        assert_eq!(same_block_backoff(3), Duration::from_millis(400));
        assert_eq!(same_block_backoff(4), Duration::from_millis(800));
        assert_eq!(same_block_backoff(5), Duration::from_millis(1_600));
        assert_eq!(same_block_backoff(6), Duration::from_millis(3_200));
        assert_eq!(same_block_backoff(7), Duration::from_millis(3_200));
        assert_eq!(same_block_backoff(30), Duration::from_millis(3_200));
    }

    #[test]
    fn block_unavailable_reason_names_the_block() {
        assert_eq!(
            block_unavailable_reason(115),
            "block 115 unavailable on all upstreams"
        );
    }

    #[test]
    fn idle_and_clean_stops_at_the_tip_do_not_exhaust_the_block_budget() {
        let tip = Some(115u32);
        for stop in [UpstreamStop::Idle, UpstreamStop::Clean] {
            let mut stuck_at = None;
            for _ in 0..8 {
                let plan = plan_same_block_failover(&mut stuck_at, Some(stop), false, tip);
                assert!(
                    plan.close_reason.is_none(),
                    "{stop:?} must not close with 1011"
                );
                assert_eq!(plan.delay, Duration::from_millis(100));
                assert!(!plan.exclude_failed);
                assert_eq!(plan.failures, 0);
            }
            assert!(
                stuck_at.is_none(),
                "{stop:?} must not count toward the same-block budget"
            );
        }
    }

    #[test]
    fn unclean_drops_at_the_same_block_still_close_after_five() {
        let mut stuck_at = None;
        for expected in 1..MAX_SAME_BLOCK_FAILURES {
            let plan = plan_same_block_failover(
                &mut stuck_at,
                Some(UpstreamStop::Unclean),
                false,
                Some(115),
            );
            assert!(plan.close_reason.is_none());
            assert!(plan.exclude_failed);
            assert_eq!(plan.failures, expected);
            assert_eq!(plan.delay, same_block_backoff(expected));
        }
        let plan =
            plan_same_block_failover(&mut stuck_at, Some(UpstreamStop::Unclean), false, Some(115));
        assert_eq!(
            plan.close_reason.as_deref(),
            Some("block 115 unavailable on all upstreams")
        );
        assert_eq!(plan.failures, MAX_SAME_BLOCK_FAILURES);
    }

    #[test]
    fn next_block_follows_the_last_delivered_block() {
        let request = {
            let mut buf = vec![1u8];
            buf.extend_from_slice(&100u32.to_le_bytes());
            buf.extend_from_slice(&200u32.to_le_bytes());
            buf
        };
        assert_eq!(next_block_after(Some(114), Some(&request)), Some(115));
        assert_eq!(next_block_after(None, Some(&request)), Some(100));
        assert_eq!(next_block_after(None, None), None);
    }
}
