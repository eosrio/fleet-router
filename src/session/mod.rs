//! Client session actor: one task per client connection.
//!
//! A single `biased` [`tokio::select!`] loop owns the client socket, the
//! upstream link, the dial task and the session's timers:
//!
//! - client frames are forwarded to the link as they arrive;
//! - upstream frames are written to the client inline, so a session holds at
//!   most the one message being written;
//! - a failover is a backoff deadline followed by a dial task that reports
//!   back into the same loop, so the client keeps being read (and shutdown
//!   keeps being honored) while an upstream handshake is in progress.
//!
//! Failover replays the latest `get_blocks` request as raw bytes with its
//! start moved past the last delivered block, and drops replayed block results
//! by number until the stream catches up. Status requests and every other
//! frame pass through unchanged in both directions, upstream Close frames
//! included. Client frames that arrive while no link is up wait in a
//! byte-capped buffer and are flushed after the replay.

mod client;
mod failover;
mod link;

use std::net::SocketAddr;
use std::time::Duration;

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::{sleep_until, timeout, Instant};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{accept_async_with_config, WebSocketStream};

use crate::abi::accept_upstream_abi;
use crate::functions::{parse_blocks_route, should_reroute_for_finality, ParsedBlocksRoute};
use crate::models::{BlockFailureQuarantine, ProxyLimits, ServerConfigDb, ServerStateDb};

use client::{extract_block_num, PendingClientFrames, MAX_PENDING_CLIENT_BYTES};
use failover::{
    failover_route, next_block_after, note_block_failure, plan_same_block_failover,
    resume_request_at, UpstreamStop, MAX_SAME_BLOCK_FAILURES,
};
pub use link::ConnectionGuard;
use link::{
    pick_upstream, report_dial_failure, DialError, DialPlan, DialTask, Established, Link,
    MAX_DIAL_ATTEMPTS,
};

type ClientWs = WebSocketStream<TcpStream>;

/// Whether the session loop keeps running after an event.
#[must_use]
enum Flow {
    Continue,
    Stop,
}

enum Phase {
    /// A link is up; frames flow both ways.
    Streaming,
    /// The link ended. The next dial for `plan` starts at `until`.
    Backoff { until: Instant, plan: DialPlan },
    /// A dial task for `plan` is running in `Session::dial`.
    Dialing { plan: DialPlan },
}

pub async fn handle_client(
    client_stream: TcpStream,
    _client_address: SocketAddr,
    backend_servers: ServerStateDb,
    server_config_db: ServerConfigDb,
    limits: ProxyLimits,
    quarantine: Option<BlockFailureQuarantine>,
    shutdown: broadcast::Receiver<()>,
) {
    let Some(client_websocket) = accept_client(client_stream, limits).await else {
        return;
    };
    tracing::info!("client connected");

    let (client_tx, client_rx) = client_websocket.split();
    let mut session = Session {
        backend_servers,
        server_config_db,
        limits,
        quarantine,
        shutdown,
        client_tx,
        client_rx,
        client_idle_from: Instant::now(),
        link: None,
        endpoint: String::new(),
        retired_guard: None,
        dial: None,
        dial_attempts: 0,
        phase: Phase::Streaming,
        last_request: None,
        last_seen_block: None,
        restarting: false,
        stuck_at: None,
        reroute_pending: false,
        pending: PendingClientFrames::new(),
    };
    if session.connect_first().await {
        session.run().await;
    }
}

/// Complete the client WebSocket handshake, bounded in time and message size.
/// Client frames use the smaller client cap; upstream links keep
/// `max_message_bytes`.
async fn accept_client(client_stream: TcpStream, limits: ProxyLimits) -> Option<ClientWs> {
    let mut client_config = WebSocketConfig::default();
    client_config.max_message_size = Some(limits.max_client_message_bytes);
    client_config.max_frame_size = Some(limits.max_client_message_bytes);

    let accept_fut = accept_async_with_config(client_stream, Some(client_config));
    if limits.handshake_timeout_ms > 0 {
        match timeout(
            Duration::from_millis(limits.handshake_timeout_ms),
            accept_fut,
        )
        .await
        {
            Ok(Ok(ws)) => Some(ws),
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "client websocket handshake failed");
                None
            }
            Err(_) => {
                tracing::warn!("client websocket handshake timed out");
                None
            }
        }
    } else {
        match accept_fut.await {
            Ok(ws) => Some(ws),
            Err(e) => {
                tracing::warn!(error = %e, "client websocket handshake failed");
                None
            }
        }
    }
}

struct Session {
    backend_servers: ServerStateDb,
    server_config_db: ServerConfigDb,
    limits: ProxyLimits,
    quarantine: Option<BlockFailureQuarantine>,
    shutdown: broadcast::Receiver<()>,

    client_tx: SplitSink<ClientWs, Message>,
    client_rx: SplitStream<ClientWs>,
    /// When the wait for the next client frame began (`idle_timeout_ms`).
    client_idle_from: Instant,

    link: Option<Link>,
    /// Endpoint of the most recent link. During a failover window it still
    /// names the upstream that failed; reroute checks compare against it.
    endpoint: String,
    /// Connection slot of the link that ended. It is held until a replacement
    /// is established, so failover ranking still counts this session against
    /// the failed upstream.
    retired_guard: Option<ConnectionGuard>,
    dial: Option<DialTask>,
    /// Picks used by the current connect or failover (see [`MAX_DIAL_ATTEMPTS`]).
    dial_attempts: u32,
    phase: Phase,

    /// Raw bytes of the latest get_blocks request (v0/v1), replayed after a
    /// failover with its start moved past `last_seen_block`.
    last_request: Option<Vec<u8>>,
    /// Number of the latest block result written to the client.
    last_seen_block: Option<u32>,
    /// Set after a failover: replayed block results below
    /// `last_seen_block + 1` are dropped until one at or past it arrives.
    restarting: bool,
    /// (next block, failures observed at that block). Cleared once a block at
    /// or past it is delivered.
    stuck_at: Option<(u32, u32)>,
    /// A reroute requested while no link was up. The next link is
    /// established and replayed as usual, then left at once for a new dial.
    reroute_pending: bool,
    /// Client frames that could not be forwarded because no link was up (or
    /// the write to it failed). Flushed after the replay.
    pending: PendingClientFrames,
}

impl Session {
    /// Dial the first upstream, check its ABI and forward the ABI to the client.
    /// Returns `false` when the session must end.
    async fn connect_first(&mut self) -> bool {
        let plan = DialPlan::initial();
        self.dial_attempts = 0;
        let established = loop {
            let Some(mut task) = self.next_dial(&plan).await else {
                tracing::error!(
                    attempts = MAX_DIAL_ATTEMPTS,
                    "unable to establish an upstream connection"
                );
                tracing::error!("no upstream available; closing client");
                self.close_client(CloseCode::Error, "No upstream server available")
                    .await;
                return false;
            };
            match task.outcome().await {
                Ok(established) => break established,
                Err(error) => {
                    report_dial_failure(&self.backend_servers, task.endpoint(), error).await;
                }
            }
        };
        let Established {
            ws,
            abi,
            guard,
            endpoint,
            name,
        } = established;
        tracing::debug!(upstream = %name, "connected to upstream");

        // Validate on a fresh handle and drop it. The parsed model is not kept
        // for the session, and it is not registered on a shared context.
        if let Err(e) = accept_upstream_abi(&abi) {
            tracing::error!(error = %e, "error setting ABI from upstream");
            return false;
        }
        self.endpoint = endpoint;
        self.link = Some(Link::new(ws, guard));

        if let Err(e) = self
            .client_tx
            .send(Message::Text(abi.as_str().into()))
            .await
        {
            tracing::warn!(error = %e, "error sending ABI to client");
            return false;
        }
        tracing::debug!("sent ABI to client");
        true
    }

    async fn run(mut self) {
        // Both idle timers start once the ABI has been forwarded.
        let now = Instant::now();
        self.client_idle_from = now;
        if let Some(link) = self.link.as_mut() {
            link.idle_from = now;
        }

        loop {
            let deadline = self.next_deadline();
            // Biased: shutdown first, then the client, so its frames are
            // applied before more upstream results are forwarded.
            let flow = tokio::select! {
                biased;
                _ = self.shutdown.recv() => self.on_shutdown().await,
                next = self.client_rx.next() => self.on_client(next).await,
                outcome = dial_outcome(&mut self.dial), if self.dial.is_some() => {
                    self.on_dial(outcome).await
                }
                next = next_frame(&mut self.link), if self.link.is_some() => {
                    self.on_upstream(next).await
                }
                () = wait_until(deadline), if deadline.is_some() => self.on_timer().await,
            };
            if let Flow::Stop = flow {
                break;
            }
        }
    }

    fn idle_timeout(&self) -> Option<Duration> {
        (self.limits.idle_timeout_ms > 0)
            .then(|| Duration::from_millis(self.limits.idle_timeout_ms))
    }

    /// Earliest of the client idle deadline, the link idle deadline and the
    /// end of a failover backoff.
    fn next_deadline(&self) -> Option<Instant> {
        let idle = self.idle_timeout();
        let client = idle.and_then(|idle| self.client_idle_from.checked_add(idle));
        let link = idle.and_then(|idle| {
            self.link
                .as_ref()
                .and_then(|link| link.idle_from.checked_add(idle))
        });
        let backoff = match &self.phase {
            Phase::Backoff { until, .. } => Some(*until),
            Phase::Streaming | Phase::Dialing { .. } => None,
        };
        [client, link, backoff].into_iter().flatten().min()
    }

    async fn on_shutdown(&mut self) -> Flow {
        self.close_client(CloseCode::Away, "server shutting down")
            .await;
        Flow::Stop
    }

    async fn on_client(&mut self, next: Option<Result<Message, WsError>>) -> Flow {
        let Some(Ok(msg)) = next else {
            tracing::debug!("client stream ended");
            return Flow::Stop;
        };
        let flow = self.forward_client(msg).await;
        self.client_idle_from = Instant::now();
        flow
    }

    /// Forward one client frame to the link, or hold it while no link is up.
    ///
    /// The latest blocks request (v0=1, v1=3) is recorded for failover replay.
    /// A request this upstream must not serve (its start block is quarantined
    /// here, or it asks for finality data this upstream's range misses while
    /// another upstream covers it) is not forwarded; the session reroutes.
    async fn forward_client(&mut self, msg: Message) -> Flow {
        if let Message::Binary(ref bin) = msg {
            if let Some(parsed) = parse_blocks_route(bin) {
                self.last_request = Some(bin.to_vec());
                if self.must_reroute(parsed).await {
                    return self.reroute().await;
                }
            }
        }

        let unsent = match self.link.as_mut() {
            Some(link) => match link.tx.send(msg.clone()).await {
                Ok(()) => return Flow::Continue,
                Err(e) => {
                    // Keep the frame for the link that replaces this one.
                    tracing::debug!(error = %e, "forward to upstream failed; buffering for failover");
                    msg
                }
            },
            None => {
                tracing::debug!("no upstream link; buffering for failover");
                msg
            }
        };
        self.buffer_client_frame(unsent).await
    }

    async fn buffer_client_frame(&mut self, msg: Message) -> Flow {
        let frame_bytes = PendingClientFrames::accounted(&msg);
        let buffered = self.pending.buffered();
        if self.pending.try_push(msg) {
            return Flow::Continue;
        }
        tracing::warn!(
            buffered,
            frame_bytes,
            cap = MAX_PENDING_CLIENT_BYTES,
            "pending client buffer exceeded; closing session"
        );
        tracing::debug!("client stream ended");
        self.close_client(CloseCode::Policy, "pending client buffer exceeded")
            .await;
        Flow::Stop
    }

    /// Whether a blocks request must leave the current upstream.
    async fn must_reroute(&self, parsed: ParsedBlocksRoute) -> bool {
        let start = parsed.start_block_num;
        // Quarantine is opt-in. Skip the state lock on the common path.
        let quarantined = self.quarantine.is_some() && {
            let mut db = self.backend_servers.lock().await;
            db.get_mut(&self.endpoint)
                .is_some_and(|state| state.quarantined_for(start, std::time::Instant::now()))
        };
        let reroute_finality = parsed.fetch_finality_data && {
            let db = self.backend_servers.lock().await;
            should_reroute_for_finality(&db, &self.endpoint, start)
        };
        if quarantined {
            tracing::info!(
                upstream = %self.endpoint,
                block = start,
                "upstream quarantined for requested block; rerouting"
            );
        } else if reroute_finality {
            tracing::info!(
                upstream = %self.endpoint,
                block = start,
                "upstream finality range does not cover request; rerouting"
            );
        }
        quarantined || reroute_finality
    }

    /// Leave the current upstream for one picked for the request just recorded.
    async fn reroute(&mut self) -> Flow {
        if self.link.is_some() {
            self.on_link_end(None, true).await
        } else {
            self.reroute_pending = true;
            Flow::Continue
        }
    }

    async fn on_upstream(&mut self, next: Option<Result<Message, WsError>>) -> Flow {
        let Some(link) = self.link.as_mut() else {
            return Flow::Continue;
        };
        let msg = match next {
            Some(Ok(msg)) => msg,
            _ => {
                let stop = if link.close_seen {
                    UpstreamStop::Clean
                } else {
                    UpstreamStop::Unclean
                };
                return self.on_link_end(Some(stop), false).await;
            }
        };

        if matches!(msg, Message::Close(_)) {
            link.close_seen = true;
        }

        let current_block = extract_block_num(&msg);

        // During failover, drop only duplicate block frames; forward everything
        // else (status results, head keep-alives) so the client never stalls.
        if self.restarting {
            if let Some(b_num) = current_block {
                let expected = self.last_seen_block.map_or(0, |b| b.saturating_add(1));
                if b_num < expected {
                    link.idle_from = Instant::now();
                    return Flow::Continue; // duplicate replayed block
                }
                self.restarting = false;
            }
        }

        if let Some(b_num) = current_block {
            if self.stuck_at.is_some_and(|(block, _)| b_num >= block) {
                self.stuck_at = None;
            }
            self.last_seen_block = Some(b_num);
        }

        if let Err(e) = self.client_tx.send(msg).await {
            tracing::warn!(error = %e, "error sending message to client");
            return Flow::Stop;
        }
        if let Some(link) = self.link.as_mut() {
            link.idle_from = Instant::now();
        }
        Flow::Continue
    }

    /// The link ended (`stop`) or is being left for a reroute: decide the
    /// failover and start its backoff.
    async fn on_link_end(&mut self, stop: Option<UpstreamStop>, rerouted: bool) -> Flow {
        if let Some(link) = self.link.take() {
            self.retired_guard = Some(link.retire());
        }

        // Patch the replay request to resume at the next block.
        if let (Some(request), Some(seen)) = (self.last_request.as_mut(), self.last_seen_block) {
            resume_request_at(request, seen.saturating_add(1));
        }
        let failed_endpoint = self.endpoint.clone();
        let next_block = next_block_after(self.last_seen_block, self.last_request.as_deref());
        // Strikes and the per-block budget count only an unclean drop. An idle
        // timeout or a clean close still fails over, but a session waiting at
        // the chain tip must not be closed with 1011.
        let budget = plan_same_block_failover(&mut self.stuck_at, stop, rerouted, next_block);
        if budget.failures > 0 {
            if let Some(block) = next_block {
                note_block_failure(
                    &self.backend_servers,
                    &failed_endpoint,
                    block,
                    self.quarantine,
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
            self.close_client(CloseCode::Error, &reason).await;
            return Flow::Stop;
        }

        if rerouted {
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

        // Pause before dialing. The first retry at a block waits 100 ms;
        // further retries at that same block back off. A reroute keeps the
        // historical 100 ms pause.
        let plan = DialPlan {
            route: failover_route(self.last_request.as_deref()),
            failover: true,
            exclude: budget.exclude_failed.then_some(failed_endpoint),
        };
        self.phase = Phase::Backoff {
            until: Instant::now() + budget.delay,
            plan,
        };
        Flow::Continue
    }

    async fn on_timer(&mut self) -> Flow {
        let now = Instant::now();
        if let Some(idle) = self.idle_timeout() {
            if passed(self.client_idle_from, idle, now) {
                tracing::info!("client idle timeout");
                tracing::debug!("client stream ended");
                return Flow::Stop;
            }
            if self
                .link
                .as_ref()
                .is_some_and(|link| passed(link.idle_from, idle, now))
            {
                tracing::debug!("upstream idle timeout; failing over");
                return self.on_link_end(Some(UpstreamStop::Idle), false).await;
            }
        }
        if let Phase::Backoff { until, plan } = &self.phase {
            if now >= *until {
                let plan = plan.clone();
                return self.begin_dialing(plan).await;
            }
        }
        Flow::Continue
    }

    async fn begin_dialing(&mut self, plan: DialPlan) -> Flow {
        self.dial_attempts = 0;
        match self.next_dial(&plan).await {
            Some(task) => {
                self.dial = Some(task);
                self.phase = Phase::Dialing { plan };
                Flow::Continue
            }
            None => self.failover_unavailable().await,
        }
    }

    /// Pick an upstream for `plan` and start dialing it, within the attempts
    /// left for this connect or failover.
    async fn next_dial(&mut self, plan: &DialPlan) -> Option<DialTask> {
        while self.dial_attempts < MAX_DIAL_ATTEMPTS {
            self.dial_attempts += 1;
            if let Some((server, guard)) =
                pick_upstream(&self.backend_servers, &self.server_config_db, plan).await
            {
                return Some(DialTask::spawn(server, guard, self.limits));
            }
        }
        None
    }

    async fn on_dial(&mut self, outcome: Result<Established, DialError>) -> Flow {
        let Some(task) = self.dial.take() else {
            return Flow::Continue;
        };
        match outcome {
            Ok(established) => self.on_failover_established(established).await,
            Err(error) => {
                report_dial_failure(&self.backend_servers, task.endpoint(), error).await;
                // A dial only runs in the Dialing phase.
                let Phase::Dialing { plan } = &self.phase else {
                    return Flow::Continue;
                };
                let plan = plan.clone();
                match self.next_dial(&plan).await {
                    Some(next) => {
                        self.dial = Some(next);
                        Flow::Continue
                    }
                    None => self.failover_unavailable().await,
                }
            }
        }
    }

    async fn on_failover_established(&mut self, established: Established) -> Flow {
        let Established {
            ws,
            abi,
            guard,
            endpoint,
            name,
        } = established;
        tracing::debug!(upstream = %name, "connected to upstream");
        // The new link takes over this session's connection slot.
        self.retired_guard = None;
        self.endpoint = endpoint;

        if let Err(e) = accept_upstream_abi(&abi) {
            tracing::error!(error = %e, "error setting ABI after failover");
            return Flow::Stop;
        }

        // Replay the request and flush any buffered client frames. The backoff
        // already ran before the dial, so a healthy failover still waits 100 ms
        // and does not pause twice.
        let mut link = Link::new(ws, guard);
        if let Some(request) = self.last_request.as_ref() {
            if let Err(e) = link.tx.send(Message::Binary(request.clone().into())).await {
                tracing::warn!(error = %e, "error replaying request after failover");
                return Flow::Stop;
            }
        }
        while let Some(frame) = self.pending.pop_front() {
            if let Err(e) = link.tx.send(frame).await {
                tracing::warn!(error = %e, "error flushing buffered client frame after failover");
                break;
            }
        }

        tracing::info!("reconnected to upstream after failover");
        self.restarting = true;
        link.idle_from = Instant::now();
        self.link = Some(link);
        self.phase = Phase::Streaming;

        if std::mem::take(&mut self.reroute_pending) {
            return self.on_link_end(None, true).await;
        }
        Flow::Continue
    }

    async fn failover_unavailable(&mut self) -> Flow {
        tracing::error!(
            attempts = MAX_DIAL_ATTEMPTS,
            "unable to establish an upstream connection"
        );
        tracing::error!("no upstream available after failover; closing client");
        self.close_client(CloseCode::Error, "no upstream available")
            .await;
        Flow::Stop
    }

    async fn close_client(&mut self, code: CloseCode, reason: &str) {
        let _ = self
            .client_tx
            .send(Message::Close(Some(CloseFrame {
                code,
                reason: reason.into(),
            })))
            .await;
    }
}

/// `true` once `idle` has elapsed since `from`.
fn passed(from: Instant, idle: Duration, now: Instant) -> bool {
    from.checked_add(idle)
        .is_some_and(|deadline| now >= deadline)
}

async fn dial_outcome(dial: &mut Option<DialTask>) -> Result<Established, DialError> {
    match dial {
        Some(task) => task.outcome().await,
        None => std::future::pending().await,
    }
}

async fn next_frame(link: &mut Option<Link>) -> Option<Result<Message, WsError>> {
    match link {
        Some(link) => link.rx.next().await,
        None => std::future::pending().await,
    }
}

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{build_config_db, build_state_db, Server};
    use mock_ship::{MockShipConfig, MockShipServer};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio_tungstenite::connect_async;

    type TestClient = WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

    const WAIT: Duration = Duration::from_secs(5);

    /// One session served in-process: `handle_client` behind a local listener,
    /// with `online` upstreams marked online in the state db.
    struct Harness {
        url: String,
        state: ServerStateDb,
        shutdown: broadcast::Sender<()>,
        session: JoinHandle<()>,
    }

    async fn start_session(upstreams: &[&str], online: &[&str], idle_timeout_ms: u64) -> Harness {
        let servers: Vec<Server> = upstreams
            .iter()
            .enumerate()
            .map(|(i, endpoint)| Server {
                name: format!("upstream-{i}"),
                endpoint: endpoint.to_string(),
                enabled: true,
            })
            .collect();
        let state = build_state_db(servers.clone());
        for endpoint in online {
            state.lock().await.get_mut(*endpoint).unwrap().online = true;
        }
        let config = build_config_db(servers);
        let limits = ProxyLimits {
            handshake_timeout_ms: 2_000,
            idle_timeout_ms,
            max_message_bytes: 1 << 20,
            max_client_message_bytes: 64 * 1024,
        };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (shutdown, shutdown_rx) = broadcast::channel(1);
        let session_state = state.clone();
        let session = tokio::spawn(async move {
            let (tcp, addr) = listener.accept().await.unwrap();
            handle_client(tcp, addr, session_state, config, limits, None, shutdown_rx).await;
        });
        Harness {
            url,
            state,
            shutdown,
            session,
        }
    }

    async fn next_message(client: &mut TestClient) -> Option<Message> {
        timeout(WAIT, client.next())
            .await
            .expect("timed out waiting for the router")
            .and_then(Result::ok)
    }

    /// Connect a client and read the forwarded ABI.
    async fn connect(url: &str) -> TestClient {
        let (mut client, _) = connect_async(url).await.expect("client connect");
        match next_message(&mut client).await {
            Some(Message::Text(text)) => assert!(text.contains("eosio::abi"), "{text}"),
            other => panic!("expected the upstream ABI, got {other:?}"),
        }
        client
    }

    fn close_of(msg: Option<Message>) -> (CloseCode, String) {
        match msg {
            Some(Message::Close(Some(frame))) => (frame.code, frame.reason.to_string()),
            other => panic!("expected a close frame, got {other:?}"),
        }
    }

    fn blocks_request(start: u32, end: u32, max_in_flight: u32) -> Message {
        let mut buf = vec![1u8];
        buf.extend_from_slice(&start.to_le_bytes());
        buf.extend_from_slice(&end.to_le_bytes());
        buf.extend_from_slice(&max_in_flight.to_le_bytes());
        buf.extend_from_slice(&[0, 0, 0, 0, 0]); // positions, irreversible_only, block, traces, deltas
        Message::binary(buf)
    }

    /// Start a mock upstream. It stops when the returned handle drops.
    async fn start_mock(config: MockShipConfig) -> (String, impl Sized) {
        let mock = MockShipServer::new(config).await;
        let endpoint = mock.endpoint();
        (endpoint, mock.start())
    }

    /// The SHiP ABI text, as a mock upstream sends it.
    async fn ship_abi() -> String {
        let (endpoint, _mock) = start_mock(MockShipConfig::default()).await;
        let (mut ws, _) = connect_async(format!("ws://{endpoint}")).await.unwrap();
        match next_message(&mut ws).await {
            Some(Message::Text(text)) => text.to_string(),
            other => panic!("expected the mock ABI, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn shutdown_closes_the_client_with_1001() {
        let (upstream, _mock) = start_mock(MockShipConfig::default()).await;
        let harness = start_session(&[&upstream], &[&upstream], 0).await;
        let mut client = connect(&harness.url).await;

        harness.shutdown.send(()).unwrap();
        let (code, reason) = close_of(next_message(&mut client).await);
        assert_eq!(code, CloseCode::Away);
        assert_eq!(reason, "server shutting down");
        timeout(WAIT, harness.session).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn no_online_upstream_closes_the_client_with_1011() {
        let (upstream, _mock) = start_mock(MockShipConfig::default()).await;
        let harness = start_session(&[&upstream], &[], 0).await;
        let (mut client, _) = connect_async(&harness.url).await.unwrap();

        let (code, reason) = close_of(next_message(&mut client).await);
        assert_eq!(code, CloseCode::Error);
        assert_eq!(reason, "No upstream server available");
        timeout(WAIT, harness.session).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn an_upstream_close_frame_is_forwarded_to_the_client() {
        let abi = ship_abi().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.send(Message::text(abi)).await.unwrap();
            ws.send(Message::Close(Some(CloseFrame {
                code: CloseCode::Restart,
                reason: "upstream restarting".into(),
            })))
            .await
            .unwrap();
            while let Some(Ok(_)) = ws.next().await {}
        });
        let harness = start_session(&[&upstream], &[&upstream], 0).await;
        let mut client = connect(&harness.url).await;

        let (code, reason) = close_of(next_message(&mut client).await);
        assert_eq!(code, CloseCode::Restart);
        assert_eq!(reason, "upstream restarting");
    }

    #[tokio::test]
    async fn a_silent_client_is_dropped_after_the_idle_timeout_without_a_close_frame() {
        let (upstream, _mock) = start_mock(MockShipConfig::default()).await;
        let harness = start_session(&[&upstream], &[&upstream], 200).await;
        let mut client = connect(&harness.url).await;

        // The session ends and drops the socket; no Close frame is sent.
        let next = timeout(WAIT, client.next())
            .await
            .expect("the idle session should end");
        assert!(
            !matches!(next, Some(Ok(_))),
            "expected the stream to end, got {next:?}"
        );
        timeout(WAIT, harness.session).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn an_unclean_drop_fails_over_and_resumes_after_the_last_block() {
        let (failing, _failing_mock) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            disconnect_after: Some(3),
            ..Default::default()
        })
        .await;
        let (healthy, _healthy_mock) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            ..Default::default()
        })
        .await;
        // Only the failing upstream is online at connect, so the session starts there.
        let harness = start_session(&[&failing, &healthy], &[&failing], 0).await;
        let mut client = connect(&harness.url).await;
        harness.state.lock().await.get_mut(&healthy).unwrap().online = true;

        client.send(blocks_request(100, 110, 100)).await.unwrap();
        let mut blocks = Vec::new();
        while blocks.len() < 10 {
            match next_message(&mut client).await {
                Some(msg @ Message::Binary(_)) => blocks.extend(extract_block_num(&msg)),
                other => panic!("unexpected frame after {blocks:?}: {other:?}"),
            }
        }
        assert_eq!(blocks, (100..110).collect::<Vec<_>>(), "gap or duplicate");

        let state = harness.state.lock().await;
        assert_eq!(state[&failing].block_failures, 1, "one strike at block 103");
        assert_eq!(state[&healthy].block_failures, 0);
    }
}
