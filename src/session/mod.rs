//! Client session actor: one task per client connection.
//!
//! A single `biased` [`tokio::select!`] loop owns the client socket, the
//! upstream link, the dial task, the session's copy of the listener's fleet
//! view and its timers. Client writes are awaited inline, so a session holds at
//! most the one message being written.
//!
//! The session mirrors what one Spring node would hold for its client
//! ([`client::ClientSide`]): the request, the send credit and the cursor.
//! Every upstream request is rebuilt from that mirror, so a failover resumes
//! at the client's cursor with its outstanding credit (acks sent while no
//! link was up are folded into it, never replayed). Blocks an upstream replays
//! after a resume are dropped by number and id inside the resume window and
//! refunded upstream with an ack; fork re-deliveries are forwarded.
//!
//! In pool topology (identical full nodes):
//! - the first upstream is dialed when the client connects, and the client
//!   gets its ABI;
//! - a `get_blocks` is checked against the current upstream before it is
//!   forwarded, and moved to another upstream that serves it better;
//! - a new request reuses the socket only when nothing of the old one can
//!   still arrive, otherwise a fresh link is opened;
//! - status is answered by the router from the fleet view; while the fleet
//!   tip is unknown it is relayed to the link, in order and without loss
//!   across failovers;
//! - an upstream Close frame is never forwarded: every link end is a
//!   transparent failover, classified by [`failover::classify`];
//! - client frames that are not a known SHiP request are forwarded (or held
//!   in a byte-capped buffer while no link is up); malformed known requests
//!   close the client with 1007.

mod client;
mod failover;
mod link;
#[cfg(test)]
mod lockstep;
mod status;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, watch};
use tokio::time::{sleep_until, timeout, Instant};
use tokio_tungstenite::tungstenite::error::CapacityError;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{accept_async_with_config, WebSocketStream};

use crate::abi::abi_fingerprint;
use crate::fleet::planner::{eligible, pool_pick, Need, Union};
use crate::fleet::view::UpstreamView;
use crate::fleet::{
    FleetHub, FleetView, LinkPurpose, Log, MetricLabel, ProtocolError, SessionGauge,
};
use crate::models::{
    BlockFailureQuarantine, ProxyLimits, ResolvedListener, ResolvedTopology, Role, StitchingPolicy,
};
use crate::ship::wire::{self, BlocksRequest, ClientMsg, DecodeError, ResultHead, UpstreamMsg};
use crate::ship::BlockPos;

use client::{
    commit, head_lib_for, judge, request_is_empty, ClientSide, Forward, Judgement,
    PendingClientFrames, ResultKind, MAX_PENDING_CLIENT_BYTES,
};
use failover::{
    classify, jitter, note_block_failure, plan_same_block_failover, EndAction, EndFacts, Exclusion,
    UpEnd, ViolationKind, DIAL_EXCLUSION,
};
pub use link::ConnectionGuard;
use link::{
    build_request, mark_offline, owed, quiescent, DialError, DialTarget, DialTask, Established,
    Link, Purpose, Target, UpCursor, MAX_DIAL_ATTEMPTS,
};
use status::{router_status, status_ready, StatusFlow, StatusStep};

type ClientWs = WebSocketStream<TcpStream>;

/// A link whose upstream the monitor marked offline is failed over after
/// this much silence while it owes blocks.
const MONITOR_OFFLINE_SILENCE: Duration = Duration::from_secs(5);

/// How long a status request waits for the fleet tip (or a link to relay it)
/// when `handshake_timeout_ms` is 0.
const DEFAULT_STATUS_WAIT: Duration = Duration::from_secs(10);

/// At most one "upstream jumped" warning per session in this interval; the
/// rest are logged at debug.
const JUMP_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// What every session of one listener shares.
pub struct ListenerCtx {
    /// `None` in single-listener mode.
    pub name: Option<Arc<str>>,
    pub topology: ResolvedTopology,
    pub stitching: StitchingPolicy,
    pub limits: ProxyLimits,
    pub quarantine: Option<BlockFailureQuarantine>,
    pub live_lag_blocks: u32,
    pub hub: Arc<FleetHub>,
    pub next_session_id: AtomicU64,
    /// Set once a status v1 reply went out with a zeroed `chain_id`.
    zero_chain_id_warned: AtomicBool,
}

impl ListenerCtx {
    pub fn new(
        listener: &ResolvedListener,
        limits: ProxyLimits,
        live_lag_blocks: u32,
        hub: Arc<FleetHub>,
    ) -> Arc<Self> {
        Arc::new(ListenerCtx {
            name: listener.name.as_deref().map(Arc::from),
            topology: listener.topology,
            stitching: listener.stitching.clone(),
            limits,
            quarantine: listener.block_failure_quarantine,
            live_lag_blocks,
            hub,
            next_session_id: AtomicU64::new(1),
            zero_chain_id_warned: AtomicBool::new(false),
        })
    }
}

/// Whether the session loop keeps running after an event.
#[must_use]
enum Flow {
    Continue,
    Stop,
}

#[derive(Clone, Copy, Debug)]
enum Phase {
    /// A link is up with no request, or with its request satisfied.
    Idle,
    /// A link is serving the client's request.
    Streaming,
    /// A dial task is running in `Session::dial`.
    Dialing { purpose: Purpose },
    /// The link ended; the next dial starts at `until`.
    Backoff { until: Instant, purpose: Purpose },
}

impl Phase {
    fn gauge(self) -> SessionGauge {
        match self {
            Phase::Idle => SessionGauge::Idle,
            Phase::Streaming => SessionGauge::Streaming,
            Phase::Dialing { .. } => SessionGauge::Dialing,
            Phase::Backoff { .. } => SessionGauge::Backoff,
        }
    }
}

/// Per-session totals for the summary line logged when the session ends.
#[derive(Debug, Default)]
struct Counts {
    results: u64,
    failovers: u64,
    dups: u64,
    keepalives: u64,
    reroutes: u64,
    patched: u64,
}

pub async fn handle_client(
    client_stream: TcpStream,
    _client_address: SocketAddr,
    ctx: Arc<ListenerCtx>,
    shutdown: broadcast::Receiver<()>,
) {
    let Some(client_websocket) = accept_client(client_stream, ctx.limits).await else {
        return;
    };
    tracing::info!("client connected");

    let (client_tx, client_rx) = client_websocket.split();
    let mut session = Session::new(ctx, client_tx, client_rx, shutdown);
    if let Flow::Continue = session.connect_first().await {
        session.run().await;
    }
    session.finish();
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

/// Planner and fleet-view deadlines use the std clock.
fn std_now() -> std::time::Instant {
    std::time::Instant::now()
}

/// A live upstream more than `lag` blocks behind the fleet head, asked for a
/// block above its own head (the planner's lag rule).
fn lagging_for(view: &FleetView, u: &UpstreamView, n: u32, lag: u32) -> bool {
    lag > 0
        && u.role == Role::Live
        && matches!((u.head, view.head), (Some(h), Some(f)) if h.num.saturating_add(lag) < f.num && n > h.num)
}

/// Whether a block result from `u` lacks a field the request asked for and
/// `u` advertises at block `n` (the frozen field contract). A result whose
/// payload fields do not decode counts as missing them.
fn missing_field(
    u: Option<&UpstreamView>,
    creq: &BlocksRequest,
    frame: &[u8],
    h: &ResultHead,
    n: u32,
) -> bool {
    let Some(u) = u else {
        return false;
    };
    let Ok(p) = wire::result_presence(frame, h) else {
        return true;
    };
    (creq.fetch_traces && u.covers(Log::Trace, n) && !p.traces)
        || (creq.fetch_deltas && u.covers(Log::ChainState, n) && !p.deltas)
        || (creq.v1 && creq.fetch_finality_data && u.covers(Log::Finality, n) && !p.finality)
        || (creq.fetch_block && u.covers_ids(n) && !p.block)
}

struct Session {
    ctx: Arc<ListenerCtx>,
    /// Listener-unique; seeds the rendezvous tie-break and the retry jitter.
    id: u64,
    /// Link activity is stored as milliseconds since this instant.
    epoch: Instant,
    shutdown: broadcast::Receiver<()>,

    client_tx: SplitSink<ClientWs, Message>,
    client_rx: SplitStream<ClientWs>,
    /// When the client last sent a frame (`idle_timeout_ms`).
    client_seen: Instant,

    view_rx: watch::Receiver<Arc<FleetView>>,
    view: Arc<FleetView>,

    c: ClientSide,
    link: Option<Link>,
    /// Slot of the link that ended. It is held until a replacement is
    /// established, so failover ranking still counts this session against
    /// the upstream that failed.
    retired: Option<ConnectionGuard>,
    dial: Option<DialTask>,
    /// Failed dials in the current connect or failover (see
    /// [`MAX_DIAL_ATTEMPTS`]).
    dial_attempts: u32,
    phase: Phase,
    /// The `fleet_router_sessions` state this session is counted in.
    gauge: Option<SessionGauge>,
    /// (next block, unclean failures at that block). Cleared once a block at
    /// or past it is delivered.
    stuck_at: Option<(u32, u32)>,
    ex: crate::fleet::planner::Excludes,
    status: StatusFlow,
    /// Pool: client frames that are not a known request, held while no link
    /// is up and flushed after the next link's request.
    pending: PendingClientFrames,
    last_jump_warn: Option<Instant>,
    counts: Counts,
    end_reason: &'static str,
}

impl Session {
    fn new(
        ctx: Arc<ListenerCtx>,
        client_tx: SplitSink<ClientWs, Message>,
        client_rx: SplitStream<ClientWs>,
        shutdown: broadcast::Receiver<()>,
    ) -> Self {
        let id = ctx.next_session_id.fetch_add(1, Ordering::Relaxed);
        let view_rx = ctx.hub.subscribe();
        let view = view_rx.borrow().clone();
        let now = Instant::now();
        Session {
            ctx,
            id,
            epoch: now,
            shutdown,
            client_tx,
            client_rx,
            client_seen: now,
            view_rx,
            view,
            c: ClientSide::default(),
            link: None,
            retired: None,
            dial: None,
            dial_attempts: 0,
            phase: Phase::Idle,
            gauge: None,
            stuck_at: None,
            ex: Default::default(),
            status: StatusFlow::default(),
            pending: PendingClientFrames::new(),
            last_jump_warn: None,
            counts: Counts::default(),
            end_reason: "client closed",
        }
    }

    fn set_phase(&mut self, phase: Phase) {
        let gauge = phase.gauge();
        self.ctx.hub.stats.session_moved(self.gauge, Some(gauge));
        self.gauge = Some(gauge);
        self.phase = phase;
    }

    /// Take the latest published fleet view, if a new one exists.
    fn sync_view(&mut self) {
        if self.view_rx.has_changed().unwrap_or(false) {
            self.view = self.view_rx.borrow_and_update().clone();
        }
    }

    fn idle_timeout(&self) -> Option<Duration> {
        (self.ctx.limits.idle_timeout_ms > 0)
            .then(|| Duration::from_millis(self.ctx.limits.idle_timeout_ms))
    }

    fn stall_timeout(&self) -> Option<Duration> {
        (self.ctx.limits.stall_timeout_ms > 0)
            .then(|| Duration::from_millis(self.ctx.limits.stall_timeout_ms))
    }

    /// How long a status request may wait for the fleet tip or a link.
    fn status_wait(&self) -> Duration {
        match self.ctx.limits.handshake_timeout_ms {
            0 => DEFAULT_STATUS_WAIT,
            ms => Duration::from_millis(ms),
        }
    }

    /// Bound for writing a request to an upstream.
    fn write_timeout(&self) -> Duration {
        Duration::from_millis(self.ctx.limits.connect_timeout_ms.max(1))
    }

    /// Dial the first upstream, check its ABI and forward the ABI to the
    /// client.
    async fn connect_first(&mut self) -> Flow {
        self.set_phase(Phase::Dialing {
            purpose: Purpose::Initial,
        });
        self.dial_attempts = 0;
        let mut established = None;
        while self.dial_attempts < MAX_DIAL_ATTEMPTS {
            self.dial_attempts += 1;
            self.sync_view();
            let Some(up) = pool_pick(
                &self.view,
                None,
                &self.ex,
                false,
                self.id,
                self.ctx.live_lag_blocks,
                std_now(),
            ) else {
                break;
            };
            let Some(mut task) = self.spawn_dial(up) else {
                break;
            };
            match task.outcome().await {
                Ok(est) => {
                    established = Some((task.target().clone(), est));
                    break;
                }
                Err(error) => self.on_dial_failed(task.target(), error).await,
            }
        }
        let Some((target, est)) = established else {
            tracing::error!(
                attempts = MAX_DIAL_ATTEMPTS,
                "unable to establish an upstream connection"
            );
            tracing::error!("no upstream available; closing client");
            self.close_client(CloseCode::Error, "No upstream server available")
                .await;
            self.end_reason = "no upstream";
            return Flow::Stop;
        };
        self.c.abi_fp = abi_fingerprint(&est.abi);
        if let Err(e) = self
            .client_tx
            .send(Message::Text(est.abi.as_str().into()))
            .await
        {
            tracing::warn!(error = %e, "error sending ABI to client");
            self.end_reason = "client write failed";
            return Flow::Stop;
        }
        tracing::debug!("sent ABI to client");
        self.adopt_link(&target, est, LinkPurpose::Initial);
        self.set_phase(Phase::Idle);
        Flow::Continue
    }

    async fn run(&mut self) {
        // The idle timer starts once the ABI has been forwarded.
        self.client_seen = Instant::now();
        loop {
            if self.view_rx.has_changed().unwrap_or(false) {
                self.view = self.view_rx.borrow_and_update().clone();
                if let Flow::Stop = self.on_view_change().await {
                    break;
                }
            }
            self.update_owed();
            let deadline = self.next_deadline();
            // A new view can answer waiting status requests. It also makes
            // the next pass re-check whether a link with a request and credit
            // owes blocks: the fleet head may have moved past what a silent
            // upstream sent (stall), or the monitor may have marked the
            // upstream offline. Nothing else wakes a session whose link sits
            // silent at its own tip.
            let waits_on_view = self.status.waiting()
                || self
                    .link
                    .as_ref()
                    .is_some_and(|l| l.st.req.is_some() && self.c.credit > 0);
            // Biased: shutdown first, then the client, so its acks, status
            // requests and new requests are applied before more upstream
            // results are forwarded.
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
                changed = self.view_rx.changed(), if waits_on_view => {
                    if changed.is_ok() {
                        self.view = self.view_rx.borrow_and_update().clone();
                        self.on_view_change().await
                    } else {
                        Flow::Continue
                    }
                }
                () = wait_until(deadline), if deadline.is_some() => self.on_timer().await,
            };
            if let Flow::Stop = flow {
                break;
            }
        }
    }

    /// Close links quietly, abort a running dial and log the summary line.
    fn finish(&mut self) {
        if let Some(link) = self.link.take() {
            drop(link.close_quietly());
        }
        self.retired = None;
        self.dial = None;
        self.ctx.hub.stats.session_moved(self.gauge.take(), None);
        let Counts {
            results,
            failovers,
            dups,
            keepalives,
            reroutes,
            patched,
        } = self.counts;
        tracing::info!(
            results,
            failovers,
            dups,
            keepalives,
            reroutes,
            patched,
            duration_ms = u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX),
            reason = self.end_reason,
            "session closed"
        );
    }

    async fn on_view_change(&mut self) -> Flow {
        self.drive_status().await
    }

    async fn on_shutdown(&mut self) -> Flow {
        self.close_client(CloseCode::Away, "server shutting down")
            .await;
        self.end_reason = "shutdown";
        Flow::Stop
    }

    // ------------------------------------------------------------------
    // Client side
    // ------------------------------------------------------------------

    async fn on_client(&mut self, next: Option<Result<Message, WsError>>) -> Flow {
        let msg = match next {
            Some(Ok(msg)) => msg,
            Some(Err(e)) => {
                tracing::debug!(error = %e, "client stream ended");
                self.end_reason = "client error";
                return Flow::Stop;
            }
            None => {
                tracing::debug!("client stream ended");
                return Flow::Stop;
            }
        };
        self.client_seen = Instant::now();
        let payload: Bytes = match msg {
            Message::Binary(bytes) => bytes,
            Message::Text(text) => text.into(),
            Message::Close(_) => {
                tracing::debug!("client stream ended");
                return Flow::Stop;
            }
            // Pings are answered by tungstenite; nothing else needs a reply.
            Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => return Flow::Continue,
        };
        match wire::decode_client(&payload) {
            Ok(ClientMsg::Status { v1 }) => self.on_status_request(v1).await,
            Ok(ClientMsg::Ack(n)) => self.on_client_ack(n).await,
            Ok(ClientMsg::Blocks(request)) => self.on_client_request(request).await,
            Err(DecodeError::UnknownVariant(variant)) => {
                self.on_unknown_variant(variant, payload).await
            }
            Err(error) => {
                self.ctx
                    .hub
                    .stats
                    .client_protocol_errors
                    .inc(ProtocolError::Malformed);
                tracing::debug!(%error, "malformed client request; closing client");
                self.close_client(CloseCode::Invalid, "invalid request")
                    .await;
                self.end_reason = "malformed request";
                Flow::Stop
            }
        }
    }

    /// `C += n` (wrapping), forwarded to the link only while it has a
    /// request. Otherwise it is folded into the credit the next request
    /// carries.
    async fn on_client_ack(&mut self, n: u32) -> Flow {
        self.c.ack(n);
        if let Some(link) = self.link.as_mut() {
            if link.st.req.is_some() {
                // A failed write shows up as the end of the link's stream.
                let _ = link
                    .tx
                    .send(Message::Binary(wire::encode_ack(n).into()))
                    .await;
            }
        }
        Flow::Continue
    }

    async fn on_unknown_variant(&mut self, variant: u32, payload: Bytes) -> Flow {
        self.ctx
            .hub
            .stats
            .client_protocol_errors
            .inc(ProtocolError::UnknownVariant);
        if self.ctx.topology == ResolvedTopology::Stitched {
            tracing::debug!(
                variant,
                "unsupported client request variant; closing client"
            );
            self.close_client(
                CloseCode::Unsupported,
                &format!("unsupported request variant {variant}"),
            )
            .await;
            self.end_reason = "unsupported request";
            return Flow::Stop;
        }
        // Pool: forwarded unchanged, for nodes newer than this router.
        tracing::debug!(variant, "forwarding an unknown client request variant");
        let msg = Message::Binary(payload);
        if let Some(link) = self.link.as_mut() {
            if link.tx.send(msg.clone()).await.is_ok() {
                link.st.unknown_forwarded_at = Some(Instant::now());
                return Flow::Continue;
            }
            tracing::debug!("forward to upstream failed; buffering for failover");
        } else {
            tracing::debug!("no upstream link; buffering for failover");
        }
        self.buffer_client_frame(msg).await
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
        self.close_client(CloseCode::Policy, "pending client buffer exceeded")
            .await;
        self.end_reason = "pending buffer exceeded";
        Flow::Stop
    }

    /// A new `get_blocks` replaces the request, the credit and the cursor.
    ///
    /// It is checked against the current upstream before anything is sent:
    /// an upstream that cannot serve it (or lags behind the fleet for it) is
    /// left for a better one. The socket is reused only when nothing of the
    /// previous request can still arrive; otherwise a fresh link is opened.
    async fn on_client_request(&mut self, r: BlocksRequest) -> Flow {
        tracing::debug!(
            start = r.start,
            end = r.end,
            max = r.max_in_flight,
            v1 = r.v1,
            positions = r.have_positions.len(),
            irreversible_only = r.irreversible_only,
            "client get_blocks"
        );
        let pre = self.c.new_request(r.clone());
        self.stuck_at = None;
        if let Some(link) = self.link.as_mut() {
            link.st.resume_floor = None;
        }
        self.sync_view();

        if request_is_empty(&r) {
            // Spring sends nothing for it; stop the link's old request.
            let reuse = match self.link.as_ref() {
                Some(link) if link.st.req.is_some() => Some(self.link_quiescent(link, pre)),
                _ => None,
            };
            match reuse {
                Some(true) => return self.send_request(r.end, Purpose::Initial).await,
                Some(false) => {
                    let up = self.link.as_ref().map(|l| l.st.up);
                    self.retire_link(false);
                    if let Some(up) = up {
                        if !self.start_dial(up, Purpose::Initial) {
                            return self.failover_unavailable().await;
                        }
                    }
                }
                None => {}
            }
            if self.link.is_some() {
                self.set_phase(Phase::Idle);
            }
            return Flow::Continue;
        }

        let need = Need::of(&r);
        let now = std_now();
        let lag = self.ctx.live_lag_blocks;
        // The upstream the session is on (or dialing).
        let current = match (&self.link, &self.dial) {
            (Some(link), _) => Some(link.st.up),
            (None, Some(dial)) => Some(dial.target().up),
            (None, None) => None,
        };
        // How well an upstream serves this request: it covers the start
        // block, and it does not lag behind the fleet for it.
        let suits = |up: usize| {
            self.view.up(up).map(|u| {
                (
                    eligible(&self.view, u, need, r.start, Union::Online, now),
                    !lagging_for(&self.view, u, r.start, lag),
                )
            })
        };
        let current_suits = current.and_then(suits);
        let pick = if current_suits == Some((true, true)) {
            None
        } else {
            pool_pick(
                &self.view,
                Some((need, r.start)),
                &self.ex,
                self.link.is_none(),
                self.id,
                lag,
                now,
            )
        };
        // Leave the current upstream only for one that serves the request
        // strictly better.
        let target = match (current, pick) {
            (Some(_), Some(p)) if suits(p) > current_suits => Some(p),
            (Some(cur), _) => Some(cur),
            (None, p) => p,
        };
        let Some(target) = target else {
            if matches!(self.phase, Phase::Backoff { .. }) {
                // The next pick happens when the backoff ends.
                self.phase = Phase::Backoff {
                    until: self.backoff_until(),
                    purpose: Purpose::Initial,
                };
                return Flow::Continue;
            }
            return self.failover_unavailable().await;
        };
        let rerouted = current.is_some_and(|up| up != target);
        if rerouted {
            self.ctx.hub.stats.reroutes.fetch_add(1, Ordering::Relaxed);
            self.counts.reroutes += 1;
            tracing::info!(
                from = %current.and_then(|up| self.view.up(up)).map_or("", |u| &*u.name),
                to = %self.view.up(target).map_or("", |u| &*u.name),
                block = r.start,
                "rerouting get_blocks to an upstream that serves it"
            );
        }
        let purpose = if rerouted {
            Purpose::Reroute
        } else {
            Purpose::Initial
        };

        if let Some(link) = self.link.as_ref() {
            if !rerouted && self.link_quiescent(link, pre) {
                return self.send_request(r.end, Purpose::Initial).await;
            }
            // Drop what the old request may still send, and dial afresh.
            self.retire_link(false);
            if !self.start_dial(target, purpose) {
                return self.failover_unavailable().await;
            }
            return Flow::Continue;
        }
        match self.phase {
            Phase::Dialing { .. } if !rerouted => {
                // The dial in progress serves it; its request is built when it
                // is established.
                self.phase = Phase::Dialing { purpose };
            }
            Phase::Backoff { until, .. } => {
                self.phase = Phase::Backoff {
                    until,
                    purpose: Purpose::Initial,
                };
            }
            _ => {
                if !self.start_dial(target, purpose) {
                    return self.failover_unavailable().await;
                }
            }
        }
        Flow::Continue
    }

    fn backoff_until(&self) -> Instant {
        match self.phase {
            Phase::Backoff { until, .. } => until,
            _ => Instant::now(),
        }
    }

    fn link_quiescent(&self, link: &Link, pre_credit: u32) -> bool {
        let legacy = self.view.up(link.st.up).is_some_and(|u| u.legacy_status);
        quiescent(&link.st, pre_credit, legacy)
    }

    // ------------------------------------------------------------------
    // Status
    // ------------------------------------------------------------------

    async fn on_status_request(&mut self, v1: bool) -> Flow {
        if !self.status.push(v1, Instant::now(), self.status_wait()) {
            tracing::warn!("too many queued status requests; closing client");
            self.close_client(CloseCode::Policy, "too many queued status requests")
                .await;
            self.end_reason = "status queue exceeded";
            return Flow::Stop;
        }
        self.drive_status().await
    }

    /// Answer or relay waiting status requests, oldest first. The router
    /// answers from the fleet view once the fleet head and LIB are known;
    /// until then a pool session relays to its link. Replies cost no credit.
    async fn drive_status(&mut self) -> Flow {
        loop {
            let tip = self.ctx.hub.fresh_tip(&self.view);
            let ready = status_ready(&self.view, tip);
            let can_relay = self.ctx.topology == ResolvedTopology::Pool && self.link.is_some();
            match self.status.next(ready, can_relay) {
                None => return Flow::Continue,
                Some(StatusStep::Reply(v1)) => {
                    let Some(reply) = router_status(&self.view, tip, &mut self.c, v1) else {
                        return Flow::Continue;
                    };
                    if reply.zero_chain_id
                        && !self.ctx.zero_chain_id_warned.swap(true, Ordering::Relaxed)
                    {
                        tracing::warn!(
                            listener = self.ctx.name.as_deref().unwrap_or("default"),
                            "no chain_id is known for this listener; get_status_result_v1 carries zeros (pin chain_id in the config)"
                        );
                    }
                    if let Err(e) = self
                        .client_tx
                        .send(Message::Binary(reply.bytes.into()))
                        .await
                    {
                        tracing::warn!(error = %e, "error sending status to client");
                        self.end_reason = "client write failed";
                        return Flow::Stop;
                    }
                    self.ctx.hub.stats.status_reply(v1, true);
                }
                Some(StatusStep::Relay(v1)) => {
                    let Some(link) = self.link.as_mut() else {
                        return Flow::Continue;
                    };
                    let request = Message::Binary(wire::encode_status_request(v1).into());
                    if link.tx.send(request).await.is_err() {
                        // The link's end returns the request to the queue.
                        return Flow::Continue;
                    }
                }
            }
        }
    }

    /// An upstream status result: the reply to the oldest relayed request.
    async fn on_status_reply(&mut self, frame: Bytes) -> Flow {
        let Some(v1) = self.status.reply_arrived() else {
            tracing::debug!("unsolicited upstream status result dropped");
            return Flow::Continue;
        };
        let Some(up) = self.link.as_ref().map(|l| l.st.up) else {
            return Flow::Continue;
        };
        let out = match wire::read_head_lib(&frame) {
            Some((head, lib)) => self.apply_head_lib(frame, head, lib, Role::Live, up),
            None => frame,
        };
        if let Err(e) = self.client_tx.send(Message::Binary(out)).await {
            tracing::warn!(error = %e, "error sending status to client");
            self.end_reason = "client write failed";
            return Flow::Stop;
        }
        self.ctx.hub.stats.status_reply(v1, false);
        Flow::Continue
    }

    // ------------------------------------------------------------------
    // Upstream side
    // ------------------------------------------------------------------

    async fn on_upstream(&mut self, next: Option<Result<Message, WsError>>) -> Flow {
        let Some(link) = self.link.as_mut() else {
            return Flow::Continue;
        };
        let msg = match next {
            Some(Ok(msg)) => msg,
            Some(Err(WsError::Capacity(CapacityError::MessageTooLong { size, max_size }))) => {
                tracing::warn!(
                    upstream = %link.endpoint,
                    size,
                    max_size,
                    block = self.c.cursor,
                    "upstream message exceeds max_message_bytes"
                );
                return self.on_link_end(UpEnd::TooLarge).await;
            }
            Some(Err(e)) => {
                tracing::debug!(upstream = %link.endpoint, error = %e, "upstream read failed");
                let end = link.st.end_kind();
                return self.on_link_end(end).await;
            }
            None => {
                let end = link.st.end_kind();
                return self.on_link_end(end).await;
            }
        };
        match msg {
            Message::Binary(frame) => self.on_upstream_binary(frame).await,
            // Never forwarded: the stream end that follows is a clean
            // failover.
            Message::Close(frame) => {
                link.st.close_code = Some(frame.map_or(1005, |f| u16::from(f.code)));
                Flow::Continue
            }
            Message::Text(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {
                Flow::Continue
            }
        }
    }

    async fn on_upstream_binary(&mut self, frame: Bytes) -> Flow {
        let Some(role) = self.link.as_ref().map(|l| l.st.role) else {
            return Flow::Continue;
        };
        match wire::classify_upstream(&frame) {
            UpstreamMsg::Blocks(head) => self.on_block_result(frame, head).await,
            UpstreamMsg::Status { .. } if role == Role::Frozen => {
                self.on_link_end(UpEnd::Violation(ViolationKind::Variant))
                    .await
            }
            UpstreamMsg::Status { .. } if self.ctx.topology == ResolvedTopology::Stitched => {
                tracing::debug!("status result on a session link dropped");
                Flow::Continue
            }
            UpstreamMsg::Status { .. } => self.on_status_reply(frame).await,
            UpstreamMsg::Other if role == Role::Frozen => {
                self.on_link_end(UpEnd::Violation(ViolationKind::Variant))
                    .await
            }
            UpstreamMsg::Other if self.ctx.topology == ResolvedTopology::Pool => {
                // Unknown to this router: passed through, as before.
                if let Err(e) = self.client_tx.send(Message::Binary(frame)).await {
                    tracing::warn!(error = %e, "error sending message to client");
                    self.end_reason = "client write failed";
                    return Flow::Stop;
                }
                Flow::Continue
            }
            UpstreamMsg::Other => {
                tracing::debug!("unrecognised upstream message dropped");
                Flow::Continue
            }
        }
    }

    async fn on_block_result(&mut self, frame: Bytes, h: ResultHead) -> Flow {
        let judgement = {
            let Some(link) = self.link.as_ref() else {
                return Flow::Continue;
            };
            match (self.c.req.as_ref(), link.st.req.as_ref()) {
                (Some(creq), Some(lr)) => {
                    let need = Need::of(creq);
                    let u = self.view.up(link.st.up);
                    let endpoint = &link.endpoint;
                    Some(judge(
                        &self.c,
                        &link.st,
                        creq,
                        lr,
                        &h,
                        |n| missing_field(u, creq, &frame, &h, n),
                        |k| self.alternative_for(k, need, endpoint),
                    ))
                }
                _ => None,
            }
        };
        match judgement {
            None => {
                let frozen = self
                    .link
                    .as_ref()
                    .is_some_and(|l| l.st.role == Role::Frozen);
                if frozen {
                    return self
                        .on_link_end(UpEnd::Violation(ViolationKind::Variant))
                        .await;
                }
                tracing::debug!("block result with no request in progress dropped");
                Flow::Continue
            }
            Some(Judgement::End(end)) => self.on_link_end(end).await,
            Some(Judgement::Duplicate { advance_to }) => self.drop_duplicate(advance_to, &h).await,
            Some(Judgement::Forward(fw)) => self.forward_result(frame, h, fw).await,
        }
    }

    /// Whether another upstream than `endpoint` could serve block `k` of the
    /// current request now (online, not excluded, under capacity, eligible).
    fn alternative_for(&self, k: u32, need: Need, endpoint: &str) -> bool {
        let now = std_now();
        self.view.upstreams.iter().any(|u| {
            &*u.endpoint != endpoint
                && u.enabled
                && u.online
                && !self.ex.hard_active(&u.endpoint, now)
                && !u.at_capacity()
                && eligible(&self.view, u, need, k, Union::Online, now)
        })
    }

    /// A replay inside the resume window: dropped, and its credit refunded
    /// upstream so the client's credit stays exact.
    async fn drop_duplicate(&mut self, advance_to: Option<u32>, h: &ResultHead) -> Flow {
        let Some(link) = self.link.as_mut() else {
            return Flow::Continue;
        };
        if let Some(next) = advance_to {
            link.st.up_cursor = UpCursor::Exact(next);
        }
        link.st.became_owed_at = Instant::now();
        self.ctx
            .hub
            .stats
            .duplicates_dropped
            .fetch_add(1, Ordering::Relaxed);
        self.counts.dups += 1;
        tracing::debug!(
            upstream = %link.endpoint,
            block = h.this_block.map(|p| p.num),
            "replayed result dropped; refunding its credit upstream"
        );
        // A failed write shows up as the end of the link's stream.
        let _ = link
            .tx
            .send(Message::Binary(wire::encode_ack(1).into()))
            .await;
        Flow::Continue
    }

    async fn forward_result(&mut self, frame: Bytes, h: ResultHead, fw: Forward) -> Flow {
        let Some((up, role, endpoint)) = self
            .link
            .as_ref()
            .map(|l| (l.st.up, l.st.role, l.endpoint.clone()))
        else {
            return Flow::Continue;
        };
        let stats = self.ctx.hub.stats.clone();
        let block = fw.cls.n;
        if fw.fork {
            stats.fork_redeliveries.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(upstream = %endpoint, block, "fork re-delivery forwarded");
        }
        if fw.jump {
            if let Some(u) = stats.upstream(up) {
                u.sequence_jumps.fetch_add(1, Ordering::Relaxed);
            }
            self.log_jump(&endpoint, self.c.cursor, block);
        }
        if fw.discontinuity {
            tracing::error!(
                upstream = %endpoint,
                block,
                "upstream result does not continue the last delivered block"
            );
        }
        if fw.variant_mismatch {
            tracing::debug!(upstream = %endpoint, "result variant differs from the request version");
        }
        if self.c.charge() {
            stats.credit_overruns.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(upstream = %endpoint, block, "result forwarded with no client credit");
        }
        let out = self.apply_head_lib(frame, h.head, h.lib, role, up);
        if let Err(e) = self.client_tx.send(Message::Binary(out)).await {
            tracing::warn!(error = %e, "error sending message to client");
            self.end_reason = "client write failed";
            return Flow::Stop;
        }
        let Some(link) = self.link.as_mut() else {
            return Flow::Continue;
        };
        commit(&mut self.c, &mut link.st, &h, &fw);
        link.st.became_owed_at = Instant::now();
        if fw.cls.kind == ResultKind::KeepAlive {
            stats.keepalives.fetch_add(1, Ordering::Relaxed);
            self.counts.keepalives += 1;
        }
        self.counts.results += 1;
        if let (Some((stuck, _)), Some(n)) = (self.stuck_at, block) {
            if n >= stuck {
                self.stuck_at = None;
            }
        }
        if !self.c.in_progress() && matches!(self.phase, Phase::Streaming) {
            self.set_phase(Phase::Idle);
        }
        Flow::Continue
    }

    fn log_jump(&mut self, endpoint: &str, expected: u32, got: Option<u32>) {
        let now = Instant::now();
        let warn = self
            .last_jump_warn
            .is_none_or(|at| now.duration_since(at) >= JUMP_WARN_INTERVAL);
        if warn {
            self.last_jump_warn = Some(now);
            tracing::warn!(
                upstream = %endpoint,
                expected,
                block = got,
                "upstream started past the requested block; adopting its sequence"
            );
        } else {
            tracing::debug!(upstream = %endpoint, expected, block = got, "upstream jumped past the requested block");
        }
    }

    /// Report a live frame's head/LIB into the fleet tip, then give the
    /// client a head and LIB that never go backwards for this session
    /// (frozen frames take the fleet tip). The frame is patched in place only
    /// when the values change.
    fn apply_head_lib(
        &mut self,
        frame: Bytes,
        head: BlockPos,
        lib: BlockPos,
        role: Role,
        up: usize,
    ) -> Bytes {
        if role == Role::Live {
            if let Some(tip) = self.ctx.hub.tips.get(up) {
                tip.observe(head, lib);
            }
        }
        let patch =
            self.ctx.topology == ResolvedTopology::Pool || self.ctx.stitching.patch_head_lib;
        let fleet = match role {
            Role::Frozen => match self.ctx.hub.fresh_tip(&self.view) {
                (Some(h), Some(l)) => Some((h, l)),
                _ => None,
            },
            Role::Live => None,
        };
        match head_lib_for(&mut self.c, role, head, lib, fleet, patch) {
            Some((head, lib)) => {
                self.ctx
                    .hub
                    .stats
                    .patched_results
                    .fetch_add(1, Ordering::Relaxed);
                self.counts.patched += 1;
                wire::patch_head_lib(frame, head, lib)
            }
            None => frame,
        }
    }

    // ------------------------------------------------------------------
    // Requests, link ends and dials
    // ------------------------------------------------------------------

    /// Send the client's request to the link, rebuilt from the session's
    /// mirror: `start` at the client's cursor once something was delivered,
    /// `max_messages_in_flight` = the client's credit now.
    async fn send_request(&mut self, end: u32, purpose: Purpose) -> Flow {
        let write_timeout = self.write_timeout();
        let topology = self.ctx.topology;
        let Some(creq) = self.c.req.as_ref() else {
            return Flow::Continue;
        };
        let Some(link) = self.link.as_mut() else {
            return Flow::Continue;
        };
        let target = Target {
            topology,
            ids_begin: self.view.up(link.st.up).and_then(|u| u.ids_begin()),
        };
        let req = build_request(
            &self.c,
            creq,
            &mut link.st,
            end,
            purpose,
            target,
            Instant::now(),
        );
        tracing::debug!(
            upstream = %link.endpoint,
            start = req.start,
            end = req.end,
            max = req.max_in_flight,
            v1 = req.v1,
            positions = req.have_positions.len(),
            ?purpose,
            "upstream get_blocks"
        );
        let bytes = wire::encode_blocks_request(&req);
        match timeout(write_timeout, link.tx.send(Message::Binary(bytes.into()))).await {
            Ok(Ok(())) => {
                let phase = if self.c.in_progress() {
                    Phase::Streaming
                } else {
                    Phase::Idle
                };
                self.set_phase(phase);
                Flow::Continue
            }
            _ => {
                let end = if link.st.frames == 0 {
                    UpEnd::SendFailed
                } else {
                    UpEnd::Unclean
                };
                self.on_link_end(end).await
            }
        }
    }

    /// Close the link in the background. `keep_slot` holds its connection
    /// slot until a replacement link is established.
    fn retire_link(&mut self, keep_slot: bool) {
        if let Some(link) = self.link.take() {
            let guard = link.close_quietly();
            self.retired = keep_slot.then_some(guard);
            self.status.link_ended(Instant::now(), self.status_wait());
        }
    }

    /// The link ended: classify the end, strike the upstream when it is to
    /// blame, and start the failover backoff (or close the client).
    async fn on_link_end(&mut self, end: UpEnd) -> Flow {
        let Some(link) = self.link.as_ref() else {
            return Flow::Continue;
        };
        let now = Instant::now();
        let k = self.c.cursor;
        let up = link.st.up;
        let endpoint = link.endpoint.clone();
        let name = link.name.clone();
        let facts = EndFacts {
            role: link.st.role,
            unknown_forwarded_at: link.st.unknown_forwarded_at,
        };
        self.retire_link(true);

        let stats = self.ctx.hub.stats.clone();
        match end {
            UpEnd::Stall => {
                if let Some(u) = stats.upstream(up) {
                    u.stalls.fetch_add(1, Ordering::Relaxed);
                }
            }
            UpEnd::Violation(kind) => {
                if let Some(u) = stats.upstream(up) {
                    u.contract_violations.inc(kind.label());
                }
                tracing::warn!(
                    upstream = %endpoint,
                    block = k,
                    kind = kind.label().as_str(),
                    "frozen upstream broke the archive contract"
                );
            }
            _ => {}
        }

        match classify(end, &facts, k, now) {
            EndAction::CloseClient {
                code,
                reason,
                too_large,
            } => {
                if too_large {
                    stats.too_large_closes.fetch_add(1, Ordering::Relaxed);
                } else {
                    tracing::warn!(
                        upstream = %endpoint,
                        "upstream dropped right after an unsupported client request; closing client"
                    );
                }
                self.close_client(CloseCode::from(code), &reason).await;
                self.end_reason = if too_large {
                    "message too large"
                } else {
                    "unsupported request"
                };
                Flow::Stop
            }
            EndAction::Failover {
                strike,
                budget,
                delay,
                exclusion,
                reason,
            } => {
                let block = self.c.req.is_some().then_some(k);
                if strike {
                    if let Some(block) = block {
                        note_block_failure(&self.ctx.hub, &endpoint, block, self.ctx.quarantine)
                            .await;
                    }
                }
                let plan = plan_same_block_failover(&mut self.stuck_at, budget, block, delay);
                if let Some(close_reason) = plan.close_reason {
                    tracing::error!(
                        upstream = %endpoint,
                        block = k,
                        attempts = plan.failures,
                        "block {k} unavailable on all upstreams"
                    );
                    self.close_client(CloseCode::Error, &close_reason).await;
                    self.end_reason = "block unavailable";
                    return Flow::Stop;
                }
                let now_std = std_now();
                match exclusion {
                    Exclusion::None => self.ex.soft = None,
                    Exclusion::Soft => self.ex.soft = Some(endpoint.clone()),
                    Exclusion::Hard(ttl) => {
                        self.ex.soft = None;
                        self.ex.add_hard(endpoint.clone(), ttl, now_std);
                    }
                }
                stats.failovers.inc(reason);
                self.counts.failovers += 1;
                if plan.failures > 0 {
                    tracing::warn!(
                        upstream = %endpoint,
                        name = %name,
                        block = k,
                        reason = reason.as_str(),
                        attempt = plan.failures,
                        max = failover::MAX_SAME_BLOCK_FAILURES,
                        "upstream stream ended; attempting failover"
                    );
                } else {
                    tracing::warn!(
                        upstream = %endpoint,
                        name = %name,
                        block = k,
                        reason = reason.as_str(),
                        "upstream stream ended; attempting failover"
                    );
                }
                let until = now + jitter(plan.delay, self.id, plan.failures);
                self.set_phase(Phase::Backoff {
                    until,
                    purpose: Purpose::Failover,
                });
                Flow::Continue
            }
        }
    }

    async fn on_backoff_expired(&mut self, purpose: Purpose) -> Flow {
        self.sync_view();
        let route = self.c.req.as_ref().map(|r| (Need::of(r), self.c.cursor));
        match self.pick_for_failover(route) {
            Some(up) if self.start_dial(up, purpose) => Flow::Continue,
            _ => self.failover_unavailable().await,
        }
    }

    /// `pool_pick` for a session that lost its link. The retired slot of the
    /// failed link keeps ranking honest; when it is all that blocks a pick
    /// (the upstream is at `max_sessions` because of it), it is released.
    fn pick_for_failover(&mut self, route: Option<(Need, u32)>) -> Option<usize> {
        let pick = |s: &Self| {
            pool_pick(
                &s.view,
                route,
                &s.ex,
                true,
                s.id,
                s.ctx.live_lag_blocks,
                std_now(),
            )
        };
        if let Some(up) = pick(self) {
            return Some(up);
        }
        self.retired.take()?;
        pick(self)
    }

    /// Spawn a dial task for upstream `up`, holding a slot on it.
    fn spawn_dial(&self, up: usize) -> Option<DialTask> {
        let u = self.view.up(up)?;
        let guard = ConnectionGuard::acquire(u.connections.clone());
        Some(DialTask::spawn(
            DialTarget::of(u),
            self.ctx.limits,
            guard,
            self.epoch,
        ))
    }

    fn start_dial(&mut self, up: usize, purpose: Purpose) -> bool {
        let Some(task) = self.spawn_dial(up) else {
            return false;
        };
        self.dial = Some(task);
        self.set_phase(Phase::Dialing { purpose });
        true
    }

    /// Install an established dial as the session's link.
    fn adopt_link(&mut self, target: &DialTarget, est: Established, label: LinkPurpose) {
        let fp = abi_fingerprint(&est.abi);
        if fp != self.c.abi_fp {
            tracing::debug!(
                upstream = %target.endpoint,
                fingerprint = %format!("{fp:016x}"),
                "upstream ABI differs from the one this client was sent"
            );
        }
        tracing::debug!(upstream = %target.name, "connected to upstream");
        if let Some(u) = self.ctx.hub.stats.upstream(target.up) {
            u.links.inc(label);
        }
        // The new link takes over this session's connection slot.
        self.retired = None;
        self.dial_attempts = 0;
        self.link = Some(Link::new(target, est, Instant::now()));
    }

    /// A failed dial: count it, avoid the upstream for this session for a
    /// moment, and mark a live upstream offline unless it was only busy.
    async fn on_dial_failed(&mut self, target: &DialTarget, error: DialError) {
        if let Some(u) = self.ctx.hub.stats.upstream(target.up) {
            u.dial_failures.inc(error.label());
        }
        self.ex
            .add_hard(target.endpoint.clone(), DIAL_EXCLUSION, std_now());
        match &error {
            DialError::Busy => tracing::warn!(
                upstream = %target.endpoint,
                "upstream is at its session limit (HTTP 503); trying another upstream"
            ),
            DialError::AbiTimeout | DialError::AbiInvalid(_) => tracing::warn!(
                upstream = %target.endpoint,
                %error,
                "invalid or absent ABI handshake; trying another upstream"
            ),
            DialError::Unreachable(_) | DialError::Handshake(_) => tracing::warn!(
                upstream = %target.endpoint,
                %error,
                "error connecting to upstream"
            ),
        }
        if !matches!(error, DialError::Busy) && target.role == Role::Live {
            mark_offline(&self.ctx.hub, &target.endpoint).await;
        }
        self.sync_view();
    }

    async fn on_dial(&mut self, outcome: Result<Established, DialError>) -> Flow {
        let Some(task) = self.dial.take() else {
            return Flow::Continue;
        };
        let target = task.target().clone();
        drop(task);
        let purpose = match self.phase {
            Phase::Dialing { purpose } => purpose,
            _ => Purpose::Failover,
        };
        match outcome {
            Ok(est) => {
                self.adopt_link(&target, est, purpose.label());
                if purpose == Purpose::Failover {
                    tracing::info!(upstream = %target.name, "reconnected to upstream after failover");
                }
                // Every request Spring could still answer goes to the new
                // link, even with the cursor at its end: positions below the
                // start, or a fork, rewind it. Only an empty request is never
                // sent.
                if let Some(end) = self
                    .c
                    .req
                    .as_ref()
                    .filter(|r| !request_is_empty(r))
                    .map(|r| r.end)
                {
                    if let Flow::Stop = self.send_request(end, purpose).await {
                        return Flow::Stop;
                    }
                } else {
                    self.set_phase(Phase::Idle);
                }
                if self.link.is_none() {
                    // The request write failed; that link already failed over.
                    return Flow::Continue;
                }
                if let Flow::Stop = self.drive_status().await {
                    return Flow::Stop;
                }
                self.flush_pending().await;
                Flow::Continue
            }
            Err(error) => {
                self.on_dial_failed(&target, error).await;
                self.dial_attempts += 1;
                if self.dial_attempts < MAX_DIAL_ATTEMPTS {
                    let route = self.c.req.as_ref().map(|r| (Need::of(r), self.c.cursor));
                    let next = pool_pick(
                        &self.view,
                        route,
                        &self.ex,
                        purpose != Purpose::Initial,
                        self.id,
                        self.ctx.live_lag_blocks,
                        std_now(),
                    );
                    if let Some(up) = next {
                        if self.start_dial(up, purpose) {
                            return Flow::Continue;
                        }
                    }
                }
                self.dial_attempts = 0;
                self.failover_unavailable().await
            }
        }
    }

    /// Forward client frames held while no link was up.
    async fn flush_pending(&mut self) {
        while let Some(frame) = self.pending.pop_front() {
            let Some(link) = self.link.as_mut() else {
                return;
            };
            if let Err(e) = link.tx.send(frame).await {
                tracing::warn!(error = %e, "error flushing buffered client frame after failover");
                return;
            }
            link.st.unknown_forwarded_at = Some(Instant::now());
        }
    }

    async fn failover_unavailable(&mut self) -> Flow {
        tracing::error!(
            attempts = MAX_DIAL_ATTEMPTS,
            "unable to establish an upstream connection"
        );
        tracing::error!("no upstream available after failover; closing client");
        self.close_client(CloseCode::Error, "no upstream available")
            .await;
        self.end_reason = "no upstream";
        Flow::Stop
    }

    // ------------------------------------------------------------------
    // Timers
    // ------------------------------------------------------------------

    /// Track when the link started to owe the client blocks: a request,
    /// credit, and an upstream cursor below its end and at or below what the
    /// upstream (or the fleet) can send.
    fn update_owed(&mut self) {
        let hub = &self.ctx.hub;
        let view = &self.view;
        let credit = self.c.credit;
        let Some(link) = self.link.as_mut() else {
            return;
        };
        let is_owed = owed(&link.st, credit, |irreversible_only| {
            let (head, lib) = hub.fresh_tip(view);
            if irreversible_only { lib } else { head }.map(|p| p.num)
        });
        if is_owed && !link.st.was_owed {
            link.st.became_owed_at = Instant::now();
        }
        link.st.was_owed = is_owed;
    }

    /// Start of the link's current silence: its last transport read, or when
    /// it last started to owe blocks or made progress.
    fn silence_from(&self, link: &Link) -> Instant {
        let read_ms = link.activity.load(Ordering::Relaxed);
        let read = self.epoch + Duration::from_millis(read_ms);
        read.max(link.st.became_owed_at)
    }

    /// The silence limits that apply to an owed link, shortest first.
    fn silence_limits(&self, link: &Link) -> [Option<(Duration, UpEnd)>; 3] {
        let offline = self
            .view
            .up(link.st.up)
            .is_some_and(|u| u.monitor_offline_since.is_some());
        [
            self.idle_timeout().map(|d| (d, UpEnd::Idle)),
            self.stall_timeout().map(|d| (d, UpEnd::Stall)),
            offline.then_some((MONITOR_OFFLINE_SILENCE, UpEnd::MonitorOffline)),
        ]
    }

    /// Earliest of the client idle deadline, the status deadline, the owed
    /// link's silence deadlines and the end of a failover backoff.
    fn next_deadline(&self) -> Option<Instant> {
        let client = self
            .idle_timeout()
            .and_then(|idle| self.client_seen.checked_add(idle));
        let status = self.status.deadline();
        let backoff = match self.phase {
            Phase::Backoff { until, .. } => Some(until),
            _ => None,
        };
        let link = self.link.as_ref().filter(|l| l.st.was_owed).and_then(|l| {
            let from = self.silence_from(l);
            self.silence_limits(l)
                .into_iter()
                .flatten()
                .filter_map(|(limit, _)| from.checked_add(limit))
                .min()
        });
        [client, status, backoff, link].into_iter().flatten().min()
    }

    async fn on_timer(&mut self) -> Flow {
        let now = Instant::now();
        if let Some(idle) = self.idle_timeout() {
            if self
                .client_seen
                .checked_add(idle)
                .is_some_and(|at| now >= at)
            {
                tracing::info!("client idle timeout");
                tracing::debug!("client stream ended");
                self.end_reason = "client idle";
                return Flow::Stop;
            }
        }
        if self.status.deadline().is_some_and(|at| now >= at) {
            tracing::warn!("status request not answered in time; closing client");
            self.close_client(CloseCode::Error, "fleet status unavailable")
                .await;
            self.end_reason = "status unavailable";
            return Flow::Stop;
        }
        let silent = self.link.as_ref().filter(|l| l.st.was_owed).and_then(|l| {
            let silence = now.saturating_duration_since(self.silence_from(l));
            self.silence_limits(l)
                .into_iter()
                .flatten()
                .find(|(limit, _)| silence >= *limit)
                .map(|(_, end)| (end, l.endpoint.clone()))
        });
        if let Some((end, endpoint)) = silent {
            match end {
                UpEnd::Idle => {
                    tracing::debug!(upstream = %endpoint, "upstream idle timeout; failing over")
                }
                UpEnd::Stall => {
                    tracing::warn!(upstream = %endpoint, "upstream stalled while blocks were owed; failing over")
                }
                _ => {
                    tracing::warn!(upstream = %endpoint, "upstream is offline and silent; failing over")
                }
            }
            return self.on_link_end(end).await;
        }
        if let Phase::Backoff { until, purpose } = self.phase {
            if now >= until {
                return self.on_backoff_expired(purpose).await;
            }
        }
        Flow::Continue
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
    use crate::fleet::{FailoverReason, FleetHub};
    use crate::models::{build_config_db, build_state_db, Server};
    use mock_ship::client::{BlockView, StatusView};
    use mock_ship::{MockShipConfig, MockShipServer};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio_tungstenite::connect_async;

    type TestClient = WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

    const WAIT: Duration = Duration::from_secs(5);

    /// One session served in-process: `handle_client` behind a local listener,
    /// with `online` upstreams marked online in the listener's state.
    struct Harness {
        url: String,
        hub: Arc<FleetHub>,
        shutdown: broadcast::Sender<()>,
        session: JoinHandle<()>,
    }

    fn listener(servers: Vec<Server>) -> ResolvedListener {
        ResolvedListener {
            name: None,
            listen_address: "127.0.0.1".to_string(),
            listen_port: 0,
            max_connections: 10,
            expected_chain_id: None,
            require_chain_id: false,
            block_failure_quarantine: None,
            topology: ResolvedTopology::Pool,
            stitching: StitchingPolicy::default(),
            abi_pin: None,
            servers,
        }
    }

    fn limits(idle_timeout_ms: u64) -> ProxyLimits {
        ProxyLimits {
            handshake_timeout_ms: 2_000,
            idle_timeout_ms,
            max_message_bytes: 1 << 20,
            max_client_message_bytes: 64 * 1024,
            stall_timeout_ms: 0,
            connect_timeout_ms: 2_000,
        }
    }

    async fn start_session(upstreams: &[&str], online: &[&str], idle_timeout_ms: u64) -> Harness {
        start_session_with(upstreams, online, limits(idle_timeout_ms)).await
    }

    async fn start_session_with(
        upstreams: &[&str],
        online: &[&str],
        limits: ProxyLimits,
    ) -> Harness {
        let servers: Vec<Server> = upstreams
            .iter()
            .enumerate()
            .map(|(i, endpoint)| Server {
                name: format!("upstream-{i}"),
                endpoint: endpoint.to_string(),
                enabled: true,
                ..Default::default()
            })
            .collect();
        let resolved = listener(servers.clone());
        let hub = FleetHub::new(
            &resolved,
            build_state_db(servers.clone()),
            build_config_db(servers),
        );
        for endpoint in online {
            set_online(&hub, endpoint).await;
        }
        let ctx = ListenerCtx::new(&resolved, limits, 1200, hub.clone());
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", tcp.local_addr().unwrap());
        let (shutdown, shutdown_rx) = broadcast::channel(1);
        let session = tokio::spawn(async move {
            let (stream, addr) = tcp.accept().await.unwrap();
            handle_client(stream, addr, ctx, shutdown_rx).await;
        });
        Harness {
            url,
            hub,
            shutdown,
            session,
        }
    }

    /// Mark `endpoint` online, as the monitor would, and publish the view.
    async fn set_online(hub: &FleetHub, endpoint: &str) {
        let mut db = hub.state.lock().await;
        db.get_mut(endpoint).unwrap().online = true;
        hub.rebuild(&db);
    }

    /// Give `endpoint` a reported head and LIB, so the fleet tip is known.
    async fn set_tip(hub: &FleetHub, endpoint: &str, head: u32, lib: u32) {
        let mut db = hub.state.lock().await;
        let state = db.get_mut(endpoint).unwrap();
        let pos = |num| BlockPos {
            num,
            id: mock_ship::fake_block_id(num),
        };
        state.head = Some(pos(head));
        state.lib = Some(pos(lib));
        state.ranges_known = true;
        state.trace_begin_block = 1;
        state.trace_end_block = head + 1;
        state.chain_state_begin_block = 1;
        state.chain_state_end_block = head + 1;
        hub.rebuild(&db);
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
        Message::binary(mock_ship::client::blocks(&mock_ship::client::BlocksReq {
            start,
            end,
            max_in_flight,
            ..Default::default()
        }))
    }

    /// Start a mock upstream. It stops when the returned handle drops.
    async fn start_mock(config: MockShipConfig) -> (String, mock_ship::MockShipHandle) {
        let mock = MockShipServer::new(config).await;
        let endpoint = mock.endpoint();
        (endpoint, mock.start())
    }

    fn positions_request(
        start: u32,
        end: u32,
        max_in_flight: u32,
        have_positions: Vec<(u32, [u8; 32])>,
    ) -> Message {
        Message::binary(mock_ship::client::blocks(&mock_ship::client::BlocksReq {
            start,
            end,
            max_in_flight,
            have_positions,
            ..Default::default()
        }))
    }

    /// The get_blocks requests `mock` received, in order: (mock session,
    /// start, end, position block numbers).
    fn blocks_requests(mock: &mock_ship::MockShipHandle) -> Vec<(u32, u32, u32, Vec<u32>)> {
        mock.requests()
            .into_iter()
            .filter_map(|o| match o {
                mock_ship::Observed::Blocks {
                    session,
                    start,
                    end,
                    have_positions,
                    ..
                } => Some((
                    session,
                    start,
                    end,
                    have_positions.iter().map(|(n, _)| *n).collect(),
                )),
                _ => None,
            })
            .collect()
    }

    async fn read_block_nums(client: &mut TestClient, n: usize) -> Vec<u32> {
        let mut blocks = Vec::new();
        while blocks.len() < n {
            match next_message(client).await {
                Some(Message::Binary(data)) => {
                    let view = BlockView::parse(&data).expect("a block result");
                    blocks.extend(view.num());
                }
                other => panic!("unexpected frame after {blocks:?}: {other:?}"),
            }
        }
        blocks
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
        assert_eq!(
            harness.hub.stats.sessions(SessionGauge::Idle),
            0,
            "a finished session leaves the gauge"
        );
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
    async fn an_upstream_close_frame_is_never_forwarded_to_the_client() {
        let abi = mock_ship::SHIP_ABI_JSON.to_string();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let closing = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.send(Message::text(abi)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(300)).await;
            ws.send(Message::Close(Some(CloseFrame {
                code: CloseCode::Restart,
                reason: "upstream restarting".into(),
            })))
            .await
            .unwrap();
            while let Some(Ok(_)) = ws.next().await {}
        });
        let (healthy, _mock) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            ..Default::default()
        })
        .await;
        // Only the closing upstream is online at connect, so the session
        // starts there.
        let harness = start_session(&[&closing, &healthy], &[&closing], 0).await;
        let mut client = connect(&harness.url).await;
        set_online(&harness.hub, &healthy).await;

        // The Close is a transparent failover: the client keeps streaming.
        tokio::time::sleep(Duration::from_millis(600)).await;
        client.send(blocks_request(100, 103, 3)).await.unwrap();
        assert_eq!(read_block_nums(&mut client, 3).await, vec![100, 101, 102]);
        assert_eq!(
            harness.hub.stats.failovers.get(FailoverReason::Clean),
            1,
            "the Close frame failed over cleanly"
        );
        let state = harness.hub.state.lock().await;
        assert_eq!(
            state[&closing].block_failures, 0,
            "a clean close never strikes"
        );
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
        let (healthy, healthy_mock) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            ..Default::default()
        })
        .await;
        // Only the failing upstream is online at connect, so the session starts there.
        let harness = start_session(&[&failing, &healthy], &[&failing], 0).await;
        let mut client = connect(&harness.url).await;
        set_online(&harness.hub, &healthy).await;

        client.send(blocks_request(100, 110, 100)).await.unwrap();
        let blocks = read_block_nums(&mut client, 10).await;
        assert_eq!(blocks, (100..110).collect::<Vec<_>>(), "gap or duplicate");

        let state = harness.hub.state.lock().await;
        assert_eq!(state[&failing].block_failures, 1, "one strike at block 103");
        assert_eq!(state[&healthy].block_failures, 0);
        drop(state);
        // The resume asked the healthy upstream for the rest, with the
        // client's remaining credit.
        let resumed = healthy_mock.requests().into_iter().find_map(|o| match o {
            mock_ship::Observed::Blocks {
                start,
                max_in_flight,
                ..
            } => Some((start, max_in_flight)),
            _ => None,
        });
        assert_eq!(resumed, Some((103, 97)));
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Unclean), 1);
    }

    #[tokio::test]
    async fn status_is_answered_from_the_fleet_view_when_the_tip_is_known() {
        let (upstream, mock) = start_mock(MockShipConfig {
            head_block: 5000,
            lib_block: 4990,
            ..Default::default()
        })
        .await;
        let harness = start_session(&[&upstream], &[&upstream], 0).await;
        set_tip(&harness.hub, &upstream, 7000, 6990).await;
        let mut client = connect(&harness.url).await;

        for v1 in [false, true] {
            let request = if v1 {
                mock_ship::client::status_v1()
            } else {
                mock_ship::client::status_v0()
            };
            client.send(Message::binary(request)).await.unwrap();
            let Some(Message::Binary(reply)) = next_message(&mut client).await else {
                panic!("expected a status result");
            };
            let status = StatusView::parse(&reply).expect("a status result");
            assert_eq!(status.variant, if v1 { 3 } else { 0 });
            assert_eq!(status.head.0, 7000, "the fleet head, not the link's");
            assert_eq!(status.lib.0, 6990);
            assert_eq!(status.trace, (1, 7001));
            assert_eq!(harness.hub.stats.status_replies(v1, true), 1);
        }
        assert!(
            !mock
                .requests()
                .iter()
                .any(|o| matches!(o, mock_ship::Observed::Status { .. })),
            "nothing is relayed while the router can answer"
        );
    }

    #[tokio::test]
    async fn status_is_relayed_to_the_link_while_the_tip_is_unknown() {
        let (upstream, mock) = start_mock(MockShipConfig {
            head_block: 5000,
            lib_block: 4990,
            ..Default::default()
        })
        .await;
        // Online, but no status reported yet: the fleet tip is unknown.
        let harness = start_session(&[&upstream], &[&upstream], 0).await;
        let mut client = connect(&harness.url).await;

        client
            .send(Message::binary(mock_ship::client::status_v0()))
            .await
            .unwrap();
        let Some(Message::Binary(reply)) = next_message(&mut client).await else {
            panic!("expected a status result");
        };
        let status = StatusView::parse(&reply).expect("a status result");
        assert_eq!(status.head.0, 5000, "the upstream's own reply");
        assert_eq!(harness.hub.stats.status_replies(false, false), 1);
        assert!(mock
            .requests()
            .iter()
            .any(|o| matches!(o, mock_ship::Observed::Status { v1: false, .. })));
    }

    #[tokio::test]
    async fn status_with_no_tip_and_no_link_closes_with_1011() {
        let (upstream, mock) = start_mock(MockShipConfig::default()).await;
        // The redial outlasts the status wait (handshake_timeout_ms).
        let slow_dial = ProxyLimits {
            connect_timeout_ms: 8_000,
            ..limits(0)
        };
        let harness = start_session_with(&[&upstream], &[&upstream], slow_dial).await;
        let mut client = connect(&harness.url).await;

        // The link drops and the redial hangs in the handshake.
        mock.stall_and_drop_sessions();
        tokio::time::sleep(Duration::from_millis(200)).await;
        client
            .send(Message::binary(mock_ship::client::status_v1()))
            .await
            .unwrap();
        let started = Instant::now();
        let (code, reason) = close_of(next_message(&mut client).await);
        assert_eq!(code, CloseCode::Error);
        assert_eq!(reason, "fleet status unavailable");
        assert!(
            started.elapsed() >= Duration::from_millis(1500),
            "held for the wait"
        );
    }

    #[tokio::test]
    async fn a_silent_link_to_an_upstream_the_monitor_lost_fails_over() {
        let (holding, _holding_mock) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            hold_at_block: Some(103),
            ..Default::default()
        })
        .await;
        let (healthy, _healthy_mock) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            ..Default::default()
        })
        .await;
        let harness = start_session(&[&holding, &healthy], &[&holding], 0).await;
        let mut client = connect(&harness.url).await;
        set_online(&harness.hub, &healthy).await;

        client.send(blocks_request(100, 106, 100)).await.unwrap();
        assert_eq!(read_block_nums(&mut client, 3).await, vec![100, 101, 102]);
        let silent = Instant::now();
        {
            // The monitor loses the upstream; the link still owes block 103.
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&holding)
                .unwrap()
                .mark_offline(std::time::Instant::now());
            harness.hub.rebuild(&db);
        }
        // Five seconds of silence, then a failover with no strike.
        timeout(Duration::from_secs(8), async {
            while harness.hub.stats.failovers.get(FailoverReason::Offline) == 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the silent link should fail over");
        assert!(
            silent.elapsed() >= Duration::from_millis(4500),
            "failed over after {:?}",
            silent.elapsed()
        );
        assert_eq!(read_block_nums(&mut client, 3).await, vec![103, 104, 105]);
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Offline), 1);
        let state = harness.hub.state.lock().await;
        assert_eq!(state[&holding].block_failures, 0, "no strike");
    }

    /// A live link that goes silent at its own tip is failed over once
    /// another upstream's status moves the fleet head past it, with no client
    /// traffic to wake the session.
    #[tokio::test]
    async fn a_link_silent_at_its_tip_stalls_once_the_fleet_moves_ahead() {
        let (stuck, _stuck_mock) = start_mock(MockShipConfig::live(1, 1000)).await;
        let (ahead, _ahead_mock) = start_mock(MockShipConfig::live(1, 1005)).await;
        let stall = ProxyLimits {
            stall_timeout_ms: 1000,
            ..limits(0)
        };
        // Only the stuck upstream is online at connect, so the session
        // starts there.
        let harness = start_session_with(&[&stuck, &ahead], &[&stuck], stall).await;
        set_tip(&harness.hub, &stuck, 1000, 990).await;
        let mut client = connect(&harness.url).await;
        set_online(&harness.hub, &ahead).await;

        client
            .send(blocks_request(995, u32::MAX, 100))
            .await
            .unwrap();
        assert_eq!(
            read_block_nums(&mut client, 6).await,
            (995..=1000).collect::<Vec<_>>()
        );
        // At its own tip and the fleet's, the link owes nothing yet.
        tokio::time::sleep(Duration::from_millis(300)).await;
        // The other upstream's status moves the fleet head ahead.
        let moved = Instant::now();
        set_tip(&harness.hub, &ahead, 1005, 995).await;
        assert_eq!(
            read_block_nums(&mut client, 5).await,
            (1001..=1005).collect::<Vec<_>>()
        );
        assert!(
            moved.elapsed() >= Duration::from_millis(900),
            "failed over after {:?}",
            moved.elapsed()
        );
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Stall), 1);
    }

    /// A request Spring answers only by rewinding to a stale position
    /// (`end <= start`) is still sent when the old request's link has to be
    /// replaced first.
    #[tokio::test]
    async fn a_positions_only_request_is_sent_on_the_replacement_link() {
        for (start, end, stale) in [(150, 150, 145), (150, 140, 135)] {
            let (up, mock) = start_mock(MockShipConfig::live(1, 200)).await;
            let harness = start_session(&[&up], &[&up], 0).await;
            let mut client = connect(&harness.url).await;
            // Streaming at the tip with credit left: the link is not
            // quiescent.
            client
                .send(blocks_request(190, 1_000_000, 100))
                .await
                .unwrap();
            assert_eq!(
                read_block_nums(&mut client, 11).await,
                (190..=200).collect::<Vec<_>>()
            );

            client
                .send(positions_request(start, end, 10, vec![(stale, [0xEE; 32])]))
                .await
                .unwrap();
            assert_eq!(
                read_block_nums(&mut client, 5).await,
                (stale..stale + 5).collect::<Vec<_>>(),
                "a Spring node rewinds to the stale position"
            );
            assert_eq!(mock.sessions_accepted(), 2, "the busy link was replaced");
            assert_eq!(
                blocks_requests(&mock).last(),
                Some(&(2, start, end, vec![stale])),
                "the client's request, verbatim"
            );
        }
    }

    /// A satisfied reversible range keeps its request across a failover, so
    /// a later fork still re-delivers the forked blocks, as on one node. The
    /// resumed link owes nothing, so its silence is no stall.
    #[tokio::test]
    async fn a_satisfied_reversible_range_is_resumed_and_follows_a_fork() {
        // LIB 189: the whole range is reversible.
        let (up, mock) = start_mock(MockShipConfig::live(1, 199)).await;
        let stall = ProxyLimits {
            stall_timeout_ms: 300,
            ..limits(0)
        };
        let harness = start_session_with(&[&up], &[&up], stall).await;
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(190, 200, 100)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 10).await,
            (190..200).collect::<Vec<_>>()
        );

        // A transparent failover while the range is satisfied.
        mock.drop_sessions();
        timeout(WAIT, async {
            while blocks_requests(&mock).len() < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the replacement link gets the request");
        assert_eq!(
            blocks_requests(&mock).last(),
            Some(&(2, 200, 200, (190..200).collect())),
            "start at the cursor, with the reversible tail"
        );
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Stall), 0);

        mock.fork(195, 7);
        assert_eq!(
            read_block_nums(&mut client, 5).await,
            (195..200).collect::<Vec<_>>()
        );
        assert_eq!(
            harness.hub.stats.fork_redeliveries.load(Ordering::Relaxed),
            1,
            "the rewind to 195 is one fork re-delivery"
        );
    }
}
