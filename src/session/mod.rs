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
//!
//! In both topologies an upstream that goes silent (a pulled cable, a frozen
//! VM) is pinged after half of `upstream_liveness_timeout_ms` without a frame,
//! and failed over when nothing at all arrives within the full time, or
//! within 1 s once the monitor lost it. Every session is listed in the
//! dashboard registry, where an operator can move it off its upstream (a
//! failover that resumes at the client's cursor) or close it.
//!
//! In stitched topology (frozen range servers plus live nodes), the session
//! presents the fleet as one SHiP node:
//! - the client gets the listener's canonical ABI without a dial, and status
//!   is always answered by the router; a status-only client opens no link;
//! - each `get_blocks` is planned by [`next_step`]: the upstream that serves
//!   the cursor, and the end of its run (clamped to the fleet head, or LIB for
//!   `irreversible_only`, for a frozen upstream). When the client's cursor
//!   reaches a clamped end, the stream is handed off to the next upstream with
//!   the outstanding credit; a handoff is never a failure. The next leg's
//!   upstream is dialed ahead of time (`predial_blocks`), and a frozen link at
//!   the end of its leg is reused for a following request;
//! - a block no upstream can serve now is held (bounded by `gap_wait_ms` in
//!   archived history, unbounded at the tip), a block above the fleet gate is
//!   waited for, and a configured hole closes the client with 1011;
//! - frozen results are checked against the archive contract, and their head
//!   and LIB are replaced by the fleet tip.

mod client;
mod failover;
mod link;
#[cfg(test)]
mod lockstep;
mod status;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
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
use crate::fleet::planner::{
    can_take_over, eligible, next_step, pool_pick, tip_region, Excludes, Need, ServeStep, Shape,
    Step, Union,
};
use crate::fleet::view::UpstreamView;
use crate::fleet::{
    FailoverReason, FleetHub, FleetView, HoldWhy, LinkPurpose, Log, MetricLabel, PredialResult,
    ProtocolError, SessionGauge,
};
use crate::models::{
    AbiMismatch, BlockFailureQuarantine, ProxyLimits, ResolvedListener, ResolvedTopology, Role,
    StitchingPolicy, UpstreamCounters,
};
use crate::ship::wire::{self, BlocksRequest, ClientMsg, DecodeError, ResultHead, UpstreamMsg};
use crate::ship::BlockPos;
use crate::telemetry::{self, Event, EventKind, Level, SessionRegistration};

use client::{
    commit, head_lib_for, judge, request_is_empty, ClientSide, Forward, Judgement,
    PendingClientFrames, ResultKind, MAX_PENDING_CLIENT_BYTES,
};
use failover::{
    block_unavailable_reason, classify, hold_backoff, hold_close_reason, jitter,
    note_block_failure, plan_same_block_failover, EndAction, EndFacts, Exclusion, UpEnd,
    ViolationKind, ABI_EXCLUSION, DIAL_EXCLUSION,
};
pub use link::ConnectionGuard;
use link::{
    build_request, mark_offline, owed, quiescent, settled, DialError, DialTarget, DialTask,
    Established, Link, Purpose, Target, UpCursor, LIVENESS_PING, MAX_DIAL_ATTEMPTS,
};
use status::{router_status, status_ready, StatusFlow, StatusStep};

type ClientWs = WebSocketStream<TcpStream>;

/// A link whose upstream the monitor marked offline is failed over after
/// this much silence while it owes blocks.
const MONITOR_OFFLINE_SILENCE: Duration = Duration::from_secs(5);

/// When the monitor marks the current upstream offline, the session pings it
/// at once and fails over if nothing arrives within this time.
const OFFLINE_PROBE: Duration = Duration::from_secs(1);

/// How long a status request waits for the fleet tip (or a link to relay it)
/// when `handshake_timeout_ms` is 0.
const DEFAULT_STATUS_WAIT: Duration = Duration::from_secs(10);

/// At most one "upstream jumped" warning per session in this interval; the
/// rest are logged at debug.
const JUMP_WARN_INTERVAL: Duration = Duration::from_secs(10);

/// Stitched: a link kept open with nothing to send is closed after this,
/// unless a following request reuses its socket first.
#[cfg(not(test))]
const LINGER: Duration = Duration::from_secs(30);
/// Short in unit tests, so a test can watch a linger run out.
#[cfg(test)]
const LINGER: Duration = Duration::from_secs(2);

/// Stitched: a pre-dialed standby link that no handoff used is dropped
/// after this.
const STANDBY_LINGER: Duration = Duration::from_secs(60);

/// Stitched: a waiting session re-plans at least this often, besides every
/// fleet view change.
const WAIT_RECHECK: Duration = Duration::from_secs(5);

/// What every session of one listener shares.
pub struct ListenerCtx {
    /// `None` in single-listener mode.
    pub name: Option<Arc<str>>,
    /// The listener's name in the session registry and events: its name, or
    /// `default` in single-listener mode.
    pub label: Arc<str>,
    pub topology: ResolvedTopology,
    pub stitching: StitchingPolicy,
    pub limits: ProxyLimits,
    pub quarantine: Option<BlockFailureQuarantine>,
    pub live_lag_blocks: u32,
    pub hub: Arc<FleetHub>,
    pub next_session_id: AtomicU64,
    /// Set once a status v1 reply went out with a zeroed `chain_id`.
    zero_chain_id_warned: AtomicBool,
    /// Stitched: (upstream index, ABI fingerprint) pairs already warned as
    /// differing from the ABI clients were sent.
    abi_warned: Mutex<HashSet<(usize, u64)>>,
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
            label: Arc::from(listener.name.as_deref().unwrap_or("default")),
            topology: listener.topology,
            stitching: listener.stitching.clone(),
            limits,
            quarantine: listener.block_failure_quarantine,
            live_lag_blocks,
            hub,
            next_session_id: AtomicU64::new(1),
            zero_chain_id_warned: AtomicBool::new(false),
            abi_warned: Mutex::new(HashSet::new()),
        })
    }

    fn stitched(&self) -> bool {
        self.topology == ResolvedTopology::Stitched
    }
}

/// Whether the session loop keeps running after an event.
#[must_use]
enum Flow {
    Continue,
    Stop,
}

#[derive(Clone, Debug)]
enum Phase {
    /// No request, or the request is satisfied (a link may be up).
    Idle,
    /// A link is serving the client's request.
    Streaming,
    /// A dial task is running in `Session::dial`. Stitched: `step` is the
    /// leg the new link is sent once it is up.
    Dialing {
        purpose: Purpose,
        step: Option<ServeStep>,
    },
    /// The link ended; the next dial starts at `until`.
    Backoff { until: Instant, purpose: Purpose },
    /// Stitched: no upstream can serve the cursor now (see
    /// [`Session::hold`]). The next attempt is at `next_retry`; a view change
    /// re-plans earlier.
    Holding {
        next_retry: Instant,
        purpose: Purpose,
    },
    /// Stitched: the cursor is above the fleet head (LIB for
    /// `irreversible_only`) for its frozen upstream, or past the top of an
    /// archive-only fleet. Re-planned on every view change and at `recheck`.
    Waiting {
        block: u32,
        recheck: Instant,
        purpose: Purpose,
    },
}

impl Phase {
    fn gauge(&self) -> SessionGauge {
        match self {
            Phase::Idle => SessionGauge::Idle,
            Phase::Streaming => SessionGauge::Streaming,
            Phase::Dialing { .. } => SessionGauge::Dialing,
            Phase::Backoff { .. } => SessionGauge::Backoff,
            Phase::Holding { .. } => SessionGauge::Holding,
            Phase::Waiting { .. } => SessionGauge::Waiting,
        }
    }
}

/// Stitched: a session holding at `block` because no upstream can serve it
/// now. It survives the retry dials, so `deadline` (the end of `gap_wait_ms`;
/// none in the tip region) is kept until a request is sent or the plan
/// changes.
#[derive(Clone, Copy, Debug)]
struct HoldInfo {
    block: u32,
    why: HoldWhy,
    since: Instant,
    deadline: Option<Instant>,
    /// Retries made so far at this block.
    attempt: u32,
}

/// Stitched: a link to the next leg's upstream, connected (ABI read) ahead of
/// the handoff and not read until it is activated.
struct Standby {
    link: Link,
    /// Dropped unused at this instant.
    until: Instant,
}

/// Per-session totals for the summary line logged when the session ends.
#[derive(Debug, Default)]
struct Counts {
    results: u64,
    handoffs: u64,
    failovers: u64,
    dups: u64,
    keepalives: u64,
    reroutes: u64,
    patched: u64,
}

pub async fn handle_client(
    client_stream: TcpStream,
    client_address: SocketAddr,
    ctx: Arc<ListenerCtx>,
    shutdown: broadcast::Receiver<()>,
) {
    let Some(client_websocket) = accept_client(client_stream, ctx.limits).await else {
        return;
    };
    tracing::info!("client connected");

    let (client_tx, client_rx) = client_websocket.split();
    let stitched = ctx.stitched();
    let mut session = Session::new(ctx, client_tx, client_rx, shutdown, client_address);
    let started = if stitched {
        session.send_canonical_abi().await
    } else {
        session.connect_first().await
    };
    if let Flow::Continue = started {
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
    ex: Excludes,
    /// Upstreams that answered this session's dial with HTTP 503, and until
    /// when that counts. A hold caused by other exclusions is reported as
    /// offline, not busy.
    busy: Vec<(Arc<str>, std::time::Instant)>,
    status: StatusFlow,
    /// Pool: client frames that are not a known request, held while no link
    /// is up and flushed after the next link's request.
    pending: PendingClientFrames,
    /// Stitched: the pre-dialed link to the next leg's upstream, and the dial
    /// that is opening one.
    standby: Option<Standby>,
    standby_dial: Option<DialTask>,
    /// Stitched: `dial` is a pre-dial that a handoff took over before it
    /// finished (its link counts as a pre-dial link).
    dial_was_predial: bool,
    /// Stitched: the leg end a pre-dial was last considered for.
    predial_for: Option<u32>,
    /// Stitched: the current hold, kept across its retry dials.
    hold: Option<HoldInfo>,
    /// Stitched: when the last handoff started (the old link's last frame),
    /// until the new link's first frame.
    handoff_started: Option<Instant>,
    last_jump_warn: Option<Instant>,
    counts: Counts,
    end_reason: &'static str,
    /// This session in the dashboard registry, which an operator moves or
    /// closes it through. Removed when the session ends.
    registry: SessionRegistration,
    /// The upstream the last failover or operator move left, and why. The
    /// move is reported once the next link is up.
    moved_from: Option<(Arc<str>, FailoverReason)>,
}

impl Session {
    fn new(
        ctx: Arc<ListenerCtx>,
        client_tx: SplitSink<ClientWs, Message>,
        client_rx: SplitStream<ClientWs>,
        shutdown: broadcast::Receiver<()>,
        client: SocketAddr,
    ) -> Self {
        let id = ctx.next_session_id.fetch_add(1, Ordering::Relaxed);
        let view_rx = ctx.hub.subscribe();
        let view = view_rx.borrow().clone();
        let now = Instant::now();
        let registry = telemetry::global().register_session(ctx.label.clone(), client, "");
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
            ex: Excludes::default(),
            busy: Vec::new(),
            status: StatusFlow::default(),
            pending: PendingClientFrames::new(),
            standby: None,
            standby_dial: None,
            dial_was_predial: false,
            predial_for: None,
            hold: None,
            handoff_started: None,
            last_jump_warn: None,
            counts: Counts::default(),
            end_reason: "client closed",
            registry,
            moved_from: None,
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

    fn liveness_timeout(&self) -> Option<Duration> {
        (self.ctx.limits.upstream_liveness_timeout_ms > 0)
            .then(|| Duration::from_millis(self.ctx.limits.upstream_liveness_timeout_ms))
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
            step: None,
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

    /// Stitched: send the listener's canonical ABI without dialing. It waits
    /// (up to the handshake timeout, 10 s when 0) until the fleet view has an
    /// ABI and at least one upstream is online.
    async fn send_canonical_abi(&mut self) -> Flow {
        let deadline = Instant::now() + self.status_wait();
        loop {
            self.view = self.view_rx.borrow_and_update().clone();
            if self.view.online > 0 {
                if let Some(abi) = self.view.abi.clone() {
                    self.c.abi_fp = abi.fp;
                    let text = Message::Text(abi.text.as_ref().into());
                    if let Err(e) = self.client_tx.send(text).await {
                        tracing::warn!(error = %e, "error sending ABI to client");
                        self.end_reason = "client write failed";
                        return Flow::Stop;
                    }
                    tracing::debug!(fingerprint = %abi.fp_hex(), "sent the canonical ABI to client");
                    self.set_phase(Phase::Idle);
                    return Flow::Continue;
                }
            }
            tokio::select! {
                biased;
                _ = self.shutdown.recv() => return self.on_shutdown().await,
                changed = self.view_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                }
                () = sleep_until(deadline) => break,
            }
        }
        tracing::error!("no upstream ABI available; closing client");
        self.close_client(CloseCode::Error, "No upstream server available")
            .await;
        self.end_reason = "no upstream";
        Flow::Stop
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
            // silent at its own tip. A stitched session that waits or holds
            // re-plans on every view, and a standby link is dropped when its
            // upstream goes away.
            let waits_on_view = self.status.waiting()
                || self
                    .link
                    .as_ref()
                    .is_some_and(|l| l.st.req.is_some() && self.c.credit > 0)
                || matches!(self.phase, Phase::Waiting { .. } | Phase::Holding { .. })
                || self.standby.is_some()
                || self.standby_dial.is_some();
            // Operator requests for this session.
            let info = self.registry.info.clone();
            // The monitor losing the link's upstream starts a probe.
            let probe_on: Option<Arc<UpstreamCounters>> = self
                .link
                .as_ref()
                .filter(|l| self.liveness_timeout().is_some() && l.st.probe_until.is_none())
                .map(|l| l.counters.clone());
            // Biased: shutdown first, then the client, so its acks, status
            // requests and new requests are applied before more upstream
            // results are forwarded.
            let flow = tokio::select! {
                biased;
                _ = self.shutdown.recv() => self.on_shutdown().await,
                next = self.client_rx.next() => self.on_client(next).await,
                () = info.control.notified() => self.on_control().await,
                outcome = dial_outcome(&mut self.dial), if self.dial.is_some() => {
                    self.on_dial(outcome).await
                }
                outcome = dial_outcome(&mut self.standby_dial), if self.standby_dial.is_some() => {
                    self.on_standby(outcome)
                }
                next = next_frame(&mut self.link), if self.link.is_some() => {
                    self.on_upstream(next).await
                }
                // Before the view: the monitor publishes the view that shows
                // the upstream offline right after this notification.
                () = went_offline(probe_on.as_deref()), if probe_on.is_some() => {
                    self.on_upstream_offline().await;
                    Flow::Continue
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

    /// Close links quietly, abort running dials and log the summary line.
    fn finish(&mut self) {
        if let Some(link) = self.link.take() {
            drop(link.close_quietly());
        }
        if let Some(standby) = self.standby.take() {
            drop(standby.link.close_quietly());
        }
        self.retired = None;
        self.dial = None;
        self.standby_dial = None;
        self.ctx.hub.stats.session_moved(self.gauge.take(), None);
        let Counts {
            results,
            handoffs,
            failovers,
            dups,
            keepalives,
            reroutes,
            patched,
        } = self.counts;
        tracing::info!(
            results,
            handoffs,
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
        self.probe_if_lost().await;
        if let Flow::Stop = self.drive_status().await {
            return Flow::Stop;
        }
        if !self.ctx.stitched() {
            return Flow::Continue;
        }
        self.check_standby();
        match self.phase.clone() {
            Phase::Waiting { purpose, .. } => self.replan(purpose).await,
            // A hold re-plans early only when the new view lets it go on;
            // otherwise its retries keep their backoff.
            Phase::Holding { purpose, .. } => {
                let step = self.plan(self.c.cursor, false);
                if matches!(step, Step::Hold { .. }) {
                    Flow::Continue
                } else {
                    self.apply_step(step, purpose, self.c.credit).await
                }
            }
            _ => Flow::Continue,
        }
    }

    async fn on_shutdown(&mut self) -> Flow {
        self.close_client(CloseCode::Away, "server shutting down")
            .await;
        self.end_reason = "shutdown";
        Flow::Stop
    }

    /// An operator asked, through the control API, to close this session or
    /// to move it off its upstream.
    async fn on_control(&mut self) -> Flow {
        if self.registry.info.close_requested() {
            tracing::info!("session closed by operator");
            self.close_client(CloseCode::Normal, "closed by operator")
                .await;
            self.end_reason = "closed by operator";
            return Flow::Stop;
        }
        if self.registry.info.take_move_request() {
            return self.on_operator_move().await;
        }
        Flow::Continue
    }

    /// Move the session off its link's upstream, with the normal failover:
    /// the client keeps its connection and resumes at its cursor. Skipped
    /// when no other upstream can serve the next block.
    async fn on_operator_move(&mut self) -> Flow {
        let Some(endpoint) = self.link.as_ref().map(|l| l.endpoint.clone()) else {
            tracing::info!("operator move skipped: the session has no upstream link");
            return Flow::Continue;
        };
        self.sync_view();
        let next_block = self.c.req.is_some().then_some(self.c.cursor);
        let other = self
            .view
            .upstreams
            .iter()
            .any(|u| u.endpoint != endpoint && can_take_over(u, next_block));
        if !other {
            tracing::info!(
                upstream = %endpoint,
                ?next_block,
                "operator move skipped: no other upstream can serve the next block"
            );
            return Flow::Continue;
        }
        tracing::info!(upstream = %endpoint, "moving session at operator request");
        self.on_link_end(UpEnd::Moved).await
    }

    /// Record a session that ends with an error in the dashboard event log.
    fn report_failure(&self, upstream: Option<&str>, reason: &str) {
        let mut event = Event::new(Level::Error, EventKind::SessionFailed, reason)
            .listener(&self.ctx.label)
            .session(self.registry.info.id);
        if let Some(upstream) = upstream {
            event = event.upstream(upstream);
        }
        telemetry::global().emit(event);
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
        self.registry
            .info
            .note_request(r.start, r.v1 && r.fetch_finality_data);
        let pre = self.c.new_request(r.clone());
        self.stuck_at = None;
        if let Some(link) = self.link.as_mut() {
            link.st.resume_floor = None;
        }
        self.sync_view();
        if self.ctx.stitched() {
            return self.on_stitched_request(r, pre).await;
        }

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
                        if !self.start_dial(up, Purpose::Initial, None) {
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
            if !self.start_dial(target, purpose, None) {
                return self.failover_unavailable().await;
            }
            return Flow::Continue;
        }
        match self.phase {
            Phase::Dialing { .. } if !rerouted => {
                // The dial in progress serves it; its request is built when it
                // is established.
                self.phase = Phase::Dialing {
                    purpose,
                    step: None,
                };
            }
            Phase::Backoff { until, .. } => {
                self.phase = Phase::Backoff {
                    until,
                    purpose: Purpose::Initial,
                };
            }
            _ => {
                if !self.start_dial(target, purpose, None) {
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

    fn link_settled(&self, link: &Link) -> bool {
        let legacy = self.view.up(link.st.up).is_some_and(|u| u.legacy_status);
        settled(&link.st, legacy)
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
        let len = out.len();
        if let Err(e) = self.client_tx.send(Message::Binary(out)).await {
            tracing::warn!(error = %e, "error sending status to client");
            self.end_reason = "client write failed";
            return Flow::Stop;
        }
        self.note_forwarded(len, None);
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
        // Any frame shows the upstream is alive; a reply to the router's own
        // liveness ping is not data for the silence limits.
        let liveness_pong =
            matches!(&msg, Message::Pong(payload) if payload.as_ref() == LIVENESS_PING);
        let read_ms = link.read_ms();
        link.st.note_frame(read_ms, liveness_pong);
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
                let len = frame.len();
                if let Err(e) = self.client_tx.send(Message::Binary(frame)).await {
                    tracing::warn!(error = %e, "error sending message to client");
                    self.end_reason = "client write failed";
                    return Flow::Stop;
                }
                self.note_forwarded(len, None);
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
                    let u = self.view.up(link.st.up);
                    let endpoint = &link.endpoint;
                    Some(judge(
                        &self.c,
                        &link.st,
                        creq,
                        lr,
                        &h,
                        |n| missing_field(u, creq, &frame, &h, n),
                        |k| self.alternative_for(k, creq, endpoint),
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
    /// Stitched: the planner would serve `k` from another upstream.
    fn alternative_for(&self, k: u32, creq: &BlocksRequest, endpoint: &str) -> bool {
        let now = std_now();
        if self.ctx.stitched() {
            let mut ex = self.ex.clone();
            ex.add_hard(Arc::from(endpoint), Duration::from_secs(60), now);
            let tip = self.ctx.hub.fresh_tip(&self.view);
            let step = next_step(
                &self.view,
                tip,
                &Shape::of(creq),
                k,
                &ex,
                &self.own_slots(),
                self.id,
                &self.ctx.stitching,
                self.ctx.live_lag_blocks,
                false,
                now,
            );
            return matches!(step, Step::Serve(_));
        }
        let need = Need::of(creq);
        self.view.upstreams.iter().any(|u| {
            &*u.endpoint != endpoint
                && u.routable()
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
        let len = out.len();
        if let Err(e) = self.client_tx.send(Message::Binary(out)).await {
            tracing::warn!(error = %e, "error sending message to client");
            self.end_reason = "client write failed";
            return Flow::Stop;
        }
        self.note_forwarded(len, block);
        let Some(link) = self.link.as_mut() else {
            return Flow::Continue;
        };
        let first = link.st.first_result_pending;
        let leg = link.st.req.as_ref().map(|r| (r.end, r.clamped));
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
        if first {
            if let Some(started) = self.handoff_started.take() {
                stats.handoff_latency.observe(started.elapsed());
            }
        }
        if self.ctx.stitched() {
            return self.after_stitched_result(leg, role).await;
        }
        if !self.c.in_progress() && matches!(self.phase, Phase::Streaming) {
            self.set_phase(Phase::Idle);
        }
        Flow::Continue
    }

    /// Count one frame forwarded from the link's upstream to the client:
    /// `bytes`, and a block when it carries `block`.
    fn note_forwarded(&self, bytes: usize, block: Option<u32>) {
        if let Some(link) = self.link.as_ref() {
            let counters = &link.counters;
            counters
                .bytes_total
                .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
            if block.is_some() {
                counters.blocks_total.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.registry.info.note_frame(bytes, block);
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
                self.hold = None;
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
        let counters = link.counters.clone();
        let facts = EndFacts {
            role: link.st.role,
            unknown_forwarded_at: link.st.unknown_forwarded_at,
        };
        // Stitched: a link that owes nothing (the client's request is
        // satisfied, or a frozen leg is done and kept for reuse) is simply
        // dropped. Nothing is lost, so there is nothing to fail over or to
        // strike; the next plan dials again when it needs to.
        let finished =
            !self.c.in_progress() || (link.st.role == Role::Frozen && link.st.leg_done());
        let quiet = matches!(
            end,
            UpEnd::Unclean
                | UpEnd::Clean(_)
                | UpEnd::Idle
                | UpEnd::Stall
                | UpEnd::MonitorOffline
                | UpEnd::SendFailed
                | UpEnd::Silent
                | UpEnd::Moved
        );
        if self.ctx.stitched() && finished && quiet {
            tracing::debug!(upstream = %endpoint, ?end, "idle upstream link ended");
            if end == UpEnd::Moved {
                // The next plan prefers another upstream.
                self.ex.soft = Some(endpoint.clone());
            }
            self.retire_link(false);
            if matches!(self.phase, Phase::Streaming) {
                self.set_phase(Phase::Idle);
            }
            return Flow::Continue;
        }
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
                    self.report_failure(Some(&endpoint), &close_reason);
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
                counters.failovers_total.fetch_add(1, Ordering::Relaxed);
                self.registry.info.note_failover();
                self.moved_from = Some((endpoint.clone(), reason));
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
        if self.ctx.stitched() {
            return self.replan(purpose).await;
        }
        let route = self.c.req.as_ref().map(|r| (Need::of(r), self.c.cursor));
        match self.pick_for_failover(route) {
            Some(up) if self.start_dial(up, purpose, None) => Flow::Continue,
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

    /// Dial upstream `up`. Stitched: `step` is the leg sent once the link is
    /// up.
    fn start_dial(&mut self, up: usize, purpose: Purpose, step: Option<ServeStep>) -> bool {
        let Some(task) = self.spawn_dial(up) else {
            return false;
        };
        self.dial = Some(task);
        self.dial_was_predial = false;
        self.set_phase(Phase::Dialing { purpose, step });
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
        self.note_link_up();
    }

    /// A new link (dialed, or a standby taken over) carries the session:
    /// count it for its upstream, show it in the registry, and report the
    /// failover or operator move that led here.
    fn note_link_up(&mut self) {
        let Some((endpoint, counters)) = self
            .link
            .as_ref()
            .map(|l| (l.endpoint.clone(), l.counters.clone()))
        else {
            return;
        };
        counters.sessions_total.fetch_add(1, Ordering::Relaxed);
        self.registry.info.set_upstream(&endpoint);
        if let Some(link) = self.link.as_mut() {
            link.st.upstream_online = self.view.up(link.st.up).is_some_and(|u| u.online);
        }
        if let Some((from, reason)) = self.moved_from.take() {
            let level = if reason == FailoverReason::Operator {
                Level::Info
            } else {
                Level::Warn
            };
            telemetry::global().emit(
                Event::new(
                    level,
                    EventKind::SessionFailover,
                    format!("moved from {from} to {endpoint} ({})", reason.as_str()),
                )
                .listener(&self.ctx.label)
                .upstream(&endpoint)
                .session(self.registry.info.id),
            );
        }
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
        let (purpose, step) = match &self.phase {
            Phase::Dialing { purpose, step } => (*purpose, step.clone()),
            _ => (Purpose::Failover, None),
        };
        if self.ctx.stitched() {
            return self.on_stitched_dial(target, outcome, purpose, step).await;
        }
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
                        if self.start_dial(up, purpose, None) {
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
        let from = self.moved_from.as_ref().map(|(from, _)| from.clone());
        self.report_failure(from.as_deref(), "no upstream available after failover");
        self.close_client(CloseCode::Error, "no upstream available")
            .await;
        self.end_reason = "no upstream";
        Flow::Stop
    }

    // ------------------------------------------------------------------
    // Stitched topology: planning, handoff, pre-dial, holds and waits
    // ------------------------------------------------------------------

    /// A new client `get_blocks` in a stitched listener: plan it from its
    /// start block. Nothing was dialed for the client before.
    async fn on_stitched_request(&mut self, r: BlocksRequest, pre: u32) -> Flow {
        self.hold = None;
        self.handoff_started = None;
        self.dial_attempts = 0;
        if request_is_empty(&r) {
            // Spring sends nothing for it; stop the link's old request.
            self.dial = None;
            self.discard_standby();
            if let Some(link) = self.link.as_ref() {
                if link.st.req.is_some() {
                    if self.link_quiescent(link, pre) {
                        // Sending clears the linger, yet the link has nothing
                        // to send: it keeps the linger it had (or starts one),
                        // so its slot is freed unless a request reuses it.
                        let linger = link.st.linger_until;
                        let flow = self.send_request(r.end, Purpose::Initial).await;
                        if let Some(link) = self.link.as_mut() {
                            link.st.linger_until =
                                Some(linger.unwrap_or_else(|| Instant::now() + LINGER));
                        }
                        return flow;
                    }
                    self.retire_link(false);
                }
            }
            self.set_phase(Phase::Idle);
            return Flow::Continue;
        }
        let step = self.plan(r.start, false);
        self.apply_step(step, Purpose::Initial, pre).await
    }

    /// The planner's next step for the client's request at block `from`.
    ///
    /// The slots of this session's link, dial and standby never count
    /// against an upstream's `max_sessions` (see [`Session::own_slots`]). A
    /// hold at capacity that only the slot of the link that just ended
    /// causes is planned again without that slot. A hold blamed on
    /// exclusions is reported as busy only while an upstream that answered
    /// 503 is among them; otherwise its upstreams are unreachable (offline).
    fn plan(&mut self, from: u32, allow_offline: bool) -> Step {
        let Some(shape) = self.c.req.as_ref().map(Shape::of) else {
            return Step::Done;
        };
        let mut step = self.next_step_for(&shape, from, allow_offline);
        if matches!(
            step,
            Step::Hold {
                why: HoldWhy::AtCapacity,
                ..
            }
        ) && self.retired.take().is_some()
        {
            step = self.next_step_for(&shape, from, allow_offline);
        }
        if let Step::Hold {
            block,
            why: HoldWhy::Busy,
            bounded,
        } = step
        {
            let now = std_now();
            self.busy.retain(|(_, until)| now < *until);
            if self.busy.is_empty() {
                step = Step::Hold {
                    block,
                    why: HoldWhy::Offline,
                    bounded,
                };
            }
        }
        step
    }

    fn next_step_for(&self, shape: &Shape, from: u32, allow_offline: bool) -> Step {
        let tip = self.ctx.hub.fresh_tip(&self.view);
        next_step(
            &self.view,
            tip,
            shape,
            from,
            &self.ex,
            &self.own_slots(),
            self.id,
            &self.ctx.stitching,
            self.ctx.live_lag_blocks,
            allow_offline,
            std_now(),
        )
    }

    /// The upstream of every connection slot this session holds: its link,
    /// its dial, its standby link and its pre-dial. A plan that serves from
    /// one of them reuses or replaces that slot (the socket, the standby, the
    /// dial in progress, or a new dial after the old link is closed), so the
    /// planner does not count it against the upstream's `max_sessions`.
    fn own_slots(&self) -> Vec<usize> {
        [
            self.link.as_ref().map(|l| l.st.up),
            self.dial.as_ref().map(|d| d.target().up),
            self.standby.as_ref().map(|s| s.link.st.up),
            self.standby_dial.as_ref().map(|d| d.target().up),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// Plan again from the client's cursor: after a failover backoff, a
    /// wait's view change or recheck, or an ABI exclusion.
    async fn replan(&mut self, purpose: Purpose) -> Flow {
        if !self.c.in_progress() {
            self.hold = None;
            self.set_phase(Phase::Idle);
            return Flow::Continue;
        }
        let step = self.plan(self.c.cursor, false);
        self.apply_step(step, purpose, self.c.credit).await
    }

    /// Act on a planner step. `pre` is the client credit that tells whether
    /// the current link is quiescent (before a new request: the old credit).
    ///
    /// Every step but `Serve` leaves the link without a new request, so it is
    /// kept only when nothing of its old request can still arrive (see
    /// [`Session::keep_link_if_settled`]). A hold for capacity (a full or
    /// busy upstream) closes it: the session waits for a slot elsewhere, and
    /// the slot its idle link fills may be the one another held session waits
    /// for. Kept, two such links would block both sessions until a linger
    /// runs out.
    async fn apply_step(&mut self, step: Step, purpose: Purpose, pre: u32) -> Flow {
        match step {
            Step::Serve(st) => self.serve(st, purpose, pre).await,
            Step::Done => {
                self.hold = None;
                self.handoff_started = None;
                self.dial = None;
                self.retired = None;
                self.discard_standby();
                self.keep_link_if_settled();
                self.set_phase(Phase::Idle);
                Flow::Continue
            }
            Step::Gap { block } => {
                tracing::warn!(
                    block,
                    "no upstream covers the block (a hole in the fleet's coverage); closing client"
                );
                let reason = block_unavailable_reason(block);
                self.report_failure(None, &reason);
                self.close_client(CloseCode::Error, &reason).await;
                self.end_reason = "coverage gap";
                Flow::Stop
            }
            Step::Wait { block } => {
                if !matches!(self.phase, Phase::Waiting { block: waiting, .. } if waiting == block)
                {
                    tracing::debug!(block, "waiting for the fleet to reach the block");
                }
                self.hold = None;
                self.handoff_started = None;
                self.dial = None;
                self.retired = None;
                self.discard_standby();
                self.keep_link_if_settled();
                self.set_phase(Phase::Waiting {
                    block,
                    recheck: Instant::now() + WAIT_RECHECK,
                    purpose,
                });
                Flow::Continue
            }
            Step::Hold {
                block,
                why,
                bounded,
            } => {
                self.dial = None;
                self.retired = None;
                self.dial_attempts = 0;
                self.discard_standby();
                if matches!(why, HoldWhy::AtCapacity | HoldWhy::Busy) {
                    self.retire_link(false);
                } else {
                    self.keep_link_if_settled();
                }
                let now = Instant::now();
                let (since, attempt) = match self.hold {
                    Some(h) if h.block == block => (h.since, h.attempt.saturating_add(1)),
                    _ => {
                        self.ctx.hub.stats.holds.inc(why);
                        tracing::warn!(
                            block,
                            why = why.as_str(),
                            bounded,
                            "no upstream can serve the block now; holding"
                        );
                        (now, 0)
                    }
                };
                let deadline =
                    bounded.then(|| since + Duration::from_millis(self.ctx.stitching.gap_wait_ms));
                self.hold = Some(HoldInfo {
                    block,
                    why,
                    since,
                    deadline,
                    attempt,
                });
                self.set_phase(Phase::Holding {
                    next_retry: now + jitter(hold_backoff(attempt), self.id, attempt),
                    purpose,
                });
                Flow::Continue
            }
        }
    }

    /// Serve the leg `st`: on the current socket when it is quiescent and on
    /// the same upstream, on the standby link pre-dialed to it, on a dial
    /// already running to it, or on a new dial.
    async fn serve(&mut self, st: ServeStep, purpose: Purpose, pre: u32) -> Flow {
        let up = st.up;
        if self
            .link
            .as_ref()
            .is_some_and(|l| l.st.up == up && self.link_quiescent(l, pre))
        {
            self.dial = None;
            return self.send_request(st.req_end, purpose).await;
        }
        // Drop what the old request may still send.
        self.retire_link(false);
        if self.standby.as_ref().is_some_and(|s| s.link.st.up == up) {
            let standby = self.standby.take().expect("checked above");
            self.dial = None;
            self.standby_dial = None;
            self.retired = None;
            self.dial_attempts = 0;
            self.link = Some(standby.link);
            self.note_link_up();
            self.ctx.hub.stats.predials.inc(PredialResult::Used);
            return self.send_request(st.req_end, purpose).await;
        }
        if self.dial.as_ref().is_some_and(|d| d.target().up == up) {
            // The dial on its way carries the new leg.
            self.discard_standby();
            self.set_phase(Phase::Dialing {
                purpose,
                step: Some(st),
            });
            return Flow::Continue;
        }
        if self
            .standby_dial
            .as_ref()
            .is_some_and(|d| d.target().up == up)
        {
            // The pre-dial becomes the leg's dial.
            self.dial = self.standby_dial.take();
            self.dial_was_predial = true;
            self.discard_standby();
            self.ctx.hub.stats.predials.inc(PredialResult::Used);
            self.set_phase(Phase::Dialing {
                purpose,
                step: Some(st),
            });
            return Flow::Continue;
        }
        self.discard_standby();
        if self.start_dial(up, purpose, Some(st)) {
            Flow::Continue
        } else {
            self.failover_unavailable().await
        }
    }

    /// Keep the current link when nothing of its request can still arrive,
    /// whatever the client acks next (it may serve a later leg; it is closed
    /// after [`LINGER`] unused), otherwise close it. A link that is quiet only
    /// because the client's credit ran out is closed too: the client's acks
    /// would reach it and resume its old request, whose frames do not follow
    /// the client's cursor.
    fn keep_link_if_settled(&mut self) {
        let Some(link) = self.link.as_ref() else {
            return;
        };
        if self.link_settled(link) {
            if let Some(link) = self.link.as_mut() {
                link.st.linger_until.get_or_insert(Instant::now() + LINGER);
            }
        } else {
            self.retire_link(false);
        }
    }

    /// After a stitched result was forwarded: the request is satisfied
    /// (idle, a frozen link lingers for a following range), or the leg
    /// reached its clamped end (hand off), or the next leg may be pre-dialed.
    async fn after_stitched_result(&mut self, leg: Option<(u32, bool)>, role: Role) -> Flow {
        if !self.c.in_progress() {
            if matches!(self.phase, Phase::Streaming) {
                self.set_phase(Phase::Idle);
            }
            if role == Role::Frozen {
                if let Some(link) = self.link.as_mut() {
                    link.st.linger_until = Some(Instant::now() + LINGER);
                }
            }
            return Flow::Continue;
        }
        if let Some((end, true)) = leg {
            if self.c.cursor >= end {
                return self.handoff().await;
            }
        }
        self.maybe_predial();
        Flow::Continue
    }

    /// The client's cursor reached the clamped end of the leg: plan the next
    /// leg from the cursor with the outstanding credit. A frozen link at its
    /// end is quiescent, so the same upstream reuses its socket. A handoff is
    /// never a failure: no strike, no budget, no backoff, no exclusion.
    async fn handoff(&mut self) -> Flow {
        self.ctx.hub.stats.handoffs.fetch_add(1, Ordering::Relaxed);
        self.counts.handoffs += 1;
        self.handoff_started = Some(Instant::now());
        let from = self.link.as_ref().map(|l| l.name.clone());
        let step = self.plan(self.c.cursor, false);
        if let Step::Serve(st) = &step {
            tracing::info!(
                from = %from.as_deref().unwrap_or(""),
                to = %self.view.up(st.up).map_or("", |u| &*u.name),
                block = self.c.cursor,
                "handing the stream off to the next upstream"
            );
        }
        self.apply_step(step, Purpose::Handoff, self.c.credit).await
    }

    /// Open the next leg's link ahead of the handoff once the cursor is
    /// within `predial_blocks` of the clamped end and the next leg is on
    /// another upstream. The standby link is connected and has read the ABI;
    /// it gets its request at the handoff.
    fn maybe_predial(&mut self) {
        let window = self.ctx.stitching.predial_blocks;
        if window == 0 || self.standby.is_some() || self.standby_dial.is_some() {
            return;
        }
        let Some((up, end)) = self.link.as_ref().and_then(|l| {
            l.st.req
                .as_ref()
                .filter(|r| r.clamped)
                .map(|r| (l.st.up, r.end))
        }) else {
            return;
        };
        if end.saturating_sub(self.c.cursor) > window || self.predial_for == Some(end) {
            return;
        }
        self.predial_for = Some(end);
        let Step::Serve(st) = self.plan(end, false) else {
            return;
        };
        if st.up == up {
            return;
        }
        if let Some(task) = self.spawn_dial(st.up) {
            tracing::debug!(upstream = %st.endpoint, block = end, "pre-dialing the next leg's upstream");
            self.standby_dial = Some(task);
        }
    }

    /// The pre-dial finished.
    fn on_standby(&mut self, outcome: Result<Established, DialError>) -> Flow {
        let Some(task) = self.standby_dial.take() else {
            return Flow::Continue;
        };
        let target = task.target().clone();
        drop(task);
        let stats = self.ctx.hub.stats.clone();
        match outcome {
            Ok(est) => {
                let fp = abi_fingerprint(&est.abi);
                if fp != self.c.abi_fp {
                    self.warn_abi_mismatch(&target, fp);
                    if self.ctx.stitching.abi_mismatch == AbiMismatch::Exclude {
                        self.ex
                            .add_hard(target.endpoint.clone(), ABI_EXCLUSION, std_now());
                        stats.predials.inc(PredialResult::Discarded);
                        return Flow::Continue;
                    }
                }
                if let Some(u) = stats.upstream(target.up) {
                    u.links.inc(LinkPurpose::Predial);
                }
                let now = Instant::now();
                self.standby = Some(Standby {
                    link: Link::new(&target, est, now),
                    until: now + STANDBY_LINGER,
                });
            }
            Err(error) => {
                if let Some(u) = stats.upstream(target.up) {
                    u.dial_failures.inc(error.label());
                }
                tracing::debug!(upstream = %target.endpoint, %error, "pre-dial failed");
                if matches!(error, DialError::Busy) {
                    self.busy
                        .push((target.endpoint.clone(), std_now() + DIAL_EXCLUSION));
                }
                self.ex
                    .add_hard(target.endpoint.clone(), DIAL_EXCLUSION, std_now());
                stats.predials.inc(PredialResult::Failed);
            }
        }
        Flow::Continue
    }

    /// Drop the standby link and any pre-dial in progress.
    fn discard_standby(&mut self) {
        let stats = &self.ctx.hub.stats;
        if let Some(standby) = self.standby.take() {
            drop(standby.link.close_quietly());
            stats.predials.inc(PredialResult::Discarded);
        }
        if self.standby_dial.take().is_some() {
            stats.predials.inc(PredialResult::Discarded);
        }
    }

    /// A view change: drop the standby when its upstream went offline or can
    /// no longer serve.
    fn check_standby(&mut self) {
        let up = match (&self.standby, &self.standby_dial) {
            (Some(standby), _) => standby.link.st.up,
            (None, Some(dial)) => dial.target().up,
            (None, None) => return,
        };
        let usable = self
            .view
            .up(up)
            .is_some_and(|u| u.routable() && u.online && u.abi_ok);
        if !usable {
            self.discard_standby();
        }
    }

    /// A stitched dial finished: send the planned leg, or plan again.
    async fn on_stitched_dial(
        &mut self,
        target: DialTarget,
        outcome: Result<Established, DialError>,
        purpose: Purpose,
        step: Option<ServeStep>,
    ) -> Flow {
        let predial = std::mem::take(&mut self.dial_was_predial);
        match outcome {
            Ok(est) => {
                let fp = abi_fingerprint(&est.abi);
                if fp != self.c.abi_fp {
                    self.warn_abi_mismatch(&target, fp);
                    if self.ctx.stitching.abi_mismatch == AbiMismatch::Exclude {
                        drop(est);
                        self.ex
                            .add_hard(target.endpoint.clone(), ABI_EXCLUSION, std_now());
                        return self.replan(purpose).await;
                    }
                }
                let label = if predial {
                    LinkPurpose::Predial
                } else {
                    purpose.label()
                };
                self.adopt_link(&target, est, label);
                if purpose == Purpose::Failover {
                    tracing::info!(upstream = %target.name, "reconnected to upstream after failover");
                }
                match step {
                    Some(st) if self.c.in_progress() => {
                        self.send_request(st.req_end, purpose).await
                    }
                    _ => {
                        // Nothing to send now: keep the link for a following
                        // request, for a while.
                        if let Some(link) = self.link.as_mut() {
                            link.st.linger_until = Some(Instant::now() + LINGER);
                        }
                        self.set_phase(Phase::Idle);
                        Flow::Continue
                    }
                }
            }
            Err(error) => {
                let busy = matches!(error, DialError::Busy);
                self.on_dial_failed(&target, error).await;
                if busy {
                    self.busy
                        .push((target.endpoint.clone(), std_now() + DIAL_EXCLUSION));
                }
                self.dial_attempts += 1;
                if self.dial_attempts < MAX_DIAL_ATTEMPTS {
                    // The failed upstream is excluded for a moment; a held
                    // session may still try an offline one.
                    let step = self.plan(self.c.cursor, self.hold.is_some());
                    return self.apply_step(step, purpose, self.c.credit).await;
                }
                let block = self.c.cursor;
                let why = if busy {
                    HoldWhy::Busy
                } else {
                    HoldWhy::Offline
                };
                let bounded = !tip_region(&self.view, block);
                self.apply_step(
                    Step::Hold {
                        block,
                        why,
                        bounded,
                    },
                    purpose,
                    self.c.credit,
                )
                .await
            }
        }
    }

    /// An upstream's ABI differs from the one this client was sent: warned
    /// once per upstream and fingerprint for the listener.
    fn warn_abi_mismatch(&self, target: &DialTarget, fp: u64) {
        let first = self
            .ctx
            .abi_warned
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert((target.up, fp));
        let exclude = self.ctx.stitching.abi_mismatch == AbiMismatch::Exclude;
        if first {
            tracing::warn!(
                upstream = %target.endpoint,
                name = %target.name,
                fingerprint = %format!("{fp:016x}"),
                served = %format!("{:016x}", self.c.abi_fp),
                excluded = exclude,
                "upstream ABI differs from the ABI clients of this listener were sent"
            );
        } else {
            tracing::debug!(upstream = %target.endpoint, "upstream ABI differs from the served ABI");
        }
    }

    /// A bounded hold ran out of `gap_wait_ms`.
    async fn hold_expired(&mut self, hold: HoldInfo) -> Flow {
        self.ctx.hub.stats.hold_closes.inc(hold.why);
        let reason = hold_close_reason(hold.why, hold.block);
        tracing::warn!(
            block = hold.block,
            why = hold.why.as_str(),
            "hold expired; closing client"
        );
        self.report_failure(None, &reason);
        self.close_client(CloseCode::Error, &reason).await;
        self.end_reason = "hold expired";
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

    /// Start of the link's current silence: its last transport read (one
    /// that carried only a reply to a liveness ping does not count), or when
    /// it last started to owe blocks or made progress.
    fn silence_from(&self, link: &Link) -> Instant {
        let read_ms = link.st.data_activity_ms(link.read_ms());
        let read = self.epoch + Duration::from_millis(read_ms);
        read.max(link.st.became_owed_at)
    }

    /// When the link's liveness check acts next: a ping after half the
    /// timeout without a frame, the failover after the full timeout, or the
    /// end of a probe started because the monitor lost the upstream.
    ///
    /// A ping always gets half the timeout for its reply, so a link the
    /// session could not read for a while (a standby taken over, or a long
    /// write to a slow client) is not failed over before the upstream had a
    /// chance to answer.
    fn liveness_deadline(&self, link: &Link) -> Option<Instant> {
        let limit = self.liveness_timeout()?;
        let last_read = self.epoch + Duration::from_millis(link.read_ms());
        let at = match link.st.ping_at {
            None => last_read + limit / 2,
            Some(ping) => (last_read + limit).max(ping + limit / 2),
        };
        Some(link.st.probe_until.map_or(at, |probe| probe.min(at)))
    }

    /// A view shows the link's upstream gone offline since the last one the
    /// session saw: probe the link, as when the monitor's notification is
    /// seen directly.
    async fn probe_if_lost(&mut self) {
        let enabled = self.liveness_timeout().is_some();
        let Some(link) = self.link.as_mut() else {
            return;
        };
        let online = self.view.up(link.st.up).is_some_and(|u| u.online);
        let lost = link.st.upstream_online && !online;
        link.st.upstream_online = online;
        if lost && enabled {
            self.on_upstream_offline().await;
        }
    }

    /// The monitor marked the link's upstream offline: ping it now and fail
    /// over unless a frame arrives within [`OFFLINE_PROBE`].
    async fn on_upstream_offline(&mut self) {
        let Some(link) = self.link.as_mut() else {
            return;
        };
        if link.st.probe_until.is_none() {
            tracing::debug!(upstream = %link.endpoint, "monitor lost the upstream; probing the link");
            let now = Instant::now();
            link.st.probe_until = Some(now + OFFLINE_PROBE);
            link.st.ping_at = Some(now);
            link.ping().await;
        }
    }

    /// No frame at all from the link's upstream, not even a reply to a ping:
    /// a live upstream is marked offline (new sessions skip it until the
    /// monitor sees it answer again) and the session fails over.
    async fn on_silent(&mut self) -> Flow {
        let Some((endpoint, role)) = self.link.as_ref().map(|l| (l.endpoint.clone(), l.st.role))
        else {
            return Flow::Continue;
        };
        tracing::warn!(
            upstream = %endpoint,
            timeout_ms = self.ctx.limits.upstream_liveness_timeout_ms,
            "upstream silent: no frames and no reply to a ping; failing over"
        );
        if role == Role::Live {
            mark_offline(&self.ctx.hub, &endpoint).await;
            self.sync_view();
        }
        self.on_link_end(UpEnd::Silent).await
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
    /// link's silence deadlines, the end of a failover backoff and, stitched,
    /// a hold's expiry and retry, a wait's recheck and the linger of an idle
    /// or standby link.
    fn next_deadline(&self) -> Option<Instant> {
        let client = self
            .idle_timeout()
            .and_then(|idle| self.client_seen.checked_add(idle));
        let status = self.status.deadline();
        let phase = match self.phase {
            Phase::Backoff { until, .. } => Some(until),
            Phase::Holding { next_retry, .. } => Some(next_retry),
            Phase::Waiting { recheck, .. } => Some(recheck),
            _ => None,
        };
        let hold = self.hold.and_then(|h| h.deadline);
        let link = self.link.as_ref().filter(|l| l.st.was_owed).and_then(|l| {
            let from = self.silence_from(l);
            self.silence_limits(l)
                .into_iter()
                .flatten()
                .filter_map(|(limit, _)| from.checked_add(limit))
                .min()
        });
        let linger = self.link.as_ref().and_then(|l| l.st.linger_until);
        let standby = self.standby.as_ref().map(|s| s.until);
        let liveness = self.link.as_ref().and_then(|l| self.liveness_deadline(l));
        [client, status, phase, hold, link, linger, standby, liveness]
            .into_iter()
            .flatten()
            .min()
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
        if let Some(hold) = self.hold {
            if hold.deadline.is_some_and(|at| now >= at) {
                return self.hold_expired(hold).await;
            }
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
        let liveness_due = self
            .link
            .as_ref()
            .and_then(|l| self.liveness_deadline(l))
            .is_some_and(|at| now >= at);
        if liveness_due {
            if let Some(link) = self.link.as_mut() {
                if link.st.ping_at.is_none() && link.st.probe_until.is_none() {
                    link.st.ping_at = Some(now);
                    link.ping().await;
                } else {
                    return self.on_silent().await;
                }
            }
        }
        match self.phase {
            Phase::Backoff { until, purpose } if now >= until => {
                return self.on_backoff_expired(purpose).await;
            }
            Phase::Holding {
                next_retry,
                purpose,
            } if now >= next_retry => {
                // A retry may dial a covering upstream the monitor lost.
                let step = self.plan(self.c.cursor, true);
                return self.apply_step(step, purpose, self.c.credit).await;
            }
            Phase::Waiting {
                recheck, purpose, ..
            } if now >= recheck => {
                return self.replan(purpose).await;
            }
            _ => {}
        }
        if self
            .link
            .as_ref()
            .and_then(|l| l.st.linger_until)
            .is_some_and(|at| now >= at)
        {
            tracing::debug!("closing an idle upstream link");
            self.retire_link(false);
        }
        if self.standby.as_ref().is_some_and(|s| now >= s.until) {
            tracing::debug!("dropping an unused standby link");
            self.discard_standby();
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

/// Completes when the monitor marks the upstream of `counters` offline.
async fn went_offline(counters: Option<&UpstreamCounters>) {
    match counters {
        Some(counters) => counters.went_offline.notified().await,
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
            upstream_liveness_timeout_ms: 0,
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
        serve_one(&resolved, hub, limits).await
    }

    /// One session of a stitched listener over archives, served in-process.
    /// Each `(endpoint, begin, end)` is a frozen upstream as the monitor
    /// would report it: online, trace and chain state `[begin, end)`, head =
    /// LIB = `end - 1`, and the mock's ABI. The fleet tip is the top archive's.
    async fn start_stitched(archives: &[(&str, u32, u32)]) -> Harness {
        start_stitched_with(archives, limits(0)).await
    }

    async fn start_stitched_with(archives: &[(&str, u32, u32)], limits: ProxyLimits) -> Harness {
        let servers: Vec<Server> = archives
            .iter()
            .enumerate()
            .map(|(i, &(endpoint, begin, end))| Server {
                name: format!("archive-{i}"),
                endpoint: endpoint.to_string(),
                enabled: true,
                role: Role::Frozen,
                range: Some(crate::ship::Range { begin, end }),
                ..Default::default()
            })
            .collect();
        let resolved = ResolvedListener {
            topology: ResolvedTopology::Stitched,
            ..listener(servers.clone())
        };
        let hub = FleetHub::new(
            &resolved,
            build_state_db(servers.clone()),
            build_config_db(servers),
        );
        {
            let abi = Arc::new(crate::abi::AbiEntry::new(mock_ship::SHIP_ABI_JSON));
            let mut db = hub.state.lock().await;
            for &(endpoint, begin, end) in archives {
                let state = db.get_mut(endpoint).unwrap();
                let last = BlockPos {
                    num: end - 1,
                    id: mock_ship::fake_block_id(end - 1),
                };
                state.online = true;
                state.ranges_known = true;
                state.trace_begin_block = begin;
                state.trace_end_block = end;
                state.chain_state_begin_block = begin;
                state.chain_state_end_block = end;
                state.head = Some(last);
                state.lib = Some(last);
                state.status_v1 = Some(true);
                state.abi = Some(abi.clone());
            }
            hub.rebuild(&db);
        }
        serve_one(&resolved, hub, limits).await
    }

    /// Serve one client of `resolved` with `handle_client` behind a local
    /// listener.
    async fn serve_one(
        resolved: &ResolvedListener,
        hub: Arc<FleetHub>,
        limits: ProxyLimits,
    ) -> Harness {
        let ctx = ListenerCtx::new(resolved, limits, 1200, hub.clone());
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

    /// Stitched: an empty request after a finished archive range goes out on
    /// the lingering socket and does not keep it. The archive slot is freed
    /// when the linger runs out, and a later range dials again.
    #[tokio::test]
    async fn an_empty_request_keeps_the_archive_link_only_for_its_linger() {
        let (archive, mock) = start_mock(MockShipConfig::frozen(1001, 2001)).await;
        let harness = start_stitched(&[(&archive, 1001, 2001)]).await;
        let mut client = connect(&harness.url).await;
        let slots = harness.hub.view().upstreams[0].connections.clone();

        client.send(blocks_request(1001, 1100, 1000)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 99).await,
            (1001..1100).collect::<Vec<_>>()
        );
        let finished = Instant::now();
        assert_eq!(slots.load(Ordering::Relaxed), 1, "the link lingers");

        // Halfway through the linger, the client stops with an empty request.
        tokio::time::sleep(LINGER / 2).await;
        client.send(blocks_request(1100, 1100, 0)).await.unwrap();
        timeout(WAIT, async {
            while blocks_requests(&mock).len() < 2 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the empty request reaches the archive");
        assert_eq!(
            blocks_requests(&mock),
            vec![(1, 1001, 1100, vec![]), (1, 1100, 1100, vec![])],
            "the empty request went out on the lingering socket"
        );

        timeout(LINGER + WAIT, async {
            while slots.load(Ordering::Relaxed) > 0 {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the empty request kept the archive slot");
        // The empty request does not restart the linger, which would end
        // LINGER / 2 later.
        let freed = finished.elapsed();
        assert!(
            freed < LINGER + LINGER * 2 / 5,
            "freed {freed:?} after the range ended"
        );

        client.send(blocks_request(1200, 1203, 10)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 3).await,
            vec![1200, 1201, 1202]
        );
        assert_eq!(mock.sessions_accepted(), 2, "the next range dials again");
    }

    /// An upstream that completes the handshake and sends the ABI, then
    /// neither reads nor writes, like a frozen VM: no pong ever comes.
    async fn dead_upstream() -> (String, JoinHandle<()>) {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = tcp.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = tcp.accept().await {
                let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
                ws.send(Message::text(mock_ship::SHIP_ABI_JSON))
                    .await
                    .unwrap();
                held.push(ws);
            }
        });
        (endpoint, task)
    }

    fn liveness(ms: u64) -> ProxyLimits {
        ProxyLimits {
            upstream_liveness_timeout_ms: ms,
            ..limits(0)
        }
    }

    /// A silent upstream gets a ping after half the liveness timeout and is
    /// failed over after the full timeout: marked offline, without a strike,
    /// and the client resumes at its next block.
    #[tokio::test]
    async fn a_silent_upstream_is_pinged_then_failed_over_and_marked_offline() {
        let (dead, dead_task) = dead_upstream().await;
        let (healthy, _healthy_mock) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            ..Default::default()
        })
        .await;
        let harness = start_session_with(&[&dead, &healthy], &[&dead], liveness(400)).await;
        let started = Instant::now();
        let mut client = connect(&harness.url).await;
        set_online(&harness.hub, &healthy).await;

        client.send(blocks_request(100, 103, 10)).await.unwrap();
        assert_eq!(read_block_nums(&mut client, 3).await, vec![100, 101, 102]);
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_millis(350),
            "failed over after {waited:?}"
        );
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Silent), 1);
        let state = harness.hub.state.lock().await;
        assert!(
            !state[&dead].online,
            "the silent upstream is marked offline"
        );
        assert_eq!(state[&dead].block_failures, 0, "no strike");
        drop(state);
        dead_task.abort();
    }

    /// When the monitor loses the link's upstream, the session pings it at
    /// once and fails over after 1 s without a reply, well before its own
    /// liveness timeout or the 5 s offline silence.
    #[tokio::test]
    async fn the_monitor_losing_the_upstream_probes_the_link_for_one_second() {
        let (dead, dead_task) = dead_upstream().await;
        let (healthy, _healthy_mock) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            ..Default::default()
        })
        .await;
        let harness = start_session_with(&[&dead, &healthy], &[&dead], liveness(20_000)).await;
        let mut client = connect(&harness.url).await;
        set_online(&harness.hub, &healthy).await;
        client.send(blocks_request(100, 103, 10)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let lost = Instant::now();
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&dead)
                .unwrap()
                .mark_offline(std::time::Instant::now());
            harness.hub.rebuild(&db);
        }
        assert_eq!(read_block_nums(&mut client, 3).await, vec![100, 101, 102]);
        let waited = lost.elapsed();
        assert!(
            waited >= Duration::from_millis(900) && waited < Duration::from_secs(4),
            "failed over after {waited:?}"
        );
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Silent), 1);
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Offline), 0);
        dead_task.abort();
    }

    /// An upstream that answers every liveness ping but sends no data while
    /// blocks are owed still reaches the idle timeout: replies to the
    /// router's own pings are not upstream activity.
    #[tokio::test]
    async fn idle_time_counts_from_the_last_data_frame_not_from_liveness_pongs() {
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
        let quiet = ProxyLimits {
            idle_timeout_ms: 800,
            upstream_liveness_timeout_ms: 200,
            ..limits(0)
        };
        let harness = start_session_with(&[&holding, &healthy], &[&holding], quiet).await;
        let mut client = connect(&harness.url).await;
        set_online(&harness.hub, &healthy).await;
        client.send(blocks_request(100, 106, 100)).await.unwrap();
        assert_eq!(read_block_nums(&mut client, 3).await, vec![100, 101, 102]);

        // The client pings to stay clear of its own idle timeout.
        let (mut tx, mut rx) = client.split();
        let blocks = timeout(Duration::from_secs(5), async {
            let mut blocks = Vec::new();
            let mut ping = tokio::time::interval(Duration::from_millis(200));
            while blocks.len() < 3 {
                tokio::select! {
                    msg = rx.next() => match msg {
                        Some(Ok(Message::Binary(data))) => {
                            let view = BlockView::parse(&data).expect("a block result");
                            blocks.extend(view.num());
                        }
                        Some(Ok(_)) => {}
                        other => panic!("unexpected {other:?}"),
                    },
                    _ = ping.tick() => tx.send(Message::Ping(Vec::new().into())).await.unwrap(),
                }
            }
            blocks
        })
        .await
        .expect("the idle link should fail over");
        assert_eq!(blocks, vec![103, 104, 105]);
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Idle), 1);
        assert_eq!(
            harness.hub.stats.failovers.get(FailoverReason::Silent),
            0,
            "the holding upstream answers pings"
        );
    }

    /// The session's entry in the dashboard registry, by its client address.
    fn registered(client: &TestClient) -> Arc<telemetry::SessionInfo> {
        let addr = match client.get_ref() {
            tokio_tungstenite::MaybeTlsStream::Plain(tcp) => tcp.local_addr().unwrap(),
            _ => unreachable!("plain test client"),
        };
        telemetry::global()
            .sessions()
            .into_iter()
            .find(|s| s.client == addr)
            .expect("the session is registered")
    }

    /// Stitched: an operator move leaves the replica the session streams
    /// from for its twin, resuming at the cursor without a gap, and an
    /// operator close ends the session with 1000.
    #[tokio::test]
    async fn an_operator_move_resumes_on_another_replica_and_close_ends_the_session() {
        let replica = || MockShipConfig {
            block_delay: Some(Duration::from_millis(5)),
            ..MockShipConfig::frozen(1, 2001)
        };
        let (a, mock_a) = start_mock(replica()).await;
        let (b, mock_b) = start_mock(replica()).await;
        let harness = start_stitched(&[(&a, 1, 2001), (&b, 1, 2001)]).await;
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        assert_eq!(&*info.listener, "default");

        client.send(blocks_request(10, 1000, 1000)).await.unwrap();
        let mut blocks = read_block_nums(&mut client, 20).await;
        assert_eq!(info.start_block(), Some(10));
        let first = info.upstream();
        assert!(first == a || first == b, "{first}");

        info.request_move();
        blocks.extend(read_block_nums(&mut client, 50).await);
        assert_eq!(blocks, (10..80).collect::<Vec<_>>(), "no gap, no duplicate");
        let second = info.upstream();
        assert_ne!(second, first, "moved to the other replica");
        assert_eq!(info.failovers(), 1);
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Operator), 1);
        let resumed = if second == a { &mock_a } else { &mock_b };
        let resume = blocks_requests(resumed)[0].1;
        assert!(
            (30..80).contains(&resume),
            "the twin resumed at the cursor, got {resume}"
        );

        info.request_close();
        let close = timeout(WAIT, async {
            loop {
                match client.next().await {
                    Some(Ok(Message::Close(frame))) => return frame,
                    Some(Ok(_)) => {}
                    other => panic!("no close frame: {other:?}"),
                }
            }
        })
        .await
        .expect("closed by the operator");
        let frame = close.expect("a close frame with a code");
        assert_eq!(frame.code, CloseCode::Normal);
        assert_eq!(frame.reason.as_str(), "closed by operator");
        timeout(WAIT, harness.session).await.unwrap().unwrap();
        assert!(
            telemetry::global().session(info.id).is_none(),
            "a finished session leaves the registry"
        );
    }

    /// Stitched: the next leg's pre-dialed link sat unread for longer than
    /// the liveness timeout. Taking it over pings it and leaves the reply
    /// time to arrive instead of failing the link over as silent.
    #[tokio::test]
    async fn a_standby_link_taken_over_after_the_liveness_timeout_is_kept() {
        let leg = |begin, end| MockShipConfig {
            block_delay: Some(Duration::from_millis(5)),
            ..MockShipConfig::frozen(begin, end)
        };
        let (a, _mock_a) = start_mock(leg(1, 1001)).await;
        let (b, mock_b) = start_mock(leg(1001, 2001)).await;
        let harness = start_stitched_with(&[(&a, 1, 1001), (&b, 1001, 2001)], liveness(400)).await;
        let mut client = connect(&harness.url).await;
        // A's 200 blocks take about a second at 5 ms each, while B's
        // standby link waits unread.
        client.send(blocks_request(800, 1100, 1000)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 300).await,
            (800..1100).collect::<Vec<_>>()
        );
        assert_eq!(harness.hub.stats.predials.get(PredialResult::Used), 1);
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Silent), 0);
        assert_eq!(mock_b.sessions_accepted(), 1, "the standby link is kept");
    }
}
