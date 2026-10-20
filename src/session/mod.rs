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
    can_take_over, eligible, next_step, pool_pick, pool_rank, run_end, serve_on, tip_region,
    Excludes, Need, ServeStep, Shape, Step, Union,
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
    commit, head_lib_for, judge, request_is_empty, ClientSide, Forward, Judgement, Lacks,
    PendingClientFrames, ResultKind, MAX_PENDING_CLIENT_BYTES,
};
use failover::{
    block_unavailable_reason, blockless_move_backoff, classify, hold_backoff, hold_close_reason,
    jitter, note_block_failure, plan_same_block_failover, EndAction, EndFacts, Exclusion, UpEnd,
    ViolationKind, ABI_EXCLUSION, DIAL_EXCLUSION, MOVE_EXCLUSION,
};
pub use link::ConnectionGuard;
use link::{
    build_request, mark_no_block_log, mark_offline, owed, quiescent, settled, BlocklessCheck,
    DialError, DialTarget, DialTask, Established, Link, Purpose, Target, UpCursor, LIVENESS_PING,
    MAX_DIAL_ATTEMPTS,
};
use status::{router_status, status_ready, StatusFlow, StatusStep};

type ClientWs = WebSocketStream<TcpStream>;

/// A link whose upstream the monitor marked offline is failed over after
/// this much silence while it owes blocks.
const MONITOR_OFFLINE_SILENCE: Duration = Duration::from_secs(5);

/// When the monitor marks the current upstream offline, the session pings it
/// at once and fails over if nothing arrives within this time.
const OFFLINE_PROBE: Duration = Duration::from_secs(1);

/// A live upstream that stalls while no other upstream can serve the next
/// block is kept for this many times `upstream_stall_timeout_ms` of silence,
/// then failed over anyway: that redials it, and a new SHiP session clears a
/// wedge that only one connection has.
const SOLE_STALL_FACTOR: u32 = 3;

/// A kept stall asks at least this often whether another upstream can take
/// the stream now, besides every fleet view change: a freed slot, or an
/// exclusion or a quarantine that ran out, publishes no new view.
const STALL_RECHECK: Duration = Duration::from_secs(1);

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

/// Stitched: a link that forwards results without the `block` its request
/// asked for asks at least this often whether an upstream that holds blocks
/// can take the stream, besides every fleet view change: a freed slot, or an
/// exclusion or a quarantine that ran out, publishes no new view.
const BLOCKLESS_RECHECK: Duration = Duration::from_secs(1);

/// What every session of one listener shares.
pub struct ListenerCtx {
    pub config_generation: u64,
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
    #[cfg(test)]
    pub fn new(
        listener: &ResolvedListener,
        limits: ProxyLimits,
        live_lag_blocks: u32,
        hub: Arc<FleetHub>,
    ) -> Arc<Self> {
        Self::with_generation(listener, limits, live_lag_blocks, hub, 0)
    }

    pub fn with_generation(
        listener: &ResolvedListener,
        limits: ProxyLimits,
        live_lag_blocks: u32,
        hub: Arc<FleetHub>,
        config_generation: u64,
    ) -> Arc<Self> {
        Arc::new(ListenerCtx {
            config_generation,
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
    /// The retry made when `deadline` came: the hold ends the next time it
    /// runs out.
    last_try: bool,
}

/// Stitched: a link to the next leg's upstream, connected (ABI read) ahead of
/// the handoff and not read until it is activated.
struct Standby {
    link: Link,
    /// Dropped unused at this instant.
    until: Instant,
}

/// Why a session left an upstream, reported once its next link is up.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Left {
    Failover(FailoverReason),
    /// Pool: a new request went to an upstream that serves it better.
    Reroute,
}

impl Left {
    fn label(self) -> &'static str {
        match self {
            Left::Failover(reason) => reason.as_str(),
            Left::Reroute => "reroute",
        }
    }

    /// Operator moves, moves off an archive without a block log (nothing is
    /// wrong with it) and reroutes are routine; other failovers are not.
    fn level(self) -> Level {
        match self {
            Left::Failover(FailoverReason::Operator | FailoverReason::NoBlockLog)
            | Left::Reroute => Level::Info,
            Left::Failover(_) => Level::Warn,
        }
    }
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

/// What a block result from `u` lacks of the fields the request asked for
/// and `u` advertises at block `n` (the frozen field contract). A result
/// whose payload fields do not decode lacks data. SHiP status says nothing
/// about block logs, so a missing `block` is told apart: an archive without
/// a block log sends none, as Spring does for a block its log lacks.
fn lacks(
    u: Option<&UpstreamView>,
    creq: &BlocksRequest,
    frame: &[u8],
    h: &ResultHead,
    n: u32,
) -> Lacks {
    let Some(u) = u else {
        return Lacks::Nothing;
    };
    let Ok(p) = wire::result_presence(frame, h) else {
        return Lacks::Data;
    };
    if (creq.fetch_traces && u.covers(Log::Trace, n) && !p.traces)
        || (creq.fetch_deltas && u.covers(Log::ChainState, n) && !p.deltas)
        || (creq.v1 && creq.fetch_finality_data && u.covers(Log::Finality, n) && !p.finality)
    {
        Lacks::Data
    } else if creq.fetch_block && u.covers_ids(n) && !p.block {
        Lacks::Block
    } else {
        Lacks::Nothing
    }
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
    /// Stitched: when the last handoff started (the old link's last frame,
    /// or the end of a wait or hold before the next leg), until the new
    /// link's first frame.
    handoff_started: Option<Instant>,
    /// Stitched: the upstream whose leg the cursor finished, until the next
    /// leg is served. Served from another upstream, that is a handoff.
    handoff_from: Option<usize>,
    last_jump_warn: Option<Instant>,
    counts: Counts,
    end_reason: &'static str,
    /// This session in the dashboard registry, which an operator moves or
    /// closes it through. Removed when the session ends.
    registry: SessionRegistration,
    /// The upstream the last failover, operator move or reroute left, and
    /// why. The move is reported once the next link is up.
    moved_from: Option<(Arc<str>, Left)>,
    /// The upstream an operator move left, hard-excluded (for at most
    /// [`MOVE_EXCLUSION`]) until the next link is up or nothing else can take
    /// the session.
    moved_off: Option<Arc<str>>,
    /// A handler took a new view with [`Session::sync_view`]; the loop has
    /// not acted on it yet (see [`Session::on_view_change`]).
    view_unseen: bool,
    /// Stitched: moves off archives without a block log made since a result
    /// last came with all it asked for, and when the last one was made (see
    /// [`blockless_move_backoff`]).
    blockless_moves: u32,
    blockless_moved_at: Option<Instant>,
    /// Upstreams left at the current seam because their first result did
    /// not continue the last delivered block. Their exclusion is lifted when
    /// no upstream continues it and the session forwards the discontinuity
    /// after all: they are not to blame then.
    seam_excluded: Vec<Arc<str>>,
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
        let registry = telemetry::global().register_generation(
            ctx.label.clone(),
            client,
            "",
            ctx.config_generation,
        );
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
            handoff_from: None,
            last_jump_warn: None,
            counts: Counts::default(),
            end_reason: "client closed",
            registry,
            moved_from: None,
            moved_off: None,
            view_unseen: false,
            blockless_moves: 0,
            blockless_moved_at: None,
            seam_excluded: Vec::new(),
        }
    }

    fn set_phase(&mut self, phase: Phase) {
        let gauge = phase.gauge();
        self.ctx.hub.stats.session_moved(self.gauge, Some(gauge));
        self.gauge = Some(gauge);
        self.phase = phase;
    }

    /// Take the latest published fleet view, if a new one exists. The loop
    /// acts on it next (a probe, a waiting status request, a standby, a wait
    /// or hold), as if it had seen the change itself.
    fn sync_view(&mut self) {
        if self.view_rx.has_changed().unwrap_or(false) {
            self.view = self.view_rx.borrow_and_update().clone();
            self.view_unseen = true;
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
            if self.view_unseen || self.view_rx.has_changed().unwrap_or(false) {
                self.sync_view();
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
            // silent at its own tip. A stitched session that waits, holds or
            // dials a leg re-plans on every view, and a standby link is dropped
            // when its upstream goes away.
            let waits_on_view = self.status.waiting()
                || self
                    .link
                    .as_ref()
                    .is_some_and(|l| l.st.req.is_some() && self.c.credit > 0)
                || matches!(
                    self.phase,
                    Phase::Waiting { .. }
                        | Phase::Holding { .. }
                        | Phase::Dialing { step: Some(_), .. }
                )
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
        self.view_unseen = false;
        // A kept stall asks again whether another upstream can take over.
        if let Some(link) = self.link.as_mut() {
            link.st.stall_recheck = None;
        }
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
            // A leg's dial is kept while its upstream can still serve the
            // cursor, whatever ranks higher by now (an exclusion that ran
            // out, a load shift): the dial itself tells. Only its request
            // end follows the view. An upstream the monitor sees offline is
            // kept only while no other online upstream can serve the cursor,
            // excluded ones and the one just failed off aside (a dead host
            // would hold the leg until the dial times out). An
            // upstream that can no longer serve the cursor (disabled,
            // quarantined, its range shrunk below it) is replaced before the
            // request is sent.
            Phase::Dialing {
                purpose,
                step: Some(cur),
            } if self.c.in_progress() => {
                if let Some(st) = self.dialed_leg(&cur) {
                    if let Some(step) = self.online_instead_of(&cur) {
                        tracing::info!(
                            upstream = %cur.endpoint,
                            block = self.c.cursor,
                            "upstream went offline while it was being dialed; serving from another"
                        );
                        return self.apply_step(step, purpose, self.c.credit).await;
                    }
                    if st.req_end != cur.req_end {
                        self.set_phase(Phase::Dialing {
                            purpose,
                            step: Some(st),
                        });
                    }
                    return Flow::Continue;
                }
                let step = self.plan(self.c.cursor, self.hold.is_some());
                match &step {
                    Step::Serve(_) => {
                        if self.view.up(cur.up).is_some_and(|u| u.routable()) {
                            tracing::info!(
                                upstream = %cur.endpoint,
                                block = self.c.cursor,
                                "upstream can no longer serve the leg it was being dialed for; serving from another"
                            );
                        } else {
                            tracing::info!(
                                upstream = %cur.endpoint,
                                "upstream disabled while it was being dialed; serving from another"
                            );
                        }
                        self.apply_step(step, purpose, self.c.credit).await
                    }
                    // Nothing else can serve the leg: it stays, as on any
                    // upstream an operator disables.
                    Step::Hold { .. }
                        if self
                            .view
                            .up(cur.up)
                            .is_some_and(|u| u.enabled && u.operator_disabled) =>
                    {
                        Flow::Continue
                    }
                    _ => self.apply_step(step, purpose, self.c.credit).await,
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
    /// when no other upstream can serve the next block now.
    ///
    /// With a request in progress, the check is the one the failover makes
    /// (the plan or the pool pick, without this upstream), so a full,
    /// quarantined or excluded upstream never counts. The failover then
    /// hard-excludes the upstream it left: the soft exclusion of an ordinary
    /// failover ranks below role preference (stitched) and below coverage
    /// and freshness (pool), so it alone could land on the same upstream.
    async fn on_operator_move(&mut self) -> Flow {
        let Some(endpoint) = self.link.as_ref().map(|l| l.endpoint.clone()) else {
            tracing::info!("operator move skipped: the session has no upstream link");
            return Flow::Continue;
        };
        self.sync_view();
        let next_block = self.c.req.is_some().then_some(self.c.cursor);
        let other = match self.c.req.as_ref() {
            Some(creq) if self.c.in_progress() => {
                self.alternative_for(self.c.cursor, creq, &endpoint)
            }
            _ => {
                let now = std_now();
                self.view.upstreams.iter().any(|u| {
                    u.endpoint != endpoint
                        && !self.ex.hard_active(&u.endpoint, now)
                        && can_take_over(u, next_block)
                })
            }
        };
        if !other {
            tracing::info!(
                upstream = %endpoint,
                ?next_block,
                "operator move skipped: no other upstream can serve the next block"
            );
            return Flow::Continue;
        }
        tracing::info!(upstream = %endpoint, "moving session at operator request");
        let flow = self.on_link_end(UpEnd::Moved).await;
        let failing_over = matches!(
            &self.moved_from,
            Some((from, Left::Failover(FailoverReason::Operator))) if *from == endpoint
        );
        if failing_over {
            self.ex
                .add_hard(endpoint.clone(), MOVE_EXCLUSION, std_now());
            self.moved_off = Some(endpoint);
        }
        flow
    }

    /// Lift the exclusion an operator move put on the upstream it left: the
    /// move is done (the next link is up), or nothing else can take the
    /// session, which then returns there rather than hold or close, as after
    /// an ordinary failover. Returns whether there was one to lift.
    fn release_move_exclusion(&mut self) -> bool {
        let Some(from) = self.moved_off.take() else {
            return false;
        };
        self.ex.hard.retain(|(ep, _)| *ep != from);
        true
    }

    /// `pool_pick` for this session, back to the upstream an operator move
    /// left when nothing else can take it.
    fn pool_pick_for(&mut self, route: Option<(Need, u32)>, failover: bool) -> Option<usize> {
        let pick = |s: &Self| {
            pool_pick(
                &s.view,
                route,
                &s.ex,
                failover,
                s.id,
                s.ctx.live_lag_blocks,
                std_now(),
            )
        };
        if let Some(up) = pick(self) {
            return Some(up);
        }
        if self.release_move_exclusion() {
            return pick(self);
        }
        None
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

    /// `C += n` (saturating), and what C rose by is forwarded to the link
    /// while it has a request, so the upstream's counter stays C less the
    /// results in flight (see [`ClientSide::ack`]). Without a link request
    /// it is folded into the credit the next request carries.
    async fn on_client_ack(&mut self, n: u32) -> Flow {
        let raised = self.c.ack(n);
        if raised == 0 {
            return Flow::Continue;
        }
        if let Some(link) = self.link.as_mut() {
            if link.st.req.is_some() {
                // A failed write shows up as the end of the link's stream.
                let _ = link
                    .tx
                    .send(Message::Binary(wire::encode_ack(raised).into()))
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
            if let Some(link) = self.link.as_ref() {
                // Counted and reported like a failover off the link's upstream.
                link.counters
                    .failovers_total
                    .fetch_add(1, Ordering::Relaxed);
                self.registry.info.note_failover();
                self.moved_from = Some((link.endpoint.clone(), Left::Reroute));
            }
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
                        |n| lacks(u, creq, &frame, &h, n),
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
            Some(Judgement::End(UpEnd::Discontinuity)) => self.on_discontinuity(&h).await,
            Some(Judgement::End(end)) => self.on_link_end(end).await,
            Some(Judgement::Duplicate { advance_to }) => self.drop_duplicate(advance_to, &h).await,
            Some(Judgement::Forward(fw)) if fw.no_block => {
                self.forward_without_block(frame, h, fw).await
            }
            Some(Judgement::Forward(fw)) => {
                // Nothing the request asked for is missing: any move off an
                // archive without a block log got there.
                self.blockless_moves = 0;
                self.forward_result(frame, h, fw).await
            }
        }
    }

    /// A live seam's first result does not continue the last delivered
    /// block, and another upstream covers the cursor: logged with both ids,
    /// then failed over (no strike; the upstream is avoided for a while).
    async fn on_discontinuity(&mut self, h: &ResultHead) -> Flow {
        let Some(endpoint) = self.link.as_ref().map(|l| l.endpoint.clone()) else {
            return Flow::Continue;
        };
        let hex = |p: Option<BlockPos>| p.map(|p| crate::tasks::hex_32(&p.id));
        tracing::warn!(
            upstream = %endpoint,
            block = h.this_block.map(|p| p.num),
            expected_prev = hex(self.c.last).as_deref(),
            prev = hex(h.prev_block).as_deref(),
            "upstream result does not continue the last delivered block; failing over"
        );
        let flow = self.on_link_end(UpEnd::Discontinuity).await;
        if self.ex.hard_active(&endpoint, std_now()) && !self.seam_excluded.contains(&endpoint) {
            self.seam_excluded.push(endpoint);
        }
        flow
    }

    /// A frozen result without the `block` the request asked for, for a block
    /// its upstream covers: the archive has no block log there. That is no
    /// contract violation (no strike, no failure count): Spring sends the
    /// same result when its block log lacks the block.
    ///
    /// The upstream is flagged for the listener (warned once), so plans rank
    /// it below upstreams that hold blocks. When the failover that leaves it
    /// would serve the cursor from one of those, the session fails over and
    /// resumes at this block. Otherwise the result is forwarded without
    /// `block`, as one node with a pruned block log sends it, and counted.
    ///
    /// That answer stands (see [`BlocklessCheck`]) until the fleet view
    /// changes, the cursor reaches the archive's run end (where another
    /// upstream may become eligible) or [`BLOCKLESS_RECHECK`] has passed. A
    /// move that ended on such an archive again delays the next one (see
    /// [`blockless_move_backoff`]).
    async fn forward_without_block(&mut self, frame: Bytes, h: ResultHead, fw: Forward) -> Flow {
        let Some((up, endpoint, name)) = self
            .link
            .as_ref()
            .map(|l| (l.st.up, l.endpoint.clone(), l.name.clone()))
        else {
            return Flow::Continue;
        };
        let block = self.c.cursor;
        if !self.view.up(up).is_some_and(|u| u.no_block_log) {
            if mark_no_block_log(&self.ctx.hub, &endpoint).await {
                tracing::warn!(
                    upstream = %endpoint,
                    name = %name,
                    block,
                    "frozen upstream sent no block for a block it covers (no block log); it is skipped for fetch_block requests while an upstream that holds blocks can serve them"
                );
            }
            self.sync_view();
        }
        let now = Instant::now();
        let generation = self.view.generation;
        let due = self.link.as_ref().is_some_and(|l| {
            l.st.blockless_checked
                .is_none_or(|c| c.due(generation, block, now))
        });
        let may_move = self
            .blockless_moved_at
            .is_none_or(|at| now >= at + blockless_move_backoff(self.blockless_moves));
        if due && may_move {
            if let Some(holder) = self.block_holder_for(block, &endpoint) {
                tracing::info!(
                    from = %name,
                    to = %self.view.up(holder).map_or("", |u| &*u.name),
                    block,
                    "moving to an upstream that holds blocks"
                );
                self.blockless_moves = self.blockless_moves.saturating_add(1);
                self.blockless_moved_at = Some(now);
                return self.on_link_end(UpEnd::NoBlockLog).await;
            }
            let until = self.blockless_until(up, block);
            if let Some(link) = self.link.as_mut() {
                link.st.blockless_checked = Some(BlocklessCheck {
                    generation,
                    until,
                    next: now + BLOCKLESS_RECHECK,
                });
            }
        }
        if let Some(u) = self.ctx.hub.stats.upstream(up) {
            u.results_without_block.fetch_add(1, Ordering::Relaxed);
        }
        self.forward_result(frame, h, fw).await
    }

    /// Stitched: the upstream (by index), not known to lack a block log,
    /// from which the failover that leaves the archive `endpoint` at block
    /// `k` ([`UpEnd::NoBlockLog`]: `endpoint` soft-excluded, the other
    /// exclusions kept) would serve the current request, if any. This is the
    /// plan that failover makes, so a session leaves only for the blocks: an
    /// upstream that holds them but ranks below the archive (stale, behind
    /// the block, full) would only send it back, again and again.
    fn block_holder_for(&self, k: u32, endpoint: &str) -> Option<usize> {
        let creq = self.c.req.as_ref()?;
        let mut ex = self.ex.clone();
        ex.soft = Some(Arc::from(endpoint));
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
            std_now(),
        );
        match step {
            Step::Serve(st)
                if *st.endpoint != *endpoint
                    && self.view.up(st.up).is_some_and(|u| !u.no_block_log) =>
            {
                Some(st.up)
            }
            _ => None,
        }
    }

    /// Stitched: where the run of archive `up` for the current request,
    /// from block `k`, ends in the current view ([`run_end`]). For an archive
    /// without a block log that is also where another upstream that holds
    /// blocks becomes eligible.
    fn blockless_until(&self, up: usize, k: u32) -> u32 {
        match (self.c.req.as_ref(), self.view.up(up)) {
            (Some(creq), Some(u)) => run_end(&self.view, u, Need::of(creq), k, std_now()),
            _ => u32::MAX,
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
            // No upstream continues the block: those left at this seam for
            // not continuing it are usable again.
            let released = std::mem::take(&mut self.seam_excluded);
            if !released.is_empty() {
                self.ex.hard.retain(|(ep, _)| !released.contains(ep));
            }
        } else {
            self.seam_excluded.clear();
        }
        if fw.variant_mismatch {
            tracing::debug!(upstream = %endpoint, "result variant differs from the request version");
        }
        if self.c.charge() {
            stats.credit_overruns.fetch_add(1, Ordering::Relaxed);
            tracing::debug!(upstream = %endpoint, block, "result forwarded with no client credit");
        }
        let frame = match role {
            Role::Frozen => self.fill_prev_block(frame, &h, up),
            Role::Live => frame,
        };
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

    /// A frozen result for block `n > 1` without `prev_block`, while the
    /// last block this session delivered is `n - 1` and irreversible: the
    /// router fills `prev_block` in from it, so the stream is the one a node
    /// that knows block `n - 1` sends. An archive leaves it out for the first
    /// block of its range when the block before is in no log it has (no
    /// block log). Only that optional changes; head/LIB are patched after.
    fn fill_prev_block(&self, frame: Bytes, h: &ResultHead, up: usize) -> Bytes {
        let (Some(this), None, Some(last)) =
            (h.this_block, h.prev_block, self.c.last_irreversible())
        else {
            return frame;
        };
        if this.num <= 1 || last.num.checked_add(1) != Some(this.num) {
            return frame;
        }
        match wire::insert_prev_block(&frame, h, last) {
            Some((filled, _)) => {
                if let Some(u) = self.ctx.hub.stats.upstream(up) {
                    u.prev_blocks_filled.fetch_add(1, Ordering::Relaxed);
                }
                tracing::debug!(
                    block = this.num,
                    "filled the missing prev_block from the block delivered before"
                );
                filled
            }
            None => frame,
        }
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
            // No upstream carries the session until its next link is up: a
            // drain of the old one must not count it, nor a move pick it.
            self.registry.info.set_upstream("");
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
                | UpEnd::NoBlockLog
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
                self.moved_from = Some((endpoint.clone(), Left::Failover(reason)));
                if reason == FailoverReason::NoBlockLog {
                    // Routine: the move was logged where it was decided.
                    tracing::debug!(
                        upstream = %endpoint,
                        block = k,
                        "leaving an archive without a block log"
                    );
                } else if plan.failures > 0 {
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
        if let Some(up) = self.pool_pick_for(route, true) {
            return Some(up);
        }
        self.retired.take()?;
        self.pool_pick_for(route, true)
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
        tracing::debug!(
            upstream = %target.endpoint,
            name = %target.name,
            "connected to upstream"
        );
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
        let _change = telemetry::global().begin_change();
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
        // An operator move is done: from now on the upstream it left is only
        // soft-excluded, like after any failover.
        self.release_move_exclusion();
        if let Some((from, left)) = self.moved_from.take() {
            telemetry::global().emit(
                Event::new(
                    left.level(),
                    EventKind::SessionFailover,
                    format!("moved from {from} to {endpoint} ({})", left.label()),
                )
                .listener(&self.ctx.label)
                .upstream(&endpoint)
                .session(self.registry.info.id),
            );
        }
    }

    /// Whether an operator disabled (or drained) the upstream of a dial that
    /// just finished while it was on its way. A drain moves only the
    /// sessions already on the upstream, so such a session is left to go
    /// elsewhere itself.
    fn disabled_while_dialing(&mut self, target: &DialTarget) -> bool {
        self.sync_view();
        !self.view.up(target.up).is_some_and(|u| u.routable())
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
                if self.disabled_while_dialing(&target) {
                    let route = self.c.req.as_ref().map(|r| (Need::of(r), self.c.cursor));
                    if let Some(up) = self.pool_pick_for(route, purpose != Purpose::Initial) {
                        tracing::info!(
                            upstream = %target.endpoint,
                            "upstream disabled while it was being dialed; dialing another"
                        );
                        drop(est);
                        if self.start_dial(up, purpose, None) {
                            return Flow::Continue;
                        }
                        return self.failover_unavailable().await;
                    }
                    // Nothing else can take the session: it stays, as on
                    // any upstream an operator disables.
                }
                self.adopt_link(&target, est, purpose.label());
                if purpose == Purpose::Failover {
                    tracing::info!(
                        upstream = %target.endpoint,
                        name = %target.name,
                        "reconnected to upstream after failover"
                    );
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
                    let next = self.pool_pick_for(route, purpose != Purpose::Initial);
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
        self.handoff_from = None;
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
    /// A hold right after an operator move is planned again with the
    /// upstream the move left: the session returns there rather than hold.
    fn plan(&mut self, from: u32, allow_offline: bool) -> Step {
        let Some(shape) = self.c.req.as_ref().map(Shape::of) else {
            return Step::Done;
        };
        let mut step = self.next_step_for(&shape, from, &self.ex, allow_offline);
        if matches!(step, Step::Hold { .. }) && self.release_move_exclusion() {
            step = self.next_step_for(&shape, from, &self.ex, allow_offline);
        }
        if matches!(
            step,
            Step::Hold {
                why: HoldWhy::AtCapacity,
                ..
            }
        ) && self.retired.take().is_some()
        {
            step = self.next_step_for(&shape, from, &self.ex, allow_offline);
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

    fn next_step_for(&self, shape: &Shape, from: u32, ex: &Excludes, allow_offline: bool) -> Step {
        let tip = self.ctx.hub.fresh_tip(&self.view);
        next_step(
            &self.view,
            tip,
            shape,
            from,
            ex,
            &self.own_slots(),
            self.id,
            &self.ctx.stitching,
            self.ctx.live_lag_blocks,
            allow_offline,
            std_now(),
        )
    }

    /// The leg the dial on its way to `cur.up` would now be sent, when that
    /// upstream can still serve the cursor (see [`serve_on`]). It has no
    /// side effects, unlike [`Session::plan`].
    fn dialed_leg(&self, cur: &ServeStep) -> Option<ServeStep> {
        let shape = self.c.req.as_ref().map(Shape::of)?;
        serve_on(
            &self.view,
            self.ctx.hub.fresh_tip(&self.view),
            &shape,
            self.c.cursor,
            cur.up,
            &self.ex,
            std_now(),
        )
    }

    /// The leg an online upstream would serve instead of the dial on its
    /// way to `cur.up`, when the monitor sees that upstream offline. It is
    /// never an excluded upstream. While the failover that set the soft
    /// exclusion is still under way (`moved_from` names it), that upstream
    /// counts as hard here, since online it would outrank the dial and take
    /// the leg back there to fail again. An older soft exclusion, from a
    /// failover whose next link came up, is only a preference, as in
    /// [`Session::plan`]. It has no side effects, unlike `plan`: an operator
    /// move's exclusion stays, so a blip on the move's target keeps its dial.
    fn online_instead_of(&self, cur: &ServeStep) -> Option<Step> {
        if self.view.up(cur.up).is_some_and(|u| u.online) {
            return None;
        }
        let shape = self.c.req.as_ref().map(Shape::of)?;
        let mut ex = self.ex.clone();
        let leaving = self.moved_from.as_ref().map(|(from, _)| from);
        if let Some(failed) = ex.soft.take_if(|soft| leaving == Some(&*soft)) {
            ex.add_hard(failed, Duration::MAX, std_now());
        }
        let step = self.next_step_for(&shape, self.c.cursor, &ex, false);
        matches!(&step, Step::Serve(st) if st.up != cur.up).then_some(step)
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
            Step::Serve(st) => {
                if purpose == Purpose::Handoff {
                    self.note_handoff(&st);
                }
                self.serve(st, purpose, pre).await
            }
            Step::Done => {
                self.hold = None;
                self.handoff_started = None;
                self.handoff_from = None;
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
                let (since, attempt, last_try) = match self.hold {
                    Some(h) if h.block == block => {
                        (h.since, h.attempt.saturating_add(1), h.last_try)
                    }
                    _ => {
                        self.ctx.hub.stats.holds.inc(why);
                        tracing::warn!(
                            block,
                            why = why.as_str(),
                            bounded,
                            "no upstream can serve the block now; holding"
                        );
                        (now, 0, false)
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
                    last_try,
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
    /// already running to it, or on a new dial. A standby to any other
    /// upstream belongs to a plan this one replaces, and is dropped; the new
    /// leg's end gets its own pre-dial.
    async fn serve(&mut self, st: ServeStep, purpose: Purpose, pre: u32) -> Flow {
        let up = st.up;
        self.predial_for = None;
        if self
            .link
            .as_ref()
            .is_some_and(|l| l.st.up == up && self.link_quiescent(l, pre))
        {
            self.dial = None;
            self.discard_standby();
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
    /// leg from the cursor with the outstanding credit. A handoff is never a
    /// failure: no strike, no budget, no backoff, no exclusion. It is counted
    /// when the next leg is served ([`Session::note_handoff`]), which may be
    /// after a wait for the fleet head or a hold.
    async fn handoff(&mut self) -> Flow {
        self.handoff_from = self.link.as_ref().map(|l| l.st.up);
        let step = self.plan(self.c.cursor, false);
        self.apply_step(step, Purpose::Handoff, self.c.credit).await
    }

    /// The leg after a handoff is served from `st.up`. From another upstream
    /// than the one whose leg ended, that is a handoff, counted and logged
    /// once. On the same upstream the stream goes on with the same request
    /// version and flags, on the same socket when it is still open (a frozen
    /// link at its end takes a new request): that is no handoff, and it is
    /// neither counted nor logged as one.
    fn note_handoff(&mut self, st: &ServeStep) {
        let Some(from) = self.handoff_from.take() else {
            return;
        };
        if from == st.up {
            tracing::debug!(
                upstream = %st.endpoint,
                block = self.c.cursor,
                end = st.req_end,
                "the next leg stays on the same upstream"
            );
            return;
        }
        self.ctx.hub.stats.handoffs.fetch_add(1, Ordering::Relaxed);
        self.counts.handoffs += 1;
        self.handoff_started = Some(Instant::now());
        let name = |up: usize| self.view.up(up).map_or("", |u| &*u.name);
        tracing::info!(
            from = %name(from),
            to = %name(st.up),
            block = self.c.cursor,
            "handing the stream off to the next upstream"
        );
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
                let disabled = self.disabled_while_dialing(&target);
                // The view just taken may have come with the dial's end,
                // unseen by the loop: the leg is checked as the Dialing arm
                // checks a view, and its request end follows it.
                let step = match step {
                    Some(cur) if !disabled && self.c.in_progress() => match self.dialed_leg(&cur) {
                        Some(st) => Some(st),
                        None => {
                            tracing::info!(
                                upstream = %target.endpoint,
                                block = self.c.cursor,
                                "upstream can no longer serve the leg it was dialed for; planning again"
                            );
                            drop(est);
                            let next = self.plan(self.c.cursor, self.hold.is_some());
                            return self.apply_step(next, purpose, self.c.credit).await;
                        }
                    },
                    step => step,
                };
                if disabled && self.c.in_progress() {
                    let step = self.plan(self.c.cursor, self.hold.is_some());
                    if matches!(step, Step::Serve(_)) {
                        tracing::info!(
                            upstream = %target.endpoint,
                            "upstream disabled while it was being dialed; serving from another"
                        );
                        drop(est);
                        return self.apply_step(step, purpose, self.c.credit).await;
                    }
                    // Nothing else can serve the leg: it stays, as on any
                    // upstream an operator disables.
                }
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
                    tracing::info!(
                        upstream = %target.endpoint,
                        name = %target.name,
                        "reconnected to upstream after failover"
                    );
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
    ///
    /// A probe ends the link after [`OFFLINE_PROBE`] without a byte from the
    /// upstream: bytes still arriving (a large message on its way, which the
    /// pong cannot overtake) keep it going, as they do for the timeout.
    fn liveness_deadline(&self, link: &Link) -> Option<Instant> {
        let limit = self.liveness_timeout()?;
        let last_read = self.epoch + Duration::from_millis(link.read_ms());
        let at = match link.st.ping_at {
            None => last_read + limit / 2,
            Some(ping) => (last_read + limit).max(ping + limit / 2),
        };
        let probe = link
            .st
            .probe_until
            .map(|probe| probe.max(last_read + OFFLINE_PROBE));
        Some(probe.map_or(at, |probe| probe.min(at)))
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

    /// Whether a stall failover off `link` (`Exclusion::Soft`) would move the
    /// session to another online upstream that covers the cursor. A live
    /// upstream with nowhere else to go is kept for a while (see
    /// [`SOLE_STALL_FACTOR`]): dropping it at once would only redial the same
    /// node and abort a block it may still be preparing.
    ///
    /// It runs a planner pass, so it is asked only once the stall is due.
    fn stall_can_move(&self, link: &Link) -> bool {
        if link.st.role == Role::Frozen {
            return true;
        }
        let Some(creq) = self.c.req.as_ref() else {
            return false;
        };
        let k = self.c.cursor;
        let now = std_now();
        let mut ex = self.ex.clone();
        ex.soft = Some(link.endpoint.clone());
        if self.ctx.stitched() {
            let own = self.standby.as_ref().map(|s| s.link.st.up);
            let tip = self.ctx.hub.fresh_tip(&self.view);
            let step = next_step(
                &self.view,
                tip,
                &Shape::of(creq),
                k,
                &ex,
                own.as_slice(),
                self.id,
                &self.ctx.stitching,
                self.ctx.live_lag_blocks,
                false,
                now,
            );
            return matches!(step, Step::Serve(st) if st.up != link.st.up);
        }
        let need = Need::of(creq);
        // No routing happens here, so a pick that does not cover the block
        // is not warned about.
        pool_rank(
            &self.view,
            Some((need, k)),
            &ex,
            false,
            self.id,
            self.ctx.live_lag_blocks,
            now,
        )
        .is_some_and(|(up, _)| {
            up != link.st.up
                && self
                    .view
                    .up(up)
                    .is_some_and(|u| eligible(&self.view, u, need, k, Union::Online, now))
        })
    }

    /// When each silence limit of an owed link is due, in the order they are
    /// checked: idle, stall, monitor offline. A kept stall is due again at its
    /// recheck.
    fn silence_deadlines(&self, link: &Link) -> [Option<(Instant, UpEnd)>; 3] {
        let from = self.silence_from(link);
        let after = |limit: Duration| from.checked_add(limit);
        let offline = self
            .view
            .up(link.st.up)
            .is_some_and(|u| u.monitor_offline_since.is_some());
        let stall = self.stall_timeout().and_then(after).map(|at| {
            let at = link.st.stall_recheck.map_or(at, |recheck| recheck.max(at));
            (at, UpEnd::Stall)
        });
        [
            self.idle_timeout()
                .and_then(after)
                .map(|at| (at, UpEnd::Idle)),
            stall,
            offline
                .then(|| after(MONITOR_OFFLINE_SILENCE))
                .flatten()
                .map(|at| (at, UpEnd::MonitorOffline)),
        ]
    }

    /// The silence limit that ends the owed link now, if any.
    ///
    /// A stall on a live upstream ends the link only when another upstream
    /// can take the stream ([`Session::stall_can_move`]) or once the silence
    /// reached [`SOLE_STALL_FACTOR`] times the stall timeout. Until then it is
    /// kept, logged once, and asked again at the next view or after
    /// [`STALL_RECHECK`].
    fn silence_due(&mut self, now: Instant) -> Option<(UpEnd, Arc<str>)> {
        let link = self.link.as_ref().filter(|l| l.st.was_owed)?;
        let from = self.silence_from(link);
        let silence = now.saturating_duration_since(from);
        let stall = self.stall_timeout().unwrap_or(Duration::MAX);
        let sole_limit = stall.saturating_mul(SOLE_STALL_FACTOR);
        let mut kept = false;
        let mut due = None;
        for (at, end) in self.silence_deadlines(link).into_iter().flatten() {
            if now < at {
                continue;
            }
            if end == UpEnd::Stall && silence < sole_limit && !self.stall_can_move(link) {
                kept = true;
                continue;
            }
            due = Some((end, link.endpoint.clone()));
            break;
        }
        if kept && due.is_none() {
            let block = self.c.cursor;
            let link = self.link.as_mut()?;
            let redial = from.checked_add(sole_limit);
            let recheck = now + STALL_RECHECK;
            link.st.stall_recheck = Some(redial.map_or(recheck, |at| at.min(recheck)));
            if link.st.stall_kept != Some(from) {
                link.st.stall_kept = Some(from);
                tracing::warn!(
                    upstream = %link.endpoint,
                    block,
                    silent_ms = u64::try_from(silence.as_millis()).unwrap_or(u64::MAX),
                    redial_after_ms = u64::try_from(sole_limit.as_millis()).unwrap_or(u64::MAX),
                    "upstream stalled while blocks were owed, and no other upstream can serve the next block; keeping it"
                );
            }
        }
        due
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
            self.silence_deadlines(l)
                .into_iter()
                .flatten()
                .map(|(at, _)| at)
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
                // The retries back off to 3.2 s, and a freed slot or an
                // expired exclusion publishes no view: one last try before
                // closing the client. A retry's dial still on its way is that
                // try while its upstream can serve the cursor, whatever ranks
                // higher by now (the dial itself tells); it is planned again
                // only when that upstream can no longer serve it.
                let holding = match self.phase {
                    Phase::Holding { purpose, .. } | Phase::Dialing { purpose, .. } => {
                        Some(purpose)
                    }
                    _ => None,
                };
                if let (Some(purpose), false) = (holding, hold.last_try) {
                    let dialed = match &self.phase {
                        Phase::Dialing {
                            step: Some(cur), ..
                        } => self.dialed_leg(cur),
                        _ => None,
                    };
                    if let Some(st) = dialed {
                        if let Some(h) = self.hold.as_mut() {
                            h.last_try = true;
                            h.deadline = None;
                        }
                        // A freed slot publishes no view, so the Dialing
                        // arm may not have seen an online upstream that can
                        // serve instead of an offline target: check once.
                        if let Some(step) = self.online_instead_of(&st) {
                            return self.apply_step(step, purpose, self.c.credit).await;
                        }
                        self.set_phase(Phase::Dialing {
                            purpose,
                            step: Some(st),
                        });
                        return Flow::Continue;
                    }
                    let step = self.plan(self.c.cursor, true);
                    if matches!(step, Step::Serve(_)) {
                        if let Some(h) = self.hold.as_mut() {
                            h.last_try = true;
                            h.deadline = None;
                        }
                        return self.apply_step(step, purpose, self.c.credit).await;
                    }
                }
                return self.hold_expired(hold).await;
            }
        }
        if let Some((end, endpoint)) = self.silence_due(now) {
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
    use std::sync::atomic::AtomicUsize;
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
        start_stitched_tuned(archives, limits, StitchingPolicy::default()).await
    }

    /// [`start_stitched_with`] under the stitching policy `stitching`.
    async fn start_stitched_tuned(
        archives: &[(&str, u32, u32)],
        limits: ProxyLimits,
        stitching: StitchingPolicy,
    ) -> Harness {
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
            stitching,
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

    /// A live upstream that owes blocks and emits no data bytes past
    /// `stall_timeout_ms`, while still answering liveness pings, is not
    /// dropped while no other upstream can take the session; once an
    /// eligible peer comes online, the session fails over immediately.
    #[tokio::test]
    async fn a_sole_live_upstream_that_owes_blocks_is_not_stalled_until_an_alternative_can_serve() {
        let (logs, _guard) = crate::test_log::capture();
        let (stuck, stuck_mock) = start_mock(MockShipConfig {
            hold_at_block: Some(103),
            ..MockShipConfig::live(1, 1000)
        })
        .await;
        let (peer, _peer_mock) = start_mock(MockShipConfig::live(1, 1000)).await;
        let stall = ProxyLimits {
            stall_timeout_ms: 600,
            upstream_liveness_timeout_ms: 200,
            ..limits(0)
        };
        let harness = start_session_with(&[&stuck, &peer], &[&stuck], stall).await;
        set_tip(&harness.hub, &stuck, 1000, 990).await;
        let mut client = connect(&harness.url).await;

        client.send(blocks_request(100, 106, 100)).await.unwrap();
        assert_eq!(read_block_nums(&mut client, 3).await, vec![100, 101, 102]);
        // Wait well past both the liveness timeout (200 ms) and the stall
        // timeout (600 ms) while `peer` is still offline, and well before
        // three stall timeouts (1.8 s).
        tokio::time::sleep(Duration::from_millis(1000)).await;
        assert_eq!(
            logs.lines_with("no other upstream can serve the next block; keeping it")
                .len(),
            1,
            "the kept stall is logged once"
        );
        assert_eq!(
            stuck_mock.sessions_accepted(),
            1,
            "the sole live upstream must not be reconnected in a loop"
        );
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Stall), 0);

        // Bringing `peer` online lets the already-elapsed stall fail over.
        set_tip(&harness.hub, &peer, 1000, 990).await;
        set_online(&harness.hub, &peer).await;
        assert_eq!(read_block_nums(&mut client, 3).await, vec![103, 104, 105]);
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Stall), 1);
    }

    /// Pool: a session streams from a full-history node at its
    /// `max_sessions` (its own link fills it) whose only peer is pruned above
    /// the cursor. The stall check, which asks whether another upstream could
    /// take the stream, runs only once a stall is due and never logs the
    /// routing warning for blocks no peer covers.
    #[tokio::test]
    async fn streaming_from_a_full_node_whose_peer_is_pruned_logs_no_coverage_warning() {
        let (logs, _guard) = crate::test_log::capture();
        let (full, _full_mock) = start_mock(MockShipConfig::live(1, 1000)).await;
        let (pruned, _pruned_mock) = start_mock(MockShipConfig::live(500, 1000)).await;
        let servers: Vec<Server> = [(&full, Some(1)), (&pruned, None)]
            .iter()
            .enumerate()
            .map(|(i, (endpoint, max))| Server {
                name: format!("upstream-{i}"),
                endpoint: endpoint.to_string(),
                enabled: true,
                max_sessions: *max,
                ..Default::default()
            })
            .collect();
        let resolved = listener(servers.clone());
        let hub = FleetHub::new(
            &resolved,
            build_state_db(servers.clone()),
            build_config_db(servers),
        );
        for endpoint in [&full, &pruned] {
            set_online(&hub, endpoint).await;
            set_tip(&hub, endpoint, 1000, 990).await;
        }
        {
            let mut db = hub.state.lock().await;
            let st = db.get_mut(&*pruned).unwrap();
            st.trace_begin_block = 500;
            st.chain_state_begin_block = 500;
            hub.rebuild(&db);
        }
        let stall = ProxyLimits {
            stall_timeout_ms: 60_000,
            ..limits(0)
        };
        let harness = serve_one(&resolved, hub, stall).await;
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        client.send(blocks_request(100, 400, 400)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 300).await,
            (100..400).collect::<Vec<_>>()
        );
        assert_eq!(info.upstream(), full);
        assert_eq!(
            logs.lines_with("no upstream covers the requested block")
                .len(),
            0
        );
    }

    /// The acks a mock received, in order.
    fn acks(mock: &mock_ship::MockShipHandle) -> Vec<u32> {
        mock.requests()
            .into_iter()
            .filter_map(|o| match o {
                mock_ship::Observed::Ack { n, .. } => Some(n),
                _ => None,
            })
            .collect()
    }

    /// Pool: a client ack that takes its window past `u32::MAX`. The
    /// session's credit saturates and the upstream is acked only what it rose
    /// by, so the upstream's own (wrapping) counter never wraps and stays
    /// equal to it: the upstream streams on when its head moves, and the
    /// session never takes it for a stalled one and moves to the peer.
    #[tokio::test]
    async fn an_ack_past_u32_max_keeps_the_upstream_credit_in_lockstep() {
        let (a, a_mock) = start_mock(MockShipConfig::live(1, 1000)).await;
        let (b, b_mock) = start_mock(MockShipConfig::live(1, 1000)).await;
        let stall = ProxyLimits {
            stall_timeout_ms: 400,
            ..limits(0)
        };
        let harness = start_session_with(&[&a, &b], &[&a], stall).await;
        set_tip(&harness.hub, &a, 1000, 990).await;
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(999, 2000, 3)).await.unwrap();
        assert_eq!(read_block_nums(&mut client, 2).await, vec![999, 1000]);
        // One credit left; the ack takes C + n past u32::MAX.
        client
            .send(Message::binary(mock_ship::client::ack(u32::MAX)))
            .await
            .unwrap();
        timeout(WAIT, async {
            while acks(&a_mock).is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the ack reaches the upstream");
        assert_eq!(acks(&a_mock), vec![u32::MAX - 1], "what C rose by");
        a_mock.set_head(1010, 1000);
        b_mock.set_head(1010, 1000);
        set_tip(&harness.hub, &a, 1010, 1000).await;
        set_tip(&harness.hub, &b, 1010, 1000).await;
        set_online(&harness.hub, &b).await;
        assert_eq!(
            read_block_nums(&mut client, 3).await,
            vec![1001, 1002, 1003]
        );
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!(blocks_requests(&b_mock).is_empty(), "no move to the peer");
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Stall), 0);
    }

    /// Stitched: an ack past `u32::MAX` while the archive still holds part of
    /// the window never makes the archive look stalled: it is acked what the
    /// session's credit rose by, its own counter does not wrap, and it
    /// streams on. No strike, and every block arrives.
    #[tokio::test]
    async fn an_ack_past_u32_max_never_strikes_an_archive() {
        for pre_read in [1, 2] {
            let (arch, _arch_mock) = start_mock(MockShipConfig {
                block_delay: Some(Duration::from_millis(50)),
                ..MockShipConfig::frozen(1, 2001)
            })
            .await;
            let stall = ProxyLimits {
                stall_timeout_ms: 400,
                ..limits(0)
            };
            let harness = start_stitched_with(&[(&arch, 1, 2001)], stall).await;
            let mut client = connect(&harness.url).await;
            client.send(blocks_request(10, 22, 3)).await.unwrap();
            let mut got = read_block_nums(&mut client, pre_read).await;
            client
                .send(Message::binary(mock_ship::client::ack(u32::MAX)))
                .await
                .unwrap();
            got.extend(read_block_nums(&mut client, 12 - pre_read).await);
            assert_eq!(got, (10..22).collect::<Vec<_>>(), "pre_read {pre_read}");
            let failures = harness
                .hub
                .state
                .lock()
                .await
                .get(&arch)
                .unwrap()
                .block_failures;
            assert_eq!(failures, 0, "pre_read {pre_read}");
            for reason in [FailoverReason::Stall, FailoverReason::Violation] {
                assert_eq!(harness.hub.stats.failovers.get(reason), 0, "{reason:?}");
            }
        }
    }

    /// Pool: the client's blocks came from `a`, which is lost; `b` and `c`
    /// both follow another chain from block 103. The first of them at the
    /// seam fails over (logged as a discontinuity) while the other could
    /// serve; the other has no alternative left and forwards the
    /// discontinuity. The first one is then usable again: when the second
    /// drops, the session resumes on it instead of an offline upstream.
    #[tokio::test]
    async fn a_discontinuity_no_upstream_continues_releases_the_upstreams_left_for_it() {
        let (logs, _guard) = crate::test_log::capture();
        let (a, a_mock) = start_mock(MockShipConfig {
            hold_at_block: Some(105),
            ..MockShipConfig::live(1, 1000)
        })
        .await;
        let forked = || MockShipConfig {
            fork: Some((103, 7)),
            ..MockShipConfig::live(1, 1000)
        };
        let (b, b_mock) = start_mock(forked()).await;
        let (c, c_mock) = start_mock(forked()).await;
        let harness = start_session_with(&[&a, &b, &c], &[&a], limits(0)).await;
        set_tip(&harness.hub, &a, 1000, 990).await;
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        // One credit is left once `a` has sent 100..=104.
        client.send(blocks_request(100, 200, 6)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 5).await,
            (100..105).collect::<Vec<_>>()
        );
        for endpoint in [&b, &c] {
            set_tip(&harness.hub, endpoint, 1000, 990).await;
            set_online(&harness.hub, endpoint).await;
        }
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&a).unwrap().online = false;
            harness.hub.rebuild(&db);
        }
        a_mock.drop_sessions();
        assert_eq!(read_block_nums(&mut client, 1).await, vec![105]);
        assert_eq!(
            harness
                .hub
                .stats
                .failovers
                .get(FailoverReason::Discontinuity),
            1
        );
        assert_eq!(
            logs.lines_with("does not continue the last delivered block; failing over")
                .len(),
            1
        );
        let on = info.upstream();
        let (on_mock, other) = if on == b {
            (&b_mock, &c)
        } else {
            (&c_mock, &b)
        };
        on_mock.drop_sessions();
        client
            .send(Message::binary(mock_ship::client::ack(1)))
            .await
            .unwrap();
        assert_eq!(read_block_nums(&mut client, 1).await, vec![106]);
        assert_eq!(&info.upstream(), other, "resumed where the chain goes on");
    }

    /// A sole live upstream that answers pings but stays stalled is not kept
    /// forever: after three stall timeouts of silence it is failed over,
    /// which redials it, and the stall is counted.
    #[tokio::test]
    async fn a_sole_live_upstream_that_stays_stalled_is_redialed_after_three_stall_timeouts() {
        let (stuck, stuck_mock) = start_mock(MockShipConfig {
            hold_at_block: Some(103),
            ..MockShipConfig::live(1, 1000)
        })
        .await;
        let stall = ProxyLimits {
            stall_timeout_ms: 400,
            upstream_liveness_timeout_ms: 200,
            ..limits(0)
        };
        let harness = start_session_with(&[&stuck], &[&stuck], stall).await;
        set_tip(&harness.hub, &stuck, 1000, 990).await;
        let mut client = connect(&harness.url).await;

        client.send(blocks_request(100, 106, 100)).await.unwrap();
        assert_eq!(read_block_nums(&mut client, 3).await, vec![100, 101, 102]);
        let silent_since = Instant::now();
        timeout(WAIT, async {
            while stuck_mock.sessions_accepted() < 2 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the stalled sole upstream is redialed");
        assert!(
            silent_since.elapsed() >= Duration::from_millis(1000),
            "kept for three stall timeouts, not one: {:?}",
            silent_since.elapsed()
        );
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Stall), 1);
        let up = harness.hub.stats.upstream(0).unwrap();
        assert_eq!(up.stalls.load(Ordering::Relaxed), 1);
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

    /// A TCP proxy to `target` that passes the upstream's bytes on in
    /// `chunk`-sized pieces with `pause` between them (requests pass
    /// freely): a slow link on which a large message takes seconds.
    async fn throttled(target: String, chunk: usize, pause: Duration) -> (String, JoinHandle<()>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = tcp.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            while let Ok((inbound, _)) = tcp.accept().await {
                let Ok(outbound) = TcpStream::connect(&target).await else {
                    continue;
                };
                let (mut in_r, mut in_w) = inbound.into_split();
                let (mut out_r, mut out_w) = outbound.into_split();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 64 * 1024];
                    while let Ok(n @ 1..) = in_r.read(&mut buf).await {
                        if out_w.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                });
                tokio::spawn(async move {
                    let mut buf = vec![0u8; chunk];
                    while let Ok(n @ 1..) = out_r.read(&mut buf).await {
                        if in_w.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                        tokio::time::sleep(pause).await;
                    }
                });
            }
        });
        (endpoint, task)
    }

    /// The monitor loses an upstream while a 3 MB block is on its way over a
    /// slow link (about 2 s). Bytes keep arriving, so the 1 s probe does not
    /// fail the link over as silent, and the block arrives whole.
    #[tokio::test]
    async fn a_probe_spares_a_link_whose_large_block_is_still_arriving() {
        let (up, _mock) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            block_data_size: 3_000_000,
            ..Default::default()
        })
        .await;
        let (slow, proxy) = throttled(up, 16 * 1024, Duration::from_millis(10)).await;
        let (healthy, healthy_mock) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            ..Default::default()
        })
        .await;
        let limits = ProxyLimits {
            upstream_liveness_timeout_ms: 20_000,
            max_message_bytes: 64 << 20,
            ..limits(0)
        };
        let harness = start_session_with(&[&slow, &healthy], &[&slow], limits).await;
        let mut client = connect(&harness.url).await;
        set_online(&harness.hub, &healthy).await;
        let request = mock_ship::client::BlocksReq {
            start: 100,
            end: 101,
            max_in_flight: 1,
            fetch_block: true,
            ..Default::default()
        };
        client
            .send(Message::binary(mock_ship::client::blocks(&request)))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&slow)
                .unwrap()
                .mark_offline(std::time::Instant::now());
            harness.hub.rebuild(&db);
        }
        let frame = match timeout(Duration::from_secs(10), client.next()).await {
            Ok(Some(Ok(Message::Binary(frame)))) => frame,
            other => panic!("expected the block, got {other:?}"),
        };
        assert!(frame.len() > 3_000_000, "{} bytes", frame.len());
        assert_eq!(BlockView::parse(&frame).and_then(|v| v.num()), Some(100));
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Silent), 0);
        assert_eq!(healthy_mock.sessions_accepted(), 0);
        proxy.abort();
    }

    /// The view that shows the link's upstream offline arrives without the
    /// monitor's wake (it was missed), and a client request takes it first.
    /// The loop still probes the link: the session fails over after about a
    /// second, not after its liveness timeout.
    #[tokio::test]
    async fn a_view_taken_by_a_request_still_probes_a_lost_upstream() {
        let (dead, dead_task) = dead_upstream().await;
        let (healthy, _healthy_mock) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            ..Default::default()
        })
        .await;
        let harness = start_session_with(&[&dead, &healthy], &[&dead], liveness(6_000)).await;
        let mut client = connect(&harness.url).await;
        set_online(&harness.hub, &healthy).await;
        tokio::time::sleep(Duration::from_millis(200)).await;

        let lost = Instant::now();
        {
            let mut db = harness.hub.state.lock().await;
            let state = db.get_mut(&dead).unwrap();
            state.online = false;
            state.offline_since = Some(std::time::Instant::now());
            harness.hub.rebuild(&db);
        }
        client.send(blocks_request(100, 103, 10)).await.unwrap();
        assert_eq!(read_block_nums(&mut client, 3).await, vec![100, 101, 102]);
        let waited = lost.elapsed();
        assert!(
            waited < Duration::from_secs(3),
            "failed over after {waited:?}"
        );
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Silent), 1);
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

    /// Stitched: a session whose leg on A ended and that holds for a full B
    /// has no link. The registry shows no upstream for it, so a drain of A
    /// does not count it as moving and a move of it is refused, and its next
    /// block is the one it waits for.
    #[tokio::test]
    async fn the_registry_shows_no_upstream_while_a_session_holds_without_a_link() {
        let leg = |begin, end| MockShipConfig {
            block_delay: Some(Duration::from_millis(5)),
            ..MockShipConfig::frozen(begin, end)
        };
        let (a, _mock_a) = start_mock(leg(1, 1001)).await;
        let (b, _mock_b) = start_mock(leg(1001, 2001)).await;
        let harness = start_stitched(&[(&a, 1, 1001), (&b, 1001, 2001)]).await;
        {
            let mut db = harness.hub.state.lock().await;
            let state = db.get_mut(&b).unwrap();
            state.max_sessions = Some(1);
            state.connections.fetch_add(1, Ordering::Relaxed);
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        client.send(blocks_request(990, 1100, 1000)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 11).await,
            (990..1001).collect::<Vec<_>>()
        );
        let held = timeout(WAIT, async {
            while harness.hub.stats.holds.get(HoldWhy::AtCapacity) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(held.is_ok(), "the session should hold for B");
        assert_eq!(info.upstream(), "", "the link to A is gone");
        assert_eq!(info.next_block(), Some(1001));
    }

    /// Pool: a request for blocks far above a lagging upstream's head is
    /// rerouted to one that has them, and the reroute is counted and reported
    /// like a failover: on the old upstream's `failovers_total`, the
    /// session's failovers, and a `session_failover` event.
    #[tokio::test]
    async fn a_reroute_is_counted_and_reported_like_a_failover() {
        let (a, _mock_a) = start_mock(MockShipConfig::live(1, 999)).await;
        let (b, mock_b) = start_mock(MockShipConfig::live(1, 2999)).await;
        let harness = start_session_with(&[&a, &b], &[&a], limits(0)).await;
        set_tip(&harness.hub, &a, 999, 989).await;
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        assert_eq!(info.upstream(), a);
        set_online(&harness.hub, &b).await;
        set_tip(&harness.hub, &b, 2999, 2989).await;

        client.send(blocks_request(1500, 1510, 100)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 10).await,
            (1500..1510).collect::<Vec<_>>()
        );
        assert_eq!(info.upstream(), b);
        assert_eq!(mock_b.sessions_accepted(), 1);
        assert_eq!(harness.hub.stats.reroutes.load(Ordering::Relaxed), 1);
        assert_eq!(info.failovers(), 1);
        let view = harness.hub.view();
        let failovers_total = |endpoint: &str| {
            view.upstreams
                .iter()
                .find(|u| &*u.endpoint == endpoint)
                .map(|u| u.counters.failovers_total.load(Ordering::Relaxed))
        };
        assert_eq!(failovers_total(&a), Some(1));
        assert_eq!(failovers_total(&b), Some(0));
        let events = telemetry::global()
            .events_after(0)
            .into_iter()
            .filter(|e| e.session == Some(info.id) && e.kind == EventKind::SessionFailover)
            .map(|e| (e.level, e.message))
            .collect::<Vec<_>>();
        assert_eq!(
            events,
            [(Level::Info, format!("moved from {a} to {b} (reroute)"))]
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

    /// Stitched: a new client request served on the current socket drops the
    /// standby link pre-dialed for the old request's next leg, which would
    /// otherwise hold a slot on that upstream until its linger runs out.
    #[tokio::test]
    async fn a_request_served_on_the_same_socket_drops_the_old_standby() {
        let (a, _mock_a) = start_mock(MockShipConfig::frozen(1, 201)).await;
        let (b, mock_b) = start_mock(MockShipConfig::frozen(201, 301)).await;
        let harness = start_stitched(&[(&a, 1, 201), (&b, 201, 301)]).await;
        let mut client = connect(&harness.url).await;
        // Five blocks of credit: the link is quiet once they are sent.
        client.send(blocks_request(10, 300, 5)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 5).await,
            vec![10, 11, 12, 13, 14]
        );
        let b_slots = || {
            harness
                .hub
                .view()
                .upstreams
                .iter()
                .find(|u| *u.endpoint == *b)
                .map(|u| u.connections.load(Ordering::Relaxed))
        };
        let standby = timeout(WAIT, async {
            while mock_b.sessions_accepted() == 0 || b_slots() != Some(1) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(standby.is_ok(), "B is pre-dialed");
        tokio::time::sleep(Duration::from_millis(100)).await;

        client.send(blocks_request(100, 150, 100)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 50).await,
            (100..150).collect::<Vec<_>>()
        );
        assert_eq!(harness.hub.stats.predials.get(PredialResult::Discarded), 1);
        assert_eq!(b_slots(), Some(0), "the standby's slot on B is free");
    }

    /// Stitched: an operator move inside the pre-dial window drops the
    /// standby to the next leg's upstream with the old link. The replica the
    /// session moves to pre-dials it again, so the handoff stays warm.
    #[tokio::test]
    async fn a_move_inside_the_predial_window_predials_the_next_leg_again() {
        let leg = |begin, end| MockShipConfig {
            block_delay: Some(Duration::from_millis(5)),
            ..MockShipConfig::frozen(begin, end)
        };
        let (a, _mock_a) = start_mock(leg(1, 101)).await;
        let (twin, _mock_twin) = start_mock(leg(1, 101)).await;
        let (b, mock_b) = start_mock(leg(101, 201)).await;
        let harness = start_stitched(&[(&a, 1, 101), (&twin, 1, 101), (&b, 101, 201)]).await;
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        client.send(blocks_request(10, 150, 1000)).await.unwrap();
        let mut blocks = read_block_nums(&mut client, 10).await;
        let first = info.upstream();
        let predialed = timeout(WAIT, async {
            while mock_b.sessions_accepted() == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(predialed.is_ok(), "B is pre-dialed");

        info.request_move();
        let rest = 150 - 10 - blocks.len();
        blocks.extend(read_block_nums(&mut client, rest).await);
        assert_eq!(blocks, (10..150).collect::<Vec<_>>());
        assert_ne!(info.upstream(), first);
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Operator), 1);
        assert_eq!(harness.hub.stats.predials.get(PredialResult::Used), 1);
        assert_eq!(
            mock_b.sessions_accepted(),
            2,
            "pre-dialed again after the move"
        );
    }

    /// Stitched: a session holds for a full archive, and a slot there frees
    /// up after the last retry before `gap_wait_ms` runs out (the retries
    /// back off, and a freed slot publishes no view). The hold makes one
    /// last try at its deadline instead of closing the client.
    #[tokio::test]
    async fn a_bounded_hold_tries_once_more_at_its_deadline() {
        let leg = |begin, end| MockShipConfig {
            block_delay: Some(Duration::from_millis(1)),
            ..MockShipConfig::frozen(begin, end)
        };
        let (a, _mock_a) = start_mock(leg(1, 1001)).await;
        let (b, mock_b) = start_mock(leg(1001, 2001)).await;
        // Retries come about 0.1, 0.3, 0.7 and 1.5 s into the hold (±20 %),
        // the next one after 2.48 s at the earliest.
        let policy = StitchingPolicy {
            gap_wait_ms: 2_400,
            ..StitchingPolicy::default()
        };
        let harness =
            start_stitched_tuned(&[(&a, 1, 1001), (&b, 1001, 2001)], limits(0), policy).await;
        let b_slots = {
            let mut db = harness.hub.state.lock().await;
            let state = db.get_mut(&b).unwrap();
            state.max_sessions = Some(1);
            state.connections.fetch_add(1, Ordering::Relaxed);
            let slots = state.connections.clone();
            harness.hub.rebuild(&db);
            slots
        };
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(990, 1010, 100)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 11).await,
            (990..1001).collect::<Vec<_>>()
        );
        let held = timeout(WAIT, async {
            while harness.hub.stats.holds.get(HoldWhy::AtCapacity) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(held.is_ok(), "the session should hold for B");
        let since = Instant::now();

        // Between the last retry (1.8 s at the latest) and the deadline.
        tokio::time::sleep_until(since + Duration::from_millis(2_100)).await;
        b_slots.fetch_sub(1, Ordering::Relaxed);
        assert_eq!(
            read_block_nums(&mut client, 9).await,
            (1001..1010).collect::<Vec<_>>()
        );
        assert_eq!(mock_b.sessions_accepted(), 1);
        assert_eq!(harness.hub.stats.hold_closes.get(HoldWhy::AtCapacity), 0);
    }

    /// Stitched: an archive replica that goes silent after its ABI is
    /// failed over to its twin as silent, without a strike, and is not marked
    /// offline: a frozen upstream is never taken out of the fleet for that.
    #[tokio::test]
    async fn a_silent_archive_fails_over_to_its_twin_and_stays_online() {
        let (dead, dead_task) = dead_upstream().await;
        let (twin, _twin_mock) = start_mock(MockShipConfig::frozen(1, 2001)).await;
        let harness =
            start_stitched_with(&[(&dead, 1, 2001), (&twin, 1, 2001)], liveness(400)).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&twin).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(10, 20, 100)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 10).await,
            (10..20).collect::<Vec<_>>()
        );
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Silent), 1);
        let state = harness.hub.state.lock().await;
        assert!(
            state[&dead].online,
            "a silent archive is not marked offline"
        );
        assert_eq!(state[&dead].block_failures, 0, "no strike");
        drop(state);
        dead_task.abort();
    }

    fn block_request(start: u32, end: u32, max_in_flight: u32) -> Message {
        Message::binary(mock_ship::client::blocks(&mock_ship::client::BlocksReq {
            start,
            end,
            max_in_flight,
            fetch_block: true,
            fetch_traces: true,
            ..Default::default()
        }))
    }

    async fn read_views(client: &mut TestClient, n: usize) -> Vec<BlockView> {
        let mut views = Vec::new();
        while views.len() < n {
            match next_message(client).await {
                Some(Message::Binary(data)) => {
                    views.push(BlockView::parse(&data).expect("a block result"));
                }
                other => panic!("unexpected frame after {} results: {other:?}", views.len()),
            }
        }
        views
    }

    fn violations(hub: &FleetHub, up: usize) -> u64 {
        let stats = hub.stats.upstream(up).unwrap();
        stats.contract_violations.values().map(|(_, n)| n).sum()
    }

    /// Stitched: an archive without a block log that sends a result without
    /// the requested block is no contract violation. It is flagged, and the
    /// session moves to its twin that holds blocks, resuming at that block.
    #[tokio::test]
    async fn a_block_request_moves_off_an_archive_without_a_block_log() {
        let (bare, _bare_mock) = start_mock(MockShipConfig {
            no_block_log: true,
            ..MockShipConfig::frozen(1, 1001)
        })
        .await;
        let (full, full_mock) = start_mock(MockShipConfig::frozen(1, 1001)).await;
        let harness = start_stitched(&[(&bare, 1, 1001), (&full, 1, 1001)]).await;
        {
            // The archive without a block log is tried first.
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&full).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        client.send(block_request(10, 20, 100)).await.unwrap();
        let views = read_views(&mut client, 10).await;
        assert_eq!(
            views.iter().filter_map(BlockView::num).collect::<Vec<_>>(),
            (10..20).collect::<Vec<_>>()
        );
        assert!(views.iter().all(|v| v.block.is_some()), "every block");
        assert_eq!(blocks_requests(&full_mock)[0].1, 10, "resumed at 10");
        let stats = &harness.hub.stats;
        assert_eq!(stats.failovers.get(FailoverReason::NoBlockLog), 1);
        assert_eq!(stats.failovers.get(FailoverReason::Violation), 0);
        assert_eq!(violations(&harness.hub, 0), 0, "no violation");
        assert_eq!(
            stats
                .upstream(0)
                .unwrap()
                .results_without_block
                .load(Ordering::Relaxed),
            0
        );
        let state = harness.hub.state.lock().await;
        assert!(state[&bare].no_block_log);
        assert!(!state[&full].no_block_log);
        assert_eq!(state[&bare].block_failures, 0, "no strike");
        assert!(state[&bare].quarantines.is_empty());
    }

    /// Stitched: the info line of a move off an archive without a block log
    /// names the archive it leaves and the upstream it moves to.
    #[tokio::test]
    async fn a_move_to_an_upstream_that_holds_blocks_logs_both_upstreams() {
        let (logs, _guard) = crate::test_log::capture();
        let (bare, _bare_mock) = start_mock(MockShipConfig {
            no_block_log: true,
            ..MockShipConfig::frozen(1, 1001)
        })
        .await;
        let (full, _full_mock) = start_mock(MockShipConfig::frozen(1, 1001)).await;
        let harness = start_stitched(&[(&bare, 1, 1001), (&full, 1, 1001)]).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&full).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        client.send(block_request(10, 20, 100)).await.unwrap();
        let views = read_views(&mut client, 10).await;
        assert!(views.iter().all(|v| v.block.is_some()));
        let moves = logs.lines_with("moving to an upstream that holds blocks");
        assert_eq!(moves.len(), 1, "{moves:?}");
        assert!(
            moves[0].contains("from=archive-0") && moves[0].contains("to=archive-1"),
            "{}",
            moves[0]
        );
    }

    /// Stitched: when no upstream holds the blocks, results come without
    /// `block`, as from one node with a pruned block log, and are counted.
    #[tokio::test]
    async fn archives_without_a_block_log_serve_block_requests_without_blocks() {
        let bare = || MockShipConfig {
            no_block_log: true,
            ..MockShipConfig::frozen(1, 1001)
        };
        let (a, _a_mock) = start_mock(bare()).await;
        let (a2, _a2_mock) = start_mock(bare()).await;
        let harness = start_stitched(&[(&a, 1, 1001), (&a2, 1, 1001)]).await;
        let mut client = connect(&harness.url).await;
        client.send(block_request(1, 300, 300)).await.unwrap();
        let views = read_views(&mut client, 299).await;
        assert_eq!(
            views.iter().filter_map(BlockView::num).collect::<Vec<_>>(),
            (1..300).collect::<Vec<_>>()
        );
        assert!(views
            .iter()
            .all(|v| v.block.is_none() && v.traces.is_some()));
        for pair in views.windows(2) {
            assert_eq!(pair[1].prev, pair[0].this, "the chain continues");
        }
        let stats = &harness.hub.stats;
        // The first archive tried moved the session to the other, which
        // then served everything without blocks.
        assert_eq!(stats.failovers.get(FailoverReason::NoBlockLog), 1);
        let without = |up: usize| {
            stats
                .upstream(up)
                .unwrap()
                .results_without_block
                .load(Ordering::Relaxed)
        };
        assert_eq!(without(0) + without(1), 299);
        assert!(without(0) == 0 || without(1) == 0, "one archive served");
        assert_eq!(violations(&harness.hub, 0) + violations(&harness.hub, 1), 0);
        let state = harness.hub.state.lock().await;
        for ep in [&a, &a2] {
            assert!(state[ep].no_block_log);
            assert_eq!(state[ep].block_failures, 0, "no strike");
        }
    }

    /// A status that differs from the last one, or one after the upstream
    /// came back, clears the flag: the upstream may have a block log now.
    #[tokio::test]
    async fn a_reconnect_or_a_new_status_clears_the_no_block_log_flag() {
        let (archive, _mock) = start_mock(MockShipConfig::frozen(1, 1001)).await;
        let harness = start_stitched(&[(&archive, 1, 1001)]).await;
        assert!(link::mark_no_block_log(&harness.hub, &archive).await);
        assert!(
            !link::mark_no_block_log(&harness.hub, &archive).await,
            "once"
        );
        assert!(harness.hub.view().upstreams[0].no_block_log);
        let server = harness.hub.config.lock().await[&archive].clone();
        let status = |end: u32| {
            mock_ship::protocol::encode_status(
                false,
                &mock_ship::protocol::StatusFields {
                    head: (end - 1, mock_ship::fake_block_id(end - 1)),
                    lib: (end - 1, mock_ship::fake_block_id(end - 1)),
                    trace: (1, end),
                    chain_state: (1, end),
                    chain_id: [0; 32],
                    finality: (0, 0),
                },
            )
        };
        // The same status keeps it.
        crate::tasks::apply_status_update(&status(1001), &harness.hub.state, &server)
            .await
            .unwrap();
        assert!(harness.hub.state.lock().await[&archive].no_block_log);
        // Another status clears it.
        crate::tasks::apply_status_update(&status(1002), &harness.hub.state, &server)
            .await
            .unwrap();
        assert!(!harness.hub.state.lock().await[&archive].no_block_log);
        // So does the first status after the upstream was lost.
        assert!(link::mark_no_block_log(&harness.hub, &archive).await);
        harness
            .hub
            .state
            .lock()
            .await
            .get_mut(&archive)
            .unwrap()
            .mark_offline(std::time::Instant::now());
        crate::tasks::apply_status_update(&status(1002), &harness.hub.state, &server)
            .await
            .unwrap();
        assert!(!harness.hub.state.lock().await[&archive].no_block_log);
    }

    /// One session of a stitched listener over archives and live nodes,
    /// served in-process. Each `(endpoint, role, begin, head, stale)` is
    /// online with trace and chain state `[begin, head]` and that head; an
    /// archive's LIB is its head, a live node's is 10 below. A stale upstream
    /// is one the monitor saw stop advancing.
    async fn start_fleet(ups: &[(&str, Role, u32, u32, bool)]) -> Harness {
        let servers: Vec<Server> = ups
            .iter()
            .enumerate()
            .map(|(i, &(endpoint, role, begin, head, _))| Server {
                name: format!("upstream-{i}"),
                endpoint: endpoint.to_string(),
                enabled: true,
                role,
                range: (role == Role::Frozen).then_some(crate::ship::Range {
                    begin,
                    end: head + 1,
                }),
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
            let pos = |num| BlockPos {
                num,
                id: mock_ship::fake_block_id(num),
            };
            for &(endpoint, role, begin, head, stale) in ups {
                let state = db.get_mut(endpoint).unwrap();
                let lib = match role {
                    Role::Frozen => head,
                    Role::Live => head - 10,
                };
                state.online = true;
                state.ranges_known = true;
                state.trace_begin_block = begin;
                state.trace_end_block = head + 1;
                state.chain_state_begin_block = begin;
                state.chain_state_end_block = head + 1;
                state.head = Some(pos(head));
                state.lib = Some(pos(lib));
                state.status_v1 = Some(true);
                state.abi = Some(abi.clone());
                state.stale = stale;
            }
            hub.rebuild(&db);
        }
        serve_one(&resolved, hub, limits(0)).await
    }

    fn results_without_block(hub: &FleetHub, up: usize) -> u64 {
        hub.stats
            .upstream(up)
            .unwrap()
            .results_without_block
            .load(Ordering::Relaxed)
    }

    /// Stitched: the only live node that covers a block request is stale.
    /// The failover that would leave the archive without a block log ranks
    /// that node below the archive and would only come back to it, so the
    /// archive serves the request without blocks rather than moving in a
    /// loop that never forwards anything.
    #[tokio::test]
    async fn a_stale_live_node_does_not_draw_a_block_request_off_an_archive_without_a_block_log() {
        let (archive, archive_mock) = start_mock(MockShipConfig {
            no_block_log: true,
            ..MockShipConfig::frozen(1, 1001)
        })
        .await;
        let (live, live_mock) = start_mock(MockShipConfig::live(900, 1500)).await;
        let harness = start_fleet(&[
            (&archive, Role::Frozen, 1, 1000, false),
            (&live, Role::Live, 900, 1500, true),
        ])
        .await;
        let mut client = connect(&harness.url).await;
        client.send(block_request(950, 960, 100)).await.unwrap();
        let views = read_views(&mut client, 10).await;
        assert_eq!(
            views.iter().filter_map(BlockView::num).collect::<Vec<_>>(),
            (950..960).collect::<Vec<_>>()
        );
        assert!(views.iter().all(|v| v.block.is_none()));
        assert_eq!(
            harness.hub.stats.failovers.get(FailoverReason::NoBlockLog),
            0
        );
        assert_eq!(results_without_block(&harness.hub, 0), 10);
        assert_eq!(archive_mock.sessions_accepted(), 1);
        assert_eq!(live_mock.sessions_accepted(), 0);
    }

    /// Stitched: the live node that covers a block request is still syncing,
    /// far behind the block, and the other one begins above it. Neither is
    /// where the failover would go, so the archive without a block log
    /// serves the request without blocks.
    #[tokio::test]
    async fn a_lagging_live_node_does_not_draw_a_block_request_off_an_archive_without_a_block_log()
    {
        let (archive, archive_mock) = start_mock(MockShipConfig {
            no_block_log: true,
            ..MockShipConfig::frozen(1, 3001)
        })
        .await;
        let (syncing, syncing_mock) = start_mock(MockShipConfig::live(1000, 1100)).await;
        let (tip, tip_mock) = start_mock(MockShipConfig::live(2500, 2600)).await;
        let harness = start_fleet(&[
            (&archive, Role::Frozen, 1, 3000, false),
            (&syncing, Role::Live, 1000, 1100, false),
            (&tip, Role::Live, 2500, 2600, false),
        ])
        .await;
        let mut client = connect(&harness.url).await;
        client.send(block_request(1500, 1510, 100)).await.unwrap();
        let views = read_views(&mut client, 10).await;
        assert_eq!(
            views.iter().filter_map(BlockView::num).collect::<Vec<_>>(),
            (1500..1510).collect::<Vec<_>>()
        );
        assert!(views.iter().all(|v| v.block.is_none()));
        assert_eq!(
            harness.hub.stats.failovers.get(FailoverReason::NoBlockLog),
            0
        );
        assert_eq!(archive_mock.sessions_accepted(), 1);
        assert_eq!(syncing_mock.sessions_accepted(), 0);
        assert_eq!(tip_mock.sessions_accepted(), 0);
    }

    /// Stitched, archive-only: the archive that holds blocks is full when the
    /// leg is planned, so its twin without a block log serves it. A freed
    /// slot publishes no new view; the session still moves to the blocks
    /// within about a second.
    #[tokio::test]
    async fn a_freed_slot_draws_a_block_request_off_an_archive_without_a_block_log() {
        let (full, full_mock) = start_mock(MockShipConfig::frozen(1, 100_001)).await;
        let (bare, _bare_mock) = start_mock(MockShipConfig {
            no_block_log: true,
            block_delay: Some(Duration::from_millis(2)),
            ..MockShipConfig::frozen(1, 100_001)
        })
        .await;
        let harness = start_stitched(&[(&full, 1, 100_001), (&bare, 1, 100_001)]).await;
        assert!(link::mark_no_block_log(&harness.hub, &bare).await);
        let slots = {
            let mut db = harness.hub.state.lock().await;
            let state = db.get_mut(&full).unwrap();
            state.max_sessions = Some(1);
            state.connections.store(1, Ordering::Relaxed);
            let slots = state.connections.clone();
            harness.hub.rebuild(&db);
            slots
        };
        let generation = harness.hub.view().generation;
        let mut client = connect(&harness.url).await;
        client
            .send(block_request(10, 90_000, 100_000))
            .await
            .unwrap();
        let views = read_views(&mut client, 20).await;
        assert!(
            views.iter().all(|v| v.block.is_none()),
            "the holder is full"
        );
        assert_eq!(full_mock.sessions_accepted(), 0);

        slots.store(0, Ordering::Relaxed);
        let freed = Instant::now();
        let mut last = views.last().and_then(BlockView::num).unwrap();
        loop {
            assert!(
                freed.elapsed() < Duration::from_secs(5),
                "still without blocks at {last}"
            );
            let view = read_views(&mut client, 1).await.remove(0);
            let n = view.num().unwrap();
            assert_eq!(n, last + 1, "the stream goes on");
            last = n;
            if view.block.is_some() {
                break;
            }
        }
        let views = read_views(&mut client, 20).await;
        assert!(views.iter().all(|v| v.block.is_some()));
        assert_eq!(harness.hub.view().generation, generation, "no new view");
        assert_eq!(
            harness.hub.stats.failovers.get(FailoverReason::NoBlockLog),
            1
        );
        assert_eq!(blocks_requests(&full_mock)[0].1, last, "resumed there");
    }

    /// Stitched: a leg ends at the fleet head + 1, and by then the head has
    /// moved on, so the next leg is on the same archive. The stream goes on
    /// over the same socket with a new request, and that is no handoff.
    #[tokio::test]
    async fn a_next_leg_on_the_same_upstream_is_no_handoff() {
        let (archive, mock) = start_mock(MockShipConfig::frozen(1, 1001)).await;
        let harness = start_stitched(&[(&archive, 1, 1001)]).await;
        let set_head = |num: u32| {
            let hub = harness.hub.clone();
            let archive = archive.clone();
            async move {
                let mut db = hub.state.lock().await;
                let pos = BlockPos {
                    num,
                    id: mock_ship::fake_block_id(num),
                };
                let state = db.get_mut(&archive).unwrap();
                state.head = Some(pos);
                state.lib = Some(pos);
                hub.rebuild(&db);
            }
        };
        set_head(300).await;
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(1, 1001, 100)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 100).await,
            (1..=100).collect::<Vec<_>>()
        );
        // The client's credit is used up below the leg's end (301).
        set_head(1000).await;
        client
            .send(Message::binary(mock_ship::client::ack(900)))
            .await
            .unwrap();
        assert_eq!(
            read_block_nums(&mut client, 900).await,
            (101..=1000).collect::<Vec<_>>()
        );
        let requests = blocks_requests(&mock);
        assert_eq!(
            requests
                .iter()
                .map(|&(_, start, end, _)| (start, end))
                .collect::<Vec<_>>(),
            [(1, 301), (301, 1001)]
        );
        assert_eq!(requests[0].0, requests[1].0, "the same socket");
        assert_eq!(mock.sessions_accepted(), 1);
        assert_eq!(harness.hub.stats.handoffs.load(Ordering::Relaxed), 0);
    }

    /// Give the frozen `endpoint` the head and LIB `num`, and publish the
    /// view.
    async fn set_frozen_head(hub: &FleetHub, endpoint: &str, num: u32) {
        let mut db = hub.state.lock().await;
        let pos = BlockPos {
            num,
            id: mock_ship::fake_block_id(num),
        };
        let state = db.get_mut(endpoint).unwrap();
        state.head = Some(pos);
        state.lib = Some(pos);
        hub.rebuild(&db);
    }

    /// Wait until the session waits for the fleet to reach a block.
    async fn wait_until_waiting(hub: &FleetHub) {
        let deadline = Instant::now() + WAIT;
        while hub.stats.sessions(SessionGauge::Waiting) != 1 {
            assert!(Instant::now() < deadline, "the session never waited");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Stitched: the client reaches the end of a leg clamped to the fleet
    /// head before the head moves, so the session waits there; then the next
    /// leg is on the same archive. The stream goes on over the same socket,
    /// and that is no handoff, after the wait too.
    #[tokio::test]
    async fn a_next_leg_on_the_same_upstream_after_a_wait_is_no_handoff() {
        let (archive, mock) = start_mock(MockShipConfig::frozen(1, 1001)).await;
        let harness = start_stitched(&[(&archive, 1, 1001)]).await;
        set_frozen_head(&harness.hub, &archive, 300).await;
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(1, 1001, 1000)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 300).await,
            (1..=300).collect::<Vec<_>>()
        );
        wait_until_waiting(&harness.hub).await;
        set_frozen_head(&harness.hub, &archive, 1000).await;
        assert_eq!(
            read_block_nums(&mut client, 700).await,
            (301..=1000).collect::<Vec<_>>()
        );
        let requests = blocks_requests(&mock);
        assert_eq!(
            requests
                .iter()
                .map(|&(session, start, end, _)| (session, start, end))
                .collect::<Vec<_>>(),
            [(requests[0].0, 1, 301), (requests[0].0, 301, 1001)],
            "the same socket"
        );
        assert_eq!(mock.sessions_accepted(), 1);
        assert_eq!(harness.hub.stats.handoffs.load(Ordering::Relaxed), 0);
    }

    /// Stitched: a client that reaches the top of an archive-only fleet
    /// waits there for more blocks; no upstream takes the stream, and that
    /// is no handoff.
    #[tokio::test]
    async fn the_top_of_an_archive_only_fleet_is_no_handoff() {
        let (archive, _mock) = start_mock(MockShipConfig::frozen(1, 1001)).await;
        let harness = start_stitched(&[(&archive, 1, 1001)]).await;
        let mut client = connect(&harness.url).await;
        client
            .send(blocks_request(1, u32::MAX, 2000))
            .await
            .unwrap();
        assert_eq!(
            read_block_nums(&mut client, 1000).await,
            (1..=1000).collect::<Vec<_>>()
        );
        wait_until_waiting(&harness.hub).await;
        assert_eq!(harness.hub.stats.handoffs.load(Ordering::Relaxed), 0);
    }

    /// Stitched: the next archive's range begins above the fleet head, so
    /// the session waits at the end of the first leg. When the head moves
    /// on, the next leg is on the other archive: one handoff, counted then.
    #[tokio::test]
    async fn a_handoff_after_a_wait_is_counted_once_on_the_next_upstream() {
        let (a, a_mock) = start_mock(MockShipConfig::frozen(1, 301)).await;
        let (b, b_mock) = start_mock(MockShipConfig::frozen(301, 1001)).await;
        let harness = start_stitched(&[(&a, 1, 301), (&b, 301, 1001)]).await;
        set_frozen_head(&harness.hub, &b, 300).await;
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(1, 1001, 1000)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 300).await,
            (1..=300).collect::<Vec<_>>()
        );
        wait_until_waiting(&harness.hub).await;
        assert_eq!(harness.hub.stats.handoffs.load(Ordering::Relaxed), 0);
        set_frozen_head(&harness.hub, &b, 1000).await;
        assert_eq!(
            read_block_nums(&mut client, 700).await,
            (301..=1000).collect::<Vec<_>>()
        );
        let legs = |mock: &mock_ship::MockShipHandle| {
            blocks_requests(mock)
                .iter()
                .map(|&(_, start, end, _)| (start, end))
                .collect::<Vec<_>>()
        };
        assert_eq!(legs(&a_mock), [(1, 301)]);
        assert_eq!(legs(&b_mock), [(301, 1001)]);
        assert_eq!(harness.hub.stats.handoffs.load(Ordering::Relaxed), 1);
    }

    /// The info line of a reconnect after a failover keys the upstream by
    /// its endpoint and names it, like the line of the failover itself, in
    /// pool and stitched listeners alike.
    #[tokio::test]
    async fn the_reconnect_after_a_failover_logs_the_endpoint_and_the_name() {
        let (logs, _guard) = crate::test_log::capture();
        let reconnects = || logs.lines_with("reconnected to upstream after failover");
        let names = |line: &str, endpoint: &str, name: &str| {
            line.contains(&format!("upstream={endpoint} "))
                && line.contains(&format!("name={name}"))
        };

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
        let harness = start_session(&[&failing, &healthy], &[&failing], 0).await;
        let mut client = connect(&harness.url).await;
        set_online(&harness.hub, &healthy).await;
        client.send(blocks_request(100, 110, 100)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 10).await,
            (100..110).collect::<Vec<_>>()
        );
        let lines = reconnects();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(names(&lines[0], &healthy, "upstream-1"), "{}", lines[0]);

        let (dropping, _dropping_mock) = start_mock(MockShipConfig {
            disconnect_after: Some(3),
            ..MockShipConfig::frozen(1, 1001)
        })
        .await;
        let (replica, _replica_mock) = start_mock(MockShipConfig::frozen(1, 1001)).await;
        let harness = start_stitched(&[(&dropping, 1, 1001), (&replica, 1, 1001)]).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&replica).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(10, 20, 100)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 10).await,
            (10..20).collect::<Vec<_>>()
        );
        let lines = reconnects();
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(names(&lines[1], &replica, "archive-1"), "{}", lines[1]);
    }

    /// Stitched: the archive that holds blocks breaks the contract at 15 and
    /// is quarantined for 15..18, so its twin without a block log takes the
    /// stream there. The session moves back to the blocks where the
    /// quarantine ends, without waiting for a new view.
    #[tokio::test]
    async fn a_block_request_returns_to_the_blocks_where_a_quarantine_ends() {
        let (full, full_mock) = start_mock(MockShipConfig {
            omit_traces_at_block: Some(15),
            ..MockShipConfig::frozen(1, 1001)
        })
        .await;
        let (bare, _bare_mock) = start_mock(MockShipConfig {
            no_block_log: true,
            ..MockShipConfig::frozen(1, 1001)
        })
        .await;
        let servers: Vec<Server> = [&full, &bare]
            .iter()
            .enumerate()
            .map(|(i, endpoint)| Server {
                name: format!("archive-{i}"),
                endpoint: endpoint.to_string(),
                enabled: true,
                role: Role::Frozen,
                range: Some(crate::ship::Range {
                    begin: 1,
                    end: 1001,
                }),
                ..Default::default()
            })
            .collect();
        let resolved = ResolvedListener {
            topology: ResolvedTopology::Stitched,
            block_failure_quarantine: Some(BlockFailureQuarantine {
                strikes: 1,
                window_secs: 300,
                ttl_secs: 600,
                range: 3,
            }),
            ..listener(servers.clone())
        };
        let hub = FleetHub::new(
            &resolved,
            build_state_db(servers.clone()),
            build_config_db(servers),
        );
        {
            let mut db = hub.state.lock().await;
            // The archive that holds blocks is tried first.
            db.get_mut(&bare).unwrap().priority = 5;
            let abi = Arc::new(crate::abi::AbiEntry::new(mock_ship::SHIP_ABI_JSON));
            for endpoint in [&full, &bare] {
                let state = db.get_mut(endpoint).unwrap();
                let last = BlockPos {
                    num: 1000,
                    id: mock_ship::fake_block_id(1000),
                };
                state.online = true;
                state.ranges_known = true;
                state.trace_begin_block = 1;
                state.trace_end_block = 1001;
                state.chain_state_begin_block = 1;
                state.chain_state_end_block = 1001;
                state.head = Some(last);
                state.lib = Some(last);
                state.status_v1 = Some(true);
                state.abi = Some(abi.clone());
            }
            hub.rebuild(&db);
        }
        let harness = serve_one(&resolved, hub, limits(0)).await;
        let mut client = connect(&harness.url).await;
        client.send(block_request(10, 40, 100)).await.unwrap();
        let views = read_views(&mut client, 30).await;
        assert_eq!(
            views.iter().filter_map(BlockView::num).collect::<Vec<_>>(),
            (10..40).collect::<Vec<_>>()
        );
        let without: Vec<u32> = views
            .iter()
            .filter(|v| v.block.is_none())
            .filter_map(BlockView::num)
            .collect();
        assert_eq!(without, [15, 16, 17]);
        assert_eq!(
            blocks_requests(&full_mock)
                .iter()
                .map(|r| r.1)
                .collect::<Vec<_>>(),
            [10, 18]
        );
        let stats = &harness.hub.stats;
        assert_eq!(stats.failovers.get(FailoverReason::Violation), 1);
        assert_eq!(stats.failovers.get(FailoverReason::NoBlockLog), 1);
    }

    /// Stitched: a block request starts below the range of the archive that
    /// holds blocks, on one without a block log, whose leg was planned before
    /// it was known to lack them. The session moves to the blocks where that
    /// range begins.
    #[tokio::test]
    async fn a_block_request_moves_to_the_blocks_where_their_range_begins() {
        let (bare, bare_mock) = start_mock(MockShipConfig {
            no_block_log: true,
            ..MockShipConfig::frozen(1, 3001)
        })
        .await;
        let (full, full_mock) = start_mock(MockShipConfig::frozen(1000, 3001)).await;
        let harness = start_stitched(&[(&bare, 1, 3001), (&full, 1000, 3001)]).await;
        let mut client = connect(&harness.url).await;
        client.send(block_request(990, 1100, 200)).await.unwrap();
        let views = read_views(&mut client, 110).await;
        assert_eq!(
            views.iter().filter_map(BlockView::num).collect::<Vec<_>>(),
            (990..1100).collect::<Vec<_>>()
        );
        let without: Vec<u32> = views
            .iter()
            .filter(|v| v.block.is_none())
            .filter_map(BlockView::num)
            .collect();
        assert_eq!(without, (990..1000).collect::<Vec<_>>());
        assert_eq!(blocks_requests(&bare_mock)[0].1, 990);
        assert_eq!(blocks_requests(&full_mock)[0].1, 1000);
        assert_eq!(
            harness.hub.stats.failovers.get(FailoverReason::NoBlockLog),
            1
        );
    }

    /// A move off an archive without a block log is routine, like an
    /// operator move: nothing is wrong with the archive.
    #[test]
    fn a_move_off_an_archive_without_a_block_log_is_reported_as_routine() {
        assert_eq!(
            Left::Failover(FailoverReason::NoBlockLog).level(),
            Level::Info
        );
        assert_eq!(
            Left::Failover(FailoverReason::Operator).level(),
            Level::Info
        );
        assert_eq!(Left::Failover(FailoverReason::Silent).level(), Level::Warn);
    }

    fn filled(hub: &FleetHub, up: usize) -> u64 {
        hub.stats
            .upstream(up)
            .unwrap()
            .prev_blocks_filled
            .load(Ordering::Relaxed)
    }

    /// Stitched: an archive without a block log sends its first block with
    /// no `prev_block`. Right after the block before it, from a handoff or
    /// from a new request that goes on where the last one ended, the router
    /// fills it in, and only there.
    #[tokio::test]
    async fn a_missing_prev_block_after_a_delivered_block_is_filled_in() {
        let (a, _a_mock) = start_mock(MockShipConfig::frozen(1, 1001)).await;
        let (b, _b_mock) = start_mock(MockShipConfig {
            no_block_log: true,
            ..MockShipConfig::frozen(1001, 2001)
        })
        .await;
        let harness = start_stitched(&[(&a, 1, 1001), (&b, 1001, 2001)]).await;
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(990, 1010, 100)).await.unwrap();
        let views = read_views(&mut client, 20).await;
        assert_eq!(
            views.iter().filter_map(BlockView::num).collect::<Vec<_>>(),
            (990..1010).collect::<Vec<_>>()
        );
        for pair in views.windows(2) {
            assert_eq!(pair[1].prev, pair[0].this, "the chain continues");
        }
        assert_eq!(views[11].prev, Some((1000, mock_ship::fake_block_id(1000))));
        assert_eq!((filled(&harness.hub, 0), filled(&harness.hub, 1)), (0, 1));

        // A new request that goes on where the last one ended.
        client.send(blocks_request(995, 1001, 10)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 6).await,
            (995..1001).collect::<Vec<_>>()
        );
        client.send(blocks_request(1001, 1003, 10)).await.unwrap();
        let views = read_views(&mut client, 2).await;
        assert_eq!(views[0].prev, Some((1000, mock_ship::fake_block_id(1000))));
        assert_eq!(filled(&harness.hub, 1), 2);

        // A request that starts there after another block is left as sent.
        client.send(blocks_request(1001, 1002, 10)).await.unwrap();
        let views = read_views(&mut client, 1).await;
        assert_eq!(views[0].prev, None, "1002 was delivered last");
        assert_eq!(filled(&harness.hub, 1), 2);
    }

    /// Stitched: a new request that goes on where the last one ended lands
    /// on an archive whose first block does not continue the block the
    /// client got (another chain): a discontinuity, so the archive is struck
    /// and its replica serves the request.
    #[tokio::test]
    async fn a_new_request_on_an_archive_that_does_not_continue_is_a_violation() {
        let (a, _a_mock) = start_mock(MockShipConfig::frozen(1, 1001)).await;
        let (b, _b_mock) = start_mock(MockShipConfig {
            fork: Some((900, 9)),
            ..MockShipConfig::frozen(1001, 2001)
        })
        .await;
        let (b2, b2_mock) = start_mock(MockShipConfig::frozen(1001, 2001)).await;
        let harness = start_stitched(&[(&a, 1, 1001), (&b, 1001, 2001), (&b2, 1001, 2001)]).await;
        {
            // The archive on the other chain is tried first.
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&b2).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(995, 1001, 10)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 6).await,
            (995..1001).collect::<Vec<_>>()
        );
        client.send(blocks_request(1001, 1005, 10)).await.unwrap();
        let views = read_views(&mut client, 4).await;
        assert_eq!(
            views.iter().filter_map(BlockView::num).collect::<Vec<_>>(),
            (1001..1005).collect::<Vec<_>>()
        );
        assert_eq!(views[0].prev, Some((1000, mock_ship::fake_block_id(1000))));
        assert_eq!(blocks_requests(&b2_mock)[0].1, 1001);
        let b_stats = harness.hub.stats.upstream(1).unwrap();
        assert_eq!(
            b_stats
                .contract_violations
                .get(crate::fleet::ContractViolation::Discontinuity),
            1
        );
        assert_eq!(harness.hub.state.lock().await[&b].block_failures, 1);
    }

    /// Stitched: a live node that goes silent while serving the tip is
    /// marked offline, and the session fails over to the other live node.
    #[tokio::test]
    async fn a_silent_live_node_is_marked_offline_in_a_stitched_fleet() {
        let (archive, _archive_mock) = start_mock(MockShipConfig::frozen(1, 1001)).await;
        let (dead, dead_task) = dead_upstream().await;
        let (live, _live_mock) = start_mock(MockShipConfig::live(900, 1500)).await;
        let server = |name: &str, endpoint: &str, role: Role| Server {
            name: name.to_string(),
            endpoint: endpoint.to_string(),
            enabled: true,
            role,
            range: (role == Role::Frozen).then_some(crate::ship::Range {
                begin: 1,
                end: 1001,
            }),
            ..Default::default()
        };
        let servers = vec![
            server("archive", &archive, Role::Frozen),
            server("dead", &dead, Role::Live),
            server("live", &live, Role::Live),
        ];
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
            let pos = |num| BlockPos {
                num,
                id: mock_ship::fake_block_id(num),
            };
            for (endpoint, begin, end, head, lib, priority) in [
                (&archive, 1, 1001, 1000, 1000, 0),
                (&dead, 900, 1501, 1500, 1490, 0),
                (&live, 900, 1501, 1500, 1490, 5),
            ] {
                let state = db.get_mut(endpoint.as_str()).unwrap();
                state.online = true;
                state.ranges_known = true;
                state.trace_begin_block = begin;
                state.trace_end_block = end;
                state.chain_state_begin_block = begin;
                state.chain_state_end_block = end;
                state.head = Some(pos(head));
                state.lib = Some(pos(lib));
                state.status_v1 = Some(true);
                state.abi = Some(abi.clone());
                state.priority = priority;
            }
            hub.rebuild(&db);
        }
        let harness = serve_one(&resolved, hub, liveness(400)).await;
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        client.send(blocks_request(1100, 1110, 100)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 10).await,
            (1100..1110).collect::<Vec<_>>()
        );
        assert_eq!(info.upstream(), live);
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Silent), 1);
        let state = harness.hub.state.lock().await;
        assert!(!state[&dead].online, "a silent live node is marked offline");
        assert_eq!(state[&dead].block_failures, 0, "no strike");
        drop(state);
        dead_task.abort();
    }

    /// Stitched: an operator drains the archive a session is dialing for its
    /// leg. The dial lands after the drain, so the session serves the leg
    /// from the twin instead of streaming it from the drained archive.
    #[tokio::test]
    async fn a_dial_that_lands_on_a_drained_archive_serves_from_its_twin() {
        let (slow, mock_slow) = start_mock(MockShipConfig {
            // The ABI takes about a second.
            throttle_bytes_per_sec: Some(60_000),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let (twin, mock_twin) = start_mock(MockShipConfig::frozen(1, 2001)).await;
        let harness = start_stitched(&[(&slow, 1, 2001), (&twin, 1, 2001)]).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&twin).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        let dialing = timeout(WAIT, async {
            while mock_slow.sessions_accepted() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(
            dialing.is_ok(),
            "the leg is dialed on the preferred archive"
        );
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&slow).unwrap().operator_disabled = true;
            harness.hub.rebuild(&db);
        }
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (10..30).collect::<Vec<_>>()
        );
        assert_eq!(info.upstream(), twin);
        assert_eq!(mock_twin.sessions_accepted(), 1);
        assert!(
            blocks_requests(&mock_slow).is_empty(),
            "no request on the drained archive"
        );
    }

    /// Stitched: while an archive is being dialed, either (1) it is quarantined
    /// at the cursor so the session switches to a replica before the dial
    /// finishes, or (2) its reported range shrinks so `req_end` is updated on
    /// the in-flight dial and hands off cleanly at the new end.
    #[tokio::test]
    async fn a_stitched_dial_replans_when_its_target_is_quarantined_or_its_range_shrinks() {
        // 1. Target quarantined while dialing: switches to twin.
        let (slow, mock_slow) = start_mock(MockShipConfig {
            throttle_bytes_per_sec: Some(60_000),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let (twin, mock_twin) = start_mock(MockShipConfig::frozen(1, 2001)).await;
        let harness = start_stitched(&[(&slow, 1, 2001), (&twin, 1, 2001)]).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&twin).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        timeout(WAIT, async {
            while mock_slow.sessions_accepted() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the leg is dialed on the preferred archive");
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&slow)
                .unwrap()
                .quarantines
                .push(crate::models::BlockQuarantine {
                    block: 1,
                    range: 100,
                    until: std::time::Instant::now() + Duration::from_secs(60),
                });
            harness.hub.rebuild(&db);
        }
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (10..30).collect::<Vec<_>>()
        );
        assert_eq!(info.upstream(), twin);
        assert_eq!(mock_twin.sessions_accepted(), 1);
        assert!(
            blocks_requests(&mock_slow).is_empty(),
            "no request on the quarantined archive"
        );

        // 2. Target's reported range shrinks while dialing: keeps the in-flight
        // dial, clamps `req_end` to the shrunk end, and hands off at 500.
        let (first, mock_first) = start_mock(MockShipConfig {
            throttle_bytes_per_sec: Some(60_000),
            ..MockShipConfig::frozen(1, 500)
        })
        .await;
        let (second, mock_second) = start_mock(MockShipConfig::frozen(500, 2001)).await;
        // Declared as 1..2001 before the monitor shrinks `first` to 1..500.
        let harness = start_stitched(&[(&first, 1, 2001), (&second, 500, 2001)]).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&second).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(490, 510, 100)).await.unwrap();
        timeout(WAIT, async {
            while mock_first.sessions_accepted() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the leg is dialed on the first archive");
        {
            let mut db = harness.hub.state.lock().await;
            let state = db.get_mut(&first).unwrap();
            let last = BlockPos {
                num: 499,
                id: mock_ship::fake_block_id(499),
            };
            state.trace_end_block = 500;
            state.chain_state_end_block = 500;
            state.head = Some(last);
            state.lib = Some(last);
            harness.hub.rebuild(&db);
        }
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (490..510).collect::<Vec<_>>()
        );
        assert_eq!(
            blocks_requests(&mock_first),
            vec![(1, 490, 500, Vec::new())],
            "the in-flight dial updated its req_end from 510 to 500"
        );
        assert_eq!(
            blocks_requests(&mock_second),
            vec![(1, 500, 510, Vec::new())]
        );
    }

    /// Stitched: two equal replicas. While the session dials one of them,
    /// other sessions connect to one replica and then to the other, and
    /// views are published. The least-connections order flips each time, but
    /// the dial in progress is kept: one dial for the leg.
    #[tokio::test]
    async fn a_leg_dial_is_kept_when_the_load_between_replicas_shifts() {
        let (a, mock_a) = start_mock(MockShipConfig {
            throttle_bytes_per_sec: Some(60_000),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let (b, mock_b) = start_mock(MockShipConfig {
            throttle_bytes_per_sec: Some(60_000),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let harness = start_stitched(&[(&a, 1, 2001), (&b, 1, 2001)]).await;
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        timeout(WAIT, async {
            while mock_a.sessions_accepted() + mock_b.sessions_accepted() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the leg is dialed");
        let (first, second) = if mock_a.sessions_accepted() == 1 {
            (&a, &b)
        } else {
            (&b, &a)
        };
        let load = |ep: &str, n: usize| {
            let view = harness.hub.view();
            let u = view.upstreams.iter().find(|u| &*u.endpoint == ep).unwrap();
            u.connections.fetch_add(n, Ordering::Relaxed);
        };
        // Any view re-checks the dial; a head the archive reports moves one.
        let publish = |head: u32| {
            let hub = harness.hub.clone();
            let ep = first.to_string();
            async move {
                let mut db = hub.state.lock().await;
                db.get_mut(&ep).unwrap().head = Some(BlockPos {
                    num: head,
                    id: mock_ship::fake_block_id(head),
                });
                hub.rebuild(&db);
            }
        };
        load(first, 1);
        publish(1999).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        load(second, 2);
        publish(1998).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (10..30).collect::<Vec<_>>()
        );
        assert_eq!(
            mock_a.sessions_accepted() + mock_b.sessions_accepted(),
            1,
            "one dial for one leg"
        );
    }

    /// Stitched: the preferred archive refuses connections. Its dial
    /// exclusion (2 s) runs out while its slow twin is still being dialed,
    /// and a view comes: the preferred archive ranks first again, but the
    /// twin's dial is kept rather than abandoned for it.
    #[tokio::test]
    async fn a_leg_dial_is_kept_when_a_failed_archive_ranks_first_again() {
        let (a, refusing) = refusing_upstream().await;
        let (b, mock_b) = start_mock(MockShipConfig {
            throttle_bytes_per_sec: Some(20_000),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let harness = start_stitched_with(
            &[(&a, 1, 2001), (&b, 1, 2001)],
            ProxyLimits {
                handshake_timeout_ms: 10_000,
                ..limits(0)
            },
        )
        .await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&b).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        timeout(WAIT, async {
            while mock_b.sessions_accepted() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the twin is dialed after the refusal");
        // The twin's ABI takes about 3 s at this rate; the exclusion of `a`
        // runs out before.
        tokio::time::sleep(DIAL_EXCLUSION + Duration::from_millis(200)).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&b).unwrap().head = Some(BlockPos {
                num: 1999,
                id: mock_ship::fake_block_id(1999),
            });
            harness.hub.rebuild(&db);
        }
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (10..30).collect::<Vec<_>>()
        );
        assert_eq!(mock_b.sessions_accepted(), 1, "the twin's dial was kept");
        refusing.abort();
    }

    /// Stitched: an operator move dials the replica, and the monitor misses
    /// one status of the replica while the dial runs. The dial is kept (it
    /// tells whether the replica answers), so the move is not undone.
    #[tokio::test]
    async fn a_move_dial_survives_a_monitor_blip_on_its_target() {
        let (a, mock_a) = start_mock(MockShipConfig {
            block_delay: Some(Duration::from_millis(5)),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let (b, mock_b) = start_mock(MockShipConfig {
            throttle_bytes_per_sec: Some(60_000),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let harness = start_stitched(&[(&a, 1, 2001), (&b, 1, 2001)]).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&b).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        client.send(blocks_request(10, 1000, 1000)).await.unwrap();
        let mut blocks = read_block_nums(&mut client, 10).await;
        assert_eq!(info.upstream(), a);
        info.request_move();
        timeout(WAIT, async {
            while mock_b.sessions_accepted() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the move dials the replica");
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&b).unwrap().online = false;
            harness.hub.rebuild(&db);
        }
        blocks.extend(read_block_nums(&mut client, 30).await);
        assert_eq!(blocks, (10..50).collect::<Vec<_>>());
        assert_eq!(info.upstream(), b, "the move landed on the replica");
        assert_eq!(mock_a.sessions_accepted(), 1, "no return to the archive");
    }

    /// Stitched: the view that quarantines a leg's archive at the cursor is
    /// published as its dial ends, so the session takes that view only once
    /// the dial's outcome is in. The leg is still checked against it before
    /// the request is sent: it goes to the twin, and the quarantined archive
    /// gets no request.
    #[tokio::test]
    async fn a_view_taken_as_a_leg_dial_ends_is_checked_before_the_request() {
        let (x, mock_x) = start_mock(MockShipConfig::frozen(1, 2001)).await;
        let (twin, mock_twin) = start_mock(MockShipConfig::frozen(1, 2001)).await;
        let harness = start_stitched(&[(&x, 1, 2001), (&twin, 1, 2001)]).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&twin).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        let counters = harness
            .hub
            .view()
            .upstreams
            .iter()
            .find(|u| *u.endpoint == x)
            .unwrap()
            .counters
            .clone();
        // A task, not the test's own future: woken as the dial's task ends,
        // it runs before the session, which that end wakes next.
        let (armed_tx, armed) = tokio::sync::oneshot::channel();
        let hub = harness.hub.clone();
        let quarantined = x.clone();
        let publish = tokio::spawn(async move {
            let woken = counters.dial_succeeded.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            armed_tx.send(()).unwrap();
            woken.await;
            let mut db = hub.state.lock().await;
            db.get_mut(&quarantined)
                .unwrap()
                .quarantines
                .push(crate::models::BlockQuarantine {
                    block: 1,
                    range: 100,
                    until: std::time::Instant::now() + Duration::from_secs(60),
                });
            hub.rebuild(&db);
        });
        armed.await.unwrap();
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (10..30).collect::<Vec<_>>()
        );
        publish.await.unwrap();
        assert_eq!(mock_x.sessions_accepted(), 1, "the archive was dialed");
        assert!(
            blocks_requests(&mock_x).is_empty(),
            "no request on the quarantined archive"
        );
        assert_eq!(info.upstream(), twin);
        assert_eq!(mock_twin.sessions_accepted(), 1);
    }

    /// A host that accepts TCP connections and never answers the WebSocket
    /// upgrade, so a dial to it runs until its timeout. Counts the accepts.
    async fn hung_upstream() -> (String, Arc<AtomicUsize>, JoinHandle<()>) {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = tcp.local_addr().unwrap().to_string();
        let accepted = Arc::new(AtomicUsize::new(0));
        let count = accepted.clone();
        let task = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((stream, _)) = tcp.accept().await {
                count.fetch_add(1, Ordering::SeqCst);
                held.push(stream);
            }
        });
        (endpoint, accepted, task)
    }

    async fn mark_online(hub: &FleetHub, endpoint: &str, online: bool) {
        let mut db = hub.state.lock().await;
        db.get_mut(endpoint).unwrap().online = online;
        hub.rebuild(&db);
    }

    async fn until_accepted(accepted: &AtomicUsize) {
        timeout(WAIT, async {
            while accepted.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the hung host is dialed");
    }

    /// Stitched: the leg is dialed to the preferred archive, whose host
    /// hangs, and the monitor then marks it offline while its twin is
    /// online. The leg moves to the twin at once rather than wait out the
    /// dial's timeout.
    #[tokio::test]
    async fn a_leg_dial_to_an_archive_gone_offline_moves_to_its_online_twin() {
        let (x, accepted, hung) = hung_upstream().await;
        let (y, mock_y) = start_mock(MockShipConfig::frozen(1, 2001)).await;
        let harness = start_stitched_with(
            &[(&x, 1, 2001), (&y, 1, 2001)],
            ProxyLimits {
                connect_timeout_ms: 8_000,
                handshake_timeout_ms: 8_000,
                ..limits(0)
            },
        )
        .await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&y).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        let started = Instant::now();
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        until_accepted(&accepted).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        mark_online(&harness.hub, &x, false).await;
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (10..30).collect::<Vec<_>>()
        );
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "the twin served before the dial timed out: {:?}",
            started.elapsed()
        );
        assert_eq!(mock_y.sessions_accepted(), 1);
        hung.abort();
    }

    /// Stitched, bounded hold: both archives are offline and a hold retry
    /// dials the preferred one, whose host hangs. The twin comes online
    /// while that dial runs: the leg moves to it, rather than keep the dial
    /// past the hold's deadline and close the client.
    #[tokio::test]
    async fn a_hold_retry_dial_to_a_hung_archive_moves_when_its_twin_comes_online() {
        let (x, accepted, hung) = hung_upstream().await;
        let (y, mock_y) = start_mock(MockShipConfig::frozen(1, 2001)).await;
        let harness = start_stitched_tuned(
            &[(&x, 1, 2001), (&y, 1, 2001)],
            ProxyLimits {
                connect_timeout_ms: 8_000,
                handshake_timeout_ms: 8_000,
                ..limits(0)
            },
            StitchingPolicy {
                gap_wait_ms: 3_000,
                ..StitchingPolicy::default()
            },
        )
        .await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&y).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&x).unwrap().online = false;
            db.get_mut(&y).unwrap().online = false;
            harness.hub.rebuild(&db);
        }
        let started = Instant::now();
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        until_accepted(&accepted).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        mark_online(&harness.hub, &y, true).await;
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (10..30).collect::<Vec<_>>()
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "the twin served before the hold's deadline: {:?}",
            started.elapsed()
        );
        assert_eq!(mock_y.sessions_accepted(), 1);
        hung.abort();
    }

    /// Stitched, bounded hold: both archives stay offline and a hold retry
    /// dials the preferred one, whose host hangs past the hold's deadline.
    /// The deadline's last try keeps that dial; once it times out, the twin
    /// is dialed and serves, rather than the client being closed while the
    /// dial still ran.
    #[tokio::test]
    async fn a_hold_retry_dial_running_at_the_deadline_is_the_last_try() {
        let (x, accepted, hung) = hung_upstream().await;
        let (y, mock_y) = start_mock(MockShipConfig::frozen(1, 2001)).await;
        let harness = start_stitched_tuned(
            &[(&x, 1, 2001), (&y, 1, 2001)],
            ProxyLimits {
                connect_timeout_ms: 1_500,
                handshake_timeout_ms: 1_500,
                ..limits(0)
            },
            StitchingPolicy {
                gap_wait_ms: 800,
                ..StitchingPolicy::default()
            },
        )
        .await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&y).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&x).unwrap().online = false;
            db.get_mut(&y).unwrap().online = false;
            harness.hub.rebuild(&db);
        }
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        until_accepted(&accepted).await;
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (10..30).collect::<Vec<_>>()
        );
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "one dial to the hung host"
        );
        assert_eq!(mock_y.sessions_accepted(), 1);
        hung.abort();
    }

    /// Stitched, bounded hold: both archives are offline and a hold retry
    /// dials the preferred one, a slow but working host. Its twin comes
    /// online at `max_sessions` while that dial runs. At the hold's deadline
    /// the dial is the last try: the full twin does not close the client
    /// while the dial still runs.
    #[tokio::test]
    async fn a_hold_retry_dial_at_the_deadline_outlasts_a_full_online_twin() {
        let (x, mock_x) = start_mock(MockShipConfig {
            throttle_bytes_per_sec: Some(40_000),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let (y, mock_y) = start_mock(MockShipConfig::frozen(1, 2001)).await;
        let harness = start_stitched_tuned(
            &[(&x, 1, 2001), (&y, 1, 2001)],
            ProxyLimits {
                connect_timeout_ms: 8_000,
                handshake_timeout_ms: 8_000,
                ..limits(0)
            },
            StitchingPolicy {
                gap_wait_ms: 800,
                ..StitchingPolicy::default()
            },
        )
        .await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&y).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&x).unwrap().online = false;
            db.get_mut(&y).unwrap().online = false;
            harness.hub.rebuild(&db);
        }
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        timeout(WAIT, async {
            while mock_x.sessions_accepted() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the retry dials the slow archive");
        harness
            .hub
            .view()
            .upstreams
            .iter()
            .find(|u| *u.endpoint == y)
            .unwrap()
            .connections
            .fetch_add(1, Ordering::Relaxed);
        {
            let mut db = harness.hub.state.lock().await;
            let twin = db.get_mut(&y).unwrap();
            twin.online = true;
            twin.max_sessions = Some(1);
            harness.hub.rebuild(&db);
        }
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (10..30).collect::<Vec<_>>()
        );
        assert_eq!(mock_x.sessions_accepted(), 1);
        assert_eq!(mock_y.sessions_accepted(), 0, "the full twin is not dialed");
    }

    /// Stitched, bounded hold: two equal slow archives are offline and a
    /// hold retry dials one of them. Its load rises while that dial runs, so
    /// the other ranks first by the hold's deadline. The dial is still the
    /// last try: no second dial replaces it.
    #[tokio::test]
    async fn a_hold_retry_dial_at_the_deadline_is_kept_through_a_load_shift() {
        let slow = || MockShipConfig {
            throttle_bytes_per_sec: Some(40_000),
            ..MockShipConfig::frozen(1, 2001)
        };
        let (a, mock_a) = start_mock(slow()).await;
        let (b, mock_b) = start_mock(slow()).await;
        let harness = start_stitched_tuned(
            &[(&a, 1, 2001), (&b, 1, 2001)],
            ProxyLimits {
                connect_timeout_ms: 8_000,
                handshake_timeout_ms: 8_000,
                ..limits(0)
            },
            StitchingPolicy {
                gap_wait_ms: 800,
                ..StitchingPolicy::default()
            },
        )
        .await;
        let mut client = connect(&harness.url).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&a).unwrap().online = false;
            db.get_mut(&b).unwrap().online = false;
            harness.hub.rebuild(&db);
        }
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        timeout(WAIT, async {
            while mock_a.sessions_accepted() + mock_b.sessions_accepted() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the retry dials an archive");
        let dialed = if mock_a.sessions_accepted() == 1 {
            &a
        } else {
            &b
        };
        harness
            .hub
            .view()
            .upstreams
            .iter()
            .find(|u| *u.endpoint == **dialed)
            .unwrap()
            .connections
            .fetch_add(5, Ordering::Relaxed);
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (10..30).collect::<Vec<_>>()
        );
        assert_eq!(
            mock_a.sessions_accepted() + mock_b.sessions_accepted(),
            1,
            "the retry's dial was not replaced"
        );
    }

    /// Hold the twin Y online at its `max_sessions` while a bounded hold's
    /// retry dials the hung, offline archive X, then free Y's slot without
    /// publishing a view. Returns Y's connection counter.
    async fn hold_on_a_hung_archive_while_a_full_twin_frees_a_slot(
        harness: &Harness,
        client: &mut TestClient,
        x: &str,
        y: &str,
        accepted: &AtomicUsize,
    ) -> Arc<AtomicUsize> {
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(y).unwrap().priority = 5;
            db.get_mut(x).unwrap().online = false;
            db.get_mut(y).unwrap().online = false;
            harness.hub.rebuild(&db);
        }
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        until_accepted(accepted).await;
        let conns = harness
            .hub
            .view()
            .upstreams
            .iter()
            .find(|u| *u.endpoint == *y)
            .unwrap()
            .connections
            .clone();
        conns.fetch_add(1, Ordering::Relaxed);
        {
            let mut db = harness.hub.state.lock().await;
            let twin = db.get_mut(y).unwrap();
            twin.online = true;
            twin.max_sessions = Some(1);
            harness.hub.rebuild(&db);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Another session on Y ends: a slot frees, and no view is published.
        conns.fetch_sub(1, Ordering::Relaxed);
        conns
    }

    /// Stitched: at a bounded hold's deadline, the retry's dial to a hung,
    /// offline archive gives way to an online twin whose slot freed without
    /// a view.
    #[tokio::test]
    async fn a_hold_retry_dial_to_a_hung_archive_gives_way_at_the_deadline_to_a_freed_twin() {
        let (x, accepted, hung) = hung_upstream().await;
        let (y, mock_y) = start_mock(MockShipConfig::frozen(1, 2001)).await;
        let harness = start_stitched_tuned(
            &[(&x, 1, 2001), (&y, 1, 2001)],
            ProxyLimits {
                connect_timeout_ms: 8_000,
                handshake_timeout_ms: 8_000,
                ..limits(0)
            },
            StitchingPolicy {
                gap_wait_ms: 800,
                ..StitchingPolicy::default()
            },
        )
        .await;
        let mut client = connect(&harness.url).await;
        let started = Instant::now();
        hold_on_a_hung_archive_while_a_full_twin_frees_a_slot(
            &harness,
            &mut client,
            &x,
            &y,
            &accepted,
        )
        .await;
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (10..30).collect::<Vec<_>>()
        );
        assert_eq!(mock_y.sessions_accepted(), 1);
        assert!(
            started.elapsed() < Duration::from_secs(4),
            "the hung dial held the leg past the deadline: {:?}",
            started.elapsed()
        );
        hung.abort();
    }

    /// Stitched: as above, and the twin's freed slot is taken again after
    /// the deadline. The leg was already moved to the twin at the deadline,
    /// so the client is served instead of closed.
    #[tokio::test]
    async fn a_hold_retry_dial_that_gave_way_at_the_deadline_survives_the_twin_filling_again() {
        let (x, accepted, hung) = hung_upstream().await;
        let (y, _mock_y) = start_mock(MockShipConfig::frozen(1, 2001)).await;
        let harness = start_stitched_tuned(
            &[(&x, 1, 2001), (&y, 1, 2001)],
            ProxyLimits {
                connect_timeout_ms: 4_000,
                handshake_timeout_ms: 4_000,
                ..limits(0)
            },
            StitchingPolicy {
                gap_wait_ms: 800,
                ..StitchingPolicy::default()
            },
        )
        .await;
        let mut client = connect(&harness.url).await;
        let conns = hold_on_a_hung_archive_while_a_full_twin_frees_a_slot(
            &harness,
            &mut client,
            &x,
            &y,
            &accepted,
        )
        .await;
        tokio::time::sleep(Duration::from_millis(1_500)).await;
        // Another session takes Y's slot again, and no view is published.
        conns.fetch_add(1, Ordering::Relaxed);
        let first = timeout(Duration::from_secs(20), client.next()).await;
        hung.abort();
        assert!(
            matches!(first, Ok(Some(Ok(Message::Binary(_))))),
            "client not served: {first:?}"
        );
    }

    /// Stitched: the preferred archive drops the stream at a corrupt block,
    /// and the failover dials its twin, which the monitor marks offline
    /// while that dial runs. The archive just failed off is no online
    /// upstream to serve instead: the dial is kept, and the leg does not
    /// return there to fail again.
    #[tokio::test]
    async fn a_failover_dial_gone_offline_does_not_return_to_the_failed_archive() {
        let hits = Arc::new(AtomicUsize::new(0));
        let (y, _mock_y) = start_mock(MockShipConfig {
            corrupt_block: Some(15),
            corrupt_hits: Some(hits.clone()),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let (x, mock_x) = start_mock(MockShipConfig {
            throttle_bytes_per_sec: Some(40_000),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let harness = start_stitched_with(
            &[(&y, 1, 2001), (&x, 1, 2001)],
            ProxyLimits {
                connect_timeout_ms: 8_000,
                handshake_timeout_ms: 8_000,
                ..limits(0)
            },
        )
        .await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&x).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        let mut blocks = read_block_nums(&mut client, 5).await;
        timeout(WAIT, async {
            while mock_x.sessions_accepted() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the failover dials the twin");
        tokio::time::sleep(Duration::from_millis(100)).await;
        mark_online(&harness.hub, &x, false).await;
        blocks.extend(read_block_nums(&mut client, 15).await);
        assert_eq!(blocks, (10..30).collect::<Vec<_>>());
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "no return to the failed archive"
        );
        assert_eq!(mock_x.sessions_accepted(), 1);
    }

    /// Stitched: the session fails off archive Y at block 15 and finishes
    /// its request on X. Later X goes offline, H comes online, and a new
    /// request goes to H, whose host hangs; the monitor then marks H
    /// offline. That failover is over, so Y, online and able to serve, takes
    /// the leg instead of the hung dial.
    #[tokio::test]
    async fn an_old_soft_exclusion_does_not_keep_a_hung_dial_on_a_new_request() {
        let hits = Arc::new(AtomicUsize::new(0));
        let (y, _mock_y) = start_mock(MockShipConfig {
            corrupt_block: Some(15),
            corrupt_hits: Some(hits.clone()),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let (x, _mock_x) = start_mock(MockShipConfig::frozen(1, 2001)).await;
        let (h, accepted, hung) = hung_upstream().await;
        let harness = start_stitched_with(
            &[(&y, 1, 2001), (&x, 1, 2001), (&h, 1, 2001)],
            ProxyLimits {
                connect_timeout_ms: 8_000,
                handshake_timeout_ms: 8_000,
                ..limits(0)
            },
        )
        .await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&x).unwrap().priority = 5;
            db.get_mut(&h).unwrap().priority = 1;
            db.get_mut(&h).unwrap().online = false;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(10, 30, 100)).await.unwrap();
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (10..30).collect::<Vec<_>>()
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&x).unwrap().online = false;
            db.get_mut(&h).unwrap().online = true;
            harness.hub.rebuild(&db);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        client.send(blocks_request(500, 520, 100)).await.unwrap();
        until_accepted(&accepted).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let marked = Instant::now();
        mark_online(&harness.hub, &h, false).await;
        assert_eq!(
            read_block_nums(&mut client, 20).await,
            (500..520).collect::<Vec<_>>()
        );
        assert!(
            marked.elapsed() < Duration::from_secs(4),
            "the hung dial held the leg while Y was online: {:?}",
            marked.elapsed()
        );
        hung.abort();
    }

    /// Stitched: one request fails off archive Y at block 15 and goes on
    /// from X, whose range ends at 100. The next leg is pre-dialed to H,
    /// whose host hangs, and the monitor marks H offline while the handoff
    /// waits on that dial. The failover at 15 is over, so Y takes the leg.
    #[tokio::test]
    async fn an_old_soft_exclusion_does_not_keep_a_hung_handoff_dial() {
        let hits = Arc::new(AtomicUsize::new(0));
        let (y, _mock_y) = start_mock(MockShipConfig {
            corrupt_block: Some(15),
            corrupt_hits: Some(hits.clone()),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let (x, _mock_x) = start_mock(MockShipConfig::frozen(1, 100)).await;
        let (h, accepted, hung) = hung_upstream().await;
        let harness = start_stitched_with(
            &[(&y, 1, 2001), (&x, 1, 100), (&h, 50, 2001)],
            ProxyLimits {
                connect_timeout_ms: 8_000,
                handshake_timeout_ms: 8_000,
                ..limits(0)
            },
        )
        .await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&x).unwrap().priority = 5;
            db.get_mut(&h).unwrap().priority = 1;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        client.send(blocks_request(10, 120, 200)).await.unwrap();
        let mut blocks = read_block_nums(&mut client, 90).await;
        assert_eq!(blocks, (10..100).collect::<Vec<_>>());
        until_accepted(&accepted).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let marked = Instant::now();
        mark_online(&harness.hub, &h, false).await;
        blocks.extend(read_block_nums(&mut client, 20).await);
        assert_eq!(blocks, (10..120).collect::<Vec<_>>());
        assert!(
            marked.elapsed() < Duration::from_secs(4),
            "the hung handoff dial held the leg while Y was online: {:?}",
            marked.elapsed()
        );
        hung.abort();
    }

    /// Pool: the upstream an operator move dials is disabled before the dial
    /// lands. The session goes back to the upstream it left instead.
    #[tokio::test]
    async fn a_move_whose_target_is_disabled_while_dialing_returns_to_where_it_was() {
        let (a, mock_a) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            block_delay: Some(Duration::from_millis(5)),
            ..Default::default()
        })
        .await;
        let (b, mock_b) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            throttle_bytes_per_sec: Some(60_000),
            ..Default::default()
        })
        .await;
        let harness = start_session_with(&[&a, &b], &[&a, &b], limits(0)).await;
        set_tip(&harness.hub, &a, 100_000, 99_000).await;
        set_tip(&harness.hub, &b, 100_000, 99_000).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&b).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        assert_eq!(info.upstream(), a);
        client
            .send(blocks_request(100, 100_000, 100))
            .await
            .unwrap();
        let mut blocks = read_block_nums(&mut client, 5).await;

        info.request_move();
        let dialing = timeout(WAIT, async {
            while mock_b.sessions_accepted() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        assert!(dialing.is_ok(), "the move dials B");
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&b).unwrap().operator_disabled = true;
            harness.hub.rebuild(&db);
        }
        blocks.extend(read_block_nums(&mut client, 20).await);
        assert_eq!(blocks, (100..125).collect::<Vec<_>>());
        assert_eq!(info.upstream(), a);
        assert_eq!(mock_a.sessions_accepted(), 2);
        assert!(
            blocks_requests(&mock_b).is_empty(),
            "no request on the disabled upstream"
        );
    }

    /// Whether the control API counts a session at `next_block` on `endpoint`
    /// as movable (its drain and move check).
    fn api_counts_movable(hub: &FleetHub, endpoint: &str, next_block: Option<u32>) -> bool {
        hub.view()
            .upstreams
            .iter()
            .any(|u| &*u.endpoint != endpoint && can_take_over(u, next_block))
    }

    /// Pool: a session streams from A when an operator drains A, while
    /// `block` keeps B, the only other upstream, from taking it. The session
    /// stays on A and keeps streaming.
    async fn drain_leaves_the_session_when_b_cannot_take_it(
        block: impl FnOnce(&mut crate::models::ServerState),
    ) {
        let config = || MockShipConfig {
            blocks_to_stream: 0,
            block_delay: Some(Duration::from_millis(5)),
            ..Default::default()
        };
        let (a, _mock_a) = start_mock(config()).await;
        let (b, mock_b) = start_mock(config()).await;
        let harness = start_session_with(&[&a, &b], &[&a, &b], limits(0)).await;
        set_tip(&harness.hub, &a, 100_000, 99_000).await;
        set_tip(&harness.hub, &b, 100_000, 99_000).await;
        {
            let mut db = harness.hub.state.lock().await;
            let state = db.get_mut(&b).unwrap();
            state.priority = 5;
            block(state);
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        assert_eq!(info.upstream(), a);
        client
            .send(blocks_request(100, 100_000, 100))
            .await
            .unwrap();
        let mut blocks = read_block_nums(&mut client, 5).await;
        assert!(
            !api_counts_movable(&harness.hub, &a, info.next_block()),
            "the drain must report the session as staying"
        );

        // The session is told to move anyway (a stale API view, say).
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&a).unwrap().operator_disabled = true;
            info.request_move();
            harness.hub.rebuild(&db);
        }
        blocks.extend(read_block_nums(&mut client, 30).await);
        assert_eq!(blocks, (100..135).collect::<Vec<_>>());
        assert_eq!(info.upstream(), a, "the session stays where it is");
        assert_eq!(info.failovers(), 0);
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Operator), 0);
        assert_eq!(mock_b.sessions_accepted(), 0);
    }

    #[tokio::test]
    async fn a_drain_leaves_a_pool_session_whose_only_alternative_is_full() {
        drain_leaves_the_session_when_b_cannot_take_it(|b| {
            b.max_sessions = Some(1);
            b.connections.fetch_add(1, Ordering::Relaxed);
        })
        .await;
    }

    #[tokio::test]
    async fn a_drain_leaves_a_pool_session_whose_only_alternative_is_quarantined() {
        drain_leaves_the_session_when_b_cannot_take_it(|b| {
            b.quarantines.push(crate::models::BlockQuarantine {
                block: 1,
                range: 1_000_000,
                until: std::time::Instant::now() + Duration::from_secs(60),
            });
        })
        .await;
    }

    /// Stitched: draining the replica a session streams from, while its
    /// twin is at `max_sessions`, leaves the session where it is instead of
    /// holding it until the client is closed.
    #[tokio::test]
    async fn a_drain_leaves_a_stitched_session_whose_only_replica_is_full() {
        let replica = || MockShipConfig {
            block_delay: Some(Duration::from_millis(5)),
            ..MockShipConfig::frozen(1, 2001)
        };
        let (a, mock_a) = start_mock(replica()).await;
        let (b, mock_b) = start_mock(replica()).await;
        let harness = start_stitched(&[(&a, 1, 2001), (&b, 1, 2001)]).await;
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        client.send(blocks_request(10, 1000, 1000)).await.unwrap();
        let mut blocks = read_block_nums(&mut client, 20).await;
        let first = info.upstream();
        let (other, other_mock) = if first == a {
            (b.clone(), &mock_b)
        } else {
            (a.clone(), &mock_a)
        };
        {
            let mut db = harness.hub.state.lock().await;
            let state = db.get_mut(&other).unwrap();
            state.max_sessions = Some(1);
            state.connections.fetch_add(1, Ordering::Relaxed);
            harness.hub.rebuild(&db);
        }
        assert!(!api_counts_movable(&harness.hub, &first, info.next_block()));

        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&first).unwrap().operator_disabled = true;
            info.request_move();
            harness.hub.rebuild(&db);
        }
        blocks.extend(read_block_nums(&mut client, 50).await);
        assert_eq!(blocks, (10..80).collect::<Vec<_>>());
        assert_eq!(info.upstream(), first, "the session stays where it is");
        assert_eq!(info.failovers(), 0);
        assert_eq!(other_mock.sessions_accepted(), 0);
    }

    /// Stitched, default `history_preference` (frozen): a session reads old
    /// blocks from archive A while live node L's range starts below its
    /// cursor. An operator move lands on L. Role preference ranks above the
    /// soft exclusion of an ordinary failover, which alone would re-dial A.
    #[tokio::test]
    async fn an_operator_move_leaves_an_archive_for_a_live_node_that_covers_the_cursor() {
        let (a, mock_a) = start_mock(MockShipConfig {
            block_delay: Some(Duration::from_millis(5)),
            ..MockShipConfig::frozen(1, 1001)
        })
        .await;
        let (l, mock_l) = start_mock(MockShipConfig {
            block_delay: Some(Duration::from_millis(5)),
            ..MockShipConfig::live(900, 1500)
        })
        .await;
        let servers = vec![
            Server {
                name: "archive".into(),
                endpoint: a.clone(),
                enabled: true,
                role: Role::Frozen,
                range: Some(crate::ship::Range {
                    begin: 1,
                    end: 1001,
                }),
                ..Default::default()
            },
            Server {
                name: "live".into(),
                endpoint: l.clone(),
                enabled: true,
                role: Role::Live,
                ..Default::default()
            },
        ];
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
            let pos = |num| BlockPos {
                num,
                id: mock_ship::fake_block_id(num),
            };
            for (endpoint, begin, end, head, lib) in
                [(&a, 1, 1001, 1000, 1000), (&l, 900, 1501, 1500, 1490)]
            {
                let state = db.get_mut(endpoint.as_str()).unwrap();
                state.online = true;
                state.ranges_known = true;
                state.trace_begin_block = begin;
                state.trace_end_block = end;
                state.chain_state_begin_block = begin;
                state.chain_state_end_block = end;
                state.head = Some(pos(head));
                state.lib = Some(pos(lib));
                state.status_v1 = Some(true);
                state.abi = Some(abi.clone());
            }
            hub.rebuild(&db);
        }
        let harness = serve_one(&resolved, hub, limits(0)).await;
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        client.send(blocks_request(950, 1000, 1000)).await.unwrap();
        let mut blocks = read_block_nums(&mut client, 10).await;
        assert_eq!(info.upstream(), a, "history is read from the archive");
        assert!(api_counts_movable(&harness.hub, &a, info.next_block()));

        info.request_move();
        blocks.extend(read_block_nums(&mut client, 20).await);
        assert_eq!(blocks, (950..980).collect::<Vec<_>>());
        assert_eq!(info.upstream(), l, "moved to the live node");
        assert_eq!(info.failovers(), 1);
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Operator), 1);
        assert_eq!(
            mock_a.sessions_accepted(),
            1,
            "the archive is not re-dialed"
        );
        assert_eq!(mock_l.sessions_accepted(), 1);
        let moved = telemetry::global()
            .events_after(0)
            .into_iter()
            .filter(|e| e.session == Some(info.id) && e.kind == EventKind::SessionFailover)
            .map(|e| e.message)
            .collect::<Vec<_>>();
        assert_eq!(moved, [format!("moved from {a} to {l} (operator)")]);
    }

    /// An upstream that accepts TCP connections and drops them before the
    /// WebSocket handshake, so every dial to it fails at once.
    async fn refusing_upstream() -> (String, JoinHandle<()>) {
        let tcp = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = tcp.local_addr().unwrap().to_string();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = tcp.accept().await {
                drop(stream);
            }
        });
        (endpoint, task)
    }

    /// Pool: the upstream an operator move picked fails its dial and nothing
    /// else can take the session. It returns to the upstream it left, as
    /// after an ordinary failover, instead of closing the client.
    #[tokio::test]
    async fn an_operator_move_whose_target_fails_returns_to_the_upstream_it_left() {
        let (a, mock_a) = start_mock(MockShipConfig {
            blocks_to_stream: 0,
            block_delay: Some(Duration::from_millis(5)),
            ..Default::default()
        })
        .await;
        let (b, refusing) = refusing_upstream().await;
        let harness = start_session_with(&[&a, &b], &[&a, &b], limits(0)).await;
        set_tip(&harness.hub, &a, 100_000, 99_000).await;
        set_tip(&harness.hub, &b, 100_000, 99_000).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&b).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        assert_eq!(info.upstream(), a);
        client
            .send(blocks_request(100, 100_000, 100))
            .await
            .unwrap();
        let mut blocks = read_block_nums(&mut client, 5).await;

        info.request_move();
        blocks.extend(read_block_nums(&mut client, 30).await);
        assert_eq!(blocks, (100..135).collect::<Vec<_>>());
        assert_eq!(info.upstream(), a);
        assert_eq!(mock_a.sessions_accepted(), 2, "re-dialed after the move");
        assert_eq!(harness.hub.stats.failovers.get(FailoverReason::Operator), 1);
        refusing.abort();
    }

    /// Stitched: the same, with a replica whose dial fails. The session
    /// returns to the archive it left at once instead of holding until the
    /// move's exclusion runs out.
    #[tokio::test]
    async fn a_stitched_move_whose_target_fails_returns_to_the_archive_it_left() {
        let (a, mock_a) = start_mock(MockShipConfig {
            block_delay: Some(Duration::from_millis(5)),
            ..MockShipConfig::frozen(1, 2001)
        })
        .await;
        let (b, refusing) = refusing_upstream().await;
        let harness = start_stitched(&[(&a, 1, 2001), (&b, 1, 2001)]).await;
        {
            let mut db = harness.hub.state.lock().await;
            db.get_mut(&b).unwrap().priority = 5;
            harness.hub.rebuild(&db);
        }
        let mut client = connect(&harness.url).await;
        let info = registered(&client);
        client.send(blocks_request(10, 1000, 1000)).await.unwrap();
        let mut blocks = read_block_nums(&mut client, 10).await;
        assert_eq!(info.upstream(), a);

        info.request_move();
        blocks.extend(read_block_nums(&mut client, 30).await);
        assert_eq!(blocks, (10..50).collect::<Vec<_>>());
        assert_eq!(info.upstream(), a);
        assert_eq!(mock_a.sessions_accepted(), 2, "re-dialed after the move");
        refusing.abort();
    }
}
