//! The upstream side of a session: the connection slot counted against an
//! upstream, the transport wrapper that timestamps reads, the spawned dial
//! task, and the link with the request it carries.
//!
//! [`LinkState`] is the part of a link that holds no socket: the request the
//! upstream is serving, its inferred cursor and the de-dup window. The pure
//! helpers here build requests from it and decide whether the socket can
//! take a new request (`quiescent`) or still owes blocks (`owed`).

use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{timeout, timeout_at, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{client_async_tls_with_config, MaybeTlsStream, WebSocketStream};
use tracing::Instrument;

use crate::abi::accept_upstream_abi;
use crate::fleet::view::UpstreamView;
use crate::fleet::{DialFailure, FleetHub, LinkPurpose};
use crate::models::{ProxyLimits, ResolvedTopology, Role, UpstreamCounters};
use crate::ship::wire::BlocksRequest;
use crate::ship::BlockPos;

use super::client::{ClientSide, RESUME_POSITIONS};
use super::failover::UpEnd;

/// An upstream WebSocket whose transport reads are timestamped.
pub(super) type UpstreamWs = WebSocketStream<MaybeTlsStream<ActivityStream<TcpStream>>>;

/// Upstream picks per connect or failover before the client is closed. A pick
/// that finds no candidate ends the attempts.
pub(super) const MAX_DIAL_ATTEMPTS: u32 = 3;

/// Payload of the router's own pings to an upstream. Pongs carrying it are
/// liveness replies, not upstream activity.
pub(super) const LIVENESS_PING: &[u8] = b"fleet-router-liveness";

/// Holds one unit of an upstream's active-connection counter. Decrements
/// synchronously (and underflow-safely) when dropped — no spawn, no lock, no
/// dependency on a live runtime handle.
pub struct ConnectionGuard {
    counter: Arc<AtomicUsize>,
}

impl ConnectionGuard {
    /// Take one unit of `counter`; it is given back when the guard drops.
    pub(super) fn acquire(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self { counter }
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

/// The upstream's next block, as far as the router can tell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UpCursor {
    Exact(u32),
    /// Positions below the start were sent: the upstream may have moved its
    /// start anywhere in `[lo, hi]`.
    Uncertain {
        lo: u32,
        hi: u32,
    },
}

/// Why the router sent a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Purpose {
    /// The client's own request, on the link it arrived for.
    Initial,
    /// Stitched: the next leg of the client's request, after the previous
    /// upstream served its range up to a clamped end.
    Handoff,
    /// A resume after the previous link ended.
    Failover,
    /// The client's request, moved before it was forwarded to an upstream
    /// that covers it better.
    Reroute,
}

impl Purpose {
    pub fn label(self) -> LinkPurpose {
        match self {
            Purpose::Initial => LinkPurpose::Initial,
            Purpose::Handoff => LinkPurpose::Handoff,
            Purpose::Failover => LinkPurpose::Failover,
            Purpose::Reroute => LinkPurpose::Reroute,
        }
    }

    /// The request continues a stream the client already has (a seam the
    /// router made). Its first result must follow the client's last block;
    /// a fresh client request may legitimately start anywhere.
    pub fn resumes(self) -> bool {
        matches!(self, Purpose::Failover | Purpose::Handoff)
    }
}

/// The request a link is serving.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct LinkReq {
    pub start: u32,
    /// Exclusive.
    pub end: u32,
    /// `end` is below the client's end.
    pub clamped: bool,
    pub irreversible_only: bool,
    pub purpose: Purpose,
    pub sent_at: Instant,
}

/// Everything the router tracks about one link, apart from its socket.
#[derive(Debug)]
pub(super) struct LinkState {
    /// `ServerState::index` of the upstream.
    pub up: usize,
    pub role: Role,
    pub req: Option<LinkReq>,
    pub up_cursor: UpCursor,
    /// No result has arrived since the last request was sent.
    pub first_result_pending: bool,
    /// The de-dup window: open from a resume until a result reaches it.
    pub resume_floor: Option<u32>,
    /// Frozen: id-less results are allowed only below this block.
    pub expect_ids_from: u32,
    pub keepalives_seen: u64,
    /// Raw head and LIB of the last result.
    pub last_head: Option<BlockPos>,
    pub last_lib: Option<BlockPos>,
    /// A Close frame was read; the stream end that follows is clean.
    pub close_code: Option<u16>,
    /// Pool: the last unknown-variant client frame forwarded on this link.
    pub unknown_forwarded_at: Option<Instant>,
    /// When the link last started to owe blocks, or last made progress.
    pub became_owed_at: Instant,
    pub was_owed: bool,
    pub frames: u64,
    /// Stitched: a link kept open with nothing to send (its leg is done) is
    /// closed at this instant unless a new request reuses it first.
    pub linger_until: Option<Instant>,
    /// When a liveness ping went out, while no frame has arrived since.
    pub ping_at: Option<Instant>,
    /// The monitor lost this upstream: the link fails over at this instant
    /// unless a frame arrives first.
    pub probe_until: Option<Instant>,
    /// Transport activity (ms since the session epoch) when the last frame
    /// that was not a liveness pong was read.
    pub data_ms: u64,
    /// Transport activity when the last liveness pong was read, while no
    /// other frame has been read since.
    pub pong_ms: Option<u64>,
    /// The upstream was online in the last fleet view the session saw, so
    /// the view that shows it offline starts a probe.
    pub upstream_online: bool,
    /// The last time no upstream that holds blocks would have taken this
    /// link's stream. Until it is due again, a frozen result without the
    /// requested `block` is forwarded without asking again.
    pub blockless_checked: Option<BlocklessCheck>,
}

/// A link's stream stays on an archive without a block log: no upstream that
/// holds blocks would take it over at the cursor (see
/// `Session::forward_without_block`). The answer holds while the fleet view
/// stays the same, up to the block where another upstream may become
/// eligible, and for a short while: a freed slot, or an exclusion or a
/// quarantine that ran out, publishes no new view.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct BlocklessCheck {
    /// The fleet view generation the answer was given in.
    pub generation: u64,
    /// The archive's run end from the cursor then: where a range begins, a
    /// quarantine ends, or its own range ends.
    pub until: u32,
    /// Asked again from this instant, even in the same view and below
    /// `until`.
    pub next: Instant,
}

impl BlocklessCheck {
    /// Whether to ask again for a result at `cursor`, in the view of
    /// `generation`.
    pub fn due(&self, generation: u64, cursor: u32, now: Instant) -> bool {
        generation != self.generation || cursor >= self.until || now >= self.next
    }
}

impl LinkState {
    pub fn new(up: usize, role: Role, now: Instant) -> Self {
        LinkState {
            up,
            role,
            req: None,
            up_cursor: UpCursor::Exact(0),
            first_result_pending: false,
            resume_floor: None,
            expect_ids_from: 0,
            keepalives_seen: 0,
            last_head: None,
            last_lib: None,
            close_code: None,
            unknown_forwarded_at: None,
            became_owed_at: now,
            was_owed: false,
            frames: 0,
            linger_until: None,
            ping_at: None,
            probe_until: None,
            data_ms: 0,
            pong_ms: None,
            upstream_online: false,
            blockless_checked: None,
        }
    }

    /// A frame was read; `read_ms` is the transport activity now. Any frame
    /// shows the upstream is alive. Only frames other than a reply to the
    /// router's own liveness ping count as data for the idle, stall and
    /// monitor-offline silence limits.
    pub fn note_frame(&mut self, read_ms: u64, liveness_pong: bool) {
        self.ping_at = None;
        self.probe_until = None;
        if liveness_pong {
            self.pong_ms = Some(read_ms);
        } else {
            self.data_ms = read_ms;
            self.pong_ms = None;
        }
    }

    /// Transport activity (ms since the session epoch) that counts as data:
    /// the last read, unless all it carried was a liveness pong. Bytes read
    /// after that pong (part of a large message, say) count again.
    pub fn data_activity_ms(&self, read_ms: u64) -> u64 {
        if self.pong_ms == Some(read_ms) {
            self.data_ms
        } else {
            read_ms
        }
    }

    /// The link's request, if any, has nothing left to send: the upstream's
    /// cursor reached its end. A frozen upstream then stays silent until a
    /// new request.
    pub fn leg_done(&self) -> bool {
        match (&self.req, self.up_cursor) {
            (None, _) => true,
            (Some(req), UpCursor::Exact(u)) => u >= req.end,
            (Some(_), UpCursor::Uncertain { .. }) => false,
        }
    }

    /// How the stream ended when it ended now: clean after a Close frame,
    /// otherwise unclean.
    pub fn end_kind(&self) -> UpEnd {
        match self.close_code {
            Some(code) => UpEnd::Clean(code),
            None => UpEnd::Unclean,
        }
    }
}

/// Whether the link can take a new request on the same socket: nothing of
/// the old request can still arrive.
///
/// `pre_credit` is the client credit before the new request. With `C == 0`
/// nothing is in flight. A frozen upstream at the end of its request sends
/// nothing more. A live one could still rewind on a fork unless the request
/// ended at or below LIB + 1, and a node that sends head keep-alives (or only
/// speaks status v0, the legacy family that does) may send at any time.
pub(super) fn quiescent(st: &LinkState, pre_credit: u32, legacy_status: bool) -> bool {
    pre_credit == 0 || settled(st, legacy_status)
}

/// Whether nothing of the link's request can ever arrive, whatever credit
/// the client adds: it has none, or its end was reached as [`quiescent`]
/// describes. A link that is quiet only for lack of credit is not settled:
/// the next ack would resume its old request.
pub(super) fn settled(st: &LinkState, legacy_status: bool) -> bool {
    let Some(req) = &st.req else {
        return true;
    };
    match st.up_cursor {
        UpCursor::Exact(u) if u >= req.end => match st.role {
            Role::Frozen => true,
            Role::Live => {
                st.last_lib
                    .is_some_and(|lib| u64::from(req.end) <= u64::from(lib.num) + 1)
                    && st.keepalives_seen == 0
                    && !legacy_status
            }
        },
        _ => false,
    }
}

/// Whether the link owes the client blocks: a request, credit, and an
/// upstream cursor below its end and (live) at or below the gate a Spring
/// node would send up to. The gate is the link's own last head (LIB for
/// `irreversible_only`) or, failing that, the fleet's (`fleet_gate`).
///
/// An uncertain cursor is judged at its upper bound, where every position
/// matched and the upstream owes the least. A resume whose start is already
/// at its end, or past the gate, may rightly get nothing, so its silence is
/// never a stall.
pub(super) fn owed(
    st: &LinkState,
    credit: u32,
    fleet_gate: impl FnOnce(bool) -> Option<u32>,
) -> bool {
    let Some(req) = &st.req else {
        return false;
    };
    if credit == 0 {
        return false;
    }
    let u = match st.up_cursor {
        UpCursor::Exact(u) | UpCursor::Uncertain { hi: u, .. } => u,
    };
    if u >= req.end {
        return false;
    }
    if st.role == Role::Frozen {
        return true;
    }
    let own = if req.irreversible_only {
        st.last_lib
    } else {
        st.last_head
    };
    own.is_some_and(|g| u <= g.num) || fleet_gate(req.irreversible_only).is_some_and(|g| u <= g)
}

/// What the router knows about the upstream a request goes to, besides its
/// role: how the listener routes, and the first block whose id it knows.
#[derive(Clone, Copy, Debug)]
pub(super) struct Target {
    pub topology: ResolvedTopology,
    pub ids_begin: Option<u32>,
}

/// The `have_positions` sent upstream.
///
/// A frozen upstream always gets none. A fresh request to a live one carries
/// the client's positions (verbatim in pool topology; in stitched topology
/// only those below the start that the upstream can know). A resume carries
/// the reversible tail the client already has, so the upstream rewinds
/// exactly where its chain differs; `irreversible_only` needs none.
pub(super) fn positions_for(
    c: &ClientSide,
    creq: &BlocksRequest,
    role: Role,
    fresh: bool,
    start: u32,
    target: Target,
) -> Vec<BlockPos> {
    match (role, fresh) {
        (Role::Frozen, _) => Vec::new(),
        (Role::Live, true) => match target.topology {
            ResolvedTopology::Pool => creq.have_positions.clone(),
            ResolvedTopology::Stitched => creq
                .have_positions
                .iter()
                .filter(|p| p.num < start && target.ids_begin.is_some_and(|b| p.num >= b))
                .copied()
                .collect(),
        },
        (Role::Live, false) => {
            if creq.irreversible_only {
                Vec::new()
            } else {
                c.delivered.reversible_tail(
                    c.lib_sent.map_or(0, |l| l.num),
                    c.cursor,
                    RESUME_POSITIONS,
                )
            }
        }
    }
}

/// Build the request a link is sent and record it on `st`.
///
/// It is always rebuilt: the client's version and flags, `start` at the
/// client's request start (fresh) or its cursor (resume), the planned `end`,
/// and `max_messages_in_flight` equal to the client credit now. A resume, or
/// a router-generated request that carries positions, opens the de-dup
/// window at the cursor.
pub(super) fn build_request(
    c: &ClientSide,
    creq: &BlocksRequest,
    st: &mut LinkState,
    end: u32,
    purpose: Purpose,
    target: Target,
    now: Instant,
) -> BlocksRequest {
    let fresh = c.delivered_for_request == 0;
    let start = if fresh { creq.start } else { c.cursor };
    let have_positions = positions_for(c, creq, st.role, fresh, start, target);
    let lo = have_positions
        .iter()
        .map(|p| p.num)
        .filter(|&n| n < start)
        .min();
    st.req = Some(LinkReq {
        start,
        end,
        clamped: end < creq.end,
        irreversible_only: creq.irreversible_only,
        purpose,
        sent_at: now,
    });
    st.up_cursor = match lo {
        Some(lo) => UpCursor::Uncertain { lo, hi: start },
        None => UpCursor::Exact(start),
    };
    st.resume_floor = (!fresh && (purpose == Purpose::Failover || !have_positions.is_empty()))
        .then_some(c.cursor);
    st.expect_ids_from = match st.role {
        Role::Frozen => target.ids_begin.unwrap_or(0),
        Role::Live => 0,
    };
    st.first_result_pending = true;
    st.became_owed_at = now;
    st.linger_until = None;
    // A new request asks afresh whether its results without `block` stay.
    st.blockless_checked = None;
    BlocksRequest {
        v1: creq.v1,
        start,
        end,
        max_in_flight: c.credit,
        have_positions,
        irreversible_only: creq.irreversible_only,
        fetch_block: creq.fetch_block,
        fetch_traces: creq.fetch_traces,
        fetch_deltas: creq.fetch_deltas,
        fetch_finality_data: creq.fetch_finality_data,
    }
}

/// A transport that records when it last read bytes, as milliseconds since
/// the session's epoch. A large message arriving slowly keeps the link alive,
/// because every partial read counts.
pub(super) struct ActivityStream<S> {
    inner: S,
    activity: Arc<AtomicU64>,
    epoch: Instant,
}

impl<S> ActivityStream<S> {
    pub fn new(inner: S, activity: Arc<AtomicU64>, epoch: Instant) -> Self {
        ActivityStream {
            inner,
            activity,
            epoch,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for ActivityStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let polled = Pin::new(&mut this.inner).poll_read(cx, buf);
        if matches!(polled, Poll::Ready(Ok(()))) && buf.filled().len() > before {
            let ms = u64::try_from(this.epoch.elapsed().as_millis()).unwrap_or(u64::MAX);
            this.activity.store(ms, Ordering::Relaxed);
        }
        polled
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for ActivityStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

/// Why a dial failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum DialError {
    /// HTTP 503 at the upgrade: the upstream is at its session limit.
    Busy,
    /// TCP connect failed or timed out.
    Unreachable(String),
    /// The TLS or WebSocket handshake failed or timed out.
    Handshake(String),
    /// No ABI within `handshake_timeout_ms`.
    AbiTimeout,
    /// The first frame was not a valid Text ABI.
    AbiInvalid(String),
}

impl DialError {
    pub fn label(&self) -> DialFailure {
        match self {
            DialError::Busy => DialFailure::Busy,
            DialError::Unreachable(_) => DialFailure::Unreachable,
            DialError::Handshake(_) => DialFailure::Handshake,
            DialError::AbiTimeout | DialError::AbiInvalid(_) => DialFailure::Abi,
        }
    }
}

impl std::fmt::Display for DialError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DialError::Busy => write!(f, "upstream answered 503 (busy)"),
            DialError::Unreachable(e) => write!(f, "connect failed: {e}"),
            DialError::Handshake(e) => write!(f, "handshake failed: {e}"),
            DialError::AbiTimeout => write!(f, "no ABI before the handshake timeout"),
            DialError::AbiInvalid(e) => write!(f, "invalid or absent ABI handshake: {e}"),
        }
    }
}

/// A connected upstream that has sent a valid ABI.
pub(super) struct Established {
    pub ws: UpstreamWs,
    pub abi: String,
    pub activity: Arc<AtomicU64>,
    pub guard: ConnectionGuard,
}

/// The upstream a dial is for, copied from the fleet view.
#[derive(Clone, Debug)]
pub(super) struct DialTarget {
    pub up: usize,
    pub endpoint: Arc<str>,
    pub name: Arc<str>,
    pub url: Arc<str>,
    pub role: Role,
    pub counters: Arc<UpstreamCounters>,
}

impl DialTarget {
    pub fn of(u: &UpstreamView) -> Self {
        DialTarget {
            up: u.index,
            endpoint: u.endpoint.clone(),
            name: u.name.clone(),
            url: u.url.clone(),
            role: u.role,
            counters: u.counters.clone(),
        }
    }
}

/// A dial running in its own task, so the session loop keeps serving the
/// client (and shutdown) while the handshake is in progress.
///
/// Dropping a `DialTask` aborts it; the connection slot it holds is released
/// when the aborted task is dropped.
pub(super) struct DialTask {
    target: DialTarget,
    handle: JoinHandle<Result<Established, DialError>>,
}

impl DialTask {
    /// Start dialing `target`, holding `guard` on its connection counter. The
    /// task runs in the caller's tracing span. A dial that gets through wakes
    /// the upstream's monitor if it is waiting to reconnect
    /// ([`UpstreamCounters::dial_succeeded`]).
    pub fn spawn(
        target: DialTarget,
        limits: ProxyLimits,
        guard: ConnectionGuard,
        epoch: Instant,
    ) -> Self {
        let url = target.url.clone();
        let counters = target.counters.clone();
        let handle = tokio::spawn(
            async move {
                let outcome = dial(url, limits, guard, epoch).await;
                if outcome.is_ok() {
                    counters.dial_succeeded.notify_waiters();
                }
                outcome
            }
            .in_current_span(),
        );
        Self { target, handle }
    }

    /// The upstream being dialed.
    pub fn target(&self) -> &DialTarget {
        &self.target
    }

    /// Wait for the dial to finish. Cancel-safe: a wait that is dropped
    /// leaves the task running, and a later call picks up its result.
    pub async fn outcome(&mut self) -> Result<Established, DialError> {
        match (&mut self.handle).await {
            Ok(outcome) => outcome,
            Err(error) => Err(DialError::Handshake(format!("dial task failed: {error}"))),
        }
    }
}

impl Drop for DialTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// The host and port a WebSocket URL dials.
fn host_port(url: &str) -> Result<(String, u16), DialError> {
    let request = url
        .into_client_request()
        .map_err(|e| DialError::Handshake(e.to_string()))?;
    let uri = request.uri();
    let host = uri
        .host()
        .ok_or_else(|| DialError::Handshake(format!("no host in {url}")))?;
    // An IPv6 literal keeps its brackets in the URI.
    let host = host
        .strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host);
    let port = uri.port_u16().unwrap_or(match uri.scheme_str() {
        Some("wss") => 443,
        _ => 80,
    });
    Ok((host.to_string(), port))
}

/// Connect to `url`, upgrade (TLS for `wss://`) and read the ABI.
///
/// The TCP connect and the upgrade share one `connect_timeout_ms` deadline;
/// the wait for the ABI is bounded by `handshake_timeout_ms`, or by
/// `connect_timeout_ms` when that is 0.
/// The session aborts the task when the client leaves or the router shuts
/// down. `guard` is released on every error.
async fn dial(
    url: Arc<str>,
    limits: ProxyLimits,
    guard: ConnectionGuard,
    epoch: Instant,
) -> Result<Established, DialError> {
    let (host, port) = host_port(&url)?;
    let deadline = Instant::now() + Duration::from_millis(limits.connect_timeout_ms.max(1));
    let tcp = match timeout_at(deadline, TcpStream::connect((host.as_str(), port))).await {
        Ok(Ok(tcp)) => tcp,
        Ok(Err(e)) => return Err(DialError::Unreachable(e.to_string())),
        Err(_) => return Err(DialError::Unreachable("connect timed out".to_string())),
    };
    let _ = tcp.set_nodelay(true);
    let activity = Arc::new(AtomicU64::new(0));
    let stream = ActivityStream::new(tcp, activity.clone(), epoch);

    let mut config = WebSocketConfig::default();
    config.max_message_size = Some(limits.max_message_bytes);
    config.max_frame_size = Some(limits.max_message_bytes);
    let upgrade = client_async_tls_with_config(&*url, stream, Some(config), None);
    let mut ws = match timeout_at(deadline, upgrade).await {
        Ok(Ok((ws, _))) => ws,
        Ok(Err(WsError::Http(response))) if response.status().as_u16() == 503 => {
            return Err(DialError::Busy)
        }
        Ok(Err(e)) => return Err(DialError::Handshake(e.to_string())),
        Err(_) => return Err(DialError::Handshake("timed out".to_string())),
    };

    let abi_wait = match limits.handshake_timeout_ms {
        0 => limits.connect_timeout_ms.max(1),
        ms => ms,
    };
    let abi = match read_first_abi(&mut ws, Duration::from_millis(abi_wait)).await {
        Ok(abi) => abi,
        Err(e) => {
            let _ = ws.close(None).await;
            return Err(e);
        }
    };
    // Validate on a fresh handle and drop it. The parsed model is not kept.
    if let Err(e) = accept_upstream_abi(&abi) {
        let _ = ws.close(None).await;
        return Err(DialError::AbiInvalid(e.to_string()));
    }
    Ok(Established {
        ws,
        abi,
        activity,
        guard,
    })
}

/// Read the upstream's first meaningful frame, which must be the Text ABI.
/// Leading Ping/Pong control frames are skipped. One deadline covers the
/// whole read, so a peer that drips control frames cannot extend it.
async fn read_first_abi(ws: &mut UpstreamWs, wait: Duration) -> Result<String, DialError> {
    let read = async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => return Ok(text.to_string()),
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => continue,
                Some(Ok(Message::Binary(_))) => {
                    return Err(DialError::AbiInvalid("binary first frame".to_string()))
                }
                Some(Ok(other)) => {
                    return Err(DialError::AbiInvalid(format!(
                        "unexpected first frame {other:?}"
                    )))
                }
                Some(Err(e)) => return Err(DialError::AbiInvalid(e.to_string())),
                None => return Err(DialError::AbiInvalid("closed before the ABI".to_string())),
            }
        }
    };
    timeout(wait, read)
        .await
        .unwrap_or(Err(DialError::AbiTimeout))
}

/// A session's upstream socket and what the router knows about it.
pub(super) struct Link {
    pub st: LinkState,
    pub endpoint: Arc<str>,
    pub name: Arc<str>,
    pub tx: SplitSink<UpstreamWs, Message>,
    pub rx: SplitStream<UpstreamWs>,
    /// Milliseconds since the session epoch of the last transport read.
    pub activity: Arc<AtomicU64>,
    /// The upstream's cumulative counters.
    pub counters: Arc<UpstreamCounters>,
    guard: ConnectionGuard,
}

impl Link {
    pub fn new(target: &DialTarget, est: Established, now: Instant) -> Self {
        let (tx, rx) = est.ws.split();
        let mut st = LinkState::new(target.up, target.role, now);
        st.data_ms = est.activity.load(Ordering::Relaxed);
        Link {
            st,
            endpoint: target.endpoint.clone(),
            name: target.name.clone(),
            tx,
            rx,
            activity: est.activity,
            counters: target.counters.clone(),
            guard: est.guard,
        }
    }

    /// Milliseconds since the session epoch of the last transport read.
    pub fn read_ms(&self) -> u64 {
        self.activity.load(Ordering::Relaxed)
    }

    /// Send a liveness ping. Bounded, so a stuck socket cannot hold the
    /// session loop.
    pub async fn ping(&mut self) {
        let ping = self.tx.send(Message::Ping(LIVENESS_PING.into()));
        if !matches!(timeout(Duration::from_secs(1), ping).await, Ok(Ok(()))) {
            tracing::debug!(upstream = %self.endpoint, "liveness ping to upstream not sent");
        }
    }

    /// Close the socket in the background (Close 1000, at most 1 s) and keep
    /// only the connection slot.
    pub fn close_quietly(self) -> ConnectionGuard {
        let Link {
            mut tx, rx, guard, ..
        } = self;
        tokio::spawn(async move {
            let _ = timeout(Duration::from_secs(1), async {
                let _ = tx
                    .send(Message::Close(Some(CloseFrame {
                        code: CloseCode::Normal,
                        reason: "".into(),
                    })))
                    .await;
                let _ = tx.close().await;
            })
            .await;
            drop(rx);
        });
        guard
    }
}

/// A frozen upstream sent a result without the `block` a session asked for,
/// for a block it covers: record that it has no block log and publish the
/// change. Returns whether this call set the flag.
pub(super) async fn mark_no_block_log(hub: &FleetHub, endpoint: &str) -> bool {
    let mut db = hub.state.lock().await;
    let newly = match db.get_mut(endpoint) {
        Some(state) if state.role == Role::Frozen && !state.no_block_log => {
            state.no_block_log = true;
            true
        }
        _ => false,
    };
    if newly {
        hub.rebuild(&db);
    }
    newly
}

/// A session failed to dial a live upstream: mark it offline, as the monitor
/// would, and publish the change. Frozen upstreams are never marked offline
/// from a session.
pub(super) async fn mark_offline(hub: &FleetHub, endpoint: &str) {
    let mut db = hub.state.lock().await;
    if let Some(state) = db.get_mut(endpoint) {
        if state.role == Role::Live {
            state.mark_offline(std::time::Instant::now());
        }
    }
    hub.rebuild(&db);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{build_config_db, build_state_db, ResolvedListener, Server};
    use futures::SinkExt;
    use tokio::net::TcpListener;

    const ABI: &str = include_str!("../../mock-ship/src/ship_abi.json");

    fn limits() -> ProxyLimits {
        ProxyLimits {
            handshake_timeout_ms: 2_000,
            idle_timeout_ms: 0,
            upstream_liveness_timeout_ms: 0,
            max_message_bytes: 1 << 20,
            max_client_message_bytes: 64 * 1024,
            stall_timeout_ms: 0,
            connect_timeout_ms: 2_000,
        }
    }

    fn target(endpoint: &str) -> DialTarget {
        DialTarget {
            up: 0,
            endpoint: Arc::from(endpoint),
            name: Arc::from("test-upstream"),
            url: Arc::from(format!("ws://{endpoint}")),
            role: Role::Live,
            counters: Arc::default(),
        }
    }

    fn slots(counter: &AtomicUsize) -> usize {
        counter.load(Ordering::Relaxed)
    }

    /// One-connection upstream that completes the WebSocket handshake, sends
    /// `first`, then reads until the dialer goes away.
    async fn upstream_sending(first: Message) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.send(first).await.unwrap();
            while let Some(Ok(_)) = ws.next().await {}
        });
        endpoint
    }

    #[tokio::test]
    async fn dial_returns_the_text_abi_and_holds_the_slot() {
        let endpoint = upstream_sending(Message::text(ABI)).await;
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = ConnectionGuard::acquire(counter.clone());
        let mut task = DialTask::spawn(target(&endpoint), limits(), guard, Instant::now());
        assert_eq!(&*task.target().endpoint, endpoint);

        let established = task.outcome().await.expect("dial should succeed");
        assert_eq!(established.abi, ABI);
        assert_eq!(slots(&counter), 1, "an established link keeps its slot");

        let link = Link::new(task.target(), established, Instant::now());
        assert_eq!(link.st.up, 0);
        assert_eq!(&*link.name, "test-upstream");
        let guard = link.close_quietly();
        assert_eq!(slots(&counter), 1, "a closed link hands over its slot");
        drop(guard);
        assert_eq!(slots(&counter), 0);
    }

    /// A dial that gets through wakes the upstream's monitor (waiting to
    /// reconnect); a failed one does not.
    #[tokio::test]
    async fn a_dial_that_gets_through_wakes_the_monitor() {
        for (first, gets_through) in [
            (Message::text(ABI), true),
            (Message::binary(vec![0u8; 4]), false),
        ] {
            let endpoint = upstream_sending(first).await;
            let target = target(&endpoint);
            let counters = target.counters.clone();
            let woken = counters.dial_succeeded.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            let guard = ConnectionGuard::acquire(Arc::new(AtomicUsize::new(0)));
            let mut task = DialTask::spawn(target, limits(), guard, Instant::now());
            assert_eq!(task.outcome().await.is_ok(), gets_through);
            let wait = Duration::from_millis(if gets_through { 1_000 } else { 100 });
            assert_eq!(
                timeout(wait, woken).await.is_ok(),
                gets_through,
                "gets through: {gets_through}"
            );
        }
    }

    #[tokio::test]
    async fn dial_rejects_a_binary_first_frame_and_releases_the_slot() {
        let endpoint = upstream_sending(Message::binary(vec![0u8; 4])).await;
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = ConnectionGuard::acquire(counter.clone());
        let mut task = DialTask::spawn(target(&endpoint), limits(), guard, Instant::now());

        let err = task.outcome().await.err().expect("dial should fail");
        assert!(matches!(err, DialError::AbiInvalid(_)), "{err:?}");
        assert_eq!(err.label(), DialFailure::Abi);
        assert_eq!(slots(&counter), 0);
    }

    #[tokio::test]
    async fn dial_rejects_text_that_is_not_an_abi() {
        let endpoint = upstream_sending(Message::text("not json")).await;
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = ConnectionGuard::acquire(counter.clone());
        let mut task = DialTask::spawn(target(&endpoint), limits(), guard, Instant::now());
        assert!(matches!(
            task.outcome().await,
            Err(DialError::AbiInvalid(_))
        ));
        assert_eq!(slots(&counter), 0);
    }

    #[tokio::test]
    async fn dial_reports_a_refused_connect_and_releases_the_slot() {
        let endpoint = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap().to_string()
        };
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = ConnectionGuard::acquire(counter.clone());
        let mut task = DialTask::spawn(target(&endpoint), limits(), guard, Instant::now());

        let err = task.outcome().await.err().expect("dial should fail");
        assert!(matches!(err, DialError::Unreachable(_)), "{err:?}");
        assert_eq!(err.label(), DialFailure::Unreachable);
        assert_eq!(slots(&counter), 0);
    }

    #[tokio::test]
    async fn a_hung_upgrade_times_out_at_the_connect_deadline() {
        // Accept TCP and never answer the WebSocket upgrade.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let holder = tokio::spawn(async move {
            let (_tcp, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = ConnectionGuard::acquire(counter.clone());
        let fast = ProxyLimits {
            connect_timeout_ms: 200,
            ..limits()
        };
        let started = Instant::now();
        let mut task = DialTask::spawn(target(&endpoint), fast, guard, Instant::now());
        let err = timeout(Duration::from_secs(5), task.outcome())
            .await
            .expect("the dial must time out on its own")
            .err()
            .expect("dial should fail");
        assert!(matches!(err, DialError::Handshake(_)), "{err:?}");
        assert!(started.elapsed() >= Duration::from_millis(150));
        assert_eq!(slots(&counter), 0);
        holder.abort();
    }

    #[tokio::test]
    async fn without_a_handshake_timeout_the_abi_wait_ends_at_the_connect_timeout() {
        // Complete the WebSocket upgrade and never send the ABI.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let holder = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let _ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            std::future::pending::<()>().await;
        });
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = ConnectionGuard::acquire(counter.clone());
        let fast = ProxyLimits {
            handshake_timeout_ms: 0,
            connect_timeout_ms: 200,
            ..limits()
        };
        let started = Instant::now();
        let mut task = DialTask::spawn(target(&endpoint), fast, guard, Instant::now());
        let err = timeout(Duration::from_secs(5), task.outcome())
            .await
            .expect("the dial must time out on its own")
            .err()
            .expect("dial should fail");
        assert_eq!(err, DialError::AbiTimeout);
        assert!(started.elapsed() >= Duration::from_millis(150));
        assert_eq!(slots(&counter), 0);
        holder.abort();
    }

    #[tokio::test]
    async fn a_503_upgrade_is_busy() {
        use mock_ship::{MockShipConfig, MockShipServer};
        let mock = MockShipServer::new(MockShipConfig {
            max_sessions: Some(0),
            ..MockShipConfig::default()
        })
        .await;
        let endpoint = mock.endpoint();
        let _mock = mock.start();
        let counter = Arc::new(AtomicUsize::new(0));
        let guard = ConnectionGuard::acquire(counter.clone());
        let mut task = DialTask::spawn(target(&endpoint), limits(), guard, Instant::now());
        let err = task.outcome().await.err().expect("dial should fail");
        assert_eq!(err, DialError::Busy);
        assert_eq!(err.label(), DialFailure::Busy);
        assert_eq!(slots(&counter), 0);
    }

    #[tokio::test]
    async fn dropping_a_dial_task_aborts_it_and_releases_the_slot() {
        // Accept TCP and never answer the WebSocket upgrade, so the dial hangs.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let holder = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            std::future::pending::<()>().await;
            drop(tcp);
        });

        let counter = Arc::new(AtomicUsize::new(0));
        let guard = ConnectionGuard::acquire(counter.clone());
        let task = DialTask::spawn(target(&endpoint), limits(), guard, Instant::now());
        timeout(Duration::from_secs(5), accepted_rx)
            .await
            .expect("dial should reach the upstream")
            .unwrap();
        assert_eq!(slots(&counter), 1);

        drop(task);
        timeout(Duration::from_secs(5), async {
            while slots(&counter) != 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("an aborted dial must release its slot");
        holder.abort();
    }

    #[tokio::test]
    async fn activity_records_transport_reads() {
        let endpoint = upstream_sending(Message::text(ABI)).await;
        let counter = Arc::new(AtomicUsize::new(0));
        let epoch = Instant::now() - Duration::from_secs(10);
        let guard = ConnectionGuard::acquire(counter);
        let mut task = DialTask::spawn(target(&endpoint), limits(), guard, epoch);
        let established = task.outcome().await.unwrap();
        let ms = established.activity.load(Ordering::Relaxed);
        assert!(ms >= 10_000, "reads are stamped from the epoch, got {ms}");
    }

    #[test]
    fn host_port_handles_schemes_ports_and_ipv6() {
        assert_eq!(
            host_port("ws://127.0.0.1:8080").unwrap(),
            ("127.0.0.1".to_string(), 8080)
        );
        assert_eq!(
            host_port("wss://node.example").unwrap(),
            ("node.example".to_string(), 443)
        );
        assert_eq!(
            host_port("ws://[::1]:9000/path").unwrap(),
            ("::1".to_string(), 9000)
        );
        assert_eq!(host_port("ws://10.0.0.1").unwrap().1, 80);
    }

    fn hub(servers: Vec<Server>) -> Arc<FleetHub> {
        let resolved = ResolvedListener {
            name: None,
            listen_address: "127.0.0.1".to_string(),
            listen_port: 0,
            max_connections: 10,
            expected_chain_id: None,
            require_chain_id: false,
            block_failure_quarantine: None,
            topology: ResolvedTopology::Pool,
            stitching: Default::default(),
            abi_pin: None,
            servers: servers.clone(),
        };
        FleetHub::new(
            &resolved,
            build_state_db(servers.clone()),
            build_config_db(servers),
        )
    }

    #[tokio::test]
    async fn a_failed_dial_marks_a_live_upstream_offline_and_publishes_it() {
        let live = Server {
            name: "live".to_string(),
            endpoint: "127.0.0.1:1".to_string(),
            enabled: true,
            ..Default::default()
        };
        let frozen = Server {
            name: "archive".to_string(),
            endpoint: "127.0.0.1:2".to_string(),
            enabled: true,
            role: Role::Frozen,
            ..Default::default()
        };
        let hub = hub(vec![live, frozen]);
        for state in hub.state.lock().await.values_mut() {
            state.online = true;
        }
        hub.refresh().await;
        assert_eq!(hub.view().online, 2);

        mark_offline(&hub, "127.0.0.1:1").await;
        mark_offline(&hub, "127.0.0.1:2").await;
        let db = hub.state.lock().await;
        assert!(!db["127.0.0.1:1"].online);
        assert!(db["127.0.0.1:1"].offline_since.is_some());
        assert!(db["127.0.0.1:2"].online, "frozen upstreams stay online");
        drop(db);
        assert_eq!(hub.view().online, 1, "the view is republished");
    }

    fn pos(num: u32) -> BlockPos {
        BlockPos {
            num,
            id: [num as u8; 32],
        }
    }

    fn live_state(start: u32, end: u32) -> LinkState {
        let mut st = LinkState::new(0, Role::Live, Instant::now());
        st.req = Some(LinkReq {
            start,
            end,
            clamped: false,
            irreversible_only: false,
            purpose: Purpose::Initial,
            sent_at: Instant::now(),
        });
        st.up_cursor = UpCursor::Exact(end);
        st.last_lib = Some(pos(990));
        st
    }

    /// Q1: when a socket can take a new request.
    #[test]
    fn quiescence_rules() {
        let mut idle = LinkState::new(0, Role::Live, Instant::now());
        assert!(quiescent(&idle, 10, false), "no request");
        assert!(settled(&idle, false), "no request");
        idle.req = live_state(100, 200).req;
        idle.up_cursor = UpCursor::Exact(150);
        assert!(quiescent(&idle, 0, false), "C == 0: nothing in flight");
        assert!(!quiescent(&idle, 10, false), "mid-stream");
        assert!(
            !settled(&idle, false),
            "quiet only for lack of credit: an ack resumes the old request"
        );

        let mut frozen = live_state(100, 200);
        frozen.role = Role::Frozen;
        frozen.last_lib = None;
        assert!(quiescent(&frozen, 10, false), "frozen at its end");
        assert!(settled(&frozen, false), "frozen at its end");

        let at_end = live_state(100, 200);
        assert!(quiescent(&at_end, 10, false), "live, end <= LIB + 1");
        assert!(!quiescent(&at_end, 10, true), "legacy status family");
        let mut keepalives = live_state(100, 200);
        keepalives.keepalives_seen = 1;
        assert!(!quiescent(&keepalives, 10, false), "keep-alive flavour");
        let above_lib = live_state(900, 1000);
        assert!(!quiescent(&above_lib, 10, false), "end above LIB + 1");
        let at_lib = live_state(900, 991);
        assert!(quiescent(&at_lib, 10, false), "end == LIB + 1");
        let mut uncertain = live_state(100, 200);
        uncertain.up_cursor = UpCursor::Uncertain { lo: 150, hi: 200 };
        assert!(!quiescent(&uncertain, 10, false));
    }

    /// A leg is done when the upstream's known cursor reached its end; a
    /// link without a request has nothing to send either.
    #[test]
    fn a_leg_is_done_at_its_end_and_only_with_a_known_cursor() {
        assert!(LinkState::new(0, Role::Frozen, Instant::now()).leg_done());
        let mut st = live_state(100, 200);
        assert!(st.leg_done(), "at its end");
        st.up_cursor = UpCursor::Exact(199);
        assert!(!st.leg_done(), "one block owed");
        st.up_cursor = UpCursor::Uncertain { lo: 150, hi: 200 };
        assert!(!st.leg_done(), "a rewind may still come");
    }

    /// Router-made seams (handoffs and failovers) must continue the client's
    /// stream; a client's own request need not.
    #[test]
    fn handoffs_and_failovers_are_seams() {
        assert!(Purpose::Handoff.resumes());
        assert!(Purpose::Failover.resumes());
        assert!(!Purpose::Initial.resumes());
        assert!(!Purpose::Reroute.resumes());
        assert_eq!(Purpose::Handoff.label(), LinkPurpose::Handoff);
    }

    #[test]
    fn owed_needs_a_request_credit_and_a_cursor_below_end_and_gate() {
        let mut st = live_state(100, 200);
        st.up_cursor = UpCursor::Exact(150);
        st.last_head = Some(pos(149));
        assert!(!owed(&st, 0, |_| Some(1000)), "no credit");
        assert!(owed(&st, 5, |_| Some(1000)), "the fleet gate is above");
        assert!(!owed(&st, 5, |_| Some(149)), "at the tip: nothing is due");
        assert!(!owed(&st, 5, |_| None));
        st.last_head = Some(pos(150));
        assert!(owed(&st, 5, |_| None), "the link's own head is the gate");
        st.up_cursor = UpCursor::Exact(200);
        assert!(!owed(&st, 5, |_| Some(1000)), "at end");
        st.up_cursor = UpCursor::Uncertain { lo: 1, hi: 2 };
        assert!(owed(&st, 5, |_| None));
        st.role = Role::Frozen;
        st.up_cursor = UpCursor::Exact(150);
        st.last_head = None;
        assert!(owed(&st, 5, |_| None), "frozen ignores the gate");
        let mut irr = live_state(100, 200);
        irr.req.as_mut().unwrap().irreversible_only = true;
        irr.up_cursor = UpCursor::Exact(150);
        irr.last_head = Some(pos(500));
        irr.last_lib = Some(pos(149));
        assert!(!owed(&irr, 5, |lib| (!lib).then_some(1000)), "gated on LIB");
    }

    /// A resume with positions leaves the cursor uncertain. It owes blocks
    /// only if it would with every position matched.
    #[test]
    fn an_uncertain_cursor_is_owed_only_at_its_upper_bound() {
        // A satisfied range resumed with its reversible tail: start == end.
        let mut st = live_state(190, 200);
        st.up_cursor = UpCursor::Uncertain { lo: 190, hi: 200 };
        assert!(!owed(&st, 90, |_| Some(1000)), "start at end: nothing due");
        // A positions-only request (end <= start) on a fresh link.
        let mut st = live_state(150, 150);
        st.up_cursor = UpCursor::Uncertain { lo: 145, hi: 150 };
        assert!(!owed(&st, 10, |_| Some(1000)));
        // A resume at the tip: due once the fleet reaches its start.
        let mut st = live_state(1001, u32::MAX);
        st.up_cursor = UpCursor::Uncertain { lo: 991, hi: 1001 };
        st.last_head = None;
        assert!(!owed(&st, 10, |_| Some(1000)), "past the gate");
        assert!(owed(&st, 10, |_| Some(1001)), "the fleet reached the start");
        st.role = Role::Frozen;
        assert!(owed(&st, 10, |_| None), "frozen ignores the gate");
    }

    fn client_with(r: BlocksRequest, delivered: &[u32], lib: u32) -> ClientSide {
        let mut c = ClientSide::default();
        c.new_request(r);
        c.lib_sent = Some(pos(lib));
        for &n in delivered {
            c.delivered.push(pos(n), c.lib_sent);
            c.cursor = n + 1;
            c.delivered_for_request += 1;
        }
        c
    }

    fn request(irreversible_only: bool, positions: &[u32]) -> BlocksRequest {
        BlocksRequest {
            v1: true,
            start: 100,
            end: 300,
            max_in_flight: 20,
            have_positions: positions.iter().map(|&n| pos(n)).collect(),
            irreversible_only,
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: false,
            fetch_finality_data: true,
        }
    }

    const POOL: Target = Target {
        topology: ResolvedTopology::Pool,
        ids_begin: Some(60),
    };
    const STITCHED: Target = Target {
        topology: ResolvedTopology::Stitched,
        ids_begin: Some(60),
    };

    /// PO1: positions for every (role, fresh, topology, irreversible_only).
    #[test]
    fn positions_policy_for_every_combination() {
        let nums = |v: Vec<BlockPos>| v.iter().map(|p| p.num).collect::<Vec<_>>();
        for irreversible_only in [false, true] {
            let creq = request(irreversible_only, &[50, 70, 99, 150]);
            let fresh = client_with(creq.clone(), &[], 90);
            let resumed = client_with(creq.clone(), &[100, 101, 102, 103], 101);
            for target in [POOL, STITCHED] {
                for c in [&fresh, &resumed] {
                    assert!(
                        positions_for(
                            c,
                            &creq,
                            Role::Frozen,
                            c.delivered_for_request == 0,
                            100,
                            target
                        )
                        .is_empty(),
                        "frozen upstreams never get positions"
                    );
                }
                let resume = positions_for(&resumed, &creq, Role::Live, false, 104, target);
                if irreversible_only {
                    assert!(resume.is_empty(), "irreversible_only resumes need none");
                } else {
                    assert_eq!(nums(resume), vec![102, 103], "the reversible tail");
                }
            }
            assert_eq!(
                nums(positions_for(&fresh, &creq, Role::Live, true, 100, POOL)),
                vec![50, 70, 99, 150],
                "pool forwards the client's positions verbatim"
            );
            assert_eq!(
                nums(positions_for(
                    &fresh,
                    &creq,
                    Role::Live,
                    true,
                    100,
                    STITCHED
                )),
                vec![70, 99],
                "stitched keeps positions below start the upstream can know"
            );
        }
    }

    #[test]
    fn requests_are_rebuilt_from_the_client_credit_and_cursor() {
        let creq = request(false, &[95]);
        // Fresh: the client's start and positions, the current credit.
        let mut c = client_with(creq.clone(), &[], 90);
        c.ack(5);
        let mut st = LinkState::new(0, Role::Live, Instant::now());
        let req = build_request(
            &c,
            &creq,
            &mut st,
            creq.end,
            Purpose::Initial,
            POOL,
            Instant::now(),
        );
        assert_eq!(req.start, 100);
        assert_eq!(req.max_in_flight, 25);
        assert_eq!(req.have_positions, vec![pos(95)]);
        assert!(req.v1 && req.fetch_block && req.fetch_traces && req.fetch_finality_data);
        assert_eq!(st.up_cursor, UpCursor::Uncertain { lo: 95, hi: 100 });
        assert_eq!(
            st.resume_floor, None,
            "a fresh request has nothing to de-dup"
        );
        assert!(!st.req.as_ref().unwrap().clamped);
        assert!(st.first_result_pending);
        // The bytes match what a Spring client sends.
        let mock = mock_ship::client::BlocksReq {
            v1: true,
            start: 100,
            end: 300,
            max_in_flight: 25,
            have_positions: vec![(95, pos(95).id)],
            irreversible_only: false,
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: false,
            fetch_finality_data: true,
        };
        assert_eq!(
            crate::ship::wire::encode_blocks_request(&req),
            mock_ship::client::blocks(&mock)
        );

        // Resume: the cursor, the tail, a de-dup window at the cursor.
        let mut c = client_with(creq.clone(), &[100, 101, 102], 100);
        c.credit = 7;
        let req = build_request(
            &c,
            &creq,
            &mut st,
            250,
            Purpose::Failover,
            POOL,
            Instant::now(),
        );
        assert_eq!(req.start, 103);
        assert_eq!(req.end, 250);
        assert_eq!(req.max_in_flight, 7);
        assert_eq!(req.have_positions, vec![pos(101), pos(102)]);
        assert_eq!(st.up_cursor, UpCursor::Uncertain { lo: 101, hi: 103 });
        assert_eq!(st.resume_floor, Some(103));
        assert!(st.req.as_ref().unwrap().clamped);

        // A frozen resume has no positions; ids are expected from its begin.
        let mut frozen = LinkState::new(1, Role::Frozen, Instant::now());
        let req = build_request(
            &c,
            &creq,
            &mut frozen,
            250,
            Purpose::Failover,
            POOL,
            Instant::now(),
        );
        assert!(req.have_positions.is_empty());
        assert_eq!(frozen.up_cursor, UpCursor::Exact(103));
        assert_eq!(frozen.expect_ids_from, 60);
        assert_eq!(
            frozen.resume_floor,
            Some(103),
            "a failover always opens the window"
        );
        let req = build_request(
            &c,
            &creq,
            &mut frozen,
            250,
            Purpose::Reroute,
            POOL,
            Instant::now(),
        );
        assert!(req.have_positions.is_empty());
        assert_eq!(
            frozen.resume_floor, None,
            "no positions, not a failover: no window"
        );

        // A stitched handoff to a frozen upstream: the next leg from the
        // cursor, no positions, no window, and a kept link stops lingering.
        frozen.linger_until = Some(Instant::now());
        let req = build_request(
            &c,
            &creq,
            &mut frozen,
            200,
            Purpose::Handoff,
            STITCHED,
            Instant::now(),
        );
        assert_eq!((req.start, req.end, req.max_in_flight), (103, 200, 7));
        assert!(req.have_positions.is_empty());
        assert_eq!(frozen.resume_floor, None);
        assert_eq!(frozen.linger_until, None, "the link is in use again");
        assert_eq!(frozen.req.as_ref().unwrap().purpose, Purpose::Handoff);
    }
}
