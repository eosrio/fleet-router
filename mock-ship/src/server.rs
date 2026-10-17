use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use futures::{FutureExt, Sink, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify};
use tokio::time::{sleep, Sleep};
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::WebSocketStream;

use crate::abi::SHIP_ABI_JSON;
use crate::clock::ChainClock;
use crate::protocol::{
    decode_request, decode_request_strict, decode_varuint32, encode_blocks_result, encode_status,
    generate_fake_data, BlockFields, ShipRequest, StatusFields,
};

/// How closely a session follows a real SHiP node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Fidelity {
    /// The historical mock. It bursts blocks without reading the socket, never
    /// gates on head, always sends `this_block`/`prev_block` and result index 1,
    /// answers status per `emit_status_v1`, and drops the socket when a
    /// `get_blocks` starts outside a non-zero `trace_end_block`.
    #[default]
    Legacy,
    /// Spring 1.x: reads and sends interleave, results and status follow the
    /// request version, sends obey `credit > 0 && cursor <= head|LIB && cursor
    /// < end`, ids are known only inside the logs and up to head, and every
    /// result costs a credit and advances the cursor. A malformed or unknown
    /// request drops the socket without a Close frame.
    Spring,
    /// EOSIO 2.0 / Leap 3: Spring's rules, plus head-only keep-alives that cost
    /// a credit without advancing the cursor, and no v1 requests (they drop the
    /// socket).
    Eosio2,
}

/// What a session does once its cursor reaches the end of the served range.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum EndOfRange {
    /// No cap: Legacy streams past its range, Spring stops only at head.
    #[default]
    Unbounded,
    /// Send nothing at or above the served end (the highest log end), stay
    /// open, keep answering status and accept a new request.
    Hold,
}

/// One message a mock session received, in arrival order across sessions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Observed {
    /// A WebSocket upgrade completed. Sessions are numbered from 1.
    Accepted {
        session: u32,
    },
    Status {
        session: u32,
        v1: bool,
    },
    /// A get_blocks request as received, before any `have_positions` rewind.
    Blocks {
        session: u32,
        v1: bool,
        start: u32,
        end: u32,
        max_in_flight: u32,
        have_positions: Vec<(u32, [u8; 32])>,
        irreversible_only: bool,
        fetch_block: bool,
        fetch_traces: bool,
        fetch_deltas: bool,
        fetch_finality_data: bool,
    },
    Ack {
        session: u32,
        n: u32,
    },
    /// A message the mock could not decode: an unknown variant, or a known one
    /// that is malformed. `variant` is the leading varuint (`u32::MAX` when the
    /// message is empty).
    Unknown {
        session: u32,
        variant: u32,
    },
}

/// Configuration for a mock SHiP server instance.
#[derive(Debug, Clone)]
pub struct MockShipConfig {
    /// TCP port to bind to. Use 0 for a random available port.
    pub port: u16,
    /// Initial head block number. Sessions read it from the server's
    /// [`ChainClock`], which [`MockShipHandle::set_head`] moves.
    pub head_block: u32,
    /// Initial last irreversible block number.
    pub lib_block: u32,
    /// Number of blocks to stream before stopping (0 = unlimited until end_block).
    pub blocks_to_stream: u32,
    /// Optional delay between sending each block result (simulates slow upstream).
    pub block_delay: Option<Duration>,
    /// Optional: disconnect after sending N blocks (simulates upstream failure).
    pub disconnect_after: Option<u32>,
    /// 32-byte chain ID.
    pub chain_id: [u8; 32],
    /// Size of fake data payloads per block (bytes). Legacy: 0 = no data even if
    /// requested. Spring/Eosio2: a requested payload inside its log is always
    /// present, as on a real node; below 4 bytes it is 4 bytes stamped with
    /// the block number.
    pub block_data_size: usize,
    /// First block with trace data available. Default: 1.
    pub trace_begin_block: u32,
    /// One past last block with trace data. Default: 0 = use head_block + 1.
    pub trace_end_block: u32,
    /// Optional fault injected into `get_status` handling. Default: none.
    pub status_fault: Option<StatusFault>,
    /// Total `get_status` requests observed across all sessions.
    pub status_requests: Option<Arc<AtomicUsize>>,
    /// Highest number of `get_status` requests observed on any single session.
    pub max_status_per_session: Option<Arc<AtomicUsize>>,
    /// Reply to status requests with `get_status_result_v1` (includes finality range).
    /// Default false keeps the historical v0 status result. Legacy only: Spring
    /// and Eosio2 answer with the version of the request.
    pub emit_status_v1: bool,
    /// First block with finality data. Legacy reports it only when
    /// `emit_status_v1` is set.
    pub finality_data_begin_block: u32,
    /// One past the last block with finality data. Spring/Eosio2: 0 with a
    /// non-zero begin follows head + 1, like `trace_end_block`.
    pub finality_data_end_block: u32,
    /// Simulate a corrupted SHiP log entry: when this block is about to be sent,
    /// drop the TCP connection without a Close frame. This is what Spring/Leap
    /// nodeos does when zlib decompression of a damaged trace_history.log entry
    /// fails ("state history connection ... failed: zlib error: iostream error").
    pub corrupt_block: Option<u32>,
    /// Counts how many times `corrupt_block` was hit (one per aborted session).
    pub corrupt_hits: Option<Arc<AtomicUsize>>,
    /// Stop streaming when this block would be sent, and leave the socket open.
    /// Models an upstream sitting at the chain tip with nothing new to send.
    pub hold_at_block: Option<u32>,
    /// Counts sessions that reached `hold_at_block` (once per session).
    pub hold_hits: Option<Arc<AtomicUsize>>,
    /// Session behaviour. Default: [`Fidelity::Legacy`].
    pub fidelity: Fidelity,
    /// First chain-state block. `None` mirrors `trace_begin_block`.
    pub chain_state_begin_block: Option<u32>,
    /// One past the last chain-state block. `None` mirrors the trace end
    /// (including its head + 1 default).
    pub chain_state_end_block: Option<u32>,
    /// Leap 4/5: every request starts at `max(start, first block with an id)`,
    /// so the first result can jump forward.
    pub first_available_jump: bool,
    /// See [`EndOfRange`].
    pub end_of_range: EndOfRange,
    /// Fault: when block N is due, send a Close frame with this code instead.
    pub close_at_block: Option<(u32, u16)>,
    /// Fault: when block N is due, send N+1 and continue from N+2 (a gap).
    pub skip_at_block: Option<u32>,
    /// Fault: send block N without `this_block`, `prev_block` or payloads
    /// (id-less inside coverage), then advance.
    pub headonly_at_block: Option<u32>,
    /// Fault: send block N without traces even if requested.
    pub omit_traces_at_block: Option<u32>,
    /// Answer the upgrade with HTTP 503 and `Retry-After: 1` while this many
    /// sessions are open.
    pub max_sessions: Option<u32>,
    /// Override the ABI text frame.
    pub abi_text: Option<Arc<str>>,
    /// Initial fork: ids for `n >= from` use this seed.
    pub fork: Option<(u32, u8)>,
    /// Pace the server's socket writes to this many bytes per second
    /// (transport-level slowness).
    pub throttle_bytes_per_sec: Option<u32>,
}

/// How the mock treats `get_status` requests. Used to exercise monitor limits.
#[derive(Debug, Clone, Copy)]
pub enum StatusFault {
    /// Accept the request and never answer it.
    Ignore,
    /// Answer `after_replies` status requests, wait `delay`, send one binary
    /// frame of `frame_bytes`, then end the session.
    Oversized {
        after_replies: u32,
        frame_bytes: usize,
        delay: Duration,
    },
}

impl Default for MockShipConfig {
    fn default() -> Self {
        Self {
            port: 0,
            head_block: 1000,
            lib_block: 990,
            blocks_to_stream: 100,
            block_delay: None,
            disconnect_after: None,
            chain_id: [0u8; 32],
            block_data_size: 0,
            trace_begin_block: 1,
            trace_end_block: 0, // 0 = use head_block + 1
            status_fault: None,
            status_requests: None,
            max_status_per_session: None,
            emit_status_v1: false,
            finality_data_begin_block: 0,
            finality_data_end_block: 0,
            corrupt_block: None,
            corrupt_hits: None,
            hold_at_block: None,
            hold_hits: None,
            fidelity: Fidelity::Legacy,
            chain_state_begin_block: None,
            chain_state_end_block: None,
            first_available_jump: false,
            end_of_range: EndOfRange::Unbounded,
            close_at_block: None,
            skip_at_block: None,
            headonly_at_block: None,
            omit_traces_at_block: None,
            max_sessions: None,
            abi_text: None,
            fork: None,
            throttle_bytes_per_sec: None,
        }
    }
}

impl MockShipConfig {
    /// A frozen range server for `[begin, end_excl)`: Spring fidelity, head =
    /// LIB = `end_excl - 1`, trace = chain_state = `[begin, end_excl)`, ids known
    /// for the whole range, no per-request cap, and it holds open at the end.
    pub fn frozen(begin: u32, end_excl: u32) -> Self {
        let last = end_excl.saturating_sub(1);
        Self {
            fidelity: Fidelity::Spring,
            head_block: last,
            lib_block: last,
            blocks_to_stream: 0,
            trace_begin_block: begin,
            trace_end_block: end_excl,
            end_of_range: EndOfRange::Hold,
            ..Self::default()
        }
    }

    /// A live node: Spring fidelity, trace = chain_state = `[begin, head + 1)`
    /// following the clock, LIB = `head - 10`, ids known for `[begin, head]`,
    /// and no per-request cap.
    pub fn live(begin: u32, head: u32) -> Self {
        Self {
            fidelity: Fidelity::Spring,
            head_block: head,
            lib_block: head.saturating_sub(10),
            blocks_to_stream: 0,
            trace_begin_block: begin,
            trace_end_block: 0,
            ..Self::default()
        }
    }

    /// The log ranges at `head`, with Spring's finality semantics.
    fn ranges(&self, head: u32) -> Ranges {
        let trace_end = if self.trace_end_block > 0 {
            self.trace_end_block
        } else {
            head.saturating_add(1)
        };
        let trace = (self.trace_begin_block, trace_end);
        let chain_state = (
            self.chain_state_begin_block.unwrap_or(trace.0),
            self.chain_state_end_block.unwrap_or(trace.1),
        );
        let finality = match (self.finality_data_begin_block, self.finality_data_end_block) {
            (0, 0) => (0, 0),
            (begin, 0) => (begin, head.saturating_add(1)),
            range => range,
        };
        Ranges {
            trace,
            chain_state,
            finality,
        }
    }

    fn abi_message(&self) -> Message {
        Message::Text(self.abi_text.as_deref().unwrap_or(SHIP_ABI_JSON).into())
    }
}

/// `[begin, end)` per log.
#[derive(Clone, Copy, Debug)]
struct Ranges {
    trace: (u32, u32),
    chain_state: (u32, u32),
    finality: (u32, u32),
}

fn contains(range: (u32, u32), n: u32) -> bool {
    range.0 <= n && n < range.1
}

impl Ranges {
    /// The first block whose id the server knows: the lowest begin among the
    /// non-empty trace and chain-state logs.
    fn id_begin(&self) -> u32 {
        [self.trace, self.chain_state]
            .iter()
            .filter(|(begin, end)| end > begin)
            .map(|(begin, _)| *begin)
            .min()
            .unwrap_or(self.trace.0)
    }

    fn id_known(&self, n: u32, head: u32) -> bool {
        self.id_begin() <= n && n <= head
    }

    /// One past the last block of any non-empty log.
    fn served_end(&self) -> u32 {
        [self.trace, self.chain_state, self.finality]
            .iter()
            .filter(|(begin, end)| end > begin)
            .map(|(_, end)| *end)
            .max()
            .unwrap_or(0)
    }
}

fn observe_status(config: &MockShipConfig, session_status: usize) {
    if let Some(counter) = &config.status_requests {
        counter.fetch_add(1, Ordering::Relaxed);
    }
    if let Some(max) = &config.max_status_per_session {
        max.fetch_max(session_status, Ordering::Relaxed);
    }
}

/// Whether this mock should stream `start_block_num`.
///
/// A block outside the configured trace range closes the session (the proxy
/// then fails over). A `fetch_finality_data` request outside the finality
/// range is still served, so tests can see which upstream actually answered.
fn serve_blocks(config: &MockShipConfig, start_block_num: u32, fetch_finality_data: bool) -> bool {
    if fetch_finality_data
        && config.finality_data_end_block > 0
        && (start_block_num < config.finality_data_begin_block
            || start_block_num >= config.finality_data_end_block)
    {
        eprintln!(
            "[mock-ship] fetch_finality_data block {start_block_num} is outside finality range {}..{}; serving anyway",
            config.finality_data_begin_block, config.finality_data_end_block
        );
    }
    if config.trace_end_block > 0
        && (start_block_num < config.trace_begin_block || start_block_num >= config.trace_end_block)
    {
        return false;
    }
    true
}

/// State every session of one server shares.
struct Shared {
    config: MockShipConfig,
    clock: Arc<ChainClock>,
    log: std::sync::Mutex<Vec<Observed>>,
    accepted: AtomicU32,
    /// Sessions holding a slot (`max_sessions`).
    active: AtomicU32,
    control: SessionControl,
}

impl Shared {
    fn new(config: MockShipConfig) -> Arc<Self> {
        let clock = Arc::new(ChainClock::new(
            config.head_block,
            config.lib_block,
            config.fork,
        ));
        Arc::new(Self {
            config,
            clock,
            log: std::sync::Mutex::new(Vec::new()),
            accepted: AtomicU32::new(0),
            active: AtomicU32::new(0),
            control: SessionControl::new(),
        })
    }

    fn record(&self, observed: Observed) {
        self.log
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(observed);
    }

    /// Take a `max_sessions` slot, or `None` when the server is full.
    fn admit(&self) -> Option<Slot<'_>> {
        let max = self.config.max_sessions.unwrap_or(u32::MAX);
        self.active
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < max).then_some(n + 1)
            })
            .ok()
            .map(|_| Slot(&self.active))
    }

    fn status_fields(&self, spring: bool) -> StatusFields {
        let config = &self.config;
        let head = self.clock.head();
        let lib = self.clock.lib();
        let ranges = config.ranges(head);
        StatusFields {
            head: (head, self.clock.id(head)),
            lib: (lib, self.clock.id(lib)),
            trace: ranges.trace,
            chain_state: ranges.chain_state,
            chain_id: config.chain_id,
            finality: if spring {
                ranges.finality
            } else {
                (
                    config.finality_data_begin_block,
                    config.finality_data_end_block,
                )
            },
        }
    }
}

/// A held `max_sessions` slot, released on drop.
struct Slot<'a>(&'a AtomicU32);

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn busy_response() -> ErrorResponse {
    let mut response = ErrorResponse::new(Some("mock-ship: max_sessions reached".to_string()));
    *response.status_mut() = http::StatusCode::SERVICE_UNAVAILABLE;
    response.headers_mut().insert(
        http::header::RETRY_AFTER,
        http::HeaderValue::from_static("1"),
    );
    response
}

/// A mock SHiP WebSocket server for testing fleet-router.
///
/// Implements the Spring v1.2.2 SHiP protocol:
/// 1. Accepts WebSocket connection
/// 2. Sends ABI JSON as Text frame
/// 3. Switches to binary and handles requests
pub struct MockShipServer {
    listener: TcpListener,
    shared: Arc<Shared>,
    /// The actual bound address (useful when port=0)
    addr: SocketAddr,
}

impl MockShipServer {
    /// Create and bind a new mock server. Does NOT start accepting connections.
    pub async fn new(config: MockShipConfig) -> Self {
        let bind_addr = format!("127.0.0.1:{}", config.port);
        let listener = TcpListener::bind(&bind_addr)
            .await
            .unwrap_or_else(|e| panic!("Failed to bind mock SHiP server to {}: {}", bind_addr, e));
        let addr = listener.local_addr().unwrap();
        Self {
            listener,
            shared: Shared::new(config),
            addr,
        }
    }

    /// Get the WebSocket URL for this server (e.g., "ws://127.0.0.1:12345").
    pub fn ws_url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Get the bound address.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Get the endpoint string (e.g., "127.0.0.1:12345").
    pub fn endpoint(&self) -> String {
        self.addr.to_string()
    }

    /// Accept and handle a single connection, then return.
    /// This is the simplest mode for unit tests.
    pub async fn handle_one_connection(&self) {
        let (stream, _peer) = self.listener.accept().await.expect("Failed to accept");
        let stream = Throttled::new(stream, self.shared.config.throttle_bytes_per_sec);
        let ws_stream = tokio_tungstenite::accept_async(stream)
            .await
            .expect("Failed WebSocket handshake");
        run_session(&self.shared, ws_stream).await;
    }

    /// Start accepting connections in a loop. Returns a handle to stop it.
    /// Each connection is handled in a spawned task.
    pub fn start(self) -> MockShipHandle {
        let shared = self.shared.clone();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let connection_count = Arc::new(Mutex::new(0u32));
        let count_clone = connection_count.clone();
        let shared_for_loop = shared.clone();

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = self.listener.accept() => {
                        match result {
                            Ok((stream, _peer)) => {
                                if shared_for_loop.control.stall.load(Ordering::Acquire) {
                                    // Hold the TCP socket open and never complete the
                                    // WebSocket handshake, so a peer blocks in connect
                                    // instead of seeing connection refused.
                                    tokio::spawn(async move {
                                        let _held = stream;
                                        std::future::pending::<()>().await;
                                    });
                                    continue;
                                }
                                // Sessions accepted before a later drop are the ones it kills.
                                let epoch = shared_for_loop.control.epoch.load(Ordering::Acquire);
                                let shared = shared_for_loop.clone();
                                let count = count_clone.clone();
                                tokio::spawn(async move {
                                    {
                                        let mut c = count.lock().await;
                                        *c += 1;
                                    }
                                    tokio::select! {
                                        _ = wait_until_killed(&shared.control, epoch) => {}
                                        _ = serve_connection(&shared, stream) => {}
                                    }
                                    {
                                        let mut c = count.lock().await;
                                        *c -= 1;
                                    }
                                });
                            }
                            Err(e) => {
                                eprintln!("[mock-ship] Accept error: {}", e);
                                break;
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        break;
                    }
                }
            }
        });

        MockShipHandle {
            shutdown_tx: Some(shutdown_tx),
            join_handle: Some(handle),
            connection_count,
            shared,
        }
    }
}

/// Upgrade one accepted TCP connection, honouring `max_sessions` and the
/// write throttle, and run its session.
async fn serve_connection(shared: &Shared, stream: TcpStream) {
    let stream = Throttled::new(stream, shared.config.throttle_bytes_per_sec);
    let slot = shared.admit();
    let admitted = slot.is_some();
    // The error type is fixed by tungstenite's handshake callback.
    #[allow(clippy::result_large_err)]
    let callback = move |_request: &Request, response: Response| {
        if admitted {
            Ok(response)
        } else {
            Err(busy_response())
        }
    };
    let Ok(ws_stream) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
        return;
    };
    run_session(shared, ws_stream).await;
    drop(slot);
}

async fn run_session<S>(shared: &Shared, ws_stream: WebSocketStream<S>)
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let session = shared.accepted.fetch_add(1, Ordering::AcqRel) + 1;
    shared.record(Observed::Accepted { session });
    match shared.config.fidelity {
        Fidelity::Legacy => legacy_session(shared, session, ws_stream).await,
        Fidelity::Spring | Fidelity::Eosio2 => {
            let (writer, reader) = ws_stream.split();
            SpringSession::new(shared, session)
                .run(writer, reader)
                .await;
        }
    }
}

/// The historical session, kept byte-for-byte: it bursts blocks without
/// reading the socket and reads requests only once credits or the range run
/// out. Positions are parsed, acks wrap, and every request is recorded.
async fn legacy_session<S>(
    shared: &Shared,
    session: u32,
    ws_stream: tokio_tungstenite::WebSocketStream<S>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let config = &shared.config;
    let clock = &shared.clock;
    let (mut writer, mut reader) = ws_stream.split();
    let close_generation = shared.control.close_generation();

    // Step 1: Send ABI as Text frame
    if writer.send(config.abi_message()).await.is_err() {
        return;
    }

    // Step 2: Handle binary requests
    let mut send_credits: u32 = 0;
    let mut current_block: Option<u32> = None;
    let mut end_block: u32 = 0;
    let mut blocks_sent: u32 = 0;
    let mut fetch_block = false;
    let mut fetch_traces = false;
    let mut fetch_deltas = false;
    let mut session_status: usize = 0;
    let mut status_replies: u32 = 0;
    let mut held = false;

    loop {
        // If we have credits and an active block request, send blocks
        if send_credits > 0 {
            if let Some(block_num) = current_block {
                let inside_range = config.end_of_range == EndOfRange::Unbounded
                    || block_num < config.ranges(clock.head()).served_end();
                if block_num < end_block
                    && inside_range
                    && (config.blocks_to_stream == 0 || blocks_sent < config.blocks_to_stream)
                {
                    // Check disconnect_after
                    if let Some(limit) = config.disconnect_after {
                        if blocks_sent >= limit {
                            return; // Simulate upstream failure
                        }
                    }

                    if config.corrupt_block == Some(block_num) {
                        if let Some(hits) = &config.corrupt_hits {
                            hits.fetch_add(1, Ordering::Relaxed);
                        }
                        return; // abrupt drop, no Close frame (nodeos zlib error)
                    }

                    if let Some((at, code)) = config.close_at_block {
                        if at == block_num {
                            close_with(&mut writer, &mut reader, code).await;
                            return;
                        }
                    }

                    if config.hold_at_block == Some(block_num) {
                        if !held {
                            held = true;
                            if let Some(hits) = &config.hold_hits {
                                hits.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        // Stay open with nothing to send. Zeroing credits
                        // keeps a client ack from releasing the hold or
                        // counting it again.
                        send_credits = 0;
                    } else {
                        // Optional delay
                        if let Some(delay) = config.block_delay {
                            sleep(delay).await;
                        }

                        let skip = config.skip_at_block == Some(block_num);
                        let num = if skip {
                            block_num.saturating_add(1)
                        } else {
                            block_num
                        };
                        let idless = !skip && config.headonly_at_block == Some(block_num);
                        let payload = |offset: u32, wanted: bool| {
                            (wanted && !idless && config.block_data_size > 0).then(|| {
                                generate_fake_data(num.wrapping_add(offset), config.block_data_size)
                            })
                        };
                        let block_data = payload(0, fetch_block);
                        let traces_data = payload(
                            1000,
                            fetch_traces && config.omit_traces_at_block != Some(num),
                        );
                        let deltas_data = payload(2000, fetch_deltas);

                        let head = clock.head();
                        let lib = clock.lib();
                        let prev = num.saturating_sub(1);
                        let result = encode_blocks_result(&BlockFields {
                            v1: false,
                            head: (head, clock.id(head)),
                            lib: (lib, clock.id(lib)),
                            this_block: (!idless).then(|| (num, clock.id(num))),
                            prev_block: (!idless).then(|| (prev, clock.id(prev))),
                            block: block_data.as_deref(),
                            traces: traces_data.as_deref(),
                            deltas: deltas_data.as_deref(),
                            finality: None,
                        });
                        if shared.control.close_requested(close_generation) {
                            let _ = writer.send(Message::Close(None)).await;
                            return;
                        }
                        if writer.send(Message::Binary(result.into())).await.is_err() {
                            return;
                        }

                        current_block = Some(num.saturating_add(1));
                        send_credits -= 1;
                        blocks_sent += 1;
                        continue; // Try to send more before reading
                    }
                }
            }
        }

        // Read next request from client, unless a clean close is requested
        // first (as nodeos does when it shuts down).
        let next = tokio::select! {
            next = reader.next() => next,
            () = wait_for_close(&shared.control, close_generation) => {
                let _ = writer.send(Message::Close(None)).await;
                return;
            }
        };
        let msg = match next {
            Some(Ok(msg)) => msg,
            Some(Err(_)) | None => return,
        };

        match msg {
            Message::Binary(data) => {
                let request = decode_request(&data);
                match request {
                    ShipRequest::GetStatusV0 | ShipRequest::GetStatusV1 => {
                        shared.record(Observed::Status {
                            session,
                            v1: request == ShipRequest::GetStatusV1,
                        });
                        session_status += 1;
                        observe_status(config, session_status);
                        if matches!(config.status_fault, Some(StatusFault::Ignore)) {
                            continue;
                        }

                        let result =
                            encode_status(config.emit_status_v1, &shared.status_fields(false));
                        if writer.send(Message::Binary(result.into())).await.is_err() {
                            return;
                        }
                        status_replies += 1;
                        if let Some(StatusFault::Oversized {
                            after_replies,
                            frame_bytes,
                            delay,
                        }) = config.status_fault
                        {
                            if status_replies >= after_replies {
                                if !delay.is_zero() {
                                    sleep(delay).await;
                                }
                                let frame = vec![0xFFu8; frame_bytes];
                                let _ = writer.send(Message::Binary(frame.into())).await;
                                return;
                            }
                        }
                    }
                    ShipRequest::GetBlocksV0 { .. } | ShipRequest::GetBlocksV1 { .. } => {
                        let Some(get) = GetBlocks::from_request(request) else {
                            continue;
                        };
                        shared.record(get.observed(session));
                        let mut start = get.start;
                        if config.first_available_jump {
                            start = start.max(config.ranges(clock.head()).id_begin());
                        }
                        if !serve_blocks(config, start, get.fetch_finality_data) {
                            return;
                        }
                        current_block = Some(start);
                        end_block = get.end;
                        send_credits = get.max_in_flight;
                        blocks_sent = 0;
                        fetch_block = get.fetch_block;
                        fetch_traces = get.fetch_traces;
                        fetch_deltas = get.fetch_deltas;
                    }
                    ShipRequest::GetBlocksAckV0 { num_messages } => {
                        shared.record(Observed::Ack {
                            session,
                            n: num_messages,
                        });
                        send_credits = send_credits.wrapping_add(num_messages);
                    }
                    ShipRequest::Unknown(idx) => {
                        shared.record(Observed::Unknown {
                            session,
                            variant: idx,
                        });
                        eprintln!("[mock-ship] Unknown request variant: {}", idx);
                    }
                }
            }
            Message::Close(_) => return,
            _ => {} // Ignore other message types
        }
    }
}

/// A decoded get_blocks request (v0 or v1).
struct GetBlocks {
    v1: bool,
    start: u32,
    end: u32,
    max_in_flight: u32,
    have_positions: Vec<(u32, [u8; 32])>,
    irreversible_only: bool,
    fetch_block: bool,
    fetch_traces: bool,
    fetch_deltas: bool,
    fetch_finality_data: bool,
}

impl GetBlocks {
    fn from_request(request: ShipRequest) -> Option<Self> {
        match request {
            ShipRequest::GetBlocksV0 {
                start_block_num,
                end_block_num,
                max_messages_in_flight,
                have_positions,
                irreversible_only,
                fetch_block,
                fetch_traces,
                fetch_deltas,
            } => Some(Self {
                v1: false,
                start: start_block_num,
                end: end_block_num,
                max_in_flight: max_messages_in_flight,
                have_positions,
                irreversible_only,
                fetch_block,
                fetch_traces,
                fetch_deltas,
                fetch_finality_data: false,
            }),
            ShipRequest::GetBlocksV1 {
                start_block_num,
                end_block_num,
                max_messages_in_flight,
                have_positions,
                irreversible_only,
                fetch_block,
                fetch_traces,
                fetch_deltas,
                fetch_finality_data,
            } => Some(Self {
                v1: true,
                start: start_block_num,
                end: end_block_num,
                max_in_flight: max_messages_in_flight,
                have_positions,
                irreversible_only,
                fetch_block,
                fetch_traces,
                fetch_deltas,
                fetch_finality_data,
            }),
            _ => None,
        }
    }

    fn observed(&self, session: u32) -> Observed {
        Observed::Blocks {
            session,
            v1: self.v1,
            start: self.start,
            end: self.end,
            max_in_flight: self.max_in_flight,
            have_positions: self.have_positions.clone(),
            irreversible_only: self.irreversible_only,
            fetch_block: self.fetch_block,
            fetch_traces: self.fetch_traces,
            fetch_deltas: self.fetch_deltas,
            fetch_finality_data: self.fetch_finality_data,
        }
    }
}

/// The request a Spring session is serving. The cursor and credit live on
/// the session, because acks and fork rewinds change them independently.
struct Current {
    v1: bool,
    end: u32,
    irreversible_only: bool,
    fetch_block: bool,
    fetch_traces: bool,
    fetch_deltas: bool,
    fetch_finality_data: bool,
}

/// What a Spring session sends next.
enum Due {
    Block(u32),
    KeepAlive,
}

/// A Spring (or EOSIO 2.0) session: one current request, a credit counter
/// and a cursor, mirroring `session.hpp`.
struct SpringSession<'a> {
    shared: &'a Shared,
    session: u32,
    eosio2: bool,
    current: Option<Current>,
    cursor: u32,
    credit: u32,
    blocks_sent: u32,
    /// EOSIO 2.0 sends one head-only result after each get_blocks and each new
    /// block while it has credit and nothing to send. Set by those events,
    /// cleared by the next result sent.
    keepalive_due: bool,
    held: bool,
    seen_generation: u64,
    seen_forks: usize,
    status_seen: usize,
    status_replies: u32,
}

impl<'a> SpringSession<'a> {
    fn new(shared: &'a Shared, session: u32) -> Self {
        let clock = &shared.clock;
        Self {
            shared,
            session,
            eosio2: shared.config.fidelity == Fidelity::Eosio2,
            current: None,
            cursor: 0,
            credit: 0,
            blocks_sent: 0,
            keepalive_due: false,
            held: false,
            seen_generation: clock.generation(),
            seen_forks: clock.forks_since(0).1,
            status_seen: 0,
            status_replies: 0,
        }
    }

    async fn run<W, R>(mut self, mut writer: W, mut reader: R)
    where
        W: Sink<Message, Error = WsError> + Unpin,
        R: Stream<Item = Result<Message, WsError>> + Unpin,
    {
        if writer.send(self.shared.config.abi_message()).await.is_err() {
            return;
        }
        let clock = self.shared.clock.clone();
        let control = &self.shared.control;
        let close_generation = control.close_generation();
        loop {
            let changed = clock.notified();
            self.sync_clock();
            if control.close_requested(close_generation) {
                let _ = writer.send(Message::Close(None)).await;
                return;
            }
            if let Some(due) = self.due() {
                // Spring reads on its own coroutine: requests already waiting
                // (status, acks, a new get_blocks) are handled before the
                // next result goes out.
                if let Some(item) = reader.next().now_or_never() {
                    if !self.on_client(item, &mut writer).await {
                        return;
                    }
                    continue;
                }
                if !self.send(due, &mut writer, &mut reader).await {
                    return;
                }
                continue;
            }
            tokio::select! {
                item = reader.next() => {
                    if !self.on_client(item, &mut writer).await {
                        return;
                    }
                }
                _ = changed => {}
                () = wait_for_close(control, close_generation) => {}
            }
        }
    }

    /// Apply clock changes: fork rewinds (`block_applied`) and EOSIO 2.0
    /// keep-alive triggers.
    fn sync_clock(&mut self) {
        let clock = &self.shared.clock;
        let generation = clock.generation();
        if generation == self.seen_generation {
            return;
        }
        self.seen_generation = generation;
        let (fork_points, total) = clock.forks_since(self.seen_forks);
        self.seen_forks = total;
        for from in fork_points {
            if self.cursor > from {
                self.cursor = from;
            }
        }
        if self.current.is_some() {
            self.keepalive_due = true;
        }
    }

    fn due(&self) -> Option<Due> {
        let current = self.current.as_ref()?;
        if self.credit == 0 {
            return None;
        }
        let config = &self.shared.config;
        let clock = &self.shared.clock;
        let head = clock.head();
        let gate = if current.irreversible_only {
            clock.lib()
        } else {
            head
        };
        if self.cursor <= gate && self.cursor < current.end {
            let capped = (config.blocks_to_stream != 0
                && self.blocks_sent >= config.blocks_to_stream)
                || (config.end_of_range == EndOfRange::Hold
                    && self.cursor >= config.ranges(head).served_end());
            return (!capped).then_some(Due::Block(self.cursor));
        }
        (self.eosio2 && self.keepalive_due).then_some(Due::KeepAlive)
    }

    /// Handle one item from the client. `false` ends the session.
    async fn on_client<W>(&mut self, item: Option<Result<Message, WsError>>, writer: &mut W) -> bool
    where
        W: Sink<Message, Error = WsError> + Unpin,
    {
        let data = match item {
            Some(Ok(Message::Binary(data))) => data,
            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return false,
            Some(Ok(_)) => return true,
        };
        let request = match decode_request_strict(&data) {
            Ok(request) => request,
            Err(_) => {
                // fc::raw::unpack throws and Spring drops the socket, no Close frame.
                let variant = if data.is_empty() {
                    u32::MAX
                } else {
                    decode_varuint32(&data).0
                };
                self.shared.record(Observed::Unknown {
                    session: self.session,
                    variant,
                });
                return false;
            }
        };
        match request {
            ShipRequest::GetStatusV0 => self.on_status(false, writer).await,
            ShipRequest::GetStatusV1 => self.on_status(true, writer).await,
            ShipRequest::GetBlocksAckV0 { num_messages } => {
                self.shared.record(Observed::Ack {
                    session: self.session,
                    n: num_messages,
                });
                self.credit = self.credit.wrapping_add(num_messages);
                true
            }
            ShipRequest::Unknown(variant) => {
                self.shared.record(Observed::Unknown {
                    session: self.session,
                    variant,
                });
                false
            }
            blocks => match GetBlocks::from_request(blocks) {
                Some(get) => self.on_blocks(get),
                None => false,
            },
        }
    }

    async fn on_status<W>(&mut self, v1: bool, writer: &mut W) -> bool
    where
        W: Sink<Message, Error = WsError> + Unpin,
    {
        let config = &self.shared.config;
        self.shared.record(Observed::Status {
            session: self.session,
            v1,
        });
        self.status_seen += 1;
        observe_status(config, self.status_seen);
        if self.eosio2 && v1 {
            return false; // EOSIO 2.0 has no get_status_request_v1
        }
        if matches!(config.status_fault, Some(StatusFault::Ignore)) {
            return true;
        }
        let reply = encode_status(v1, &self.shared.status_fields(true));
        if writer.send(Message::Binary(reply.into())).await.is_err() {
            return false;
        }
        self.status_replies += 1;
        if let Some(StatusFault::Oversized {
            after_replies,
            frame_bytes,
            delay,
        }) = config.status_fault
        {
            if self.status_replies >= after_replies {
                if !delay.is_zero() {
                    sleep(delay).await;
                }
                let frame = vec![0xFFu8; frame_bytes];
                let _ = writer.send(Message::Binary(frame.into())).await;
                return false;
            }
        }
        true
    }

    /// A new get_blocks replaces the request, the credit and the cursor.
    fn on_blocks(&mut self, get: GetBlocks) -> bool {
        self.shared.record(get.observed(self.session));
        if self.eosio2 && get.v1 {
            return false; // EOSIO 2.0 has no get_blocks_request_v1
        }
        let config = &self.shared.config;
        let clock = &self.shared.clock;
        let head = clock.head();
        let ranges = config.ranges(head);
        // Spring: a position below start whose id is unknown or different
        // moves start down to it; the positions are then cleared.
        let mut start = get.start;
        for &(num, id) in &get.have_positions {
            if start <= num {
                continue;
            }
            if !ranges.id_known(num, head) || clock.id(num) != id {
                start = num;
            }
        }
        if config.first_available_jump {
            start = start.max(ranges.id_begin());
        }
        self.cursor = start;
        self.credit = get.max_in_flight;
        self.blocks_sent = 0;
        self.keepalive_due = true;
        self.current = Some(Current {
            v1: get.v1,
            end: get.end,
            irreversible_only: get.irreversible_only,
            fetch_block: get.fetch_block,
            fetch_traces: get.fetch_traces,
            fetch_deltas: get.fetch_deltas,
            fetch_finality_data: get.fetch_finality_data,
        });
        true
    }

    /// Send what [`due`](Self::due) chose. `false` ends the session.
    async fn send<W, R>(&mut self, due: Due, writer: &mut W, reader: &mut R) -> bool
    where
        W: Sink<Message, Error = WsError> + Unpin,
        R: Stream<Item = Result<Message, WsError>> + Unpin,
    {
        let n = match due {
            Due::KeepAlive => {
                let frame = self.keepalive_frame();
                self.credit -= 1;
                self.keepalive_due = false;
                return writer.send(Message::Binary(frame.into())).await.is_ok();
            }
            Due::Block(n) => n,
        };
        let config = &self.shared.config;
        if let Some(limit) = config.disconnect_after {
            if self.blocks_sent >= limit {
                return false;
            }
        }
        if config.corrupt_block == Some(n) {
            if let Some(hits) = &config.corrupt_hits {
                hits.fetch_add(1, Ordering::Relaxed);
            }
            return false;
        }
        if let Some((at, code)) = config.close_at_block {
            if at == n {
                close_with(writer, reader, code).await;
                return false;
            }
        }
        if config.hold_at_block == Some(n) {
            if !self.held {
                self.held = true;
                if let Some(hits) = &config.hold_hits {
                    hits.fetch_add(1, Ordering::Relaxed);
                }
            }
            self.credit = 0;
            return true;
        }
        if let Some(delay) = config.block_delay {
            sleep(delay).await;
        }
        let (num, idless) = if config.skip_at_block == Some(n) {
            (n.saturating_add(1), false)
        } else {
            (n, config.headonly_at_block == Some(n))
        };
        let frame = self.block_frame(num, idless);
        if writer.send(Message::Binary(frame.into())).await.is_err() {
            return false;
        }
        self.cursor = num.saturating_add(1);
        self.credit -= 1;
        self.blocks_sent += 1;
        self.keepalive_due = false;
        true
    }

    /// The result for block `num`. Ids are known inside the logs up to head;
    /// each payload is present when requested and `num` is inside its log.
    fn block_frame(&self, num: u32, idless: bool) -> Vec<u8> {
        let config = &self.shared.config;
        let clock = &self.shared.clock;
        let current = self
            .current
            .as_ref()
            .expect("a block is due only with a request");
        let head = clock.head();
        let lib = clock.lib();
        let ranges = config.ranges(head);
        let known = !idless && ranges.id_known(num, head);
        let size = config.block_data_size.max(4);
        let payload = |offset: u32, wanted: bool| {
            (known && wanted).then(|| generate_fake_data(num.wrapping_add(offset), size))
        };
        let block = payload(0, current.fetch_block);
        let traces = payload(
            1000,
            current.fetch_traces
                && contains(ranges.trace, num)
                && config.omit_traces_at_block != Some(num),
        );
        let deltas = payload(
            2000,
            current.fetch_deltas && contains(ranges.chain_state, num),
        );
        let finality = payload(
            3000,
            current.v1 && current.fetch_finality_data && contains(ranges.finality, num),
        );
        encode_blocks_result(&BlockFields {
            v1: current.v1,
            head: (head, clock.id(head)),
            lib: (lib, clock.id(lib)),
            this_block: known.then(|| (num, clock.id(num))),
            prev_block: (known && num > 1).then(|| (num - 1, clock.id(num - 1))),
            block: block.as_deref(),
            traces: traces.as_deref(),
            deltas: deltas.as_deref(),
            finality: finality.as_deref(),
        })
    }

    /// An EOSIO 2.0 keep-alive: head and LIB only.
    fn keepalive_frame(&self) -> Vec<u8> {
        let clock = &self.shared.clock;
        let head = clock.head();
        let lib = clock.lib();
        encode_blocks_result(&BlockFields {
            head: (head, clock.id(head)),
            lib: (lib, clock.id(lib)),
            ..BlockFields::default()
        })
    }
}

/// Send a Close frame with `code` and wait briefly for the peer's reply, so
/// the closing handshake completes before the socket drops.
async fn close_with<W, R>(writer: &mut W, reader: &mut R, code: u16)
where
    W: Sink<Message, Error = WsError> + Unpin,
    R: Stream<Item = Result<Message, WsError>> + Unpin,
{
    let frame = CloseFrame {
        code: CloseCode::from(code),
        reason: "".into(),
    };
    if writer.send(Message::Close(Some(frame))).await.is_err() {
        return;
    }
    let _ = tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(Ok(msg)) = reader.next().await {
            if msg.is_close() {
                break;
            }
        }
    })
    .await;
}

struct SessionControl {
    /// Set by [`MockShipHandle::stall_and_drop_sessions`]; never cleared.
    stall: AtomicBool,
    /// Bumped by every drop. A session dies once the epoch moves past the
    /// one it was accepted under.
    epoch: AtomicU64,
    kill: Notify,
    /// Bumped by [`MockShipHandle::close_sessions`]. A session closes when the
    /// value differs from the one it started with.
    close_epoch: AtomicU64,
    close: Notify,
}

impl SessionControl {
    fn new() -> Self {
        Self {
            stall: AtomicBool::new(false),
            epoch: AtomicU64::new(0),
            kill: Notify::new(),
            close_epoch: AtomicU64::new(0),
            close: Notify::new(),
        }
    }

    fn drop_sessions(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.kill.notify_waiters();
    }

    /// The close generation a session starts under.
    fn close_generation(&self) -> u64 {
        self.close_epoch.load(Ordering::Acquire)
    }

    /// Whether a clean close was requested after `generation`.
    fn close_requested(&self, generation: u64) -> bool {
        self.close_epoch.load(Ordering::Acquire) != generation
    }

    fn close_sessions(&self) {
        self.close_epoch.fetch_add(1, Ordering::AcqRel);
        self.close.notify_waiters();
    }
}

/// Completes once a clean close was requested after `generation`. Registers
/// interest before the check, like [`wait_until_killed`].
async fn wait_for_close(control: &SessionControl, generation: u64) {
    loop {
        let notified = control.close.notified();
        if control.close_requested(generation) {
            return;
        }
        notified.await;
    }
}

/// Completes once a drop happened after this session was accepted under
/// `epoch`, or once the server stalls.
///
/// `notify_waiters` is observed by a `Notified` future as soon as that future
/// is created, so registering interest before the flag check does not miss a
/// kill that lands in between.
async fn wait_until_killed(control: &SessionControl, epoch: u64) {
    loop {
        let notified = control.kill.notified();
        if control.stall.load(Ordering::Acquire) || control.epoch.load(Ordering::Acquire) != epoch {
            return;
        }
        notified.await;
    }
}

/// Handle for a running mock server. Drop to stop.
pub struct MockShipHandle {
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    join_handle: Option<tokio::task::JoinHandle<()>>,
    connection_count: Arc<Mutex<u32>>,
    shared: Arc<Shared>,
}

impl MockShipHandle {
    /// Get the current number of active connections.
    pub async fn connection_count(&self) -> u32 {
        *self.connection_count.lock().await
    }

    /// Drop sessions that are already running, and accept further TCP
    /// connections without completing a WebSocket handshake.
    ///
    /// Used to force an upstream write failure while keeping the listen port
    /// open so a reconnect blocks instead of failing immediately.
    pub fn stall_and_drop_sessions(&self) {
        self.shared.control.stall.store(true, Ordering::Release);
        self.shared.control.drop_sessions();
    }

    /// Drop every running session without a Close frame (a crash), and keep
    /// accepting new connections normally.
    pub fn drop_sessions(&self) {
        self.shared.control.drop_sessions();
    }

    /// Send a WebSocket Close on every open session, as nodeos does when it
    /// shuts down cleanly. New connections are still accepted, like a node
    /// that has restarted.
    pub fn close_sessions(&self) {
        self.shared.control.close_sessions();
    }

    /// WebSocket upgrades completed so far (refused or stalled ones excluded).
    pub fn sessions_accepted(&self) -> u32 {
        self.shared.accepted.load(Ordering::Acquire)
    }

    /// Every message received so far, across sessions, in arrival order.
    pub fn requests(&self) -> Vec<Observed> {
        self.shared
            .log
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// The server's chain clock.
    pub fn clock(&self) -> Arc<ChainClock> {
        self.shared.clock.clone()
    }

    /// Move head and LIB. Wakes sessions: under Spring they may now send,
    /// under Eosio2 an idle session with credit sends a keep-alive.
    pub fn set_head(&self, head: u32, lib: u32) {
        self.shared.clock.set(head, lib);
    }

    /// Add `n` to head and LIB.
    pub fn advance(&self, n: u32) {
        self.shared.clock.advance(n);
    }

    /// Ids of blocks `>= from` change to branch `seed`. Spring and Eosio2
    /// sessions whose cursor is past `from` rewind to it, which re-delivers
    /// the forked blocks.
    pub fn fork(&self, from: u32, seed: u8) {
        self.shared.clock.fork(from, seed);
    }

    /// Gracefully shut down the server.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.join_handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for MockShipHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Run one SHiP session on an already-upgraded WebSocket.
///
/// [`MockShipServer::start`] accepts plaintext TCP. Tests that terminate TLS
/// themselves upgrade the socket, then hand the WebSocket here.
pub async fn serve_websocket<S>(
    config: MockShipConfig,
    ws_stream: tokio_tungstenite::WebSocketStream<S>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    run_session(&Shared::new(config), ws_stream).await;
}

/// A stream whose writes are paced to an average byte rate. Reads pass through.
struct Throttled<S> {
    inner: S,
    bytes_per_sec: Option<u32>,
    /// Armed after each write for the time those bytes cost.
    pause: Option<Pin<Box<Sleep>>>,
}

impl<S> Throttled<S> {
    fn new(inner: S, bytes_per_sec: Option<u32>) -> Self {
        Self {
            inner,
            bytes_per_sec: bytes_per_sec.filter(|rate| *rate > 0),
            pause: None,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Throttled<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Throttled<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let Some(rate) = this.bytes_per_sec else {
            return Pin::new(&mut this.inner).poll_write(cx, buf);
        };
        if let Some(pause) = this.pause.as_mut() {
            ready!(pause.as_mut().poll(cx));
            this.pause = None;
        }
        // At most 50 ms worth per write, so the pace is smooth.
        let chunk = buf.len().min((rate as usize / 20).max(1));
        let written = ready!(Pin::new(&mut this.inner).poll_write(cx, &buf[..chunk]))?;
        let cost = Duration::from_secs_f64(written as f64 / f64::from(rate));
        this.pause = Some(Box::pin(sleep(cost)));
        Poll::Ready(Ok(written))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}
