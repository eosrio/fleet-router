//! A mock HTTP chain API, for testing a router's chain API listener.
//!
//! [`MockChainApi::live`] plays a nodeos live node. It answers `get_info`
//! from its head and LIB, the five block endpoints for blocks from
//! `earliest_available_block_num` up to head, and any other call it lists
//! with a tagged answer ([`call_answer`]).
//!
//! [`MockChainApi::frozen`] plays a frozen range server's chain API. It
//! answers `/v1/archive/describe` (any method, body ignored), and
//! `get_block_info`, `get_block_header` and `get_raw_block` for the blocks of
//! its block log. Every other target gets nodeos's 404.
//!
//! Both handle a request the way nodeos does (Spring 1.1.0 to 1.2.2):
//! 1. the request-target check;
//! 2. the Host check (live only, nodeos's `http-validate-host`);
//! 3. OPTIONS answered `200 {}`;
//! 4. an exact lookup of the whole target, query string included.
//!
//! Parameters follow fc's rules for the bodies a frozen range server also
//! reads, and every error body is nodeos's, byte for byte. A block the mock
//! holds gets a deterministic body tagged with the mock's name
//! ([`block_answer`]).
//!
//! Responses carry `Content-Type: application/json`, `Content-Length`
//! framing and title-case header names, and no `Date`. Every request is
//! recorded ([`ApiRequest`]). [`Fault`]s inject delays, 503s, dropped
//! connections and canned answers per target.
//!
//! **Block ids** are nodeos-shaped: the first four bytes hold the block
//! number big-endian ([`block_id`]), so a router reads the same block from an
//! id as nodeos does. The SHiP mock's ids keep the number little-endian. A
//! frozen API therefore reports `last` with the SHiP mock's id of that block
//! ([`fake_block_id`]), so a router can verify it against a
//! [`MockShipConfig::frozen`](crate::MockShipConfig::frozen) server.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::header::{HeaderName, HeaderValue, CONTENT_TYPE, HOST, SERVER};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode, Uri};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Notify};
use tokio::time::{sleep, timeout};

use crate::clock::ChainClock;
use crate::protocol::{fake_block_id, fake_block_id_seeded};

const DESCRIBE: &str = "/v1/archive/describe";
const SUPPORTED_APIS: &str = "/v1/node/get_supported_apis";
const GET_INFO: &str = "/v1/chain/get_info";

/// What a frozen range server serves: the block endpoints that are pure
/// functions of the stored block.
const FROZEN_APIS: [&str; 3] = [
    "/v1/chain/get_block_header",
    "/v1/chain/get_block_info",
    "/v1/chain/get_raw_block",
];

/// What a live mock serves unless [`MockChainApi::apis`] says otherwise: the
/// chain API of a node with the read-write calls enabled.
const LIVE_APIS: [&str; 19] = [
    "/v1/chain/compute_transaction",
    "/v1/chain/get_abi",
    "/v1/chain/get_account",
    "/v1/chain/get_block",
    "/v1/chain/get_block_header",
    "/v1/chain/get_block_header_state",
    "/v1/chain/get_block_info",
    "/v1/chain/get_currency_balance",
    "/v1/chain/get_info",
    "/v1/chain/get_producers",
    "/v1/chain/get_raw_abi",
    "/v1/chain/get_raw_block",
    "/v1/chain/get_table_rows",
    "/v1/chain/push_block",
    "/v1/chain/push_transaction",
    "/v1/chain/push_transactions",
    "/v1/chain/send_read_only_transaction",
    "/v1/chain/send_transaction",
    "/v1/chain/send_transaction2",
];

/// nodeos reads request headers for at most this long (the router's default
/// handshake timeout).
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// nodeos's answer to a request-target that is not origin-form.
const ILLEGAL_TARGET: &str = r#"{"code":400,"message":"Illegal request-target","error":{"code":0,"name":"","what":"","details":[]}}"#;

/// nodeos's answer to a `Host` it does not serve.
const DISALLOWED_HOST: &str = r#"{"code":400,"message":"Disallowed HTTP HOST header in the request","error":{"code":0,"name":"","what":"","details":[]}}"#;

/// nodeos's answer to an unknown or disabled endpoint.
pub const NOT_FOUND_BODY: &str = r#"{"code":404,"message":"Not Found","error":{"code":0,"name":"exception","what":"unspecified","details":[{"message":"Unknown Endpoint","file":"beast_http_session.hpp","line_number":186,"method":"handle_request"}]}}"#;

/// Which upstream API the mock plays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiRole {
    /// A nodeos live node.
    Live,
    /// A frozen range server.
    Frozen,
}

/// A fault injected into every request with a given target. See
/// [`MockChainApiHandle::set_fault`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Wait this long before answering, then answer as usual. Longer than the
    /// router's upstream timeout, it is an answer that never comes in time.
    Delay(Duration),
    /// Answer 503 Busy, in the frozen range server's shape.
    Busy,
    /// Close the connection after the request head, without reading the
    /// body and without an answer.
    DropBeforeBody,
    /// Read the whole request, then close the connection without an answer:
    /// the request was delivered, the answer is lost.
    DropAfterBody,
    /// Answer a block endpoint with its Unknown Block error, even for a block
    /// the mock holds.
    UnknownBlock,
    /// Answer this status and body instead.
    Respond { status: u16, body: Bytes },
}

/// One request the mock received. It is recorded once its body was read, or
/// once its head was for [`Fault::DropBeforeBody`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApiRequest {
    pub method: String,
    /// The raw request-target, query string included.
    pub target: String,
    /// The `Host` header.
    pub host: Option<String>,
    /// Every header, with lowercase names.
    pub headers: Vec<(String, String)>,
    /// The body, byte for byte. Empty when [`Fault::DropBeforeBody`] closed
    /// the connection before reading it.
    pub body: Vec<u8>,
}

impl ApiRequest {
    /// The first header with this name, ignoring case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }
}

/// A mock chain API. Build one with [`live`](Self::live) or
/// [`frozen`](Self::frozen), adjust the public fields, then
/// [`start`](Self::start) it.
#[derive(Clone, Debug)]
pub struct MockChainApi {
    /// TCP port on 127.0.0.1. 0 picks a free port.
    pub port: u16,
    /// Tags every block and call answer (`served_by`).
    pub name: String,
    pub role: ApiRole,
    /// Reported by `get_info` (live) and `describe` (frozen).
    pub chain_id: [u8; 32],
    /// Live: `server_version_string` of `get_info`. Frozen: `server` of
    /// `describe`.
    pub version: String,
    /// Live: initial head, unless `clock` is set.
    pub head: u32,
    /// Live: initial LIB, unless `clock` is set.
    pub lib: u32,
    /// Live: follow this clock instead of `head` and `lib`. Pass a SHiP
    /// mock's `MockShipHandle::clock()` to move both together; ids then
    /// follow its forks.
    pub clock: Option<Arc<ChainClock>>,
    /// Live: `earliest_available_block_num`. Blocks from it up to head are
    /// served. `None` omits the field, as EOSIO 2.0 does, and serves from
    /// block 1.
    pub earliest: Option<u32>,
    /// Frozen: the blocks served, `[begin, end)`.
    pub blocks: (u32, u32),
    /// Frozen: `describe.last`, the SHiP head of the server, and the id
    /// `get_block_info` reports for that block.
    pub last: (u32, [u8; 32]),
    /// Frozen: `describe.max_in_flight`. A block request beyond it gets 503
    /// at once (the server's queue wait is not modelled).
    pub max_in_flight: u32,
    /// The paths served and listed by `get_supported_apis` (and `describe`).
    /// `None` is the role's own set. A listed path with no handler of its
    /// own gets [`call_answer`].
    pub apis: Option<Vec<String>>,
    /// Whether `/v1/node/get_supported_apis` exists. Without it the answer is
    /// the 404, as from a node that predates it.
    pub supported_apis: bool,
    /// nodeos's `http-validate-host`. A `Host` naming an IP address must name
    /// the address the connection arrived on (the port is not checked); a
    /// host name is refused, since the mock has no `http-alias`. The answer
    /// is 400 "Disallowed HTTP HOST header in the request".
    pub validate_host: bool,
    /// The `Server` header, on every answer. `None` sends none.
    pub server_header: Option<String>,
    /// Headers added to every answer after the Host check, the way nodeos
    /// adds its configured `Access-Control-*` headers.
    pub extra_headers: Vec<(String, String)>,
}

impl MockChainApi {
    /// A live node at `head`/`lib` serving blocks from `earliest` (block 1
    /// when `None`, which also omits `earliest_available_block_num`).
    /// `version` is its `server_version_string`, such as `v1.2.2`. It
    /// validates `Host` and sends `Server: nodeos/<version>`.
    pub fn live(
        name: &str,
        chain_id: [u8; 32],
        head: u32,
        lib: u32,
        earliest: Option<u32>,
        version: &str,
    ) -> Self {
        Self {
            port: 0,
            name: name.to_string(),
            role: ApiRole::Live,
            chain_id,
            version: version.to_string(),
            head,
            lib,
            clock: None,
            earliest,
            blocks: (0, 0),
            last: (0, [0; 32]),
            max_in_flight: u32::MAX,
            apis: None,
            supported_apis: true,
            validate_host: true,
            server_header: Some(format!("nodeos/{version}")),
            extra_headers: Vec::new(),
        }
    }

    /// A frozen range server's chain API serving `blocks` (`[begin, end)`).
    /// `last` is its SHiP head; its id is the SHiP mock's id of that block,
    /// `fake_block_id(last)`, so a router verifies this API against a
    /// [`MockShipConfig::frozen`](crate::MockShipConfig::frozen) server that
    /// ends at `last + 1`. It does not validate `Host`.
    pub fn frozen(
        name: &str,
        chain_id: [u8; 32],
        blocks: (u32, u32),
        last: u32,
        max_in_flight: u32,
    ) -> Self {
        let version = format!("mock-frozen-api/{}", env!("CARGO_PKG_VERSION"));
        Self {
            port: 0,
            name: name.to_string(),
            role: ApiRole::Frozen,
            chain_id,
            server_header: Some(version.clone()),
            version,
            head: 0,
            lib: 0,
            clock: None,
            earliest: None,
            blocks,
            last: (last, fake_block_id(last)),
            max_in_flight,
            apis: None,
            supported_apis: true,
            validate_host: false,
            extra_headers: Vec::new(),
        }
    }

    /// Bind 127.0.0.1:`port` and serve until the handle is shut down or
    /// dropped.
    pub async fn start(self) -> MockChainApiHandle {
        let bind = format!("127.0.0.1:{}", self.port);
        let listener = TcpListener::bind(&bind)
            .await
            .unwrap_or_else(|e| panic!("Failed to bind mock chain API to {bind}: {e}"));
        let addr = listener.local_addr().unwrap();
        let shared = Arc::new(Shared::new(self));
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let accepting = shared.clone();
        let join = tokio::spawn(async move {
            loop {
                tokio::select! {
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _peer)) => {
                            tokio::spawn(serve_connection(accepting.clone(), stream));
                        }
                        Err(e) => {
                            // A peer that resets before the accept completes
                            // must not stop the mock.
                            eprintln!("[mock-chain-api] accept error: {e}");
                            sleep(Duration::from_millis(10)).await;
                        }
                    },
                    _ = &mut shutdown_rx => break,
                }
            }
        });
        MockChainApiHandle {
            addr,
            shared,
            shutdown_tx: Some(shutdown_tx),
            join: Some(join),
        }
    }
}

/// A running mock chain API. Dropping it stops the mock.
pub struct MockChainApiHandle {
    addr: SocketAddr,
    shared: Arc<Shared>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    join: Option<tokio::task::JoinHandle<()>>,
}

impl MockChainApiHandle {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// `127.0.0.1:<port>`.
    pub fn endpoint(&self) -> String {
        self.addr.to_string()
    }

    /// `http://127.0.0.1:<port>`, the server's `api_endpoint`.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn name(&self) -> &str {
        &self.shared.name
    }

    /// Every request received so far, in arrival order.
    pub fn requests(&self) -> Vec<ApiRequest> {
        self.shared.log().clone()
    }

    /// The requests received for exactly this target.
    pub fn requests_to(&self, target: &str) -> Vec<ApiRequest> {
        self.shared
            .log()
            .iter()
            .filter(|r| r.target == target)
            .cloned()
            .collect()
    }

    pub fn clear_requests(&self) {
        self.shared.log().clear();
    }

    /// TCP connections accepted so far.
    pub fn connections_accepted(&self) -> u32 {
        self.shared.accepted.load(Ordering::Acquire)
    }

    /// Block requests being answered now. A frozen API counts only the ones
    /// it admitted.
    pub fn in_flight(&self) -> u32 {
        self.shared.in_flight.load(Ordering::Acquire)
    }

    /// The most block requests ever answered at once.
    pub fn peak_in_flight(&self) -> u32 {
        self.shared.peak_in_flight.load(Ordering::Acquire)
    }

    /// Block requests a frozen API refused at `max_in_flight`.
    pub fn rejected(&self) -> u32 {
        self.shared.rejected.load(Ordering::Acquire)
    }

    /// The clock behind a live API's head, LIB and ids.
    pub fn clock(&self) -> Arc<ChainClock> {
        self.shared.clock.clone()
    }

    /// Move a live API's head and LIB (and a SHiP mock sharing the clock).
    pub fn set_head(&self, head: u32, lib: u32) {
        self.shared.clock.set(head, lib);
    }

    /// Add `n` to a live API's head and LIB.
    pub fn advance(&self, n: u32) {
        self.shared.clock.advance(n);
    }

    /// Report another chain id from now on.
    pub fn set_chain_id(&self, chain_id: [u8; 32]) {
        self.shared.state().chain_id = chain_id;
    }

    /// Live: change `earliest_available_block_num` and the first block
    /// served.
    pub fn set_earliest(&self, earliest: Option<u32>) {
        self.shared.state().earliest = earliest;
    }

    /// Frozen: change the blocks served, `[begin, end)`.
    pub fn set_blocks(&self, begin: u32, end: u32) {
        self.shared.state().blocks = (begin, end);
    }

    /// Frozen: change `describe.last` and the id served for that block.
    pub fn set_last(&self, num: u32, id: [u8; 32]) {
        self.shared.state().last = (num, id);
    }

    /// Frozen: while set, `get_block_info` for `last` reports an id other
    /// than `describe.last.id`, so the API fails identity verification.
    pub fn set_last_mismatch(&self, mismatch: bool) {
        self.shared.state().last_mismatch = mismatch;
    }

    /// Frozen: change `describe.max_in_flight` and the admission cap.
    pub fn set_max_in_flight(&self, max_in_flight: u32) {
        self.shared.state().max_in_flight = max_in_flight;
    }

    /// Change the paths served and listed (`None`: the role's own set).
    pub fn set_apis(&self, apis: Option<Vec<String>>) {
        self.shared.state().apis = apis;
    }

    /// Whether `/v1/node/get_supported_apis` exists.
    pub fn set_supported_apis(&self, supported: bool) {
        self.shared.state().supported_apis = supported;
    }

    /// Inject `fault` into every request for `target`, or into every request
    /// when `target` is `None`. A fault on the exact target wins over the
    /// one for every request. `None` removes the fault.
    pub fn set_fault(&self, target: Option<&str>, fault: Option<Fault>) {
        let mut state = self.shared.state();
        let key = target.map(str::to_string);
        state.faults.retain(|(t, _)| *t != key);
        if let Some(fault) = fault {
            state.faults.push((key, fault));
        }
    }

    pub fn clear_faults(&self) {
        self.shared.state().faults.clear();
    }

    /// Close every open connection (in-flight requests get no answer) and
    /// keep accepting new ones.
    pub fn drop_connections(&self) {
        self.shared.kill.kill_all();
    }

    /// Stop accepting, close every connection and free the port.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        self.shared.kill.kill_all();
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }
}

impl Drop for MockChainApiHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
            self.shared.kill.kill_all();
        }
    }
}

/// The body of a block the mock holds, from any block endpoint:
/// `{"block_num":N,"id":"<id>","served_by":"<name>"}`.
pub fn block_answer(served_by: &str, block_num: u32, id: &[u8; 32]) -> Vec<u8> {
    format!(
        r#"{{"block_num":{block_num},"id":"{}","served_by":{}}}"#,
        hex(id),
        json_string(served_by)
    )
    .into_bytes()
}

/// The body of any other call the mock serves:
/// `{"target":"<target>","body_bytes":N,"served_by":"<name>"}`.
pub fn call_answer(served_by: &str, target: &str, body_bytes: usize) -> Vec<u8> {
    format!(
        r#"{{"target":{},"body_bytes":{body_bytes},"served_by":{}}}"#,
        json_string(target),
        json_string(served_by)
    )
    .into_bytes()
}

/// The nodeos-shaped id of block `n` on the main branch: `fake_block_id(n)`
/// with the block number in its first four bytes big-endian.
pub fn block_id(n: u32) -> [u8; 32] {
    nodeos_id(fake_block_id(n), n)
}

/// The nodeos-shaped id of block `n` on branch `seed` (0 is the main
/// branch), as a live API whose clock forked to `seed` reports it.
pub fn block_id_seeded(n: u32, seed: u8) -> [u8; 32] {
    nodeos_id(fake_block_id_seeded(n, seed), n)
}

fn nodeos_id(mut id: [u8; 32], n: u32) -> [u8; 32] {
    id[..4].copy_from_slice(&n.to_be_bytes());
    id
}

/// State that tests change while the mock runs.
struct State {
    chain_id: [u8; 32],
    earliest: Option<u32>,
    blocks: (u32, u32),
    last: (u32, [u8; 32]),
    last_mismatch: bool,
    max_in_flight: u32,
    apis: Option<Vec<String>>,
    supported_apis: bool,
    faults: Vec<(Option<String>, Fault)>,
}

/// Everything the connections of one mock share.
struct Shared {
    name: String,
    role: ApiRole,
    version: String,
    validate_host: bool,
    server_header: Option<HeaderValue>,
    extra_headers: Vec<(HeaderName, HeaderValue)>,
    clock: Arc<ChainClock>,
    state: Mutex<State>,
    log: Mutex<Vec<ApiRequest>>,
    in_flight: AtomicU32,
    peak_in_flight: AtomicU32,
    rejected: AtomicU32,
    accepted: AtomicU32,
    kill: Kill,
}

impl Shared {
    fn new(config: MockChainApi) -> Self {
        let MockChainApi {
            port: _,
            name,
            role,
            chain_id,
            version,
            head,
            lib,
            clock,
            earliest,
            blocks,
            last,
            max_in_flight,
            apis,
            supported_apis,
            validate_host,
            server_header,
            extra_headers,
        } = config;
        let server_header = server_header.map(|value| {
            HeaderValue::from_str(&value)
                .unwrap_or_else(|e| panic!("invalid mock server_header {value:?}: {e}"))
        });
        let extra_headers = extra_headers
            .into_iter()
            .map(|(name, value)| {
                let header = HeaderName::from_bytes(name.as_bytes())
                    .unwrap_or_else(|e| panic!("invalid mock header name {name:?}: {e}"));
                let value = HeaderValue::from_str(&value)
                    .unwrap_or_else(|e| panic!("invalid mock header value {value:?}: {e}"));
                (header, value)
            })
            .collect();
        Self {
            name,
            role,
            version,
            validate_host,
            server_header,
            extra_headers,
            clock: clock.unwrap_or_else(|| Arc::new(ChainClock::new(head, lib, None))),
            state: Mutex::new(State {
                chain_id,
                earliest,
                blocks,
                last,
                last_mismatch: false,
                max_in_flight,
                apis,
                supported_apis,
                faults: Vec::new(),
            }),
            log: Mutex::new(Vec::new()),
            in_flight: AtomicU32::new(0),
            peak_in_flight: AtomicU32::new(0),
            rejected: AtomicU32::new(0),
            accepted: AtomicU32::new(0),
            kill: Kill::new(),
        }
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn log(&self) -> MutexGuard<'_, Vec<ApiRequest>> {
        self.log.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn fault_for(&self, target: &str) -> Option<Fault> {
        let state = self.state();
        let find = |key: Option<&str>| {
            state
                .faults
                .iter()
                .find(|(t, _)| t.as_deref() == key)
                .map(|(_, f)| f.clone())
        };
        find(Some(target)).or_else(|| find(None))
    }

    fn role_apis(&self) -> &'static [&'static str] {
        match self.role {
            ApiRole::Live => &LIVE_APIS,
            ApiRole::Frozen => &FROZEN_APIS,
        }
    }

    fn serves(&self, state: &State, target: &str) -> bool {
        match &state.apis {
            Some(apis) => apis.iter().any(|path| path == target),
            None => self.role_apis().contains(&target),
        }
    }

    /// The served paths, sorted by bytes.
    fn listed(&self, state: &State) -> BTreeSet<String> {
        match &state.apis {
            Some(apis) => apis.iter().cloned().collect(),
            None => self.role_apis().iter().map(|p| p.to_string()).collect(),
        }
    }

    /// Whether this request is a block request the mock answers: those are
    /// counted in flight, and a frozen API admits them up to its cap.
    fn is_block_request(&self, method: &Method, target: &str) -> bool {
        method != Method::OPTIONS
            && BlockEp::from_target(target).is_some()
            && self.serves(&self.state(), target)
    }

    fn holds(&self, state: &State, n: u32) -> bool {
        match self.role {
            ApiRole::Live => state.earliest.unwrap_or(1) <= n && n <= self.clock.head(),
            ApiRole::Frozen => state.blocks.0 <= n && n < state.blocks.1,
        }
    }

    fn id_of(&self, state: &State, n: u32) -> [u8; 32] {
        match self.role {
            ApiRole::Live => nodeos_id(self.clock.id(n), n),
            ApiRole::Frozen if n == state.last.0 => {
                let mut id = state.last.1;
                if state.last_mismatch {
                    for byte in &mut id[4..] {
                        *byte ^= 0xff;
                    }
                }
                id
            }
            ApiRole::Frozen => block_id(n),
        }
    }

    /// The block `query` names, if the mock holds it: by number, or by an id
    /// whose number (big-endian, as nodeos reads it) names a held block with
    /// exactly that id. A frozen API also finds `last` by its SHiP id.
    fn find(&self, state: &State, query: &Query) -> Option<(u32, [u8; 32])> {
        let n = match *query {
            Query::Num(n) => n,
            Query::Id(id) if self.role == ApiRole::Frozen && id == state.last.1 => state.last.0,
            Query::Id(id) => u32::from_be_bytes([id[0], id[1], id[2], id[3]]),
        };
        if !self.holds(state, n) {
            return None;
        }
        let id = self.id_of(state, n);
        match *query {
            Query::Id(want) if want != id => None,
            _ => Some((n, id)),
        }
    }

    /// Admit a block request: always for a live API, up to `max_in_flight`
    /// for a frozen one. `None` is a refusal.
    fn enter(self: &Arc<Self>) -> Option<InFlight> {
        let cap = match self.role {
            ApiRole::Live => u32::MAX,
            ApiRole::Frozen => self.state().max_in_flight,
        };
        let admitted = self
            .in_flight
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < cap).then_some(n + 1)
            });
        match admitted {
            Ok(before) => {
                self.peak_in_flight.fetch_max(before + 1, Ordering::AcqRel);
                Some(InFlight(self.clone()))
            }
            Err(_) => {
                self.rejected.fetch_add(1, Ordering::AcqRel);
                None
            }
        }
    }

    /// 503 Busy, naming the block requests in flight.
    fn busy(&self) -> Answer {
        let what = format!(
            "Too many requests in flight: {}",
            self.in_flight.load(Ordering::Acquire)
        );
        Answer::new(503, busy_body(&what))
    }

    /// Steps 3 and 4 of the request handling: OPTIONS is answered first,
    /// then the exact target is dispatched.
    fn dispatch(&self, method: &Method, target: &str, body: &[u8], unknown: bool) -> Answer {
        if method == Method::OPTIONS {
            return Answer::new(200, "{}");
        }
        let state = self.state();
        if self.role == ApiRole::Frozen && target == DESCRIBE {
            return Answer::new(200, self.describe(&state));
        }
        if target == SUPPORTED_APIS {
            if !state.supported_apis {
                return Answer::new(404, NOT_FOUND_BODY);
            }
            return Answer::new(
                200,
                format!(r#"{{"apis":{}}}"#, json_list(&self.listed(&state))),
            );
        }
        if !self.serves(&state, target) {
            return Answer::new(404, NOT_FOUND_BODY);
        }
        if let Some(ep) = BlockEp::from_target(target) {
            return self.block(&state, ep, body, unknown);
        }
        if self.role == ApiRole::Live && target == GET_INFO {
            return self.get_info(&state, body);
        }
        Answer::new(200, call_answer(&self.name, target, body.len()))
    }

    fn block(&self, state: &State, ep: BlockEp, body: &[u8], unknown: bool) -> Answer {
        let request = match parse_block_request(ep, body) {
            Ok(request) => request,
            Err(e) => {
                let (status, body) = e.answer();
                return Answer::new(status, body);
            }
        };
        match self.find(state, &request.query).filter(|_| !unknown) {
            Some((n, id)) => Answer::new(200, block_answer(&self.name, n, &id)),
            None => Answer::new(
                400,
                unknown_block_body(ep, request.include_extensions, &request.echo),
            ),
        }
    }

    /// nodeos's `get_info` result, in its reflection order, plus
    /// `served_by`. `get_info` takes no parameters: a body gets the parse
    /// error.
    fn get_info(&self, state: &State, body: &[u8]) -> Answer {
        if !is_empty_content(body) {
            let (status, body) = ParamError::Unparsable.answer();
            return Answer::new(status, body);
        }
        let head = self.clock.head();
        let lib = self.clock.lib();
        let head_id = hex(&nodeos_id(self.clock.id(head), head));
        let lib_id = hex(&nodeos_id(self.clock.id(lib), lib));
        let earliest = state
            .earliest
            .map(|e| format!(r#","earliest_available_block_num":{e}"#))
            .unwrap_or_default();
        let body = format!(
            concat!(
                r#"{{"server_version":"00000000","chain_id":"{chain_id}","#,
                r#""head_block_num":{head},"last_irreversible_block_num":{lib},"#,
                r#""last_irreversible_block_id":"{lib_id}","head_block_id":"{head_id}","#,
                r#""head_block_time":"{head_time}","head_block_producer":"eosio","#,
                r#""virtual_block_cpu_limit":200000000,"virtual_block_net_limit":1048576000,"#,
                r#""block_cpu_limit":200000,"block_net_limit":1048576,"#,
                r#""server_version_string":{version},"fork_db_head_block_num":{head},"#,
                r#""fork_db_head_block_id":"{head_id}","server_full_version_string":{full}"#,
                r#"{earliest},"last_irreversible_block_time":"{lib_time}","served_by":{name}}}"#,
            ),
            chain_id = hex(&state.chain_id),
            head = head,
            lib = lib,
            lib_id = lib_id,
            head_id = head_id,
            head_time = block_time(head),
            version = json_string(&self.version),
            full = json_string(&format!("{}-mock", self.version)),
            earliest = earliest,
            lib_time = block_time(lib),
            name = json_string(&self.name),
        );
        Answer::new(200, body)
    }

    /// The frozen range server's self-description.
    fn describe(&self, state: &State) -> String {
        format!(
            concat!(
                r#"{{"server":{},"chain_id":"{}","last":{{"num":{},"id":"{}"}},"#,
                r#""blocks":{{"begin":{},"end":{}}},"apis":{},"get_block":"off","#,
                r#""max_in_flight":{},"in_flight":{}}}"#,
            ),
            json_string(&self.version),
            hex(&state.chain_id),
            state.last.0,
            hex(&state.last.1),
            state.blocks.0,
            state.blocks.1,
            json_list(&self.listed(state)),
            state.max_in_flight,
            self.in_flight.load(Ordering::Acquire),
        )
    }

    fn response(&self, answer: Answer) -> Response<Full<Bytes>> {
        let mut response = Response::new(Full::new(answer.body));
        *response.status_mut() =
            StatusCode::from_u16(answer.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let headers = response.headers_mut();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(server) = &self.server_header {
            headers.insert(SERVER, server.clone());
        }
        if answer.extra_headers {
            for (name, value) in &self.extra_headers {
                headers.append(name.clone(), value.clone());
            }
        }
        response
    }
}

/// An admitted block request; leaving drops the count.
struct InFlight(Arc<Shared>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Closes connections on demand: each connection remembers the epoch it was
/// accepted under and ends once the epoch moves.
struct Kill {
    epoch: AtomicU64,
    notify: Notify,
}

impl Kill {
    fn new() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    fn kill_all(&self) {
        self.epoch.fetch_add(1, Ordering::AcqRel);
        self.notify.notify_waiters();
    }

    /// Completes once the epoch moved past `epoch`. The `Notified` is created
    /// before the check, so a kill in between is not missed.
    async fn wait(&self, epoch: u64) {
        loop {
            let notified = self.notify.notified();
            if self.epoch() != epoch {
                return;
            }
            notified.await;
        }
    }
}

/// What the mock answers, before it becomes a response.
struct Answer {
    status: u16,
    body: Bytes,
    /// Whether the configured extra headers apply. nodeos adds them only
    /// after the target and Host checks.
    extra_headers: bool,
}

impl Answer {
    fn new(status: u16, body: impl Into<Bytes>) -> Self {
        Self {
            status,
            body: body.into(),
            extra_headers: true,
        }
    }

    /// An answer written before nodeos adds its configured headers.
    fn early(status: u16, body: impl Into<Bytes>) -> Self {
        Self {
            extra_headers: false,
            ..Self::new(status, body)
        }
    }
}

/// Serve one connection: HTTP/1.1, keep-alive, no `Date`, title-case names.
async fn serve_connection(shared: Arc<Shared>, stream: TcpStream) {
    shared.accepted.fetch_add(1, Ordering::AcqRel);
    let epoch = shared.kill.epoch();
    let local = stream.local_addr().ok().map(|addr| addr.ip());
    let _ = stream.set_nodelay(true);
    let service = {
        let shared = shared.clone();
        service_fn(move |request| handle(shared.clone(), local, request))
    };
    let mut builder = http1::Builder::new();
    builder
        .timer(TokioTimer::new())
        .header_read_timeout(HEADER_READ_TIMEOUT)
        .auto_date_header(false)
        .title_case_headers(true)
        .keep_alive(true);
    let connection = builder.serve_connection(TokioIo::new(stream), service);
    tokio::select! {
        _ = connection => {}
        _ = shared.kill.wait(epoch) => {}
    }
}

/// Answer one request. An `Err` makes hyper close the connection without an
/// answer, which is how the drop faults work.
async fn handle(
    shared: Arc<Shared>,
    local: Option<IpAddr>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, io::Error> {
    let (parts, body) = request.into_parts();
    let target = parts.uri.to_string();
    let fault = shared.fault_for(&target);
    let mut record = ApiRequest {
        method: parts.method.to_string(),
        target: target.clone(),
        host: parts.headers.get(HOST).map(lossy),
        headers: parts
            .headers
            .iter()
            .map(|(name, value)| (name.as_str().to_string(), lossy(value)))
            .collect(),
        body: Vec::new(),
    };
    if fault == Some(Fault::DropBeforeBody) {
        shared.log().push(record);
        return Err(dropped());
    }
    let body = body.collect().await.map_err(io::Error::other)?.to_bytes();
    record.body = body.to_vec();
    shared.log().push(record);
    if fault == Some(Fault::DropAfterBody) {
        return Err(dropped());
    }

    let _in_flight = if shared.is_block_request(&parts.method, &target) {
        match shared.enter() {
            Some(guard) => Some(guard),
            None => return Ok(shared.response(shared.busy())),
        }
    } else {
        None
    };
    match &fault {
        Some(Fault::Delay(delay)) => sleep(*delay).await,
        Some(Fault::Busy) => return Ok(shared.response(shared.busy())),
        Some(Fault::Respond { status, body }) => {
            return Ok(shared.response(Answer::new(*status, body.clone())));
        }
        _ => {}
    }
    let answer = match origin_target(&parts.uri) {
        None => Answer::early(400, ILLEGAL_TARGET),
        Some(_) if shared.validate_host && !host_allowed(parts.headers.get(HOST), local) => {
            Answer::early(400, DISALLOWED_HOST)
        }
        Some(target) => shared.dispatch(
            &parts.method,
            target,
            &body,
            fault == Some(Fault::UnknownBlock),
        ),
    };
    Ok(shared.response(answer))
}

fn dropped() -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, "mock chain API fault")
}

fn lossy(value: &HeaderValue) -> String {
    String::from_utf8_lossy(value.as_bytes()).into_owned()
}

/// The request-target as nodeos sees it: origin-form (no scheme, no
/// authority), starting with `/` and with no `..` anywhere.
fn origin_target(uri: &Uri) -> Option<&str> {
    if uri.scheme().is_some() || uri.authority().is_some() {
        return None;
    }
    let target = uri.path_and_query()?.as_str();
    (target.starts_with('/') && !target.contains("..")).then_some(target)
}

/// nodeos's `host_is_valid` with no `http-alias`: the host must be an IP
/// address equal to the one the connection arrived on. The port is not
/// checked.
fn host_allowed(host: Option<&HeaderValue>, local: Option<IpAddr>) -> bool {
    let Some(host) = host.and_then(|h| h.to_str().ok()).filter(|h| !h.is_empty()) else {
        return false;
    };
    let name = match host.strip_prefix('[') {
        Some(rest) => rest.split(']').next().unwrap_or(rest),
        None => match host.rsplit_once(':') {
            Some((name, _port)) if !name.contains(':') => name,
            _ => host,
        },
    };
    match (name.parse::<IpAddr>(), local) {
        (Ok(ip), Some(local)) => ip.to_canonical() == local.to_canonical(),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// nodeos parameter rules and error bodies
// ---------------------------------------------------------------------------

/// A block endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BlockEp {
    Block,
    BlockHeader,
    BlockHeaderState,
    BlockInfo,
    RawBlock,
}

impl BlockEp {
    fn from_target(target: &str) -> Option<Self> {
        Some(match target {
            "/v1/chain/get_block" => BlockEp::Block,
            "/v1/chain/get_block_header" => BlockEp::BlockHeader,
            "/v1/chain/get_block_header_state" => BlockEp::BlockHeaderState,
            "/v1/chain/get_block_info" => BlockEp::BlockInfo,
            "/v1/chain/get_raw_block" => BlockEp::RawBlock,
            _ => return None,
        })
    }

    /// The handler that throws this endpoint's errors: `get_block` looks the
    /// block up through `get_raw_block`.
    fn method(self) -> &'static str {
        match self {
            BlockEp::Block | BlockEp::RawBlock => "get_raw_block",
            BlockEp::BlockHeader => "get_block_header",
            BlockEp::BlockHeaderState => "get_block_header_state",
            BlockEp::BlockInfo => "get_block_info",
        }
    }
}

/// A block, by number or by id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Query {
    Num(u32),
    Id([u8; 32]),
}

/// A block request whose parameters parsed.
#[derive(Clone, Debug, PartialEq, Eq)]
struct BlockRequest {
    query: Query,
    include_extensions: bool,
    /// The parameter as error messages echo it: fc's string of
    /// `block_num_or_id`, cleaned ([`echo_text`]), or the u32 `block_num`.
    echo: String,
}

/// A parameter error, thrown before any block lookup.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ParamError {
    /// An empty body on an endpoint that requires parameters.
    BodyRequired,
    /// A body that does not parse or convert.
    Unparsable,
    /// `block_num_or_id` empty or longer than 64 bytes.
    BadLength(BlockEp),
    /// `block_num_or_id` neither a number nor a valid id.
    BadId {
        ep: BlockEp,
        include_extensions: bool,
        echo: String,
    },
}

/// One nodeos exception type, as its error body names it.
struct Exception {
    status: u16,
    message: &'static str,
    code: u64,
    name: &'static str,
    what: &'static str,
}

const INVALID_REQUEST: Exception = Exception {
    status: 400,
    message: "Invalid Request",
    code: 3_200_006,
    name: "invalid_http_request",
    what: "invalid http request",
};

const UNKNOWN_BLOCK: Exception = Exception {
    status: 400,
    message: "Unknown Block",
    code: 3_100_002,
    name: "unknown_block_exception",
    what: "Unknown block",
};

const INVALID_BLOCK_ID: Exception = Exception {
    status: 500,
    message: "Internal Service Error",
    code: 3_010_008,
    name: "block_id_type_exception",
    what: "Invalid block ID",
};

/// A non-verbose nodeos error body with its one detail.
fn exception_body(e: &Exception, detail: &str, file: &str, line: u32, method: &str) -> Vec<u8> {
    format!(
        concat!(
            r#"{{"code":{},"message":"{}","error":{{"code":{},"name":"{}","what":"{}","#,
            r#""details":[{{"message":{},"file":"{}","line_number":{},"method":"{}"}}]}}}}"#,
        ),
        e.status,
        e.message,
        e.code,
        e.name,
        e.what,
        json_string(detail),
        file,
        line,
        method,
    )
    .into_bytes()
}

impl ParamError {
    /// Status and body, with Spring 1.1.0-1.2.2's file and line numbers. The
    /// length assert spans several lines; its first line is used.
    fn answer(&self) -> (u16, Vec<u8>) {
        let body = match self {
            ParamError::BodyRequired => exception_body(
                &INVALID_REQUEST,
                "A Request body is required",
                "http_plugin.hpp",
                257,
                "parse_params",
            ),
            ParamError::Unparsable => exception_body(
                &INVALID_REQUEST,
                "Unable to parse valid input from POST body",
                "http_plugin.hpp",
                278,
                "parse_params",
            ),
            ParamError::BadLength(ep) => exception_body(
                &INVALID_BLOCK_ID,
                "Invalid Block number or ID, must be greater than 0 and less than 65 characters",
                "chain_plugin.cpp",
                if *ep == BlockEp::BlockHeader {
                    1987
                } else {
                    1949
                },
                ep.method(),
            ),
            ParamError::BadId {
                ep,
                include_extensions,
                echo,
            } => {
                let line = match ep {
                    BlockEp::BlockHeader if *include_extensions => 2015,
                    BlockEp::BlockHeader => 2004,
                    BlockEp::BlockHeaderState => 2082,
                    _ => 1963,
                };
                exception_body(
                    &INVALID_BLOCK_ID,
                    &format!("Invalid block ID: {echo}"),
                    "chain_plugin.cpp",
                    line,
                    ep.method(),
                )
            }
        };
        let status = match self {
            ParamError::BodyRequired | ParamError::Unparsable => INVALID_REQUEST.status,
            ParamError::BadLength(_) | ParamError::BadId { .. } => INVALID_BLOCK_ID.status,
        };
        (status, body)
    }
}

/// The Unknown Block error of `ep` (status 400).
fn unknown_block_body(ep: BlockEp, include_extensions: bool, echo: &str) -> Vec<u8> {
    let (prefix, line) = match ep {
        BlockEp::BlockHeader if include_extensions => ("Could not find block header: ", 2017),
        BlockEp::BlockHeader => ("Could not find block header: ", 2006),
        BlockEp::BlockInfo => ("Could not find block: ", 2049),
        BlockEp::BlockHeaderState => ("Could not find block: ", 2085),
        BlockEp::Block | BlockEp::RawBlock => ("Could not find block: ", 1966),
    };
    exception_body(
        &UNKNOWN_BLOCK,
        &format!("{prefix}{echo}"),
        "chain_plugin.cpp",
        line,
        ep.method(),
    )
}

/// A frozen range server's 503 body.
fn busy_body(what: &str) -> String {
    format!(
        r#"{{"code":503,"message":"Busy","error":{{"code":503,"name":"Busy","what":{},"details":[]}}}}"#,
        json_string(what)
    )
}

/// nodeos's `is_empty_content`: after trimming spaces (only spaces), nothing
/// is left, or `{` and `}` with only spaces and tabs between them.
fn is_empty_content(body: &[u8]) -> bool {
    let start = body.iter().position(|b| *b != b' ');
    let end = body.iter().rposition(|b| *b != b' ');
    let (Some(start), Some(end)) = (start, end) else {
        return true;
    };
    let trimmed = &body[start..=end];
    trimmed.len() > 1
        && trimmed[0] == b'{'
        && trimmed[trimmed.len() - 1] == b'}'
        && trimmed[1..trimmed.len() - 1]
            .iter()
            .all(|b| *b == b' ' || *b == b'\t')
}

/// The parameters of a block request, as nodeos reads them: fc's parser,
/// then the struct conversion, then (except for `get_block_info`) the
/// `block_num_or_id` checks.
fn parse_block_request(ep: BlockEp, body: &[u8]) -> Result<BlockRequest, ParamError> {
    if is_empty_content(body) {
        return Err(ParamError::BodyRequired);
    }
    let params = parse_params(body).ok_or(ParamError::Unparsable)?;
    if ep == BlockEp::BlockInfo {
        let n = u32_field(field(&params, "block_num")).ok_or(ParamError::Unparsable)?;
        return Ok(BlockRequest {
            query: Query::Num(n),
            include_extensions: false,
            echo: n.to_string(),
        });
    }
    let s = text_field(field(&params, "block_num_or_id")).ok_or(ParamError::Unparsable)?;
    let include_extensions = if ep == BlockEp::BlockHeader {
        bool_field(field(&params, "include_extensions")).ok_or(ParamError::Unparsable)?
    } else {
        false
    };
    let echo = echo_text(&s);
    // get_block_header_state has no length check: its id is fc's sha256 of
    // the string.
    if ep != BlockEp::BlockHeaderState && (s.is_empty() || s.len() > 64) {
        return Err(ParamError::BadLength(ep));
    }
    let query = match to_uint64(&s) {
        Some(v) => Query::Num(v as u32),
        None => {
            let id = if ep == BlockEp::BlockHeaderState {
                sha256_id(&s)
            } else {
                hex_id(&s)
            };
            Query::Id(id.ok_or_else(|| ParamError::BadId {
                ep,
                include_extensions,
                echo: echo.clone(),
            })?)
        }
    };
    Ok(BlockRequest {
        query,
        include_extensions,
        echo,
    })
}

/// boost `lexical_cast<uint64_t>`: one optional sign, then digits only. A
/// negative value wraps (`-1` is `u64::MAX`).
fn to_uint64(s: &[u8]) -> Option<u64> {
    let (negative, digits) = match s.first()? {
        b'+' => (false, &s[1..]),
        b'-' => (true, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let value: u64 = std::str::from_utf8(digits).ok()?.parse().ok()?;
    Some(if negative {
        value.wrapping_neg()
    } else {
        value
    })
}

fn nibble(c: u8) -> Option<u8> {
    (c as char).to_digit(16).map(|d| d as u8)
}

/// fc's `block_id_type` from a variant: even-length hex (at most 32 bytes
/// here), zero-padded.
fn hex_id(s: &[u8]) -> Option<[u8; 32]> {
    if !s.len().is_multiple_of(2) || s.len() > 64 {
        return None;
    }
    let mut id = [0u8; 32];
    for (byte, pair) in id.iter_mut().zip(s.chunks(2)) {
        *byte = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    Some(id)
}

/// fc's `sha256(const std::string&)`: up to 64 hex characters are read and
/// the rest ignored, an odd last nibble is the high one, and the id is
/// zero-padded.
fn sha256_id(s: &[u8]) -> Option<[u8; 32]> {
    let mut id = [0u8; 32];
    for (byte, pair) in id.iter_mut().zip(s.chunks(2)) {
        let low = match pair.get(1) {
            Some(c) => nibble(*c)?,
            None => 0,
        };
        *byte = (nibble(pair[0])? << 4) | low;
    }
    Some(id)
}

/// The text nodeos writes for an echoed parameter, before JSON escaping:
/// fc's `clean_append`. Control bytes other than TAB, LF and CR, and DEL,
/// become the text `\u00xx`; invalid UTF-8 is dropped.
fn echo_text(s: &[u8]) -> String {
    let mut out = String::with_capacity(s.len());
    for chunk in s.utf8_chunks() {
        for c in chunk.valid().chars() {
            if (c < ' ' && !matches!(c, '\t' | '\n' | '\r')) || c == '\x7f' {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            } else {
                out.push(c);
            }
        }
    }
    out
}

/// A JSON string as fc's writer escapes it.
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            c if c < ' ' || c == '\x7f' => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn json_list(items: &BTreeSet<String>) -> String {
    let items: Vec<String> = items.iter().map(|s| json_string(s)).collect();
    format!("[{}]", items.join(","))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// The `block_timestamp` of slot `n` (500 ms slots from 2000-01-01) as fc
/// writes a time point: `YYYY-MM-DDTHH:MM:SS.mmm`.
fn block_time(n: u32) -> String {
    let ms = 946_684_800_000u64 + u64::from(n) * 500;
    let secs = ms / 1000;
    let (year, month, day) = civil_from_days((secs / 86_400) as i64);
    let rem = secs % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        ms % 1000
    )
}

/// Days since 1970-01-01 to a proleptic Gregorian date.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

// ---------------------------------------------------------------------------
// fc's JSON parser, restricted to what a frozen range server also reads
// ---------------------------------------------------------------------------

/// A JSON value as fc reads it. Strings are raw bytes: fc keeps control
/// bytes and does not check UTF-8.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Value {
    Null,
    Bool(bool),
    U64(u64),
    I64(i64),
    Str(Vec<u8>),
    Array(Vec<Value>),
    /// Members in order. Duplicates are kept; the first one is read.
    Object(Vec<(Vec<u8>, Value)>),
}

/// fc nests at most this deep.
const MAX_DEPTH: usize = 200;

/// The members of the body's top-level object, read the way fc reads them:
/// - leading whitespace is skipped and bytes after the object are ignored;
/// - members may be separated by any number of commas, or none;
/// - in strings, `\t`, `\n`, `\r` and `\\` decode and `\X` gives `X`, raw
///   control bytes are kept, and byte 0x04 fails;
/// - numbers are integers (`-?[0-9]+`, not followed by a letter, digit or
///   `.`): negative ones are i64, others u64, and overflow fails;
/// - `true`, `false` and `null` must not run into a letter or digit.
///
/// Fractions, exponents and other tokens fail: a frozen range server
/// refuses them, and the router never sends them there.
fn parse_params(body: &[u8]) -> Option<Vec<(Vec<u8>, Value)>> {
    let mut reader = Reader {
        bytes: body,
        pos: 0,
    };
    reader.skip_ws();
    if reader.peek()? != b'{' {
        return None;
    }
    match reader.value(0)? {
        Value::Object(members) => Some(members),
        _ => None,
    }
}

fn field<'a>(members: &'a [(Vec<u8>, Value)], key: &str) -> Option<&'a Value> {
    members
        .iter()
        .find(|(k, _)| k.as_slice() == key.as_bytes())
        .map(|(_, v)| v)
}

/// fc's `as_string` for a string field: `None` (fc throws) for an object or
/// an array.
fn text_field(value: Option<&Value>) -> Option<Vec<u8>> {
    Some(match value {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Str(s)) => s.clone(),
        Some(Value::U64(v)) => v.to_string().into_bytes(),
        Some(Value::I64(v)) => v.to_string().into_bytes(),
        Some(Value::Bool(b)) => b.to_string().into_bytes(),
        Some(Value::Array(_) | Value::Object(_)) => return None,
    })
}

/// fc's `as_uint64` truncated to u32, for `block_num`.
fn u32_field(value: Option<&Value>) -> Option<u32> {
    Some(match value {
        None | Some(Value::Null) => 0,
        Some(Value::U64(v)) => *v as u32,
        Some(Value::I64(v)) => *v as u64 as u32,
        Some(Value::Bool(b)) => u32::from(*b),
        Some(Value::Str(s)) => to_uint64(s)? as u32,
        Some(Value::Array(_) | Value::Object(_)) => return None,
    })
}

/// fc's `as_bool`, for `include_extensions`.
fn bool_field(value: Option<&Value>) -> Option<bool> {
    Some(match value {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::U64(v)) => *v != 0,
        Some(Value::I64(v)) => *v != 0,
        Some(Value::Str(s)) if s == b"true" => true,
        Some(Value::Str(s)) if s == b"false" => false,
        Some(Value::Str(_) | Value::Array(_) | Value::Object(_)) => return None,
    })
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn bump(&mut self) -> Option<u8> {
        let byte = self.peek()?;
        self.pos += 1;
        Some(byte)
    }

    fn skip_ws(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    /// The value at the cursor; `depth` is the number of enclosing containers.
    fn value(&mut self, depth: usize) -> Option<Value> {
        self.skip_ws();
        match self.peek()? {
            b'{' => self.object(depth + 1),
            b'[' => self.array(depth + 1),
            b'"' => self.string().map(Value::Str),
            b'-' | b'0'..=b'9' => self.number(),
            _ => self.keyword(),
        }
    }

    fn object(&mut self, depth: usize) -> Option<Value> {
        if depth > MAX_DEPTH {
            return None;
        }
        self.pos += 1;
        let mut members = Vec::new();
        loop {
            while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r' | b',')) {
                self.pos += 1;
            }
            match self.peek()? {
                b'}' => {
                    self.pos += 1;
                    return Some(Value::Object(members));
                }
                b'"' => {
                    let key = self.string()?;
                    self.skip_ws();
                    if self.bump()? != b':' {
                        return None;
                    }
                    let value = self.value(depth)?;
                    members.push((key, value));
                }
                _ => return None,
            }
        }
    }

    fn array(&mut self, depth: usize) -> Option<Value> {
        if depth > MAX_DEPTH {
            return None;
        }
        self.pos += 1;
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek()? == b']' {
            self.pos += 1;
            return Some(Value::Array(items));
        }
        loop {
            items.push(self.value(depth)?);
            self.skip_ws();
            match self.bump()? {
                b',' => {}
                b']' => return Some(Value::Array(items)),
                _ => return None,
            }
        }
    }

    fn string(&mut self) -> Option<Vec<u8>> {
        self.pos += 1;
        let mut out = Vec::new();
        loop {
            match self.bump()? {
                b'"' => return Some(out),
                b'\\' => out.push(match self.bump()? {
                    b't' => b'\t',
                    b'n' => b'\n',
                    b'r' => b'\r',
                    other => other,
                }),
                0x04 => return None,
                byte => out.push(byte),
            }
        }
    }

    fn number(&mut self) -> Option<Value> {
        let start = self.pos;
        let negative = self.peek() == Some(b'-');
        if negative {
            self.pos += 1;
        }
        let digits = self.pos;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.pos += 1;
        }
        if self.pos == digits
            || matches!(self.peek(), Some(b) if b.is_ascii_alphanumeric() || b == b'.')
        {
            return None;
        }
        let text = std::str::from_utf8(&self.bytes[start..self.pos]).ok()?;
        if negative {
            text.parse().ok().map(Value::I64)
        } else {
            text.parse().ok().map(Value::U64)
        }
    }

    fn keyword(&mut self) -> Option<Value> {
        let rest = &self.bytes[self.pos..];
        let (len, value) = if rest.starts_with(b"true") {
            (4, Value::Bool(true))
        } else if rest.starts_with(b"false") {
            (5, Value::Bool(false))
        } else if rest.starts_with(b"null") {
            (4, Value::Null)
        } else {
            return None;
        };
        if rest.get(len).is_some_and(u8::is_ascii_alphanumeric) {
            return None;
        }
        self.pos += len;
        Some(value)
    }
}

// ---------------------------------------------------------------------------
// A raw HTTP/1.1 client, for exact-bytes tests
// ---------------------------------------------------------------------------

/// One HTTP response as read off the wire.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawResponse {
    pub status: u16,
    /// Header names exactly as sent (so their case shows), in order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl RawResponse {
    /// The first header with this name, ignoring case.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// The body as text (lossy).
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// A client connection that writes raw request bytes and reads responses,
/// so a test controls and sees every byte. Every read has a deadline.
pub struct RawConn {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl RawConn {
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        Ok(Self {
            stream: TcpStream::connect(addr).await?,
            buf: Vec::new(),
        })
    }

    pub async fn send(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.stream.write_all(bytes).await
    }

    /// Read one response, skipping interim 1xx ones. The body is framed by
    /// `Content-Length`, by chunked encoding, or by the end of the
    /// connection. `TimedOut` after `within`.
    pub async fn response(&mut self, within: Duration) -> io::Result<RawResponse> {
        timeout(within, self.read_response())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no response in time"))?
    }

    /// Whether the peer closes the connection (or resets it) within
    /// `within` without sending anything more.
    pub async fn closed(&mut self, within: Duration) -> bool {
        if !self.buf.is_empty() {
            return false;
        }
        let mut chunk = [0u8; 1024];
        matches!(
            timeout(within, self.stream.read(&mut chunk)).await,
            Ok(Ok(0) | Err(_))
        )
    }

    async fn fill(&mut self) -> io::Result<()> {
        let mut chunk = [0u8; 16 * 1024];
        let n = self.stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed",
            ));
        }
        self.buf.extend_from_slice(&chunk[..n]);
        Ok(())
    }

    /// One line, without its CRLF.
    async fn line(&mut self) -> io::Result<String> {
        loop {
            if let Some(at) = find(&self.buf, b"\r\n") {
                let line = String::from_utf8_lossy(&self.buf[..at]).into_owned();
                self.buf.drain(..at + 2);
                return Ok(line);
            }
            self.fill().await?;
        }
    }

    async fn take(&mut self, len: usize) -> io::Result<Vec<u8>> {
        while self.buf.len() < len {
            self.fill().await?;
        }
        Ok(self.buf.drain(..len).collect())
    }

    async fn read_response(&mut self) -> io::Result<RawResponse> {
        loop {
            let status_line = self.line().await?;
            let status: u16 = status_line
                .split(' ')
                .nth(1)
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| invalid(&format!("bad status line {status_line:?}")))?;
            let mut headers = Vec::new();
            loop {
                let line = self.line().await?;
                if line.is_empty() {
                    break;
                }
                let (name, value) = line
                    .split_once(':')
                    .ok_or_else(|| invalid(&format!("bad header line {line:?}")))?;
                headers.push((name.to_string(), value.trim().to_string()));
            }
            if (100..200).contains(&status) {
                continue;
            }
            let mut response = RawResponse {
                status,
                headers,
                body: Vec::new(),
            };
            response.body = if let Some(len) = response.header("content-length") {
                let len = len
                    .parse()
                    .map_err(|_| invalid(&format!("bad Content-Length {len:?}")))?;
                self.take(len).await?
            } else if response
                .header("transfer-encoding")
                .is_some_and(|te| te.eq_ignore_ascii_case("chunked"))
            {
                self.chunked().await?
            } else {
                self.until_close().await?
            };
            return Ok(response);
        }
    }

    async fn chunked(&mut self) -> io::Result<Vec<u8>> {
        let mut body = Vec::new();
        loop {
            let line = self.line().await?;
            let size = line.split(';').next().unwrap_or("").trim();
            let size = usize::from_str_radix(size, 16)
                .map_err(|_| invalid(&format!("bad chunk size {line:?}")))?;
            if size == 0 {
                // Trailers, up to the empty line.
                while !self.line().await?.is_empty() {}
                return Ok(body);
            }
            body.extend(self.take(size).await?);
            if self.take(2).await? != b"\r\n" {
                return Err(invalid("chunk without CRLF"));
            }
        }
    }

    async fn until_close(&mut self) -> io::Result<Vec<u8>> {
        loop {
            match self.fill().await {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    return Ok(std::mem::take(&mut self.buf));
                }
                Err(e) => return Err(e),
            }
        }
    }
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn invalid(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what.to_string())
}

/// A request with a `Host`, a JSON `Content-Type` and a `Content-Length`.
pub fn request(method: &str, target: &str, host: &str, body: &[u8]) -> Vec<u8> {
    let mut bytes = format!(
        "{method} {target} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )
    .into_bytes();
    bytes.extend_from_slice(body);
    bytes
}

/// Send `request` on a new connection and read one response, all within
/// `within`.
pub async fn exchange(
    addr: SocketAddr,
    request: &[u8],
    within: Duration,
) -> io::Result<RawResponse> {
    let deadline = tokio::time::Instant::now() + within;
    let mut conn = timeout(within, RawConn::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))??;
    conn.send(request).await?;
    conn.response(deadline.saturating_duration_since(tokio::time::Instant::now()))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MockShipConfig, MockShipServer};

    const WAIT: Duration = Duration::from_secs(3);
    const CHAIN: [u8; 32] = [0xaa; 32];

    fn text(body: &[u8]) -> String {
        String::from_utf8_lossy(body).into_owned()
    }

    fn parse(ep: BlockEp, body: &str) -> Result<BlockRequest, ParamError> {
        parse_block_request(ep, body.as_bytes())
    }

    fn num(n: u32, echo: &str) -> Result<BlockRequest, ParamError> {
        Ok(BlockRequest {
            query: Query::Num(n),
            include_extensions: false,
            echo: echo.to_string(),
        })
    }

    fn bad_id(ep: BlockEp, echo: &str) -> Result<BlockRequest, ParamError> {
        Err(ParamError::BadId {
            ep,
            include_extensions: false,
            echo: echo.to_string(),
        })
    }

    async fn post(api: &MockChainApiHandle, target: &str, body: &str) -> RawResponse {
        exchange(
            api.addr(),
            &request("POST", target, &api.endpoint(), body.as_bytes()),
            WAIT,
        )
        .await
        .unwrap()
    }

    async fn live() -> MockChainApiHandle {
        MockChainApi::live("live-1", CHAIN, 200, 190, Some(100), "v1.2.2")
            .start()
            .await
    }

    async fn frozen() -> MockChainApiHandle {
        MockChainApi::frozen("frozen-0a", CHAIN, (1, 101), 100, 4)
            .start()
            .await
    }

    #[test]
    fn empty_content_follows_nodeos() {
        for empty in ["", "   ", "{}", "{ \t }", " {} "] {
            assert!(is_empty_content(empty.as_bytes()), "{empty:?}");
        }
        for full in ["{}\n", "\t{}", "{", "{ \n }", "x", r#"{"a":1}"#] {
            assert!(!is_empty_content(full.as_bytes()), "{full:?}");
        }
    }

    #[test]
    fn to_uint64_follows_lexical_cast() {
        assert_eq!(to_uint64(b"0"), Some(0));
        assert_eq!(to_uint64(b"0001"), Some(1));
        assert_eq!(to_uint64(b"+5"), Some(5));
        assert_eq!(to_uint64(b"-1"), Some(u64::MAX));
        assert_eq!(to_uint64(b"-0"), Some(0));
        assert_eq!(to_uint64(b"18446744073709551615"), Some(u64::MAX));
        for bad in [
            "",
            "+",
            "-",
            " 1",
            "1 ",
            "18446744073709551616",
            "1e5",
            "0x10",
            "++1",
        ] {
            assert_eq!(to_uint64(bad.as_bytes()), None, "{bad:?}");
        }
    }

    #[test]
    fn bodies_parse_like_fc() {
        let members = |body: &str| parse_params(body.as_bytes());
        let str_of = |body: &str, key: &str| {
            text_field(field(&members(body).expect(body), key)).map(|s| text(&s))
        };
        // Duplicates are kept and the first wins; commas are optional and
        // repeatable; bytes after the object are ignored.
        assert_eq!(str_of(r#"{"k":"1","k":"2"}"#, "k").as_deref(), Some("1"));
        assert_eq!(str_of(r#"{,,"a":1 "k":"x",}"#, "k").as_deref(), Some("x"));
        assert_eq!(
            str_of(r#" {"k":"x"} trailing [junk"#, "k").as_deref(),
            Some("x")
        );
        // Escapes: only \t \n \r \\ decode, \X gives X.
        assert_eq!(str_of(r#"{"k":"\u0031"}"#, "k").as_deref(), Some("u0031"));
        assert_eq!(
            str_of(r#"{"k":"a\tb\\c\"d\qe"}"#, "k").as_deref(),
            Some("a\tb\\c\"dqe")
        );
        // Raw control bytes are kept; 0x04 fails.
        assert_eq!(str_of("{\"k\":\"a\x01b\"}", "k").as_deref(), Some("a\x01b"));
        assert_eq!(members("{\"k\":\"a\x04b\"}"), None);
        // Numbers: integers only.
        assert_eq!(
            members(r#"{"a":5,"b":-5,"c":true,"d":null,"e":[],"f":[1,"x"],"g":{}}"#),
            Some(vec![
                (b"a".to_vec(), Value::U64(5)),
                (b"b".to_vec(), Value::I64(-5)),
                (b"c".to_vec(), Value::Bool(true)),
                (b"d".to_vec(), Value::Null),
                (b"e".to_vec(), Value::Array(vec![])),
                (
                    b"f".to_vec(),
                    Value::Array(vec![Value::U64(1), Value::Str(b"x".to_vec())])
                ),
                (b"g".to_vec(), Value::Object(vec![])),
            ])
        );
        for bad in [
            r#"{"a":1e5}"#,
            r#"{"a":1.5}"#,
            r#"{"a":12a}"#,
            r#"{"a":18446744073709551616}"#,
            r#"{"a":-9223372036854775809}"#,
            r#"{"a":-}"#,
            r#"{"a":truex}"#,
            r#"{"a":nul}"#,
            r#"{"a":[1,]}"#,
            r#"{"a":[1 2]}"#,
            r#"{"a":"open}"#,
            r#"{"a" 1}"#,
            r#"{a:1}"#,
            r#"[{"a":1}]"#,
            r#""x""#,
            "",
        ] {
            assert_eq!(members(bad), None, "{bad:?}");
        }
        // Nesting: 200 containers deep parses, 201 fails.
        let nested = |depth: usize| {
            format!(
                r#"{{"a":{}1{}}}"#,
                "[".repeat(depth - 1),
                "]".repeat(depth - 1)
            )
        };
        assert!(members(&nested(200)).is_some());
        assert_eq!(members(&nested(201)), None);
    }

    #[test]
    fn fields_convert_like_fc() {
        let v = |s: &str| Value::Str(s.as_bytes().to_vec());
        // block_num_or_id
        assert_eq!(text_field(None), Some(vec![]));
        assert_eq!(text_field(Some(&Value::Null)), Some(vec![]));
        assert_eq!(text_field(Some(&Value::U64(5))), Some(b"5".to_vec()));
        assert_eq!(text_field(Some(&Value::I64(-5))), Some(b"-5".to_vec()));
        assert_eq!(text_field(Some(&Value::Bool(true))), Some(b"true".to_vec()));
        assert_eq!(text_field(Some(&Value::Array(vec![]))), None);
        assert_eq!(text_field(Some(&Value::Object(vec![]))), None);
        // block_num
        assert_eq!(u32_field(None), Some(0));
        assert_eq!(u32_field(Some(&Value::U64(4_294_967_297))), Some(1));
        assert_eq!(u32_field(Some(&Value::I64(-1))), Some(u32::MAX));
        assert_eq!(u32_field(Some(&Value::Bool(true))), Some(1));
        assert_eq!(u32_field(Some(&v("+7"))), Some(7));
        assert_eq!(u32_field(Some(&v("x"))), None);
        assert_eq!(u32_field(Some(&Value::Array(vec![]))), None);
        // include_extensions
        assert_eq!(bool_field(None), Some(false));
        assert_eq!(bool_field(Some(&Value::U64(2))), Some(true));
        assert_eq!(bool_field(Some(&Value::I64(0))), Some(false));
        assert_eq!(bool_field(Some(&v("true"))), Some(true));
        assert_eq!(bool_field(Some(&v("false"))), Some(false));
        assert_eq!(bool_field(Some(&v("1"))), None);
        assert_eq!(bool_field(Some(&Value::Object(vec![]))), None);
    }

    #[test]
    fn block_num_or_id_resolves_like_nodeos() {
        use BlockEp::*;
        assert_eq!(parse(RawBlock, ""), Err(ParamError::BodyRequired));
        assert_eq!(parse(RawBlock, "{ }"), Err(ParamError::BodyRequired));
        assert_eq!(parse(RawBlock, "{"), Err(ParamError::Unparsable));
        assert_eq!(
            parse(RawBlock, r#"{"block_num_or_id":[]}"#),
            Err(ParamError::Unparsable)
        );
        assert_eq!(parse(RawBlock, r#"{"block_num_or_id":"1"}"#), num(1, "1"));
        assert_eq!(parse(RawBlock, r#"{"block_num_or_id":1}"#), num(1, "1"));
        assert_eq!(parse(RawBlock, r#"{"block_num_or_id":"+1"}"#), num(1, "+1"));
        assert_eq!(
            parse(RawBlock, r#"{"block_num_or_id":"-1"}"#),
            num(u32::MAX, "-1")
        );
        assert_eq!(
            parse(RawBlock, r#"{"block_num_or_id":"4294967297"}"#),
            num(1, "4294967297")
        );
        // Missing, null or empty, and more than 64 bytes: the length assert.
        for body in [
            r#"{"x":1}"#,
            r#"{"block_num_or_id":null}"#,
            r#"{"block_num_or_id":""}"#,
        ] {
            assert_eq!(parse(RawBlock, body), Err(ParamError::BadLength(RawBlock)));
        }
        let long = format!(r#"{{"block_num_or_id":"{}"}}"#, "a".repeat(65));
        assert_eq!(
            parse(BlockHeader, &long),
            Err(ParamError::BadLength(BlockHeader))
        );
        // Not a number: even-length hex, zero-padded, else Invalid block ID.
        assert_eq!(
            parse(RawBlock, r#"{"block_num_or_id":"00000A"}"#).map(|r| r.query),
            Ok(Query::Id({
                let mut id = [0u8; 32];
                id[2] = 0x0a;
                id
            }))
        );
        assert_eq!(
            parse(RawBlock, r#"{"block_num_or_id":"000005"}"#).map(|r| r.query),
            Ok(Query::Num(5))
        );
        // Digits are a number whatever their length (up to 64).
        assert_eq!(
            parse(Block, r#"{"block_num_or_id":"0000050"}"#),
            num(50, "0000050")
        );
        assert_eq!(
            parse(Block, r#"{"block_num_or_id":"abc"}"#),
            bad_id(Block, "abc")
        );
        assert_eq!(
            parse(RawBlock, r#"{"block_num_or_id":" 1"}"#),
            bad_id(RawBlock, " 1")
        );
        assert_eq!(
            parse(RawBlock, r#"{"block_num_or_id":true}"#),
            bad_id(RawBlock, "true")
        );
        assert_eq!(
            parse(RawBlock, r#"{"block_num_or_id":"\u0031"}"#),
            bad_id(RawBlock, "u0031")
        );
        assert_eq!(
            parse(RawBlock, r#"{"block_num_or_id":"zz"}"#),
            bad_id(RawBlock, "zz")
        );
        let hex64 = "0000000a".to_string() + &"f".repeat(56);
        let request = parse(RawBlock, &format!(r#"{{"block_num_or_id":"{hex64}"}}"#)).unwrap();
        assert_eq!(request.query, Query::Id(hex_id(hex64.as_bytes()).unwrap()));
        // include_extensions only on get_block_header, converted like fc.
        let header = parse(
            BlockHeader,
            r#"{"block_num_or_id":"7","include_extensions":"true"}"#,
        )
        .unwrap();
        assert!(header.include_extensions);
        assert_eq!(
            parse(
                BlockHeader,
                r#"{"block_num_or_id":"7","include_extensions":"yes"}"#
            ),
            Err(ParamError::Unparsable)
        );
        assert!(
            !parse(
                RawBlock,
                r#"{"block_num_or_id":"7","include_extensions":true}"#
            )
            .unwrap()
            .include_extensions
        );
        // get_block_info reads block_num.
        assert_eq!(parse(BlockInfo, r#"{"block_num":"8"}"#), num(8, "8"));
        assert_eq!(
            parse(BlockInfo, r#"{"block_num":-1}"#),
            num(u32::MAX, "4294967295")
        );
        assert_eq!(parse(BlockInfo, r#"{"other":1}"#), num(0, "0"));
        assert_eq!(
            parse(BlockInfo, r#"{"block_num":"x"}"#),
            Err(ParamError::Unparsable)
        );
        // get_block_header_state: no length check, fc's sha256 of the string.
        assert_eq!(
            parse(BlockHeaderState, r#"{"block_num_or_id":""}"#).map(|r| r.query),
            Ok(Query::Id([0; 32]))
        );
        let odd = parse(BlockHeaderState, r#"{"block_num_or_id":"abc"}"#).unwrap();
        let mut want = [0u8; 32];
        want[..2].copy_from_slice(&[0xab, 0xc0]);
        assert_eq!(odd.query, Query::Id(want));
        let long = format!(r#"{{"block_num_or_id":"{}zz"}}"#, "0".repeat(64));
        assert_eq!(
            parse(BlockHeaderState, &long).map(|r| r.query),
            Ok(Query::Id([0; 32]))
        );
        assert_eq!(
            parse(BlockHeaderState, r#"{"block_num_or_id":"0g"}"#),
            bad_id(BlockHeaderState, "0g")
        );
    }

    #[test]
    fn echoed_text_is_cleaned_then_escaped() {
        assert_eq!(echo_text(b"plain"), "plain");
        assert_eq!(
            echo_text(b"a\x01b\x7f\tc\nd\re"),
            "a\\u0001b\\u007f\tc\nd\re"
        );
        assert_eq!(echo_text(b"ok\xffok\xc3"), "okok");
        assert_eq!(echo_text("é".as_bytes()), "é");
        assert_eq!(
            json_string(&echo_text(b"q\"b\\\x02\t")),
            r#""q\"b\\\\u0002\t""#
        );
    }

    #[test]
    fn error_bodies_are_exact() {
        let answer = |e: ParamError| {
            let (status, body) = e.answer();
            (status, text(&body))
        };
        assert_eq!(
            answer(ParamError::BodyRequired),
            (
                400,
                r#"{"code":400,"message":"Invalid Request","error":{"code":3200006,"name":"invalid_http_request","what":"invalid http request","details":[{"message":"A Request body is required","file":"http_plugin.hpp","line_number":257,"method":"parse_params"}]}}"#.to_string()
            )
        );
        assert_eq!(
            answer(ParamError::Unparsable),
            (
                400,
                r#"{"code":400,"message":"Invalid Request","error":{"code":3200006,"name":"invalid_http_request","what":"invalid http request","details":[{"message":"Unable to parse valid input from POST body","file":"http_plugin.hpp","line_number":278,"method":"parse_params"}]}}"#.to_string()
            )
        );
        assert_eq!(
            answer(ParamError::BadLength(BlockEp::RawBlock)),
            (
                500,
                r#"{"code":500,"message":"Internal Service Error","error":{"code":3010008,"name":"block_id_type_exception","what":"Invalid block ID","details":[{"message":"Invalid Block number or ID, must be greater than 0 and less than 65 characters","file":"chain_plugin.cpp","line_number":1949,"method":"get_raw_block"}]}}"#.to_string()
            )
        );
        assert!(answer(ParamError::BadLength(BlockEp::BlockHeader))
            .1
            .contains(r#""line_number":1987,"method":"get_block_header""#));
        assert_eq!(
            answer(ParamError::BadId {
                ep: BlockEp::Block,
                include_extensions: false,
                echo: "zz".to_string()
            }),
            (
                500,
                r#"{"code":500,"message":"Internal Service Error","error":{"code":3010008,"name":"block_id_type_exception","what":"Invalid block ID","details":[{"message":"Invalid block ID: zz","file":"chain_plugin.cpp","line_number":1963,"method":"get_raw_block"}]}}"#.to_string()
            )
        );
        for (ep, ext, line) in [
            (BlockEp::BlockHeader, false, 2004),
            (BlockEp::BlockHeader, true, 2015),
            (BlockEp::BlockHeaderState, false, 2082),
        ] {
            let body = answer(ParamError::BadId {
                ep,
                include_extensions: ext,
                echo: "zz".to_string(),
            })
            .1;
            assert!(
                body.contains(&format!(
                    r#""line_number":{line},"method":"{}""#,
                    ep.method()
                )),
                "{body}"
            );
        }
        assert_eq!(
            text(&unknown_block_body(BlockEp::RawBlock, false, "123")),
            r#"{"code":400,"message":"Unknown Block","error":{"code":3100002,"name":"unknown_block_exception","what":"Unknown block","details":[{"message":"Could not find block: 123","file":"chain_plugin.cpp","line_number":1966,"method":"get_raw_block"}]}}"#
        );
        for (ep, ext, message, line) in [
            (BlockEp::Block, false, "Could not find block: 7", 1966),
            (
                BlockEp::BlockHeader,
                false,
                "Could not find block header: 7",
                2006,
            ),
            (
                BlockEp::BlockHeader,
                true,
                "Could not find block header: 7",
                2017,
            ),
            (BlockEp::BlockInfo, false, "Could not find block: 7", 2049),
            (
                BlockEp::BlockHeaderState,
                false,
                "Could not find block: 7",
                2085,
            ),
        ] {
            let body = text(&unknown_block_body(ep, ext, "7"));
            assert!(
                body.contains(&format!(
                    r#"{{"message":"{message}","file":"chain_plugin.cpp","line_number":{line},"method":"{}"}}"#,
                    ep.method()
                )),
                "{body}"
            );
        }
        // The echo is cleaned, then escaped.
        assert!(text(&unknown_block_body(
            BlockEp::RawBlock,
            false,
            &echo_text(b"a\"\x01")
        ))
        .contains(r#""message":"Could not find block: a\"\\u0001""#));
        assert_eq!(
            busy_body("Too many requests in flight: 5"),
            r#"{"code":503,"message":"Busy","error":{"code":503,"name":"Busy","what":"Too many requests in flight: 5","details":[]}}"#
        );
    }

    #[test]
    fn block_time_follows_block_timestamp() {
        assert_eq!(block_time(0), "2000-01-01T00:00:00.000");
        assert_eq!(block_time(1), "2000-01-01T00:00:00.500");
        // 2000 is a leap year: 31 + 29 days to March 1st.
        assert_eq!(block_time(60 * 86_400 * 2), "2000-03-01T00:00:00.000");
        assert_eq!(block_time(u32::MAX), "2068-01-19T03:14:07.500");
    }

    #[test]
    fn ids_carry_the_block_number_big_endian() {
        let id = block_id(0x0102_0304);
        assert_eq!(id[..4], [1, 2, 3, 4]);
        assert_eq!(id[4..], fake_block_id(0x0102_0304)[4..]);
        assert_ne!(block_id_seeded(9, 1), block_id(9));
        assert_eq!(block_id_seeded(9, 1)[..4], 9u32.to_be_bytes());
        assert_eq!(block_id_seeded(9, 0), block_id(9));
        assert_eq!(
            text(&block_answer("live-1", 9, &[0xab; 32])),
            format!(
                r#"{{"block_num":9,"id":"{}","served_by":"live-1"}}"#,
                "ab".repeat(32)
            )
        );
        assert_eq!(
            text(&call_answer("a\"b", "/v1/chain/get_account", 12)),
            r#"{"target":"/v1/chain/get_account","body_bytes":12,"served_by":"a\"b"}"#
        );
    }

    #[test]
    fn host_check_compares_the_address_only() {
        let local = Some(IpAddr::from([127, 0, 0, 1]));
        let host = |h: &str| HeaderValue::from_str(h).unwrap();
        assert!(host_allowed(Some(&host("127.0.0.1:8888")), local));
        assert!(host_allowed(Some(&host("127.0.0.1")), local));
        assert!(host_allowed(Some(&host("[::ffff:127.0.0.1]:1")), local));
        assert!(!host_allowed(Some(&host("10.0.0.5:8888")), local));
        assert!(!host_allowed(Some(&host("localhost:8888")), local));
        assert!(!host_allowed(Some(&host("")), local));
        assert!(!host_allowed(None, local));
    }

    #[tokio::test]
    async fn live_get_info_follows_the_clock() {
        let api = live().await;
        let info = post(&api, "/v1/chain/get_info", "").await;
        assert_eq!(info.status, 200);
        let want = |head: u32, lib: u32, earliest: &str| {
            format!(
                concat!(
                    r#"{{"server_version":"00000000","chain_id":"{chain}","head_block_num":{head},"#,
                    r#""last_irreversible_block_num":{lib},"last_irreversible_block_id":"{lib_id}","#,
                    r#""head_block_id":"{head_id}","head_block_time":"{head_time}","#,
                    r#""head_block_producer":"eosio","virtual_block_cpu_limit":200000000,"#,
                    r#""virtual_block_net_limit":1048576000,"block_cpu_limit":200000,"#,
                    r#""block_net_limit":1048576,"server_version_string":"v1.2.2","#,
                    r#""fork_db_head_block_num":{head},"fork_db_head_block_id":"{head_id}","#,
                    r#""server_full_version_string":"v1.2.2-mock"{earliest},"#,
                    r#""last_irreversible_block_time":"{lib_time}","served_by":"live-1"}}"#,
                ),
                chain = "aa".repeat(32),
                head = head,
                lib = lib,
                lib_id = hex(&block_id(lib)),
                head_id = hex(&block_id(head)),
                head_time = block_time(head),
                earliest = earliest,
                lib_time = block_time(lib),
            )
        };
        assert_eq!(
            info.text(),
            want(200, 190, r#","earliest_available_block_num":100"#)
        );
        // nodeos framing: JSON, Content-Length, title-case names, no Date.
        assert_eq!(info.header("content-type"), Some("application/json"));
        assert_eq!(info.header("server"), Some("nodeos/v1.2.2"));
        assert_eq!(
            info.header("content-length"),
            Some(info.body.len().to_string().as_str())
        );
        assert!(info.header("date").is_none());
        for name in ["Content-Type", "Content-Length", "Server"] {
            assert!(
                info.headers.iter().any(|(n, _)| n == name),
                "{:?}",
                info.headers
            );
        }

        api.set_head(250, 240);
        api.set_earliest(None);
        assert_eq!(
            post(&api, "/v1/chain/get_info", "{}").await.text(),
            want(250, 240, "")
        );
        api.advance(5);
        assert_eq!(api.clock().head(), 255);
        // no_params: any body is the parse error.
        let with_body = post(&api, "/v1/chain/get_info", r#"{"a":1}"#).await;
        assert_eq!(with_body.status, 400);
        assert_eq!(with_body.body, ParamError::Unparsable.answer().1);
        api.set_chain_id([0xbb; 32]);
        assert!(post(&api, "/v1/chain/get_info", "")
            .await
            .text()
            .contains(&format!(r#""chain_id":"{}""#, "bb".repeat(32))));
    }

    #[tokio::test]
    async fn a_live_api_can_share_a_ship_mock_clock() {
        let ship = MockShipServer::new(MockShipConfig::live(1, 500))
            .await
            .start();
        let api = MockChainApi {
            clock: Some(ship.clock()),
            ..MockChainApi::live("live-1", CHAIN, 0, 0, None, "v1.2.2")
        }
        .start()
        .await;
        assert!(post(&api, "/v1/chain/get_info", "")
            .await
            .text()
            .contains(r#""head_block_num":500,"last_irreversible_block_num":490,"#));
        ship.set_head(600, 590);
        assert!(post(&api, "/v1/chain/get_info", "")
            .await
            .text()
            .contains(r#""head_block_num":600,"last_irreversible_block_num":590,"#));
        // A fork on the SHiP side changes the API's ids from that block.
        ship.fork(550, 3);
        let body = format!(
            r#"{{"block_num_or_id":"{}"}}"#,
            hex(&block_id_seeded(560, 3))
        );
        assert_eq!(
            post(&api, "/v1/chain/get_raw_block", &body).await.body,
            block_answer("live-1", 560, &block_id_seeded(560, 3))
        );
        assert_eq!(
            post(
                &api,
                "/v1/chain/get_raw_block",
                r#"{"block_num_or_id":549}"#
            )
            .await
            .body,
            block_answer("live-1", 549, &block_id(549))
        );
    }

    #[tokio::test]
    async fn live_block_endpoints_answer_by_number_and_id() {
        let api = live().await;
        let ok = |n: u32| block_answer("live-1", n, &block_id(n));
        for target in [
            "/v1/chain/get_block",
            "/v1/chain/get_raw_block",
            "/v1/chain/get_block_header",
            "/v1/chain/get_block_header_state",
        ] {
            let by_num = post(&api, target, r#"{"block_num_or_id":150}"#).await;
            assert_eq!((by_num.status, by_num.body), (200, ok(150)), "{target}");
            let by_id = format!(r#"{{"block_num_or_id":"{}"}}"#, hex(&block_id(200)));
            assert_eq!(post(&api, target, &by_id).await.body, ok(200), "{target}");
        }
        assert_eq!(
            post(&api, "/v1/chain/get_block_info", r#"{"block_num":100}"#)
                .await
                .body,
            ok(100)
        );

        // Below earliest, above head, and an id whose number is held but
        // whose bytes differ: Unknown Block, echoing the parameter.
        let ep = BlockEp::RawBlock;
        for (body, echo) in [
            (r#"{"block_num_or_id":99}"#, "99"),
            (r#"{"block_num_or_id":"+201"}"#, "+201"),
        ] {
            let answer = post(&api, "/v1/chain/get_raw_block", body).await;
            assert_eq!(answer.status, 400);
            assert_eq!(answer.body, unknown_block_body(ep, false, echo));
        }
        let mut wrong = block_id(150);
        wrong[31] ^= 1;
        let body = format!(r#"{{"block_num_or_id":"{}"}}"#, hex(&wrong));
        assert_eq!(
            post(&api, "/v1/chain/get_block", &body).await.body,
            unknown_block_body(BlockEp::Block, false, &hex(&wrong))
        );
        assert_eq!(
            post(
                &api,
                "/v1/chain/get_block_header",
                r#"{"block_num_or_id":"1","include_extensions":true}"#
            )
            .await
            .body,
            unknown_block_body(BlockEp::BlockHeader, true, "1")
        );
        assert_eq!(
            post(&api, "/v1/chain/get_block_info", r#"{"block_num":"-1"}"#)
                .await
                .body,
            unknown_block_body(BlockEp::BlockInfo, false, "4294967295")
        );
        // Parameter errors.
        let zz = post(
            &api,
            "/v1/chain/get_raw_block",
            r#"{"block_num_or_id":"zz"}"#,
        )
        .await;
        assert_eq!(zz.status, 500);
        assert_eq!(
            zz.body,
            ParamError::BadId {
                ep,
                include_extensions: false,
                echo: "zz".to_string()
            }
            .answer()
            .1
        );
        let empty = post(&api, "/v1/chain/get_raw_block", r#"{"block_num_or_id":""}"#).await;
        assert_eq!(
            (empty.status, empty.body),
            (500, ParamError::BadLength(ep).answer().1)
        );
        let none = post(&api, "/v1/chain/get_raw_block", "  ").await;
        assert_eq!(
            (none.status, none.body),
            (400, ParamError::BodyRequired.answer().1)
        );
        let float = post(
            &api,
            "/v1/chain/get_raw_block",
            r#"{"block_num_or_id":1.5}"#,
        )
        .await;
        assert_eq!(
            (float.status, float.body),
            (400, ParamError::Unparsable.answer().1)
        );
    }

    #[tokio::test]
    async fn requests_are_handled_in_nodeos_order() {
        let api = MockChainApi {
            extra_headers: vec![("Access-Control-Allow-Origin".into(), "*".into())],
            ..MockChainApi::live("live-1", CHAIN, 200, 190, Some(1), "v1.2.2")
        }
        .start()
        .await;
        let host = api.endpoint();
        let raw = |method: &str, target: &str| request(method, target, &host, b"");
        let illegal = [
            raw("OPTIONS", "*"),
            raw("GET", "http://127.0.0.1/v1/chain/get_info"),
            raw("POST", "/v1/chain/../chain/get_info"),
        ];
        for bytes in illegal {
            let answer = exchange(api.addr(), &bytes, WAIT).await.unwrap();
            assert_eq!(
                (answer.status, answer.text()),
                (400, ILLEGAL_TARGET.to_string())
            );
            // Written before the configured headers, but with Server.
            assert!(answer.header("access-control-allow-origin").is_none());
            assert_eq!(answer.header("server"), Some("nodeos/v1.2.2"));
        }
        // Host check: a name or another address is refused; the port is not
        // checked.
        for bad in ["localhost", "10.0.0.5:8888"] {
            let bytes = request("POST", "/v1/chain/get_info", bad, b"");
            let answer = exchange(api.addr(), &bytes, WAIT).await.unwrap();
            assert_eq!(
                (answer.status, answer.text()),
                (400, DISALLOWED_HOST.to_string())
            );
        }
        let other_port = request("POST", "/v1/chain/get_info", "127.0.0.1:1", b"");
        assert_eq!(
            exchange(api.addr(), &other_port, WAIT)
                .await
                .unwrap()
                .status,
            200
        );
        // OPTIONS on any legal target, with the configured headers.
        let options = exchange(api.addr(), &raw("OPTIONS", "/nowhere"), WAIT)
            .await
            .unwrap();
        assert_eq!((options.status, options.text()), (200, "{}".to_string()));
        assert_eq!(options.header("access-control-allow-origin"), Some("*"));
        // Exact dispatch: a query string or an unlisted path is the 404.
        for target in [
            "/v1/chain/get_info?x=1",
            "/v1/chain/get_unknown",
            "/v1/history/get_actions",
            "/v1/archive/describe",
        ] {
            let answer = post(&api, target, "").await;
            assert_eq!(
                (answer.status, answer.text()),
                (404, NOT_FOUND_BODY.to_string())
            );
            assert_eq!(answer.header("access-control-allow-origin"), Some("*"));
        }
        // Any method reaches the handler.
        let get = exchange(api.addr(), &raw("GET", "/v1/chain/get_info"), WAIT)
            .await
            .unwrap();
        assert_eq!(get.status, 200);
        // Other calls get the tagged answer.
        let call = post(&api, "/v1/chain/push_transaction", r#"{"signatures":[]}"#).await;
        assert_eq!(
            (call.status, call.body),
            (200, call_answer("live-1", "/v1/chain/push_transaction", 17))
        );
        // get_supported_apis lists the served set, sorted, never itself.
        let apis = post(&api, "/v1/node/get_supported_apis", "").await;
        let mut sorted: Vec<&str> = LIVE_APIS.to_vec();
        sorted.sort_unstable();
        assert_eq!(
            apis.text(),
            format!(
                r#"{{"apis":[{}]}}"#,
                sorted
                    .iter()
                    .map(|p| format!("\"{p}\""))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        );
        api.set_apis(Some(vec![
            "/v1/chain/get_info".into(),
            "/v1/trace_api/get_block".into(),
            "/v1/chain/get_block".into(),
        ]));
        assert_eq!(
            post(&api, "/v1/node/get_supported_apis", "").await.text(),
            r#"{"apis":["/v1/chain/get_block","/v1/chain/get_info","/v1/trace_api/get_block"]}"#
        );
        assert_eq!(
            post(&api, "/v1/chain/get_raw_block", "{}").await.status,
            404
        );
        assert_eq!(
            post(&api, "/v1/trace_api/get_block", "{}").await.body,
            call_answer("live-1", "/v1/trace_api/get_block", 2)
        );
        api.set_supported_apis(false);
        let gone = post(&api, "/v1/node/get_supported_apis", "").await;
        assert_eq!(
            (gone.status, gone.text()),
            (404, NOT_FOUND_BODY.to_string())
        );
    }

    #[tokio::test]
    async fn a_frozen_api_serves_its_block_log_only() {
        let api = frozen().await;
        let describe = |last: u32, id: &[u8; 32], begin: u32, end: u32, chain: &str| {
            format!(
                concat!(
                    r#"{{"server":"mock-frozen-api/{}","chain_id":"{}","last":{{"num":{},"id":"{}"}},"#,
                    r#""blocks":{{"begin":{},"end":{}}},"apis":["/v1/chain/get_block_header","#,
                    r#""/v1/chain/get_block_info","/v1/chain/get_raw_block"],"get_block":"off","#,
                    r#""max_in_flight":4,"in_flight":0}}"#,
                ),
                env!("CARGO_PKG_VERSION"),
                chain,
                last,
                hex(id),
                begin,
                end
            )
        };
        // describe: any method, body ignored, Host not checked.
        for method in ["GET", "POST"] {
            let bytes = request(method, "/v1/archive/describe", "anything", b"garbage");
            let answer = exchange(api.addr(), &bytes, WAIT).await.unwrap();
            assert_eq!(answer.status, 200);
            assert_eq!(
                answer.text(),
                describe(100, &fake_block_id(100), 1, 101, &"aa".repeat(32))
            );
            assert_eq!(
                answer.header("server"),
                Some(format!("mock-frozen-api/{}", env!("CARGO_PKG_VERSION")).as_str())
            );
        }
        assert_eq!(
            post(&api, "/v1/node/get_supported_apis", "").await.text(),
            r#"{"apis":["/v1/chain/get_block_header","/v1/chain/get_block_info","/v1/chain/get_raw_block"]}"#
        );
        // The first and last blocks are served; one past either end is not.
        let ok = |n: u32| block_answer("frozen-0a", n, &block_id(n));
        assert_eq!(
            post(&api, "/v1/chain/get_raw_block", r#"{"block_num_or_id":1}"#)
                .await
                .body,
            ok(1)
        );
        assert_eq!(
            post(
                &api,
                "/v1/chain/get_block_header",
                r#"{"block_num_or_id":"99"}"#
            )
            .await
            .body,
            ok(99)
        );
        for (target, body, echo) in [
            ("/v1/chain/get_raw_block", r#"{"block_num_or_id":0}"#, "0"),
            (
                "/v1/chain/get_raw_block",
                r#"{"block_num_or_id":101}"#,
                "101",
            ),
        ] {
            let answer = post(&api, target, body).await;
            assert_eq!(answer.status, 400);
            assert_eq!(
                answer.body,
                unknown_block_body(BlockEp::RawBlock, false, echo)
            );
        }
        // `last` carries the SHiP mock's id, by number and by that id.
        let last = block_answer("frozen-0a", 100, &fake_block_id(100));
        assert_eq!(
            post(&api, "/v1/chain/get_block_info", r#"{"block_num":100}"#)
                .await
                .body,
            last
        );
        let by_id = format!(r#"{{"block_num_or_id":"{}"}}"#, hex(&fake_block_id(100)));
        assert_eq!(
            post(&api, "/v1/chain/get_raw_block", &by_id).await.body,
            last
        );
        // Nothing else: get_info, state, push, get_block, get_block_header_state.
        for target in [
            "/v1/chain/get_info",
            "/v1/chain/get_account",
            "/v1/chain/push_transaction",
            "/v1/chain/get_block",
            "/v1/chain/get_block_header_state",
        ] {
            let answer = post(&api, target, r#"{"block_num_or_id":5}"#).await;
            assert_eq!(
                (answer.status, answer.text()),
                (404, NOT_FOUND_BODY.to_string())
            );
        }

        // Runtime changes show in describe and in the answers.
        api.set_last_mismatch(true);
        let mismatched = post(&api, "/v1/chain/get_block_info", r#"{"block_num":100}"#).await;
        assert_eq!(mismatched.status, 200);
        assert_ne!(mismatched.body, last);
        api.set_last_mismatch(false);
        api.set_blocks(50, 201);
        api.set_last(200, fake_block_id(200));
        api.set_chain_id([0xcc; 32]);
        let bytes = request("GET", "/v1/archive/describe", "x", b"");
        assert_eq!(
            exchange(api.addr(), &bytes, WAIT).await.unwrap().text(),
            describe(200, &fake_block_id(200), 50, 201, &"cc".repeat(32))
        );
        assert_eq!(
            post(&api, "/v1/chain/get_raw_block", r#"{"block_num_or_id":49}"#)
                .await
                .status,
            400
        );
        assert_eq!(
            post(
                &api,
                "/v1/chain/get_raw_block",
                r#"{"block_num_or_id":"100"}"#
            )
            .await
            .body,
            ok(100)
        );
    }

    #[tokio::test]
    async fn a_frozen_api_refuses_block_requests_past_max_in_flight() {
        let api = MockChainApi::frozen("frozen-0a", CHAIN, (1, 101), 100, 1)
            .start()
            .await;
        api.set_fault(
            Some("/v1/chain/get_raw_block"),
            Some(Fault::Delay(Duration::from_millis(400))),
        );
        let addr = api.addr();
        let host = api.endpoint();
        let slow = tokio::spawn(async move {
            let bytes = request(
                "POST",
                "/v1/chain/get_raw_block",
                &host,
                br#"{"block_num_or_id":5}"#,
            );
            exchange(addr, &bytes, WAIT).await.unwrap()
        });
        let deadline = tokio::time::Instant::now() + WAIT;
        while api.in_flight() == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the slow request never arrived"
            );
            sleep(Duration::from_millis(5)).await;
        }
        // The cap shows in describe, and a second block request is refused
        // at once, without waiting for the delay.
        assert!(post(&api, "/v1/archive/describe", "")
            .await
            .text()
            .ends_with(r#""max_in_flight":1,"in_flight":1}"#));
        let refused = post(&api, "/v1/chain/get_block_info", r#"{"block_num":5}"#).await;
        assert_eq!(refused.status, 503);
        assert_eq!(refused.text(), busy_body("Too many requests in flight: 1"));
        assert!(!slow.is_finished());
        let served = slow.await.unwrap();
        assert_eq!(served.body, block_answer("frozen-0a", 5, &block_id(5)));
        assert_eq!(
            (api.peak_in_flight(), api.rejected(), api.in_flight()),
            (1, 1, 0)
        );
        // Probes are never counted.
        api.set_max_in_flight(0);
        assert_eq!(post(&api, "/v1/archive/describe", "").await.status, 200);
        assert_eq!(
            post(&api, "/v1/node/get_supported_apis", "").await.status,
            200
        );
    }

    #[tokio::test]
    async fn faults_apply_per_target() {
        let api = live().await;
        let raw_block = "/v1/chain/get_raw_block";
        let body = r#"{"block_num_or_id":150}"#;

        api.set_fault(Some(raw_block), Some(Fault::Busy));
        assert_eq!(post(&api, raw_block, body).await.status, 503);
        assert_eq!(post(&api, "/v1/chain/get_info", "").await.status, 200);

        api.set_fault(Some(raw_block), Some(Fault::UnknownBlock));
        let unknown = post(&api, raw_block, body).await;
        assert_eq!(
            (unknown.status, unknown.body),
            (400, unknown_block_body(BlockEp::RawBlock, false, "150"))
        );
        // Parameter errors still come first.
        assert_eq!(post(&api, raw_block, "").await.status, 400);

        let big: Bytes = vec![b'x'; 3 * 1024 * 1024].into();
        api.set_fault(
            Some("/v1/chain/get_table_rows"),
            Some(Fault::Respond {
                status: 200,
                body: big.clone(),
            }),
        );
        let canned = post(&api, "/v1/chain/get_table_rows", "{}").await;
        assert_eq!((canned.status, canned.body.len()), (200, big.len()));
        assert!(canned.body == big);

        api.set_fault(
            Some(raw_block),
            Some(Fault::Delay(Duration::from_millis(200))),
        );
        let started = tokio::time::Instant::now();
        assert_eq!(post(&api, raw_block, body).await.status, 200);
        assert!(started.elapsed() >= Duration::from_millis(200));

        // A fault for every request yields to the target's own.
        api.set_fault(None, Some(Fault::Busy));
        assert_eq!(post(&api, "/v1/chain/get_info", "").await.status, 503);
        assert_eq!(post(&api, raw_block, body).await.status, 200);
        api.clear_faults();
        assert_eq!(post(&api, "/v1/chain/get_info", "").await.status, 200);

        // Dropped connections: no answer, but the request is recorded.
        let push = "/v1/chain/push_transaction";
        for (fault, recorded_body) in [
            (Fault::DropBeforeBody, Vec::new()),
            (Fault::DropAfterBody, b"{\"trx\":1}".to_vec()),
        ] {
            api.clear_requests();
            api.set_fault(Some(push), Some(fault.clone()));
            let mut conn = RawConn::connect(api.addr()).await.unwrap();
            conn.send(&request("POST", push, &api.endpoint(), b"{\"trx\":1}"))
                .await
                .unwrap();
            assert!(conn.closed(WAIT).await, "{fault:?}");
            let seen = api.requests_to(push);
            assert_eq!(seen.len(), 1, "{fault:?}");
            assert_eq!(seen[0].body, recorded_body, "{fault:?}");
        }
        api.set_fault(Some(push), None);
        assert_eq!(post(&api, push, "{}").await.status, 200);
    }

    #[tokio::test]
    async fn requests_are_recorded_byte_for_byte() {
        let api = frozen().await;
        let body = b"{\"block_num_or_id\":\"7\",  \"x\":\"\\u0031\"}";
        let mut bytes = format!(
            "POST /v1/chain/get_raw_block HTTP/1.1\r\nHost: 127.0.0.1:1\r\nUser-Agent: test\r\nX-Extra: a\r\nContent-Length: {}\r\n\r\n",
            body.len()
        )
        .into_bytes();
        bytes.extend_from_slice(body);
        assert_eq!(
            exchange(api.addr(), &bytes, WAIT).await.unwrap().status,
            200
        );
        let mut seen = api.requests();
        assert_eq!(seen.len(), 1);
        seen[0].headers.sort();
        assert_eq!(
            seen[0],
            ApiRequest {
                method: "POST".to_string(),
                target: "/v1/chain/get_raw_block".to_string(),
                host: Some("127.0.0.1:1".to_string()),
                headers: vec![
                    ("content-length".to_string(), body.len().to_string()),
                    ("host".to_string(), "127.0.0.1:1".to_string()),
                    ("user-agent".to_string(), "test".to_string()),
                    ("x-extra".to_string(), "a".to_string()),
                ],
                body: body.to_vec(),
            }
        );
        assert_eq!(api.requests()[0].header("USER-AGENT"), Some("test"));
        let star = request("OPTIONS", "*", "x", b"");
        exchange(api.addr(), &star, WAIT).await.unwrap();
        assert_eq!(api.requests_to("*").len(), 1);
        api.clear_requests();
        assert!(api.requests().is_empty());
    }

    #[tokio::test]
    async fn connections_stay_open_until_dropped() {
        let api = live().await;
        let mut conn = RawConn::connect(api.addr()).await.unwrap();
        for n in [150u32, 151, 152] {
            let body = format!(r#"{{"block_num":{n}}}"#);
            conn.send(&request(
                "POST",
                "/v1/chain/get_block_info",
                &api.endpoint(),
                body.as_bytes(),
            ))
            .await
            .unwrap();
            let answer = conn.response(WAIT).await.unwrap();
            assert_eq!(answer.body, block_answer("live-1", n, &block_id(n)));
        }
        assert_eq!(api.connections_accepted(), 1);
        // Connection: close is honoured.
        let mut closing = RawConn::connect(api.addr()).await.unwrap();
        closing
            .send(
                format!(
                    "GET /v1/chain/get_info HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
                    api.endpoint()
                )
                .as_bytes(),
            )
            .await
            .unwrap();
        assert_eq!(closing.response(WAIT).await.unwrap().status, 200);
        assert!(closing.closed(WAIT).await);

        api.drop_connections();
        assert!(conn.closed(WAIT).await);
        assert_eq!(post(&api, "/v1/chain/get_info", "").await.status, 200);

        let mut open = RawConn::connect(api.addr()).await.unwrap();
        open.send(&request("POST", "/v1/chain/get_info", &api.endpoint(), b""))
            .await
            .unwrap();
        open.response(WAIT).await.unwrap();
        let addr = api.addr();
        api.shutdown().await;
        assert!(open.closed(WAIT).await);
        assert!(matches!(
            timeout(Duration::from_secs(5), TcpStream::connect(addr)).await,
            Ok(Err(_))
        ));
    }

    #[tokio::test]
    async fn the_raw_client_reads_chunked_and_interim_responses() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5;x=1\r\nhello\r\n6\r\n world\r\n0\r\nTrailer: t\r\n\r\nHTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        });
        let mut conn = RawConn::connect(addr).await.unwrap();
        conn.send(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
        let chunked = conn.response(WAIT).await.unwrap();
        assert_eq!(
            (chunked.status, chunked.text()),
            (200, "hello world".to_string())
        );
        assert_eq!(chunked.header("TRANSFER-ENCODING"), Some("chunked"));
        // No length and no chunks: the body runs to the end of the connection.
        let to_end = conn.response(WAIT).await.unwrap();
        assert_eq!((to_end.status, to_end.body), (204, Vec::new()));
        server.await.unwrap();
    }
}
