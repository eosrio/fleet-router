//! An opt-in, nodeos-compatible HTTP chain API in front of a listener's
//! servers.
//!
//! Block endpoints are routed by block number. An irreversible block goes to
//! the frozen range server whose block log holds it, falling back to live
//! nodes; a reversible block goes to live nodes only, because fork answers
//! must come from one node's fork database. `get_info` and read-only state
//! calls are proxied unchanged to live nodes; push and exec are off unless
//! allowed. The router never rewrites a response body and never invents a
//! parameter error or an Unknown Block: when no upstream can answer it says
//! Busy, in nodeos's error shape.
//!
//! The decisions are pure functions, kept apart from the runtime:
//! - [`route`] classifies the raw request-target the way nodeos dispatches it;
//! - [`hint`] reads the block number from a body, only when nodeos would read
//!   the same block from it;
//! - [`pick`] decides a block's region, orders the candidate upstream APIs and
//!   classifies each attempt's answer;
//! - [`errors`] holds the bodies the router writes itself.
//!
//! The runtime:
//! - [`directory`] holds what the router knows about each upstream API;
//! - [`probe`] keeps it current, one task per API;
//! - [`upstream`] sends the attempts;
//! - this module serves the listener: one hyper HTTP/1.1 connection per
//!   client, nodeos's framing (no `Date`, title-case header names), and the
//!   request flows of each endpoint class;
//! - [`socket`] cuts off a client that takes none of its answer;
//! - [`stats`] counts what happens.

pub mod directory;
pub mod errors;
pub mod hint;
pub mod pick;
pub mod probe;
pub mod route;
pub mod socket;
pub mod stats;
pub mod upstream;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use http_body_util::BodyExt;
use hyper::body::{Body, Incoming};
use hyper::header::{
    HeaderMap, HeaderName, HeaderValue, ACCESS_CONTROL_ALLOW_CREDENTIALS,
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_MAX_AGE, CONNECTION,
    CONTENT_TYPE,
};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::timeout_at;

use crate::fleet::{FleetHub, MetricLabel};
use crate::models::{ApiClass, ChainApiConfig, ChainApiCors, ResolvedChainApi, Role};

use directory::{ApiDirectory, ApiSlot, Snapshot};
use errors::{Busy, RouterError, OPTIONS_BODY};
use pick::{
    classify, exhausted, info_order, info_primary, keeps, needs_peek, next_attempt, plan_block,
    push_targets, settles, state_order, Answer, ApiView, BlockPlan, BlockQuery, NextAttempt,
    Outcome, Region, RetryReason, StateQuery, PEEK_LIMIT,
};
use route::{BlockEndpoint, Route};
use socket::Socket;
use stats::{AnswerRole, ApiStats, BlockRegion, PushResult, Rejection, UpstreamResult};
use upstream::{
    full, peek, streamed, ApiBody, Attempt, Buffered, Call, Hold, Peeked, Replay, StallGuard,
    UpstreamClient,
};

/// Largest request head and buffered body chunk read from a client.
const MAX_BUF: usize = 16 * 1024;
/// Most request headers a client may send.
const MAX_HEADERS: usize = 64;
/// The client header and body read time, and the longest a write to a
/// client may wait with no progress, when `handshake_timeout_ms` is 0.
const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(10);
/// The pause after an accept error other than a failed connection (out of
/// file descriptors, say): retrying at once would spin.
const ACCEPT_BACKOFF: Duration = Duration::from_secs(1);
/// Idle connections kept per upstream API.
const POOL_PER_UPSTREAM: usize = 32;
/// Idle connections the probes keep per upstream API.
const PROBE_POOL_PER_UPSTREAM: usize = 2;
/// Times a request re-reads the candidates after losing a free slot to
/// another request, before it answers Busy.
const SLOT_RACES: usize = 16;

/// A request's hold on the listener's `max_in_flight`; counted in the
/// in-flight gauge while held.
pub struct RequestPermit {
    _permit: OwnedSemaphorePermit,
    stats: Arc<ApiStats>,
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        self.stats.leave();
    }
}

/// CORS headers set on every response, parsed once.
#[derive(Clone, Debug)]
pub struct CorsHeaders {
    origin: HeaderValue,
    headers: Option<HeaderValue>,
    max_age: Option<HeaderValue>,
    credentials: bool,
}

impl CorsHeaders {
    /// `None` when a value is not a valid header value (validation refuses
    /// such configs).
    pub fn new(cors: &ChainApiCors) -> Option<CorsHeaders> {
        Some(CorsHeaders {
            origin: HeaderValue::from_str(&cors.allow_origin).ok()?,
            headers: match &cors.allow_headers {
                Some(h) => Some(HeaderValue::from_str(h).ok()?),
                None => None,
            },
            max_age: cors.max_age.map(HeaderValue::from),
            credentials: cors.allow_credentials,
        })
    }
}

/// Make a response's CORS headers the router's: every upstream
/// `Access-Control-*` header is removed, then the configured ones are set.
/// Without `cors` the answer has none, like a nodeos with no CORS
/// configured. `Access-Control-Allow-Methods` is never set (nodeos sets
/// none). The other headers keep their order.
pub fn apply_cors(headers: &mut HeaderMap, cors: Option<&CorsHeaders>) {
    let is_cors = |name: &HeaderName| name.as_str().starts_with("access-control-");
    if headers.keys().any(is_cors) {
        // Rebuilt rather than removed from: `HeaderMap::remove` moves the
        // last entry into the gap, which would reorder the relayed head.
        let mut current: Option<HeaderName> = None;
        for (name, value) in std::mem::take(headers) {
            // `None` continues the previous name.
            if name.is_some() {
                current = name;
            }
            if let Some(name) = current.as_ref().filter(|name| !is_cors(name)) {
                headers.append(name.clone(), value);
            }
        }
    }
    let Some(cors) = cors else {
        return;
    };
    headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, cors.origin.clone());
    if let Some(value) = &cors.headers {
        headers.insert(ACCESS_CONTROL_ALLOW_HEADERS, value.clone());
    }
    if let Some(value) = &cors.max_age {
        headers.insert(ACCESS_CONTROL_MAX_AGE, value.clone());
    }
    if cors.credentials {
        headers.insert(
            ACCESS_CONTROL_ALLOW_CREDENTIALS,
            HeaderValue::from_static("true"),
        );
    }
}

/// The router's `get_supported_apis` body: `{"apis":[...]}`, the sorted
/// union of the paths `sets` advertise that this listener serves. It never
/// lists itself and never lists a path no upstream advertises.
pub fn supported_apis_body<'a>(
    sets: impl IntoIterator<Item = &'a BTreeSet<String>>,
    allow: &[ApiClass],
) -> Vec<u8> {
    let paths: BTreeSet<&str> = sets
        .into_iter()
        .flatten()
        .map(String::as_str)
        .filter(|path| route::serves_path(path, allow))
        .collect();
    let items: Vec<String> = paths
        .into_iter()
        .map(|p| serde_json::to_string(p).expect("a string always serializes"))
        .collect();
    format!(r#"{{"apis":[{}]}}"#, items.join(",")).into_bytes()
}

/// A JSON answer the router writes itself: no `Server`, no `Date`.
fn generated(status: u16, body: impl Into<Bytes>) -> Response<ApiBody> {
    let mut response = Response::new(full(body));
    *response.status_mut() = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn router_error(err: RouterError) -> Response<ApiBody> {
    generated(err.status(), err.body())
}

fn busy(why: Busy) -> Response<ApiBody> {
    router_error(RouterError::Busy(why))
}

/// How the attempts of one request move on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Policy<'a> {
    /// A hinted block request in a region ([`classify`]). `claimed`: a
    /// frozen range server claims the block, so no Unknown Block is kept
    /// as the answer ([`keeps`]). An Unknown Block from one of `id_holders`
    /// is the answer ([`settles`]).
    Block {
        region: Region,
        claimed: bool,
        id_holders: &'a [usize],
    },
    /// State calls and unhinted block bodies: a transport failure or a 503
    /// moves on, any other answer is final.
    State,
    /// `get_info`: also a 5xx moves on while candidates remain.
    Info,
    /// Exec: only a connect error moves on; nothing is sent twice.
    Exec,
}

/// The answer to a proxied request and who wrote it.
struct Answered {
    response: Response<ApiBody>,
    role: Option<Role>,
    /// The API that answered.
    upstream: Option<Arc<ApiSlot>>,
    /// Attempts made.
    tries: usize,
}

impl Answered {
    fn router(response: Response<ApiBody>, tries: usize) -> Answered {
        Answered {
            response,
            role: None,
            upstream: None,
            tries,
        }
    }
}

/// Why a connection closes without an answer: the request body did not
/// arrive in time, or could not be read.
#[derive(Debug)]
pub struct Unanswered(&'static str);

impl std::fmt::Display for Unanswered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for Unanswered {}

/// What a client body read ended with, other than the body.
enum BodyError {
    TooLarge,
    Timeout,
    Failed,
}

/// Whether an accept error is about one connection only (it failed before
/// the accept completed), so the next accept can follow at once.
fn connection_error(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::Interrupted
    )
}

/// The upstream result an answer counts as.
fn result_of(status: u16, reason: Option<RetryReason>) -> UpstreamResult {
    match (reason, status) {
        (Some(RetryReason::UnknownBlock), _) => UpstreamResult::UnknownBlock,
        (Some(RetryReason::NotFound), _) | (None, 404) => UpstreamResult::NotFound,
        (_, 503) => UpstreamResult::Busy,
        (_, 200..=299) => UpstreamResult::Ok,
        _ => UpstreamResult::Error,
    }
}

fn answer_role(role: Option<Role>) -> AnswerRole {
    match role {
        Some(Role::Live) => AnswerRole::Live,
        Some(Role::Frozen) => AnswerRole::Frozen,
        None => AnswerRole::None,
    }
}

/// One listener's chain API.
pub struct ChainApiCtx {
    pub listener: Option<Arc<str>>,
    /// `host:port` it is bound to.
    pub bind: String,
    cfg: ChainApiConfig,
    hub: Arc<FleetHub>,
    pub directory: Arc<ApiDirectory>,
    pub stats: Arc<ApiStats>,
    client: UpstreamClient,
    probe_client: UpstreamClient,
    in_flight: Arc<Semaphore>,
    /// One permit per open client connection.
    pub connections: Arc<Semaphore>,
    cors: Option<CorsHeaders>,
    /// `ServerState::index` of each `push_servers` entry.
    push: Vec<usize>,
    live_lag_blocks: u32,
    /// Client header read, body read after the headers, and the longest a
    /// write to the client may wait with no progress.
    read_timeout: Duration,
    /// The sticky `get_info` primary.
    primary: Mutex<Option<usize>>,
    /// Round-robin position of state calls.
    rr: AtomicU64,
}

impl ChainApiCtx {
    /// `connect_timeout` bounds upstream connects; `handshake_timeout_ms`
    /// (10 s when 0) bounds a client's request head and body.
    pub fn new(
        api: &ResolvedChainApi,
        hub: Arc<FleetHub>,
        connect_timeout: Duration,
        handshake_timeout_ms: u64,
        live_lag_blocks: u32,
    ) -> Arc<ChainApiCtx> {
        let cfg = api.cfg.clone();
        Arc::new(ChainApiCtx {
            listener: api.listener.as_deref().map(Arc::from),
            bind: api.bind.clone(),
            hub,
            directory: Arc::new(ApiDirectory::new(
                &api.upstreams,
                cfg.max_in_flight_per_upstream,
            )),
            stats: Arc::new(ApiStats::default()),
            client: UpstreamClient::new(connect_timeout, POOL_PER_UPSTREAM),
            probe_client: UpstreamClient::new(connect_timeout, PROBE_POOL_PER_UPSTREAM),
            in_flight: Arc::new(Semaphore::new(cfg.max_in_flight)),
            connections: Arc::new(Semaphore::new(cfg.max_connections)),
            cors: cfg.cors.as_ref().and_then(CorsHeaders::new),
            push: api.push.clone(),
            live_lag_blocks,
            read_timeout: match handshake_timeout_ms {
                0 => DEFAULT_READ_TIMEOUT,
                ms => Duration::from_millis(ms),
            },
            primary: Mutex::new(None),
            rr: AtomicU64::new(0),
            cfg,
        })
    }

    pub fn max_connections(&self) -> usize {
        self.cfg.max_connections
    }

    /// Start one probe per upstream API and serve `listener` until the
    /// shutdown broadcast. The returned task ends once the listener stops
    /// accepting; open connections finish their request and close.
    pub fn spawn(
        self: &Arc<Self>,
        listener: TcpListener,
        shutdown: &broadcast::Sender<()>,
    ) -> JoinHandle<()> {
        let prober = Arc::new(probe::Prober {
            listener: self.listener.clone(),
            client: self.probe_client.clone(),
            hub: self.hub.clone(),
            directory: self.directory.clone(),
            every: Duration::from_millis(self.cfg.probe_ms),
            timeout: Duration::from_millis(self.cfg.upstream_timeout_ms),
            per_upstream: self.cfg.max_in_flight_per_upstream,
        });
        for slot in self.directory.slots() {
            tokio::spawn(prober.clone().run(slot.clone(), shutdown.subscribe()));
        }
        tokio::spawn(self.clone().serve(listener, shutdown.subscribe()))
    }

    async fn serve(self: Arc<Self>, listener: TcpListener, mut shutdown: broadcast::Receiver<()>) {
        self.stats.set_bound(true);
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, peer) = match accepted {
                        Ok(pair) => pair,
                        Err(e) if connection_error(&e) => {
                            tracing::debug!(error = %e, "chain API connection failed before its accept");
                            continue;
                        }
                        Err(e) => {
                            // Out of file descriptors, say: the pending
                            // connection stays queued, so an immediate retry
                            // would spin.
                            tracing::warn!(error = %e, "chain API accept error; pausing accepts");
                            tokio::select! {
                                _ = tokio::time::sleep(ACCEPT_BACKOFF) => continue,
                                _ = shutdown.recv() => break,
                            }
                        }
                    };
                    // Excess connections are closed at once.
                    let Ok(permit) = self.connections.clone().try_acquire_owned() else {
                        self.stats.rejected.inc(Rejection::Connections);
                        tracing::debug!(%peer, max = self.cfg.max_connections, "chain API connection limit reached; closing");
                        drop(stream);
                        continue;
                    };
                    let ctx = self.clone();
                    let conn_shutdown = shutdown.resubscribe();
                    tokio::spawn(async move {
                        let _permit = permit;
                        ctx.connection(stream, conn_shutdown).await;
                    });
                }
                _ = shutdown.recv() => break,
            }
        }
        self.stats.set_bound(false);
        tracing::debug!(address = %self.bind, "chain API stopped accepting");
    }

    /// Serve one client connection: HTTP/1.1, keep-alive, nodeos framing.
    /// On shutdown, the request in progress finishes and the connection
    /// closes. A write that waits the read timeout with no progress closes
    /// the connection ([`Socket`]).
    async fn connection(
        self: Arc<Self>,
        stream: tokio::net::TcpStream,
        mut shutdown: broadcast::Receiver<()>,
    ) {
        let _ = stream.set_nodelay(true);
        let stream = Socket::new(stream, self.read_timeout, self.stats.clone());
        let ctx = self.clone();
        let service = service_fn(move |request| {
            let ctx = ctx.clone();
            async move { ctx.handle(request).await }
        });
        let mut builder = http1::Builder::new();
        builder
            .timer(TokioTimer::new())
            .header_read_timeout(self.read_timeout)
            .auto_date_header(false)
            .title_case_headers(true)
            .max_buf_size(MAX_BUF)
            .max_headers(MAX_HEADERS)
            .keep_alive(true);
        let conn = builder.serve_connection(TokioIo::new(stream), service);
        tokio::pin!(conn);
        tokio::select! {
            result = conn.as_mut() => {
                if let Err(e) = result {
                    tracing::debug!(error = %e, "chain API connection ended");
                }
            }
            _ = shutdown.recv() => {
                conn.as_mut().graceful_shutdown();
                let _ = conn.await;
            }
        }
    }

    /// Answer one request. An `Err` closes the connection without an
    /// answer (a body that did not arrive in time).
    ///
    /// The whole body is read first, as nodeos reads it before it handles a
    /// request, for the answers the router writes itself too: hyper closes
    /// a connection whose request body was left unread, which resets a
    /// client still writing it and loses the keep-alive connection.
    async fn handle(
        self: Arc<Self>,
        request: Request<Incoming>,
    ) -> Result<Response<ApiBody>, Unanswered> {
        let started = Instant::now();
        let route = route::classify(request.method(), request.uri(), &self.cfg.allow);
        let (parts, body) = request.into_parts();
        let body = match self.read_body(body, route.class().is_some()).await {
            Ok(body) => body,
            Err(BodyError::TooLarge) => {
                self.stats.rejected.inc(Rejection::BodyTooLarge);
                let mut response = router_error(RouterError::TooLarge(self.cfg.max_body_bytes));
                // The rest of the body is never read: the connection closes.
                response
                    .headers_mut()
                    .insert(CONNECTION, HeaderValue::from_static("close"));
                return Ok(self.finish(route, started, Answered::router(response, 0)));
            }
            Err(BodyError::Timeout) => {
                self.stats.rejected.inc(Rejection::Timeout);
                return Err(Unanswered("request body not received in time"));
            }
            Err(BodyError::Failed) => return Err(Unanswered("request body read failed")),
        };
        let answered = match route {
            Route::IllegalTarget => Answered::router(router_error(RouterError::IllegalTarget), 0),
            Route::Options => Answered::router(generated(200, OPTIONS_BODY), 0),
            Route::SupportedApis => Answered::router(generated(200, self.supported_apis()), 0),
            Route::NotFound => Answered::router(router_error(RouterError::NotFound), 0),
            Route::Block(_) | Route::Info | Route::State | Route::Exec | Route::Push => {
                self.proxy(route, parts, body).await
            }
        };
        Ok(self.finish(route, started, answered))
    }

    /// The CORS headers, the counters and the log line of an answer.
    fn finish(&self, route: Route, started: Instant, answered: Answered) -> Response<ApiBody> {
        let mut response = answered.response;
        // nodeos checks the target before it sets its CORS headers, so its
        // "Illegal request-target" answer never carries them.
        let cors = self.cors.as_ref().filter(|_| route != Route::IllegalTarget);
        apply_cors(response.headers_mut(), cors);
        let status = response.status().as_u16();
        self.stats.request(route.label(), status, started.elapsed());
        tracing::debug!(
            class = route.label().as_str(),
            status,
            upstream = answered.upstream.as_ref().map(|s| &*s.name),
            tries = answered.tries,
            micros = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
            "chain API request"
        );
        response
    }

    /// The router's `get_supported_apis`: what the APIs a request may use
    /// advertise.
    fn supported_apis(&self) -> Vec<u8> {
        let snaps = self.snapshot();
        let sets = snaps
            .iter()
            .filter(|s| s.up && (s.role.is_live() || s.verified))
            .filter_map(|s| s.apis.as_deref());
        supported_apis_body(sets, &self.cfg.allow)
    }

    /// Take an in-flight permit and run the route's flow with the body.
    async fn proxy(
        &self,
        route: Route,
        parts: hyper::http::request::Parts,
        body: Bytes,
    ) -> Answered {
        let target = route::origin_target(&parts.uri).unwrap_or("/").to_string();
        let Ok(permit) = self.in_flight.clone().try_acquire_owned() else {
            self.stats.rejected.inc(Rejection::InFlight);
            return Answered::router(busy(Busy::InFlight(self.cfg.max_in_flight)), 0);
        };
        self.stats.enter();
        let mut permit = Some(RequestPermit {
            _permit: permit,
            stats: self.stats.clone(),
        });
        let call = Call::new(parts.method, &target, &parts.headers, body);
        let deadline = Instant::now() + Duration::from_millis(self.cfg.request_deadline_ms);
        match route {
            Route::Block(ep) => self.block(ep, &call, deadline, &mut permit).await,
            Route::Info => self.info(&call, deadline, &mut permit).await,
            Route::State => {
                self.state(&call, &call.target, Policy::State, deadline, &mut permit)
                    .await
            }
            Route::Exec => {
                self.state(&call, &call.target, Policy::Exec, deadline, &mut permit)
                    .await
            }
            Route::Push => self.push(&call, deadline, &mut permit).await,
            _ => Answered::router(router_error(RouterError::NotFound), 0),
        }
    }

    /// The whole body, within `max_body_bytes` and the read timeout. Without
    /// `keep` it is read and dropped: the router answers such a request
    /// itself.
    async fn read_body(&self, mut body: Incoming, keep: bool) -> Result<Bytes, BodyError> {
        let max = self.cfg.max_body_bytes;
        if body.size_hint().exact().is_some_and(|len| len > max as u64) {
            return Err(BodyError::TooLarge);
        }
        let read = async {
            let mut buf = BytesMut::new();
            let mut len = 0usize;
            while let Some(frame) = body.frame().await {
                let frame = frame.map_err(|_| BodyError::Failed)?;
                if let Ok(data) = frame.into_data() {
                    len += data.len();
                    if len > max {
                        return Err(BodyError::TooLarge);
                    }
                    if keep {
                        buf.extend_from_slice(&data);
                    }
                }
            }
            Ok(buf.freeze())
        };
        let deadline = tokio::time::Instant::now() + self.read_timeout;
        timeout_at(deadline, read)
            .await
            .unwrap_or(Err(BodyError::Timeout))
    }

    /// Every upstream API as a request sees it now.
    pub fn snapshot(&self) -> Vec<Snapshot> {
        self.directory.snapshot(&self.hub, Instant::now())
    }

    /// The fleet head and LIB of the listener's SHiP view, as block numbers.
    fn ship_tip(&self) -> (Option<u32>, Option<u32>) {
        let view = self.hub.view();
        let (head, lib) = self.hub.fresh_tip(&view);
        (head.map(|p| p.num), lib.map(|p| p.num))
    }

    /// The `get_info` primary, kept sticky across requests.
    fn primary(&self, views: &[ApiView<'_>]) -> Option<usize> {
        let mut current = self
            .primary
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *current = info_primary(views, *current);
        *current
    }

    async fn block(
        &self,
        ep: BlockEndpoint,
        call: &Call,
        deadline: Instant,
        permit: &mut Option<RequestPermit>,
    ) -> Answered {
        let snaps = self.snapshot();
        let views: Vec<ApiView<'_>> = snaps.iter().map(Snapshot::view).collect();
        let (_, ship_lib) = self.ship_tip();
        let query = BlockQuery {
            endpoint: ep,
            hint: hint::hint(ep, &call.body),
            ship_lib,
            block_source: self.cfg.block_source,
            max_attempts: self.cfg.max_attempts,
        };
        match plan_block(&query, &views) {
            BlockPlan::Unhinted => {
                // One live API answers the body as it is, never a frozen one.
                let answered = self
                    .state(call, ep.path(), Policy::State, deadline, permit)
                    .await;
                self.stats
                    .block_request(BlockRegion::Unhinted, answer_role(answered.role));
                answered
            }
            BlockPlan::Routed {
                n,
                region,
                order,
                claimed,
                id_holders,
            } => {
                drop(views);
                let answered = if order.is_empty() {
                    Answered::router(busy(Busy::NoUpstream), 0)
                } else {
                    self.attempts(
                        call,
                        &order,
                        Policy::Block {
                            region,
                            claimed,
                            id_holders: &id_holders,
                        },
                        Busy::NoUpstream,
                        deadline,
                        permit,
                    )
                    .await
                };
                let label = match region {
                    Region::Irreversible => BlockRegion::Irreversible,
                    Region::Reversible => BlockRegion::Reversible,
                };
                self.stats.block_request(label, answer_role(answered.role));
                tracing::debug!(
                    endpoint = ep.path(),
                    block = n,
                    region = region.as_str(),
                    candidates = order.len(),
                    "chain API block request"
                );
                answered
            }
        }
    }

    async fn info(
        &self,
        call: &Call,
        deadline: Instant,
        permit: &mut Option<RequestPermit>,
    ) -> Answered {
        let snaps = self.snapshot();
        let views: Vec<ApiView<'_>> = snaps.iter().map(Snapshot::view).collect();
        let primary = self.primary(&views);
        let order = info_order(&views, primary, self.cfg.max_attempts);
        drop(views);
        if order.is_empty() {
            return Answered::router(busy(Busy::NoLiveUpstream), 0);
        }
        self.attempts(
            call,
            &order,
            Policy::Info,
            Busy::NoLiveUpstream,
            deadline,
            permit,
        )
        .await
    }

    /// State and exec calls, and unhinted block bodies: live APIs that list
    /// `path`, laggards skipped.
    async fn state(
        &self,
        call: &Call,
        path: &str,
        policy: Policy<'_>,
        deadline: Instant,
        permit: &mut Option<RequestPermit>,
    ) -> Answered {
        let snaps = self.snapshot();
        let views: Vec<ApiView<'_>> = snaps.iter().map(Snapshot::view).collect();
        let (ship_head, _) = self.ship_tip();
        let query = StateQuery {
            path,
            ship_head,
            lag: self.live_lag_blocks,
            rr: self.rr.fetch_add(1, Ordering::Relaxed),
            max_attempts: self.cfg.max_attempts,
        };
        let order = state_order(&query, &views);
        drop(views);
        if order.is_empty() {
            return Answered::router(busy(Busy::NoLiveUpstream), 0);
        }
        self.attempts(call, &order, policy, Busy::NoUpstream, deadline, permit)
            .await
    }

    /// Run the attempts of one request over `order`.
    async fn attempts(
        &self,
        call: &Call,
        order: &[usize],
        policy: Policy<'_>,
        exhausted_why: Busy,
        deadline: Instant,
        permit: &mut Option<RequestPermit>,
    ) -> Answered {
        let queue = Duration::from_millis(self.cfg.upstream_queue_ms);
        let head_timeout = Duration::from_millis(self.cfg.upstream_timeout_ms);
        let mut tried: Vec<usize> = Vec::new();
        let mut kept: Vec<(Role, Buffered)> = Vec::new();
        let mut lost_after_send = false;
        let mut races = 0;
        loop {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let full = |i: usize| self.directory.slot(i).is_none_or(|s| s.is_full());
            let (slot, guard) = match next_attempt(order, &tried, full) {
                NextAttempt::Exhausted => break,
                NextAttempt::Try(i) => {
                    let Some(slot) = self.directory.slot(i) else {
                        tried.push(i);
                        continue;
                    };
                    match slot.try_acquire() {
                        Some(guard) => (slot.clone(), guard),
                        None if races < SLOT_RACES => {
                            races += 1;
                            continue;
                        }
                        None => break,
                    }
                }
                NextAttempt::Wait(i) => {
                    let Some(slot) = self.directory.slot(i) else {
                        break;
                    };
                    let until = (now + queue).min(deadline);
                    match slot.acquire_until(until.into()).await {
                        Some(guard) => (slot.clone(), guard),
                        None => break,
                    }
                }
            };
            tried.push(slot.index);
            let Ok(request) = call.request_for(&slot) else {
                continue;
            };
            let remaining = deadline.saturating_duration_since(Instant::now());
            // The deadline leaves this attempt less than its own timeout: a
            // timeout is then the request's, not the upstream's.
            let cut_short = remaining < head_timeout;
            let attempt = self.client.send(request, head_timeout.min(remaining)).await;
            let response = match attempt {
                Attempt::Response(response) => response,
                Attempt::Connect => {
                    self.transport_failed(&slot, Answer::Connect, UpstreamResult::Connect);
                    continue;
                }
                Attempt::Lost | Attempt::Timeout if policy == Policy::Exec => {
                    // The request may have been executed: never sent twice.
                    let result = if matches!(attempt, Attempt::Timeout) {
                        UpstreamResult::Timeout
                    } else {
                        UpstreamResult::Connect
                    };
                    slot.results.inc(result);
                    lost_after_send = true;
                    break;
                }
                Attempt::ConnectTimeout | Attempt::Timeout if cut_short => {
                    // The deadline passed: no attempt is left, and the API
                    // is not taken down for it.
                    slot.results.inc(UpstreamResult::Timeout);
                    break;
                }
                Attempt::Lost => {
                    self.transport_failed(&slot, Answer::Connect, UpstreamResult::Connect);
                    continue;
                }
                Attempt::ConnectTimeout | Attempt::Timeout => {
                    self.transport_failed(&slot, Answer::Timeout, UpstreamResult::Timeout);
                    continue;
                }
            };
            let status = response.status().as_u16();
            let (region, claimed, id_holders) = match policy {
                Policy::Block {
                    region,
                    claimed,
                    id_holders,
                } => (region, claimed, id_holders),
                Policy::State | Policy::Info | Policy::Exec => (Region::Reversible, false, &[][..]),
            };
            let more = order.iter().any(|i| !tried.contains(i));
            // get_info fails over on any 5xx while another node may answer.
            if policy == Policy::Info && status >= 500 && more {
                slot.results.inc(result_of(status, None));
                let reason = if status == 503 {
                    RetryReason::Busy
                } else {
                    RetryReason::Error
                };
                self.moved_on(&slot, reason, reason.marks_busy());
                continue;
            }
            if policy == Policy::Exec {
                slot.results.inc(result_of(status, None));
                return self.answered(
                    streamed(response, self.hold(permit, Some(guard)), head_timeout),
                    &slot,
                    tried.len(),
                );
            }
            let length = response.body().size_hint().exact();
            if !needs_peek(region, status, length) {
                match classify(region, Answer::Response { status, body: None }) {
                    Outcome::Final => {
                        slot.results.inc(result_of(status, None));
                        return self.answered(
                            streamed(response, self.hold(permit, Some(guard)), head_timeout),
                            &slot,
                            tried.len(),
                        );
                    }
                    Outcome::Next { reason, .. } => {
                        slot.results.inc(result_of(status, Some(reason)));
                        self.moved_on(&slot, reason, reason.marks_busy());
                        continue;
                    }
                }
            }
            // A 400 or 404 on an irreversible block: read it to classify it.
            let (parts, body) = response.into_parts();
            let peeked = timeout_at(deadline.into(), peek(body, PEEK_LIMIT)).await;
            let bytes = match peeked {
                Ok(Ok(Peeked::Whole(bytes))) => bytes,
                Ok(Ok(Peeked::Partial { prefix, rest })) => {
                    // Larger than anything worth classifying: final.
                    slot.results.inc(result_of(status, None));
                    let mut out = Response::new(upstream::Held::body(
                        StallGuard::body(Replay::body(prefix, rest), head_timeout),
                        self.hold(permit, Some(guard)),
                    ));
                    *out.status_mut() = parts.status;
                    *out.headers_mut() = upstream::relay_headers(&parts.headers);
                    return self.answered(out, &slot, tried.len());
                }
                // The connection broke while the body was read.
                Ok(Err(_)) => {
                    self.transport_failed(&slot, Answer::Connect, UpstreamResult::Connect);
                    continue;
                }
                // The request's deadline passed: no attempt is left.
                Err(_) => {
                    slot.results.inc(UpstreamResult::Timeout);
                    break;
                }
            };
            let buffered = Buffered {
                status,
                headers: upstream::relay_headers(&parts.headers),
                body: bytes,
            };
            match classify(
                region,
                Answer::Response {
                    status,
                    body: Some(&buffered.body),
                },
            ) {
                Outcome::Final => {
                    slot.results.inc(result_of(status, None));
                    return self.answered(
                        buffered.into_response(self.hold(permit, Some(guard))),
                        &slot,
                        tried.len(),
                    );
                }
                Outcome::Next { reason, keep } => {
                    slot.results.inc(result_of(status, Some(reason)));
                    if settles(reason, id_holders.contains(&slot.index)) {
                        // An archive that holds the block refused the id:
                        // no upstream holds a block with that id.
                        return self.answered(
                            buffered.into_response(self.hold(permit, Some(guard))),
                            &slot,
                            tried.len(),
                        );
                    }
                    self.moved_on(&slot, reason, false);
                    if keep && keeps(reason, claimed) {
                        kept.push((slot.role, buffered));
                    }
                }
            }
        }
        let tries = tried.len();
        if lost_after_send {
            return Answered::router(busy(Busy::LostAfterSend), tries);
        }
        // Every attempt is used: the last kept answer from a live node, else
        // the last kept answer, else Busy.
        let roles: Vec<(Role, usize)> = kept
            .iter()
            .enumerate()
            .map(|(i, (role, _))| (*role, i))
            .collect();
        match exhausted(&roles) {
            Some(&i) => {
                let (role, buffered) = kept.swap_remove(i);
                Answered {
                    response: buffered.into_response(self.hold(permit, None)),
                    role: Some(role),
                    upstream: None,
                    tries,
                }
            }
            None => Answered::router(busy(exhausted_why), tries),
        }
    }

    fn hold(&self, permit: &mut Option<RequestPermit>, slot: Option<directory::SlotGuard>) -> Hold {
        Hold::new(permit.take(), slot)
    }

    fn answered(&self, response: Response<ApiBody>, slot: &Arc<ApiSlot>, tries: usize) -> Answered {
        Answered {
            response,
            role: Some(slot.role),
            upstream: Some(slot.clone()),
            tries,
        }
    }

    /// A connect error or a timeout on a real request (`answer` is
    /// [`Answer::Connect`] or [`Answer::Timeout`]): the API is down until
    /// its next good probe.
    fn transport_failed(&self, slot: &ApiSlot, answer: Answer<'_>, result: UpstreamResult) {
        slot.results.inc(result);
        let Outcome::Next { reason, .. } = classify(Region::Reversible, answer) else {
            return;
        };
        self.stats.retries.inc(reason);
        if reason.marks_down() && slot.mark_down() {
            tracing::warn!(
                listener = self.listener.as_deref().unwrap_or_default(),
                server = %slot.name,
                api = %slot.base,
                reason = reason.as_str(),
                "chain API down until its next good probe"
            );
        }
    }

    /// An answer that moves on to the next candidate.
    fn moved_on(&self, slot: &ApiSlot, reason: RetryReason, busy: bool) {
        self.stats.retries.inc(reason);
        if busy {
            slot.mark_busy(Instant::now());
        }
    }

    /// A push: written once, to the first reachable target, within the
    /// request's deadline. A connection that cannot be made moves on to the
    /// next target; a failure after the push was written is answered Busy
    /// and never resent.
    async fn push(
        &self,
        call: &Call,
        deadline: Instant,
        permit: &mut Option<RequestPermit>,
    ) -> Answered {
        let snaps = self.snapshot();
        let views: Vec<ApiView<'_>> = snaps.iter().map(Snapshot::view).collect();
        let primary = self.primary(&views);
        let targets = push_targets(&views, &call.target, &self.push, primary);
        drop(views);
        let queue = Duration::from_millis(self.cfg.upstream_queue_ms);
        let head_timeout = Duration::from_millis(self.cfg.upstream_timeout_ms);
        let mut tries = 0;
        for index in targets {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            let Some(slot) = self.directory.slot(index) else {
                continue;
            };
            let guard = match slot.try_acquire() {
                Some(guard) => guard,
                None => match slot.acquire_until((now + queue).min(deadline).into()).await {
                    Some(guard) => guard,
                    None => continue,
                },
            };
            let Ok(request) = call.request_for(slot) else {
                continue;
            };
            tries += 1;
            let remaining = deadline.saturating_duration_since(Instant::now());
            let cut_short = remaining < head_timeout;
            match self.client.send(request, head_timeout.min(remaining)).await {
                Attempt::Response(response) => {
                    let status = response.status().as_u16();
                    slot.results.inc(result_of(status, None));
                    self.stats.pushes.inc(if (200..300).contains(&status) {
                        PushResult::Ok
                    } else {
                        PushResult::Error
                    });
                    return self.answered(
                        streamed(response, self.hold(permit, Some(guard)), head_timeout),
                        slot,
                        tries,
                    );
                }
                Attempt::Connect => {
                    self.transport_failed(slot, Answer::Connect, UpstreamResult::Connect);
                }
                // Nothing was written, and the deadline passed.
                Attempt::ConnectTimeout if cut_short => {
                    slot.results.inc(UpstreamResult::Timeout);
                    break;
                }
                Attempt::ConnectTimeout => {
                    self.transport_failed(slot, Answer::Timeout, UpstreamResult::Timeout);
                }
                Attempt::Lost | Attempt::Timeout => {
                    slot.results.inc(UpstreamResult::Connect);
                    self.stats.pushes.inc(PushResult::Ambiguous);
                    tracing::warn!(
                        server = %slot.name,
                        "chain API push lost after it was sent; not resent"
                    );
                    return Answered::router(busy(Busy::LostAfterSend), tries);
                }
            }
        }
        self.stats.pushes.inc(PushResult::Error);
        let why = if tries == 0 {
            Busy::NoLiveUpstream
        } else {
            Busy::NoUpstream
        };
        Answered::router(busy(why), tries)
    }
}

#[cfg(test)]
mod tests;
