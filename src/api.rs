//! JSON API behind the dashboard: status views, sessions, history, events, the
//! push streams, control actions, and the shared dashboard publisher.
//!
//! See `docs/api.md` for the contract.

use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::Semaphore;

use crate::fleet::planner::can_take_over;
use crate::health::ListenerHealthState;
use crate::models::{Server, ServerConfigDb, ServerState};
use crate::tasks::hex_32;
use crate::telemetry::{self, Event, EventKind, Level, ListenerStats};

mod dashboard;
pub use dashboard::{run as run_dashboard, Feed};

/// Everything the HTTP endpoint knows about one client listener.
#[derive(Clone)]
pub struct ListenerContext {
    pub config_generation: u64,
    pub health: ListenerHealthState,
    pub config_db: ServerConfigDb,
    /// Enabled endpoints in config order, for stable display order.
    pub order: Vec<String>,
    pub port: u16,
    pub chain_id: Option<[u8; 32]>,
    pub max_connections: usize,
    pub stats: Arc<ListenerStats>,
}

impl ListenerContext {
    /// `default` in single-port mode.
    pub fn name(&self) -> &str {
        self.health.name.as_deref().unwrap_or("default")
    }

    /// Whether another upstream than `endpoint` can take over a session
    /// whose next block is `next_block`, for an operator move or drain. See
    /// [`can_take_over`].
    fn has_alternative_for(&self, endpoint: &str, next_block: Option<u32>) -> bool {
        self.health.hub.as_ref().is_some_and(|hub| {
            hub.view()
                .upstreams
                .iter()
                .any(|u| &*u.endpoint != endpoint && can_take_over(u, next_block))
        })
    }

    /// Publish a change to the state map to the listener's sessions.
    fn publish(&self, db: &HashMap<String, ServerState>) {
        if let Some(hub) = &self.health.hub {
            hub.rebuild(db);
        }
    }
}

/// Shared state of the HTTP endpoint.
pub struct AppState {
    pub runtime: std::sync::Weak<crate::runtime::Runtime>,
    pub config_revision: u64,
    pub draining: Vec<ListenerContext>,
    pub listeners: Vec<ListenerContext>,
    pub health: Vec<ListenerHealthState>,
    pub dashboard: bool,
    pub control_token: Option<String>,
    /// Extra `Host` names the dashboard answers to, lowercased. IP literals
    /// and `localhost` are always accepted.
    pub allowed_hosts: Vec<String>,
    /// Open dashboard and compatibility stream connections. Bounded well below the HTTP
    /// connection cap so streams cannot starve health and readiness probes.
    pub streams: Arc<Semaphore>,
    /// One publisher shared by all dashboard viewers.
    pub dashboard_feed: Arc<Feed>,
}

/// Concurrent SSE connections across both stream routes.
pub const MAX_STREAMS: usize = 32;

/// Long-lived HTTP connections and publishers resolve state without retaining
/// a retired listener's upstream clients and configuration for their lifetime.
pub struct StateHandle {
    runtime: std::sync::Weak<crate::runtime::Runtime>,
    fallback: Option<SharedState>,
}

impl StateHandle {
    pub fn current(&self) -> Option<SharedState> {
        self.runtime
            .upgrade()
            .map(|r| r.app())
            .or_else(|| self.fallback.clone())
    }
}

impl AppState {
    pub fn handle(self: &Arc<Self>) -> StateHandle {
        StateHandle {
            runtime: self.runtime.clone(),
            fallback: (self.runtime.strong_count() == 0).then(|| self.clone()),
        }
    }

    pub fn current(self: &Arc<Self>) -> SharedState {
        self.runtime
            .upgrade()
            .map_or_else(|| self.clone(), |r| r.app())
    }

    /// An endpoint with health, readiness, coverage and metrics only.
    #[cfg(test)]
    pub fn health_only(health: Vec<ListenerHealthState>) -> SharedState {
        Arc::new(AppState {
            runtime: Default::default(),
            config_revision: 0,
            draining: Vec::new(),
            listeners: Vec::new(),
            health,
            dashboard: false,
            control_token: None,
            allowed_hosts: Vec::new(),
            streams: Arc::new(Semaphore::new(MAX_STREAMS)),
            dashboard_feed: Arc::default(),
        })
    }
}

pub type SharedState = Arc<AppState>;

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct RangeView {
    pub begin: u32,
    pub end: u32,
}

fn range(begin: u32, end: u32) -> Option<RangeView> {
    (end > 0).then_some(RangeView { begin, end })
}

#[derive(Debug, Clone, Serialize)]
pub struct QuarantineView {
    pub block: u32,
    pub range: u32,
    pub expires_in_secs: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigView {
    pub priority: u32,
    pub weight: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct UpstreamView {
    pub listener: String,
    pub endpoint: String,
    pub name: String,
    pub tls: bool,
    pub status: &'static str,
    pub online: bool,
    pub stale: bool,
    pub operator_disabled: bool,
    pub priority: u32,
    pub weight: u32,
    pub config: ConfigView,
    pub chain_id: Option<String>,
    pub trace: Option<RangeView>,
    pub chain_state: Option<RangeView>,
    pub finality: Option<RangeView>,
    pub head_block: Option<u32>,
    /// Chain head from SHiP, independent of retained history ranges.
    pub chain_head_block: Option<u32>,
    pub head_sample_age_ms: Option<u64>,
    pub head_lag_blocks: Option<u32>,
    pub sessions: usize,
    pub sessions_total: u64,
    pub failovers_total: u64,
    pub block_failures_total: u64,
    pub blocks_total: u64,
    pub bytes_total: u64,
    pub quarantines: Vec<QuarantineView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ListenerView {
    pub name: String,
    pub port: u16,
    pub chain_id: Option<String>,
    pub ready: bool,
    pub max_connections: usize,
    pub sessions: usize,
    pub accepted_total: u64,
    pub rejected_total: u64,
    pub upstreams: Vec<UpstreamView>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ControlView {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct TotalsView {
    pub sessions: usize,
    pub upstreams: usize,
    pub upstreams_up: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusView {
    pub version: &'static str,
    pub now_ms: u64,
    pub started_ms: u64,
    pub uptime_secs: u64,
    pub control: ControlView,
    pub totals: TotalsView,
    pub listeners: Vec<ListenerView>,
    pub config: crate::runtime::ConfigStatus,
}

/// One word for display. See the `status` table in `docs/api.md`.
pub fn upstream_status(state: &mut ServerState, now: Instant) -> &'static str {
    if state.operator_disabled {
        "disabled"
    } else if !state.online {
        "down"
    } else if state.stale {
        "stale"
    } else if state.any_quarantine_active(now) {
        "quarantined"
    } else {
        "up"
    }
}

fn head_block(state: &ServerState) -> Option<u32> {
    state
        .trace_end_block
        .max(state.chain_state_end_block)
        .checked_sub(1)
}

fn upstream_view(
    listener: &str,
    endpoint: &str,
    state: &mut ServerState,
    config: Option<&Server>,
    now: Instant,
) -> UpstreamView {
    let status = upstream_status(state, now);
    let quarantines = state
        .quarantines
        .iter()
        .map(|quarantine| QuarantineView {
            block: quarantine.block,
            range: quarantine.range,
            expires_in_secs: quarantine.until.saturating_duration_since(now).as_secs(),
        })
        .collect();
    UpstreamView {
        listener: listener.to_string(),
        endpoint: endpoint.to_string(),
        name: state.name.clone(),
        tls: endpoint.trim().to_ascii_lowercase().starts_with("wss://"),
        status,
        online: state.online,
        stale: state.stale,
        operator_disabled: state.operator_disabled,
        priority: state.priority,
        weight: state.weight,
        config: ConfigView {
            priority: config.map_or(state.priority, |cfg| cfg.priority),
            weight: config.map_or(state.weight, |cfg| cfg.weight),
        },
        chain_id: state.chain_id.as_ref().map(hex_32),
        trace: range(state.trace_begin_block, state.trace_end_block),
        chain_state: range(state.chain_state_begin_block, state.chain_state_end_block),
        finality: range(
            state.finality_data_begin_block,
            state.finality_data_end_block,
        ),
        head_block: head_block(state),
        chain_head_block: state.head.map(|head| head.num),
        head_sample_age_ms: state
            .status_at
            .map(|at| now.saturating_duration_since(at).as_millis() as u64),
        head_lag_blocks: state.online.then_some(state.head_lag_blocks).flatten(),
        sessions: state.connections.load(Ordering::Relaxed),
        sessions_total: state.counters.sessions_total.load(Ordering::Relaxed),
        failovers_total: state.counters.failovers_total.load(Ordering::Relaxed),
        block_failures_total: state.block_failures,
        blocks_total: state.counters.blocks_total.load(Ordering::Relaxed),
        bytes_total: state.counters.bytes_total.load(Ordering::Relaxed),
        quarantines,
    }
}

async fn listener_view(ctx: &ListenerContext, now: Instant, sessions: usize) -> ListenerView {
    let configs = ctx.config_db.lock().await.clone();
    let mut db = ctx.health.state_db.lock().await;
    let mut upstreams = Vec::with_capacity(db.len());
    for endpoint in &ctx.order {
        if let Some(state) = db.get_mut(endpoint) {
            upstreams.push(upstream_view(
                ctx.name(),
                endpoint,
                state,
                configs.get(endpoint),
                now,
            ));
        }
    }
    drop(db);
    // The verdict of `/ready`: a stitched listener needs a live upstream.
    let ready = crate::health::tip_ready(&ctx.health).await;
    ListenerView {
        name: ctx.name().to_string(),
        port: ctx.port,
        chain_id: ctx.chain_id.as_ref().map(hex_32),
        ready,
        max_connections: ctx.max_connections,
        // Client sessions, as `max_connections` counts them. An upstream's
        // `sessions` are its connection slots: a stitched session can hold
        // two (a standby link, a pre-dial) or none (holding, waiting).
        sessions,
        accepted_total: ctx.stats.accepted_total.load(Ordering::Relaxed),
        rejected_total: ctx.stats.rejected_total.load(Ordering::Relaxed),
        upstreams,
    }
}

pub async fn status_view(app: &AppState) -> StatusView {
    let mut counts = HashMap::new();
    for session in telemetry::global().sessions() {
        *counts
            .entry((session.listener.to_string(), session.config_generation))
            .or_default() += 1;
    }
    status_with_session_counts(app, counts).await
}

async fn status_with_session_counts(
    app: &AppState,
    mut counts: HashMap<(String, u64), usize>,
) -> StatusView {
    let now = Instant::now();
    let mut listeners = Vec::with_capacity(app.listeners.len());
    for ctx in &app.listeners {
        listeners.push(
            listener_view(
                ctx,
                now,
                counts
                    .remove(&(ctx.name().to_string(), ctx.config_generation))
                    .unwrap_or(0),
            )
            .await,
        );
    }
    let upstreams = listeners.iter().flat_map(|l| &l.upstreams);
    let totals = TotalsView {
        sessions: listeners.iter().map(|l| l.sessions).sum::<usize>()
            + app
                .draining
                .iter()
                .map(|ctx| {
                    counts
                        .get(&(ctx.name().to_string(), ctx.config_generation))
                        .copied()
                        .unwrap_or(0)
                })
                .sum::<usize>(),
        upstreams: upstreams.clone().count(),
        upstreams_up: upstreams
            .filter(|u| u.online && !u.operator_disabled)
            .count(),
    };
    let telemetry = telemetry::global();
    let now_ms = telemetry::now_ms();
    StatusView {
        config: crate::runtime::ConfigStatus::from_app(app, &counts),
        version: env!("CARGO_PKG_VERSION"),
        now_ms,
        started_ms: telemetry.started_ms,
        uptime_secs: now_ms.saturating_sub(telemetry.started_ms) / 1000,
        control: ControlView {
            enabled: app.control_token.is_some(),
        },
        totals,
        listeners,
    }
}

#[derive(Debug, Serialize)]
struct SessionView {
    id: u64,
    listener: String,
    config_generation: u64,
    draining: bool,
    client: String,
    upstream: String,
    upstream_name: String,
    started_ms: u64,
    start_block: Option<u32>,
    last_block: Option<u32>,
    finality: bool,
    blocks: u64,
    bytes: u64,
    failovers: u32,
}

// ---------------------------------------------------------------------------
// Errors and auth
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct ApiError(StatusCode, String);

impl ApiError {
    pub(crate) fn new(status: StatusCode, message: impl Into<String>) -> Self {
        ApiError(status, message.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct Body {
            error: String,
        }
        (self.0, Json(Body { error: self.1 })).into_response()
    }
}

/// Compare without an early exit on the first differing byte.
fn token_matches(expected: &str, given: &str) -> bool {
    let (expected, given) = (expected.as_bytes(), given.as_bytes());
    expected.len() == given.len()
        && expected
            .iter()
            .zip(given)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0
}

pub(crate) fn authorize(app: &AppState, headers: &HeaderMap) -> Result<(), ApiError> {
    let Some(expected) = &app.control_token else {
        return Err(ApiError::new(
            StatusCode::FORBIDDEN,
            "control API is disabled: no control token is configured",
        ));
    };
    let given = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim);
    match given {
        Some(given) if token_matches(expected, given) => Ok(()),
        _ => Err(ApiError::new(
            StatusCode::UNAUTHORIZED,
            "missing or invalid bearer token",
        )),
    }
}

fn find_listener<'a>(app: &'a AppState, name: &str) -> Result<&'a ListenerContext, ApiError> {
    app.listeners
        .iter()
        .find(|ctx| ctx.name() == name)
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, format!("unknown listener '{name}'")))
}

// ---------------------------------------------------------------------------
// Read handlers
// ---------------------------------------------------------------------------

async fn get_status(State(app): State<SharedState>) -> Json<StatusView> {
    Json(status_view(&app).await)
}

#[derive(Debug, Deserialize)]
struct SessionsQuery {
    listener: Option<String>,
    upstream: Option<String>,
}

async fn get_sessions(
    State(app): State<SharedState>,
    Query(query): Query<SessionsQuery>,
) -> Json<serde_json::Value> {
    let sessions: Vec<_> = session_views(&app)
        .await
        .into_iter()
        .filter(|s| query.listener.as_deref().is_none_or(|l| s.listener == l))
        .filter(|s| query.upstream.as_deref().is_none_or(|u| s.upstream == u))
        .collect();
    Json(serde_json::json!({ "sessions": sessions }))
}

async fn session_views(app: &AppState) -> Vec<SessionView> {
    let mut names: HashMap<(String, u64, String), String> = HashMap::new();
    for ctx in app.listeners.iter().chain(&app.draining) {
        for (endpoint, cfg) in ctx.config_db.lock().await.iter() {
            names.insert(
                (
                    ctx.name().to_string(),
                    ctx.config_generation,
                    endpoint.clone(),
                ),
                cfg.name.clone(),
            );
        }
    }
    telemetry::global()
        .sessions()
        .into_iter()
        .map(|s| {
            let upstream = s.upstream();
            let upstream_name = names
                .get(&(
                    s.listener.to_string(),
                    s.config_generation,
                    upstream.clone(),
                ))
                .cloned()
                .unwrap_or_default();
            SessionView {
                id: s.id,
                config_generation: s.config_generation,
                draining: app.draining.iter().any(|ctx| {
                    ctx.config_generation == s.config_generation && ctx.name() == &*s.listener
                }),
                listener: s.listener.to_string(),
                client: s.client.to_string(),
                upstream,
                upstream_name,
                started_ms: s.started_ms,
                start_block: s.start_block(),
                last_block: s.last_block(),
                finality: s.finality(),
                blocks: s.blocks(),
                bytes: s.bytes(),
                failovers: s.failovers(),
            }
        })
        .collect()
}

async fn get_history() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "interval_ms": 1000,
        "samples": telemetry::global().history(),
    }))
}

#[derive(Debug, Deserialize)]
struct EventsQuery {
    after: Option<u64>,
}

async fn get_events(Query(query): Query<EventsQuery>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "events": telemetry::global().events_after(query.after.unwrap_or(0)),
    }))
}

/// One `snapshot` per second plus every new event, until the client goes away.
/// At most [`MAX_STREAMS`] at once; further requests get 503.
async fn get_stream(
    State(app): State<SharedState>,
    headers: HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    let Ok(permit) = app.streams.clone().try_acquire_owned() else {
        return Err(ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "too many open dashboard streams",
        ));
    };
    let events = telemetry::global().subscribe();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let state = app.handle();

    // The permit lives in the stream state and is released when it drops.
    let stream = futures::stream::unfold(
        (state, events, ticker, permit, headers),
        |(state, mut events, mut ticker, permit, headers)| async move {
            loop {
                let next = tokio::select! {
                    _ = ticker.tick() => {
                        SseEvent::default().event("snapshot").json_data(status_view(state.current()?.as_ref()).await)
                    }
                    received = events.recv() => match received {
                        Ok(event) => SseEvent::default().event("event").json_data(&event),
                        // The client refills gaps from /api/v1/events?after=.
                        Err(RecvError::Lagged(_)) => continue,
                        Err(RecvError::Closed) => return None,
                    },
                };
                if !stream_allowed(state.current()?.as_ref(), &headers) {
                    return None;
                }
                match next {
                    Ok(sse) => return Some((Ok(sse), (state, events, ticker, permit, headers))),
                    Err(e) => {
                        tracing::debug!(error = %e, "failed to encode SSE payload");
                        continue;
                    }
                }
            }
        },
    );
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}

/// Open streams must obey access policy changes as well as new HTTP requests.
fn stream_allowed(app: &AppState, headers: &HeaderMap) -> bool {
    app.dashboard
        && headers
            .get(axum::http::header::HOST)
            .and_then(|value| value.to_str().ok())
            .is_none_or(|host| crate::http::host_allowed(host, &app.allowed_hosts))
}

// ---------------------------------------------------------------------------
// Control handlers
// ---------------------------------------------------------------------------

async fn get_auth(
    State(app): State<SharedState>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    authorize(&app, &headers)?;
    Ok(StatusCode::NO_CONTENT)
}

/// Record an operator action as an event and a log line.
fn audit(listener: &str, endpoint: Option<&str>, session: Option<u64>, message: String) {
    tracing::info!(listener, upstream = endpoint, session, "control: {message}");
    let mut event = Event::new(Level::Info, EventKind::Control, message).listener(listener);
    if let Some(endpoint) = endpoint {
        event = event.upstream(endpoint);
    }
    if let Some(session) = session {
        event = event.session(session);
    }
    telemetry::global().emit(event);
}

#[derive(Debug, Clone, Copy)]
enum UpstreamAction {
    Disable,
    Drain,
    Enable,
    Reset,
    ClearQuarantine,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct UpstreamPatch {
    priority: Option<u32>,
    weight: Option<u32>,
}

/// Apply an action or a patch to one upstream and return its updated view.
async fn update_upstream(
    app: &AppState,
    listener: &str,
    endpoint: &str,
    action: Option<UpstreamAction>,
    patch: UpstreamPatch,
) -> Result<UpstreamView, ApiError> {
    let ctx = find_listener(app, listener)?;
    let config = ctx.config_db.lock().await.get(endpoint).cloned();
    let mut db = ctx.health.state_db.lock().await;
    if !db.contains_key(endpoint) {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("unknown upstream '{endpoint}' on listener '{listener}'"),
        ));
    }
    // For a drain, decide per session before this upstream is disabled: a
    // session moves only when another upstream can serve its next block.
    let (mut movable, mut stranded) = (Vec::new(), 0usize);
    if matches!(action, Some(UpstreamAction::Drain)) {
        for session in telemetry::global().sessions() {
            if &*session.listener == listener
                && session.config_generation == ctx.config_generation
                && session.upstream() == endpoint
            {
                if ctx.has_alternative_for(endpoint, session.next_block()) {
                    movable.push(session);
                } else {
                    stranded += 1;
                }
            }
        }
    }
    let Some(state) = db.get_mut(endpoint) else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("unknown upstream '{endpoint}' on listener '{listener}'"),
        ));
    };
    let name = state.name.clone();
    let _change = telemetry::global().begin_change();
    let message = match action {
        Some(UpstreamAction::Disable) => {
            state.operator_disabled = true;
            format!("disabled {name}")
        }
        Some(UpstreamAction::Enable) => {
            state.operator_disabled = false;
            format!("enabled {name}")
        }
        Some(UpstreamAction::Drain) => {
            state.operator_disabled = true;
            for session in &movable {
                session.request_move();
            }
            let mut message = format!("drained {name}: moving {} session(s)", movable.len());
            if stranded > 0 {
                message.push_str(&format!(
                    ", {stranded} stay because no other upstream has their next block"
                ));
            }
            message
        }
        Some(UpstreamAction::Reset) => {
            state.operator_disabled = false;
            if let Some(cfg) = &config {
                state.priority = cfg.priority;
                state.weight = cfg.weight.max(1);
            }
            format!("reset {name} to its configured settings")
        }
        Some(UpstreamAction::ClearQuarantine) => {
            state.clear_quarantines();
            format!("cleared quarantines on {name}")
        }
        None => {
            if patch.priority.is_none() && patch.weight.is_none() {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "set at least one of priority and weight",
                ));
            }
            if patch.weight == Some(0) {
                return Err(ApiError::new(
                    StatusCode::BAD_REQUEST,
                    "weight must be at least 1",
                ));
            }
            let mut changes = Vec::new();
            if let Some(priority) = patch.priority {
                state.priority = priority;
                changes.push(format!("priority {priority}"));
            }
            if let Some(weight) = patch.weight {
                state.weight = weight;
                changes.push(format!("weight {weight}"));
            }
            format!("set {name} {}", changes.join(", "))
        }
    };
    let view = upstream_view(listener, endpoint, state, config.as_ref(), Instant::now());
    // Sessions plan with the new settings from the next view on.
    ctx.publish(&db);
    audit(listener, Some(endpoint), None, message);
    drop(db);
    Ok(view)
}

macro_rules! upstream_action {
    ($handler:ident, $action:expr) => {
        async fn $handler(
            State(app): State<SharedState>,
            headers: HeaderMap,
            Path((listener, endpoint)): Path<(String, String)>,
        ) -> Result<Json<UpstreamView>, ApiError> {
            authorize(&app, &headers)?;
            update_upstream(
                &app,
                &listener,
                &endpoint,
                Some($action),
                UpstreamPatch::default(),
            )
            .await
            .map(Json)
        }
    };
}

upstream_action!(post_disable, UpstreamAction::Disable);
upstream_action!(post_drain, UpstreamAction::Drain);
upstream_action!(post_enable, UpstreamAction::Enable);
upstream_action!(post_reset, UpstreamAction::Reset);
upstream_action!(post_clear_quarantine, UpstreamAction::ClearQuarantine);

async fn patch_upstream(
    State(app): State<SharedState>,
    headers: HeaderMap,
    Path((listener, endpoint)): Path<(String, String)>,
    body: Result<Json<UpstreamPatch>, axum::extract::rejection::JsonRejection>,
) -> Result<Json<UpstreamView>, ApiError> {
    authorize(&app, &headers)?;
    let Json(patch) = body.map_err(|e| ApiError::new(StatusCode::BAD_REQUEST, e.body_text()))?;
    update_upstream(&app, &listener, &endpoint, None, patch)
        .await
        .map(Json)
}

#[derive(Debug, Clone, Copy)]
enum SessionAction {
    Move,
    Close,
}

async fn session_action(
    app: &AppState,
    id: u64,
    action: SessionAction,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    let _change = telemetry::global().begin_change();
    let Some(session) = telemetry::global().session(id) else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("no active session {id}"),
        ));
    };
    let upstream = session.upstream();
    match action {
        SessionAction::Move => {
            let ctx = app
                .listeners
                .iter()
                .chain(&app.draining)
                .find(|ctx| {
                    ctx.name() == &*session.listener
                        && ctx.config_generation == session.config_generation
                })
                .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "listener has drained"))?;
            let next_block = session.next_block();
            if !ctx.has_alternative_for(&upstream, next_block) {
                return Err(ApiError::new(
                    StatusCode::CONFLICT,
                    match next_block {
                        Some(block) => format!("no other upstream online has block {block}"),
                        None => "no other upstream is online".to_string(),
                    },
                ));
            }
            session.request_move();
            audit(
                &session.listener,
                Some(&upstream),
                Some(id),
                format!("moving session {id} ({}) off {upstream}", session.client),
            );
        }
        SessionAction::Close => {
            session.request_close();
            audit(
                &session.listener,
                Some(&upstream),
                Some(id),
                format!("closing session {id} ({})", session.client),
            );
        }
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "ok": true })),
    ))
}

async fn post_session_move(
    State(app): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    authorize(&app, &headers)?;
    session_action(&app, id, SessionAction::Move).await
}

async fn post_session_close(
    State(app): State<SharedState>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> Result<(StatusCode, Json<serde_json::Value>), ApiError> {
    authorize(&app, &headers)?;
    session_action(&app, id, SessionAction::Close).await
}

async fn api_not_found() -> ApiError {
    ApiError::new(StatusCode::NOT_FOUND, "not found")
}

/// Routes under `/api/v1`.
pub fn router() -> Router<SharedState> {
    let upstream = "/listeners/{listener}/upstreams/{endpoint}";
    Router::new()
        .route("/status", get(get_status))
        .route("/sessions", get(get_sessions))
        .route("/history", get(get_history))
        .route("/events", get(get_events))
        .route("/stream", get(get_stream))
        .route("/dashboard/stream", get(dashboard::get_stream))
        .route("/auth", get(get_auth))
        .merge(crate::runtime::routes())
        .route(upstream, axum::routing::patch(patch_upstream))
        .route(&format!("{upstream}/disable"), post(post_disable))
        .route(&format!("{upstream}/drain"), post(post_drain))
        .route(&format!("{upstream}/enable"), post(post_enable))
        .route(&format!("{upstream}/reset"), post(post_reset))
        .route(
            &format!("{upstream}/clear-quarantine"),
            post(post_clear_quarantine),
        )
        .route("/sessions/{id}/move", post(post_session_move))
        .route("/sessions/{id}/close", post(post_session_close))
        .fallback(api_not_found)
}

/// The event for a status change, if it is one worth reporting. Operator
/// actions and quarantines are reported where they happen.
fn transition_event(name: &str, before: &str, after: &str) -> Option<Event> {
    let serving = |status: &str| matches!(status, "up" | "stale" | "quarantined");
    if before == after || before == "disabled" || after == "disabled" {
        return None;
    }
    if after == "down" {
        return Some(Event::new(
            Level::Warn,
            EventKind::UpstreamDown,
            format!("{name} went offline"),
        ));
    }
    if before == "down" && serving(after) {
        return Some(Event::new(
            Level::Info,
            EventKind::UpstreamUp,
            format!("{name} is online"),
        ));
    }
    if after == "stale" {
        return Some(Event::new(
            Level::Warn,
            EventKind::UpstreamStale,
            format!("{name} stopped advancing"),
        ));
    }
    if before == "stale" {
        return Some(Event::new(
            Level::Info,
            EventKind::UpstreamFresh,
            format!("{name} is advancing again"),
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_comparison_requires_an_exact_match() {
        assert!(token_matches("0123456789abcdef", "0123456789abcdef"));
        assert!(!token_matches("0123456789abcdef", "0123456789abcdeF"));
        assert!(!token_matches("0123456789abcdef", "0123456789abcde"));
        assert!(!token_matches("0123456789abcdef", ""));
    }

    #[test]
    fn status_prefers_operator_state_then_health() {
        let now = Instant::now();
        let mut state = ServerState::new();
        assert_eq!(upstream_status(&mut state, now), "down");
        state.online = true;
        assert_eq!(upstream_status(&mut state, now), "up");
        state.stale = true;
        assert_eq!(upstream_status(&mut state, now), "stale");
        state.operator_disabled = true;
        assert_eq!(upstream_status(&mut state, now), "disabled");
    }

    /// A stitched listener whose archives are online but whose live node is
    /// not is shown as not ready, as `/ready` answers 503 for it.
    #[tokio::test]
    async fn a_stitched_listener_is_ready_on_the_dashboard_only_as_for_ready() {
        use crate::fleet::FleetHub;
        use crate::models::{
            build_config_db, build_state_db, ResolvedListener, ResolvedTopology, Role,
            StitchingPolicy,
        };
        let (archive, live) = ("127.0.0.1:18081", "127.0.0.1:18082");
        let servers = vec![
            Server {
                name: "archive".to_string(),
                endpoint: archive.to_string(),
                enabled: true,
                role: Role::Frozen,
                range: Some(crate::ship::Range {
                    begin: 1,
                    end: 1001,
                }),
                ..Default::default()
            },
            Server {
                name: "live".to_string(),
                endpoint: live.to_string(),
                enabled: true,
                role: Role::Live,
                ..Default::default()
            },
        ];
        let resolved = ResolvedListener {
            name: None,
            listen_address: "127.0.0.1".to_string(),
            listen_port: 0,
            max_connections: 10,
            expected_chain_id: None,
            require_chain_id: false,
            block_failure_quarantine: None,
            servers: servers.clone(),
            topology: ResolvedTopology::Stitched,
            stitching: StitchingPolicy::default(),
            abi_pin: None,
        };
        let hub = FleetHub::new(
            &resolved,
            build_state_db(servers.clone()),
            build_config_db(servers.clone()),
        );
        let ctx = ListenerContext {
            config_generation: 0,
            health: ListenerHealthState {
                name: None,
                state_db: hub.state.clone(),
                hub: Some(hub.clone()),
                chain_api: None,
            },
            config_db: build_config_db(servers),
            order: vec![archive.to_string(), live.to_string()],
            port: 0,
            chain_id: None,
            max_connections: 10,
            stats: Arc::new(ListenerStats::default()),
        };
        let set_online = |endpoint: &'static str| {
            let hub = hub.clone();
            async move {
                let mut db = hub.state.lock().await;
                db.get_mut(endpoint).unwrap().online = true;
                hub.rebuild(&db);
            }
        };
        set_online(archive).await;
        assert!(!listener_view(&ctx, Instant::now(), 0).await.ready);
        set_online(live).await;
        assert!(listener_view(&ctx, Instant::now(), 0).await.ready);
    }

    /// A listener's session count is its client sessions, not the slots its
    /// upstreams hold: a stitched session with a standby link holds two, and
    /// one that holds or waits holds none.
    #[tokio::test]
    async fn listener_sessions_count_clients_not_upstream_slots() {
        let servers = vec![
            Server {
                name: "a".to_string(),
                endpoint: "127.0.0.1:18083".to_string(),
                enabled: true,
                ..Default::default()
            },
            Server {
                name: "b".to_string(),
                endpoint: "127.0.0.1:18084".to_string(),
                enabled: true,
                ..Default::default()
            },
        ];
        let state_db = crate::models::build_state_db(servers.clone());
        {
            let db = state_db.lock().await;
            db["127.0.0.1:18083"]
                .connections
                .store(1, Ordering::Relaxed);
            db["127.0.0.1:18084"]
                .connections
                .store(1, Ordering::Relaxed);
        }
        let ctx = ListenerContext {
            config_generation: 0,
            health: ListenerHealthState {
                name: Some("slot-count".to_string()),
                state_db,
                hub: None,
                chain_api: None,
            },
            config_db: crate::models::build_config_db(servers),
            order: vec!["127.0.0.1:18083".to_string(), "127.0.0.1:18084".to_string()],
            port: 0,
            chain_id: None,
            max_connections: 10,
            stats: Arc::new(ListenerStats::default()),
        };
        // One client: its link to a and its standby link to b.
        let client = "127.0.0.1:5002".parse().unwrap();
        let session = telemetry::global().register_session(
            Arc::from("slot-count"),
            client,
            "127.0.0.1:18083",
        );
        let mut app = AppState::health_only(Vec::new());
        Arc::get_mut(&mut app).unwrap().listeners.push(ctx);
        let status = status_view(&app).await;
        let view = &status.listeners[0];
        assert_eq!(view.sessions, 1);
        assert_eq!(
            view.upstreams
                .iter()
                .map(|u| u.sessions)
                .collect::<Vec<_>>(),
            [1, 1]
        );
        drop(session);
        assert_eq!(status_view(&app).await.listeners[0].sessions, 0);
    }

    #[test]
    fn transitions_report_down_up_stale_and_fresh_only() {
        let kind = |before, after| transition_event("n", before, after).map(|e| e.kind);
        assert_eq!(kind("up", "down"), Some(EventKind::UpstreamDown));
        assert_eq!(kind("down", "up"), Some(EventKind::UpstreamUp));
        assert_eq!(kind("up", "stale"), Some(EventKind::UpstreamStale));
        assert_eq!(kind("stale", "up"), Some(EventKind::UpstreamFresh));
        assert_eq!(kind("up", "disabled"), None);
        assert_eq!(kind("disabled", "up"), None);
        assert_eq!(kind("up", "quarantined"), None);
        assert_eq!(kind("up", "up"), None);
    }
}
