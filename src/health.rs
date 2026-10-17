//! Readiness, fleet coverage and Prometheus metrics for the HTTP endpoint in
//! [`crate::http`]. Opt-in via the `metrics_port` config field.
//!
//! - `/ready`, `/ready/<name>`: whether every listener (or one) can serve
//!   the tip; `?scope=history` also requires complete, online archive
//!   coverage (stitched); `?scope=api` asks about the chain API instead.
//! - `/coverage`, `/coverage/<name>`: JSON fleet coverage of every listener,
//!   or of one, with its chain API upstreams.
//! - `/metrics`: Prometheus text exposition of upstream and listener state,
//!   and of the chain API where one is configured.
//!
//! A pool listener can serve the tip when at least one usable upstream is
//! online. A stitched listener needs an online live upstream (any online
//! upstream when it has no live servers). An upstream an operator disabled
//! through the control API does not count. History readiness is never part
//! of plain `/ready`, so one archive outage cannot pull the router out of a
//! load balancer.
//!
//! A chain API is ready when it accepts connections and a live API on the
//! listener's chain is up; with no live server API configured, a verified
//! frozen range server's API instead. Chain API readiness is never part of
//! plain `/ready` either: the chain API port is balanced on its own.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::chain_api::directory::{ApiSlot, Snapshot};
use crate::chain_api::route::RequestClass;
use crate::chain_api::stats::{AnswerRole, ApiStats, BlockRegion, StatusClass};
use crate::chain_api::ChainApiCtx;
use crate::fleet::view::AbiSource;
use crate::fleet::{
    FleetHub, FleetView, HistogramSnapshot, ListenerStats, Log, MetricLabel, SessionGauge,
};
use crate::models::{ResolvedTopology, Role, ServerStateDb};
use crate::ship::{BlockPos, Range};

/// Health and metrics state for a single ingress listener.
#[derive(Clone)]
pub struct ListenerHealthState {
    /// `None` in legacy single-port mode, `Some(name)` in multi-listener mode.
    pub name: Option<String>,
    pub state_db: ServerStateDb,
    /// The listener's fleet view and statistics. `None` keeps the
    /// state-map-only behaviour (pool readiness, upstream series).
    pub hub: Option<Arc<FleetHub>>,
    /// The listener's chain API, when it has one.
    pub chain_api: Option<Arc<ChainApiCtx>>,
}

/// What `/ready` checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scope {
    /// The listener can serve the chain tip.
    Tip,
    /// Tip readiness, plus (stitched) no coverage gap, no archive segment
    /// without an online upstream, and no unreported frozen upstream.
    History,
    /// The listener's chain API can answer: it accepts connections and has
    /// an upstream API to send requests to. Only listeners with a chain API
    /// count.
    Api,
}

/// `scope=tip` (the default), `scope=history` or `scope=api` from a query
/// string. Other parameters are ignored; an unknown scope is returned as the
/// error.
fn parse_scope(query: Option<&str>) -> Result<Scope, String> {
    for pair in query.unwrap_or("").split('&') {
        if let Some(value) = pair.strip_prefix("scope=") {
            return match value {
                "" | "tip" => Ok(Scope::Tip),
                "history" => Ok(Scope::History),
                "api" => Ok(Scope::Api),
                other => Err(other.to_string()),
            };
        }
    }
    Ok(Scope::Tip)
}

/// Status line, content type and body of a response.
pub(crate) type Response = (&'static str, &'static str, String);

fn not_found() -> Response {
    ("404 Not Found", "text/plain", "not found\n".to_string())
}

/// `GET /ready` (`target` is `None`) or `/ready/<target>`, with the request's
/// query string. Only `scope` is read from it.
pub(crate) async fn readiness(
    listeners: &[ListenerHealthState],
    target: Option<&str>,
    query: Option<&str>,
) -> Response {
    let scope = match parse_scope(query) {
        Ok(scope) => scope,
        Err(other) => {
            return (
                "400 Bad Request",
                "text/plain",
                format!("unknown scope '{other}'; use tip, history or api\n"),
            )
        }
    };
    match target {
        None => evaluate_overall(listeners, scope).await,
        Some(target) => evaluate_listener(listeners, target, scope).await,
    }
}

/// One listener's readiness verdict.
struct Readiness {
    /// Upstreams online (for the chain API, upstream APIs up), for the 200
    /// body.
    online: usize,
    /// Empty when ready.
    reasons: Vec<String>,
    /// The verdict came from the state map alone (pool, or no hub).
    pool: bool,
}

impl Readiness {
    fn ready(&self) -> bool {
        self.reasons.is_empty()
    }
}

const NO_UPSTREAMS_ONLINE: &str = "no upstreams online";
const NO_CHAIN_API: &str = "no chain API configured";

/// "N upstream(s) online", or what the chain API counts.
fn online_text(scope: Scope, online: usize) -> String {
    match scope {
        Scope::Tip | Scope::History => format!("{online} upstream(s) online"),
        Scope::Api => format!("{online} chain API upstream(s) up"),
    }
}

/// Whether the listener's chain API can answer: it accepts connections, and
/// a live upstream API on the listener's chain is up. With no live server
/// API configured (an archive-only chain API), a verified frozen range
/// server API that is up stands in. APIs an operator disabled through the
/// control API do not count, as the chain API does not use them.
fn api_readiness(l: &ListenerHealthState) -> Readiness {
    let Some(api) = &l.chain_api else {
        return Readiness {
            online: 0,
            reasons: vec![NO_CHAIN_API.to_string()],
            pool: false,
        };
    };
    let snaps = api.snapshot();
    let usable = |s: &&Snapshot| s.up && (s.role.is_live() || s.verified);
    let live_configured = snaps.iter().any(|s| s.role.is_live());
    let live_up = snaps
        .iter()
        .filter(usable)
        .filter(|s| s.role.is_live())
        .count();
    let online = snaps.iter().filter(usable).count();
    let mut reasons = Vec::new();
    if !api.stats.bound() {
        reasons.push("chain API not listening".to_string());
    }
    if live_configured && live_up == 0 {
        reasons.push("no live chain API upstream up".to_string());
    } else if !live_configured && online == 0 {
        reasons.push("no verified frozen chain API upstream up".to_string());
    }
    Readiness {
        online,
        reasons,
        pool: false,
    }
}

/// Whether `l` can serve the chain tip, as `GET /ready/<listener>` answers.
pub(crate) async fn tip_ready(l: &ListenerHealthState) -> bool {
    listener_readiness(l, Scope::Tip).await.ready()
}

async fn listener_readiness(l: &ListenerHealthState, scope: Scope) -> Readiness {
    if scope == Scope::Api {
        return api_readiness(l);
    }
    let view = match &l.hub {
        Some(hub) if hub.topology == ResolvedTopology::Stitched => hub.view(),
        _ => {
            // Pool: tip and history readiness are the same.
            let online = count_online(&l.state_db).await;
            return Readiness {
                online,
                reasons: if online > 0 {
                    Vec::new()
                } else {
                    vec![NO_UPSTREAMS_ONLINE.to_string()]
                },
                pool: true,
            };
        }
    };
    let mut reasons = Vec::new();
    if view.live_configured && view.live_online == 0 {
        reasons.push("no live upstreams online".to_string());
    } else if !view.live_configured && view.online == 0 {
        reasons.push(NO_UPSTREAMS_ONLINE.to_string());
    }
    if scope == Scope::History {
        reasons.extend(history_reasons(&view));
    }
    Readiness {
        online: view.online,
        reasons,
        pool: false,
    }
}

/// Why a stitched listener's history is not fully servable: holes and
/// segments without an online upstream in the trace and chain-state logs, and
/// frozen upstreams that neither reported nor declared a range.
fn history_reasons(view: &FleetView) -> Vec<String> {
    // (problem, logs it applies to), in first-seen order.
    let mut found: Vec<(String, Vec<&'static str>)> = Vec::new();
    let mut note = |problem: String, log: Log| match found.iter_mut().find(|(p, _)| *p == problem) {
        Some((_, logs)) => logs.push(log.as_str()),
        None => found.push((problem, vec![log.as_str()])),
    };
    for log in [Log::Trace, Log::ChainState] {
        let cov = view.cov(log);
        for gap in &cov.gaps {
            note(
                format!("gap [{},{}) has no upstream", gap.begin, gap.end),
                log,
            );
        }
        for segment in &cov.offline_only {
            note(
                format!(
                    "segment [{},{}) has no online upstream",
                    segment.begin, segment.end
                ),
                log,
            );
        }
    }
    let mut out: Vec<String> = found
        .into_iter()
        .map(|(problem, logs)| format!("{problem} ({})", logs.join(", ")))
        .collect();
    if view.frozen_unknown > 0 {
        out.push(format!(
            "{} frozen upstream(s) have neither reported nor declared a range",
            view.frozen_unknown
        ));
    }
    out
}

#[cfg(test)]
async fn evaluate_overall_readiness(listeners: &[ListenerHealthState]) -> Response {
    evaluate_overall(listeners, Scope::Tip).await
}

async fn evaluate_overall(listeners: &[ListenerHealthState], scope: Scope) -> Response {
    if listeners.len() == 1 && listeners[0].name.is_none() {
        let r = listener_readiness(&listeners[0], scope).await;
        return if r.ready() {
            (
                "200 OK",
                "text/plain",
                format!("ready: {}\n", online_text(scope, r.online)),
            )
        } else {
            (
                "503 Service Unavailable",
                "text/plain",
                format!("not ready: {}\n", r.reasons.join("; ")),
            )
        };
    }

    // The chain API scope asks about the listeners that have one.
    let listeners: Vec<&ListenerHealthState> = listeners
        .iter()
        .filter(|l| scope != Scope::Api || l.chain_api.is_some())
        .collect();
    if scope == Scope::Api && listeners.is_empty() {
        return (
            "503 Service Unavailable",
            "text/plain",
            format!("not ready: {NO_CHAIN_API}\n"),
        );
    }

    let mut total_online = 0usize;
    let mut unready: Vec<(String, Readiness)> = Vec::new();
    for l in &listeners {
        let r = listener_readiness(l, scope).await;
        total_online += r.online;
        if !r.ready() {
            unready.push((l.name.as_deref().unwrap_or("default").to_string(), r));
        }
    }

    if unready.is_empty() && !listeners.is_empty() {
        (
            "200 OK",
            "text/plain",
            format!(
                "ready: {} listener(s) ready, {}\n",
                listeners.len(),
                online_text(scope, total_online)
            ),
        )
    } else if unready.iter().all(|(_, r)| r.pool) {
        let names: Vec<&str> = unready.iter().map(|(name, _)| name.as_str()).collect();
        (
            "503 Service Unavailable",
            "text/plain",
            format!(
                "not ready: listener(s) with no online upstreams: {}\n",
                names.join(", ")
            ),
        )
    } else {
        let parts: Vec<String> = unready
            .iter()
            .map(|(name, r)| format!("{name}: {}", r.reasons.join("; ")))
            .collect();
        (
            "503 Service Unavailable",
            "text/plain",
            format!("not ready: {}\n", parts.join(", ")),
        )
    }
}

#[cfg(test)]
async fn evaluate_listener_readiness(listeners: &[ListenerHealthState], target: &str) -> Response {
    evaluate_listener(listeners, target, Scope::Tip).await
}

async fn evaluate_listener(
    listeners: &[ListenerHealthState],
    target: &str,
    scope: Scope,
) -> Response {
    if target.is_empty() {
        return not_found();
    }
    let Some(listener) = listeners.iter().find(|l| l.name.as_deref() == Some(target)) else {
        return not_found();
    };
    let r = listener_readiness(listener, scope).await;
    if r.ready() {
        (
            "200 OK",
            "text/plain",
            format!("ready ({target}): {}\n", online_text(scope, r.online)),
        )
    } else {
        (
            "503 Service Unavailable",
            "text/plain",
            format!("not ready ({target}): {}\n", r.reasons.join("; ")),
        )
    }
}

async fn count_online(state_db: &ServerStateDb) -> usize {
    state_db
        .lock()
        .await
        .values()
        .filter(|s| s.online && s.routable())
        .count()
}

/// `GET /coverage` (every listener) or `/coverage/<name>` (one; 404 if
/// unknown).
pub(crate) fn coverage_response(
    listeners: &[ListenerHealthState],
    target: Option<&str>,
) -> Response {
    let selected: Vec<&ListenerHealthState> = match target {
        None => listeners.iter().collect(),
        Some(name) => {
            let found: Vec<&ListenerHealthState> = listeners
                .iter()
                .filter(|l| !name.is_empty() && l.name.as_deref() == Some(name))
                .collect();
            if found.is_empty() {
                return not_found();
            }
            found
        }
    };
    let body = json!({
        "listeners": selected.into_iter().filter_map(coverage_json).collect::<Vec<Value>>(),
    });
    ("200 OK", "application/json", format!("{body}\n"))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

fn pos_json(p: Option<BlockPos>) -> Value {
    match p {
        Some(p) => json!({ "num": p.num, "id": hex(&p.id) }),
        None => Value::Null,
    }
}

fn range_json(r: Range) -> Value {
    json!({ "begin": r.begin, "end": r.end })
}

/// One listener's entry in `/coverage`. `None` for a listener without a hub.
fn coverage_json(l: &ListenerHealthState) -> Option<Value> {
    let hub = l.hub.as_ref()?;
    let view = hub.view();
    let (head, lib) = hub.fresh_tip(&view);
    let abi = view.abi.as_ref().map(|abi| {
        let source = match view.abi_source {
            Some(AbiSource::File) => "abi_file".to_string(),
            Some(AbiSource::Live(i) | AbiSource::Provisional(i)) => view
                .up(i)
                .map_or_else(|| i.to_string(), |u| u.name.to_string()),
            None => String::new(),
        };
        let mismatched: Vec<&str> = view
            .upstreams
            .iter()
            .filter(|u| u.abi_fp.is_some_and(|fp| fp != abi.fp))
            .map(|u| &*u.name)
            .collect();
        json!({
            "fingerprint": abi.fp_hex(),
            "source": source,
            "provisional": matches!(view.abi_source, Some(AbiSource::Provisional(_))),
            "mismatched": mismatched,
        })
    });
    let mut logs = serde_json::Map::new();
    for log in Log::ALL {
        let cov = view.cov(log);
        let segments: Vec<Value> = view
            .segments(log)
            .into_iter()
            .map(|segment| {
                let upstreams: Vec<Value> = segment
                    .upstreams
                    .iter()
                    .filter_map(|&pos| view.upstreams.get(pos))
                    .map(|u| {
                        let mut entry = json!({
                            "server": &*u.name,
                            "role": u.role.as_str(),
                            "online": u.online,
                            "sessions": u.connections.load(Ordering::Relaxed),
                        });
                        // Why a segment of online upstreams can count as
                        // offline: an operator took this one out of service.
                        if u.operator_disabled {
                            entry["operator_disabled"] = Value::Bool(true);
                        }
                        entry
                    })
                    .collect();
                json!({
                    "begin": segment.range.begin,
                    "end": segment.range.end,
                    "upstreams": upstreams,
                })
            })
            .collect();
        logs.insert(
            log.as_str().to_string(),
            json!({
                "reported": range_json(cov.top),
                "gaps": cov.gaps.iter().copied().map(range_json).collect::<Vec<_>>(),
                "offline_only": cov.offline_only.iter().copied().map(range_json).collect::<Vec<_>>(),
                "segments": segments,
            }),
        );
    }
    Some(json!({
        "name": l.name,
        "topology": topology_str(view.topology),
        "generation": view.generation,
        "head": pos_json(head),
        "lib": pos_json(lib),
        "chain_id": view.chain_id.map(|id| hex(&id)),
        "status_ready": view.status_ready,
        "frozen_unreported": view.frozen_unknown,
        "abi": abi,
        "logs": logs,
        "chain_api": l.chain_api.as_deref().map(chain_api_json),
    }))
}

/// A listener's chain API in `/coverage`: where it listens, and what the
/// probes last learned about each upstream API. `up`: the API answers its
/// probes on the listener's chain, and no request to it failed in transport
/// since. Unknown values are `null`.
fn chain_api_json(api: &ChainApiCtx) -> Value {
    let mut live = Vec::new();
    let mut frozen = Vec::new();
    for slot in api.directory.slots() {
        let state = slot.state();
        let up = state.up && !state.chain_mismatch;
        match slot.role {
            Role::Live => live.push(json!({
                "server": &*slot.name,
                "up": up,
                "head": state.head,
                "lib": state.lib,
                "earliest": state.earliest,
                "version": state.version,
            })),
            Role::Frozen => frozen.push(json!({
                "server": &*slot.name,
                "up": up,
                "verified": state.verification.verified().is_some(),
                "blocks": state.blocks.map(range_json),
                "get_block": state.get_block,
            })),
        }
    }
    json!({
        "listen": api.bind,
        "live": live,
        "frozen": frozen,
    })
}

fn topology_str(topology: ResolvedTopology) -> &'static str {
    match topology {
        ResolvedTopology::Pool => "pool",
        ResolvedTopology::Stitched => "stitched",
    }
}

fn format_labels(listener_name: Option<&str>, endpoint: &str, server_name: &str) -> String {
    let endpoint_label = format!("endpoint=\"{}\"", escape(endpoint));
    let server_label = format!("server=\"{}\"", escape(server_name));
    match listener_name {
        Some(name) => format!(
            "listener=\"{}\",{endpoint_label},{server_label}",
            escape(name)
        ),
        None => format!("{endpoint_label},{server_label}"),
    }
}

#[derive(Clone)]
struct MetricSnapshot {
    labels: String,
    online: bool,
    stale: bool,
    operator_disabled: bool,
    priority: u32,
    weight: u32,
    connections: usize,
    trace_begin_block: u32,
    trace_end_block: u32,
    chain_state_begin_block: u32,
    chain_state_end_block: u32,
    finality_data_begin_block: u32,
    finality_data_end_block: u32,
    block_failures: u64,
    quarantined: bool,
    quarantine_block: u32,
    role: Role,
    head_block: u32,
    lib_block: u32,
    ranges_known: bool,
    /// Fingerprint (16 hex) and compatibility of the monitor's ABI, if seen.
    abi: Option<(String, bool)>,
    range_mismatch: bool,
    /// A frozen upstream known to have no block log.
    no_block_log: bool,
    /// Whether the ABI equals the listener's canonical ABI (stitched, once
    /// both are known).
    abi_match: Option<bool>,
    /// The listener's statistics and this upstream's index in them.
    stats: Option<(Arc<ListenerStats>, usize)>,
}

/// What a listener contributes to the per-listener series.
struct ListenerSnapshot {
    name: Option<String>,
    hub: Arc<FleetHub>,
    view: Arc<FleetView>,
    head: Option<BlockPos>,
    lib: Option<BlockPos>,
    head_age: Duration,
}

pub(crate) async fn render_metrics(listeners: &[ListenerHealthState]) -> String {
    let now = Instant::now();
    let mut snapshots = Vec::new();
    let mut fleets = Vec::new();
    for l in listeners {
        let view = l.hub.as_ref().map(|hub| hub.view());
        if let (Some(hub), Some(view)) = (&l.hub, &view) {
            let (head, lib) = hub.fresh_tip(view);
            fleets.push(ListenerSnapshot {
                name: l.name.clone(),
                hub: hub.clone(),
                view: view.clone(),
                head,
                lib,
                head_age: hub.head_age(now),
            });
        }
        let canonical = view
            .as_ref()
            .filter(|v| v.topology == ResolvedTopology::Stitched)
            .and_then(|v| v.abi.as_ref().map(|abi| abi.fp));
        let mut db = l.state_db.lock().await;
        for (endpoint, state) in db.iter_mut() {
            snapshots.push(MetricSnapshot {
                abi_match: canonical
                    .zip(state.abi.as_ref())
                    .map(|(canonical, abi)| abi.fp == canonical),
                stats: l.hub.as_ref().map(|hub| (hub.stats.clone(), state.index)),
                labels: format_labels(l.name.as_deref(), endpoint, &state.name),
                online: state.online,
                stale: state.stale,
                operator_disabled: state.operator_disabled,
                priority: state.priority,
                weight: state.weight,
                connections: state.connections.load(Ordering::Relaxed),
                trace_begin_block: state.trace_begin_block,
                trace_end_block: state.trace_end_block,
                chain_state_begin_block: state.chain_state_begin_block,
                chain_state_end_block: state.chain_state_end_block,
                finality_data_begin_block: state.finality_data_begin_block,
                finality_data_end_block: state.finality_data_end_block,
                block_failures: state.block_failures,
                quarantined: state.any_quarantine_active(now),
                quarantine_block: state.active_quarantine_block(now).unwrap_or(0),
                role: state.role,
                head_block: state.head.map_or(0, |head| head.num),
                lib_block: state.lib.map_or(0, |lib| lib.num),
                ranges_known: state.ranges_usable(),
                abi: state.abi.as_ref().map(|abi| (abi.fp_hex(), abi.compatible)),
                range_mismatch: state.range_mismatch(),
                no_block_log: state.no_block_log,
            });
        }
    }

    let mut out = String::new();

    out.push_str("# HELP fleet_router_up Whether the router process is up.\n");
    out.push_str("# TYPE fleet_router_up gauge\n");
    out.push_str("fleet_router_up 1\n");

    out.push_str(
        "# HELP fleet_router_upstream_up Whether an upstream is online (1) or offline (0).\n",
    );
    out.push_str("# TYPE fleet_router_upstream_up gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_up{{{}}} {}\n",
            s.labels,
            u8::from(s.online)
        ));
    }

    out.push_str("# HELP fleet_router_upstream_stale Whether an upstream has stopped advancing (1) or not (0).\n");
    out.push_str("# TYPE fleet_router_upstream_stale gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_stale{{{}}} {}\n",
            s.labels,
            u8::from(s.stale)
        ));
    }

    out.push_str("# HELP fleet_router_upstream_operator_disabled Whether an operator disabled or drained an upstream through the control API (1) or not (0).\n");
    out.push_str("# TYPE fleet_router_upstream_operator_disabled gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_operator_disabled{{{}}} {}\n",
            s.labels,
            u8::from(s.operator_disabled)
        ));
    }

    out.push_str("# HELP fleet_router_upstream_priority Priority tier in effect for an upstream (lower is preferred).\n");
    out.push_str("# TYPE fleet_router_upstream_priority gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_priority{{{}}} {}\n",
            s.labels, s.priority
        ));
    }

    out.push_str("# HELP fleet_router_upstream_weight Weight in effect for an upstream within its priority tier.\n");
    out.push_str("# TYPE fleet_router_upstream_weight gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_weight{{{}}} {}\n",
            s.labels, s.weight
        ));
    }

    out.push_str(
        "# HELP fleet_router_active_connections Active client connections routed to an upstream.\n",
    );
    out.push_str("# TYPE fleet_router_active_connections gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_active_connections{{{}}} {}\n",
            s.labels, s.connections
        ));
    }

    out.push_str("# HELP fleet_router_upstream_trace_begin_block First block with trace data reported by an upstream (0 until a status result arrives).\n");
    out.push_str("# TYPE fleet_router_upstream_trace_begin_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_trace_begin_block{{{}}} {}\n",
            s.labels, s.trace_begin_block
        ));
    }

    out.push_str("# HELP fleet_router_upstream_trace_end_block One past the last block with trace data reported by an upstream (0 until a status result arrives).\n");
    out.push_str("# TYPE fleet_router_upstream_trace_end_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_trace_end_block{{{}}} {}\n",
            s.labels, s.trace_end_block
        ));
    }

    out.push_str("# HELP fleet_router_upstream_chain_state_begin_block First block with chain-state data reported by an upstream (0 until a status result arrives).\n");
    out.push_str("# TYPE fleet_router_upstream_chain_state_begin_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_chain_state_begin_block{{{}}} {}\n",
            s.labels, s.chain_state_begin_block
        ));
    }

    out.push_str("# HELP fleet_router_upstream_chain_state_end_block Last chain-state block reported by an upstream.\n");
    out.push_str("# TYPE fleet_router_upstream_chain_state_end_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_chain_state_end_block{{{}}} {}\n",
            s.labels, s.chain_state_end_block
        ));
    }

    out.push_str("# HELP fleet_router_upstream_finality_data_begin_block First block with finality data reported by an upstream (0 if unknown, including pre-Savanna nodes that only report get_status_result_v0).\n");
    out.push_str("# TYPE fleet_router_upstream_finality_data_begin_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_finality_data_begin_block{{{}}} {}\n",
            s.labels, s.finality_data_begin_block
        ));
    }

    out.push_str("# HELP fleet_router_upstream_finality_data_end_block One past the last block with finality data (0 if unknown).\n");
    out.push_str("# TYPE fleet_router_upstream_finality_data_end_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_finality_data_end_block{{{}}} {}\n",
            s.labels, s.finality_data_end_block
        ));
    }

    out.push_str("# HELP fleet_router_upstream_block_failures_total Unclean mid-get_blocks drops observed for an upstream.\n");
    out.push_str("# TYPE fleet_router_upstream_block_failures_total counter\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_block_failures_total{{{}}} {}\n",
            s.labels, s.block_failures
        ));
    }

    out.push_str("# HELP fleet_router_upstream_quarantined Whether an upstream is excluded for a block range (1) or not (0). The upstream stays online.\n");
    out.push_str("# TYPE fleet_router_upstream_quarantined gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_quarantined{{{}}} {}\n",
            s.labels,
            u8::from(s.quarantined)
        ));
    }

    out.push_str("# HELP fleet_router_upstream_quarantine_block Start block of an upstream's active quarantine (0 when none). When several ranges are active, this is the lowest start block.\n");
    out.push_str("# TYPE fleet_router_upstream_quarantine_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_quarantine_block{{{}}} {}\n",
            s.labels, s.quarantine_block
        ));
    }

    out.push_str("# HELP fleet_router_upstream_head_block Head block reported by an upstream (0 until a status result arrives).\n");
    out.push_str("# TYPE fleet_router_upstream_head_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_head_block{{{}}} {}\n",
            s.labels, s.head_block
        ));
    }

    out.push_str("# HELP fleet_router_upstream_lib_block Last irreversible block reported by an upstream (0 until a status result arrives).\n");
    out.push_str("# TYPE fleet_router_upstream_lib_block gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_lib_block{{{}}} {}\n",
            s.labels, s.lib_block
        ));
    }

    out.push_str("# HELP fleet_router_upstream_info Configured role of an upstream (always 1).\n");
    out.push_str("# TYPE fleet_router_upstream_info gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_info{{{},role=\"{}\"}} 1\n",
            s.labels,
            s.role.as_str()
        ));
    }

    out.push_str("# HELP fleet_router_upstream_ranges_known Whether an upstream's block ranges are known (1): reported at least once, or declared by a frozen server.\n");
    out.push_str("# TYPE fleet_router_upstream_ranges_known gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_ranges_known{{{}}} {}\n",
            s.labels,
            u8::from(s.ranges_known)
        ));
    }

    out.push_str("# HELP fleet_router_upstream_range_mismatch Whether a frozen server's reported trace or chain-state range differs from its declared range (1) or not (0). A log starting at block 2 when the range starts at 1, or ending one block before the other log at the range's end, is no mismatch.\n");
    out.push_str("# TYPE fleet_router_upstream_range_mismatch gauge\n");
    for s in &snapshots {
        out.push_str(&format!(
            "fleet_router_upstream_range_mismatch{{{}}} {}\n",
            s.labels,
            u8::from(s.range_mismatch)
        ));
    }

    out.push_str("# HELP fleet_router_upstream_abi_info Fingerprint of the ABI an upstream sent its monitor (always 1; absent until an ABI arrives).\n");
    out.push_str("# TYPE fleet_router_upstream_abi_info gauge\n");
    for s in &snapshots {
        if let Some((fingerprint, _)) = &s.abi {
            out.push_str(&format!(
                "fleet_router_upstream_abi_info{{{},fingerprint=\"{fingerprint}\"}} 1\n",
                s.labels
            ));
        }
    }

    out.push_str("# HELP fleet_router_upstream_abi_compatible Whether an upstream ABI's request and result variants are a prefix of Spring's (1) or not (0). Absent until an ABI arrives.\n");
    out.push_str("# TYPE fleet_router_upstream_abi_compatible gauge\n");
    for s in &snapshots {
        if let Some((_, compatible)) = &s.abi {
            out.push_str(&format!(
                "fleet_router_upstream_abi_compatible{{{}}} {}\n",
                s.labels,
                u8::from(*compatible)
            ));
        }
    }

    render_upstream_session_series(&mut out, &snapshots);
    render_listener_series(&mut out, &fleets);
    render_chain_api_series(&mut out, listeners);

    out
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
}

/// Per-upstream series fed by the listener's sessions and fleet view.
fn render_upstream_session_series(out: &mut String, snapshots: &[MetricSnapshot]) {
    header(
        out,
        "fleet_router_upstream_abi_match",
        "gauge",
        "Whether an upstream's ABI equals the ABI its stitched listener serves (1) or not (0). Absent for pool listeners and until both are known.",
    );
    for s in snapshots {
        if let Some(matches) = s.abi_match {
            out.push_str(&format!(
                "fleet_router_upstream_abi_match{{{}}} {}\n",
                s.labels,
                u8::from(matches)
            ));
        }
    }

    let with_stats = || {
        snapshots.iter().filter_map(|s| {
            let (stats, index) = s.stats.as_ref()?;
            Some((s, stats.upstream(*index)?))
        })
    };

    header(
        out,
        "fleet_router_upstream_links_total",
        "counter",
        "Upstream links opened by client sessions, by purpose.",
    );
    for (s, stats) in with_stats() {
        for (purpose, n) in stats.links.values() {
            out.push_str(&format!(
                "fleet_router_upstream_links_total{{{},purpose=\"{}\"}} {n}\n",
                s.labels,
                purpose.as_str()
            ));
        }
    }

    header(
        out,
        "fleet_router_upstream_dial_failures_total",
        "counter",
        "Failed session dials to an upstream, by reason (busy is an HTTP 503 at the upgrade).",
    );
    for (s, stats) in with_stats() {
        for (reason, n) in stats.dial_failures.values() {
            out.push_str(&format!(
                "fleet_router_upstream_dial_failures_total{{{},reason=\"{}\"}} {n}\n",
                s.labels,
                reason.as_str()
            ));
        }
    }

    header(
        out,
        "fleet_router_upstream_contract_violations_total",
        "counter",
        "Archive contract violations by a frozen upstream, by kind.",
    );
    for (s, stats) in with_stats().filter(|(s, _)| s.role == Role::Frozen) {
        for (kind, n) in stats.contract_violations.values() {
            out.push_str(&format!(
                "fleet_router_upstream_contract_violations_total{{{},kind=\"{}\"}} {n}\n",
                s.labels,
                kind.as_str()
            ));
        }
    }

    header(
        out,
        "fleet_router_upstream_no_block_log",
        "gauge",
        "Whether a frozen upstream sent a result without the requested block for a block it covers, so it has no block log (1) or not (0). Sessions prefer upstreams that hold blocks for fetch_block requests. Cleared when it reconnects or reports another status.",
    );
    for s in snapshots.iter().filter(|s| s.role == Role::Frozen) {
        out.push_str(&format!(
            "fleet_router_upstream_no_block_log{{{}}} {}\n",
            s.labels,
            u8::from(s.no_block_log)
        ));
    }

    header(
        out,
        "fleet_router_upstream_results_without_block_total",
        "counter",
        "Frozen results forwarded without the requested block, because no upstream that holds blocks could serve them.",
    );
    for (s, stats) in with_stats().filter(|(s, _)| s.role == Role::Frozen) {
        out.push_str(&format!(
            "fleet_router_upstream_results_without_block_total{{{}}} {}\n",
            s.labels,
            stats.results_without_block.load(Ordering::Relaxed)
        ));
    }

    header(
        out,
        "fleet_router_upstream_prev_blocks_filled_total",
        "counter",
        "Frozen results that lacked prev_block right after a block the session delivered, filled in by the router from that block.",
    );
    for (s, stats) in with_stats().filter(|(s, _)| s.role == Role::Frozen) {
        out.push_str(&format!(
            "fleet_router_upstream_prev_blocks_filled_total{{{}}} {}\n",
            s.labels,
            stats.prev_blocks_filled.load(Ordering::Relaxed)
        ));
    }

    header(
        out,
        "fleet_router_upstream_stalls_total",
        "counter",
        "Session links to an upstream that went silent while blocks were owed.",
    );
    for (s, stats) in with_stats() {
        out.push_str(&format!(
            "fleet_router_upstream_stalls_total{{{}}} {}\n",
            s.labels,
            stats.stalls.load(Ordering::Relaxed)
        ));
    }

    header(
        out,
        "fleet_router_upstream_sequence_jumps_total",
        "counter",
        "Forward block jumps from a live upstream that sessions adopted.",
    );
    for (s, stats) in with_stats() {
        out.push_str(&format!(
            "fleet_router_upstream_sequence_jumps_total{{{}}} {}\n",
            s.labels,
            stats.sequence_jumps.load(Ordering::Relaxed)
        ));
    }
}

/// `{listener="x",<extra>}`, `{<extra>}`, or nothing in single-listener mode
/// without extra labels.
fn listener_labels(name: Option<&str>, extra: &str) -> String {
    match (name, extra.is_empty()) {
        (Some(name), true) => format!("{{listener=\"{}\"}}", escape(name)),
        (Some(name), false) => format!("{{listener=\"{}\",{extra}}}", escape(name)),
        (None, true) => String::new(),
        (None, false) => format!("{{{extra}}}"),
    }
}

/// One line per listener: `metric{labels} value`.
fn per_listener(
    out: &mut String,
    fleets: &[ListenerSnapshot],
    metric: &str,
    extra: &str,
    value: impl Fn(&ListenerSnapshot) -> Option<String>,
) {
    for f in fleets {
        if let Some(v) = value(f) {
            out.push_str(&format!(
                "{metric}{} {v}\n",
                listener_labels(f.name.as_deref(), extra)
            ));
        }
    }
}

/// A counter family with one fixed label, for every listener.
fn labeled_counter<L: MetricLabel>(
    out: &mut String,
    fleets: &[ListenerSnapshot],
    metric: &str,
    label: &str,
    help: &str,
    counter: impl Fn(&ListenerStats) -> &crate::fleet::LabeledCounter<L>,
) {
    header(out, metric, "counter", help);
    for f in fleets {
        for (value, n) in counter(&f.hub.stats).values() {
            let extra = format!("{label}=\"{}\"", value.as_str());
            out.push_str(&format!(
                "{metric}{} {n}\n",
                listener_labels(f.name.as_deref(), &extra)
            ));
        }
    }
}

/// The `_bucket`, `_sum` and `_count` lines of one histogram of one
/// listener; `extra` labels come before `le`.
fn histogram_lines(
    out: &mut String,
    metric: &str,
    name: Option<&str>,
    extra: &str,
    snap: &HistogramSnapshot,
) {
    let with_le = |le: &str| match extra {
        "" => format!("le=\"{le}\""),
        extra => format!("{extra},le=\"{le}\""),
    };
    for (le, n) in &snap.cumulative {
        out.push_str(&format!(
            "{metric}_bucket{} {n}\n",
            listener_labels(name, &with_le(le))
        ));
    }
    out.push_str(&format!(
        "{metric}_bucket{} {}\n",
        listener_labels(name, &with_le("+Inf")),
        snap.count
    ));
    out.push_str(&format!(
        "{metric}_sum{} {}\n",
        listener_labels(name, extra),
        snap.sum_seconds
    ));
    out.push_str(&format!(
        "{metric}_count{} {}\n",
        listener_labels(name, extra),
        snap.count
    ));
}

/// A plain counter for every listener.
fn plain_counter(
    out: &mut String,
    fleets: &[ListenerSnapshot],
    metric: &str,
    help: &str,
    counter: impl Fn(&ListenerStats) -> &std::sync::atomic::AtomicU64,
) {
    header(out, metric, "counter", help);
    per_listener(out, fleets, metric, "", |f| {
        Some(counter(&f.hub.stats).load(Ordering::Relaxed).to_string())
    });
}

/// Per-listener series: the fleet tip and coverage, session states and the
/// session counters.
fn render_listener_series(out: &mut String, fleets: &[ListenerSnapshot]) {
    header(
        out,
        "fleet_router_fleet_head_block",
        "gauge",
        "Fleet head block: what a stitched listener's status reports (0 until known).",
    );
    per_listener(out, fleets, "fleet_router_fleet_head_block", "", |f| {
        Some(f.head.map_or(0, |p| p.num).to_string())
    });
    header(
        out,
        "fleet_router_fleet_lib_block",
        "gauge",
        "Fleet last irreversible block (0 until known). Never decreases.",
    );
    per_listener(out, fleets, "fleet_router_fleet_lib_block", "", |f| {
        Some(f.lib.map_or(0, |p| p.num).to_string())
    });
    header(
        out,
        "fleet_router_fleet_head_age_seconds",
        "gauge",
        "Seconds since the fleet head last advanced (since startup while no head is known).",
    );
    per_listener(
        out,
        fleets,
        "fleet_router_fleet_head_age_seconds",
        "",
        |f| Some(format!("{:.3}", f.head_age.as_secs_f64())),
    );

    let per_log = |out: &mut String,
                   metric: &str,
                   help: &str,
                   logs: &[Log],
                   value: &dyn Fn(&FleetView, Log) -> Option<String>| {
        header(out, metric, "gauge", help);
        for f in fleets {
            for &log in logs {
                if let Some(v) = value(&f.view, log) {
                    let extra = format!("log=\"{}\"", log.as_str());
                    out.push_str(&format!(
                        "{metric}{} {v}\n",
                        listener_labels(f.name.as_deref(), &extra)
                    ));
                }
            }
        }
    };
    per_log(
        out,
        "fleet_router_fleet_coverage_begin_block",
        "First block of the top coverage component of a log (what status reports; 0 if none).",
        &Log::ALL,
        &|v, log| Some(v.cov(log).top.begin.to_string()),
    );
    per_log(
        out,
        "fleet_router_fleet_coverage_end_block",
        "One past the last block of the top coverage component of a log (0 if none).",
        &Log::ALL,
        &|v, log| Some(v.cov(log).top.end.to_string()),
    );
    per_log(
        out,
        "fleet_router_fleet_coverage_gaps",
        "Holes in a log's coverage below its top component.",
        &Log::ALL,
        &|v, log| Some(v.cov(log).gaps.len().to_string()),
    );
    per_log(
        out,
        "fleet_router_fleet_offline_coverage_blocks",
        "Blocks of a log's top coverage component that no online upstream covers.",
        &Log::ALL,
        &|v, log| Some(v.cov(log).offline_blocks().to_string()),
    );
    per_log(
        out,
        "fleet_router_fleet_live_overlap_blocks",
        "Highest frozen end minus lowest online live begin. Negative means a hole between the archives and the live tier.",
        &[Log::Trace, Log::ChainState],
        &|v, log| {
            let slot = usize::from(log == Log::ChainState);
            v.live_overlap[slot].map(|n| n.to_string())
        },
    );

    header(
        out,
        "fleet_router_fleet_frozen_unreported",
        "gauge",
        "Enabled frozen upstreams that have neither reported nor declared a range.",
    );
    per_listener(
        out,
        fleets,
        "fleet_router_fleet_frozen_unreported",
        "",
        |f| Some(f.view.frozen_unknown.to_string()),
    );

    header(
        out,
        "fleet_router_sessions",
        "gauge",
        "Client sessions by state.",
    );
    for f in fleets {
        for &state in SessionGauge::ALL {
            let extra = format!("state=\"{}\"", state.as_str());
            out.push_str(&format!(
                "fleet_router_sessions{} {}\n",
                listener_labels(f.name.as_deref(), &extra),
                f.hub.stats.sessions(state)
            ));
        }
    }

    header(
        out,
        "fleet_router_status_replies_total",
        "counter",
        "Status replies sent to clients, by request version and by who encoded them.",
    );
    for f in fleets {
        for (v1, version) in [(false, "v0"), (true, "v1")] {
            for (from_router, source) in [(true, "router"), (false, "upstream")] {
                let extra = format!("version=\"{version}\",source=\"{source}\"");
                out.push_str(&format!(
                    "fleet_router_status_replies_total{} {}\n",
                    listener_labels(f.name.as_deref(), &extra),
                    f.hub.stats.status_replies(v1, from_router)
                ));
            }
        }
    }

    plain_counter(
        out,
        fleets,
        "fleet_router_handoffs_total",
        "Planned handoffs from one upstream to the next at a range boundary.",
        |s| &s.handoffs,
    );

    let metric = "fleet_router_handoff_latency_seconds";
    header(
        out,
        metric,
        "histogram",
        "Time from the last frame of the old link to the first frame of the new link at a handoff.",
    );
    for f in fleets {
        let snap = f.hub.stats.handoff_latency.snapshot();
        histogram_lines(out, metric, f.name.as_deref(), "", &snap);
    }

    labeled_counter(
        out,
        fleets,
        "fleet_router_failovers_total",
        "reason",
        "Session failovers to another upstream link, by reason.",
        |s| &s.failovers,
    );
    plain_counter(
        out,
        fleets,
        "fleet_router_reroutes_total",
        "Pool sessions moved to another upstream before forwarding a get_blocks request.",
        |s| &s.reroutes,
    );
    plain_counter(
        out,
        fleets,
        "fleet_router_duplicates_dropped_total",
        "Replayed results dropped after a failover (each refunded upstream with an ack).",
        |s| &s.duplicates_dropped,
    );
    plain_counter(
        out,
        fleets,
        "fleet_router_fork_redeliveries_total",
        "Results for an already delivered block number with a new id (fork switch), forwarded.",
        |s| &s.fork_redeliveries,
    );
    plain_counter(
        out,
        fleets,
        "fleet_router_keepalives_total",
        "Head-only keep-alive results from legacy upstreams forwarded to clients.",
        |s| &s.keepalives,
    );
    plain_counter(
        out,
        fleets,
        "fleet_router_credit_overruns_total",
        "Results forwarded while the client had no credit left. Stays 0 with Spring upstreams.",
        |s| &s.credit_overruns,
    );
    plain_counter(
        out,
        fleets,
        "fleet_router_patched_results_total",
        "Results whose head or LIB the router rewrote.",
        |s| &s.patched_results,
    );
    labeled_counter(
        out,
        fleets,
        "fleet_router_holds_total",
        "why",
        "Sessions that started holding for a block no upstream could serve yet, by reason.",
        |s| &s.holds,
    );
    labeled_counter(
        out,
        fleets,
        "fleet_router_hold_closes_total",
        "why",
        "Sessions closed with 1011 after holding for gap_wait_ms, by reason.",
        |s| &s.hold_closes,
    );
    labeled_counter(
        out,
        fleets,
        "fleet_router_predials_total",
        "result",
        "Standby links opened ahead of a handoff, by outcome.",
        |s| &s.predials,
    );
    labeled_counter(
        out,
        fleets,
        "fleet_router_client_protocol_errors_total",
        "kind",
        "Client frames that were not a valid SHiP request, by kind.",
        |s| &s.client_protocol_errors,
    );
    plain_counter(
        out,
        fleets,
        "fleet_router_too_large_closes_total",
        "Clients closed because an upstream message exceeded max_message_bytes.",
        |s| &s.too_large_closes,
    );
}

/// What one upstream chain API contributes to the per-upstream API series,
/// copied under its probe-state lock.
struct ApiUpstreamSnapshot<'a> {
    /// The server's usual upstream labels: `endpoint` is its SHiP endpoint,
    /// so the API series join every other upstream series.
    labels: String,
    slot: &'a ApiSlot,
    /// Up on its probes, on the listener's chain.
    up: bool,
    chain_mismatch: bool,
    head: Option<u32>,
    earliest: Option<u32>,
    blocks: Option<Range>,
    verified: bool,
}

/// The block requests that can occur, by region and by who answered: only
/// an irreversible block is ever sent to a frozen range server.
const BLOCK_ANSWERS: [(BlockRegion, AnswerRole); 7] = [
    (BlockRegion::Irreversible, AnswerRole::Live),
    (BlockRegion::Irreversible, AnswerRole::Frozen),
    (BlockRegion::Irreversible, AnswerRole::None),
    (BlockRegion::Reversible, AnswerRole::Live),
    (BlockRegion::Reversible, AnswerRole::None),
    (BlockRegion::Unhinted, AnswerRole::Live),
    (BlockRegion::Unhinted, AnswerRole::None),
];

/// The chain API series: per upstream API, then per listener, for the
/// listeners that have a chain API. Nothing when none has one.
fn render_chain_api_series(out: &mut String, listeners: &[ListenerHealthState]) {
    let apis: Vec<(Option<&str>, &ChainApiCtx)> = listeners
        .iter()
        .filter_map(|l| Some((l.name.as_deref(), l.chain_api.as_deref()?)))
        .collect();
    if apis.is_empty() {
        return;
    }
    let mut upstreams = Vec::new();
    for &(name, api) in &apis {
        for slot in api.directory.slots() {
            let state = slot.state();
            upstreams.push(ApiUpstreamSnapshot {
                labels: format_labels(name, &slot.ship_endpoint, &slot.name),
                slot,
                up: state.up && !state.chain_mismatch,
                chain_mismatch: state.chain_mismatch,
                head: state.head,
                earliest: state.earliest,
                blocks: state.blocks,
                verified: state.verification.verified().is_some(),
            });
        }
    }
    render_api_upstream_series(out, &upstreams);
    render_api_listener_series(out, &apis);
}

fn render_api_upstream_series(out: &mut String, upstreams: &[ApiUpstreamSnapshot<'_>]) {
    type ValueOf<'v> = &'v dyn Fn(&ApiUpstreamSnapshot<'_>) -> Option<u64>;
    let gauge = |out: &mut String, metric: &str, help: &str, value: ValueOf<'_>| {
        header(out, metric, "gauge", help);
        for u in upstreams {
            if let Some(v) = value(u) {
                out.push_str(&format!("{metric}{{{}}} {v}\n", u.labels));
            }
        }
    };
    let flag = |b: bool| u64::from(u8::from(b));
    let live = |u: &ApiUpstreamSnapshot<'_>| u.slot.role == Role::Live;

    gauge(
        out,
        "fleet_router_upstream_api_up",
        "Whether an upstream's chain API is up (1): it answers its probes on the listener's chain, and no request to it failed in transport since. 0 otherwise.",
        &|u| Some(flag(u.up)),
    );
    gauge(
        out,
        "fleet_router_upstream_api_chain_id_mismatch",
        "Whether an upstream's chain API reports another chain_id than the listener's (1) or not (0). Such an API is not used.",
        &|u| Some(flag(u.chain_mismatch)),
    );
    gauge(
        out,
        "fleet_router_upstream_api_head_block",
        "Head block a live upstream's chain API reported to its probes (0 until known).",
        &|u| live(u).then(|| u.head.unwrap_or(0).into()),
    );
    gauge(
        out,
        "fleet_router_upstream_api_earliest_block",
        "First block a live upstream's chain API holds, from earliest_available_block_num (0 until known, and for nodes that do not report it).",
        &|u| live(u).then(|| u.earliest.unwrap_or(0).into()),
    );
    gauge(
        out,
        "fleet_router_upstream_api_blocks_begin_block",
        "First block a frozen range server's chain API serves (0 until it described itself).",
        &|u| (!live(u)).then(|| u.blocks.map_or(0, |r| r.begin).into()),
    );
    gauge(
        out,
        "fleet_router_upstream_api_blocks_end_block",
        "One past the last block a frozen range server's chain API serves (0 until it described itself).",
        &|u| (!live(u)).then(|| u.blocks.map_or(0, |r| r.end).into()),
    );
    gauge(
        out,
        "fleet_router_upstream_api_verified",
        "Whether a frozen range server's chain API is verified against its SHiP endpoint (1) or not (0). An API never verified is not used.",
        &|u| (!live(u)).then(|| flag(u.verified)),
    );
    gauge(
        out,
        "fleet_router_upstream_api_in_flight",
        "Chain API requests in flight to an upstream.",
        &|u| Some(u.slot.in_flight() as u64),
    );
    gauge(
        out,
        "fleet_router_upstream_api_max_in_flight",
        "Chain API requests an upstream may have in flight: max_in_flight_per_upstream, for a frozen range server at most its own limit.",
        &|u| Some(u.slot.cap() as u64),
    );

    let metric = "fleet_router_upstream_api_requests_total";
    header(
        out,
        metric,
        "counter",
        "Chain API request attempts sent to an upstream, by result.",
    );
    for u in upstreams {
        for (result, n) in u.slot.results.values() {
            out.push_str(&format!(
                "{metric}{{{},result=\"{}\"}} {n}\n",
                u.labels,
                result.as_str()
            ));
        }
    }
}

/// A counter family with one fixed label, for every chain API listener.
fn api_labeled_counter<L: MetricLabel>(
    out: &mut String,
    apis: &[(Option<&str>, &ChainApiCtx)],
    metric: &str,
    label: &str,
    help: &str,
    counter: impl Fn(&ApiStats) -> &crate::fleet::LabeledCounter<L>,
) {
    header(out, metric, "counter", help);
    for &(name, api) in apis {
        for (value, n) in counter(&api.stats).values() {
            let extra = format!("{label}=\"{}\"", value.as_str());
            out.push_str(&format!("{metric}{} {n}\n", listener_labels(name, &extra)));
        }
    }
}

fn render_api_listener_series(out: &mut String, apis: &[(Option<&str>, &ChainApiCtx)]) {
    let metric = "fleet_router_api_requests_total";
    header(
        out,
        metric,
        "counter",
        "Chain API requests a listener answered, by endpoint class and HTTP status class.",
    );
    for &(name, api) in apis {
        for &class in RequestClass::ALL {
            for &code in StatusClass::ALL {
                let extra = format!("class=\"{}\",code=\"{}\"", class.as_str(), code.as_str());
                out.push_str(&format!(
                    "{metric}{} {}\n",
                    listener_labels(name, &extra),
                    api.stats.requests(class, code)
                ));
            }
        }
    }

    let metric = "fleet_router_api_request_duration_seconds";
    header(
        out,
        metric,
        "histogram",
        "Time from a chain API request's head to the head of its answer (body read included), by endpoint class.",
    );
    for &(name, api) in apis {
        for &class in RequestClass::ALL {
            let extra = format!("class=\"{}\"", class.as_str());
            histogram_lines(out, metric, name, &extra, &api.stats.duration(class));
        }
    }

    let metric = "fleet_router_api_in_flight";
    header(
        out,
        metric,
        "gauge",
        "Chain API requests holding one of a listener's max_in_flight slots.",
    );
    for &(name, api) in apis {
        out.push_str(&format!(
            "{metric}{} {}\n",
            listener_labels(name, ""),
            api.stats.in_flight()
        ));
    }

    api_labeled_counter(
        out,
        apis,
        "fleet_router_api_rejected_total",
        "reason",
        "Chain API connections closed and requests refused by a listener limit, by reason.",
        |s| &s.rejected,
    );
    api_labeled_counter(
        out,
        apis,
        "fleet_router_api_retries_total",
        "reason",
        "Chain API attempts whose answer passed the request on to the next candidate upstream, by reason.",
        |s| &s.retries,
    );

    let metric = "fleet_router_api_block_requests_total";
    header(
        out,
        metric,
        "counter",
        "Chain API block requests, by region (unhinted: no block number the router could read) and by who wrote the answer (none: the router answered Busy).",
    );
    for &(name, api) in apis {
        for (region, role) in BLOCK_ANSWERS {
            let extra = format!("region=\"{}\",role=\"{}\"", region.as_str(), role.as_str());
            out.push_str(&format!(
                "{metric}{} {}\n",
                listener_labels(name, &extra),
                api.stats.block_requests(region, role)
            ));
        }
    }

    api_labeled_counter(
        out,
        apis,
        "fleet_router_api_pushes_total",
        "result",
        "Chain API pushes, by outcome: ok (a 2xx), error (another answer, or no node reached), ambiguous (the connection failed after the push was written; never resent).",
        |s| &s.pushes,
    );
}

/// Prometheus text-format label value. Backslash is replaced first so the
/// escapes inserted for `"` and newline are not doubled.
fn escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::models::{build_state_db, Server};

    #[tokio::test]
    async fn single_listener_metrics_and_readiness_keep_legacy_format() {
        let db = build_state_db(vec![Server {
            name: "s1".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            ..Default::default()
        }]);
        let listeners = vec![ListenerHealthState {
            name: None,
            state_db: db.clone(),
            hub: None,
            chain_api: None,
        }];

        let (status, _, body) = evaluate_overall_readiness(&listeners).await;
        assert_eq!(status, "503 Service Unavailable");
        assert_eq!(body, "not ready: no upstreams online\n");

        db.lock().await.get_mut("127.0.0.1:18080").unwrap().online = true;

        let (status, _, body) = evaluate_overall_readiness(&listeners).await;
        assert_eq!(status, "200 OK");
        assert_eq!(body, "ready: 1 upstream(s) online\n");

        let metrics = render_metrics(&listeners).await;
        assert!(metrics
            .contains("fleet_router_upstream_up{endpoint=\"127.0.0.1:18080\",server=\"s1\"} 1\n"));
        assert!(
            metrics.contains("fleet_router_up 1\n"),
            "process gauge must stay unlabeled"
        );
        assert!(
            !metrics.contains("fleet_router_up{"),
            "process gauge must not gain a server label"
        );
    }

    #[tokio::test]
    async fn multi_listener_metrics_and_readiness_include_listener_names() {
        let db_eos = build_state_db(vec![Server {
            name: "eos-1".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            ..Default::default()
        }]);
        let db_wax = build_state_db(vec![Server {
            name: "wax-1".to_string(),
            endpoint: "127.0.0.1:28080".to_string(),
            enabled: true,
            ..Default::default()
        }]);

        db_eos
            .lock()
            .await
            .get_mut("127.0.0.1:18080")
            .unwrap()
            .online = true;

        let listeners = vec![
            ListenerHealthState {
                name: Some("eos".to_string()),
                state_db: db_eos.clone(),
                hub: None,
                chain_api: None,
            },
            ListenerHealthState {
                name: Some("wax".to_string()),
                state_db: db_wax.clone(),
                hub: None,
                chain_api: None,
            },
        ];

        // Overall readiness is 503 while 'wax' has 0 online upstreams.
        let (status, _, body) = evaluate_overall_readiness(&listeners).await;
        assert_eq!(status, "503 Service Unavailable");
        assert!(body.contains("wax"), "unexpected body: {body}");

        // Per-listener readiness reflects each pool independently.
        let (eos_status, _, _) = evaluate_listener_readiness(&listeners, "eos").await;
        assert_eq!(eos_status, "200 OK");
        let (wax_status, _, _) = evaluate_listener_readiness(&listeners, "wax").await;
        assert_eq!(wax_status, "503 Service Unavailable");
        let (missing_status, _, _) = evaluate_listener_readiness(&listeners, "telos").await;
        assert_eq!(missing_status, "404 Not Found");

        // Once 'wax' comes online, overall readiness turns 200 OK.
        db_wax
            .lock()
            .await
            .get_mut("127.0.0.1:28080")
            .unwrap()
            .online = true;
        let (status, _, body) = evaluate_overall_readiness(&listeners).await;
        assert_eq!(status, "200 OK");
        assert_eq!(body, "ready: 2 listener(s) ready, 2 upstream(s) online\n");

        let metrics = render_metrics(&listeners).await;
        assert!(metrics.contains(
            "fleet_router_upstream_up{listener=\"eos\",endpoint=\"127.0.0.1:18080\",server=\"eos-1\"} 1\n"
        ));
        assert!(metrics.contains(
            "fleet_router_upstream_up{listener=\"wax\",endpoint=\"127.0.0.1:28080\",server=\"wax-1\"} 1\n"
        ));
    }

    #[test]
    fn prometheus_escape_covers_backslash_quote_and_newline() {
        assert_eq!(escape("plain"), "plain");
        assert_eq!(escape(r#"a\b"c"#), r#"a\\b\"c"#);
        assert_eq!(escape("line1\nline2"), r"line1\nline2");
        assert_eq!(escape("EOS \"SHiP\"\n1\\west"), r#"EOS \"SHiP\"\n1\\west"#);
    }

    #[tokio::test]
    async fn server_label_is_on_every_upstream_series_and_escaped() {
        let nasty = "EOS \"SHiP\"\n1\\west";
        let legacy = build_state_db(vec![Server {
            name: nasty.to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            priority: 2,
            weight: 3,
            ..Default::default()
        }]);
        let named = build_state_db(vec![Server {
            name: "wax node".to_string(),
            endpoint: "10.0.0.2:8080".to_string(),
            enabled: true,
            ..Default::default()
        }]);
        {
            let mut db = legacy.lock().await;
            let state = db.get_mut("127.0.0.1:18080").unwrap();
            state.online = true;
            state.stale = true;
            state.connections.store(3, Ordering::Relaxed);
            state.chain_state_end_block = 42;
            state.finality_data_begin_block = 7;
            state.finality_data_end_block = 99;
        }
        {
            let mut db = named.lock().await;
            let state = db.get_mut("10.0.0.2:8080").unwrap();
            state.online = false;
            state.chain_state_end_block = 5;
        }

        let listeners = vec![
            ListenerHealthState {
                name: None,
                state_db: legacy,
                hub: None,
                chain_api: None,
            },
            ListenerHealthState {
                name: Some("wax\"main".to_string()),
                state_db: named,
                hub: None,
                chain_api: None,
            },
        ];
        let metrics = render_metrics(&listeners).await;

        let legacy_labels = r#"endpoint="127.0.0.1:18080",server="EOS \"SHiP\"\n1\\west""#;
        let named_labels = r#"listener="wax\"main",endpoint="10.0.0.2:8080",server="wax node""#;
        let series = [
            ("fleet_router_upstream_up", "1", "0"),
            ("fleet_router_upstream_stale", "1", "0"),
            ("fleet_router_upstream_operator_disabled", "0", "0"),
            ("fleet_router_upstream_priority", "2", "0"),
            ("fleet_router_upstream_weight", "3", "1"),
            ("fleet_router_active_connections", "3", "0"),
            ("fleet_router_upstream_chain_state_end_block", "42", "5"),
            ("fleet_router_upstream_finality_data_begin_block", "7", "0"),
            ("fleet_router_upstream_finality_data_end_block", "99", "0"),
        ];
        for (name, legacy_value, named_value) in series {
            let legacy_line = format!("{name}{{{legacy_labels}}} {legacy_value}\n");
            let named_line = format!("{name}{{{named_labels}}} {named_value}\n");
            assert!(
                metrics.contains(&legacy_line),
                "missing legacy series {legacy_line} in:\n{metrics}"
            );
            assert!(
                metrics.contains(&named_line),
                "missing listener series {named_line} in:\n{metrics}"
            );
        }
    }

    fn assert_gauge(metrics: &str, name: &str, endpoint: &str, value: u32, listener: Option<&str>) {
        let line = metrics
            .lines()
            .find(|line| {
                line.starts_with(&format!("{name}{{"))
                    && line.contains(&format!("endpoint=\"{endpoint}\""))
            })
            .unwrap_or_else(|| panic!("missing {name} for endpoint {endpoint} in:\n{metrics}"));
        assert!(
            line.ends_with(&format!(" {value}")),
            "{name} for {endpoint}: expected {value}, got {line}"
        );
        match listener {
            Some(listener_name) => assert!(
                line.contains(&format!("listener=\"{listener_name}\"")),
                "missing listener label on {line}"
            ),
            None => assert!(
                !line.contains("listener="),
                "single-port series must not carry a listener label: {line}"
            ),
        }
    }

    #[tokio::test]
    async fn metrics_expose_ship_begin_blocks_including_zero() {
        let legacy = build_state_db(vec![Server {
            name: "s1".to_string(),
            endpoint: "127.0.0.1:18080".to_string(),
            enabled: true,
            ..Default::default()
        }]);
        let named = build_state_db(vec![Server {
            name: "wax-1".to_string(),
            endpoint: "127.0.0.1:28080".to_string(),
            enabled: true,
            ..Default::default()
        }]);
        {
            let mut db = legacy.lock().await;
            let state = db.get_mut("127.0.0.1:18080").unwrap();
            state.trace_begin_block = 100;
            state.chain_state_begin_block = 50;
            state.chain_state_end_block = 900;
            state.finality_data_begin_block = 400;
            state.finality_data_end_block = 900;
        }

        let listeners = vec![
            ListenerHealthState {
                name: None,
                state_db: legacy,
                hub: None,
                chain_api: None,
            },
            ListenerHealthState {
                name: Some("wax".to_string()),
                state_db: named,
                hub: None,
                chain_api: None,
            },
        ];
        let metrics = render_metrics(&listeners).await;

        assert!(metrics.contains(
            "# HELP fleet_router_upstream_trace_begin_block First block with trace data reported by an upstream (0 until a status result arrives).\n"
        ));
        assert!(metrics.contains("# TYPE fleet_router_upstream_trace_begin_block gauge\n"));
        assert!(metrics.contains(
            "# HELP fleet_router_upstream_chain_state_begin_block First block with chain-state data reported by an upstream (0 until a status result arrives).\n"
        ));
        assert!(metrics.contains("# TYPE fleet_router_upstream_chain_state_begin_block gauge\n"));
        assert!(metrics.contains(
            "# HELP fleet_router_upstream_finality_data_begin_block First block with finality data reported by an upstream (0 if unknown, including pre-Savanna nodes that only report get_status_result_v0).\n"
        ));
        assert!(metrics.contains("# TYPE fleet_router_upstream_finality_data_begin_block gauge\n"));

        for name in [
            "fleet_router_upstream_trace_begin_block",
            "fleet_router_upstream_chain_state_begin_block",
            "fleet_router_upstream_finality_data_begin_block",
        ] {
            assert_eq!(
                metrics
                    .lines()
                    .filter(|line| line.starts_with(&format!("{name}{{")))
                    .count(),
                2,
                "{name} must be emitted for every upstream, including 0"
            );
        }

        assert_gauge(
            &metrics,
            "fleet_router_upstream_trace_begin_block",
            "127.0.0.1:18080",
            100,
            None,
        );
        assert_gauge(
            &metrics,
            "fleet_router_upstream_chain_state_begin_block",
            "127.0.0.1:18080",
            50,
            None,
        );
        assert_gauge(
            &metrics,
            "fleet_router_upstream_chain_state_end_block",
            "127.0.0.1:18080",
            900,
            None,
        );
        assert_gauge(
            &metrics,
            "fleet_router_upstream_finality_data_begin_block",
            "127.0.0.1:18080",
            400,
            None,
        );
        // Unreported ranges stay 0 and are still exported. Finality stays 0
        // for pre-Savanna nodes that never send get_status_result_v1.
        assert_gauge(
            &metrics,
            "fleet_router_upstream_trace_begin_block",
            "127.0.0.1:28080",
            0,
            Some("wax"),
        );
        assert_gauge(
            &metrics,
            "fleet_router_upstream_chain_state_begin_block",
            "127.0.0.1:28080",
            0,
            Some("wax"),
        );
        assert_gauge(
            &metrics,
            "fleet_router_upstream_finality_data_begin_block",
            "127.0.0.1:28080",
            0,
            Some("wax"),
        );
    }

    #[tokio::test]
    async fn block_failure_metrics_use_upstream_labels_without_a_block_label() {
        use std::time::Duration;

        use crate::models::BlockQuarantine;

        let db = build_state_db(vec![
            Server {
                name: "bad".to_string(),
                endpoint: "127.0.0.1:18080".to_string(),
                enabled: true,
                ..Default::default()
            },
            Server {
                name: "good".to_string(),
                endpoint: "127.0.0.1:18081".to_string(),
                enabled: true,
                ..Default::default()
            },
        ]);
        {
            let mut guard = db.lock().await;
            let bad = guard.get_mut("127.0.0.1:18080").unwrap();
            bad.online = true;
            bad.block_failures = 3;
            bad.quarantines.push(BlockQuarantine {
                block: 200,
                range: 1,
                until: Instant::now() + Duration::from_secs(60),
            });
            bad.quarantines.push(BlockQuarantine {
                block: 115,
                range: 1,
                until: Instant::now() + Duration::from_secs(60),
            });
            // Expired exclusion must not keep the gauge up or hide block 115.
            bad.quarantines.push(BlockQuarantine {
                block: 10,
                range: 1,
                until: Instant::now()
                    .checked_sub(Duration::from_secs(1))
                    .unwrap_or_else(Instant::now),
            });
            guard.get_mut("127.0.0.1:18081").unwrap().online = true;
        }
        let listeners = vec![ListenerHealthState {
            name: Some("eos".to_string()),
            state_db: db,
            hub: None,
            chain_api: None,
        }];
        let metrics = render_metrics(&listeners).await;
        {
            let guard = listeners[0].state_db.lock().await;
            let bad = guard.get("127.0.0.1:18080").unwrap();
            assert_eq!(
                bad.quarantines.len(),
                2,
                "rendering metrics must drop the expired exclusion"
            );
            assert!(bad
                .quarantines
                .iter()
                .all(|quarantine| quarantine.block != 10));
        }
        let bad = "listener=\"eos\",endpoint=\"127.0.0.1:18080\",server=\"bad\"";
        let good = "listener=\"eos\",endpoint=\"127.0.0.1:18081\",server=\"good\"";
        assert!(metrics.contains(&format!(
            "fleet_router_upstream_block_failures_total{{{bad}}} 3\n"
        )));
        assert!(metrics.contains(&format!(
            "fleet_router_upstream_block_failures_total{{{good}}} 0\n"
        )));
        assert!(
            metrics.contains(&format!("fleet_router_upstream_quarantined{{{bad}}} 1\n")),
            "quarantine gauge must not carry a block label:\n{metrics}"
        );
        assert!(metrics.contains(&format!("fleet_router_upstream_quarantined{{{good}}} 0\n")));
        assert!(metrics.contains(&format!(
            "fleet_router_upstream_quarantine_block{{{bad}}} 115\n"
        )));
        assert!(metrics.contains(&format!(
            "fleet_router_upstream_quarantine_block{{{good}}} 0\n"
        )));
        assert!(!metrics.contains("block=\"115\""));
        assert!(metrics.contains("fleet_router_upstream_up{"));
    }

    #[tokio::test]
    async fn role_tip_range_and_abi_series_render_per_upstream() {
        use std::sync::Arc;

        use crate::abi::AbiEntry;
        use crate::models::Role;
        use crate::ship::{BlockPos, Range};

        let db = build_state_db(vec![
            Server {
                name: "live".to_string(),
                endpoint: "127.0.0.1:18080".to_string(),
                enabled: true,
                ..Default::default()
            },
            Server {
                name: "archive".to_string(),
                endpoint: "127.0.0.1:18081".to_string(),
                enabled: true,
                role: Role::Frozen,
                range: Some(Range {
                    begin: 2,
                    end: 1001,
                }),
                ..Default::default()
            },
        ]);
        let abi = Arc::new(AbiEntry::new(include_str!(
            "../mock-ship/src/ship_abi.json"
        )));
        {
            let mut guard = db.lock().await;
            let live = guard.get_mut("127.0.0.1:18080").unwrap();
            live.online = true;
            live.ranges_known = true;
            live.trace_begin_block = 900;
            live.trace_end_block = 2301;
            live.head = Some(BlockPos {
                num: 2300,
                id: [1; 32],
            });
            live.lib = Some(BlockPos {
                num: 2290,
                id: [2; 32],
            });
            live.abi = Some(abi.clone());
            // The archive has not reported yet: its declared range counts as
            // known, and a mismatch cannot be judged.
        }
        let listeners_for = |name: Option<&str>| {
            vec![ListenerHealthState {
                name: name.map(str::to_string),
                state_db: db.clone(),
                hub: None,
                chain_api: None,
            }]
        };

        for listener in [None, Some("main")] {
            let metrics = render_metrics(&listeners_for(listener)).await;
            let prefix = listener
                .map(|name| format!("listener=\"{name}\","))
                .unwrap_or_default();
            let live = format!("{prefix}endpoint=\"127.0.0.1:18080\",server=\"live\"");
            let archive = format!("{prefix}endpoint=\"127.0.0.1:18081\",server=\"archive\"");
            for line in [
                format!("fleet_router_upstream_trace_end_block{{{live}}} 2301\n"),
                format!("fleet_router_upstream_trace_end_block{{{archive}}} 0\n"),
                format!("fleet_router_upstream_head_block{{{live}}} 2300\n"),
                format!("fleet_router_upstream_lib_block{{{live}}} 2290\n"),
                format!("fleet_router_upstream_head_block{{{archive}}} 0\n"),
                format!("fleet_router_upstream_lib_block{{{archive}}} 0\n"),
                format!("fleet_router_upstream_info{{{live},role=\"live\"}} 1\n"),
                format!("fleet_router_upstream_info{{{archive},role=\"frozen\"}} 1\n"),
                format!("fleet_router_upstream_ranges_known{{{live}}} 1\n"),
                format!("fleet_router_upstream_ranges_known{{{archive}}} 1\n"),
                format!("fleet_router_upstream_range_mismatch{{{live}}} 0\n"),
                format!("fleet_router_upstream_range_mismatch{{{archive}}} 0\n"),
                format!(
                    "fleet_router_upstream_abi_info{{{live},fingerprint=\"{}\"}} 1\n",
                    abi.fp_hex()
                ),
                format!("fleet_router_upstream_abi_compatible{{{live}}} 1\n"),
            ] {
                assert!(metrics.contains(&line), "missing {line} in:\n{metrics}");
            }
            for name in [
                "fleet_router_upstream_trace_end_block",
                "fleet_router_upstream_head_block",
                "fleet_router_upstream_lib_block",
                "fleet_router_upstream_info",
                "fleet_router_upstream_ranges_known",
                "fleet_router_upstream_range_mismatch",
                "fleet_router_upstream_abi_info",
                "fleet_router_upstream_abi_compatible",
            ] {
                assert!(
                    metrics.contains(&format!("# TYPE {name} gauge\n")),
                    "missing TYPE for {name}"
                );
            }
            assert!(
                !metrics.contains(&format!("fleet_router_upstream_abi_info{{{archive}")),
                "no ABI series before the archive's monitor saw one"
            );
            assert!(!metrics.contains(&format!("fleet_router_upstream_abi_compatible{{{archive}")));
        }

        // The archive reports a range that differs from its declaration.
        {
            let mut guard = db.lock().await;
            let archive = guard.get_mut("127.0.0.1:18081").unwrap();
            archive.ranges_known = true;
            archive.trace_begin_block = 2;
            archive.trace_end_block = 900;
            archive.chain_state_begin_block = 2;
            archive.chain_state_end_block = 900;
        }
        let metrics = render_metrics(&listeners_for(None)).await;
        assert!(metrics.contains(
            "fleet_router_upstream_range_mismatch{endpoint=\"127.0.0.1:18081\",server=\"archive\"} 1\n"
        ));
    }

    mod fleet {
        use std::collections::HashMap;
        use std::time::Duration;

        use super::*;
        use crate::abi::AbiEntry;
        use crate::fleet::{FailoverReason, HoldWhy, LinkPurpose};
        use crate::models::{build_config_db, ResolvedListener, ServerState, StitchingPolicy};

        const ARCHIVE_A: &str = "127.0.0.1:19000";
        const ARCHIVE_B: &str = "127.0.0.1:19001";
        const LIVE: &str = "127.0.0.1:19002";

        fn frozen(name: &str, endpoint: &str, range: Option<(u32, u32)>) -> Server {
            Server {
                name: name.to_string(),
                endpoint: endpoint.to_string(),
                enabled: true,
                role: Role::Frozen,
                range: range.map(|(begin, end)| Range { begin, end }),
                ..Default::default()
            }
        }

        fn live(name: &str, endpoint: &str) -> Server {
            Server {
                name: name.to_string(),
                endpoint: endpoint.to_string(),
                enabled: true,
                ..Default::default()
            }
        }

        fn listener_state(
            name: Option<&str>,
            topology: ResolvedTopology,
            servers: Vec<Server>,
        ) -> ListenerHealthState {
            let db = build_state_db(servers.clone());
            let resolved = ResolvedListener {
                name: name.map(str::to_string),
                listen_address: "127.0.0.1".to_string(),
                listen_port: 17000,
                max_connections: 10,
                expected_chain_id: Some([7; 32]),
                require_chain_id: false,
                block_failure_quarantine: None,
                servers: servers.clone(),
                topology,
                stitching: StitchingPolicy::default(),
                abi_pin: None,
            };
            let hub = FleetHub::new(&resolved, db.clone(), build_config_db(servers));
            ListenerHealthState {
                name: name.map(str::to_string),
                state_db: db,
                hub: Some(hub),
                chain_api: None,
            }
        }

        fn pos(num: u32) -> BlockPos {
            BlockPos {
                num,
                id: [num as u8; 32],
            }
        }

        fn report(state: &mut ServerState, begin: u32, end: u32, head: u32, lib: u32) {
            state.online = true;
            state.ranges_known = true;
            state.trace_begin_block = begin;
            state.trace_end_block = end;
            state.chain_state_begin_block = begin;
            state.chain_state_end_block = end;
            state.head = Some(pos(head));
            state.lib = Some(pos(lib));
            state.offline_since = None;
        }

        /// Apply `f` to the listener's state map, then rebuild its view.
        async fn update(
            l: &ListenerHealthState,
            f: impl FnOnce(&mut HashMap<String, ServerState>),
        ) {
            let mut db = l.state_db.lock().await;
            f(&mut db);
            l.hub.as_ref().unwrap().rebuild(&db);
        }

        /// A = frozen [2,1001), B = frozen [1001,2001), both declared; C live.
        fn stitched(name: Option<&str>) -> ListenerHealthState {
            listener_state(
                name,
                ResolvedTopology::Stitched,
                vec![
                    frozen("archive-a", ARCHIVE_A, Some((2, 1001))),
                    frozen("archive-b", ARCHIVE_B, Some((1001, 2001))),
                    live("live-1", LIVE),
                ],
            )
        }

        /// B and C report; A stays down (declared only).
        async fn archive_a_down(l: &ListenerHealthState) {
            update(l, |db| {
                report(db.get_mut(ARCHIVE_B).unwrap(), 1001, 2001, 2000, 2000);
                report(db.get_mut(LIVE).unwrap(), 1900, 2301, 2300, 2290);
            })
            .await;
        }

        /// The status line and body of `GET path` from the HTTP endpoint.
        async fn get(listeners: &[ListenerHealthState], path: &str) -> (String, String) {
            let app = crate::api::AppState::health_only(listeners.to_vec());
            let resp = crate::http::request(
                app,
                &format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
            )
            .await;
            let (head, body) = resp.split_once("\r\n\r\n").unwrap();
            (head.lines().next().unwrap().to_string(), body.to_string())
        }

        #[tokio::test]
        async fn readiness_matrix_for_pool_stitched_and_archive_only_listeners() {
            // HE1, pool: unchanged, and history readiness is tip readiness.
            let pool = listener_state(
                Some("pool"),
                ResolvedTopology::Pool,
                vec![live("a", "127.0.0.1:18080")],
            );
            let pools = vec![pool.clone()];
            for path in ["/ready/pool", "/ready/pool?scope=history"] {
                let (status, body) = get(&pools, path).await;
                assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
                assert_eq!(body, "not ready (pool): no upstreams online\n");
            }
            update(&pool, |db| {
                db.get_mut("127.0.0.1:18080").unwrap().online = true;
            })
            .await;
            for path in [
                "/ready/pool",
                "/ready/pool?scope=history",
                "/ready?scope=tip",
            ] {
                assert_eq!(get(&pools, path).await.0, "HTTP/1.1 200 OK", "{path}");
            }

            // Stitched: the tip is servable with live online and an archive down,
            // history is not, and the body names the archive's segment.
            let main = stitched(Some("main"));
            let listeners = vec![main.clone()];
            let (status, body) = get(&listeners, "/ready/main").await;
            assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
            assert_eq!(body, "not ready (main): no live upstreams online\n");
            archive_a_down(&main).await;
            let (status, body) = get(&listeners, "/ready/main").await;
            assert_eq!(status, "HTTP/1.1 200 OK");
            assert_eq!(body, "ready (main): 2 upstream(s) online\n");
            assert_eq!(get(&listeners, "/ready").await.0, "HTTP/1.1 200 OK");
            let (status, body) = get(&listeners, "/ready/main?scope=history").await;
            assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
            assert_eq!(
                body,
                "not ready (main): segment [2,1001) has no online upstream (trace, chain_state)\n"
            );
            let (status, body) = get(&listeners, "/readyz?scope=history").await;
            assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
            assert_eq!(
                body,
                "not ready: main: segment [2,1001) has no online upstream (trace, chain_state)\n"
            );
            let (status, _) = get(&listeners, "/ready?scope=everything").await;
            assert_eq!(status, "HTTP/1.1 400 Bad Request");

            // A returns: history is complete.
            update(&main, |db| {
                report(db.get_mut(ARCHIVE_A).unwrap(), 2, 1001, 1000, 1000);
            })
            .await;
            assert_eq!(
                get(&listeners, "/ready/main?scope=history").await.0,
                "HTTP/1.1 200 OK"
            );

            // The live tier goes down: archives alone do not make the tip ready.
            update(&main, |db| {
                db.get_mut(LIVE).unwrap().mark_offline(Instant::now());
            })
            .await;
            let (status, body) = get(&listeners, "/ready/main").await;
            assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
            assert_eq!(body, "not ready (main): no live upstreams online\n");

            // A hole and an unreported, undeclared archive.
            let holes = listener_state(
                Some("holes"),
                ResolvedTopology::Stitched,
                vec![
                    frozen("archive-a", ARCHIVE_A, Some((2, 1001))),
                    frozen("archive-b", ARCHIVE_B, Some((1101, 2001))),
                    frozen("archive-c", "127.0.0.1:19003", None),
                    live("live-1", LIVE),
                ],
            );
            update(&holes, |db| {
                report(db.get_mut(ARCHIVE_A).unwrap(), 2, 1001, 1000, 1000);
                report(db.get_mut(ARCHIVE_B).unwrap(), 1101, 2001, 2000, 2000);
                report(db.get_mut(LIVE).unwrap(), 1900, 2301, 2300, 2290);
            })
            .await;
            let (status, body) = get(&[holes], "/ready/holes?scope=history").await;
            assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
            assert_eq!(
                body,
                "not ready (holes): gap [1001,1101) has no upstream (trace, chain_state); \
                 1 frozen upstream(s) have neither reported nor declared a range\n"
            );

            // Archive-only: any online upstream makes the tip ready.
            let archive = listener_state(
                None,
                ResolvedTopology::Stitched,
                vec![frozen("archive-a", ARCHIVE_A, Some((2, 1001)))],
            );
            let only = vec![archive.clone()];
            let (status, body) = get(&only, "/ready").await;
            assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
            assert_eq!(body, "not ready: no upstreams online\n");
            update(&archive, |db| {
                report(db.get_mut(ARCHIVE_A).unwrap(), 2, 1001, 1000, 1000);
            })
            .await;
            assert_eq!(
                get(&only, "/ready").await,
                (
                    "HTTP/1.1 200 OK".to_string(),
                    "ready: 1 upstream(s) online\n".to_string()
                )
            );
            assert_eq!(
                get(&only, "/ready?scope=history").await.0,
                "HTTP/1.1 200 OK"
            );

            // Overall readiness is the AND of every listener's tip readiness.
            let both = vec![pool, main];
            let (status, body) = get(&both, "/ready").await;
            assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
            assert_eq!(body, "not ready: main: no live upstreams online\n");
        }

        #[tokio::test]
        async fn coverage_marks_an_operator_disabled_upstream() {
            let l = stitched(None);
            update(&l, |db| {
                report(db.get_mut(ARCHIVE_A).unwrap(), 2, 1001, 1000, 1000);
                report(db.get_mut(ARCHIVE_B).unwrap(), 1001, 2001, 2000, 2000);
                report(db.get_mut(LIVE).unwrap(), 1900, 2301, 2300, 2290);
                db.get_mut(ARCHIVE_A).unwrap().operator_disabled = true;
            })
            .await;
            let (_, body) = get(std::slice::from_ref(&l), "/coverage").await;
            let doc: Value = serde_json::from_str(&body).unwrap();
            let trace = &doc["listeners"][0]["logs"]["trace"];
            assert_eq!(trace["offline_only"], json!([{ "begin": 2, "end": 1001 }]));
            assert_eq!(
                trace["segments"][0],
                json!({ "begin": 2, "end": 1001, "upstreams": [
                    { "server": "archive-a", "role": "frozen", "online": true,
                      "operator_disabled": true, "sessions": 0 } ] })
            );
            assert_eq!(
                trace["segments"][1]["upstreams"][0].get("operator_disabled"),
                None
            );
        }

        #[tokio::test]
        async fn coverage_endpoint_reports_segments_tip_and_abi() {
            // HE2
            let main = stitched(Some("main"));
            archive_a_down(&main).await;
            let abi = Arc::new(AbiEntry::new(include_str!(
                "../mock-ship/src/ship_abi.json"
            )));
            update(&main, |db| {
                db.get_mut(LIVE).unwrap().abi = Some(abi.clone());
                db.get_mut(ARCHIVE_B).unwrap().abi = Some(Arc::new(AbiEntry::new("{}")));
                db.get_mut(LIVE)
                    .unwrap()
                    .connections
                    .store(3, Ordering::Relaxed);
            })
            .await;
            let listeners = vec![main, stitched(Some("other"))];

            let (status, body) = get(&listeners, "/coverage").await;
            assert_eq!(status, "HTTP/1.1 200 OK");
            let all: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(all["listeners"].as_array().unwrap().len(), 2);

            let (status, body) = get(&listeners, "/coverage/main").await;
            assert_eq!(status, "HTTP/1.1 200 OK");
            let doc: Value = serde_json::from_str(&body).unwrap();
            let l = &doc["listeners"][0];
            assert_eq!(doc["listeners"].as_array().unwrap().len(), 1);
            assert_eq!(l["name"], "main");
            assert_eq!(l["topology"], "stitched");
            assert!(l["generation"].as_u64().unwrap() >= 2);
            assert_eq!(l["head"]["num"], 2300);
            assert_eq!(l["head"]["id"].as_str().unwrap().len(), 64);
            assert_eq!(l["lib"]["num"], 2290);
            assert_eq!(l["chain_id"], "07".repeat(32));
            assert_eq!(l["status_ready"], true);
            assert_eq!(l["frozen_unreported"], 0);
            assert_eq!(l["abi"]["fingerprint"], abi.fp_hex());
            assert_eq!(l["abi"]["source"], "live-1");
            assert_eq!(l["abi"]["mismatched"], json!(["archive-b"]));

            let trace = &l["logs"]["trace"];
            assert_eq!(trace["reported"], json!({ "begin": 2, "end": 2301 }));
            assert_eq!(trace["gaps"], json!([]));
            assert_eq!(trace["offline_only"], json!([{ "begin": 2, "end": 1001 }]));
            assert_eq!(
                trace["segments"],
                json!([
                    { "begin": 2, "end": 1001, "upstreams": [
                        { "server": "archive-a", "role": "frozen", "online": false, "sessions": 0 } ] },
                    { "begin": 1001, "end": 1900, "upstreams": [
                        { "server": "archive-b", "role": "frozen", "online": true, "sessions": 0 } ] },
                    { "begin": 1900, "end": 2001, "upstreams": [
                        { "server": "archive-b", "role": "frozen", "online": true, "sessions": 0 },
                        { "server": "live-1", "role": "live", "online": true, "sessions": 3 } ] },
                    { "begin": 2001, "end": 2301, "upstreams": [
                        { "server": "live-1", "role": "live", "online": true, "sessions": 3 } ] },
                ])
            );
            assert_eq!(
                l["logs"]["chain_state"]["reported"],
                json!({ "begin": 2, "end": 2301 })
            );
            assert_eq!(
                l["logs"]["finality"]["reported"],
                json!({ "begin": 0, "end": 0 })
            );
            assert_eq!(l["logs"]["finality"]["segments"], json!([]));

            // The other listener has seen nothing yet.
            let (_, body) = get(&listeners, "/coverage/other").await;
            let other: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(other["listeners"][0]["head"], Value::Null);
            assert_eq!(other["listeners"][0]["status_ready"], false);
            assert_eq!(other["listeners"][0]["abi"], Value::Null);

            assert_eq!(
                get(&listeners, "/coverage/missing").await.0,
                "HTTP/1.1 404 Not Found"
            );
            assert_eq!(
                get(&listeners, "/coverage/").await.0,
                "HTTP/1.1 404 Not Found"
            );

            // Single-listener mode: no name, and pool topology.
            let pool = listener_state(
                None,
                ResolvedTopology::Pool,
                vec![live("a", "127.0.0.1:18080")],
            );
            let (_, body) = get(&[pool], "/coverage").await;
            let doc: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(doc["listeners"][0]["name"], Value::Null);
            assert_eq!(doc["listeners"][0]["topology"], "pool");
        }

        #[tokio::test]
        async fn listener_series_render_with_and_without_the_listener_label() {
            // HE3
            for name in [None, Some("main")] {
                let l = stitched(name);
                archive_a_down(&l).await;
                let abi = Arc::new(AbiEntry::new(include_str!(
                    "../mock-ship/src/ship_abi.json"
                )));
                update(&l, |db| {
                    db.get_mut(LIVE).unwrap().abi = Some(abi.clone());
                    let archive_b = db.get_mut(ARCHIVE_B).unwrap();
                    archive_b.abi = Some(Arc::new(AbiEntry::new("{}")));
                    archive_b.no_block_log = true;
                })
                .await;
                let hub = l.hub.clone().unwrap();
                let stats = &hub.stats;
                stats.session_moved(None, Some(SessionGauge::Streaming));
                stats.failovers.inc(FailoverReason::Clean);
                stats.holds.add(HoldWhy::AtCapacity, 2);
                stats.status_reply(true, true);
                stats.handoffs.fetch_add(1, Ordering::Relaxed);
                stats.handoff_latency.observe(Duration::from_micros(500));
                stats.handoff_latency.observe(Duration::from_millis(30));
                stats.handoff_latency.observe(Duration::from_secs(20));
                stats.upstream(1).unwrap().links.inc(LinkPurpose::Handoff);
                stats
                    .upstream(1)
                    .unwrap()
                    .contract_violations
                    .inc(crate::fleet::ContractViolation::Gap);
                stats
                    .upstream(2)
                    .unwrap()
                    .sequence_jumps
                    .fetch_add(4, Ordering::Relaxed);
                stats
                    .upstream(1)
                    .unwrap()
                    .results_without_block
                    .fetch_add(3, Ordering::Relaxed);
                stats
                    .upstream(1)
                    .unwrap()
                    .prev_blocks_filled
                    .fetch_add(2, Ordering::Relaxed);
                stats.failovers.inc(FailoverReason::NoBlockLog);

                let metrics = render_metrics(std::slice::from_ref(&l)).await;
                let lab = |extra: &str| listener_labels(name, extra);
                let up = |endpoint: &str, server: &str, extra: &str| {
                    let base = format_labels(name, endpoint, server);
                    if extra.is_empty() {
                        format!("{{{base}}}")
                    } else {
                        format!("{{{base},{extra}}}")
                    }
                };
                let expected = [
                    format!("fleet_router_fleet_head_block{} 2300\n", lab("")),
                    format!("fleet_router_fleet_lib_block{} 2290\n", lab("")),
                    format!(
                        "fleet_router_fleet_coverage_begin_block{} 2\n",
                        lab("log=\"trace\"")
                    ),
                    format!(
                        "fleet_router_fleet_coverage_end_block{} 2301\n",
                        lab("log=\"chain_state\"")
                    ),
                    format!(
                        "fleet_router_fleet_coverage_end_block{} 0\n",
                        lab("log=\"finality\"")
                    ),
                    format!(
                        "fleet_router_fleet_coverage_gaps{} 0\n",
                        lab("log=\"trace\"")
                    ),
                    format!(
                        "fleet_router_fleet_offline_coverage_blocks{} 999\n",
                        lab("log=\"trace\"")
                    ),
                    format!(
                        "fleet_router_fleet_live_overlap_blocks{} 101\n",
                        lab("log=\"chain_state\"")
                    ),
                    format!("fleet_router_fleet_frozen_unreported{} 0\n", lab("")),
                    format!("fleet_router_sessions{} 1\n", lab("state=\"streaming\"")),
                    format!("fleet_router_sessions{} 0\n", lab("state=\"holding\"")),
                    format!(
                        "fleet_router_status_replies_total{} 1\n",
                        lab("version=\"v1\",source=\"router\"")
                    ),
                    format!(
                        "fleet_router_status_replies_total{} 0\n",
                        lab("version=\"v0\",source=\"upstream\"")
                    ),
                    format!("fleet_router_handoffs_total{} 1\n", lab("")),
                    format!(
                        "fleet_router_handoff_latency_seconds_bucket{} 1\n",
                        lab("le=\"0.001\"")
                    ),
                    format!(
                        "fleet_router_handoff_latency_seconds_bucket{} 2\n",
                        lab("le=\"0.05\"")
                    ),
                    format!(
                        "fleet_router_handoff_latency_seconds_bucket{} 2\n",
                        lab("le=\"10\"")
                    ),
                    format!(
                        "fleet_router_handoff_latency_seconds_bucket{} 3\n",
                        lab("le=\"+Inf\"")
                    ),
                    format!(
                        "fleet_router_handoff_latency_seconds_sum{} 20.0305\n",
                        lab("")
                    ),
                    format!("fleet_router_handoff_latency_seconds_count{} 3\n", lab("")),
                    format!(
                        "fleet_router_failovers_total{} 1\n",
                        lab("reason=\"clean\"")
                    ),
                    format!("fleet_router_failovers_total{} 0\n", lab("reason=\"send\"")),
                    format!("fleet_router_holds_total{} 2\n", lab("why=\"at_capacity\"")),
                    format!("fleet_router_hold_closes_total{} 0\n", lab("why=\"busy\"")),
                    format!(
                        "fleet_router_predials_total{} 0\n",
                        lab("result=\"discarded\"")
                    ),
                    format!(
                        "fleet_router_client_protocol_errors_total{} 0\n",
                        lab("kind=\"unknown_variant\"")
                    ),
                    format!("fleet_router_too_large_closes_total{} 0\n", lab("")),
                    format!("fleet_router_credit_overruns_total{} 0\n", lab("")),
                    format!(
                        "fleet_router_upstream_links_total{} 1\n",
                        up(ARCHIVE_B, "archive-b", "purpose=\"handoff\"")
                    ),
                    format!(
                        "fleet_router_upstream_dial_failures_total{} 0\n",
                        up(LIVE, "live-1", "reason=\"busy\"")
                    ),
                    format!(
                        "fleet_router_upstream_contract_violations_total{} 1\n",
                        up(ARCHIVE_B, "archive-b", "kind=\"gap\"")
                    ),
                    format!(
                        "fleet_router_upstream_sequence_jumps_total{} 4\n",
                        up(LIVE, "live-1", "")
                    ),
                    format!(
                        "fleet_router_upstream_no_block_log{} 1\n",
                        up(ARCHIVE_B, "archive-b", "")
                    ),
                    format!(
                        "fleet_router_upstream_no_block_log{} 0\n",
                        up(ARCHIVE_A, "archive-a", "")
                    ),
                    format!(
                        "fleet_router_upstream_results_without_block_total{} 3\n",
                        up(ARCHIVE_B, "archive-b", "")
                    ),
                    format!(
                        "fleet_router_upstream_prev_blocks_filled_total{} 2\n",
                        up(ARCHIVE_B, "archive-b", "")
                    ),
                    format!(
                        "fleet_router_failovers_total{} 1\n",
                        lab("reason=\"no_block_log\"")
                    ),
                    format!(
                        "fleet_router_upstream_stalls_total{} 0\n",
                        up(ARCHIVE_A, "archive-a", "")
                    ),
                    format!(
                        "fleet_router_upstream_abi_match{} 1\n",
                        up(LIVE, "live-1", "")
                    ),
                    format!(
                        "fleet_router_upstream_abi_match{} 0\n",
                        up(ARCHIVE_B, "archive-b", "")
                    ),
                ];
                for line in expected {
                    assert!(metrics.contains(&line), "missing {line:?} in:\n{metrics}");
                }
                assert!(
                    !metrics.contains(&format!(
                        "fleet_router_upstream_contract_violations_total{}",
                        up(LIVE, "live-1", "kind=\"gap\"")
                    )),
                    "violations are frozen-only"
                );
                for series in [
                    "fleet_router_upstream_no_block_log",
                    "fleet_router_upstream_results_without_block_total",
                    "fleet_router_upstream_prev_blocks_filled_total",
                ] {
                    assert!(
                        !metrics.contains(&format!("{series}{}", up(LIVE, "live-1", ""))),
                        "{series} is frozen-only"
                    );
                }
                assert!(
                    !metrics.contains(&format!(
                        "fleet_router_upstream_abi_match{}",
                        up(ARCHIVE_A, "archive-a", "")
                    )),
                    "no ABI seen for archive-a"
                );
                for (name, kind) in [
                    ("fleet_router_fleet_head_block", "gauge"),
                    ("fleet_router_fleet_head_age_seconds", "gauge"),
                    ("fleet_router_fleet_live_overlap_blocks", "gauge"),
                    ("fleet_router_sessions", "gauge"),
                    ("fleet_router_handoff_latency_seconds", "histogram"),
                    ("fleet_router_failovers_total", "counter"),
                    ("fleet_router_reroutes_total", "counter"),
                    ("fleet_router_duplicates_dropped_total", "counter"),
                    ("fleet_router_fork_redeliveries_total", "counter"),
                    ("fleet_router_keepalives_total", "counter"),
                    ("fleet_router_patched_results_total", "counter"),
                    ("fleet_router_upstream_links_total", "counter"),
                    ("fleet_router_upstream_abi_match", "gauge"),
                    ("fleet_router_upstream_no_block_log", "gauge"),
                    (
                        "fleet_router_upstream_results_without_block_total",
                        "counter",
                    ),
                    ("fleet_router_upstream_prev_blocks_filled_total", "counter"),
                ] {
                    assert_eq!(
                        metrics.matches(&format!("# TYPE {name} {kind}\n")).count(),
                        1,
                        "one TYPE line for {name}"
                    );
                }
                let age = metrics
                    .lines()
                    .find(|line| line.starts_with("fleet_router_fleet_head_age_seconds"))
                    .unwrap();
                let secs: f64 = age.rsplit(' ').next().unwrap().parse().unwrap();
                assert!(secs < 5.0, "{age}");
                if name.is_none() {
                    assert!(metrics.contains("fleet_router_fleet_head_block 2300\n"));
                    assert!(!metrics.contains("fleet_router_fleet_head_block{"));
                }
            }
        }

        #[tokio::test]
        async fn pool_listeners_have_no_abi_match_series() {
            let pool = listener_state(
                None,
                ResolvedTopology::Pool,
                vec![live("a", "127.0.0.1:18080")],
            );
            update(&pool, |db| {
                let s = db.get_mut("127.0.0.1:18080").unwrap();
                s.online = true;
                s.abi = Some(Arc::new(AbiEntry::new("{}")));
            })
            .await;
            let metrics = render_metrics(&[pool]).await;
            assert!(!metrics.contains("fleet_router_upstream_abi_match{"));
            assert!(metrics.contains(
                "fleet_router_upstream_links_total{endpoint=\"127.0.0.1:18080\",server=\"a\",purpose=\"initial\"} 0\n"
            ));
            assert!(metrics.contains("fleet_router_fleet_coverage_gaps{log=\"trace\"} 0\n"));
        }

        #[test]
        fn scope_parsing() {
            assert_eq!(parse_scope(None), Ok(Scope::Tip));
            assert_eq!(parse_scope(Some("probe=k8s&v=1")), Ok(Scope::Tip));
            assert_eq!(parse_scope(Some("scope=tip")), Ok(Scope::Tip));
            assert_eq!(parse_scope(Some("v=1&scope=history")), Ok(Scope::History));
            assert_eq!(parse_scope(Some("scope=all")), Err("all".to_string()));
        }
    }

    /// The chain API parts: readiness with `scope=api`, the `/coverage`
    /// block and the series. The listener's chain API is not served here:
    /// the probe state is set by hand.
    mod chain_api {
        use std::time::Duration;

        use super::*;
        use crate::chain_api::pick::RetryReason;
        use crate::chain_api::stats::{PushResult, Rejection, UpstreamResult};
        use crate::models::{build_config_db, build_state_db, ServerConfig};

        const ARCHIVE: &str = "127.0.0.1:19000";
        const LIVE: &str = "127.0.0.1:19002";
        const CHAIN: [u8; 32] = [0x5a; 32];

        /// A frozen archive with a chain API, and a live node with one when
        /// `live`.
        fn servers(live: bool) -> Value {
            let mut servers = vec![json!({
                "name": "archive-a",
                "endpoint": ARCHIVE,
                "enabled": true,
                "role": "frozen",
                "range": { "begin": 2, "end": 1001 },
                "api_endpoint": "http://127.0.0.1:18890",
            })];
            if live {
                servers.push(json!({
                    "name": "live-1",
                    "endpoint": LIVE,
                    "enabled": true,
                    "api_endpoint": "127.0.0.1:18888",
                }));
            }
            Value::Array(servers)
        }

        /// One listener over `servers` (named in multi-listener mode), with
        /// a chain API on 127.0.0.1:18889 when `with_api`.
        fn listener(name: Option<&str>, servers: Value, with_api: bool) -> ListenerHealthState {
            let mut entry = json!({
                "listen_port": 17000,
                "chain_id": hex(&CHAIN),
                "servers": servers,
            });
            if with_api {
                entry["chain_api"] = json!({ "listen_address": "127.0.0.1", "listen_port": 18889 });
            }
            let mut cfg = json!({
                "listen_address": "127.0.0.1",
                "upstream_reconnect_ms": 1000,
                "upstream_monitoring_ms": 1000,
                "upstream_status_ms": 1000,
            });
            match name {
                None => {
                    for (key, value) in entry.as_object().unwrap() {
                        cfg[key] = value.clone();
                    }
                }
                Some(name) => {
                    entry["name"] = json!(name);
                    cfg["listeners"] = json!([entry]);
                }
            }
            let cfg: ServerConfig = serde_json::from_value(cfg).unwrap();
            let resolved = cfg.resolved_listeners().remove(0);
            let db = build_state_db(resolved.servers.clone());
            let hub = FleetHub::new(
                &resolved,
                db.clone(),
                build_config_db(resolved.servers.clone()),
            );
            let chain_api = cfg
                .resolved_chain_apis()
                .remove(0)
                .map(|api| ChainApiCtx::new(&api, hub.clone(), Duration::from_secs(1), 1_000, 0));
            ListenerHealthState {
                name: name.map(str::to_string),
                state_db: db,
                hub: Some(hub),
                chain_api,
            }
        }

        fn api(l: &ListenerHealthState) -> &ChainApiCtx {
            l.chain_api.as_deref().unwrap()
        }

        fn slot(l: &ListenerHealthState, role: Role) -> &Arc<ApiSlot> {
            api(l)
                .directory
                .slots()
                .iter()
                .find(|s| s.role == role)
                .unwrap()
        }

        /// What good probes leave: the archive described and verified, the
        /// live node's head, LIB, earliest block and version; the listener
        /// accepting.
        fn probed(l: &ListenerHealthState) {
            for slot in api(l).directory.slots() {
                let mut state = slot.state();
                state.up = true;
                state.chain_id = Some(CHAIN);
                match slot.role {
                    Role::Frozen => {
                        let last = BlockPos {
                            num: 1000,
                            id: [1; 32],
                        };
                        state.blocks = Some(Range {
                            begin: 1,
                            end: 1001,
                        });
                        state.last = Some(last);
                        state.get_block = Some("off".to_string());
                        state.verification.described(last, true, Some(last));
                        state.verification.checked(last, last.id);
                    }
                    Role::Live => {
                        state.head = Some(2000);
                        state.lib = Some(1990);
                        state.earliest = Some(1);
                        state.version = Some("v1.2.2".to_string());
                    }
                }
            }
            api(l).stats.set_bound(true);
        }

        /// The API of `role` reports another chain (as a probe records it),
        /// or the listener's again.
        fn chain_mismatch(l: &ListenerHealthState, role: Role, mismatch: bool) {
            let mut state = slot(l, role).state();
            state.chain_mismatch = mismatch;
            state.up = !mismatch;
        }

        async fn get(listeners: &[ListenerHealthState], path: &str) -> (String, String) {
            let app = crate::api::AppState::health_only(listeners.to_vec());
            let resp = crate::http::request(
                app,
                &format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"),
            )
            .await;
            let (head, body) = resp.split_once("\r\n\r\n").unwrap();
            (head.lines().next().unwrap().to_string(), body.to_string())
        }

        fn ok(body: &str) -> (String, String) {
            ("HTTP/1.1 200 OK".to_string(), body.to_string())
        }

        fn unavailable(body: &str) -> (String, String) {
            (
                "HTTP/1.1 503 Service Unavailable".to_string(),
                body.to_string(),
            )
        }

        #[tokio::test]
        async fn api_readiness_needs_a_listening_chain_api_and_an_upstream_api() {
            assert_eq!(parse_scope(Some("v=1&scope=api")), Ok(Scope::Api));
            let l = listener(None, servers(true), true);
            let one = std::slice::from_ref(&l);
            assert_eq!(
                get(one, "/ready?scope=api").await,
                unavailable("not ready: chain API not listening; no live chain API upstream up\n")
            );
            probed(&l);
            assert_eq!(
                get(one, "/ready?scope=api").await,
                ok("ready: 2 chain API upstream(s) up\n")
            );
            // Independent of the SHiP side, which has nothing online.
            assert_eq!(
                get(one, "/ready").await.0,
                "HTTP/1.1 503 Service Unavailable"
            );

            // A live API on another chain does not count, and a verified
            // archive does not stand in while a live API is configured.
            chain_mismatch(&l, Role::Live, true);
            assert_eq!(
                get(one, "/ready?scope=api").await,
                unavailable("not ready: no live chain API upstream up\n")
            );
            chain_mismatch(&l, Role::Live, false);
            assert_eq!(get(one, "/ready?scope=api").await.0, "HTTP/1.1 200 OK");
            // Nor does one an operator disabled: the chain API skips it.
            {
                let mut db = l.state_db.lock().await;
                db.get_mut(LIVE).unwrap().operator_disabled = true;
                l.hub.as_ref().unwrap().rebuild(&db);
            }
            assert_eq!(
                get(one, "/ready?scope=api").await,
                unavailable("not ready: no live chain API upstream up\n")
            );

            // Archive-only: a verified frozen API that is up.
            let archive = listener(None, servers(false), true);
            api(&archive).stats.set_bound(true);
            slot(&archive, Role::Frozen).state().up = true;
            let only = std::slice::from_ref(&archive);
            assert_eq!(
                get(only, "/ready?scope=api").await,
                unavailable("not ready: no verified frozen chain API upstream up\n")
            );
            probed(&archive);
            assert_eq!(
                get(only, "/ready?scope=api").await,
                ok("ready: 1 chain API upstream(s) up\n")
            );

            // Unknown scopes name the new one.
            assert_eq!(
                get(only, "/ready?scope=apis").await,
                (
                    "HTTP/1.1 400 Bad Request".to_string(),
                    "unknown scope 'apis'; use tip, history or api\n".to_string()
                )
            );
        }

        #[tokio::test]
        async fn api_readiness_asks_only_listeners_with_a_chain_api() {
            let main = listener(Some("main"), servers(true), true);
            let plain = listener(Some("plain"), servers(true), false);
            let both = vec![main.clone(), plain.clone()];
            probed(&main);
            assert_eq!(
                get(&both, "/ready?scope=api").await,
                ok("ready: 1 listener(s) ready, 2 chain API upstream(s) up\n")
            );
            assert_eq!(
                get(&both, "/readyz/main?scope=api").await,
                ok("ready (main): 2 chain API upstream(s) up\n")
            );
            assert_eq!(
                get(&both, "/ready/plain?scope=api").await,
                unavailable("not ready (plain): no chain API configured\n")
            );
            assert_eq!(
                get(&both, "/ready/missing?scope=api").await.0,
                "HTTP/1.1 404 Not Found"
            );
            assert_eq!(
                get(std::slice::from_ref(&plain), "/ready?scope=api").await,
                unavailable("not ready: no chain API configured\n")
            );
            let single = listener(None, servers(true), false);
            assert_eq!(
                get(std::slice::from_ref(&single), "/ready?scope=api").await,
                unavailable("not ready: no chain API configured\n")
            );

            chain_mismatch(&main, Role::Live, true);
            assert_eq!(
                get(&both, "/ready?scope=api").await,
                unavailable("not ready: main: no live chain API upstream up\n")
            );
            api(&main).stats.set_bound(false);
            assert_eq!(
                get(&both, "/ready/main?scope=api").await,
                unavailable(
                    "not ready (main): chain API not listening; no live chain API upstream up\n"
                )
            );
        }

        #[tokio::test]
        async fn coverage_lists_the_chain_api_upstreams() {
            let main = listener(Some("main"), servers(true), true);
            let plain = listener(Some("plain"), servers(true), false);
            let both = vec![main.clone(), plain];

            // Before any probe: nothing is known.
            let (_, body) = get(&both, "/coverage").await;
            let doc: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(
                doc["listeners"][0]["chain_api"],
                json!({
                    "listen": "127.0.0.1:18889",
                    "live": [{ "server": "live-1", "up": false, "head": null, "lib": null,
                               "earliest": null, "version": null }],
                    "frozen": [{ "server": "archive-a", "up": false, "verified": false,
                                 "blocks": null, "get_block": null }],
                })
            );
            assert_eq!(doc["listeners"][1]["chain_api"], Value::Null);

            probed(&main);
            chain_mismatch(&main, Role::Live, true);
            let (_, body) = get(&both, "/coverage/main").await;
            let doc: Value = serde_json::from_str(&body).unwrap();
            assert_eq!(
                doc["listeners"][0]["chain_api"],
                json!({
                    "listen": "127.0.0.1:18889",
                    "live": [{ "server": "live-1", "up": false, "head": 2000, "lib": 1990,
                               "earliest": 1, "version": "v1.2.2" }],
                    "frozen": [{ "server": "archive-a", "up": true, "verified": true,
                                 "blocks": { "begin": 1, "end": 1001 }, "get_block": "off" }],
                })
            );
        }

        #[tokio::test]
        async fn chain_api_series_render_per_upstream_and_per_listener() {
            for name in [None, Some("main")] {
                let l = listener(name, servers(true), true);
                probed(&l);
                let archive = slot(&l, Role::Frozen);
                let live = slot(&l, Role::Live);
                archive.set_cap(4);
                archive.results.add(UpstreamResult::Ok, 2);
                live.results.inc(UpstreamResult::UnknownBlock);
                let _held = live.try_acquire().unwrap();
                let stats = &api(&l).stats;
                stats.request(RequestClass::Block, 200, Duration::from_millis(3));
                stats.request(RequestClass::Block, 503, Duration::from_millis(30));
                stats.request(RequestClass::Denied, 404, Duration::from_micros(10));
                stats.rejected.inc(Rejection::InFlight);
                stats.retries.inc(RetryReason::UnknownBlock);
                stats.block_request(BlockRegion::Irreversible, AnswerRole::Frozen);
                stats.pushes.inc(PushResult::Ambiguous);

                let metrics = render_metrics(std::slice::from_ref(&l)).await;
                let lab = |extra: &str| listener_labels(name, extra);
                let up = |endpoint: &str, server: &str, extra: &str| {
                    let base = format_labels(name, endpoint, server);
                    if extra.is_empty() {
                        format!("{{{base}}}")
                    } else {
                        format!("{{{base},{extra}}}")
                    }
                };
                let a = |extra: &str| up(ARCHIVE, "archive-a", extra);
                let v = |extra: &str| up(LIVE, "live-1", extra);
                let expected = [
                    format!("fleet_router_upstream_api_up{} 1\n", a("")),
                    format!("fleet_router_upstream_api_up{} 1\n", v("")),
                    format!("fleet_router_upstream_api_chain_id_mismatch{} 0\n", v("")),
                    format!("fleet_router_upstream_api_head_block{} 2000\n", v("")),
                    format!("fleet_router_upstream_api_earliest_block{} 1\n", v("")),
                    format!("fleet_router_upstream_api_blocks_begin_block{} 1\n", a("")),
                    format!("fleet_router_upstream_api_blocks_end_block{} 1001\n", a("")),
                    format!("fleet_router_upstream_api_verified{} 1\n", a("")),
                    format!("fleet_router_upstream_api_in_flight{} 0\n", a("")),
                    format!("fleet_router_upstream_api_in_flight{} 1\n", v("")),
                    format!("fleet_router_upstream_api_max_in_flight{} 4\n", a("")),
                    format!("fleet_router_upstream_api_max_in_flight{} 64\n", v("")),
                    format!(
                        "fleet_router_upstream_api_requests_total{} 2\n",
                        a("result=\"ok\"")
                    ),
                    format!(
                        "fleet_router_upstream_api_requests_total{} 1\n",
                        v("result=\"unknown_block\"")
                    ),
                    format!(
                        "fleet_router_upstream_api_requests_total{} 0\n",
                        v("result=\"timeout\"")
                    ),
                    format!(
                        "fleet_router_api_requests_total{} 1\n",
                        lab("class=\"block\",code=\"2xx\"")
                    ),
                    format!(
                        "fleet_router_api_requests_total{} 1\n",
                        lab("class=\"block\",code=\"5xx\"")
                    ),
                    format!(
                        "fleet_router_api_requests_total{} 1\n",
                        lab("class=\"denied\",code=\"4xx\"")
                    ),
                    format!(
                        "fleet_router_api_requests_total{} 0\n",
                        lab("class=\"invalid\",code=\"3xx\"")
                    ),
                    format!(
                        "fleet_router_api_request_duration_seconds_bucket{} 1\n",
                        lab("class=\"block\",le=\"0.005\"")
                    ),
                    format!(
                        "fleet_router_api_request_duration_seconds_bucket{} 2\n",
                        lab("class=\"block\",le=\"0.05\"")
                    ),
                    format!(
                        "fleet_router_api_request_duration_seconds_bucket{} 2\n",
                        lab("class=\"block\",le=\"+Inf\"")
                    ),
                    format!(
                        "fleet_router_api_request_duration_seconds_sum{} 0.033\n",
                        lab("class=\"block\"")
                    ),
                    format!(
                        "fleet_router_api_request_duration_seconds_count{} 2\n",
                        lab("class=\"block\"")
                    ),
                    format!(
                        "fleet_router_api_request_duration_seconds_count{} 0\n",
                        lab("class=\"push\"")
                    ),
                    format!("fleet_router_api_in_flight{} 0\n", lab("")),
                    format!(
                        "fleet_router_api_rejected_total{} 1\n",
                        lab("reason=\"in_flight\"")
                    ),
                    format!(
                        "fleet_router_api_rejected_total{} 0\n",
                        lab("reason=\"connections\"")
                    ),
                    format!(
                        "fleet_router_api_retries_total{} 1\n",
                        lab("reason=\"unknown_block\"")
                    ),
                    format!(
                        "fleet_router_api_retries_total{} 0\n",
                        lab("reason=\"error\"")
                    ),
                    format!(
                        "fleet_router_api_block_requests_total{} 1\n",
                        lab("region=\"irreversible\",role=\"frozen\"")
                    ),
                    format!(
                        "fleet_router_api_block_requests_total{} 0\n",
                        lab("region=\"unhinted\",role=\"none\"")
                    ),
                    format!(
                        "fleet_router_api_pushes_total{} 1\n",
                        lab("result=\"ambiguous\"")
                    ),
                ];
                for line in expected {
                    assert!(metrics.contains(&line), "missing {line:?} in:\n{metrics}");
                }
                // Role-specific series, and answers that cannot occur.
                for absent in [
                    format!("fleet_router_upstream_api_head_block{}", a("")),
                    format!("fleet_router_upstream_api_earliest_block{}", a("")),
                    format!("fleet_router_upstream_api_verified{}", v("")),
                    format!("fleet_router_upstream_api_blocks_begin_block{}", v("")),
                    format!(
                        "fleet_router_api_block_requests_total{}",
                        lab("region=\"reversible\",role=\"frozen\"")
                    ),
                    format!(
                        "fleet_router_api_block_requests_total{}",
                        lab("region=\"unhinted\",role=\"frozen\"")
                    ),
                ] {
                    assert!(
                        !metrics.contains(&absent),
                        "unexpected {absent} in:\n{metrics}"
                    );
                }
                for (metric, kind, series) in [
                    ("fleet_router_upstream_api_up", "gauge", 2),
                    ("fleet_router_upstream_api_chain_id_mismatch", "gauge", 2),
                    ("fleet_router_upstream_api_head_block", "gauge", 1),
                    ("fleet_router_upstream_api_earliest_block", "gauge", 1),
                    ("fleet_router_upstream_api_blocks_begin_block", "gauge", 1),
                    ("fleet_router_upstream_api_blocks_end_block", "gauge", 1),
                    ("fleet_router_upstream_api_verified", "gauge", 1),
                    ("fleet_router_upstream_api_in_flight", "gauge", 2),
                    ("fleet_router_upstream_api_max_in_flight", "gauge", 2),
                    ("fleet_router_upstream_api_requests_total", "counter", 14),
                    ("fleet_router_api_requests_total", "counter", 32),
                    (
                        "fleet_router_api_request_duration_seconds",
                        "histogram",
                        8 * 15,
                    ),
                    ("fleet_router_api_in_flight", "gauge", 1),
                    ("fleet_router_api_rejected_total", "counter", 4),
                    ("fleet_router_api_retries_total", "counter", 6),
                    ("fleet_router_api_block_requests_total", "counter", 7),
                    ("fleet_router_api_pushes_total", "counter", 3),
                ] {
                    assert_eq!(
                        metrics
                            .matches(&format!("# TYPE {metric} {kind}\n"))
                            .count(),
                        1,
                        "one TYPE line for {metric}"
                    );
                    let lines: Vec<&str> = metrics
                        .lines()
                        .filter(|line| {
                            line.strip_prefix(metric).is_some_and(|rest| {
                                rest.starts_with('{')
                                    || rest.starts_with(' ')
                                    || rest.starts_with("_bucket{")
                                    || rest.starts_with("_sum")
                                    || rest.starts_with("_count")
                            })
                        })
                        .collect();
                    assert_eq!(lines.len(), series, "{metric}: {lines:#?}");
                    assert!(
                        lines
                            .iter()
                            .all(|line| line.contains("listener=") == name.is_some()),
                        "{metric}: the listener label only with several listeners: {lines:#?}"
                    );
                }
                if name.is_none() {
                    assert!(metrics.contains("fleet_router_api_in_flight 0\n"));
                }
            }
        }

        #[tokio::test]
        async fn no_chain_api_series_without_a_chain_api() {
            let plain = listener(None, servers(true), false);
            let metrics = render_metrics(&[plain]).await;
            assert!(!metrics.contains("fleet_router_api_"), "{metrics}");
            assert!(!metrics.contains("fleet_router_upstream_api_"), "{metrics}");
        }
    }
}
