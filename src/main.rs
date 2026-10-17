//! # fleet-router
//!
//! A reverse proxy and load balancer for the Antelope **SHiP** (State History
//! Plugin) WebSocket protocol. Clients connect to the router; it selects an
//! upstream SHiP node (range-aware, least-connections) and proxies the
//! WebSocket bidirectionally, transparently failing over to another upstream
//! and de-duplicating replayed blocks on reconnect.
//!
//! Run with `fleet-router run --config config.json`. See `docs/configuration.md`
//! for the full configuration reference and `docs/operations.md` for
//! operational guidance.

use std::any::Any;
use std::collections::HashSet;
use std::fs::{read_to_string, write};
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use clap::{arg, value_parser, Command};
use color_print::cprintln;
use futures::{SinkExt, StreamExt};
use serde_json::from_str;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::{broadcast, Semaphore};
use tokio::time::{sleep, timeout, timeout_at};
use tokio_tungstenite::connect_async_with_config;
use tokio_tungstenite::tungstenite::Message;
use tracing::Instrument;

use crate::config_sample::CONFIG_SAMPLE;
use crate::fleet::{FleetHub, FleetView, Log};
use crate::models::{
    build_config_db, build_state_db_with_chain_id, format_bind_addr, ResolvedListener,
    ResolvedTopology, Role, ServerConfig, ServerConfigDb, ServerStateDb, StaticConfig,
};
use crate::session::{handle_client, ListenerCtx};
use crate::ship::wire::{self, UpstreamMsg};
use crate::ship::Range;
use crate::tasks::{
    apply_status_update, monitoring_connection, state_monitoring_loop, upstream_ws_config,
};

mod abi;
mod api;
mod config_sample;
mod fleet;
mod functions;
mod health;
mod http;
mod models;
mod session;
mod ship;
mod tasks;
mod telemetry;
mod zcd;

#[tokio::main]
async fn main() -> Result<()> {
    // rustls 0.23 panics while building a ClientConfig when both the ring and
    // aws-lc-rs providers are linked and neither was installed. This binary
    // selects aws-lc-rs, the provider reqwest 0.13 already uses.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .ok();

    let config: ServerConfig = match process_args() {
        Ok(Some((Action::Test { strict }, conf))) => {
            if !run_tests(&conf, strict).await {
                std::process::exit(1);
            }
            return Ok(());
        }
        Ok(Some((Action::Run, conf))) => conf,
        Ok(None) => return Ok(()),
        Err(e) => bail!(e),
    };

    init_tracing();

    // Validate the configuration up front with clear, actionable errors.
    config.validate()?;
    for warning in config.warnings() {
        tracing::warn!("{warning}");
    }
    let control_token = if config.dashboard.enabled {
        config.dashboard.resolve_control_token()?
    } else {
        if config.dashboard.control_token.is_some() || config.dashboard.control_token_file.is_some()
        {
            tracing::warn!("a control token is configured but dashboard.enabled is false; the control API stays off");
        }
        None
    };

    let static_config = StaticConfig {
        upstream_status_ms: config.upstream_status_ms,
        upstream_monitoring_ms: config.upstream_monitoring_ms,
        upstream_reconnect_ms: config.upstream_reconnect_ms,
        connect_timeout_ms: config.upstream_connect_timeout_ms(),
        max_message_bytes: config.max_message_bytes,
    };
    let limits = config.proxy_limits();
    let resolved_listeners = config.resolved_listeners();

    // Bind all client TCP listeners up front so any port conflict fails fast.
    let mut bound_listeners = Vec::with_capacity(resolved_listeners.len());
    for listener_cfg in resolved_listeners {
        let bind_addr = format_bind_addr(&listener_cfg.listen_address, listener_cfg.listen_port);
        let tcp_listener = TcpListener::bind(&bind_addr)
            .await
            .map_err(|e| anyhow::anyhow!("error binding to {}: {}", bind_addr, e))?;
        match &listener_cfg.name {
            Some(name) => {
                tracing::info!(
                    listener = %name,
                    address = %listener_cfg.listen_address,
                    port = listener_cfg.listen_port,
                    "listening for clients"
                );
                if listener_cfg.expected_chain_id.is_none() {
                    tracing::warn!(
                        listener = %name,
                        port = listener_cfg.listen_port,
                        "listener has no pinned chain_id; first online upstream will establish the pool chain_id (set chain_id in config to prevent cross-chain misconfiguration)"
                    );
                }
            }
            None => {
                tracing::info!(
                    address = %listener_cfg.listen_address,
                    port = listener_cfg.listen_port,
                    "listening for clients"
                );
            }
        }
        bound_listeners.push((listener_cfg, tcp_listener));
    }

    // Graceful-shutdown broadcast: every long-lived task subscribes.
    let (shutdown_tx, _) = broadcast::channel::<()>(64);

    let mut health_states = Vec::with_capacity(bound_listeners.len());
    let mut listener_contexts = Vec::with_capacity(bound_listeners.len());
    let mut conn_limits = Vec::with_capacity(bound_listeners.len());
    let mut accept_handles = Vec::with_capacity(bound_listeners.len());

    for (listener_cfg, tcp_listener) in bound_listeners {
        let server_config_db: ServerConfigDb = build_config_db(listener_cfg.servers.clone());
        let server_state_db: ServerStateDb = build_state_db_with_chain_id(
            listener_cfg.servers.clone(),
            listener_cfg.expected_chain_id,
            listener_cfg.require_chain_id,
        );
        // The listener's fleet view: monitors publish into it, the metrics
        // endpoint reads it.
        let hub = FleetHub::new(
            &listener_cfg,
            server_state_db.clone(),
            server_config_db.clone(),
        );

        let health_state = health::ListenerHealthState {
            name: listener_cfg.name.clone(),
            state_db: server_state_db.clone(),
            hub: Some(hub.clone()),
        };
        let listener_stats = Arc::new(telemetry::ListenerStats::default());
        listener_contexts.push(api::ListenerContext {
            health: health_state.clone(),
            config_db: server_config_db.clone(),
            order: listener_cfg
                .servers
                .iter()
                .filter(|s| s.enabled)
                .map(|s| s.endpoint.clone())
                .collect(),
            port: listener_cfg.listen_port,
            chain_id: listener_cfg.expected_chain_id,
            max_connections: listener_cfg.max_connections,
            stats: listener_stats.clone(),
        });
        health_states.push(health_state);

        // Per-upstream monitoring loops for this listener's pool.
        for server in listener_cfg.servers.iter().filter(|s| s.enabled).cloned() {
            match &listener_cfg.name {
                Some(listener_name) => {
                    tracing::info!(
                        listener = %listener_name,
                        name = %server.name,
                        upstream = %server.endpoint,
                        "starting upstream monitor"
                    );
                }
                None => {
                    tracing::info!(
                        name = %server.name,
                        upstream = %server.endpoint,
                        "starting upstream monitor"
                    );
                }
            }
            spawn(monitoring_connection(
                server,
                static_config.clone(),
                hub.clone(),
                shutdown_tx.subscribe(),
            ));
        }

        // What every client session of this listener shares.
        let ctx = ListenerCtx::new(&listener_cfg, limits, config.live_lag_blocks, hub.clone());

        // Periodic block-progress / staleness monitor for this listener's pool.
        spawn(state_monitoring_loop(
            hub,
            static_config.upstream_monitoring_ms,
            shutdown_tx.subscribe(),
        ));

        // Connection backpressure: at most `max_connections` concurrent clients on this listener.
        let conn_limit = Arc::new(Semaphore::new(listener_cfg.max_connections));
        let max_connections = listener_cfg.max_connections;
        conn_limits.push((conn_limit.clone(), max_connections));

        let listener_name = listener_cfg.name.clone();
        let listener_shutdown_tx = shutdown_tx.clone();
        let mut accept_shutdown_rx = shutdown_tx.subscribe();

        accept_handles.push(spawn(async move {
            loop {
                tokio::select! {
                    result = tcp_listener.accept() => {
                        let (client_stream, client_addr) = match result {
                            Ok(pair) => pair,
                            Err(e) => {
                                tracing::warn!(error = %e, "error accepting incoming connection");
                                continue;
                            }
                        };

                        // Reject immediately when at capacity (backpressure, not queueing).
                        let permit = match conn_limit.clone().try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
                                listener_stats.rejected_total.fetch_add(1, Ordering::Relaxed);
                                tracing::warn!(%client_addr, max = max_connections, "connection limit reached; rejecting");
                                drop(client_stream);
                                continue;
                            }
                        };

                        listener_stats.accepted_total.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!(%client_addr, "accepted tcp connection");

                        let ctx = ctx.clone();
                        let shutdown_rx = listener_shutdown_tx.subscribe();
                        let span = match &listener_name {
                            Some(name) => tracing::info_span!("client", listener = %name, addr = %client_addr),
                            None => tracing::info_span!("client", addr = %client_addr),
                        };

                        spawn(
                            async move {
                                let _permit = permit; // released when this task ends
                                handle_client(client_stream, client_addr, ctx, shutdown_rx).await;
                            }
                            .instrument(span),
                        );
                    }
                    _ = accept_shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        }));
    }

    // Optional health/metrics HTTP endpoint, with the opt-in dashboard.
    if let Some((host, port)) = config.resolved_metrics_host_port() {
        let addr = format_bind_addr(&host, port);
        if control_token.is_some() && !is_loopback_host(&host) {
            tracing::warn!(
                %addr,
                "the control API is enabled on a non-loopback address; restrict access to this port"
            );
        }
        let app = Arc::new(api::AppState {
            listeners: listener_contexts,
            health: health_states,
            dashboard: config.dashboard.enabled,
            control_token,
            allowed_hosts: config
                .dashboard
                .allowed_hosts
                .iter()
                .map(|host| host.trim().to_ascii_lowercase())
                .collect(),
            streams: Arc::new(tokio::sync::Semaphore::new(api::MAX_STREAMS)),
        });
        if app.dashboard {
            spawn(api::run_sampler(app.clone(), shutdown_tx.subscribe()));
        }
        spawn(http::run_http_server(addr, app, shutdown_tx.subscribe()));
    }
    telemetry::global().emit(telemetry::Event::new(
        telemetry::Level::Info,
        telemetry::EventKind::Started,
        format!("fleet-router {} started", env!("CARGO_PKG_VERSION")),
    ));

    shutdown_signal().await;
    tracing::info!("shutdown signal received; draining connections");
    let _ = shutdown_tx.send(());
    for handle in accept_handles {
        let _ = handle.await;
    }
    drain_connections(&conn_limits, config.shutdown_grace_ms).await;
    tracing::info!("shutdown complete");
    Ok(())
}

/// Initialize `tracing` with a `RUST_LOG`-driven filter (default `info`).
fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_target(false).init();
}

/// Whether `host` only accepts local connections.
fn is_loopback_host(host: &str) -> bool {
    let host = host.trim().trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// Resolve when a shutdown signal (SIGINT/Ctrl+C, or SIGTERM on Unix) arrives.
#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Wait (up to `grace_ms`) for active client connections across all listeners to drain.
async fn drain_connections(conn_limits: &[(Arc<Semaphore>, usize)], grace_ms: u64) {
    let start = Instant::now();
    loop {
        let remaining: usize = conn_limits
            .iter()
            .map(|(sem, max)| max.saturating_sub(sem.available_permits()))
            .sum();
        if remaining == 0 {
            break;
        }
        if start.elapsed() >= Duration::from_millis(grace_ms) {
            tracing::warn!(remaining, "shutdown grace period elapsed; forcing exit");
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

enum Probe {
    Abi,
    ConnectTimeout,
    ConnectFailed(String),
    ReadTimeout,
    NoMessage,
    ReadFailed(String),
    InvalidAbi,
    Unexpected(String),
}

/// `config test`: connect, read the ABI frame, and report. Both steps are
/// bounded, and the socket uses the configured message size.
async fn probe_upstream(endpoint: &str, max_message_bytes: usize, bound: Duration) -> Probe {
    let connect =
        connect_async_with_config(endpoint, Some(upstream_ws_config(max_message_bytes)), true);
    let ws = match timeout(bound, connect).await {
        Err(_) => return Probe::ConnectTimeout,
        Ok(Err(e)) => return Probe::ConnectFailed(e.to_string()),
        Ok(Ok((ws, _))) => ws,
    };

    let (_, mut reader) = ws.split();
    match timeout(bound, reader.next()).await {
        Err(_) => Probe::ReadTimeout,
        Ok(None) => Probe::NoMessage,
        Ok(Some(Err(e))) => Probe::ReadFailed(e.to_string()),
        Ok(Some(Ok(Message::Text(txt)))) => {
            if txt.contains("eosio::abi") {
                Probe::Abi
            } else {
                Probe::InvalidAbi
            }
        }
        Ok(Some(Ok(other))) => Probe::Unexpected(format!("{:?}", other.type_id())),
    }
}

/// `config test`: ask an upstream for its status, v1 first and then v0 for a
/// node that does not answer v1. Returns the raw status result.
async fn probe_status(
    endpoint: &str,
    max_message_bytes: usize,
    bound: Duration,
) -> Option<Vec<u8>> {
    for v1 in [true, false] {
        if let Some(status) = probe_status_once(endpoint, max_message_bytes, bound, v1).await {
            return Some(status);
        }
    }
    None
}

/// One status probe; the whole exchange is bounded by `bound`.
async fn probe_status_once(
    endpoint: &str,
    max_message_bytes: usize,
    bound: Duration,
    v1: bool,
) -> Option<Vec<u8>> {
    let deadline = tokio::time::Instant::now() + bound;
    let connect =
        connect_async_with_config(endpoint, Some(upstream_ws_config(max_message_bytes)), true);
    let (ws, _) = timeout_at(deadline, connect).await.ok()?.ok()?;
    let (mut writer, mut reader) = ws.split();
    // The ABI comes first.
    match timeout_at(deadline, reader.next()).await {
        Ok(Some(Ok(Message::Text(_)))) => {}
        _ => return None,
    }
    let request = Message::Binary(wire::encode_status_request(v1).into());
    timeout_at(deadline, writer.send(request))
        .await
        .ok()?
        .ok()?;
    loop {
        match timeout_at(deadline, reader.next()).await.ok()?? {
            Ok(Message::Binary(frame)) => {
                if matches!(wire::classify_upstream(&frame), UpstreamMsg::Status { .. }) {
                    let _ = timeout_at(deadline, writer.send(Message::Close(None))).await;
                    return Some(frame.to_vec());
                }
            }
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => {}
        }
    }
}

/// `[begin,end)`, or `-` for an empty range.
fn range_text(r: Range) -> String {
    if r.is_empty() {
        "-".to_string()
    } else {
        format!("[{},{})", r.begin, r.end)
    }
}

/// `config test` for a stitched listener: apply the probed status results the
/// way the monitor does, then print each upstream's ranges and the coverage
/// they add up to. Returns what `--strict` fails on: a hole in the trace or
/// chain-state coverage, and an enabled frozen upstream that did not report
/// its ranges (a declared range is not verified).
async fn report_coverage(
    listener: &ResolvedListener,
    statuses: &[(String, Option<Vec<u8>>)],
) -> Vec<String> {
    let state_db = build_state_db_with_chain_id(
        listener.servers.clone(),
        listener.expected_chain_id,
        listener.require_chain_id,
    );
    let hub = FleetHub::new(
        listener,
        state_db,
        build_config_db(listener.servers.clone()),
    );
    let mut reported: HashSet<&str> = HashSet::new();
    for (endpoint, status) in statuses {
        let Some(status) = status else {
            continue;
        };
        let Some(server) = hub.config.lock().await.get(endpoint).cloned() else {
            continue;
        };
        let applied = apply_status_update(status, &hub.state, &server).await;
        let known = hub
            .state
            .lock()
            .await
            .get(endpoint)
            .is_some_and(|s| s.ranges_known && s.online);
        if matches!(applied, Ok(Some(_))) && known {
            reported.insert(endpoint.as_str());
        }
    }
    hub.refresh().await;
    let view = hub.view();
    print_coverage(&view, &reported);

    let mut failures = Vec::new();
    for log in [Log::Trace, Log::ChainState] {
        for gap in &view.cov(log).gaps {
            failures.push(format!(
                "hole {} in {} coverage",
                range_text(*gap),
                log.as_str()
            ));
        }
    }
    for u in view
        .upstreams
        .iter()
        .filter(|u| u.enabled && u.role == Role::Frozen && !reported.contains(&*u.endpoint))
    {
        failures.push(format!(
            "frozen upstream '{}' did not report its ranges",
            u.name
        ));
    }
    failures
}

fn print_coverage(view: &FleetView, reported: &HashSet<&str>) {
    cprintln!("\n 🗺️  <bright-magenta,bold>Coverage</> (stitched)");
    for log in Log::ALL {
        let cov = view.cov(log);
        let parts: Vec<String> = cov.components.iter().map(|c| range_text(*c)).collect();
        let parts = if parts.is_empty() {
            "-".to_string()
        } else {
            parts.join(" ")
        };
        let gaps: Vec<String> = cov.gaps.iter().map(|g| range_text(*g)).collect();
        if gaps.is_empty() {
            cprintln!("    {:<12} {}", log.as_str(), parts);
        } else {
            cprintln!(
                "    {:<12} {}  <red>holes: {}</>",
                log.as_str(),
                parts,
                gaps.join(" ")
            );
        }
    }
    for u in &view.upstreams {
        let source = if reported.contains(&*u.endpoint) {
            "reported"
        } else if u.known {
            "declared"
        } else {
            "unknown"
        };
        let ranges: Vec<String> = Log::ALL
            .iter()
            .map(|&log| format!("{} {}", log.as_str(), range_text(u.range(log))))
            .collect();
        cprintln!(
            "    <cyan>{:<16}</> {:<6} {:<8} {}",
            u.name,
            u.role.as_str(),
            source,
            ranges.join("  ")
        );
    }
}

/// Probe every server of every listener. Returns false when `strict` and a
/// stitched listener's coverage has a hole or an unreported frozen upstream.
async fn run_tests(config: &ServerConfig, strict: bool) -> bool {
    let mut total_valid = 0;
    let mut all_listeners_valid = true;
    let mut strict_failures: Vec<String> = Vec::new();
    let bound = Duration::from_millis(config.upstream_connect_timeout_ms());

    for listener in config.resolved_listeners() {
        if let Some(name) = &listener.name {
            cprintln!(
                "\n 🌐 Listener <bright-magenta,bold>{}</> (<cyan>{}</>)",
                name,
                format_bind_addr(&listener.listen_address, listener.listen_port)
            );
            if listener.expected_chain_id.is_none() {
                cprintln!(
                    " ⚠️  <yellow>No chain_id pinned for listener '{}'; first online upstream will establish the pool chain_id</>",
                    name
                );
            }
        }
        let mut listener_valid = 0;
        // Stitched: the status of every server that answered with an ABI.
        let mut statuses: Vec<(String, Option<Vec<u8>>)> = Vec::new();
        for server in &listener.servers {
            let endpoint = server.ws_url();
            cprintln!("\n 🧪 Testing <bright-cyan>{}</>", endpoint);
            match probe_upstream(&endpoint, config.max_message_bytes, bound).await {
                Probe::Abi => {
                    cprintln!(" ✅  <green>Websocket connected!</>");
                    cprintln!(" ✅  <green>ABI Received!</>");
                    listener_valid += 1;
                    total_valid += 1;
                    if listener.topology == ResolvedTopology::Stitched && server.enabled {
                        let status = probe_status(&endpoint, config.max_message_bytes, bound).await;
                        if status.is_some() {
                            cprintln!(" ✅  <green>Status Received!</>");
                        } else {
                            cprintln!(" ❌  <red>{}:</> No status result!", endpoint);
                        }
                        statuses.push((server.endpoint.clone(), status));
                    }
                }
                Probe::ConnectTimeout => {
                    cprintln!(
                        " ❌  <red>{}:</> Websocket connect timed out after {} ms",
                        endpoint,
                        bound.as_millis()
                    );
                }
                Probe::ConnectFailed(e) => {
                    cprintln!(" ❌  <red>Connection failed! Error: {}</>", e);
                }
                Probe::ReadTimeout => {
                    cprintln!(" ✅  <green>Websocket connected!</>");
                    cprintln!(
                        " ❌  <red>{}:</> Timed out waiting for the ABI after {} ms",
                        endpoint,
                        bound.as_millis()
                    );
                }
                Probe::NoMessage => {
                    cprintln!(" ✅  <green>Websocket connected!</>");
                    cprintln!(" ❌  <red>{}:</> No Message Received!", endpoint);
                }
                Probe::ReadFailed(e) => {
                    cprintln!(" ✅  <green>Websocket connected!</>");
                    cprintln!(
                        " ❌  <red>{}:</> Failed to read message! Error: {}",
                        endpoint,
                        e
                    );
                }
                Probe::InvalidAbi => {
                    cprintln!(" ✅  <green>Websocket connected!</>");
                    cprintln!(" ❌  <red>{}:</> Invalid ABI!", endpoint);
                }
                Probe::Unexpected(kind) => {
                    cprintln!(" ✅  <green>Websocket connected!</>");
                    cprintln!(
                        " ❌  <red>{}:</> Unexpected Message type: {}",
                        endpoint,
                        kind
                    );
                }
            }
        }
        if listener_valid == 0 {
            all_listeners_valid = false;
        }
        if listener.topology == ResolvedTopology::Stitched {
            let failures = report_coverage(&listener, &statuses).await;
            for failure in &failures {
                cprintln!("    <yellow>warning: {}</>", failure);
            }
            let prefix = listener
                .name
                .as_deref()
                .map(|name| format!("listener '{name}': "))
                .unwrap_or_default();
            strict_failures.extend(failures.into_iter().map(|f| format!("{prefix}{f}")));
        }
    }
    if total_valid > 0 && all_listeners_valid {
        cprintln!("\n ✅  Valid servers: <cyan,bold>{}</>", total_valid);
    } else {
        cprintln!("\n ❌  <red,bold>No valid servers, please check your configuration file!</>");
    }
    let passed = !strict || strict_failures.is_empty();
    if !passed {
        cprintln!(
            "\n ❌  <red,bold>--strict: {} coverage problem(s), see the warnings above</>",
            strict_failures.len()
        );
    }
    println!();
    passed
}

/// What the command line asks for.
enum Action {
    Run,
    /// `config test`; `strict` makes coverage problems fail the run.
    Test {
        strict: bool,
    },
}

fn process_args() -> Result<Option<(Action, ServerConfig)>> {
    let cmd = Command::new("fleet-router")
        .about("Protocol-aware reverse proxy for Antelope SHiP")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            arg!(--"config" <PATH>)
                .global(true)
                .default_value("config.json")
                .value_parser(value_parser!(PathBuf)),
        )
        .subcommand(
            Command::new("config")
                .subcommand_required(true)
                .about("Manage configuration")
                .subcommand(
                    Command::new("init")
                        .about("Create config file")
                        .arg_required_else_help(true)
                        .arg(
                            arg!(<CONFIG_OUT> "New config file")
                                .value_parser(value_parser!(PathBuf)),
                        ),
                )
                .subcommand(
                    Command::new("test")
                        .about("Test the configuration")
                        .arg_required_else_help(true)
                        .arg(
                            arg!(<CONFIG> "config file to test")
                                .value_parser(value_parser!(PathBuf)),
                        )
                        .arg(arg!(--strict "Exit 1 when the configuration is invalid, or a stitched listener's coverage has a hole or a frozen upstream that did not report its ranges")),
                ),
        )
        .subcommand(Command::new("run").about("Start the proxy server (default)"))
        .subcommand(
            Command::new("check")
                .about("Exit 0 if the proxy port accepts a TCP connection (container healthcheck)")
                .arg(
                    arg!(--port <PORT> "Port to probe on 127.0.0.1")
                        .value_parser(value_parser!(u16))
                        .default_value("17000"),
                ),
        );

    let matches = cmd.get_matches();

    if let Some(("check", check)) = matches.subcommand() {
        let port = check.get_one::<u16>("port").copied().unwrap_or(17000);
        probe_listen_port(port)?;
        return Ok(None);
    }

    if let Some(("config", config)) = matches.subcommand() {
        match config.subcommand() {
            Some(("init", init)) => {
                let path = init
                    .get_one::<PathBuf>("CONFIG_OUT")
                    .cloned()
                    .unwrap_or_else(|| PathBuf::from("./config.json"));
                if let Err(e) = write(&path, CONFIG_SAMPLE) {
                    bail!("failed to write config file {}: {}", path.display(), e);
                }
                let display_path = path.canonicalize().unwrap_or(path);
                cprintln!(
                    "Creating new config file: <bright-cyan>{:?}</>",
                    display_path
                );
                cprintln!("Configuration saved!\nPlease edit and test it with: <green>fleet-router config test {}</>", display_path.display());
                return Ok(None);
            }
            Some(("test", test)) => match test.get_one::<PathBuf>("CONFIG") {
                None => bail!("missing config file"),
                Some(test_path) => {
                    let strict = test.get_flag("strict");
                    return match test_config(test_path) {
                        Some(config) => Ok(Some((Action::Test { strict }, config))),
                        None if strict => bail!("configuration test failed"),
                        None => Ok(None),
                    };
                }
            },
            _ => {}
        };
    }

    let config_path = match matches.get_one::<PathBuf>("config") {
        Some(path) => path,
        None => bail!("missing config path"),
    };
    tracing::debug!(?config_path, "loading configuration");

    let config_contents = match read_to_string(config_path) {
        Ok(data) => data,
        Err(e) => bail!("failed to read configuration file: {}", e),
    };

    let config: ServerConfig = match from_str(&config_contents) {
        Ok(config) => config,
        Err(e) => bail!("failed to parse configuration file: {}", e),
    };

    Ok(Some((Action::Run, config)))
}

/// TCP liveness probe used by the container `HEALTHCHECK`.
///
/// Connects to `127.0.0.1:port` with a bounded timeout so the image does not
/// need a shell or `/dev/tcp`. The default port matches the image's published
/// proxy port.
fn probe_listen_port(port: u16) -> Result<()> {
    let addr = std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, port));
    std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(4))
        .map_err(|e| anyhow::anyhow!("listen port {port} is not accepting connections: {e}"))?;
    Ok(())
}

fn test_config(path: &PathBuf) -> Option<ServerConfig> {
    match read_to_string(path) {
        Ok(data) => match from_str::<ServerConfig>(&data) {
            Ok(config) => {
                if let Err(e) = config.validate() {
                    cprintln!("<red>configuration is invalid: {}</>", e);
                    return None;
                }
                cprintln!("<green>configuration is valid.</>");
                for warning in config.warnings() {
                    cprintln!("<yellow>warning: {}</>", warning);
                }
                Some(config)
            }
            Err(e) => {
                cprintln!("<red>failed to parse configuration file: {}</>", e);
                None
            }
        },
        Err(e) => {
            cprintln!("<red>failed to read configuration file: {}</>", e);
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::probe_listen_port;
    use std::net::TcpListener;

    #[test]
    fn probe_accepts_an_open_local_port() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        probe_listen_port(port).unwrap();
    }

    #[test]
    fn probe_rejects_a_closed_port() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        assert!(probe_listen_port(port).is_err());
    }
}
