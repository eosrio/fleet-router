//! # fleet-router
//!
//! A reverse proxy and load balancer for the Antelope **SHiP** (State History
//! Plugin) WebSocket protocol. Clients connect to the router; it selects an
//! upstream SHiP node (range-aware, least-connections) and proxies the
//! WebSocket bidirectionally, transparently failing over to another upstream
//! and de-duplicating replayed blocks on reconnect.
//!
//! Run with `fleet-router run --config config.json`. See the project README for
//! the full configuration reference and operational guidance.

use std::any::Any;
use std::fs::{read_to_string, write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use clap::{arg, value_parser, Command};
use color_print::cprintln;
use futures::StreamExt;
use rs_abieos::Abieos;
use serde_json::from_str;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::{broadcast, Mutex, Semaphore};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::Instrument;

use crate::config_sample::CONFIG_SAMPLE;
use crate::connection_handler::handle_client;
use crate::models::{
    build_config_db, build_state_db, Server, ServerConfig, ServerConfigDb, ServerStateDb,
    StaticConfig,
};
use crate::tasks::{monitoring_connection, state_monitoring_loop};

mod config_sample;
mod connection_handler;
mod errors;
mod functions;
mod health;
mod models;
mod tasks;
mod zcd;

#[tokio::main]
async fn main() -> Result<()> {
    let config: ServerConfig = match process_args() {
        Ok(Some((test, conf))) => {
            if test {
                run_tests(conf.servers.clone()).await;
                return Ok(());
            }
            conf
        }
        Ok(None) => return Ok(()),
        Err(e) => bail!(e),
    };

    init_tracing();

    // Validate the configuration up front with clear, actionable errors.
    config.validate()?;

    let main_abieos = Abieos::new();
    let ctx = main_abieos.as_ptr();
    if ctx.is_null() {
        bail!("Error instancing Abieos! No context was created!");
    }
    let shared_abieos = Arc::new(Mutex::new(Abieos::from_context(ctx)));

    let static_config = StaticConfig {
        upstream_status_ms: config.upstream_status_ms,
        upstream_monitoring_ms: config.upstream_monitoring_ms,
        upstream_reconnect_ms: config.upstream_reconnect_ms,
    };
    let limits = config.proxy_limits();

    let server_config_db: ServerConfigDb = build_config_db(config.servers.clone());
    let server_state_db: ServerStateDb = build_state_db(config.servers.clone());

    // Graceful-shutdown broadcast: every long-lived task subscribes.
    let (shutdown_tx, _) = broadcast::channel::<()>(16);

    // Per-upstream monitoring loops.
    for server in config.servers.iter().filter(|s| s.enabled).cloned() {
        tracing::info!(name = %server.name, upstream = %server.endpoint, "starting upstream monitor");
        spawn(monitoring_connection(
            server,
            static_config.clone(),
            server_state_db.clone(),
            shared_abieos.clone(),
            shutdown_tx.subscribe(),
        ));
    }

    // Periodic block-progress / staleness monitor.
    spawn(state_monitoring_loop(
        server_state_db.clone(),
        static_config.upstream_monitoring_ms,
        shutdown_tx.subscribe(),
    ));

    // Optional health/metrics HTTP endpoint.
    if let Some(port) = config.metrics_port {
        let addr = format!(
            "{}:{}",
            config
                .metrics_address
                .clone()
                .unwrap_or_else(|| config.listen_address.clone()),
            port
        );
        spawn(health::run_metrics_server(
            addr,
            server_state_db.clone(),
            shutdown_tx.subscribe(),
        ));
    }

    let bind_addr = format!("{}:{}", config.listen_address, config.listen_port);
    let listener = TcpListener::bind(&bind_addr)
        .await
        .map_err(|e| anyhow::anyhow!("error binding to {}: {}", bind_addr, e))?;
    tracing::info!(address = %config.listen_address, port = config.listen_port, "listening for clients");

    // Connection backpressure: at most `max_connections` concurrent clients.
    let conn_limit = Arc::new(Semaphore::new(config.max_connections));
    let max_connections = config.max_connections;

    loop {
        tokio::select! {
            result = listener.accept() => {
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
                        tracing::warn!(%client_addr, max = max_connections, "connection limit reached; rejecting");
                        drop(client_stream);
                        continue;
                    }
                };

                tracing::debug!(%client_addr, "accepted tcp connection");

                let s_state_db = server_state_db.clone();
                let s_config_db = server_config_db.clone();
                let abieos = shared_abieos.clone();
                let shutdown_rx = shutdown_tx.subscribe();

                spawn(
                    async move {
                        let _permit = permit; // released when this task ends
                        handle_client(
                            client_stream,
                            client_addr,
                            s_state_db,
                            s_config_db,
                            abieos,
                            limits,
                            shutdown_rx,
                        )
                        .await;
                    }
                    .instrument(tracing::info_span!("client", addr = %client_addr)),
                );
            }
            _ = shutdown_signal() => {
                tracing::info!("shutdown signal received; draining connections");
                let _ = shutdown_tx.send(());
                drain_connections(&conn_limit, max_connections, config.shutdown_grace_ms).await;
                tracing::info!("shutdown complete");
                break Ok(());
            }
        }
    }
}

/// Initialize `tracing` with a `RUST_LOG`-driven filter (default `info`).
fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_target(false).init();
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

/// Wait (up to `grace_ms`) for active client connections to drain.
async fn drain_connections(conn_limit: &Arc<Semaphore>, max: usize, grace_ms: u64) {
    let start = Instant::now();
    while conn_limit.available_permits() < max {
        if start.elapsed() >= Duration::from_millis(grace_ms) {
            let remaining = max - conn_limit.available_permits();
            tracing::warn!(remaining, "shutdown grace period elapsed; forcing exit");
            break;
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn run_tests(servers: Vec<Server>) {
    let mut valid_count = 0;
    for server in servers {
        let endpoint = format!("ws://{}", server.endpoint);
        cprintln!("\n 🧪 Testing <bright-cyan>{}</>", endpoint);
        match connect_async(endpoint.clone()).await {
            Ok((ws, _)) => {
                cprintln!(" ✅  <green>Websocket connected!</>");
                let (_, mut reader) = ws.split();
                let message = reader.next().await;
                match message {
                    None => {
                        cprintln!(" ❌  <red>{}:</> No Message Received!", endpoint);
                    }
                    Some(Ok(Message::Text(txt))) => {
                        if txt.contains("eosio::abi") {
                            cprintln!(" ✅  <green>ABI Received!</>");
                            valid_count += 1;
                        } else {
                            cprintln!(" ❌  <red>{}:</> Invalid ABI!", endpoint);
                        }
                    }
                    Some(Err(e)) => {
                        cprintln!(
                            " ❌  <red>{}:</> Failed to read message! Error: {}",
                            endpoint,
                            e
                        );
                    }
                    Some(Ok(m)) => {
                        cprintln!(
                            " ❌  <red>{}:</> Unexpected Message type: {:?}",
                            endpoint,
                            m.type_id()
                        );
                    }
                }
            }
            Err(e) => {
                cprintln!(" ❌  <red>Connection failed! Error: {}</>", e);
            }
        }
    }
    if valid_count > 0 {
        cprintln!("\n ✅  Valid servers: <cyan,bold>{}</>", valid_count);
    } else {
        cprintln!("\n ❌  <red,bold>No valid servers, please check your configuration file!</>");
    }
    println!();
}

fn process_args() -> Result<Option<(bool, ServerConfig)>> {
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
                        ),
                ),
        )
        .subcommand(Command::new("run").about("Start the proxy server (default)"));

    let matches = cmd.get_matches();

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
                Some(test_path) => return Ok(test_config(test_path)),
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

    Ok(Some((false, config)))
}

fn test_config(path: &PathBuf) -> Option<(bool, ServerConfig)> {
    match read_to_string(path) {
        Ok(data) => match from_str::<ServerConfig>(&data) {
            Ok(config) => {
                if let Err(e) = config.validate() {
                    cprintln!("<red>configuration is invalid: {}</>", e);
                    return None;
                }
                cprintln!("<green>configuration is valid.</>");
                Some((true, config))
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
