use std::any::Any;
use std::fs::{read_to_string, write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Result};
use clap::{arg, value_parser, Command};
use color_print::cprintln;
use futures::StreamExt;
use rs_abieos::Abieos;
use serde_json::from_str;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

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
        Ok(None) => {
            return Ok(());
        }
        Err(e) => {
            bail!(e)
        }
    };

    let main_abieos = Abieos::new();
    let ctx = main_abieos.as_ptr();
    if ctx.is_null() {
        bail!("Error instancing Abieos! No context was created!");
    }
    let shared_abieos = Arc::new(Mutex::new(Abieos::from_context(ctx)));

    if config.servers.iter().filter(|s| s.enabled).count() == 0 {
        bail!("No enabled servers found in config.json. Aborting.");
    }

    // global settings
    let static_config = StaticConfig {
        listen_port: config.listen_port,
        upstream_status_ms: config.upstream_status_ms,
        upstream_monitoring_ms: config.upstream_monitoring_ms,
        listen_address: config.listen_address,
        upstream_reconnect_ms: config.upstream_reconnect_ms,
    };

    if static_config.listen_address.is_empty() {
        bail!("Invalid address");
    }

    // Create a shared state for the server configuration HashMap
    let server_config_db: ServerConfigDb = build_config_db(config.servers.clone());

    // Create a shared state for the server state HashMap
    let server_state_db: ServerStateDb = build_state_db(config.servers.clone());

    // Backend Monitoring Loop
    for server in config.servers.iter().filter(|s| s.enabled).cloned() {
        println!("{}", server.name);
        // Get references
        let server_state_db_clone = server_state_db.clone();
        let abieos_arc_clone = shared_abieos.clone();
        println!("Monitoring started for server: {}", server.endpoint);
        spawn(monitoring_connection(
            server,
            static_config.clone(),
            server_state_db_clone,
            abieos_arc_clone,
        ));
    }

    // spawn a new async task to print the server state every 5 seconds
    let server_state_db_clone = server_state_db.clone();
    spawn(state_monitoring_loop(
        server_state_db_clone,
        static_config.upstream_monitoring_ms,
    ));

    let listener = match TcpListener::bind(format!(
        "{}:{}",
        static_config.listen_address, static_config.listen_port
    ))
    .await
    {
        Ok(listener) => listener,
        Err(e) => {
            bail!("Error binding to address: {}", e);
        }
    };

    println!(
        "Listening on: {}:{}",
        static_config.listen_address, static_config.listen_port
    );

    // Graceful shutdown channel
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    loop {
        tokio::select! {
            // Accept new TCP connections on the main thread
            result = listener.accept() => {
                let (client_stream, client_addr) = match result {
                    Ok((stream, address)) => (stream, address),
                    Err(e) => {
                        eprintln!("Error accepting incoming connection: {}", e);
                        // continue to the next iteration of the loop
                        continue;
                    }
                };

                println!(
                    "New incoming TCP connection: {}:{}",
                    client_addr.ip(),
                    client_addr.port()
                );

                let s_state_db = server_state_db.clone();
                let s_config_db = server_config_db.clone();
                let abieos = shared_abieos.clone();
                let mut shutdown_rx = shutdown_tx.subscribe();

                // Spawn a new task to handle the new TCP connection
                spawn(async move {
                    tokio::select! {
                        _ = handle_client(client_stream, client_addr, s_state_db, s_config_db, abieos) => {}
                        _ = shutdown_rx.recv() => {
                            println!("[{}] Connection closed due to server shutdown", client_addr);
                        }
                    }
                });
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\n[main] Received Ctrl+C / SIGINT. Shutting down gracefully...");
                let _ = shutdown_tx.send(());

                // Allow a brief moment for inflight connections to drop naturally
                tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                break Ok(());
            }
        }
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
        .version("0.2.0")
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
                let path = {
                    if let Some(config_out_path) = init.get_one::<PathBuf>("CONFIG_OUT") {
                        config_out_path.to_owned()
                    } else {
                        PathBuf::from("./config.json").canonicalize().unwrap()
                    }
                };
                cprintln!(
                    "Creating new config file: <bright-cyan>{:?}</>",
                    path.canonicalize().unwrap()
                );
                write(&path, CONFIG_SAMPLE).unwrap();
                cprintln!("Configuration saved!\nPlease edit and test it with: <green>fleet-router config test {}</>", path.canonicalize().unwrap().display());
                return Ok(None);
            }
            Some(("test", test)) => match test.get_one::<PathBuf>("CONFIG") {
                None => {
                    bail!("missing config file");
                }
                Some(test_path) => {
                    return Ok(test_config(test_path));
                }
            },
            _ => {}
        };
    } else if let Some(("run", _run_matches)) = matches.subcommand() {
        // Explicit run subcommand, which is exactly the same as no subcommand (default)
        // No-op here since the config arg is registered globally
    }

    let config_path = match matches.get_one::<PathBuf>("config") {
        Some(path) => path,
        None => {
            bail!("missing config path");
        }
    };
    println!("Using config file at: {:?}", config_path);

    let config_contents = match read_to_string(config_path) {
        Ok(data) => data,
        Err(e) => {
            bail!("failed to read configuration file: {}", e);
        }
    };

    let config: ServerConfig = match from_str(&config_contents) {
        Ok(config) => config,
        Err(e) => {
            bail!("failed to parse configuration file: {}", e);
        }
    };

    Ok(Some((false, config)))
}

fn test_config(path: &PathBuf) -> Option<(bool, ServerConfig)> {
    match read_to_string(path) {
        Ok(data) => {
            match from_str::<ServerConfig>(&data) {
                Ok(config) => {
                    // println!("{:#?}", config);
                    Some((true, config))
                }
                Err(e) => {
                    cprintln!("<red>failed to parse configuration file: {}</>", e);
                    None
                }
            }
        }
        Err(e) => {
            cprintln!("<red>failed to read configuration file: {}</>", e);
            None
        }
    }
}
