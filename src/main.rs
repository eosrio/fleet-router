use std::fs::read_to_string;
use std::sync::Arc;

use rs_abieos::Abieos;
use tokio::net::TcpListener;
use tokio::spawn;
use tokio::sync::Mutex;

use crate::connection_handler::handle_client;
use crate::models::{build_config_db, build_state_db, ServerConfig, ServerConfigDb, ServerStateDb, StaticConfig};
use crate::tasks::{monitoring_connection, state_monitoring_loop};

mod errors;
mod models;
mod tasks;
mod zcd;
mod functions;
mod connection_handler;

#[tokio::main]
async fn main() {
    let main_abieos = Abieos::new();

    let ctx = match main_abieos.context {
        Some(ctx) => ctx,
        None => {
            eprintln!("Error instancing Abieos! No context was created!");
            return;
        }
    };

    let shared_abieos = Arc::new(Mutex::new(Abieos::from_context(ctx)));
    let config_contents = read_to_string("config.json").expect(errors::CONFIG_READ);
    let config: ServerConfig = match serde_json::from_str(&config_contents) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error parsing configuration file: {}", e);
            return;
        }
    };

    if config.servers.iter().filter(|s| s.enabled).count() == 0 {
        eprintln!("No enabled servers found in config.json. Aborting.");
        return;
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
        eprintln!("Invalid address");
        return;
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
    spawn(state_monitoring_loop(server_state_db_clone, static_config.upstream_monitoring_ms));


    let listener = match TcpListener::bind(format!("{}:{}", static_config.listen_address, static_config.listen_port)).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Error binding to address: {}", e);
            return;
        }
    };

    println!("Listening on: {}:{}", static_config.listen_address, static_config.listen_port);

    loop {
        // Accept new TCP connections on the main thread
        let (client_stream, client_addr) = match listener.accept().await {
            Ok((stream, address)) => (stream, address),
            Err(e) => {
                eprintln!("Error accepting incoming connection: {}", e);
                // continue to the next iteration of the loop
                continue;
            }
        };

        println!("New incoming TCP connection: {}:{}", client_addr.ip(), client_addr.port());

        let s_state_db = server_state_db.clone();
        let s_config_db = server_config_db.clone();
        let abieos = shared_abieos.clone();

        // Spawn a new task to handle the new TCP connection
        spawn(async move {
            handle_client(
                client_stream,
                client_addr,
                s_state_db,
                s_config_db,
                abieos,
            ).await;
        });
    }
}