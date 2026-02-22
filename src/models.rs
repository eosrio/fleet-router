use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

pub type ServerStateDb = Arc<Mutex<HashMap<String, ServerState>>>;
pub type ServerConfigDb = Arc<Mutex<HashMap<String, Server>>>;

// config.json file format
#[derive(Debug, Deserialize, Serialize, Hash, Eq, PartialEq, Clone)]
pub struct Server {
    pub name: String,
    pub endpoint: String,
    pub enabled: bool,
}

impl Server {
    pub fn ws_url(&self) -> String {
        format!("ws://{}", self.endpoint)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    pub listen_port: u16,
    pub listen_address: String,
    pub upstream_reconnect_ms: u64,
    pub upstream_monitoring_ms: u64,
    pub upstream_status_ms: u64,
    pub servers: Vec<Server>,
}

#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct ServerState {
    // ... Add fields to track server usage (e.g., connections)
    pub connections: usize,
    pub enabled: bool,
    pub online: bool,
    pub trace_begin_block: u32,
    pub trace_end_block: u32,
    pub chain_state_begin_block: u32,
    pub chain_state_end_block: u32,
}

impl ServerState {
    pub fn new() -> ServerState {
        ServerState {
            connections: 0,
            enabled: true,
            online: false,
            trace_begin_block: 0,
            trace_end_block: 0,
            chain_state_begin_block: 0,
            chain_state_end_block: 0,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct GetStatusResultV0 {
    pub head_block_num: u32,
    pub head_block_id: String,
    pub last_irreversible_block_num: u32,
    pub last_irreversible_block_id: String,
    pub trace_begin_block: u32,
    pub trace_end_block: u32,
    pub chain_state_begin_block: u32,
    pub chain_state_end_block: u32,
    pub chain_id: String,
}


#[derive(Debug, Clone)]
pub struct StaticConfig {
    pub listen_port: u16,
    pub listen_address: String,
    pub upstream_reconnect_ms: u64,
    pub upstream_monitoring_ms: u64,
    pub upstream_status_ms: u64,
}

pub fn build_state_db(servers: Vec<Server>) -> Arc<Mutex<HashMap<String, ServerState>>> {
    Arc::new(Mutex::new(
        servers
            .clone()
            .iter()
            .filter(|s| s.enabled)
            .map(|s| (s.endpoint.clone(), ServerState::new()))
            .collect::<HashMap<String, ServerState>>(),
    ))
}

pub fn build_config_db(servers: Vec<Server>) -> Arc<Mutex<HashMap<String, Server>>> {
    Arc::new(Mutex::new(
        servers
            .into_iter()
            .filter(|s| s.enabled)
            .map(|s| (s.endpoint.clone(), s.clone()))
            .collect::<HashMap<String, Server>>(),
    ))
}
