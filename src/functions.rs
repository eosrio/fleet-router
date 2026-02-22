use std::collections::HashMap;

use crate::errors;
use crate::models::ServerState;

/// Select a backend server from the list of available servers based on the number of connections
pub fn select_backend_server(
    servers: &mut HashMap<String, ServerState>,
) -> Result<String, &'static str> {
    let mut selected_server = None;
    let mut min_connections = usize::MAX;
    for (server, state) in servers.iter() {
        if state.enabled && state.online && state.connections < min_connections {
            selected_server = Some(server.clone());
            min_connections = state.connections;
        }
    }
    if let Some(server) = selected_server {
        Ok(server)
    } else {
        Err(errors::NO_SERVERS_AVAILABLE)
    }
}

pub fn buffer_to_hex(buffer: Vec<u8>) -> String {
    let mut hex = String::new();
    for byte in buffer {
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}
