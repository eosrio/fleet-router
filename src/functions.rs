use std::collections::HashMap;

use crate::errors;
use crate::models::ServerState;

/// Select a backend server from the list of available servers.
///
/// When `requested_block` is `Some`, prefer servers whose trace range covers that block
/// (least connections among those). Falls back to any online server if none covers the range.
pub fn select_backend_server(
    servers: &mut HashMap<String, ServerState>,
    requested_block: Option<u32>,
) -> Result<String, &'static str> {
    let mut best_in_range: Option<(String, usize)> = None;
    let mut best_fallback: Option<(String, usize)> = None;

    for (server, state) in servers.iter() {
        if !state.enabled || !state.online {
            continue;
        }

        // Check if this server's trace range covers the requested block
        let covers_range = match requested_block {
            Some(block) => {
                state.trace_end_block > 0
                    && state.trace_begin_block <= block
                    && block < state.trace_end_block
            }
            None => false,
        };

        if covers_range
            && (best_in_range.is_none()
                || state.connections < best_in_range.as_ref().unwrap().1)
        {
            best_in_range = Some((server.clone(), state.connections));
        }

        // Always track the overall least-connections fallback
        if best_fallback.is_none() || state.connections < best_fallback.as_ref().unwrap().1 {
            best_fallback = Some((server.clone(), state.connections));
        }
    }

    // Prefer in-range, fall back to any online server
    if let Some((server, _)) = best_in_range {
        Ok(server)
    } else if let Some((server, _)) = best_fallback {
        if let Some(block) = requested_block {
            eprintln!(
                "[select_backend] Warning: no upstream covers block {}, falling back to least connections",
                block
            );
        }
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
