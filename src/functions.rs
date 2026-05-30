use std::collections::HashMap;
use std::sync::atomic::Ordering;

use crate::errors;
use crate::models::ServerState;

/// Select a backend server from the list of available (enabled + online) servers.
///
/// Candidates are ranked, best first:
///
/// 1. covers the requested block AND is fresh (advancing)
/// 2. covers the requested block but is stale
/// 3. does not cover the block but is fresh
/// 4. does not cover the block and is stale
///
/// Within the same rank, the server with the fewest active connections wins
/// (least-connections load balancing). Stale servers are deprioritized but never
/// excluded, so selection never starves when every upstream is stale.
///
/// The range check uses an exclusive upper bound (`block < trace_end_block`) because SHiP's
/// `get_status_result` reports `trace_end_block` as one-past-the-last available block.
pub fn select_backend_server(
    servers: &HashMap<String, ServerState>,
    requested_block: Option<u32>,
) -> Result<String, &'static str> {
    // Lower (rank, connections) is better.
    let mut best: Option<(u8, usize, String)> = None;

    for (server, state) in servers.iter() {
        if !state.enabled || !state.online {
            continue;
        }

        let conns = state.connections.load(Ordering::Relaxed);

        let covers_range = match requested_block {
            Some(block) => {
                state.trace_end_block > 0
                    && state.trace_begin_block <= block
                    && block < state.trace_end_block
            }
            None => false,
        };

        let rank = match (covers_range, state.stale) {
            (true, false) => 0u8,
            (true, true) => 1,
            (false, false) => 2,
            (false, true) => 3,
        };

        let better = match &best {
            None => true,
            Some((best_rank, best_conns, _)) => (rank, conns) < (*best_rank, *best_conns),
        };
        if better {
            best = Some((rank, conns, server.clone()));
        }
    }

    match best {
        Some((rank, _, server)) => {
            if let Some(block) = requested_block {
                if rank >= 2 {
                    tracing::warn!(
                        block,
                        "no upstream covers the requested block; falling back to least-connections"
                    );
                }
            }
            Ok(server)
        }
        None => Err(errors::NO_SERVERS_AVAILABLE),
    }
}

pub fn buffer_to_hex(buffer: Vec<u8>) -> String {
    let mut hex = String::with_capacity(buffer.len() * 2);
    for byte in buffer {
        hex.push_str(&format!("{:02x}", byte));
    }
    hex
}
