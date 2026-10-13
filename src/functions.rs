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
    select_ranked(servers, requested_block, true)
}

/// Select an upstream for a session whose socket just died.
///
/// Prefers the same ranking as [`select_backend_server`]. When every candidate
/// is offline, still returns an enabled server so failover can attempt a
/// reconnect. The monitor marks an upstream offline as soon as its own read
/// fails, and that is often the same drop the proxy just saw. Refusing the
/// only server would close the client before the handshake and the
/// pending-byte cap can run. New sessions keep the online requirement.
pub fn select_failover_server(
    servers: &HashMap<String, ServerState>,
    requested_block: Option<u32>,
) -> Result<String, &'static str> {
    if let Ok(server) = select_backend_server(servers, requested_block) {
        return Ok(server);
    }
    select_ranked(servers, requested_block, false)
}

fn select_ranked(
    servers: &HashMap<String, ServerState>,
    requested_block: Option<u32>,
    require_online: bool,
) -> Result<String, &'static str> {
    // Lower (rank, connections) is better.
    let mut best: Option<(u8, usize, String)> = None;

    for (server, state) in servers.iter() {
        if !state.enabled || (require_online && !state.online) {
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use super::*;

    fn state(enabled: bool, online: bool, conns: usize) -> ServerState {
        let mut server = ServerState::new();
        server.enabled = enabled;
        server.online = online;
        server.connections = Arc::new(AtomicUsize::new(conns));
        server
    }

    #[test]
    fn new_sessions_skip_offline_upstreams() {
        let mut servers = HashMap::new();
        servers.insert("dead".into(), state(true, false, 0));
        assert!(select_backend_server(&servers, None).is_err());
        assert_eq!(select_failover_server(&servers, None).unwrap(), "dead");
    }

    #[test]
    fn failover_prefers_an_online_upstream() {
        let mut servers = HashMap::new();
        servers.insert("dead".into(), state(true, false, 0));
        servers.insert("live".into(), state(true, true, 5));
        assert_eq!(select_backend_server(&servers, None).unwrap(), "live");
        assert_eq!(select_failover_server(&servers, None).unwrap(), "live");
    }

    #[test]
    fn failover_skips_disabled_upstreams() {
        let mut servers = HashMap::new();
        servers.insert("off".into(), state(false, false, 0));
        servers.insert("also".into(), state(false, true, 0));
        assert!(select_failover_server(&servers, None).is_err());
        assert!(select_backend_server(&servers, None).is_err());
    }
}
