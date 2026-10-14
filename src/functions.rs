use std::collections::HashMap;
use std::sync::atomic::Ordering;

use crate::errors;
use crate::models::ServerState;

/// What the in-flight client request needs from an upstream.
///
/// `block` is `None` before the client has sent a `get_blocks` request (initial
/// connect). `fetch_finality_data` is set only for `get_blocks_request_v1`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RouteRequest {
    pub block: Option<u32>,
    pub fetch_finality_data: bool,
}

/// Start block and finality flag parsed from a client `get_blocks` request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedBlocksRoute {
    pub start_block_num: u32,
    pub fetch_finality_data: bool,
}

/// SHiP `*_end_block` fields are one past the last available block.
fn range_covers(begin: u32, end: u32, block: u32) -> bool {
    end > 0 && begin <= block && block < end
}

fn trace_covers(state: &ServerState, block: u32) -> bool {
    range_covers(state.trace_begin_block, state.trace_end_block, block)
}

fn finality_covers(state: &ServerState, block: u32) -> bool {
    range_covers(
        state.finality_data_begin_block,
        state.finality_data_end_block,
        block,
    )
}

/// Decode a varuint32. Returns `None` if the buffer ends mid-value or overflows.
fn decode_varuint32(data: &[u8]) -> Option<(u32, usize)> {
    let mut val: u32 = 0;
    for (i, &byte) in data.iter().take(5).enumerate() {
        val |= u32::from(byte & 0x7f).checked_shl((i as u32).saturating_mul(7))?;
        if byte & 0x80 == 0 {
            return Some((val, i + 1));
        }
    }
    None
}

/// Parse `get_blocks_request_v0` (variant 1) or `v1` (variant 3).
///
/// `fetch_finality_data` is true only for v1 when that bool is set. Other
/// frames, and frames too short to hold a start block, return `None`.
pub fn parse_blocks_route(bin: &[u8]) -> Option<ParsedBlocksRoute> {
    let (variant, consumed) = decode_varuint32(bin)?;
    if variant != 1 && variant != 3 {
        return None;
    }
    let payload = bin.get(consumed..)?;
    if payload.len() < 4 {
        return None;
    }
    let start_block_num = u32::from_le_bytes(payload[0..4].try_into().ok()?);
    let fetch_finality_data = variant == 3 && read_fetch_finality_data(payload);
    Some(ParsedBlocksRoute {
        start_block_num,
        fetch_finality_data,
    })
}

/// `get_blocks_request_v1` bool layout after `have_positions`:
/// irreversible_only, fetch_block, fetch_traces, fetch_deltas, fetch_finality_data.
fn read_fetch_finality_data(payload: &[u8]) -> bool {
    if payload.len() < 12 {
        return false;
    }
    let Some((count, count_len)) = decode_varuint32(&payload[12..]) else {
        return false;
    };
    let positions = (count as usize).saturating_mul(4 + 32);
    let Some(bools_at) = 12usize
        .checked_add(count_len)
        .and_then(|n| n.checked_add(positions))
    else {
        return false;
    };
    payload.get(bools_at + 4).copied().unwrap_or(0) != 0
}

/// Whether a finality-data request should leave `endpoint`.
///
/// True only when this upstream's finality range does not cover `block` and at
/// least one other enabled, online upstream does. When nobody covers, callers
/// keep v0 (trace-range) routing instead of bouncing the client.
pub fn should_reroute_for_finality(
    servers: &HashMap<String, ServerState>,
    endpoint: &str,
    block: u32,
) -> bool {
    if servers
        .get(endpoint)
        .is_some_and(|state| finality_covers(state, block))
    {
        return false;
    }
    servers.iter().any(|(name, state)| {
        name != endpoint && state.enabled && state.online && finality_covers(state, block)
    })
}

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
///
/// When `route.fetch_finality_data` is set and at least one upstream's
/// `finality_data` range covers the block, only those upstreams are eligible.
/// Otherwise selection is unchanged (v0 trace-range behaviour), including when
/// no upstream has reported a covering finality range.
pub fn select_backend_server(
    servers: &HashMap<String, ServerState>,
    route: RouteRequest,
) -> Result<String, &'static str> {
    select_ranked(servers, route, true)
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
    route: RouteRequest,
) -> Result<String, &'static str> {
    if let Ok(server) = select_backend_server(servers, route) {
        return Ok(server);
    }
    select_ranked(servers, route, false)
}

fn select_ranked(
    servers: &HashMap<String, ServerState>,
    route: RouteRequest,
    require_online: bool,
) -> Result<String, &'static str> {
    let restrict_finality = route.fetch_finality_data
        && route.block.is_some_and(|block| {
            servers
                .values()
                .any(|state| state.enabled && state.online && finality_covers(state, block))
        });

    // Lower (rank, connections) is better.
    let mut best: Option<(u8, usize, String)> = None;

    for (server, state) in servers.iter() {
        if !state.enabled || (require_online && !state.online) {
            continue;
        }
        if let Some(block) = route.block {
            if restrict_finality && !finality_covers(state, block) {
                continue;
            }
        }

        let conns = state.connections.load(Ordering::Relaxed);

        let covers_range = route.block.is_some_and(|block| trace_covers(state, block));

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
            if let Some(block) = route.block {
                if rank >= 2 && !restrict_finality {
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
        assert!(select_backend_server(&servers, RouteRequest::default()).is_err());
        assert_eq!(
            select_failover_server(&servers, RouteRequest::default()).unwrap(),
            "dead"
        );
    }

    #[test]
    fn failover_prefers_an_online_upstream() {
        let mut servers = HashMap::new();
        servers.insert("dead".into(), state(true, false, 0));
        servers.insert("live".into(), state(true, true, 5));
        assert_eq!(
            select_backend_server(&servers, RouteRequest::default()).unwrap(),
            "live"
        );
        assert_eq!(
            select_failover_server(&servers, RouteRequest::default()).unwrap(),
            "live"
        );
    }

    #[test]
    fn failover_skips_disabled_upstreams() {
        let mut servers = HashMap::new();
        servers.insert("off".into(), state(false, false, 0));
        servers.insert("also".into(), state(false, true, 0));
        assert!(select_failover_server(&servers, RouteRequest::default()).is_err());
        assert!(select_backend_server(&servers, RouteRequest::default()).is_err());
    }

    fn server(conns: usize, trace: (u32, u32), finality: (u32, u32)) -> ServerState {
        let mut state = ServerState::new();
        state.online = true;
        state.connections = Arc::new(AtomicUsize::new(conns));
        state.trace_begin_block = trace.0;
        state.trace_end_block = trace.1;
        state.finality_data_begin_block = finality.0;
        state.finality_data_end_block = finality.1;
        state
    }

    fn fleet() -> HashMap<String, ServerState> {
        let mut servers = HashMap::new();
        // Fewer connections, traces cover block 100, finality does not.
        servers.insert("b".to_string(), server(0, (1, 20_001), (10_001, 20_001)));
        // More connections, traces and finality both cover block 100.
        servers.insert("a".to_string(), server(5, (1, 20_001), (1, 10_001)));
        servers
    }

    fn blocks_request(variant: u8, start: u32, fetch_finality: bool, positions: u32) -> Vec<u8> {
        let mut buf = vec![variant];
        buf.extend_from_slice(&start.to_le_bytes());
        buf.extend_from_slice(&200u32.to_le_bytes());
        buf.extend_from_slice(&10u32.to_le_bytes());
        buf.push(positions as u8); // have_positions count, single-byte varuint
        for i in 0..positions {
            buf.extend_from_slice(&i.to_le_bytes());
            buf.extend_from_slice(&[0u8; 32]);
        }
        buf.push(0); // irreversible_only
        buf.push(0); // fetch_block
        buf.push(0); // fetch_traces
        buf.push(0); // fetch_deltas
        if variant == 3 {
            buf.push(u8::from(fetch_finality));
        }
        buf
    }

    #[test]
    fn parse_v0_request_has_no_finality_flag() {
        let parsed = parse_blocks_route(&blocks_request(1, 100, true, 0)).unwrap();
        assert_eq!(parsed.start_block_num, 100);
        assert!(!parsed.fetch_finality_data);
    }

    #[test]
    fn parse_v1_request_reads_finality_flag_after_positions() {
        let on = parse_blocks_route(&blocks_request(3, 100, true, 1)).unwrap();
        assert_eq!(on.start_block_num, 100);
        assert!(on.fetch_finality_data);
        let off = parse_blocks_route(&blocks_request(3, 40, false, 0)).unwrap();
        assert_eq!(off.start_block_num, 40);
        assert!(!off.fetch_finality_data);
    }

    #[test]
    fn parse_ignores_non_blocks_frames() {
        assert!(parse_blocks_route(&[0]).is_none());
        assert!(parse_blocks_route(&[2, 1, 0, 0, 0]).is_none());
        assert!(parse_blocks_route(&[3, 1, 0]).is_none());
    }

    #[test]
    fn finality_request_skips_upstream_that_does_not_cover() {
        let picked = select_backend_server(
            &fleet(),
            RouteRequest {
                block: Some(100),
                fetch_finality_data: true,
            },
        )
        .unwrap();
        assert_eq!(picked, "a");
    }

    #[test]
    fn non_finality_request_keeps_least_connections() {
        let picked = select_backend_server(
            &fleet(),
            RouteRequest {
                block: Some(100),
                fetch_finality_data: false,
            },
        )
        .unwrap();
        assert_eq!(picked, "b");
    }

    #[test]
    fn finality_request_falls_back_to_v0_when_no_range_covers() {
        let mut servers = fleet();
        servers.get_mut("a").unwrap().finality_data_end_block = 50; // does not cover 100
        let picked = select_backend_server(
            &servers,
            RouteRequest {
                block: Some(100),
                fetch_finality_data: true,
            },
        )
        .unwrap();
        assert_eq!(picked, "b");
    }

    #[test]
    fn finality_request_falls_back_when_ranges_were_never_reported() {
        let mut servers = HashMap::new();
        servers.insert("busy".to_string(), server(4, (1, 500), (0, 0)));
        servers.insert("idle".to_string(), server(0, (1, 500), (0, 0)));
        let picked = select_backend_server(
            &servers,
            RouteRequest {
                block: Some(10),
                fetch_finality_data: true,
            },
        )
        .unwrap();
        assert_eq!(picked, "idle");
    }

    #[test]
    fn reroute_only_when_another_upstream_covers_finality() {
        let servers = fleet();
        assert!(should_reroute_for_finality(&servers, "b", 100));
        assert!(!should_reroute_for_finality(&servers, "a", 100));
        assert!(!should_reroute_for_finality(&servers, "b", 15_000));
        let mut nobody = fleet();
        nobody.get_mut("a").unwrap().finality_data_end_block = 50;
        assert!(!should_reroute_for_finality(&nobody, "b", 100));
    }

    #[test]
    fn offline_finality_upstream_does_not_force_a_reroute() {
        let mut servers = fleet();
        servers.get_mut("a").unwrap().online = false;
        assert!(!should_reroute_for_finality(&servers, "b", 100));
        let err = select_backend_server(
            &servers,
            RouteRequest {
                block: Some(100),
                fetch_finality_data: true,
            },
        )
        .unwrap();
        assert_eq!(err, "b");
    }
}
