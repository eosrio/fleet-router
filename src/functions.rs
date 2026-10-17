use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

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
        name != endpoint && state.routable() && state.online && finality_covers(state, block)
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
/// Within the same rank, the lowest configured `priority` tier wins. Inside a
/// tier, the server with the fewest active connections per unit of `weight`
/// wins (weighted least-connections); on a tie the higher weight wins. Stale
/// servers are deprioritized but never excluded, so selection never starves
/// when every upstream is stale.
///
/// The range check uses an exclusive upper bound (`block < trace_end_block`) because SHiP's
/// `get_status_result` reports `trace_end_block` as one-past-the-last available block.
///
/// When `route.fetch_finality_data` is set and at least one upstream's
/// `finality_data` range covers the block, only those upstreams are eligible.
/// Otherwise selection is unchanged (v0 trace-range behaviour), including when
/// no upstream has reported a covering finality range.
pub fn select_backend_server(
    servers: &mut HashMap<String, ServerState>,
    route: RouteRequest,
) -> Result<String, &'static str> {
    select_ranked(servers, route, true, None)
}

/// Select an upstream for a session whose socket just died.
///
/// Prefers the same ranking as [`select_backend_server`]. When every candidate
/// is offline, still returns an enabled server so failover can attempt a
/// reconnect. The monitor marks an upstream offline as soon as its own read
/// fails, and that is often the same drop the proxy just saw. Refusing the
/// only server would close the client before the handshake and the
/// pending-byte cap can run. New sessions keep the online requirement.
///
/// `exclude` is the upstream that just failed at this block. Another upstream
/// at the same or better rank is preferred, even from a lower-priority tier.
/// The excluded upstream is still returned when it is the only one that covers
/// the block, or the only candidate at all.
pub fn select_failover_server(
    servers: &mut HashMap<String, ServerState>,
    route: RouteRequest,
    exclude: Option<&str>,
) -> Result<String, &'static str> {
    if let Ok(server) = select_ranked(servers, route, true, exclude) {
        return Ok(server);
    }
    select_ranked(servers, route, false, exclude)
}

/// Coverage and freshness of an upstream for `route`. Lower is better.
///
/// 0: covers the block and fresh, 1: covers but stale, 2: does not cover but
/// fresh, 3: does not cover and stale.
fn route_rank(state: &ServerState, route: RouteRequest) -> u8 {
    let covers_range = route.block.is_some_and(|block| trace_covers(state, block));
    match (covers_range, state.stale) {
        (true, false) => 0,
        (true, true) => 1,
        (false, false) => 2,
        (false, true) => 3,
    }
}

/// Selection key for one eligible upstream.
#[derive(Clone, Copy)]
struct Candidate {
    rank: u8,
    priority: u32,
    connections: usize,
    weight: u32,
}

impl Candidate {
    /// `Less` means `self` is the better choice.
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Weighted least-connections: compare connections / weight without
        // dividing, as connections * other.weight vs other.connections * weight.
        let load = |conns: usize, weight: u32| (conns as u128) * u128::from(weight);
        self.rank
            .cmp(&other.rank)
            .then(self.priority.cmp(&other.priority))
            .then(load(self.connections, other.weight).cmp(&load(other.connections, self.weight)))
            .then(other.weight.cmp(&self.weight))
    }
}

fn select_ranked(
    servers: &mut HashMap<String, ServerState>,
    route: RouteRequest,
    require_online: bool,
    exclude: Option<&str>,
) -> Result<String, &'static str> {
    let now = Instant::now();
    let restrict_finality = route.fetch_finality_data
        && route.block.is_some_and(|block| {
            servers
                .values()
                .any(|state| state.routable() && state.online && finality_covers(state, block))
        });

    let mut best: Option<(Candidate, String)> = None;
    let mut best_other: Option<(Candidate, String)> = None;

    for (server, state) in servers.iter_mut() {
        state.prune_expired_quarantines(now);
        if !state.routable() || (require_online && !state.online) {
            continue;
        }
        if let Some(block) = route.block {
            if state.quarantined_for(block, now) {
                continue;
            }
            if restrict_finality && !finality_covers(state, block) {
                continue;
            }
        }

        let candidate = Candidate {
            rank: route_rank(state, route),
            priority: state.priority,
            connections: state.connections.load(Ordering::Relaxed),
            weight: state.weight.max(1),
        };

        if exclude != Some(server.as_str())
            && best_other
                .as_ref()
                .is_none_or(|(other, _)| candidate.cmp(other).is_lt())
        {
            best_other = Some((candidate, server.clone()));
        }
        if best
            .as_ref()
            .is_none_or(|(best, _)| candidate.cmp(best).is_lt())
        {
            best = Some((candidate, server.clone()));
        }
    }

    // Prefer a different upstream only when it covers the block as well as
    // the best candidate. Priority does not keep a session on the upstream
    // that just failed, but a peer that does not cover the block must not
    // replace the only upstream that does.
    let (chosen, rank) = match (exclude, best, best_other) {
        (Some(_), Some((best, _)), Some((other, server))) if other.rank <= best.rank => {
            (server, other.rank)
        }
        (_, Some((best, server)), _) => (server, best.rank),
        _ => return Err(errors::NO_SERVERS_AVAILABLE),
    };
    if let Some(block) = route.block {
        if rank >= 2 && !restrict_finality {
            tracing::warn!(
                block,
                "no upstream covers the requested block; falling back to least-connections"
            );
        }
    }
    Ok(chosen)
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
        assert!(select_backend_server(&mut servers, RouteRequest::default()).is_err());
        assert_eq!(
            select_failover_server(&mut servers, RouteRequest::default(), None).unwrap(),
            "dead"
        );
    }

    #[test]
    fn failover_prefers_an_online_upstream() {
        let mut servers = HashMap::new();
        servers.insert("dead".into(), state(true, false, 0));
        servers.insert("live".into(), state(true, true, 5));
        assert_eq!(
            select_backend_server(&mut servers, RouteRequest::default()).unwrap(),
            "live"
        );
        assert_eq!(
            select_failover_server(&mut servers, RouteRequest::default(), None).unwrap(),
            "live"
        );
    }

    #[test]
    fn failover_skips_disabled_upstreams() {
        let mut servers = HashMap::new();
        servers.insert("off".into(), state(false, false, 0));
        servers.insert("also".into(), state(false, true, 0));
        assert!(select_failover_server(&mut servers, RouteRequest::default(), None).is_err());
        assert!(select_backend_server(&mut servers, RouteRequest::default()).is_err());
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
        let mut servers = fleet();
        let picked = select_backend_server(
            &mut servers,
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
        let mut servers = fleet();
        let picked = select_backend_server(
            &mut servers,
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
            &mut servers,
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
            &mut servers,
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
            &mut servers,
            RouteRequest {
                block: Some(100),
                fetch_finality_data: true,
            },
        )
        .unwrap();
        assert_eq!(err, "b");
    }

    fn route_at(block: u32) -> RouteRequest {
        RouteRequest {
            block: Some(block),
            fetch_finality_data: false,
        }
    }

    #[test]
    fn retry_prefers_another_upstream_that_covers_the_block() {
        let mut servers = HashMap::new();
        // The upstream that just failed has fewer connections, so plain
        // least-connections would pick it again.
        servers.insert("failed".to_string(), server(0, (1, 500), (0, 0)));
        servers.insert("other".to_string(), server(4, (1, 500), (0, 0)));
        let picked = select_failover_server(&mut servers, route_at(100), Some("failed")).unwrap();
        assert_eq!(picked, "other");
    }

    #[test]
    fn retry_keeps_the_only_upstream_that_covers_the_block() {
        let mut servers = HashMap::new();
        servers.insert("failed".to_string(), server(1, (1, 500), (0, 0)));
        servers.insert("short".to_string(), server(0, (1, 50), (0, 0)));
        let picked = select_failover_server(&mut servers, route_at(100), Some("failed")).unwrap();
        assert_eq!(picked, "failed");
    }

    #[test]
    fn retry_of_the_only_upstream_still_selects_it() {
        let mut servers = HashMap::new();
        servers.insert("only".to_string(), server(1, (1, 500), (0, 0)));
        let picked = select_failover_server(&mut servers, route_at(100), Some("only")).unwrap();
        assert_eq!(picked, "only");
    }

    #[test]
    fn quarantined_upstream_is_skipped_only_inside_its_range() {
        use std::time::Duration;

        use crate::models::BlockQuarantine;

        let mut servers = HashMap::new();
        let mut bad = server(0, (1, 500), (0, 0));
        bad.quarantines.push(BlockQuarantine {
            block: 115,
            range: 1,
            until: Instant::now() + Duration::from_secs(60),
        });
        bad.online = true;
        servers.insert("bad".to_string(), bad);
        servers.insert("good".to_string(), server(5, (1, 500), (0, 0)));

        assert_eq!(
            select_backend_server(&mut servers, route_at(115)).unwrap(),
            "good"
        );
        assert_eq!(
            select_backend_server(&mut servers, route_at(100)).unwrap(),
            "bad",
            "a quarantine at 115 must not affect a request starting at 100"
        );
        assert_eq!(
            select_backend_server(&mut servers, RouteRequest::default()).unwrap(),
            "bad",
            "initial connect has no block and must ignore range quarantine"
        );
    }

    #[test]
    fn expired_quarantine_does_not_exclude() {
        use std::time::Duration;

        use crate::models::BlockQuarantine;

        let mut servers = HashMap::new();
        let mut bad = server(0, (1, 500), (0, 0));
        bad.quarantines.push(BlockQuarantine {
            block: 115,
            range: 1,
            until: Instant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(Instant::now),
        });
        servers.insert("bad".to_string(), bad);
        servers.insert("good".to_string(), server(3, (1, 500), (0, 0)));
        assert_eq!(
            select_backend_server(&mut servers, route_at(115)).unwrap(),
            "bad"
        );
    }

    #[test]
    fn quarantine_does_not_fall_back_to_the_excluded_upstream() {
        use std::time::Duration;

        use crate::models::BlockQuarantine;

        let mut servers = HashMap::new();
        let mut only = server(0, (1, 500), (0, 0));
        only.quarantines.push(BlockQuarantine {
            block: 115,
            range: 2,
            until: Instant::now() + Duration::from_secs(60),
        });
        servers.insert("only".to_string(), only);
        assert!(select_backend_server(&mut servers, route_at(116)).is_err());
        assert!(select_failover_server(&mut servers, route_at(116), Some("only")).is_err());
        assert_eq!(
            select_backend_server(&mut servers, route_at(117)).unwrap(),
            "only"
        );
    }

    fn tiered(conns: usize, priority: u32, weight: u32) -> ServerState {
        let mut state = server(conns, (1, 500), (0, 0));
        state.priority = priority;
        state.weight = weight;
        state
    }

    #[test]
    fn lower_priority_tier_wins_over_fewer_connections() {
        let mut servers = HashMap::new();
        servers.insert("primary".to_string(), tiered(9, 0, 1));
        servers.insert("backup".to_string(), tiered(0, 1, 1));
        assert_eq!(
            select_backend_server(&mut servers, route_at(100)).unwrap(),
            "primary"
        );
        assert_eq!(
            select_backend_server(&mut servers, RouteRequest::default()).unwrap(),
            "primary",
            "priority also applies on initial connect, before any block is known"
        );
    }

    #[test]
    fn backup_tier_serves_when_the_preferred_tier_is_offline() {
        let mut servers = HashMap::new();
        let mut primary = tiered(0, 0, 1);
        primary.online = false;
        servers.insert("primary".to_string(), primary);
        servers.insert("backup".to_string(), tiered(3, 100, 1));
        assert_eq!(
            select_backend_server(&mut servers, route_at(100)).unwrap(),
            "backup"
        );
    }

    #[test]
    fn coverage_and_freshness_outrank_priority() {
        let mut servers = HashMap::new();
        let mut short = tiered(0, 0, 1);
        short.trace_end_block = 50;
        servers.insert("preferred-short".to_string(), short);
        servers.insert("backup-covers".to_string(), tiered(0, 5, 1));
        assert_eq!(
            select_backend_server(&mut servers, route_at(100)).unwrap(),
            "backup-covers"
        );

        let mut servers = HashMap::new();
        let mut stale = tiered(0, 0, 1);
        stale.stale = true;
        servers.insert("preferred-stale".to_string(), stale);
        servers.insert("backup-fresh".to_string(), tiered(0, 5, 1));
        assert_eq!(
            select_backend_server(&mut servers, route_at(100)).unwrap(),
            "backup-fresh"
        );
    }

    #[test]
    fn weight_splits_sessions_within_a_tier() {
        let mut servers = HashMap::new();
        servers.insert("big".to_string(), tiered(0, 0, 2));
        servers.insert("small".to_string(), tiered(0, 0, 1));
        servers.insert("backup".to_string(), tiered(0, 1, 10));
        for _ in 0..30 {
            let picked = select_backend_server(&mut servers, route_at(100)).unwrap();
            servers[&picked].connections.fetch_add(1, Ordering::Relaxed);
        }
        let conns = |name: &str| servers[name].connections.load(Ordering::Relaxed);
        assert_eq!(conns("big"), 20);
        assert_eq!(conns("small"), 10);
        assert_eq!(conns("backup"), 0);
    }

    #[test]
    fn equal_load_prefers_the_higher_weight() {
        let mut servers = HashMap::new();
        servers.insert("small".to_string(), tiered(1, 0, 1));
        servers.insert("big".to_string(), tiered(2, 0, 2));
        assert_eq!(
            select_backend_server(&mut servers, route_at(100)).unwrap(),
            "big"
        );
    }

    #[test]
    fn operator_disabled_upstream_is_skipped_by_new_sessions_and_failover() {
        let mut servers = HashMap::new();
        let mut drained = tiered(0, 0, 1);
        drained.operator_disabled = true;
        servers.insert("drained".to_string(), drained);
        servers.insert("other".to_string(), tiered(5, 9, 1));
        assert_eq!(
            select_backend_server(&mut servers, route_at(100)).unwrap(),
            "other"
        );
        assert_eq!(
            select_failover_server(&mut servers, route_at(100), None).unwrap(),
            "other"
        );
        servers.remove("other");
        assert!(select_backend_server(&mut servers, route_at(100)).is_err());
        assert!(select_failover_server(&mut servers, route_at(100), None).is_err());
    }

    #[test]
    fn retry_leaves_a_failed_preferred_upstream_for_a_backup() {
        let mut servers = HashMap::new();
        servers.insert("primary".to_string(), tiered(0, 0, 1));
        servers.insert("backup".to_string(), tiered(0, 5, 1));
        let picked = select_failover_server(&mut servers, route_at(100), Some("primary")).unwrap();
        assert_eq!(picked, "backup");
    }
}
