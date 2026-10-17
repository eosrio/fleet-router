//! Failover decisions: the per-session same-block budget and its backoff, the
//! strike recorded on an unclean drop, and the replay request that resumes the
//! stream after the last delivered block.

use std::time::{Duration, Instant};

use crate::functions::{parse_blocks_route, RouteRequest};
use crate::models::{BlockFailureQuarantine, ServerStateDb};

/// Per session, how many times the upstream stream may die with the same next
/// block before the client is closed. The drop that starts the loop counts, so
/// one bad block cannot be re-dialed without a limit.
pub(super) const MAX_SAME_BLOCK_FAILURES: u32 = 5;

/// First retry waits 100 ms (the historical failover pause). Each further retry
/// at that same block doubles the wait, up to 3.2 s.
fn same_block_backoff(failures: u32) -> Duration {
    // 100, 200, 400, ... capped at 3.2 s. Shift is capped so `100 << shift` fits.
    let shift = failures.saturating_sub(1).min(5);
    Duration::from_millis((100u64 << shift).min(3_200))
}

fn block_unavailable_reason(block: u32) -> String {
    format!("block {block} unavailable on all upstreams")
}

/// Next block a replay should request: the block after the last one delivered,
/// or the in-flight request's start when nothing has been delivered yet.
pub(super) fn next_block_after(last_seen: Option<u32>, request: Option<&[u8]>) -> Option<u32> {
    if let Some(seen) = last_seen {
        return Some(seen.saturating_add(1));
    }
    request
        .and_then(parse_blocks_route)
        .map(|parsed| parsed.start_block_num)
}

/// Point a raw `get_blocks_request_v0/v1` at `next_block` by rewriting
/// `start_block_num` (bytes 1..5, right after the one-byte variant index).
/// A buffer too short to hold the field is left unchanged.
pub(super) fn resume_request_at(request: &mut [u8], next_block: u32) {
    if request.len() >= 5 {
        request[1..5].copy_from_slice(&next_block.to_le_bytes());
    }
}

/// Routing input for a failover dial: the replay request's start block and
/// finality flag, or the default (least-connections) when there is no request.
pub(super) fn failover_route(request: Option<&[u8]>) -> RouteRequest {
    request
        .and_then(parse_blocks_route)
        .map(|parsed| RouteRequest {
            block: Some(parsed.start_block_num),
            fetch_finality_data: parsed.fetch_finality_data,
        })
        .unwrap_or_default()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UpstreamStop {
    Idle,
    /// TCP dropped, or the websocket ended without a Close frame.
    Unclean,
    /// Upstream sent a WebSocket Close frame.
    Clean,
}

pub(super) struct SameBlockPlan {
    /// Set when the unclean-drop budget at this block is exhausted.
    pub close_reason: Option<String>,
    pub delay: Duration,
    /// Prefer an upstream other than the one that just failed.
    pub exclude_failed: bool,
    pub failures: u32,
}

/// Decide the next failover step.
///
/// Only an unclean drop consumes the per-block budget. An idle timeout or a
/// clean close still fails over, with the historical 100 ms pause, and does
/// not move `stuck_at`. A client following the chain tip waits at head+1;
/// those waits must not close the session with 1011.
pub(super) fn plan_same_block_failover(
    stuck_at: &mut Option<(u32, u32)>,
    stop: Option<UpstreamStop>,
    rerouted: bool,
    next_block: Option<u32>,
) -> SameBlockPlan {
    let passthrough = SameBlockPlan {
        close_reason: None,
        delay: Duration::from_millis(100),
        exclude_failed: false,
        failures: 0,
    };
    if rerouted || !matches!(stop, Some(UpstreamStop::Unclean)) {
        return passthrough;
    }
    let Some(block) = next_block else {
        return passthrough;
    };
    let failures = match *stuck_at {
        Some((stuck_block, seen)) if stuck_block == block => seen.saturating_add(1),
        _ => 1,
    };
    *stuck_at = Some((block, failures));
    if failures >= MAX_SAME_BLOCK_FAILURES {
        SameBlockPlan {
            close_reason: Some(block_unavailable_reason(block)),
            delay: Duration::from_millis(100),
            exclude_failed: false,
            failures,
        }
    } else {
        SameBlockPlan {
            close_reason: None,
            delay: same_block_backoff(failures),
            exclude_failed: true,
            failures,
        }
    }
}

/// Record an unclean mid-`get_blocks` drop. Does not mark the upstream offline.
pub(super) async fn note_block_failure(
    backend_servers: &ServerStateDb,
    endpoint: &str,
    block: u32,
    policy: Option<BlockFailureQuarantine>,
) {
    let mut db = backend_servers.lock().await;
    let Some(state) = db.get_mut(endpoint) else {
        return;
    };
    if state.record_block_failure(block, Instant::now(), policy) {
        let range = policy.map(|item| item.range).unwrap_or(1);
        tracing::warn!(
            upstream = %endpoint,
            block,
            range,
            "upstream quarantined after repeated mid-stream block failures"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_block_backoff_doubles_from_100ms_and_caps_at_3_2s() {
        assert_eq!(same_block_backoff(1), Duration::from_millis(100));
        assert_eq!(same_block_backoff(2), Duration::from_millis(200));
        assert_eq!(same_block_backoff(3), Duration::from_millis(400));
        assert_eq!(same_block_backoff(4), Duration::from_millis(800));
        assert_eq!(same_block_backoff(5), Duration::from_millis(1_600));
        assert_eq!(same_block_backoff(6), Duration::from_millis(3_200));
        assert_eq!(same_block_backoff(7), Duration::from_millis(3_200));
        assert_eq!(same_block_backoff(30), Duration::from_millis(3_200));
    }

    #[test]
    fn block_unavailable_reason_names_the_block() {
        assert_eq!(
            block_unavailable_reason(115),
            "block 115 unavailable on all upstreams"
        );
    }

    #[test]
    fn idle_and_clean_stops_at_the_tip_do_not_exhaust_the_block_budget() {
        let tip = Some(115u32);
        for stop in [UpstreamStop::Idle, UpstreamStop::Clean] {
            let mut stuck_at = None;
            for _ in 0..8 {
                let plan = plan_same_block_failover(&mut stuck_at, Some(stop), false, tip);
                assert!(
                    plan.close_reason.is_none(),
                    "{stop:?} must not close with 1011"
                );
                assert_eq!(plan.delay, Duration::from_millis(100));
                assert!(!plan.exclude_failed);
                assert_eq!(plan.failures, 0);
            }
            assert!(
                stuck_at.is_none(),
                "{stop:?} must not count toward the same-block budget"
            );
        }
    }

    #[test]
    fn unclean_drops_at_the_same_block_still_close_after_five() {
        let mut stuck_at = None;
        for expected in 1..MAX_SAME_BLOCK_FAILURES {
            let plan = plan_same_block_failover(
                &mut stuck_at,
                Some(UpstreamStop::Unclean),
                false,
                Some(115),
            );
            assert!(plan.close_reason.is_none());
            assert!(plan.exclude_failed);
            assert_eq!(plan.failures, expected);
            assert_eq!(plan.delay, same_block_backoff(expected));
        }
        let plan =
            plan_same_block_failover(&mut stuck_at, Some(UpstreamStop::Unclean), false, Some(115));
        assert_eq!(
            plan.close_reason.as_deref(),
            Some("block 115 unavailable on all upstreams")
        );
        assert_eq!(plan.failures, MAX_SAME_BLOCK_FAILURES);
    }

    #[test]
    fn next_block_follows_the_last_delivered_block() {
        let request = {
            let mut buf = vec![1u8];
            buf.extend_from_slice(&100u32.to_le_bytes());
            buf.extend_from_slice(&200u32.to_le_bytes());
            buf
        };
        assert_eq!(next_block_after(Some(114), Some(&request)), Some(115));
        assert_eq!(next_block_after(None, Some(&request)), Some(100));
        assert_eq!(next_block_after(None, None), None);
    }

    /// `get_blocks_request_v1` with no positions and the given finality flag.
    fn blocks_request_v1(start: u32, end: u32, fetch_finality_data: bool) -> Vec<u8> {
        let mut buf = vec![3u8];
        buf.extend_from_slice(&start.to_le_bytes());
        buf.extend_from_slice(&end.to_le_bytes());
        buf.extend_from_slice(&10u32.to_le_bytes());
        buf.push(0); // have_positions
        buf.extend_from_slice(&[0, 0, 0, 0]); // irreversible_only, block, traces, deltas
        buf.push(u8::from(fetch_finality_data));
        buf
    }

    #[test]
    fn resume_request_rewrites_only_the_start_block() {
        let mut request = blocks_request_v1(100, 200, true);
        let original = request.clone();
        resume_request_at(&mut request, 115);
        assert_eq!(&request[1..5], &115u32.to_le_bytes());
        assert_eq!(request[0], original[0]);
        assert_eq!(&request[5..], &original[5..]);
        assert_eq!(next_block_after(None, Some(&request)), Some(115));
    }

    #[test]
    fn resume_request_leaves_a_short_buffer_alone() {
        let mut request = vec![1u8, 2, 3, 4];
        resume_request_at(&mut request, 115);
        assert_eq!(request, vec![1u8, 2, 3, 4]);
    }

    #[test]
    fn failover_route_follows_the_replay_request() {
        let request = blocks_request_v1(100, 200, true);
        assert_eq!(
            failover_route(Some(&request)),
            RouteRequest {
                block: Some(100),
                fetch_finality_data: true,
            }
        );
        let without_finality = blocks_request_v1(100, 200, false);
        assert_eq!(
            failover_route(Some(&without_finality)),
            RouteRequest {
                block: Some(100),
                fetch_finality_data: false,
            }
        );
        // No request yet (or not a blocks request): least-connections.
        assert_eq!(failover_route(None), RouteRequest::default());
        assert_eq!(failover_route(Some(&[0u8])), RouteRequest::default());
    }
}
