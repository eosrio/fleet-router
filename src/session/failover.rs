//! Failover decisions: how a link end is classified, the per-session
//! same-block budget and its backoff, the strike recorded against an
//! upstream, and the jitter added to every retry delay.

use std::time::Duration;

use tokio::time::Instant;

use crate::abi::fnv1a64;
use crate::fleet::{ContractViolation, FailoverReason, FleetHub, HoldWhy};
use crate::models::{BlockFailureQuarantine, Role};
use crate::telemetry::{self, Event, EventKind, Level};

/// Per session, how many times the upstream stream may die with the same next
/// block before the client is closed. The drop that starts the loop counts, so
/// one bad block cannot be re-dialed without a limit.
pub(super) const MAX_SAME_BLOCK_FAILURES: u32 = 5;

/// A pool unknown-variant frame forwarded this recently makes an unclean
/// drop the client's doing.
pub(super) const CLIENT_INDUCED_WINDOW: Duration = Duration::from_secs(5);

/// The pause before a failover that is not an unclean drop.
const CLEAN_DELAY: Duration = Duration::from_millis(100);

/// How long an upstream is not re-dialed by a session after a failed dial or
/// a request write that failed before any frame.
pub(super) const DIAL_EXCLUSION: Duration = Duration::from_secs(2);

/// The longest an operator move keeps the session off the upstream it left.
/// The exclusion ends sooner: once the next link is up, or when nothing
/// else can take the session, which then returns there.
pub(super) const MOVE_EXCLUSION: Duration = Duration::from_secs(10);

/// How long an upstream that jumped past the cursor on a resume is avoided.
const UNCOVERED_EXCLUSION: Duration = Duration::from_secs(30);

/// Stitched: how long a session avoids an upstream whose ABI differs from the
/// one its client was sent, under `abi_mismatch: "exclude"`.
pub(super) const ABI_EXCLUSION: Duration = Duration::from_secs(300);

/// First retry waits 100 ms (the historical failover pause). Each further retry
/// at that same block doubles the wait, up to 3.2 s.
fn same_block_backoff(failures: u32) -> Duration {
    // 100, 200, 400, ... capped at 3.2 s. Shift is capped so `100 << shift` fits.
    let shift = failures.saturating_sub(1).min(5);
    Duration::from_millis((100u64 << shift).min(3_200))
}

pub(super) fn block_unavailable_reason(block: u32) -> String {
    format!("block {block} unavailable on all upstreams")
}

/// The pause before retry `attempt` (from 0) of a held session: 100 ms,
/// doubling to 3.2 s, before jitter.
pub(super) fn hold_backoff(attempt: u32) -> Duration {
    same_block_backoff(attempt.saturating_add(1))
}

/// The close reason when a bounded hold at `block` runs out of `gap_wait_ms`.
pub(super) fn hold_close_reason(why: HoldWhy, block: u32) -> String {
    match why {
        HoldWhy::Offline | HoldWhy::Quarantined => block_unavailable_reason(block),
        HoldWhy::AtCapacity | HoldWhy::Busy => {
            format!("upstream capacity exhausted for block {block}")
        }
    }
}

pub(super) struct SameBlockPlan {
    /// Set when the unclean-drop budget at this block is exhausted.
    pub close_reason: Option<String>,
    pub delay: Duration,
    pub failures: u32,
}

impl SameBlockPlan {
    /// A failover that does not touch the budget.
    pub fn passthrough(delay: Duration) -> Self {
        SameBlockPlan {
            close_reason: None,
            delay,
            failures: 0,
        }
    }
}

/// Decide the next failover step.
///
/// Only an end that `classify` charges to the block (an unclean drop, a
/// frozen stall or violation) consumes the per-block budget. Any other end
/// still fails over after `delay` and does not move `stuck_at`: a client
/// following the chain tip waits at head+1, and those waits must not close
/// the session with 1011.
pub(super) fn plan_same_block_failover(
    stuck_at: &mut Option<(u32, u32)>,
    budget: bool,
    next_block: Option<u32>,
    delay: Duration,
) -> SameBlockPlan {
    let passthrough = SameBlockPlan::passthrough(delay);
    if !budget {
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
            delay: CLEAN_DELAY,
            failures,
        }
    } else {
        SameBlockPlan {
            close_reason: None,
            delay: same_block_backoff(failures),
            failures,
        }
    }
}

/// A frozen upstream broke the archive contract.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ViolationKind {
    /// An id-less or keep-alive result where ids are known.
    Idless,
    /// `this_block` above the expected block.
    Gap,
    /// `this_block` below the expected block, outside a resume window.
    Rewind,
    /// A requested field missing although the upstream advertises that log.
    MissingField,
    /// A result while the client had no credit.
    Credit,
    /// The wrong result variant, a status result, or garbage.
    Variant,
    /// `prev_block` does not continue the last delivered block at a seam.
    Discontinuity,
}

impl ViolationKind {
    pub fn label(self) -> ContractViolation {
        match self {
            ViolationKind::Idless => ContractViolation::Idless,
            ViolationKind::Gap => ContractViolation::Gap,
            ViolationKind::Rewind => ContractViolation::Rewind,
            ViolationKind::MissingField => ContractViolation::MissingField,
            ViolationKind::Credit => ContractViolation::Credit,
            ViolationKind::Variant => ContractViolation::Variant,
            ViolationKind::Discontinuity => ContractViolation::Discontinuity,
        }
    }
}

/// How a link ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum UpEnd {
    /// The stream ended or errored without a Close frame.
    Unclean,
    /// A Close frame with this code.
    Clean(u16),
    /// `idle_timeout_ms` of silence while blocks were owed.
    Idle,
    /// `upstream_stall_timeout_ms` of silence while blocks were owed.
    Stall,
    /// The monitor marked the upstream offline and the owed link went silent.
    MonitorOffline,
    /// Frozen only.
    Violation(ViolationKind),
    /// A live resume jumped past the cursor while another upstream covers it.
    Uncovered,
    /// A message above `max_message_bytes`.
    TooLarge,
    /// The request write failed on a link that never produced a frame.
    SendFailed,
    /// No frame at all, not even a reply to a liveness ping, within
    /// `upstream_liveness_timeout_ms` (1 s once the monitor lost the
    /// upstream). The socket may still look open.
    Silent,
    /// An operator moved the session off this upstream.
    Moved,
}

/// Which upstream a session avoids after a link end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Exclusion {
    None,
    /// Prefer another upstream of the same rank.
    Soft,
    /// Never pick it again for this long.
    Hard(Duration),
}

/// What a link end leads to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum EndAction {
    /// Close the client with this code and reason.
    CloseClient {
        code: u16,
        reason: String,
        too_large: bool,
    },
    Failover {
        /// Count `block_failures`, a quarantine strike.
        strike: bool,
        /// Consume the same-block budget (and use its backoff).
        budget: bool,
        /// The pause when `budget` is false.
        delay: Duration,
        exclusion: Exclusion,
        reason: FailoverReason,
    },
}

/// What the classification needs to know about the link that ended.
#[derive(Clone, Copy, Debug)]
pub(super) struct EndFacts {
    pub role: Role,
    /// Pool: when an unknown-variant client frame was last forwarded on it.
    pub unknown_forwarded_at: Option<Instant>,
}

/// The link-end table. Only unclean drops, frozen stalls and frozen contract
/// violations strike the upstream; a clean close, an idle or live stall, a
/// busy, uncovered or silent upstream, an operator move, an oversized message
/// or a client-induced drop never do. `cursor` is the client's next block.
pub(super) fn classify(end: UpEnd, facts: &EndFacts, cursor: u32, now: Instant) -> EndAction {
    let failover = |strike: bool, budget: bool, delay: Duration, exclusion: Exclusion, reason| {
        EndAction::Failover {
            strike,
            budget,
            delay,
            exclusion,
            reason,
        }
    };
    match end {
        UpEnd::Unclean => {
            let induced = facts
                .unknown_forwarded_at
                .is_some_and(|at| now.saturating_duration_since(at) <= CLIENT_INDUCED_WINDOW);
            if induced {
                EndAction::CloseClient {
                    code: 1003,
                    reason: "unsupported request".to_string(),
                    too_large: false,
                }
            } else {
                failover(
                    true,
                    true,
                    CLEAN_DELAY,
                    Exclusion::Soft,
                    FailoverReason::Unclean,
                )
            }
        }
        UpEnd::Clean(_) => failover(
            false,
            false,
            CLEAN_DELAY,
            Exclusion::None,
            FailoverReason::Clean,
        ),
        UpEnd::Idle => failover(
            false,
            false,
            CLEAN_DELAY,
            Exclusion::None,
            FailoverReason::Idle,
        ),
        UpEnd::Stall => match facts.role {
            Role::Live => failover(
                false,
                false,
                CLEAN_DELAY,
                Exclusion::Soft,
                FailoverReason::Stall,
            ),
            Role::Frozen => failover(
                true,
                true,
                CLEAN_DELAY,
                Exclusion::Soft,
                FailoverReason::Stall,
            ),
        },
        UpEnd::MonitorOffline => failover(
            false,
            false,
            CLEAN_DELAY,
            Exclusion::Soft,
            FailoverReason::Offline,
        ),
        UpEnd::Violation(_) => failover(
            true,
            true,
            CLEAN_DELAY,
            Exclusion::Soft,
            FailoverReason::Violation,
        ),
        UpEnd::Uncovered => failover(
            false,
            false,
            Duration::ZERO,
            Exclusion::Hard(UNCOVERED_EXCLUSION),
            FailoverReason::Uncovered,
        ),
        UpEnd::TooLarge => EndAction::CloseClient {
            code: 1011,
            reason: format!("block {cursor} exceeds max_message_bytes"),
            too_large: true,
        },
        UpEnd::SendFailed => failover(
            false,
            false,
            CLEAN_DELAY,
            Exclusion::Hard(DIAL_EXCLUSION),
            FailoverReason::Send,
        ),
        // A dead node, not a bad block: prefer another upstream, but no
        // strike against the block.
        UpEnd::Silent => failover(
            false,
            false,
            CLEAN_DELAY,
            Exclusion::Soft,
            FailoverReason::Silent,
        ),
        UpEnd::Moved => failover(
            false,
            false,
            CLEAN_DELAY,
            Exclusion::Soft,
            FailoverReason::Operator,
        ),
    }
}

/// `delay` ± 20 %, deterministic for a given session and failure count, so
/// sessions that failed together do not retry in lockstep.
pub(super) fn jitter(delay: Duration, session: u64, failures: u32) -> Duration {
    let mut seed = [0u8; 12];
    seed[..8].copy_from_slice(&session.to_le_bytes());
    seed[8..].copy_from_slice(&failures.to_le_bytes());
    // 0..=4000 per mille steps of 0.01 %, mapped onto [-20 %, +20 %].
    let step = fnv1a64(&seed) % 4001;
    let factor = 0.8 + step as f64 / 10_000.0;
    delay.mul_f64(factor)
}

/// Record an unclean mid-`get_blocks` drop (or a frozen stall or contract
/// violation) at `block`, and publish a quarantine it arms. Does not mark the
/// upstream offline.
pub(super) async fn note_block_failure(
    hub: &FleetHub,
    endpoint: &str,
    block: u32,
    policy: Option<BlockFailureQuarantine>,
) {
    let mut db = hub.state.lock().await;
    let Some(state) = db.get_mut(endpoint) else {
        return;
    };
    if state.record_block_failure(block, std::time::Instant::now(), policy) {
        let range = policy.map(|item| item.range).unwrap_or(1);
        tracing::warn!(
            upstream = %endpoint,
            block,
            range,
            "upstream quarantined after repeated mid-stream block failures"
        );
        let last = block.saturating_add(range.saturating_sub(1));
        telemetry::global().emit(
            Event::new(
                Level::Warn,
                EventKind::UpstreamQuarantined,
                format!("{} quarantined for blocks {block}..={last}", state.name),
            )
            .listener(hub.name.as_deref().unwrap_or("default"))
            .upstream(endpoint),
        );
    }
    hub.rebuild(&db);
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
    fn holds_back_off_from_100ms_and_close_with_the_reason_of_their_cause() {
        let waits: Vec<u64> = (0..8)
            .map(|attempt| hold_backoff(attempt).as_millis() as u64)
            .collect();
        assert_eq!(waits, [100, 200, 400, 800, 1600, 3200, 3200, 3200]);
        for why in [HoldWhy::Offline, HoldWhy::Quarantined] {
            assert_eq!(
                hold_close_reason(why, 1001),
                "block 1001 unavailable on all upstreams"
            );
        }
        for why in [HoldWhy::AtCapacity, HoldWhy::Busy] {
            assert_eq!(
                hold_close_reason(why, 1001),
                "upstream capacity exhausted for block 1001"
            );
        }
    }

    #[test]
    fn idle_and_clean_stops_at_the_tip_do_not_exhaust_the_block_budget() {
        let tip = Some(115u32);
        let live = facts(Role::Live, None);
        for stop in [UpEnd::Idle, UpEnd::Clean(1000)] {
            let EndAction::Failover { budget, delay, .. } =
                classify(stop, &live, 115, Instant::now())
            else {
                panic!("{stop:?} must fail over");
            };
            let mut stuck_at = None;
            for _ in 0..8 {
                let plan = plan_same_block_failover(&mut stuck_at, budget, tip, delay);
                assert!(
                    plan.close_reason.is_none(),
                    "{stop:?} must not close with 1011"
                );
                assert_eq!(plan.delay, Duration::from_millis(100));
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
            let plan = plan_same_block_failover(&mut stuck_at, true, Some(115), CLEAN_DELAY);
            assert!(plan.close_reason.is_none());
            assert_eq!(plan.failures, expected);
            assert_eq!(plan.delay, same_block_backoff(expected));
        }
        let plan = plan_same_block_failover(&mut stuck_at, true, Some(115), CLEAN_DELAY);
        assert_eq!(
            plan.close_reason.as_deref(),
            Some("block 115 unavailable on all upstreams")
        );
        assert_eq!(plan.failures, MAX_SAME_BLOCK_FAILURES);
    }

    fn facts(role: Role, forwarded: Option<Instant>) -> EndFacts {
        EndFacts {
            role,
            unknown_forwarded_at: forwarded,
        }
    }

    /// F1: the link-end table of the spec, row by row.
    #[test]
    fn link_ends_are_classified_exactly_as_the_table() {
        let now = Instant::now();
        let live = facts(Role::Live, None);
        let frozen = facts(Role::Frozen, None);
        let fo = |strike, budget, delay, exclusion, reason| EndAction::Failover {
            strike,
            budget,
            delay,
            exclusion,
            reason,
        };
        let ms = Duration::from_millis;
        let rows = [
            (
                UpEnd::Unclean,
                live,
                fo(
                    true,
                    true,
                    ms(100),
                    Exclusion::Soft,
                    FailoverReason::Unclean,
                ),
            ),
            (
                UpEnd::Clean(1000),
                live,
                fo(
                    false,
                    false,
                    ms(100),
                    Exclusion::None,
                    FailoverReason::Clean,
                ),
            ),
            (
                UpEnd::Clean(1001),
                frozen,
                fo(
                    false,
                    false,
                    ms(100),
                    Exclusion::None,
                    FailoverReason::Clean,
                ),
            ),
            (
                UpEnd::Idle,
                live,
                fo(false, false, ms(100), Exclusion::None, FailoverReason::Idle),
            ),
            (
                UpEnd::Stall,
                live,
                fo(
                    false,
                    false,
                    ms(100),
                    Exclusion::Soft,
                    FailoverReason::Stall,
                ),
            ),
            (
                UpEnd::Stall,
                frozen,
                fo(true, true, ms(100), Exclusion::Soft, FailoverReason::Stall),
            ),
            (
                UpEnd::MonitorOffline,
                live,
                fo(
                    false,
                    false,
                    ms(100),
                    Exclusion::Soft,
                    FailoverReason::Offline,
                ),
            ),
            (
                UpEnd::Violation(ViolationKind::Gap),
                frozen,
                fo(
                    true,
                    true,
                    ms(100),
                    Exclusion::Soft,
                    FailoverReason::Violation,
                ),
            ),
            (
                UpEnd::Uncovered,
                live,
                fo(
                    false,
                    false,
                    Duration::ZERO,
                    Exclusion::Hard(Duration::from_secs(30)),
                    FailoverReason::Uncovered,
                ),
            ),
            (
                UpEnd::SendFailed,
                live,
                fo(
                    false,
                    false,
                    ms(100),
                    Exclusion::Hard(Duration::from_secs(2)),
                    FailoverReason::Send,
                ),
            ),
        ];
        for (end, f, want) in rows {
            assert_eq!(classify(end, &f, 115, now), want, "{end:?} on {:?}", f.role);
        }
        assert_eq!(
            classify(UpEnd::TooLarge, &live, 115, now),
            EndAction::CloseClient {
                code: 1011,
                reason: "block 115 exceeds max_message_bytes".to_string(),
                too_large: true,
            }
        );

        // ClientInduced: an unclean drop within 5 s of a forwarded unknown
        // variant closes the client and strikes nobody.
        let induced = facts(Role::Live, Some(now - Duration::from_secs(4)));
        assert_eq!(
            classify(UpEnd::Unclean, &induced, 115, now),
            EndAction::CloseClient {
                code: 1003,
                reason: "unsupported request".to_string(),
                too_large: false,
            }
        );
        // Outside the window it is a plain unclean drop.
        let earlier = now - Duration::from_secs(6);
        let stale = facts(Role::Live, Some(earlier));
        assert!(matches!(
            classify(UpEnd::Unclean, &stale, 115, now),
            EndAction::Failover {
                strike: true,
                reason: FailoverReason::Unclean,
                ..
            }
        ));
        // A forwarded unknown variant does not change other ends.
        assert!(matches!(
            classify(UpEnd::Clean(1000), &induced, 115, now),
            EndAction::Failover {
                strike: false,
                reason: FailoverReason::Clean,
                ..
            }
        ));
    }

    /// A silent upstream and an operator move fail over to another upstream
    /// without a strike or the same-block budget.
    #[test]
    fn silent_upstreams_and_operator_moves_are_avoided_without_a_block_strike() {
        let now = Instant::now();
        for role in [Role::Live, Role::Frozen] {
            for (end, reason) in [
                (UpEnd::Silent, FailoverReason::Silent),
                (UpEnd::Moved, FailoverReason::Operator),
            ] {
                assert_eq!(
                    classify(end, &facts(role, None), 115, now),
                    EndAction::Failover {
                        strike: false,
                        budget: false,
                        delay: Duration::from_millis(100),
                        exclusion: Exclusion::Soft,
                        reason,
                    },
                    "{end:?} on {role:?}"
                );
            }
        }
        let mut stuck_at = None;
        for _ in 0..8 {
            let plan = plan_same_block_failover(&mut stuck_at, false, Some(115), CLEAN_DELAY);
            assert!(
                plan.close_reason.is_none(),
                "a dead node must not close with 1011"
            );
            assert_eq!(plan.failures, 0);
            assert_eq!(plan.delay, Duration::from_millis(100));
        }
        assert!(stuck_at.is_none(), "a dead node is not a bad block");
    }

    /// F2: jitter is within ±20 % and deterministic.
    #[test]
    fn jitter_is_bounded_and_deterministic() {
        let base = Duration::from_millis(1000);
        let mut seen = std::collections::HashSet::new();
        for session in 0..500u64 {
            for failures in 0..5 {
                let d = jitter(base, session, failures);
                assert!(d >= Duration::from_millis(800), "{d:?}");
                assert!(d <= Duration::from_millis(1200), "{d:?}");
                assert_eq!(d, jitter(base, session, failures));
                seen.insert(d.as_millis());
            }
        }
        assert!(
            seen.len() > 100,
            "jitter should spread retries: {}",
            seen.len()
        );
        assert_eq!(jitter(Duration::ZERO, 7, 1), Duration::ZERO);
    }

    #[test]
    fn violation_kinds_have_metric_labels() {
        use crate::fleet::MetricLabel;
        for (kind, label) in [
            (ViolationKind::Idless, "idless"),
            (ViolationKind::Gap, "gap"),
            (ViolationKind::Rewind, "rewind"),
            (ViolationKind::MissingField, "missing_field"),
            (ViolationKind::Credit, "credit"),
            (ViolationKind::Variant, "variant"),
            (ViolationKind::Discontinuity, "discontinuity"),
        ] {
            assert_eq!(kind.label().as_str(), label);
        }
    }
}
