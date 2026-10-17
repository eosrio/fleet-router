//! Status replies to clients.
//!
//! The router answers `get_status_request_v0/v1` itself from the listener's
//! fleet view, in every topology: v0 gets `get_status_result_v0` and v1 gets
//! `get_status_result_v1`.
//! - head and LIB are the fleet tip: the highest of the fresh live anchors'
//!   status and observed frames, with the listener's LIB high-water mark,
//!   made monotonic for the session and `lib <= head`;
//! - each log's range is the top contiguous component of its sticky
//!   coverage, and a range that includes a live upstream ends at the fleet
//!   head + 1;
//! - `chain_id` is the pinned one, else the one the online upstreams report.
//!   With none known, v0 omits the extension (as nodes without it do) and v1
//!   sends zeros.
//!
//! Until the fleet tip is known, a pool session relays the request to its
//! upstream when a link is up (the reply gets the same head/LIB clamp), and
//! otherwise holds it until a link is up or the tip is known. Requests and
//! replies stay in order across failovers. Replies cost no credit.

use std::collections::VecDeque;
use std::time::Duration;

use tokio::time::Instant;

use crate::fleet::view::LogCoverage;
use crate::fleet::FleetView;
use crate::models::ResolvedTopology;
use crate::ship::wire::{encode_status, StatusBody, STATUS_V0_LEN};
use crate::ship::{BlockPos, Range};

use super::client::{mono, ClientSide};

/// Status requests a session may have waiting or in flight.
pub(super) const MAX_QUEUED_STATUS: usize = 1024;

/// A `get_status_result_v0` without its `chain_id` binary extension.
const STATUS_V0_NO_CHAIN_ID: usize = STATUS_V0_LEN - 32;

/// What to do with the oldest waiting status request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StatusStep {
    /// Answer it from the fleet view (`v1`).
    Reply(bool),
    /// Send it to the link's upstream and wait for its reply.
    Relay(bool),
}

/// The session's status requests, in order: those not answered yet (`hold`)
/// and those relayed to the current link and not answered yet (`backlog`).
#[derive(Debug, Default)]
pub(super) struct StatusFlow {
    hold: VecDeque<bool>,
    backlog: VecDeque<bool>,
    deadline: Option<Instant>,
}

impl StatusFlow {
    /// Queue a client request. `false` when the session already has
    /// [`MAX_QUEUED_STATUS`] waiting or in flight. A waiting request is
    /// answered (or relayed) within `wait`, or the client is closed.
    pub fn push(&mut self, v1: bool, now: Instant, wait: Duration) -> bool {
        if self.hold.len() + self.backlog.len() >= MAX_QUEUED_STATUS {
            return false;
        }
        self.hold.push_back(v1);
        self.deadline.get_or_insert(now + wait);
        true
    }

    /// The next step for the oldest held request, given whether the router
    /// can answer (`ready`) and whether a link may relay (`can_relay`).
    /// `None` when nothing can be done now.
    pub fn next(&mut self, ready: bool, can_relay: bool) -> Option<StatusStep> {
        if self.hold.is_empty() {
            self.deadline = None;
            return None;
        }
        // Replies keep request order: while relayed requests are
        // outstanding, later ones are relayed behind them.
        let step = if !self.backlog.is_empty() || !ready {
            if !can_relay {
                return None;
            }
            let v1 = self.hold.pop_front()?;
            self.backlog.push_back(v1);
            StatusStep::Relay(v1)
        } else {
            StatusStep::Reply(self.hold.pop_front()?)
        };
        if self.hold.is_empty() {
            self.deadline = None;
        }
        Some(step)
    }

    /// An upstream status reply arrived: the version of the oldest relayed
    /// request, or `None` when nothing was relayed (an unsolicited reply).
    pub fn reply_arrived(&mut self) -> Option<bool> {
        self.backlog.pop_front()
    }

    /// The link ended: relayed requests go back to the front of the queue.
    pub fn link_ended(&mut self, now: Instant, wait: Duration) {
        while let Some(v1) = self.backlog.pop_back() {
            self.hold.push_front(v1);
        }
        if !self.hold.is_empty() {
            self.deadline.get_or_insert(now + wait);
        }
    }

    /// Requests wait for the fleet view or a link.
    pub fn waiting(&self) -> bool {
        !self.hold.is_empty()
    }

    /// When the oldest waiting request gives up.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}

/// Whether the router can answer status for this view: the fleet head and
/// LIB are known and, in a stitched listener, every enabled frozen upstream
/// has reported or declared its range.
pub(super) fn status_ready(view: &FleetView, tip: (Option<BlockPos>, Option<BlockPos>)) -> bool {
    tip.0.is_some()
        && tip.1.is_some()
        && (view.topology == ResolvedTopology::Pool || view.frozen_unknown == 0)
}

/// The range status reports for one log: the top coverage component, with a
/// live end moved to `head + 1` (the view may predate the fresh head).
pub(super) fn status_range(cov: &LogCoverage, head: u32) -> Range {
    let mut range = cov.top;
    if !range.is_empty() && cov.live_tail_begin.is_some_and(|b| b <= range.end) {
        range.end = range.end.max(head.saturating_add(1));
    }
    range
}

/// A status reply the router encoded.
pub(super) struct RouterStatus {
    pub bytes: Vec<u8>,
    /// A v1 reply carries zeros because no `chain_id` is known.
    pub zero_chain_id: bool,
}

/// Encode the router's status reply and record the head and LIB sent.
/// `None` when the fleet tip is not known.
pub(super) fn router_status(
    view: &FleetView,
    tip: (Option<BlockPos>, Option<BlockPos>),
    c: &mut ClientSide,
    v1: bool,
) -> Option<RouterStatus> {
    let (Some(head), Some(lib)) = tip else {
        return None;
    };
    let head = mono(head, c.head_sent);
    let mut lib = mono(lib, c.lib_sent);
    if lib.num > head.num {
        lib = head;
    }
    c.head_sent = Some(head);
    c.lib_sent = Some(lib);
    let body = StatusBody {
        head,
        lib,
        trace: status_range(&view.trace, head.num),
        chain_state: status_range(&view.chain_state, head.num),
        finality: status_range(&view.finality, head.num),
        chain_id: view.chain_id.unwrap_or([0; 32]),
    };
    let mut bytes = encode_status(v1, &body);
    let missing = view.chain_id.is_none();
    if missing && !v1 {
        bytes.truncate(STATUS_V0_NO_CHAIN_ID);
    }
    Some(RouterStatus {
        bytes,
        zero_chain_id: missing && v1,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::fleet::view::fixtures::{live, pos, states, view};
    use crate::fleet::ObservedTip;
    use crate::models::ServerState;
    use crate::ship::wire::STATUS_V1_LEN;

    const WAIT: Duration = Duration::from_secs(10);

    #[test]
    fn requests_are_answered_relayed_or_held_in_order() {
        let now = Instant::now();
        let mut flow = StatusFlow::default();

        // Ready: answered by the router at once.
        assert!(flow.push(false, now, WAIT));
        assert_eq!(flow.next(true, true), Some(StatusStep::Reply(false)));
        assert_eq!(flow.next(true, true), None);
        assert_eq!(flow.deadline(), None);

        // Not ready with a link: relayed, the reply is matched in order.
        assert!(flow.push(true, now, WAIT));
        assert_eq!(flow.next(false, true), Some(StatusStep::Relay(true)));
        // A later request stays behind the relayed one even once ready.
        assert!(flow.push(false, now, WAIT));
        assert_eq!(flow.next(true, true), Some(StatusStep::Relay(false)));
        assert_eq!(flow.reply_arrived(), Some(true));
        assert_eq!(flow.reply_arrived(), Some(false));
        assert_eq!(flow.reply_arrived(), None, "unsolicited");

        // No link and not ready: held with a deadline.
        assert!(flow.push(true, now, WAIT));
        assert_eq!(flow.next(false, false), None);
        assert!(flow.waiting());
        assert_eq!(flow.deadline(), Some(now + WAIT));
        assert_eq!(flow.next(true, false), Some(StatusStep::Reply(true)));
        assert!(!flow.waiting());
        assert_eq!(flow.deadline(), None);
    }

    #[test]
    fn relayed_requests_survive_a_failover_in_order() {
        let now = Instant::now();
        let mut flow = StatusFlow::default();
        for v1 in [true, false, true] {
            assert!(flow.push(v1, now, WAIT));
            assert!(matches!(flow.next(false, true), Some(StatusStep::Relay(_))));
        }
        assert_eq!(flow.reply_arrived(), Some(true));
        assert!(flow.push(false, now, WAIT));
        // The link drops before the other two replies arrive.
        flow.link_ended(now, WAIT);
        assert_eq!(flow.deadline(), Some(now + WAIT));
        let mut order = Vec::new();
        while let Some(step) = flow.next(true, false) {
            order.push(step);
        }
        assert_eq!(
            order,
            vec![
                StatusStep::Reply(false),
                StatusStep::Reply(true),
                StatusStep::Reply(false)
            ]
        );
    }

    #[test]
    fn the_queue_is_bounded() {
        let now = Instant::now();
        let mut flow = StatusFlow::default();
        for _ in 0..MAX_QUEUED_STATUS {
            assert!(flow.push(false, now, WAIT));
        }
        assert!(!flow.push(false, now, WAIT));
        assert!(matches!(
            flow.next(false, true),
            Some(StatusStep::Relay(false))
        ));
        assert!(!flow.push(false, now, WAIT), "relayed requests count too");
        assert_eq!(flow.reply_arrived(), Some(false));
        assert!(flow.push(false, now, WAIT));
    }

    fn pool_view(list: Vec<ServerState>, chain_id: Option<[u8; 32]>) -> FleetView {
        let ctx = crate::fleet::view::BuildCtx {
            pinned_chain_id: chain_id,
            ..crate::fleet::view::fixtures::ctx(ResolvedTopology::Pool)
        };
        let mut mem = crate::fleet::view::HubMemory::new(std::time::Instant::now());
        let tips: Vec<Arc<ObservedTip>> = (0..list.len()).map(|_| Arc::default()).collect();
        crate::fleet::view::build_view(
            &ctx,
            &states(list),
            &tips,
            &mut mem,
            std::time::Instant::now(),
        )
    }

    #[test]
    fn pool_status_aggregates_the_fleet() {
        let mut a = live(0, 2, 1000);
        a.chain_state_begin_block = 5;
        let b = live(1, 900, 1200);
        let v = pool_view(vec![a, b], Some([7; 32]));
        let tip = (v.head, v.lib);
        assert_eq!(tip.0, Some(pos(1200)));
        assert!(status_ready(&v, tip));

        let mut c = ClientSide::default();
        let reply = router_status(&v, tip, &mut c, false).unwrap();
        let s = mock_ship::client::StatusView::parse(&reply.bytes).unwrap();
        assert_eq!(s.variant, 0);
        assert_eq!(s.len, STATUS_V0_LEN);
        assert_eq!(s.head.0, 1200);
        assert_eq!(s.lib.0, 1190);
        assert_eq!(s.trace, (2, 1201));
        assert_eq!(s.chain_state, (5, 1201));
        assert_eq!(s.chain_id, Some([7; 32]));
        assert!(!reply.zero_chain_id);

        // A fresher tip moves the live ends with it.
        let fresher = (Some(pos(1300)), Some(pos(1290)));
        let s = router_status(&v, fresher, &mut c, true).unwrap();
        let s = mock_ship::client::StatusView::parse(&s.bytes).unwrap();
        assert_eq!((s.variant, s.len), (3, STATUS_V1_LEN));
        assert_eq!(s.trace, (2, 1301));
        assert_eq!(s.finality, Some((0, 0)));

        // Head and LIB never go back for this session.
        let older = (Some(pos(1100)), Some(pos(1050)));
        let s = router_status(&v, older, &mut c, false).unwrap();
        let s = mock_ship::client::StatusView::parse(&s.bytes).unwrap();
        assert_eq!((s.head.0, s.lib.0), (1300, 1290));
        assert_eq!(c.head_sent, Some(pos(1300)));

        // Not ready without a tip.
        assert!(!status_ready(&v, (None, None)));
        assert!(router_status(&v, (None, None), &mut c, false).is_none());
    }

    #[test]
    fn without_a_chain_id_v0_omits_it_and_v1_sends_zeros() {
        let v = pool_view(vec![live(0, 1, 500)], None);
        assert_eq!(v.chain_id, None);
        let tip = (v.head, v.lib);
        let mut c = ClientSide::default();
        let v0 = router_status(&v, tip, &mut c, false).unwrap();
        assert_eq!(v0.bytes.len(), STATUS_V0_NO_CHAIN_ID);
        assert!(!v0.zero_chain_id);
        let parsed = mock_ship::client::StatusView::parse(&v0.bytes).unwrap();
        assert_eq!(parsed.chain_id, None);
        let v1 = router_status(&v, tip, &mut c, true).unwrap();
        assert_eq!(v1.bytes.len(), STATUS_V1_LEN);
        assert!(v1.zero_chain_id);
        assert_eq!(
            mock_ship::client::StatusView::parse(&v1.bytes)
                .unwrap()
                .chain_id,
            Some([0; 32])
        );
    }

    #[test]
    fn stitched_status_waits_for_unreported_frozen_upstreams() {
        use crate::fleet::view::fixtures::frozen_unreported;
        let v = view(vec![live(0, 900, 1200), frozen_unreported(1, None)]);
        assert!(!status_ready(&v, (v.head, v.lib)));
        let v = view(vec![
            live(0, 900, 1200),
            frozen_unreported(1, Some((2, 900))),
        ]);
        assert!(status_ready(&v, (v.head, v.lib)));
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn position(p: BlockPos) -> serde_json::Value {
        serde_json::json!({ "block_num": p.num, "block_id": hex(&p.id) })
    }

    /// W5: the router's status bytes equal what a node serializing with the
    /// SHiP ABI writes.
    #[test]
    fn status_replies_match_the_ship_abi_serializer() {
        let abieos = rs_abieos::Abieos::new();
        abieos
            .set_abi_json("ship", mock_ship::SHIP_ABI_JSON)
            .expect("SHiP ABI loads");
        let body = StatusBody {
            head: pos(2300),
            lib: pos(2290),
            trace: Range {
                begin: 2,
                end: 2301,
            },
            chain_state: Range {
                begin: 5,
                end: 2301,
            },
            finality: Range {
                begin: 950,
                end: 2301,
            },
            chain_id: [0xAB; 32],
        };
        let common = |with_chain_id: bool| {
            let mut fields = serde_json::json!({
                "head": position(body.head),
                "last_irreversible": position(body.lib),
                "trace_begin_block": body.trace.begin,
                "trace_end_block": body.trace.end,
                "chain_state_begin_block": body.chain_state.begin,
                "chain_state_end_block": body.chain_state.end,
            });
            if with_chain_id {
                fields["chain_id"] = serde_json::json!(hex(&body.chain_id));
            }
            fields
        };
        let to_bin = |json: serde_json::Value| {
            abieos
                .json_to_bin("ship", "result", &json.to_string())
                .expect("status serializes")
        };

        let v0 = to_bin(serde_json::json!(["get_status_result_v0", common(true)]));
        assert_eq!(encode_status(false, &body), v0);
        assert_eq!(v0.len(), STATUS_V0_LEN);

        let mut v1_fields = common(true);
        v1_fields["finality_data_begin_block"] = serde_json::json!(body.finality.begin);
        v1_fields["finality_data_end_block"] = serde_json::json!(body.finality.end);
        let v1 = to_bin(serde_json::json!(["get_status_result_v1", v1_fields]));
        assert_eq!(encode_status(true, &body), v1);
        assert_eq!(v1.len(), STATUS_V1_LEN);

        // Without chain_id, v0 is what a node that omits the extension sends.
        let bare = to_bin(serde_json::json!(["get_status_result_v0", common(false)]));
        assert_eq!(bare.len(), STATUS_V0_NO_CHAIN_ID);
        let mut ours = encode_status(false, &body);
        ours.truncate(STATUS_V0_NO_CHAIN_ID);
        assert_eq!(ours, bare);
        // The status decoders the monitor uses read it back.
        let parsed = crate::zcd::deserialize_status_result(&ours[1..]).expect("decodes");
        assert!(matches!(
            parsed.get("head_block_num"),
            Ok(Some(crate::zcd::ZCDValues::U32(2300)))
        ));
    }
}
