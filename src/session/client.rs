//! The client-facing side of a session: a mirror of what one Spring node
//! would hold for this client, and the pure rules applied to every upstream
//! result before it is forwarded.
//!
//! [`ClientSide`] mirrors `session.hpp`: the current request, the send-credit
//! counter `C`, and the cursor (the next block the client expects). It also
//! remembers the head and LIB already sent, so they never move backwards, and
//! the reversible blocks already delivered, for de-duplication and for the
//! `have_positions` of a resume.
//!
//! Nothing here touches a socket. The session actor calls these helpers, and
//! the lockstep test drives them against simulated upstreams.

use std::collections::VecDeque;

use tokio_tungstenite::tungstenite::Message;

use crate::models::Role;
use crate::ship::wire::{BlocksRequest, ResultHead};
use crate::ship::BlockPos;

use super::failover::{UpEnd, ViolationKind};
use super::link::{LinkReq, LinkState, UpCursor};

/// Maximum payload bytes of client frames buffered per session while an upstream
/// is mid-failover. A frame counts as at least one byte, so an empty-frame flood
/// cannot grow the queue without bound. Exceeding the cap closes the session
/// (close code 1008) instead of dropping frames or pinning unbounded memory.
pub(super) const MAX_PENDING_CLIENT_BYTES: usize = 64 * 1024;

/// Reversible blocks remembered per session (`4096 × 36` bytes at most).
pub(super) const DELIVERED_CAP: usize = 4096;

/// Positions sent with a resume request: the newest reversible blocks.
pub(super) const RESUME_POSITIONS: usize = 1024;

/// Client frames held during a failover window, accounted in bytes.
pub(super) struct PendingClientFrames {
    frames: VecDeque<Message>,
    bytes: usize,
}

impl PendingClientFrames {
    pub(super) fn new() -> Self {
        Self {
            frames: VecDeque::new(),
            bytes: 0,
        }
    }

    /// Bytes this frame contributes toward [`MAX_PENDING_CLIENT_BYTES`].
    pub(super) fn accounted(msg: &Message) -> usize {
        msg.len().max(1)
    }

    /// Bytes currently held.
    pub(super) fn buffered(&self) -> usize {
        self.bytes
    }

    /// Queue `msg` when it fits. Returns `false` without modifying the queue
    /// when the byte cap would be exceeded (the message is dropped).
    pub(super) fn try_push(&mut self, msg: Message) -> bool {
        let n = Self::accounted(&msg);
        if self.bytes.saturating_add(n) > MAX_PENDING_CLIENT_BYTES {
            return false;
        }
        self.bytes += n;
        self.frames.push_back(msg);
        true
    }

    pub(super) fn pop_front(&mut self) -> Option<Message> {
        let msg = self.frames.pop_front()?;
        self.bytes = self.bytes.saturating_sub(Self::accounted(&msg));
        Some(msg)
    }
}

/// The reversible `(num, id)` pairs delivered to the client, ascending by
/// number. Blocks at or below the LIB already sent are pruned (the newest one
/// is kept), and at most [`DELIVERED_CAP`] entries are held.
#[derive(Debug, Default)]
pub(super) struct DeliveredRing {
    buf: VecDeque<BlockPos>,
}

impl DeliveredRing {
    /// Record a delivered block. A lower or equal number (a fork switch)
    /// first drops the entries it replaces.
    pub fn push(&mut self, p: BlockPos, lib: Option<BlockPos>) {
        while self.buf.back().is_some_and(|back| back.num >= p.num) {
            self.buf.pop_back();
        }
        self.buf.push_back(p);
        if let Some(lib) = lib {
            while self.buf.len() > 1 && self.buf.front().is_some_and(|f| f.num <= lib.num) {
                self.buf.pop_front();
            }
        }
        while self.buf.len() > DELIVERED_CAP {
            self.buf.pop_front();
        }
    }

    /// The id delivered for block `n`, if it is still remembered.
    pub fn get(&self, n: u32) -> Option<[u8; 32]> {
        let (front, back) = self.buf.as_slices();
        let find = |s: &[BlockPos]| s.binary_search_by_key(&n, |p| p.num).ok().map(|i| s[i].id);
        find(front).or_else(|| find(back))
    }

    /// Drop the entries with `num >= n`.
    pub fn truncate_from(&mut self, n: u32) {
        while self.buf.back().is_some_and(|back| back.num >= n) {
            self.buf.pop_back();
        }
    }

    /// Entries with `lib < num < below`: the newest `max`, ascending.
    pub fn reversible_tail(&self, lib: u32, below: u32, max: usize) -> Vec<BlockPos> {
        let picked: Vec<BlockPos> = self
            .buf
            .iter()
            .rev()
            .filter(|p| p.num > lib && p.num < below)
            .take(max)
            .copied()
            .collect();
        picked.into_iter().rev().collect()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.buf.len()
    }
}

/// What one Spring node would hold for this client (`session.hpp`), plus the
/// head, LIB and blocks the router has already sent it.
#[derive(Debug, Default)]
pub(super) struct ClientSide {
    /// The current client request, as decoded.
    pub req: Option<BlocksRequest>,
    /// `send_credits` mirror (C): replaced by a request, raised by acks
    /// (wrapping), lowered by every forwarded result.
    pub credit: u32,
    /// The next block the client expects.
    pub cursor: u32,
    /// Results forwarded since the current request.
    pub delivered_for_request: u64,
    pub head_sent: Option<BlockPos>,
    pub lib_sent: Option<BlockPos>,
    pub delivered: DeliveredRing,
    /// Highest delivered block number at or below `lib_sent`.
    pub irr_hwm: Option<u32>,
    /// Last delivered `this_block` (seam continuity).
    pub last: Option<BlockPos>,
    /// Fingerprint of the ABI this client was sent.
    pub abi_fp: u64,
}

impl ClientSide {
    /// A client ack: `C += n`, wrapping like Spring's `uint32_t` add. It is
    /// folded in whether or not a link is up.
    pub fn ack(&mut self, n: u32) {
        self.credit = self.credit.wrapping_add(n);
    }

    /// A new `get_blocks` replaces the request, the credit and the cursor.
    /// Returns the credit before the request (used to tell whether the link
    /// is quiescent).
    pub fn new_request(&mut self, r: BlocksRequest) -> u32 {
        let pre = self.credit;
        self.credit = r.max_in_flight;
        self.cursor = r.start;
        self.delivered_for_request = 0;
        self.delivered.truncate_from(r.start);
        self.req = Some(r);
        pre
    }

    /// Charge one forwarded result. Returns `true` when the client had no
    /// credit left (an overrun; C stays 0).
    pub fn charge(&mut self) -> bool {
        match self.credit.checked_sub(1) {
            Some(left) => {
                self.credit = left;
                false
            }
            None => true,
        }
    }

    /// Whether the current request still owes the client blocks.
    pub fn in_progress(&self) -> bool {
        self.req
            .as_ref()
            .is_some_and(|r| !request_is_empty(r) && self.cursor < r.end)
    }

    /// Inside a resume window, whether block `n` (below the cursor) is a
    /// replay of what the client already has.
    ///
    /// An exact `(num, id)` match is a duplicate; a different id is a fork
    /// and is forwarded. A block no longer remembered is a duplicate when it
    /// is at or below the irreversible high-water mark. An id-less result
    /// below the cursor is always a duplicate.
    pub fn is_dup(&self, n: u32, this: Option<BlockPos>) -> bool {
        match this {
            Some(p) => match self.delivered.get(n) {
                Some(id) => id == p.id,
                None => self.irr_hwm.is_some_and(|w| n <= w),
            },
            None => true,
        }
    }
}

/// `end <= start` and no position below `min(start, end)`: Spring can never
/// send anything for this request.
pub(super) fn request_is_empty(r: &BlocksRequest) -> bool {
    r.end <= r.start && !r.have_positions.iter().any(|p| p.num < r.start.min(r.end))
}

/// `x`, unless `sent` is higher: head and LIB never move backwards.
pub(super) fn mono(x: BlockPos, sent: Option<BlockPos>) -> BlockPos {
    match sent {
        Some(s) if s.num > x.num => s,
        _ => x,
    }
}

/// The head and LIB a result frame must carry, and the session's new
/// high-water marks.
///
/// A frozen frame takes the fleet tip (`fleet`). Every frame is then made
/// monotonic against what this session already sent, with `lib <= head`.
/// Returns the values to write when they differ from the frame's and
/// patching is on; `None` leaves the frame untouched.
pub(super) fn head_lib_for(
    c: &mut ClientSide,
    role: Role,
    head: BlockPos,
    lib: BlockPos,
    fleet: Option<(BlockPos, BlockPos)>,
    patch: bool,
) -> Option<(BlockPos, BlockPos)> {
    let (mut h, mut l) = (head, lib);
    if role == Role::Frozen {
        if let Some((fh, fl)) = fleet {
            h = fh;
            l = fl;
        }
    }
    h = mono(h, c.head_sent);
    l = mono(l, c.lib_sent);
    if l.num > h.num {
        l = h;
    }
    c.head_sent = Some(h);
    c.lib_sent = Some(l);
    (patch && (h != head || l != lib)).then_some((h, l))
}

/// How a block result moves the upstream's cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ResultKind {
    /// `this_block` is present.
    Block,
    /// No `this_block`, but Spring's send rule held for the upstream's
    /// cursor: an id-less result for that block, which advances it.
    IdLess,
    /// No `this_block` and the send rule did not hold: an EOSIO 2.0 / Leap 3
    /// head keep-alive. It costs a credit and does not advance.
    KeepAlive,
    /// No `this_block` while the upstream's cursor is unknown (positions
    /// were sent): treated as not advancing.
    IdLessUncertain,
}

/// One result, classified against the upstream cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Classified {
    /// The block number the result stands for, when known.
    pub n: Option<u32>,
    /// Whether the upstream's (and the client's) cursor moves to `n + 1`.
    pub advance: bool,
    pub kind: ResultKind,
}

/// The keep-alive rule. The frame's own head (or LIB for
/// `irreversible_only`) is the node's decision snapshot, so Spring's send
/// rule `cursor <= gate && cursor < end` evaluated on it tells an id-less
/// result from a head keep-alive.
pub(super) fn classify_result(h: &ResultHead, cursor: UpCursor, lr: &LinkReq) -> Classified {
    let gate = if lr.irreversible_only {
        h.lib.num
    } else {
        h.head.num
    };
    match (h.this_block, cursor) {
        (Some(p), _) => Classified {
            n: Some(p.num),
            advance: true,
            kind: ResultKind::Block,
        },
        (None, UpCursor::Exact(u)) if u <= gate && u < lr.end => Classified {
            n: Some(u),
            advance: true,
            kind: ResultKind::IdLess,
        },
        (None, UpCursor::Exact(_)) => Classified {
            n: None,
            advance: false,
            kind: ResultKind::KeepAlive,
        },
        (None, UpCursor::Uncertain { .. }) => Classified {
            n: None,
            advance: false,
            kind: ResultKind::IdLessUncertain,
        },
    }
}

/// What a frozen block result lacks of the fields its request asked for and
/// its upstream advertises at that block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Lacks {
    /// Nothing: every such field is there.
    Nothing,
    /// Only `block`: the archive has no block log there. Spring leaves
    /// `block` out the same way when its block log lacks the block, so this
    /// is no contract violation.
    Block,
    /// Traces, deltas or finality data its status claims, or payload fields
    /// that do not decode: a contract violation.
    Data,
}

/// A result that will be forwarded, and what it means.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Forward {
    pub cls: Classified,
    /// A block below the cursor outside a resume window: a fork switch.
    pub fork: bool,
    /// A live block above the cursor (Leap 4/5 first-available jump).
    pub jump: bool,
    /// A live result whose `prev_block` does not continue the last delivered
    /// block at a router-generated seam.
    pub discontinuity: bool,
    /// A live result whose variant does not follow the request version.
    pub variant_mismatch: bool,
    /// A frozen result without the requested `block`, for a block the
    /// upstream covers ([`Lacks::Block`]).
    pub no_block: bool,
}

/// What to do with one upstream block result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Judgement {
    Forward(Forward),
    /// A replay inside the resume window: drop it and refund `ack(1)`. When
    /// it carried a block number the upstream cursor moves to `advance_to`.
    Duplicate {
        advance_to: Option<u32>,
    },
    /// End the link (contract violation, or a live resume jump while another
    /// upstream covers the cursor).
    End(UpEnd),
}

/// Judge one block result before anything is forwarded.
///
/// `lacks(n)` tells what a frozen result lacks of the fields the request
/// asked for and the upstream advertises at `n`: missing data is a contract
/// violation, a missing `block` is not (see [`Lacks`]).
/// `alternative(k)` tells whether another upstream could serve block `k`.
pub(super) fn judge(
    c: &ClientSide,
    st: &LinkState,
    creq: &BlocksRequest,
    lr: &LinkReq,
    h: &ResultHead,
    lacks: impl FnOnce(u32) -> Lacks,
    alternative: impl FnOnce(u32) -> bool,
) -> Judgement {
    let cls = classify_result(h, st.up_cursor, lr);
    let frozen = st.role == Role::Frozen;
    let violation = |kind| Judgement::End(UpEnd::Violation(kind));
    let mut no_block = false;
    if frozen {
        if h.v1 != creq.v1 {
            return violation(ViolationKind::Variant);
        }
        match (cls.kind, cls.n) {
            (ResultKind::KeepAlive | ResultKind::IdLessUncertain, _) => {
                return violation(ViolationKind::Idless);
            }
            (ResultKind::IdLess, Some(n)) if n >= st.expect_ids_from => {
                return violation(ViolationKind::Idless);
            }
            (ResultKind::Block, Some(n)) => match lacks(n) {
                Lacks::Data => return violation(ViolationKind::MissingField),
                Lacks::Block => no_block = true,
                Lacks::Nothing => {}
            },
            _ => {}
        }
    }
    let mut fw = Forward {
        cls,
        fork: false,
        jump: false,
        discontinuity: false,
        variant_mismatch: !frozen && h.v1 != creq.v1,
        no_block,
    };
    let k = c.cursor;
    let seam = st.first_result_pending && lr.purpose.resumes();
    if let Some(n) = cls.n {
        if n < k {
            if st.resume_floor.is_some() && c.is_dup(n, h.this_block) {
                return Judgement::Duplicate {
                    advance_to: cls.advance.then(|| n.saturating_add(1)),
                };
            }
            if frozen {
                return violation(ViolationKind::Rewind);
            }
            fw.fork = true;
        } else if n > k {
            if frozen {
                return violation(ViolationKind::Gap);
            }
            if seam && alternative(k) {
                return Judgement::End(UpEnd::Uncovered);
            }
            fw.jump = true;
        }
    }
    if seam {
        if let (Some(this), Some(prev), Some(last)) = (h.this_block, h.prev_block, c.last) {
            if last.num.checked_add(1) == Some(this.num) && prev.id != last.id {
                if frozen {
                    return violation(ViolationKind::Discontinuity);
                }
                fw.discontinuity = true;
            }
        }
    }
    if frozen && c.credit == 0 {
        return violation(ViolationKind::Credit);
    }
    Judgement::Forward(fw)
}

/// Commit a result the client has just been sent. The head and LIB the
/// client got must already be recorded (`head_lib_for`), because pruning and
/// the irreversible high-water mark follow `lib_sent`.
pub(super) fn commit(c: &mut ClientSide, st: &mut LinkState, h: &ResultHead, fw: &Forward) {
    let cls = fw.cls;
    if fw.fork {
        if let Some(n) = cls.n {
            c.delivered.truncate_from(n);
        }
    }
    match (cls.advance, cls.n) {
        (true, Some(n)) => {
            let next = n.saturating_add(1);
            c.cursor = next;
            st.up_cursor = UpCursor::Exact(next);
        }
        _ if cls.kind == ResultKind::KeepAlive => st.keepalives_seen += 1,
        _ => {}
    }
    if let Some(p) = h.this_block {
        c.delivered.push(p, c.lib_sent);
        c.last = Some(p);
        if c.lib_sent.is_some_and(|l| p.num <= l.num) {
            c.irr_hwm = Some(c.irr_hwm.map_or(p.num, |w| w.max(p.num)));
        }
    }
    c.delivered_for_request += 1;
    st.frames += 1;
    st.last_head = Some(h.head);
    st.last_lib = Some(h.lib);
    st.first_result_pending = false;
    if st
        .resume_floor
        .is_some_and(|floor| cls.n.is_some_and(|n| n >= floor))
    {
        st.resume_floor = None;
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::Instant;

    use super::super::link::Purpose;
    use super::*;

    fn binary(n: usize) -> Message {
        Message::binary(vec![0u8; n])
    }

    #[test]
    fn pending_buffer_accepts_frames_until_the_byte_cap() {
        let mut pending = PendingClientFrames::new();
        assert!(pending.try_push(binary(1024)));
        assert!(pending.try_push(binary(MAX_PENDING_CLIENT_BYTES - 1024)));
        assert_eq!(pending.bytes, MAX_PENDING_CLIENT_BYTES);
        assert_eq!(pending.frames.len(), 2);
        assert!(!pending.try_push(binary(1)));
        assert_eq!(pending.frames.len(), 2);
        assert_eq!(pending.bytes, MAX_PENDING_CLIENT_BYTES);
    }

    #[test]
    fn pending_buffer_rejects_a_single_oversized_frame() {
        let mut pending = PendingClientFrames::new();
        assert!(!pending.try_push(binary(MAX_PENDING_CLIENT_BYTES + 1)));
        assert!(pending.frames.is_empty());
        assert_eq!(pending.bytes, 0);
        assert!(pending.try_push(binary(1)));
    }

    #[test]
    fn pending_buffer_exact_cap_fits_and_pop_frees_budget() {
        let mut pending = PendingClientFrames::new();
        assert!(pending.try_push(binary(MAX_PENDING_CLIENT_BYTES)));
        assert!(!pending.try_push(binary(1)));
        let got = pending.pop_front().unwrap();
        assert_eq!(got.len(), MAX_PENDING_CLIENT_BYTES);
        assert_eq!(pending.bytes, 0);
        assert!(pending.pop_front().is_none());
        assert!(pending.try_push(binary(MAX_PENDING_CLIENT_BYTES)));
    }

    #[test]
    fn pending_buffer_counts_empty_frames_as_one_byte() {
        let mut pending = PendingClientFrames::new();
        assert!(pending.try_push(binary(MAX_PENDING_CLIENT_BYTES - 1)));
        assert!(pending.try_push(Message::binary(Vec::<u8>::new())));
        assert_eq!(pending.bytes, MAX_PENDING_CLIENT_BYTES);
        assert!(!pending.try_push(Message::text("")));
        assert_eq!(pending.frames.len(), 2);
    }

    #[test]
    fn pending_buffer_counts_text_and_binary_payloads() {
        let mut pending = PendingClientFrames::new();
        assert!(pending.try_push(Message::text("abcd")));
        assert!(pending.try_push(Message::binary(vec![1, 2, 3])));
        assert_eq!(pending.bytes, 7);
        let first = pending.pop_front().unwrap();
        assert_eq!(first.len(), 4);
        assert_eq!(pending.bytes, 3);
    }

    #[test]
    fn pending_buffer_reports_its_buffered_bytes() {
        let mut pending = PendingClientFrames::new();
        assert_eq!(pending.buffered(), 0);
        assert!(pending.try_push(binary(10)));
        assert!(pending.try_push(Message::binary(Vec::<u8>::new())));
        assert_eq!(pending.buffered(), 11);
        pending.pop_front();
        assert_eq!(pending.buffered(), 1);
    }

    fn id(num: u32, seed: u8) -> [u8; 32] {
        let mut id = [seed; 32];
        id[..4].copy_from_slice(&num.to_be_bytes());
        id
    }

    fn pos(num: u32) -> BlockPos {
        BlockPos {
            num,
            id: id(num, 0),
        }
    }

    fn forked(num: u32) -> BlockPos {
        BlockPos {
            num,
            id: id(num, 9),
        }
    }

    fn nums(ring: &DeliveredRing) -> Vec<u32> {
        ring.buf.iter().map(|p| p.num).collect()
    }

    /// D1: a lower number replaces the tail it forks from.
    #[test]
    fn delivered_ring_push_and_fork_truncation() {
        let mut ring = DeliveredRing::default();
        for n in 100..110 {
            ring.push(pos(n), None);
        }
        assert_eq!(nums(&ring), (100..110).collect::<Vec<_>>());
        assert_eq!(ring.get(105), Some(id(105, 0)));
        assert_eq!(ring.get(110), None);
        assert_eq!(ring.get(99), None);

        ring.push(forked(105), None);
        assert_eq!(nums(&ring), (100..106).collect::<Vec<_>>());
        assert_eq!(ring.get(105), Some(id(105, 9)));

        ring.truncate_from(103);
        assert_eq!(nums(&ring), vec![100, 101, 102]);
        ring.truncate_from(50);
        assert_eq!(ring.len(), 0);
    }

    /// D2: entries at or below LIB are pruned, but the newest one stays.
    #[test]
    fn delivered_ring_prunes_to_lib_and_keeps_the_last_entry() {
        let mut ring = DeliveredRing::default();
        for n in 100..110 {
            ring.push(pos(n), Some(pos(104)));
        }
        assert_eq!(nums(&ring), (105..110).collect::<Vec<_>>());
        // Everything delivered is irreversible: only the newest is kept.
        let mut ring = DeliveredRing::default();
        for n in 100..110 {
            ring.push(pos(n), Some(pos(500)));
        }
        assert_eq!(nums(&ring), vec![109]);
    }

    /// D3: lib < num < below, the newest `max`, ascending.
    #[test]
    fn delivered_ring_reversible_tail_is_newest_first_capped_and_ascending() {
        let mut ring = DeliveredRing::default();
        for n in 1..=3000 {
            ring.push(pos(n), None);
        }
        let tail = ring.reversible_tail(190, 200, 1024);
        assert_eq!(
            tail.iter().map(|p| p.num).collect::<Vec<_>>(),
            (191..200).collect::<Vec<_>>()
        );
        let tail = ring.reversible_tail(0, 3001, RESUME_POSITIONS);
        assert_eq!(tail.len(), RESUME_POSITIONS);
        assert_eq!(tail.first().unwrap().num, 3001 - RESUME_POSITIONS as u32);
        assert_eq!(tail.last().unwrap().num, 3000);
        assert!(tail.windows(2).all(|w| w[0].num < w[1].num));
        assert!(ring.reversible_tail(2999, 3000, 10).is_empty());
    }

    /// D4: the ring never holds more than DELIVERED_CAP entries.
    #[test]
    fn delivered_ring_enforces_its_cap() {
        let mut ring = DeliveredRing::default();
        for n in 1..=(DELIVERED_CAP as u32 + 500) {
            ring.push(pos(n), None);
        }
        assert_eq!(ring.len(), DELIVERED_CAP);
        assert_eq!(ring.buf.front().unwrap().num, 501);
        // Lookups still work across the VecDeque's two slices.
        for n in [501, 2000, DELIVERED_CAP as u32 + 500] {
            assert_eq!(ring.get(n), Some(id(n, 0)), "block {n}");
        }
    }

    /// DU1: the de-dup rule inside a resume window.
    #[test]
    fn duplicates_are_exact_matches_misses_below_the_irreversible_mark_or_idless() {
        let mut c = ClientSide::default();
        for n in 191..200 {
            c.delivered.push(pos(n), Some(pos(190)));
        }
        c.irr_hwm = Some(190);
        c.cursor = 200;
        assert!(c.is_dup(195, Some(pos(195))), "exact (num, id)");
        assert!(
            !c.is_dup(195, Some(forked(195))),
            "a different id is a fork"
        );
        assert!(
            c.is_dup(150, Some(pos(150))),
            "not remembered, below irr_hwm"
        );
        assert!(
            c.is_dup(150, Some(forked(150))),
            "an irreversible block cannot fork"
        );
        assert!(c.is_dup(198, None), "id-less replay below the cursor");
        c.irr_hwm = None;
        assert!(
            !c.is_dup(150, Some(pos(150))),
            "unknown and above any known irreversible block: forward"
        );
    }

    fn head(v1: bool, head: u32, lib: u32, this: Option<BlockPos>) -> ResultHead {
        ResultHead {
            v1,
            head: pos(head),
            lib: pos(lib),
            this_block: this,
            prev_block: this.map(|p| pos(p.num - 1)),
            fields_at: 0,
        }
    }

    fn link_req(start: u32, end: u32, irreversible_only: bool, purpose: Purpose) -> LinkReq {
        LinkReq {
            start,
            end,
            clamped: false,
            irreversible_only,
            purpose,
            sent_at: Instant::now(),
        }
    }

    /// K1: the keep-alive rule.
    #[test]
    fn keepalive_rule_follows_springs_send_rule_on_the_frames_own_head() {
        let lr = link_req(100, 200, false, Purpose::Initial);
        // EOSIO 2.0 keep-alive: the node's cursor is above its head.
        let ka = classify_result(&head(false, 100, 90, None), UpCursor::Exact(105), &lr);
        assert_eq!(
            ka,
            Classified {
                n: None,
                advance: false,
                kind: ResultKind::KeepAlive
            }
        );
        // Spring id-less result: cursor <= head and below end, so it advances.
        let idless = classify_result(&head(false, 1000, 990, None), UpCursor::Exact(105), &lr);
        assert_eq!(
            idless,
            Classified {
                n: Some(105),
                advance: true,
                kind: ResultKind::IdLess
            }
        );
        // At or past end nothing can be sent: a keep-alive.
        let past = classify_result(&head(false, 1000, 990, None), UpCursor::Exact(200), &lr);
        assert_eq!(past.kind, ResultKind::KeepAlive);
        // Unknown upstream cursor: never advances.
        let unknown = classify_result(
            &head(false, 1000, 990, None),
            UpCursor::Uncertain { lo: 95, hi: 105 },
            &lr,
        );
        assert_eq!(unknown.kind, ResultKind::IdLessUncertain);
        assert!(!unknown.advance);
        // irreversible_only gates on the frame's LIB.
        let irr = link_req(100, 200, true, Purpose::Initial);
        let gated = classify_result(&head(false, 1000, 104, None), UpCursor::Exact(105), &irr);
        assert_eq!(gated.kind, ResultKind::KeepAlive);
        let open = classify_result(&head(false, 1000, 105, None), UpCursor::Exact(105), &irr);
        assert_eq!(open.kind, ResultKind::IdLess);
        // A block is a block, whatever the cursor.
        let block = classify_result(
            &head(false, 1000, 990, Some(pos(150))),
            UpCursor::Uncertain { lo: 1, hi: 2 },
            &lr,
        );
        assert_eq!(block.n, Some(150));
        assert_eq!(block.kind, ResultKind::Block);
    }

    fn state(role: Role, start: u32, end: u32, purpose: Purpose) -> LinkState {
        let mut st = LinkState::new(0, role, Instant::now());
        st.req = Some(link_req(start, end, false, purpose));
        st.up_cursor = UpCursor::Exact(start);
        st.first_result_pending = true;
        st
    }

    fn client_at(cursor: u32, credit: u32, v1: bool) -> ClientSide {
        let mut c = ClientSide::default();
        c.new_request(BlocksRequest {
            v1,
            start: cursor,
            end: 1000,
            max_in_flight: credit,
            ..BlocksRequest::default()
        });
        c
    }

    fn forward_and_commit(
        c: &mut ClientSide,
        st: &mut LinkState,
        h: &ResultHead,
    ) -> Option<Forward> {
        let creq = c.req.clone().unwrap();
        let lr = st.req.clone().unwrap();
        match judge(c, st, &creq, &lr, h, |_| Lacks::Nothing, |_| false) {
            Judgement::Forward(fw) => {
                assert!(!c.charge(), "unexpected overrun");
                head_lib_for(c, st.role, h.head, h.lib, None, true);
                commit(c, st, h, &fw);
                Some(fw)
            }
            _ => None,
        }
    }

    /// K2: after positions, the upstream cursor is uncertain until the first
    /// `this_block`, then exact.
    #[test]
    fn upstream_cursor_resolves_on_the_first_block_after_positions() {
        let mut c = client_at(200, 10, false);
        let mut st = state(Role::Live, 200, 1000, Purpose::Failover);
        st.up_cursor = UpCursor::Uncertain { lo: 191, hi: 200 };
        // A keep-alive while uncertain neither advances nor resolves.
        let fw = forward_and_commit(&mut c, &mut st, &head(false, 195, 185, None)).unwrap();
        assert_eq!(fw.cls.kind, ResultKind::IdLessUncertain);
        assert_eq!(st.up_cursor, UpCursor::Uncertain { lo: 191, hi: 200 });
        assert_eq!(c.cursor, 200);
        // The first block resolves it.
        forward_and_commit(&mut c, &mut st, &head(false, 205, 195, Some(pos(200)))).unwrap();
        assert_eq!(st.up_cursor, UpCursor::Exact(201));
        assert_eq!(c.cursor, 201);
    }

    /// L1: the credit ledger.
    #[test]
    fn credit_ledger_replaces_wraps_charges_and_refunds() {
        let mut c = ClientSide::default();
        c.ack(5);
        assert_eq!(c.credit, 5, "acks before any request are folded in");
        let pre = c.new_request(BlocksRequest {
            start: 100,
            end: 200,
            max_in_flight: 3,
            ..BlocksRequest::default()
        });
        assert_eq!(pre, 5);
        assert_eq!(c.credit, 3, "a request replaces C");
        c.ack(u32::MAX);
        assert_eq!(c.credit, 2, "an ack wraps like Spring's uint32_t add");

        // Every forwarded kind costs one.
        let mut st = state(Role::Live, 100, 200, Purpose::Initial);
        c.credit = 4;
        for h in [
            head(false, 1000, 990, Some(pos(100))),
            head(false, 1000, 990, None), // id-less at 101
            head(false, 50, 40, None),    // keep-alive
        ] {
            let before = c.credit;
            forward_and_commit(&mut c, &mut st, &h).unwrap();
            assert_eq!(c.credit, before - 1);
        }
        assert_eq!(c.cursor, 102);

        // A dropped duplicate leaves C unchanged (the session refunds ack(1)).
        st.resume_floor = Some(102);
        let creq = c.req.clone().unwrap();
        let lr = st.req.clone().unwrap();
        let dup = head(false, 1000, 990, Some(pos(100)));
        let before = c.credit;
        assert_eq!(
            judge(&c, &st, &creq, &lr, &dup, |_| Lacks::Nothing, |_| false),
            Judgement::Duplicate {
                advance_to: Some(101)
            }
        );
        assert_eq!(c.credit, before);

        // At C == 0: a live result is an overrun, a frozen one a violation.
        c.credit = 0;
        assert!(c.charge(), "overrun");
        assert_eq!(c.credit, 0);
        let mut frozen = state(Role::Frozen, 102, 200, Purpose::Initial);
        frozen.up_cursor = UpCursor::Exact(102);
        assert_eq!(
            judge(
                &c,
                &frozen,
                &creq,
                &lr,
                &head(false, 1000, 990, Some(pos(102))),
                |_| Lacks::Nothing,
                |_| false
            ),
            Judgement::End(UpEnd::Violation(ViolationKind::Credit))
        );
    }

    #[test]
    fn sequence_judgement_covers_forks_jumps_and_seams() {
        // A live block below the cursor outside a window is a fork.
        let mut c = client_at(100, 100, false);
        let mut st = state(Role::Live, 100, 1000, Purpose::Initial);
        for n in 100..110 {
            forward_and_commit(&mut c, &mut st, &head(false, 1000, 90, Some(pos(n)))).unwrap();
        }
        let fw =
            forward_and_commit(&mut c, &mut st, &head(false, 1000, 90, Some(forked(105)))).unwrap();
        assert!(fw.fork);
        assert_eq!(c.cursor, 106);
        assert_eq!(c.delivered.get(105), Some(forked(105).id));
        assert_eq!(c.delivered.get(106), None);

        // A forward jump on an initial request is adopted.
        let mut c = client_at(100, 100, false);
        let mut st = state(Role::Live, 100, 1000, Purpose::Initial);
        let fw =
            forward_and_commit(&mut c, &mut st, &head(false, 1000, 990, Some(pos(500)))).unwrap();
        assert!(fw.jump);
        assert_eq!(c.cursor, 501);

        // On a resume with an alternative, the jump ends the link instead.
        let c = client_at(100, 100, false);
        let st = state(Role::Live, 100, 1000, Purpose::Failover);
        let creq = c.req.clone().unwrap();
        let lr = st.req.clone().unwrap();
        let h = head(false, 1000, 990, Some(pos(500)));
        assert_eq!(
            judge(&c, &st, &creq, &lr, &h, |_| Lacks::Nothing, |k| k == 100),
            Judgement::End(UpEnd::Uncovered)
        );

        // A fresh request moved to another upstream may jump too.
        let c = client_at(100, 100, false);
        let st = state(Role::Live, 100, 1000, Purpose::Reroute);
        let creq = c.req.clone().unwrap();
        let lr = st.req.clone().unwrap();
        assert!(matches!(
            judge(&c, &st, &creq, &lr, &h, |_| Lacks::Nothing, |_| true),
            Judgement::Forward(Forward { jump: true, .. })
        ));

        // Frozen sequence and seam violations.
        let mut c = client_at(100, 100, false);
        c.last = Some(pos(99));
        let mut st = state(Role::Frozen, 100, 200, Purpose::Failover);
        st.expect_ids_from = 100;
        let lr = st.req.clone().unwrap();
        let creq = c.req.clone().unwrap();
        let end = |h: &ResultHead, st: &LinkState| {
            judge(&c, st, &creq, &lr, h, |_| Lacks::Nothing, |_| false)
        };
        assert_eq!(
            end(&head(false, 199, 199, Some(pos(101))), &st),
            Judgement::End(UpEnd::Violation(ViolationKind::Gap))
        );
        assert_eq!(
            end(&head(true, 199, 199, Some(pos(100))), &st),
            Judgement::End(UpEnd::Violation(ViolationKind::Variant))
        );
        assert_eq!(
            end(&head(false, 199, 199, None), &st),
            Judgement::End(UpEnd::Violation(ViolationKind::Idless))
        );
        let mut broken = head(false, 199, 199, Some(pos(100)));
        broken.prev_block = Some(forked(99));
        assert_eq!(
            end(&broken, &st),
            Judgement::End(UpEnd::Violation(ViolationKind::Discontinuity))
        );
        assert!(matches!(
            end(&head(false, 199, 199, Some(pos(100))), &st),
            Judgement::Forward(_)
        ));
        assert_eq!(
            judge(
                &c,
                &st,
                &creq,
                &lr,
                &head(false, 199, 199, Some(pos(100))),
                |_| Lacks::Data,
                |_| false
            ),
            Judgement::End(UpEnd::Violation(ViolationKind::MissingField))
        );
        // A missing block is no violation: the result is forwarded and
        // flagged, unless another check fails first.
        let blockless = |h: &ResultHead, st: &LinkState| {
            judge(&c, st, &creq, &lr, h, |_| Lacks::Block, |_| false)
        };
        assert!(matches!(
            blockless(&head(false, 199, 199, Some(pos(100))), &st),
            Judgement::Forward(Forward { no_block: true, .. })
        ));
        assert_eq!(
            blockless(&head(false, 199, 199, Some(pos(101))), &st),
            Judgement::End(UpEnd::Violation(ViolationKind::Gap))
        );
        st.up_cursor = UpCursor::Exact(100);
        let mut ahead = client_at(150, 100, false);
        ahead.cursor = 150;
        assert_eq!(
            judge(
                &ahead,
                &st,
                &creq,
                &lr,
                &head(false, 199, 199, Some(pos(120))),
                |_| Lacks::Nothing,
                |_| false
            ),
            Judgement::End(UpEnd::Violation(ViolationKind::Rewind))
        );
        // Below its ids a frozen upstream may answer id-less.
        st.expect_ids_from = 120;
        assert!(matches!(
            end(&head(false, 199, 199, None), &st),
            Judgement::Forward(Forward {
                cls: Classified {
                    kind: ResultKind::IdLess,
                    ..
                },
                ..
            })
        ));
    }

    /// H1: head/LIB application.
    #[test]
    fn head_and_lib_never_regress_and_frozen_frames_take_the_fleet_tip() {
        use crate::ship::wire::patch_head_lib;
        use bytes::Bytes;

        // Frozen frames are replaced by the fleet tip.
        let mut c = ClientSide::default();
        let got = head_lib_for(
            &mut c,
            Role::Frozen,
            pos(999),
            pos(999),
            Some((pos(2300), pos(2290))),
            true,
        );
        assert_eq!(got, Some((pos(2300), pos(2290))));

        // A non-regressing live frame is unchanged, and the bytes keep their
        // buffer.
        let mut frame = vec![1u8];
        frame.extend_from_slice(&2400u32.to_le_bytes());
        frame.extend_from_slice(&pos(2400).id);
        frame.extend_from_slice(&2395u32.to_le_bytes());
        frame.extend_from_slice(&pos(2395).id);
        frame.extend_from_slice(&[0, 0, 0, 0, 0]);
        let bytes = Bytes::from(frame);
        let ptr = bytes.as_ptr();
        assert_eq!(
            head_lib_for(&mut c, Role::Live, pos(2400), pos(2395), None, true),
            None
        );
        assert_eq!(bytes.as_ptr(), ptr);

        // A frame from a lagging node after a switch is clamped up.
        let got = head_lib_for(&mut c, Role::Live, pos(2000), pos(1990), None, true);
        assert_eq!(got, Some((pos(2400), pos(2395))));
        let patched = patch_head_lib(bytes, pos(2400), pos(2395));
        assert_eq!(patched.as_ptr(), ptr, "a unique frame is patched in place");

        // lib <= head always.
        let mut c = ClientSide::default();
        let got = head_lib_for(&mut c, Role::Live, pos(100), pos(100), None, true);
        assert_eq!(got, None);
        c.lib_sent = Some(pos(150));
        let got = head_lib_for(&mut c, Role::Live, pos(120), pos(110), None, true);
        assert_eq!(got, Some((pos(120), pos(120))));

        // Equal values are not patched; with patching off nothing is, but the
        // marks still move.
        let mut c = ClientSide::default();
        assert_eq!(
            head_lib_for(&mut c, Role::Live, pos(10), pos(5), None, true),
            None
        );
        assert_eq!(
            head_lib_for(
                &mut c,
                Role::Frozen,
                pos(3),
                pos(3),
                Some((pos(20), pos(15))),
                false
            ),
            None
        );
        assert_eq!(c.head_sent, Some(pos(20)));
        assert_eq!(c.lib_sent, Some(pos(15)));
    }

    #[test]
    fn empty_requests_can_never_send() {
        let r = |start, end, positions: Vec<u32>| BlocksRequest {
            start,
            end,
            have_positions: positions.into_iter().map(pos).collect(),
            ..BlocksRequest::default()
        };
        assert!(request_is_empty(&r(100, 100, vec![])));
        assert!(request_is_empty(&r(100, 0, vec![])));
        assert!(request_is_empty(&r(100, 50, vec![60])));
        assert!(!request_is_empty(&r(100, 50, vec![40])));
        assert!(!request_is_empty(&r(100, 101, vec![])));
    }
}
