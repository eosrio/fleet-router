//! L2: a lockstep property test of the session's pure helpers.
//!
//! A simulated client talks to a simulated router, built from the helpers
//! the session actor uses (`judge`, `commit`, `head_lib_for`,
//! `build_request`, `quiescent` and the credit ledger), in front of simulated
//! upstreams that follow their own send rules:
//! - Spring: sends while `credit > 0 && cursor <= gate && cursor < end`,
//!   rewinds on forks and on unknown `have_positions`, and may lag the chain;
//! - EOSIO 2.0: Spring's rule plus head keep-alives that cost a credit and do
//!   not advance;
//! - Leap 4: starts at its first available block (a forward jump);
//! - frozen segments: a fixed range with every id known, below the fleet LIB.
//!
//! Random acks, pauses, new requests, chain growth, forks, drops, Close
//! frames, handoffs at segment ends and failovers interleave. Messages from
//! the router reach the upstream at once; results travel through a queue, so
//! they can be in flight across acks, forks and link ends.
//!
//! Checked after every step:
//! - (a) no result reaches the client beyond its Spring window;
//! - (b) the delivered `(num, id)` sequence is what one node would send: each
//!   block follows the previous one, except fork re-deliveries (a new id for
//!   a delivered number), keep-alives and a fresh request's first-available
//!   jump, and nothing already delivered is delivered again;
//! - (c) the upstream's credit plus the results in flight equals the router's
//!   C, so U <= C;
//! - (d) every request starts at the client's next block, and the router's
//!   cursor equals it whenever the upstream's cursor is known.

use std::collections::{BTreeMap, VecDeque};

use tokio::time::Instant;

use crate::models::{ResolvedTopology, Role};
use crate::ship::wire::{BlocksRequest, ResultHead};
use crate::ship::BlockPos;

use super::client::{commit, head_lib_for, judge, ClientSide, Judgement, ResultKind};
use super::failover::UpEnd;
use super::link::{build_request, quiescent, LinkState, Purpose, Target, UpCursor};

/// Frozen segments `[begin, end)`, back to back from block 1.
const FROZEN: [(u32, u32); 2] = [(1, 400), (400, 800)];
/// Where the frozen tier ends and live upstreams take over.
const HISTORY_END: u32 = 800;
/// First block a Spring or EOSIO 2.0 node knows.
const LIVE_BEGIN: u32 = 700;
/// First block a Leap 4 node knows (and jumps to).
const LEAP4_FIRST: u32 = 900;
const STEPS: usize = 20_000;

/// xorshift64*: deterministic, no dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }

    /// Uniform in `lo..=hi`.
    fn range(&mut self, lo: u32, hi: u32) -> u32 {
        lo + self.below(u64::from(hi - lo) + 1) as u32
    }
}

/// The chain every simulated upstream follows. Forks happen above LIB and
/// change the ids of every block from their start.
struct Chain {
    head: u32,
    lib: u32,
    forks: Vec<(u32, u8)>,
}

impl Chain {
    fn id(&self, n: u32) -> [u8; 32] {
        let seed = self
            .forks
            .iter()
            .rev()
            .find(|(from, _)| *from <= n)
            .map_or(0, |(_, seed)| *seed);
        let mut id = [seed; 32];
        id[..4].copy_from_slice(&n.to_be_bytes());
        id
    }

    fn pos(&self, n: u32) -> BlockPos {
        BlockPos {
            num: n,
            id: self.id(n),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// A live node `lag` blocks behind the chain head.
    Spring {
        lag: u32,
    },
    Eosio2 {
        lag: u32,
    },
    Leap4 {
        lag: u32,
    },
    Frozen {
        begin: u32,
        end: u32,
    },
}

impl Kind {
    fn role(self) -> Role {
        match self {
            Kind::Frozen { .. } => Role::Frozen,
            _ => Role::Live,
        }
    }

    fn lag(self) -> u32 {
        match self {
            Kind::Spring { lag } | Kind::Eosio2 { lag } | Kind::Leap4 { lag } => lag,
            Kind::Frozen { .. } => 0,
        }
    }

    /// First block whose id it knows.
    fn ids_begin(self) -> u32 {
        match self {
            Kind::Spring { .. } | Kind::Eosio2 { .. } => LIVE_BEGIN,
            Kind::Leap4 { .. } => LEAP4_FIRST,
            Kind::Frozen { begin, .. } => begin,
        }
    }

    /// Its own head and LIB numbers.
    fn tip(self, chain: &Chain) -> (u32, u32) {
        match self {
            Kind::Frozen { end, .. } => (end - 1, end - 1),
            live => {
                let head = chain.head - live.lag();
                (head, chain.lib.min(head))
            }
        }
    }

    fn knows(self, chain: &Chain, n: u32) -> bool {
        n >= self.ids_begin()
            && match self {
                Kind::Frozen { end, .. } => n < end,
                live => n <= live.tip(chain).0,
            }
    }
}

/// One result on the wire, and the block the upstream's cursor advanced
/// from when it sent it (`None` for a keep-alive).
struct Frame {
    h: ResultHead,
    advanced_from: Option<u32>,
}

struct Upstream {
    kind: Kind,
    /// `(end, irreversible_only)` of the request it serves.
    req: Option<(u32, bool)>,
    cursor: u32,
    credit: u32,
    keepalive_due: bool,
    wire: VecDeque<Frame>,
}

impl Upstream {
    fn new(kind: Kind) -> Self {
        Upstream {
            kind,
            req: None,
            cursor: 0,
            credit: 0,
            keepalive_due: false,
            wire: VecDeque::new(),
        }
    }

    /// Spring's get_blocks: a position below start whose id is unknown or
    /// different moves start down to it; Leap 4 starts at its first block.
    fn request(&mut self, r: &BlocksRequest, chain: &Chain) {
        let mut start = r.start;
        for p in &r.have_positions {
            if p.num < start && (!self.kind.knows(chain, p.num) || chain.id(p.num) != p.id) {
                start = p.num;
            }
        }
        if let Kind::Leap4 { .. } = self.kind {
            start = start.max(LEAP4_FIRST);
        }
        self.cursor = start;
        self.credit = r.max_in_flight;
        self.req = Some((r.end, r.irreversible_only));
        self.keepalive_due = true;
    }

    fn head_lib(&self, chain: &Chain) -> (BlockPos, BlockPos) {
        let (head, lib) = self.kind.tip(chain);
        (chain.pos(head), chain.pos(lib))
    }

    /// Send one result when the node's own rule allows it.
    fn send(&mut self, chain: &Chain) -> bool {
        let Some((end, irreversible_only)) = self.req else {
            return false;
        };
        if self.credit == 0 {
            return false;
        }
        let (head, lib) = self.head_lib(chain);
        let gate = if irreversible_only { lib.num } else { head.num };
        if self.cursor <= gate && self.cursor < end {
            let n = self.cursor;
            let known = self.kind.knows(chain, n);
            let h = ResultHead {
                v1: false,
                head,
                lib,
                this_block: known.then(|| chain.pos(n)),
                prev_block: (known && n > 1).then(|| chain.pos(n - 1)),
                fields_at: 0,
            };
            self.wire.push_back(Frame {
                h,
                advanced_from: Some(n),
            });
            self.cursor += 1;
            self.credit -= 1;
            self.keepalive_due = false;
            return true;
        }
        if matches!(self.kind, Kind::Eosio2 { .. }) && self.keepalive_due {
            let h = ResultHead {
                v1: false,
                head,
                lib,
                this_block: None,
                prev_block: None,
                fields_at: 0,
            };
            self.wire.push_back(Frame {
                h,
                advanced_from: None,
            });
            self.credit -= 1;
            self.keepalive_due = false;
            return true;
        }
        false
    }
}

/// What the client has: its Spring window, its next block and the blocks
/// it was given for the current request.
#[derive(Default)]
struct ClientModel {
    window: u32,
    next: u32,
    delivered: BTreeMap<u32, [u8; 32]>,
    since_request: u64,
    /// The lowest fork point since the request: a fork rewinds a Spring
    /// session there, even below the request's start.
    fork_low: Option<u32>,
}

impl ClientModel {
    fn request(&mut self, r: &BlocksRequest) {
        self.window = r.max_in_flight;
        self.next = r.start;
        self.delivered.clear();
        self.since_request = 0;
        self.fork_low = None;
    }

    /// Check (a) and (b) for one delivered result.
    fn deliver(&mut self, f: &Frame, jump_ok: bool, step: usize) {
        assert!(
            self.window > 0,
            "step {step}: (a) a result beyond the client's window"
        );
        self.window -= 1;
        match f.h.this_block {
            Some(p) => {
                if p.num > self.next {
                    assert!(
                        jump_ok && self.since_request == 0,
                        "step {step}: (b) gap: expected {}, got {}",
                        self.next,
                        p.num
                    );
                } else if p.num < self.next {
                    let had = self.delivered.get(&p.num);
                    let rewound = self.fork_low.is_some_and(|low| p.num >= low);
                    assert!(
                        had.map_or(rewound, |id| *id != p.id),
                        "step {step}: (b) block {} delivered again (next {}, had {:?})",
                        p.num,
                        self.next,
                        had.map(|id| id[4])
                    );
                }
                self.delivered.split_off(&p.num);
                self.delivered.insert(p.num, p.id);
                self.next = p.num + 1;
            }
            None => {
                if let Some(n) = f.advanced_from {
                    assert_eq!(
                        n, self.next,
                        "step {step}: (b) id-less result off the cursor"
                    );
                    self.next += 1;
                }
            }
        }
        self.since_request += 1;
    }
}

/// The router's side: the session's mirror, its link and the upstream.
struct Sim {
    rng: Rng,
    chain: Chain,
    c: ClientSide,
    client: ClientModel,
    link: Option<(LinkState, Upstream)>,
    next_seed: u8,
    stats: Stats,
}

#[derive(Default, Debug)]
struct Stats {
    forwarded: u64,
    keepalives: u64,
    idless: u64,
    duplicates: u64,
    forks: u64,
    jumps: u64,
    handoffs: u64,
    failovers: u64,
    reuses: u64,
    patched: u64,
}

impl Sim {
    fn new(seed: u64) -> Self {
        Sim {
            rng: Rng(seed),
            chain: Chain {
                head: 1500,
                lib: 1490,
                forks: Vec::new(),
            },
            c: ClientSide::default(),
            client: ClientModel::default(),
            link: None,
            next_seed: 1,
            stats: Stats::default(),
        }
    }

    fn live_kind(&mut self, from: u32, fresh: bool) -> Kind {
        let lag = if self.rng.chance(30) {
            self.rng.range(1, 5)
        } else {
            0
        };
        // A Leap 4 node jumps when asked below its first block: fine for a
        // fresh request, never for a resume.
        let leap4_ok = fresh || from >= LEAP4_FIRST;
        match self.rng.below(if leap4_ok { 3 } else { 2 }) {
            0 => Kind::Spring { lag },
            1 => Kind::Eosio2 { lag },
            _ => Kind::Leap4 { lag },
        }
    }

    /// Which upstream serves the client's cursor, and the request end.
    fn plan(&mut self, from: u32, fresh: bool) -> (Kind, u32) {
        let creq_end = self.c.req.as_ref().map_or(0, |r| r.end);
        if from < HISTORY_END {
            let (begin, end) = if from < FROZEN[0].1 {
                FROZEN[0]
            } else {
                FROZEN[1]
            };
            return (Kind::Frozen { begin, end }, end.min(creq_end));
        }
        (self.live_kind(from, fresh), creq_end)
    }

    /// Open a link to `kind` and send it the request built from the mirror.
    fn open(&mut self, kind: Kind, end: u32, purpose: Purpose, step: usize) {
        let creq = self.c.req.clone().expect("a request");
        let mut st = LinkState::new(0, kind.role(), Instant::now());
        let mut up = Upstream::new(kind);
        self.send_request(&creq, &mut st, &mut up, end, purpose, step);
        self.link = Some((st, up));
    }

    fn send_request(
        &mut self,
        creq: &BlocksRequest,
        st: &mut LinkState,
        up: &mut Upstream,
        end: u32,
        purpose: Purpose,
        step: usize,
    ) {
        let target = Target {
            topology: ResolvedTopology::Pool,
            ids_begin: Some(up.kind.ids_begin()),
        };
        let req = build_request(&self.c, creq, st, end, purpose, target, Instant::now());
        assert_eq!(
            req.start, self.client.next,
            "step {step}: (d) a request starts off the client's next block"
        );
        assert_eq!(req.max_in_flight, self.c.credit);
        if up.kind.role() == Role::Frozen {
            assert!(req.have_positions.is_empty());
        }
        up.request(&req, &self.chain);
    }

    fn new_request(&mut self, step: usize) {
        let start = if self.rng.chance(5) {
            0 // below the fleet's first block: id-less results
        } else if self.rng.chance(35) {
            self.rng.range(1, HISTORY_END - 1)
        } else if self.rng.chance(50) {
            // At the tip: reversible blocks, keep-alives, forks, lagging nodes.
            self.rng.range(self.chain.head - 20, self.chain.head + 3)
        } else {
            self.rng.range(LIVE_BEGIN, self.chain.head + 5)
        };
        let end = if self.rng.chance(30) {
            u32::MAX
        } else {
            start + self.rng.range(1, 300)
        };
        let r = BlocksRequest {
            start,
            end,
            max_in_flight: self.rng.range(0, 30),
            irreversible_only: self.rng.chance(10),
            ..BlocksRequest::default()
        };
        let pre = self.c.new_request(r.clone());
        self.client.request(&r);
        let (kind, run_end) = self.plan(start, true);
        if let Some((mut st, mut up)) = self.link.take() {
            st.resume_floor = None;
            let legacy = matches!(up.kind, Kind::Eosio2 { .. });
            if up.kind.role() == kind.role()
                && (kind.role() == Role::Live || up.kind == kind)
                && quiescent(&st, pre, legacy)
            {
                // Nothing of the old request can still arrive.
                assert!(
                    up.wire.is_empty(),
                    "step {step}: a quiescent link has results in flight"
                );
                self.stats.reuses += 1;
                self.send_request(&r, &mut st, &mut up, run_end, Purpose::Initial, step);
                self.link = Some((st, up));
                return;
            }
        }
        self.open(kind, run_end, Purpose::Initial, step);
    }

    fn ack(&mut self) {
        let n = self.rng.range(1, 5);
        self.c.ack(n);
        self.client.window += n;
        if let Some((st, up)) = self.link.as_mut() {
            if st.req.is_some() {
                up.credit = up.credit.wrapping_add(n);
            }
        }
    }

    /// The link ended (a drop, a Close or a stall): resume elsewhere at the
    /// client's cursor with its outstanding credit.
    fn failover(&mut self, step: usize) {
        if self.link.take().is_none() || !self.c.in_progress() {
            return;
        }
        self.stats.failovers += 1;
        let fresh = self.c.delivered_for_request == 0;
        let (kind, end) = self.plan(self.c.cursor, fresh);
        self.open(kind, end, Purpose::Failover, step);
    }

    fn advance(&mut self) {
        self.chain.head += self.rng.range(1, 3);
        self.chain.lib = self.chain.head - 10;
        if let Some((_, up)) = self.link.as_mut() {
            if up.req.is_some() {
                up.keepalive_due = true;
            }
        }
    }

    fn fork(&mut self) {
        let from = self.rng.range(self.chain.lib + 1, self.chain.head);
        self.chain.forks.push((from, self.next_seed));
        self.client.fork_low = Some(self.client.fork_low.map_or(from, |low| low.min(from)));
        self.next_seed = self.next_seed.wrapping_add(1).max(1);
        if let Some((_, up)) = self.link.as_mut() {
            // block_applied rewinds a live session past the fork point.
            if up.kind.role() == Role::Live && up.req.is_some() && up.cursor > from {
                up.cursor = from;
            }
        }
    }

    /// The router reads one result off the wire.
    fn process(&mut self, step: usize) {
        let Some((st, up)) = self.link.as_mut() else {
            return;
        };
        let Some(f) = up.wire.pop_front() else {
            return;
        };
        let creq = self.c.req.clone().expect("a request");
        let lr = st.req.clone().expect("a link request");
        let judgement = judge(&self.c, st, &creq, &lr, &f.h, |_| false, |_| true);
        match judgement {
            Judgement::End(UpEnd::Uncovered) => {
                self.failover(step);
            }
            Judgement::End(end) => panic!("step {step}: honest upstream ended as {end:?}"),
            Judgement::Duplicate { advance_to } => {
                if let Some(next) = advance_to {
                    st.up_cursor = UpCursor::Exact(next);
                }
                // The refund ack reaches the upstream at once.
                up.credit = up.credit.wrapping_add(1);
                self.stats.duplicates += 1;
            }
            Judgement::Forward(fw) => {
                assert!(!self.c.charge(), "step {step}: (a) router credit overrun");
                let fleet = Some((
                    self.chain.pos(self.chain.head),
                    self.chain.pos(self.chain.lib),
                ));
                let patched = head_lib_for(&mut self.c, st.role, f.h.head, f.h.lib, fleet, true);
                if let Some((head, lib)) = patched {
                    self.stats.patched += 1;
                    assert!(lib.num <= head.num);
                }
                let jump_ok = matches!(up.kind, Kind::Leap4 { .. }) && fw.jump;
                self.client.deliver(&f, jump_ok, step);
                commit(&mut self.c, st, &f.h, &fw);
                self.stats.forwarded += 1;
                match fw.cls.kind {
                    ResultKind::KeepAlive => self.stats.keepalives += 1,
                    ResultKind::IdLess => self.stats.idless += 1,
                    _ => {}
                }
                self.stats.forks += u64::from(fw.fork);
                self.stats.jumps += u64::from(fw.jump);
                if let UpCursor::Exact(_) = st.up_cursor {
                    assert_eq!(
                        self.c.cursor, self.client.next,
                        "step {step}: (d) the router's cursor left the client's"
                    );
                }
                // A segment end: hand off to the next upstream at the cursor.
                if self.c.in_progress() && lr.clamped && self.c.cursor >= lr.end {
                    self.stats.handoffs += 1;
                    self.link = None;
                    let (kind, end) = self.plan(self.c.cursor, false);
                    self.open(kind, end, Purpose::Handoff, step);
                }
            }
        }
    }

    /// Check (c): U plus the results in flight equals C.
    fn check_ledger(&self, step: usize) {
        assert_eq!(
            self.c.credit, self.client.window,
            "step {step}: the router's C differs from the client's window"
        );
        if let Some((st, up)) = &self.link {
            if st.req.is_some() {
                let in_flight = up.wire.len() as u64;
                assert_eq!(
                    u64::from(up.credit) + in_flight,
                    u64::from(self.c.credit),
                    "step {step}: (c) U + in flight != C"
                );
            }
        }
    }

    fn run(&mut self) {
        self.new_request(0);
        for step in 1..=STEPS {
            match self.rng.below(100) {
                0..=29 => {
                    if let Some((_, up)) = self.link.as_mut() {
                        up.send(&self.chain);
                    }
                }
                30..=59 => self.process(step),
                60..=71 => self.ack(),
                72..=74 => self.new_request(step),
                75..=82 => self.advance(),
                83 => self.fork(),
                84..=86 => self.failover(step),
                _ => {}
            }
            self.check_ledger(step);
        }
    }
}

#[test]
fn credit_and_cursor_stay_in_lockstep_with_simulated_upstreams() {
    let mut total = Stats::default();
    for seed in [
        0x9e37_79b9_7f4a_7c15,
        0x0123_4567_89ab_cdef,
        42,
        7,
        0xdead_beef,
    ] {
        let mut sim = Sim::new(seed);
        sim.run();
        let s = sim.stats;
        total.forwarded += s.forwarded;
        total.keepalives += s.keepalives;
        total.idless += s.idless;
        total.duplicates += s.duplicates;
        total.forks += s.forks;
        total.jumps += s.jumps;
        total.handoffs += s.handoffs;
        total.failovers += s.failovers;
        total.reuses += s.reuses;
        total.patched += s.patched;
    }
    // The runs must actually exercise every path they claim to check.
    assert!(total.forwarded > 10_000, "{total:?}");
    for (what, n) in [
        ("keep-alives", total.keepalives),
        ("id-less results", total.idless),
        ("duplicates", total.duplicates),
        ("fork re-deliveries", total.forks),
        ("first-available jumps", total.jumps),
        ("handoffs", total.handoffs),
        ("failovers", total.failovers),
        ("socket reuses", total.reuses),
        ("patched head/LIB", total.patched),
    ] {
        assert!(n > 0, "no {what} in {total:?}");
    }
}
