//! Pure routing decisions over a fleet view: which upstream may serve a
//! request from a given block, where its run ends, and when a session must
//! hold or wait instead.
//!
//! Eligibility follows the virtual-node union rule: for each field the client
//! asked for, if any upstream in the union has that log at block `n`, the
//! serving upstream must have it too. Live upstreams are open-ended above
//! their begin; frozen ones cover exactly their range.

use std::cmp::Ordering as CmpOrdering;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::abi::fnv1a64;
use crate::models::{HistoryPreference, Role, StitchingPolicy};
use crate::ship::{wire, BlockPos};

use super::view::{FleetView, Log, UpstreamView};

/// The data a request asks for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Need {
    pub traces: bool,
    pub deltas: bool,
    /// Only a v1 request can ask for finality data.
    pub finality: bool,
    pub block: bool,
}

impl Need {
    pub fn of(r: &wire::BlocksRequest) -> Need {
        Need {
            traces: r.fetch_traces,
            deltas: r.fetch_deltas,
            finality: r.v1 && r.fetch_finality_data,
            block: r.fetch_block,
        }
    }
}

/// What the planner needs to know about a client request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shape {
    pub need: Need,
    /// Exclusive end block of the client request.
    pub end: u32,
    pub irreversible_only: bool,
}

impl Shape {
    pub fn of(r: &wire::BlocksRequest) -> Shape {
        Shape {
            need: Need::of(r),
            end: r.end,
            irreversible_only: r.irreversible_only,
        }
    }
}

/// Which upstreams define "the union" for eligibility.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Union {
    /// Sticky coverage: every enabled known upstream, online or not.
    Sticky,
    /// Online upstreams only (pool topology).
    Online,
}

/// Upstreams a session avoids. Per session.
#[derive(Clone, Debug, Default)]
pub struct Excludes {
    /// Prefer another upstream of the same rank (the one that just failed).
    pub soft: Option<Arc<str>>,
    /// Never pick these until their deadline.
    pub hard: Vec<(Arc<str>, Instant)>,
}

impl Excludes {
    pub fn hard_active(&self, endpoint: &str, now: Instant) -> bool {
        self.hard
            .iter()
            .any(|(ep, until)| &**ep == endpoint && now < *until)
    }

    /// Exclude `endpoint` for `ttl`, replacing an earlier exclusion of it and
    /// dropping expired ones.
    pub fn add_hard(&mut self, endpoint: Arc<str>, ttl: Duration, now: Instant) {
        self.hard
            .retain(|(ep, until)| now < *until && **ep != *endpoint);
        let until = now
            .checked_add(ttl)
            .unwrap_or_else(|| now + Duration::from_secs(86_400 * 365));
        self.hard.push((endpoint, until));
    }
}

/// Serve `[start, req_end)` from upstream `up`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServeStep {
    /// `ServerState::index` of the upstream.
    pub up: usize,
    pub endpoint: Arc<str>,
    pub role: Role,
    pub start: u32,
    /// Always above `start`.
    pub req_end: u32,
}

metric_label! {
    /// Why a session holds instead of serving.
    pub enum HoldWhy {
        /// The covering upstream is offline.
        Offline => "offline",
        /// Every covering upstream is quarantined at this block.
        Quarantined => "quarantined",
        /// The best covering upstream is at `max_sessions`.
        AtCapacity => "at_capacity",
        /// The covering upstreams answered 503 recently.
        Busy => "busy",
    }
}

/// What a session should do for its next block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    Serve(ServeStep),
    /// Retry with backoff. `bounded` holds end after `gap_wait_ms` with 1011.
    Hold {
        block: u32,
        why: HoldWhy,
        bounded: bool,
    },
    /// Above the fleet head/LIB gate, or at the top of an archive-only fleet:
    /// wait for the view to change, unbounded.
    Wait {
        block: u32,
    },
    /// A configured hole: no upstream ever covered `block`. Close with 1011.
    Gap {
        block: u32,
    },
    /// `from >= end`.
    Done,
}

/// Deterministic tie-break: `fnv1a64(session_id.to_le_bytes() ++ endpoint)`.
pub fn rendezvous(session_id: u64, endpoint: &str) -> u64 {
    let mut bytes = Vec::with_capacity(8 + endpoint.len());
    bytes.extend_from_slice(&session_id.to_le_bytes());
    bytes.extend_from_slice(endpoint.as_bytes());
    fnv1a64(&bytes)
}

/// Whether `u` may serve block `n` of a request that needs `need`.
pub fn eligible(
    v: &FleetView,
    u: &UpstreamView,
    need: Need,
    n: u32,
    union: Union,
    now: Instant,
) -> bool {
    eligible_with(v, u, need, n, union, now, true)
}

/// [`eligible`] without the quarantine clause (`pool_pick` filters
/// quarantine itself).
pub fn eligible_ignoring_quarantine(
    v: &FleetView,
    u: &UpstreamView,
    need: Need,
    n: u32,
    union: Union,
    now: Instant,
) -> bool {
    eligible_with(v, u, need, n, union, now, false)
}

fn eligible_with(
    v: &FleetView,
    u: &UpstreamView,
    need: Need,
    n: u32,
    union: Union,
    now: Instant,
    check_quarantine: bool,
) -> bool {
    if !u.routable() || !u.known || !u.abi_ok {
        return false;
    }
    if check_quarantine && u.quarantined_at(n, now) {
        return false;
    }
    let has = |log: Log| match union {
        Union::Sticky => v.cov(log).has(n),
        Union::Online => v
            .upstreams
            .iter()
            .any(|w| w.routable() && w.online && w.covers(log, n)),
    };
    if need.traces && has(Log::Trace) && !u.covers(Log::Trace, n) {
        return false;
    }
    if need.deltas && has(Log::ChainState) && !u.covers(Log::ChainState, n) {
        return false;
    }
    if need.finality && has(Log::Finality) && !u.covers(Log::Finality, n) {
        return false;
    }
    // this_block / prev_block / block fidelity: whoever the union knows ids
    // for must know them too.
    if (has(Log::Trace) || has(Log::ChainState) || has(Log::Finality)) && !u.covers_ids(n) {
        return false;
    }
    true
}

/// Operator preference between two upstreams that serve a request equally
/// well: the lower `priority` tier first, then fewer connections per unit of
/// `weight` (weighted least-connections), then the higher weight. `Less`
/// means `a` is preferred. `a_conns` and `b_conns` are the connections that
/// count for each.
///
/// The load is compared as `a_conns * b.weight` against `b_conns * a.weight`,
/// without dividing, so it is exact.
fn preference(a: &UpstreamView, a_conns: usize, b: &UpstreamView, b_conns: usize) -> CmpOrdering {
    let (a_weight, b_weight) = (a.weight.max(1), b.weight.max(1));
    let load = |conns: usize, weight: u32| (conns as u128) * u128::from(weight);
    a.priority
        .cmp(&b.priority)
        .then_with(|| load(a_conns, b_weight).cmp(&load(b_conns, a_weight)))
        .then_with(|| b_weight.cmp(&a_weight))
}

/// Whether `u` can take over a session whose next block is `next_block`, for
/// an operator move or drain: routable, online and usable, below
/// `max_sessions`, not quarantined at the block, and its trace range starts
/// at or before the block. A live range that ends earlier still qualifies (a
/// session at the tip waits for the node to advance, as on any failover); a
/// frozen one must contain the block. A node whose range starts after the
/// block (a pruned node, for a reader catching up on old history) does not.
/// `None` (no block requested yet) accepts any such upstream.
///
/// A full or quarantined upstream is skipped by the failover that follows
/// the move as well: counting it would close the client (or hold it) instead
/// of leaving it where it is.
pub fn can_take_over(u: &UpstreamView, next_block: Option<u32>) -> bool {
    let now = Instant::now();
    u.routable()
        && u.online
        && u.known
        && u.abi_ok
        && !u.at_capacity()
        && next_block.is_none_or(|block| {
            !u.quarantined_at(block, now)
                && match u.role {
                    Role::Live => u.covers(Log::Trace, block),
                    Role::Frozen => u.covers_ids(block),
                }
        })
}

/// A live upstream more than `lag` blocks behind the fleet head, asked for a
/// block above its own head.
fn lagging_for(v: &FleetView, u: &UpstreamView, n: u32, lag: u32) -> bool {
    lag > 0
        && u.role == Role::Live
        && matches!((u.head, v.head), (Some(h), Some(f)) if h.num.saturating_add(lag) < f.num && n > h.num)
}

/// Every block where eligibility can change for `u`: the begins and ends of
/// every enabled known upstream's logs (live ends are open, so excluded),
/// `u`'s active quarantine bounds, and the fleet's first id.
fn breakpoints(v: &FleetView, u: &UpstreamView, now: Instant) -> Vec<u32> {
    let mut out = Vec::new();
    for w in v.upstreams.iter().filter(|w| w.enabled && w.known) {
        for log in Log::ALL {
            if !w.has_log(log) {
                continue;
            }
            let r = w.range(log);
            out.push(r.begin);
            if w.role == Role::Frozen {
                out.push(r.end);
            }
        }
    }
    for q in u.quarantines.iter().filter(|q| q.active(now)) {
        out.push(q.block);
        if let Some(end) = q.block.checked_add(q.range) {
            out.push(end);
        }
    }
    if let Some(first) = v.ids.components.first() {
        out.push(first.begin);
    }
    out
}

/// The first block above `from` that `u` cannot serve under the sticky
/// union: a range end or begin anywhere in the fleet, a quarantine bound, or
/// (frozen) its own end. Eligibility is constant between breakpoints, so the
/// run stops exactly where the field set changes in a way `u` cannot serve.
///
/// A frozen upstream's run never goes past its own end (the highest end of
/// its logs), even where nothing in the fleet covers the blocks after it: it
/// has nothing to send there, so a request past it would only go silent.
pub fn run_end(v: &FleetView, u: &UpstreamView, need: Need, from: u32, now: Instant) -> u32 {
    let cap = match u.role {
        Role::Frozen => u.max_end(),
        Role::Live => u32::MAX,
    };
    let mut bps: Vec<u32> = breakpoints(v, u, now)
        .into_iter()
        .filter(|&b| b > from)
        .collect();
    bps.sort_unstable();
    bps.dedup();
    for b in bps {
        if b >= cap {
            return cap;
        }
        if !eligible(v, u, need, b, Union::Sticky, now) {
            return b;
        }
    }
    cap
}

/// True when every upstream that could serve `from` is live (the tip
/// region): holds there are unbounded.
pub fn tip_region(v: &FleetView, from: u32) -> bool {
    let mut any = false;
    for u in v
        .upstreams
        .iter()
        .filter(|u| u.enabled && u.known && u.covers_ids(from))
    {
        if u.role == Role::Frozen {
            return false;
        }
        any = true;
    }
    any
}

/// The next step of a stitched session whose cursor is `from`.
///
/// `tip` is `hub.fresh_tip(v)`. A frozen upstream's request is clamped to its
/// run end and to the gate + 1 (fleet head, or fleet LIB for
/// `irreversible_only`). A gate below `from` gives [`Step::Wait`], never an
/// empty request; it is checked before capacity, so a session waiting for the
/// fleet to reach `from` waits unbounded even while the archive is full.
/// `own` lists the upstream (by index) of every connection slot the session
/// holds itself: its link, dial, standby link and pre-dial. Serving from such
/// an upstream reuses or replaces that slot, so it does not count against
/// the upstream's `max_sessions`. `allow_offline` lets a bounded hold's retry
/// dial an offline but enabled covering upstream.
///
/// While an enabled frozen upstream has neither reported nor declared a
/// range, it may hold blocks below the known fleet begin or inside a hole:
/// those hold (bounded, as offline) instead of being served id-less by
/// another archive or closed as a gap.
///
/// Candidates are ranked by: online, fresh and not lagging, under capacity,
/// role preference, not soft-excluded, lower `priority` tier, the longest
/// run, fewer connections per unit of `weight` (weighted least-connections),
/// higher weight, then the rendezvous hash. Priority and weight come after
/// role preference and capacity, so neither pulls history reads onto a live
/// node or a session onto a full upstream, and after the soft exclusion, so
/// a failover leaves a failed upstream for a peer in any tier. Priority comes
/// before the longest run so that an operator's tiers hold across upstreams
/// whose ranges differ. Load comes after it, so with the default priority
/// and weight the order is the plain longest run, then least connections:
/// a busier upstream with a longer run still saves a handoff, and weights
/// split sessions among upstreams whose runs are equal (replicas). The
/// session's own slots do not count as load on their upstream, so a session
/// is not pushed off the upstream it already holds by its own connection.
#[allow(clippy::too_many_arguments)]
pub fn next_step(
    v: &FleetView,
    tip: (Option<BlockPos>, Option<BlockPos>),
    sh: &Shape,
    from: u32,
    ex: &Excludes,
    own: &[usize],
    sid: u64,
    pol: &StitchingPolicy,
    lag: u32,
    allow_offline: bool,
    now: Instant,
) -> Step {
    if from >= sh.end {
        return Step::Done;
    }
    let ids_begin = v.ids.components.first().map(|c| c.begin);
    let below = ids_begin.is_some_and(|b| from < b);
    let unknown_hold = Step::Hold {
        block: from,
        why: HoldWhy::Offline,
        bounded: true,
    };
    if !below && !v.ids.has(from) {
        return if ids_begin.is_none() || (from >= v.ids.top.end && v.ids.live_tail_begin.is_none())
        {
            // Nothing known yet, or the top of an archive-only fleet.
            Step::Wait { block: from }
        } else if v.frozen_unknown > 0 {
            // The archive that has not reported may cover the hole.
            unknown_hold
        } else {
            // A configured hole: ids exist below and above `from`.
            Step::Gap { block: from }
        };
    }
    if below && v.frozen_unknown > 0 {
        // The archive that has not reported may hold these blocks: never
        // deliver them id-less from the owner of the known fleet begin.
        return unknown_hold;
    }
    let mine = |u: &UpstreamView| own.iter().filter(|&&i| i == u.index).count();
    let full = |u: &UpstreamView| u.at_capacity_besides(mine(u));
    let cands: Vec<&UpstreamView> = v
        .upstreams
        .iter()
        .filter(|u| !ex.hard_active(&u.endpoint, now))
        .filter(|u| {
            if below {
                // Below fleet begin only the owner of fleet begin serves; it
                // answers with id-less results until its own begin.
                u.routable() && u.known && u.abi_ok && u.ids_begin() == ids_begin
            } else {
                eligible(v, u, sh.need, from, Union::Sticky, now)
            }
        })
        .collect();
    if cands.is_empty() {
        let why = if v.upstreams.iter().any(|u| ex.hard_active(&u.endpoint, now)) {
            HoldWhy::Busy
        } else if v
            .upstreams
            .iter()
            .any(|u| u.enabled && u.operator_disabled && u.covers_ids(from))
        {
            // An operator took the covering upstream out of service: as if
            // it were offline.
            HoldWhy::Offline
        } else {
            HoldWhy::Quarantined
        };
        return Step::Hold {
            block: from,
            why,
            bounded: !tip_region(v, from),
        };
    }
    // Each candidate's keys are read once, so one ranking sees one
    // connection count per upstream.
    let ranked: Vec<Ranked<'_>> = cands
        .iter()
        .map(|&u| Ranked {
            up: u,
            head: [
                u8::from(!u.online),
                u8::from(u.stale || lagging_for(v, u, from, lag)),
                u8::from(full(u)),
                match (pol.history_preference, u.role) {
                    (HistoryPreference::Frozen, Role::Live) => 1,
                    _ => 0,
                },
                u8::from(ex.soft.as_deref() == Some(&*u.endpoint)),
            ],
            conns: u
                .connections
                .load(Ordering::Relaxed)
                .saturating_sub(mine(u)),
            run: run_end(v, u, sh.need, from, now).min(sh.end),
            hash: rendezvous(sid, &u.endpoint),
        })
        .collect();
    let chosen = ranked
        .iter()
        .min_by(|a, b| {
            // `preference` compares priority again; it is equal by then.
            a.head
                .cmp(&b.head)
                .then_with(|| a.up.priority.cmp(&b.up.priority))
                .then_with(|| b.run.cmp(&a.run))
                .then_with(|| preference(a.up, a.conns, b.up, b.conns))
                .then_with(|| a.hash.cmp(&b.hash))
        })
        .expect("cands is not empty");
    let best = chosen.up;
    if !best.online && !allow_offline {
        return Step::Hold {
            block: from,
            why: HoldWhy::Offline,
            bounded: !tip_region(v, from),
        };
    }
    let mut req_end = chosen.run;
    if best.role == Role::Frozen {
        let (head, lib) = tip;
        let gate = if sh.irreversible_only { lib } else { head };
        match gate {
            Some(g) if from <= g.num => req_end = req_end.min(g.num.saturating_add(1)),
            _ => return Step::Wait { block: from },
        }
    }
    if full(best) {
        return Step::Hold {
            block: from,
            why: HoldWhy::AtCapacity,
            bounded: !tip_region(v, from),
        };
    }
    debug_assert!(req_end > from, "a Serve step is never empty");
    Step::Serve(ServeStep {
        up: best.index,
        endpoint: best.endpoint.clone(),
        role: best.role,
        start: from,
        req_end,
    })
}

/// A [`next_step`] candidate with its ranking keys.
struct Ranked<'a> {
    up: &'a UpstreamView,
    /// Online, fresh, under capacity, role preference, not soft-excluded:
    /// `0` is better for each.
    head: [u8; 5],
    /// Connections that count as load (the session's own slots do not).
    conns: usize,
    /// The run's end, clamped to the request's end. Longer is better.
    run: u32,
    hash: u64,
}

/// A [`pool_pick`] candidate with its ranking keys.
struct PoolRanked<'a> {
    up: &'a UpstreamView,
    offline: bool,
    /// Coverage and freshness, `0` best (see `pool_pick`).
    rank: u8,
    conns: usize,
    hash: u64,
}

/// Pick an upstream for a pool session. Replaces `select_backend_server`
/// (`failover = false`: online only) and `select_failover_server`
/// (`failover = true`: an offline upstream is a last resort).
///
/// `route` is the request's need and start block, `None` before the first
/// request. Candidates are ranked by: online, covers the block (online
/// union), fresh and not lagging, then the lower `priority` tier, then fewer
/// connections per unit of `weight` (weighted least-connections), then the
/// higher weight, then the rendezvous hash. Coverage and freshness rank
/// above priority, so a preferred upstream that lacks the block or is stale
/// never wins over one that has it. The soft exclusion picks another
/// upstream that covers the block as well and is as fresh, when there is
/// one, in any priority tier: failover leaves a failed upstream even for a
/// backup tier.
pub fn pool_pick(
    v: &FleetView,
    route: Option<(Need, u32)>,
    ex: &Excludes,
    failover: bool,
    sid: u64,
    lag: u32,
    now: Instant,
) -> Option<usize> {
    let cands = || {
        v.upstreams.iter().filter(move |u| {
            u.routable()
                && (u.online || failover)
                && !ex.hard_active(&u.endpoint, now)
                && !u.at_capacity()
                && route.is_none_or(|(_, b)| !u.quarantined_at(b, now))
        })
    };
    let rank = |u: &UpstreamView| -> u8 {
        match route {
            None => u8::from(u.stale || lagging_for(v, u, u32::MAX, lag)),
            Some((need, b)) => {
                let covers = eligible_ignoring_quarantine(v, u, need, b, Union::Online, now);
                u8::from(!covers) * 2 + u8::from(u.stale || lagging_for(v, u, b, lag))
            }
        }
    };
    // Each candidate's keys are read once, so one ranking sees one
    // connection count per upstream.
    let ranked: Vec<PoolRanked<'_>> = cands()
        .map(|u| PoolRanked {
            up: u,
            offline: !u.online,
            rank: rank(u),
            conns: u.connections.load(Ordering::Relaxed),
            hash: rendezvous(sid, &u.endpoint),
        })
        .collect();
    let cmp = |a: &&PoolRanked<'_>, b: &&PoolRanked<'_>| {
        (a.offline, a.rank)
            .cmp(&(b.offline, b.rank))
            .then_with(|| preference(a.up, a.conns, b.up, b.conns))
            .then_with(|| a.hash.cmp(&b.hash))
    };
    let best = ranked.iter().min_by(cmp)?;
    // Prefer a peer of the one that just failed when it covers the block as
    // well, whatever its priority tier.
    let pick = if ex.soft.as_deref() == Some(&*best.up.endpoint) {
        ranked
            .iter()
            .filter(|c| c.up.endpoint != best.up.endpoint && c.rank <= best.rank)
            .min_by(cmp)
            .unwrap_or(best)
    } else {
        best
    };
    if let Some((_, block)) = route {
        if pick.rank >= 2 {
            tracing::warn!(
                block,
                "no upstream covers the requested block; falling back to least-connections"
            );
        }
    }
    Some(pick.up.index)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::fleet::view::fixtures::*;
    use crate::models::{BlockQuarantine, ResolvedTopology, ServerState};
    use crate::ship::Range;

    const NOW_TTL: Duration = Duration::from_secs(60);

    fn now() -> Instant {
        Instant::now()
    }

    fn shape(need: Need, end: u32) -> Shape {
        Shape {
            need,
            end,
            irreversible_only: false,
        }
    }

    fn all() -> Need {
        Need {
            traces: true,
            deltas: true,
            finality: false,
            block: true,
        }
    }

    fn tip_of(v: &FleetView) -> (Option<BlockPos>, Option<BlockPos>) {
        (v.head, v.lib)
    }

    fn plan(v: &FleetView, need: Need, from: u32) -> Step {
        next_step(
            v,
            tip_of(v),
            &shape(need, u32::MAX),
            from,
            &Excludes::default(),
            &[],
            1,
            &StitchingPolicy::default(),
            1200,
            false,
            now(),
        )
    }

    fn served_by(step: &Step) -> (usize, u32, u32) {
        match step {
            Step::Serve(s) => (s.up, s.start, s.req_end),
            other => panic!("expected Serve, got {other:?}"),
        }
    }

    /// The default fleet: A = frozen [2,1001), B = frozen [1001,2001),
    /// C = live from 1900 at head 2300.
    fn fleet() -> Vec<ServerState> {
        vec![
            frozen(0, 2, 1001),
            frozen(1, 1001, 2001),
            live(2, 1900, 2300),
        ]
    }

    fn with_finality(mut s: ServerState, begin: u32, end: u32) -> ServerState {
        s.finality_data_begin_block = begin;
        s.finality_data_end_block = end;
        s
    }

    #[test]
    fn need_follows_the_request_flags() {
        let mut r = wire::BlocksRequest {
            fetch_traces: true,
            fetch_finality_data: true,
            ..Default::default()
        };
        assert_eq!(
            Need::of(&r),
            Need {
                traces: true,
                ..Default::default()
            },
            "a v0 request never asks for finality"
        );
        r.v1 = true;
        r.end = 50;
        r.irreversible_only = true;
        let sh = Shape::of(&r);
        assert!(sh.need.finality && sh.irreversible_only);
        assert_eq!(sh.end, 50);
    }

    #[test]
    fn eligibility_follows_the_union_rule_for_each_field() {
        // PL1
        let now = now();
        let v = view(vec![
            frozen(0, 2, 1001),
            // A frozen upstream with deltas only from 500.
            {
                let mut s = frozen(1, 2, 1001);
                s.trace_begin_block = 0;
                s.trace_end_block = 0;
                s.chain_state_begin_block = 500;
                s
            },
        ]);
        let (full, deltas) = (&v.upstreams[0], &v.upstreams[1]);
        let traces = Need {
            traces: true,
            ..Default::default()
        };
        let delta_need = Need {
            deltas: true,
            ..Default::default()
        };
        let none = Need::default();
        // Traces: the union has traces at 600, so a trace-less upstream is out.
        assert!(eligible(&v, full, traces, 600, Union::Sticky, now));
        assert!(!eligible(&v, deltas, traces, 600, Union::Sticky, now));
        // Deltas: both have chain state at 600.
        assert!(eligible(&v, full, delta_need, 600, Union::Sticky, now));
        assert!(eligible(&v, deltas, delta_need, 600, Union::Sticky, now));
        // Ids: at 100 the union knows ids, the deltas-only upstream does not.
        assert!(!eligible(&v, deltas, delta_need, 100, Union::Sticky, now));
        assert!(!eligible(&v, deltas, none, 100, Union::Sticky, now));
        assert!(eligible(&v, deltas, none, 600, Union::Sticky, now));
        // Nothing anywhere at 5000: every enabled known upstream is eligible.
        assert!(eligible(&v, deltas, all(), 5000, Union::Sticky, now));

        // Finality: a pre-Savanna frozen upstream (no finality log) is
        // eligible outside F* and ineligible inside it.
        let v = view(vec![
            frozen(0, 2, 1001),
            with_finality(live(1, 900, 2000), 950, 2001),
        ]);
        let fin = Need {
            finality: true,
            ..Default::default()
        };
        let pre = &v.upstreams[0];
        assert!(eligible(&v, pre, fin, 940, Union::Sticky, now));
        assert!(!eligible(&v, pre, fin, 950, Union::Sticky, now));
        assert!(eligible(&v, pre, none, 950, Union::Sticky, now));

        // The online union ignores offline upstreams.
        let mut off = with_finality(live(1, 900, 2000), 950, 2001);
        off.online = false;
        let v = view(vec![frozen(0, 2, 1001), off]);
        assert!(!eligible(&v, &v.upstreams[0], fin, 960, Union::Sticky, now));
        assert!(eligible(&v, &v.upstreams[0], fin, 960, Union::Online, now));

        // Unknown, disabled and ABI-excluded upstreams are never eligible.
        let v = view(vec![frozen_unreported(0, None), frozen(1, 2, 1001)]);
        assert!(!eligible(&v, &v.upstreams[0], none, 10, Union::Sticky, now));
        let mut excluded = v.upstreams[1].clone();
        excluded.abi_ok = false;
        assert!(!eligible(&v, &excluded, none, 10, Union::Sticky, now));
        excluded.abi_ok = true;
        excluded.enabled = false;
        assert!(!eligible(&v, &excluded, none, 10, Union::Sticky, now));
    }

    #[test]
    fn head_plus_one_is_served_live_and_the_archive_top_waits() {
        // PL2
        let v = view(fleet());
        let step = plan(&v, all(), 2301);
        assert_eq!(served_by(&step), (2, 2301, u32::MAX));

        let archive_only = view(vec![frozen(0, 2, 1001), frozen(1, 1001, 2001)]);
        assert_eq!(plan(&archive_only, all(), 2001), Step::Wait { block: 2001 });
        assert_eq!(plan(&archive_only, all(), 5000), Step::Wait { block: 5000 });
        assert_eq!(
            served_by(&plan(&archive_only, all(), 2000)),
            (1, 2000, 2001)
        );
        // Nothing known yet: wait.
        let v = view(vec![frozen_unreported(0, None)]);
        assert_eq!(plan(&v, all(), 10), Step::Wait { block: 10 });
        assert_eq!(
            next_step(
                &v,
                (None, None),
                &shape(all(), 10),
                10,
                &Excludes::default(),
                &[],
                1,
                &StitchingPolicy::default(),
                0,
                false,
                now()
            ),
            Step::Done
        );
    }

    #[test]
    fn history_preference_picks_frozen_or_the_longest_run() {
        // PL3: B and C overlap on [1900, 2001).
        let v = view(fleet());
        assert_eq!(served_by(&plan(&v, all(), 1950)), (1, 1950, 2001));
        let any = StitchingPolicy {
            history_preference: HistoryPreference::Any,
            ..Default::default()
        };
        let step = next_step(
            &v,
            tip_of(&v),
            &shape(all(), u32::MAX),
            1950,
            &Excludes::default(),
            &[],
            1,
            &any,
            1200,
            false,
            now(),
        );
        assert_eq!(
            served_by(&step),
            (2, 1950, u32::MAX),
            "the live run is longer"
        );
    }

    #[test]
    fn replicas_split_by_least_connections_then_rendezvous() {
        // PL4
        let a = frozen(0, 2, 1001);
        let a2 = frozen(1, 2, 1001);
        a.connections.store(3, Ordering::Relaxed);
        let v = view(vec![a, a2]);
        assert_eq!(served_by(&plan(&v, all(), 10)).0, 1, "fewer connections");

        let v = view(vec![frozen(0, 2, 1001), frozen(1, 2, 1001)]);
        let pick = |sid: u64| match next_step(
            &v,
            tip_of(&v),
            &shape(all(), u32::MAX),
            10,
            &Excludes::default(),
            &[],
            sid,
            &StitchingPolicy::default(),
            1200,
            false,
            now(),
        ) {
            Step::Serve(s) => s.up,
            other => panic!("{other:?}"),
        };
        let first: Vec<usize> = (0..10_000u64).map(pick).collect();
        let to_zero = first.iter().filter(|&&up| up == 0).count();
        assert!(
            (4_700..=5_300).contains(&to_zero),
            "rendezvous must split evenly, got {to_zero} of 10000"
        );
        let again: Vec<usize> = (0..10_000u64).map(pick).collect();
        assert_eq!(first, again, "rendezvous is deterministic");
        // Also for addresses that differ before the port, and for three.
        for replicas in [
            &["10.0.3.10:8080", "10.0.3.11:8080"][..],
            &[
                "ws://10.0.3.10:8080",
                "ws://10.0.3.11:8080",
                "ws://10.0.3.12:8080",
            ][..],
        ] {
            let mut wins = vec![0usize; replicas.len()];
            for sid in 0..10_000u64 {
                let best = (0..replicas.len())
                    .min_by_key(|&i| rendezvous(sid, replicas[i]))
                    .unwrap();
                wins[best] += 1;
            }
            let fair = 10_000 / replicas.len();
            for w in &wins {
                assert!(
                    w.abs_diff(fair) <= 300,
                    "uneven rendezvous split {wins:?} for {replicas:?}"
                );
            }
        }
        assert_eq!(rendezvous(7, "a"), rendezvous(7, "a"));
        assert_ne!(rendezvous(7, "a"), rendezvous(8, "a"));
    }

    #[test]
    fn run_end_stops_at_quarantines_frozen_ends_and_finality_begins() {
        // PL5
        let now = now();
        let mut a = frozen(0, 2, 1001);
        a.quarantines.push(BlockQuarantine {
            block: 600,
            range: 5,
            until: now + NOW_TTL,
        });
        let v = view(vec![a, frozen(1, 1001, 2001), live(2, 1900, 2300)]);
        let (a, b, c) = (&v.upstreams[0], &v.upstreams[1], &v.upstreams[2]);
        assert_eq!(run_end(&v, a, all(), 10, now), 600, "quarantine start");
        assert_eq!(run_end(&v, a, all(), 605, now), 1001, "frozen max end");
        assert_eq!(run_end(&v, b, all(), 1500, now), 2001);
        assert_eq!(run_end(&v, c, all(), 2001, now), u32::MAX);
        // An expired quarantine is no breakpoint.
        assert_eq!(run_end(&v, a, all(), 10, now + NOW_TTL * 2), 1001);

        // Another upstream's finality begins at 950: a finality request on the
        // pre-Savanna archive stops there.
        let v = view(vec![
            frozen(0, 2, 1001),
            with_finality(live(1, 900, 2000), 950, 2001),
        ]);
        let fin = Need {
            finality: true,
            ..all()
        };
        assert_eq!(run_end(&v, &v.upstreams[0], fin, 2, now), 950);
        assert_eq!(run_end(&v, &v.upstreams[0], all(), 2, now), 1001);
        assert_eq!(served_by(&plan(&v, fin, 2)), (0, 2, 950));
        assert_eq!(served_by(&plan(&v, fin, 950)), (1, 950, u32::MAX));

        // A hole after an archive: its run still ends at its own end, never
        // at the next upstream's begin (it has nothing to send in the hole).
        let v = view(vec![
            frozen(0, 2, 1001),
            frozen(1, 1101, 2001),
            live(2, 1900, 2300),
        ]);
        assert_eq!(run_end(&v, &v.upstreams[0], all(), 900, now), 1001);
        assert_eq!(served_by(&plan(&v, all(), 900)), (0, 900, 1001));
        assert_eq!(plan(&v, all(), 1001), Step::Gap { block: 1001 });
        // The same between the top archive and a pruned live tier.
        let v = view(vec![frozen(0, 2, 1001), live(1, 1101, 2300)]);
        assert!(v.live_overlap[0].is_some_and(|o| o < 0));
        assert_eq!(run_end(&v, &v.upstreams[0], all(), 900, now), 1001);
        assert_eq!(served_by(&plan(&v, all(), 900)), (0, 900, 1001));
        assert_eq!(plan(&v, all(), 1001), Step::Gap { block: 1001 });
        // A log that goes on past another log's end is still served there.
        let mut longer = frozen(0, 2, 1001);
        longer.chain_state_end_block = 1101;
        let v = view(vec![longer, frozen(1, 1201, 2001)]);
        let deltas = Need {
            deltas: true,
            ..Default::default()
        };
        assert_eq!(run_end(&v, &v.upstreams[0], deltas, 900, now), 1101);
    }

    #[test]
    fn below_fleet_begin_only_the_owner_of_fleet_begin_serves() {
        // PL6
        let v = view(vec![
            frozen(0, 1001, 2001),
            frozen(1, 2, 1001),
            live(2, 1900, 2300),
        ]);
        assert_eq!(served_by(&plan(&v, all(), 1)), (1, 1, 1001));
        // The owner's replica splits like any other candidate.
        let v = view(vec![
            frozen(0, 2, 1001),
            frozen(1, 2, 1001),
            frozen(2, 1001, 2001),
        ]);
        let up = served_by(&plan(&v, all(), 1)).0;
        assert!(up == 0 || up == 1);
    }

    #[test]
    fn holes_give_gap_and_offline_holds_are_bounded_only_in_history() {
        // PL7
        let v = view(vec![
            frozen(0, 2, 1001),
            frozen(1, 1101, 2001),
            live(2, 1900, 2300),
        ]);
        assert_eq!(plan(&v, all(), 1050), Step::Gap { block: 1050 });

        let mut c = live(2, 1900, 2300);
        c.online = false;
        let v = view(vec![frozen(0, 2, 1001), frozen(1, 1001, 2001), c]);
        assert_eq!(
            plan(&v, all(), 2400),
            Step::Hold {
                block: 2400,
                why: HoldWhy::Offline,
                bounded: false
            }
        );

        let mut a = frozen(0, 2, 1001);
        a.online = false;
        let v = view(vec![a, frozen(1, 1001, 2001), live(2, 1900, 2300)]);
        assert_eq!(
            plan(&v, all(), 500),
            Step::Hold {
                block: 500,
                why: HoldWhy::Offline,
                bounded: true
            }
        );
        // A bounded hold's retry may dial the offline upstream.
        let step = next_step(
            &v,
            tip_of(&v),
            &shape(all(), u32::MAX),
            500,
            &Excludes::default(),
            &[],
            1,
            &StitchingPolicy::default(),
            1200,
            true,
            now(),
        );
        assert_eq!(served_by(&step), (0, 500, 1001));
    }

    #[test]
    fn frozen_requests_are_gated_by_the_fleet_head_and_lib() {
        // PL8
        let mut c = live(2, 400, 1500);
        c.lib = Some(pos(1200));
        let v = view(vec![frozen(0, 2, 1001), frozen(1, 1001, 2001), c]);
        let pol = StitchingPolicy::default();
        let step_for = |from: u32, irreversible_only: bool| {
            next_step(
                &v,
                tip_of(&v),
                &Shape {
                    need: all(),
                    end: u32::MAX,
                    irreversible_only,
                },
                from,
                &Excludes::default(),
                &[],
                1,
                &pol,
                1200,
                false,
                now(),
            )
        };
        // Above the fleet head on a frozen candidate: wait.
        assert_eq!(step_for(1600, false), Step::Wait { block: 1600 });
        // Without irreversible_only the request is capped at head + 1, not LIB.
        assert_eq!(served_by(&step_for(1001, false)), (1, 1001, 1501));
        // irreversible_only caps it at LIB + 1, and waits above LIB.
        assert_eq!(served_by(&step_for(1001, true)), (1, 1001, 1201));
        assert_eq!(step_for(1300, true), Step::Wait { block: 1300 });
        // A live candidate is never gated.
        let v2 = view(vec![frozen(0, 2, 1001), live(1, 900, 1500)]);
        assert_eq!(served_by(&plan(&v2, all(), 1400)), (1, 1400, u32::MAX));
        // No tip known: a frozen request waits.
        let v3 = view(vec![frozen(0, 2, 1001)]);
        let step = next_step(
            &v3,
            (None, None),
            &shape(all(), u32::MAX),
            10,
            &Excludes::default(),
            &[],
            1,
            &pol,
            1200,
            false,
            now(),
        );
        assert_eq!(step, Step::Wait { block: 10 });
    }

    /// xorshift64*, deterministic for a seed.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
        }

        fn below(&mut self, n: u32) -> u32 {
            (self.next() % u64::from(n.max(1))) as u32
        }

        fn chance(&mut self, percent: u32) -> bool {
            self.below(100) < percent
        }
    }

    fn random_fleet(rng: &mut Rng) -> Vec<ServerState> {
        let n = 1 + rng.below(5) as usize;
        (0..n)
            .map(|index| {
                let begin = 1 + rng.below(3000);
                let len = 1 + rng.below(2000);
                let mut s = if rng.chance(50) {
                    frozen(index, begin, begin + len)
                } else {
                    live(index, begin, begin + len)
                };
                if rng.chance(30) {
                    s.trace_begin_block = 0;
                    s.trace_end_block = 0;
                }
                if rng.chance(30) {
                    let fb = begin + rng.below(len);
                    s.finality_data_begin_block = fb;
                    s.finality_data_end_block = fb + 1 + rng.below(len);
                }
                if rng.chance(20) {
                    s.online = false;
                }
                if rng.chance(15) {
                    s.stale = true;
                }
                if rng.chance(10) {
                    s.ranges_known = false;
                    if s.role == Role::Frozen && rng.chance(50) {
                        s.declared = Some(Range {
                            begin,
                            end: begin + len,
                        });
                    }
                }
                if rng.chance(15) {
                    s.max_sessions = Some(1);
                    s.connections = Arc::new(AtomicUsize::new(rng.below(2) as usize));
                }
                if rng.chance(15) {
                    s.quarantines.push(BlockQuarantine {
                        block: begin + rng.below(len),
                        range: 1 + rng.below(50),
                        until: Instant::now() + NOW_TTL,
                    });
                }
                if rng.chance(30) {
                    let head = s.head.map_or(1, |h| h.num);
                    s.head = Some(pos(head.saturating_sub(rng.below(1500))));
                }
                s
            })
            .collect()
    }

    #[test]
    fn serve_steps_are_never_empty() {
        // PL9
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let mut serves = 0;
        for _ in 0..5_000 {
            let v = view(random_fleet(&mut rng));
            let from = rng.below(6000);
            let end = if rng.chance(50) {
                u32::MAX
            } else {
                from + rng.below(3000)
            };
            let need = Need {
                traces: rng.chance(50),
                deltas: rng.chance(50),
                finality: rng.chance(30),
                block: rng.chance(50),
            };
            let mut ex = Excludes::default();
            if rng.chance(20) {
                if let Some(u) = v.upstreams.first() {
                    ex.add_hard(u.endpoint.clone(), NOW_TTL, now());
                }
            }
            let tip = (
                v.head.filter(|_| !rng.chance(10)),
                v.lib.map(|l| pos(l.num.saturating_sub(rng.below(200)))),
            );
            let step = next_step(
                &v,
                tip,
                &Shape {
                    need,
                    end,
                    irreversible_only: rng.chance(30),
                },
                from,
                &ex,
                &[],
                rng.next(),
                &StitchingPolicy::default(),
                if rng.chance(50) { 0 } else { 200 },
                rng.chance(50),
                now(),
            );
            if let Step::Serve(s) = step {
                serves += 1;
                assert!(
                    s.req_end > s.start,
                    "empty Serve {s:?} for from {from} end {end}"
                );
                assert_eq!(s.start, from);
                assert!(s.req_end <= end);
            }
        }
        assert!(
            serves > 1_000,
            "the random fleets should mostly serve, got {serves}"
        );
    }

    #[test]
    fn lagging_live_nodes_are_avoided_above_their_head() {
        // PL10
        let close = live(0, 100, 5000);
        let far = live(1, 100, 3000);
        far.connections.store(0, Ordering::Relaxed);
        close.connections.store(5, Ordering::Relaxed);
        let v = view(vec![close, far]);
        // Above the lagging node's head the up-to-date node wins despite its
        // connections.
        assert_eq!(served_by(&plan(&v, all(), 4000)).0, 0);
        assert_eq!(
            pool_pick(
                &v,
                Some((all(), 4000)),
                &Excludes::default(),
                false,
                1,
                1200,
                now()
            ),
            Some(0)
        );
        // Below its head the lagging node is fine (fewer connections).
        assert_eq!(served_by(&plan(&v, all(), 2000)).0, 1);
        // When it is the only candidate, it is chosen.
        let mut ex = Excludes::default();
        ex.add_hard(v.upstreams[0].endpoint.clone(), NOW_TTL, now());
        let step = next_step(
            &v,
            tip_of(&v),
            &shape(all(), u32::MAX),
            4000,
            &ex,
            &[],
            1,
            &StitchingPolicy::default(),
            1200,
            false,
            now(),
        );
        assert_eq!(served_by(&step).0, 1);
        assert_eq!(
            pool_pick(&v, Some((all(), 4000)), &ex, false, 1, 1200, now()),
            Some(1)
        );
        // live_lag_blocks = 0 disables the rule.
        assert_eq!(
            pool_pick(
                &v,
                Some((all(), 4000)),
                &Excludes::default(),
                false,
                1,
                0,
                now()
            ),
            Some(1)
        );
    }

    #[test]
    fn capacity_hard_and_soft_exclusions() {
        // PL11
        let mut a = frozen(0, 2, 1001);
        a.max_sessions = Some(2);
        a.connections.store(2, Ordering::Relaxed);
        let v = view(vec![a, frozen(1, 1001, 2001), live(2, 1900, 2300)]);
        assert_eq!(
            plan(&v, all(), 10),
            Step::Hold {
                block: 10,
                why: HoldWhy::AtCapacity,
                bounded: true
            }
        );
        // A full candidate loses to a free replica.
        let mut full = frozen(0, 2, 1001);
        full.max_sessions = Some(1);
        full.connections.store(1, Ordering::Relaxed);
        let v = view(vec![full, frozen(1, 2, 1001)]);
        assert_eq!(served_by(&plan(&v, all(), 10)).0, 1);

        // Hard exclusion filters the candidate out; with no other, the
        // session holds as busy.
        let v = view(vec![frozen(0, 2, 1001), frozen(1, 2, 1001)]);
        let mut ex = Excludes::default();
        ex.add_hard(v.upstreams[0].endpoint.clone(), NOW_TTL, now());
        let step = |ex: &Excludes, at: Instant| {
            next_step(
                &v,
                tip_of(&v),
                &shape(all(), u32::MAX),
                10,
                ex,
                &[],
                1,
                &StitchingPolicy::default(),
                1200,
                false,
                at,
            )
        };
        assert_eq!(served_by(&step(&ex, now())).0, 1);
        ex.add_hard(v.upstreams[1].endpoint.clone(), NOW_TTL, now());
        assert_eq!(
            step(&ex, now()),
            Step::Hold {
                block: 10,
                why: HoldWhy::Busy,
                bounded: true
            }
        );
        // ...until the exclusions expire.
        assert!(matches!(step(&ex, now() + NOW_TTL * 2), Step::Serve(_)));
        assert!(!ex.hard_active(&v.upstreams[0].endpoint, now() + NOW_TTL * 2));
        // Re-adding replaces the earlier entry.
        ex.add_hard(v.upstreams[1].endpoint.clone(), NOW_TTL, now());
        assert_eq!(ex.hard.len(), 2);

        // Soft exclusion: prefer another upstream of the same rank...
        for sid in 0..50 {
            let natural = match next_step(
                &v,
                tip_of(&v),
                &shape(all(), u32::MAX),
                10,
                &Excludes::default(),
                &[],
                sid,
                &StitchingPolicy::default(),
                1200,
                false,
                now(),
            ) {
                Step::Serve(s) => s,
                other => panic!("{other:?}"),
            };
            let soft = Excludes {
                soft: Some(natural.endpoint.clone()),
                ..Default::default()
            };
            let moved = next_step(
                &v,
                tip_of(&v),
                &shape(all(), u32::MAX),
                10,
                &soft,
                &[],
                sid,
                &StitchingPolicy::default(),
                1200,
                false,
                now(),
            );
            assert_ne!(served_by(&moved).0, natural.up);
        }
        // ...but keep the excluded one when it is the only candidate.
        let only = view(vec![frozen(0, 2, 1001), live(1, 1900, 2300)]);
        let soft = Excludes {
            soft: Some(only.upstreams[0].endpoint.clone()),
            ..Default::default()
        };
        let step = next_step(
            &only,
            tip_of(&only),
            &shape(all(), u32::MAX),
            10,
            &soft,
            &[],
            1,
            &StitchingPolicy::default(),
            1200,
            false,
            now(),
        );
        assert_eq!(served_by(&step).0, 0);
    }

    /// `next_step` with the default policy and `own` slots.
    fn plan_owning(v: &FleetView, from: u32, irreversible_only: bool, own: &[usize]) -> Step {
        next_step(
            v,
            tip_of(v),
            &Shape {
                need: all(),
                end: u32::MAX,
                irreversible_only,
            },
            from,
            &Excludes::default(),
            own,
            1,
            &StitchingPolicy::default(),
            1200,
            false,
            now(),
        )
    }

    #[test]
    fn the_sessions_own_slots_never_fill_an_upstream() {
        // B has max_sessions 1 and the only connection on it is the
        // session's own (its lingering link, or its pre-dialed standby).
        let mut b = frozen(1, 1001, 2001);
        b.max_sessions = Some(1);
        b.connections.store(1, Ordering::Relaxed);
        let mut c = live(2, 1900, 2300);
        c.lib = Some(pos(1500));
        let v = view(vec![frozen(0, 2, 1001), b, c]);
        let at_capacity = Step::Hold {
            block: 1100,
            why: HoldWhy::AtCapacity,
            bounded: true,
        };
        assert_eq!(plan_owning(&v, 1100, false, &[]), at_capacity);
        assert_eq!(
            served_by(&plan_owning(&v, 1100, false, &[1])),
            (1, 1100, 2001)
        );
        // A slot on another upstream does not help.
        assert_eq!(plan_owning(&v, 1100, false, &[0, 2]), at_capacity);
        // irreversible_only at LIB: the session waits (unbounded) for LIB,
        // and serves on its own slot once LIB has moved.
        assert_eq!(
            plan_owning(&v, 1501, true, &[1]),
            Step::Wait { block: 1501 }
        );
        assert_eq!(
            served_by(&plan_owning(&v, 1001, true, &[1])),
            (1, 1001, 1501)
        );
        // Another session's slot still counts.
        v.upstreams[1].connections.store(2, Ordering::Relaxed);
        assert_eq!(plan_owning(&v, 1100, false, &[1]), at_capacity);
        // Above the gate, a full archive is waited for like a free one:
        // capacity matters once there is something to send.
        assert_eq!(plan_owning(&v, 1501, true, &[]), Step::Wait { block: 1501 });
        // Ranking discounts the own slot too: a full replica loses to the
        // one the session already holds.
        let mut b = frozen(0, 1001, 2001);
        b.max_sessions = Some(1);
        b.connections.store(1, Ordering::Relaxed);
        let mut b2 = frozen(1, 1001, 2001);
        b2.max_sessions = Some(1);
        b2.connections.store(1, Ordering::Relaxed);
        let v = view(vec![b, b2, live(2, 1900, 2300)]);
        assert_eq!(served_by(&plan_owning(&v, 1100, false, &[1])).0, 1);
        assert_eq!(served_by(&plan_owning(&v, 1100, false, &[0])).0, 0);
    }

    #[test]
    fn an_unreported_archive_holds_the_blocks_it_may_hold() {
        // A is enabled, down and has never reported or declared a range: the
        // blocks below the known fleet begin (B's) may be A's, so they are
        // held like an offline archive's, never served id-less by B.
        let v = view(vec![
            frozen_unreported(0, None),
            frozen(1, 1001, 2001),
            live(2, 1900, 2300),
        ]);
        assert_eq!(v.frozen_unknown, 1);
        let held = Step::Hold {
            block: 500,
            why: HoldWhy::Offline,
            bounded: true,
        };
        assert_eq!(plan(&v, all(), 500), held);
        let retry = next_step(
            &v,
            tip_of(&v),
            &shape(all(), u32::MAX),
            500,
            &Excludes::default(),
            &[],
            1,
            &StitchingPolicy::default(),
            1200,
            true,
            now(),
        );
        assert_eq!(
            retry, held,
            "a retry may dial offline upstreams, not unknown ones"
        );
        // Covered blocks are served as usual.
        assert_eq!(served_by(&plan(&v, all(), 1500)), (1, 1500, 2001));
        // A known but offline A holds the same way.
        let mut a = frozen(0, 2, 1001);
        a.online = false;
        let v = view(vec![a, frozen(1, 1001, 2001), live(2, 1900, 2300)]);
        assert_eq!(plan(&v, all(), 500), held);
        // With every archive known, blocks below the fleet begin go to its
        // owner (PL6).
        let v = view(vec![frozen(1, 1001, 2001), live(2, 1900, 2300)]);
        assert_eq!(served_by(&plan(&v, all(), 500)), (1, 500, 2001));

        // A hole the unreported archive may fill holds instead of closing.
        let v = view(vec![
            frozen(0, 2, 1001),
            frozen(1, 1101, 2001),
            live(2, 1900, 2300),
            frozen_unreported(3, None),
        ]);
        assert_eq!(
            plan(&v, all(), 1050),
            Step::Hold {
                block: 1050,
                why: HoldWhy::Offline,
                bounded: true
            }
        );
        // A declared range is known: the hole is a gap again.
        let v = view(vec![
            frozen(0, 2, 1001),
            frozen(1, 1101, 2001),
            live(2, 1900, 2300),
            frozen_unreported(3, Some((5000, 6000))),
        ]);
        assert_eq!(plan(&v, all(), 1050), Step::Gap { block: 1050 });
    }

    // PL12: every routing test of `functions.rs`, ported to `pool_pick`.

    /// A live pool upstream with traces `trace` and finality `finality`.
    fn pool_server(
        index: usize,
        conns: usize,
        trace: (u32, u32),
        finality: (u32, u32),
    ) -> ServerState {
        let mut s = live(index, 1, 1);
        s.head = None;
        s.lib = None;
        s.trace_begin_block = trace.0;
        s.trace_end_block = trace.1;
        s.chain_state_begin_block = 0;
        s.chain_state_end_block = 0;
        s.finality_data_begin_block = finality.0;
        s.finality_data_end_block = finality.1;
        s.connections = Arc::new(AtomicUsize::new(conns));
        s
    }

    fn state(index: usize, enabled: bool, online: bool, conns: usize) -> ServerState {
        let mut s = pool_server(index, conns, (0, 0), (0, 0));
        s.ranges_known = false;
        s.enabled = enabled;
        s.online = online;
        s
    }

    fn pool_view(list: Vec<ServerState>) -> FleetView {
        let mut mem = crate::fleet::view::HubMemory::new(Instant::now());
        crate::fleet::view::build_view(
            &ctx(ResolvedTopology::Pool),
            &states(list),
            &[],
            &mut mem,
            Instant::now(),
        )
    }

    fn pick(
        v: &FleetView,
        route: Option<(Need, u32)>,
        soft: Option<usize>,
        failover: bool,
    ) -> Option<usize> {
        let ex = Excludes {
            soft: soft.map(|i| v.up(i).unwrap().endpoint.clone()),
            ..Default::default()
        };
        pool_pick(v, route, &ex, failover, 1, 1200, Instant::now())
    }

    fn at(block: u32) -> Option<(Need, u32)> {
        Some((Need::default(), block))
    }

    fn finality_at(block: u32) -> Option<(Need, u32)> {
        Some((
            Need {
                finality: true,
                ..Default::default()
            },
            block,
        ))
    }

    /// `b` (index 0): fewer connections, traces cover 100, finality does not.
    /// `a` (index 1): more connections, traces and finality cover 100.
    fn finality_fleet() -> Vec<ServerState> {
        vec![
            pool_server(0, 0, (1, 20_001), (10_001, 20_001)),
            pool_server(1, 5, (1, 20_001), (1, 10_001)),
        ]
    }

    #[test]
    fn pool_new_sessions_skip_offline_upstreams() {
        let v = pool_view(vec![state(0, true, false, 0)]);
        assert_eq!(pick(&v, None, None, false), None);
        assert_eq!(pick(&v, None, None, true), Some(0));
    }

    #[test]
    fn pool_failover_prefers_an_online_upstream() {
        let v = pool_view(vec![state(0, true, false, 0), state(1, true, true, 5)]);
        assert_eq!(pick(&v, None, None, false), Some(1));
        assert_eq!(pick(&v, None, None, true), Some(1));
    }

    #[test]
    fn pool_failover_skips_disabled_upstreams() {
        let v = pool_view(vec![state(0, false, false, 0), state(1, false, true, 0)]);
        assert_eq!(pick(&v, None, None, true), None);
        assert_eq!(pick(&v, None, None, false), None);
    }

    #[test]
    fn pool_finality_request_skips_upstream_that_does_not_cover() {
        let v = pool_view(finality_fleet());
        assert_eq!(pick(&v, finality_at(100), None, false), Some(1));
    }

    #[test]
    fn pool_non_finality_request_keeps_least_connections() {
        let v = pool_view(finality_fleet());
        assert_eq!(pick(&v, at(100), None, false), Some(0));
    }

    #[test]
    fn pool_finality_request_falls_back_to_v0_when_no_range_covers() {
        // A live range is open-ended above its begin, so "does not cover
        // 100" is a finality range that begins above it.
        let mut fleet = finality_fleet();
        fleet[1].finality_data_begin_block = 200;
        let v = pool_view(fleet);
        assert_eq!(pick(&v, finality_at(100), None, false), Some(0));
    }

    #[test]
    fn pool_finality_request_falls_back_when_ranges_were_never_reported() {
        let v = pool_view(vec![
            pool_server(0, 4, (1, 500), (0, 0)),
            pool_server(1, 0, (1, 500), (0, 0)),
        ]);
        assert_eq!(pick(&v, finality_at(10), None, false), Some(1));
    }

    #[test]
    fn pool_finality_eligibility_matches_the_old_reroute_rule() {
        // `should_reroute_for_finality(b, n)` == the current upstream is not
        // eligible under the online union.
        let now = Instant::now();
        let fin = Need {
            finality: true,
            ..Default::default()
        };
        let reroute = |v: &FleetView, i: usize, n: u32| {
            !eligible(v, v.up(i).unwrap(), fin, n, Union::Online, now)
        };
        let v = pool_view(finality_fleet());
        assert!(reroute(&v, 0, 100));
        assert!(!reroute(&v, 1, 100));
        assert!(!reroute(&v, 0, 15_000));
        let mut nobody = finality_fleet();
        nobody[1].finality_data_begin_block = 200;
        let v = pool_view(nobody);
        assert!(!reroute(&v, 0, 100));
    }

    #[test]
    fn pool_offline_finality_upstream_does_not_force_a_reroute() {
        let mut fleet = finality_fleet();
        fleet[1].online = false;
        let v = pool_view(fleet);
        let fin = Need {
            finality: true,
            ..Default::default()
        };
        assert!(eligible(
            &v,
            v.up(0).unwrap(),
            fin,
            100,
            Union::Online,
            Instant::now()
        ));
        assert_eq!(pick(&v, finality_at(100), None, false), Some(0));
    }

    #[test]
    fn pool_retry_prefers_another_upstream_that_covers_the_block() {
        // The upstream that just failed has fewer connections, so plain
        // least-connections would pick it again.
        let v = pool_view(vec![
            pool_server(0, 0, (1, 500), (0, 0)),
            pool_server(1, 4, (1, 500), (0, 0)),
        ]);
        assert_eq!(pick(&v, at(100), Some(0), true), Some(1));
    }

    #[test]
    fn pool_retry_keeps_the_only_upstream_that_covers_the_block() {
        // Live ranges are open-ended above their begin, so "does not cover"
        // is now a node far behind the pool: here 4951 blocks, above the
        // default live_lag_blocks of 1200, asked for a block above its head.
        let mut failed = pool_server(0, 1, (1, 5001), (0, 0));
        failed.head = Some(pos(5000));
        let mut short = pool_server(1, 0, (1, 50), (0, 0));
        short.head = Some(pos(49));
        let v = pool_view(vec![failed, short]);
        assert_eq!(pick(&v, at(1000), Some(0), true), Some(0));
        // Below its own head the short node is not lagging: the peer is taken.
        assert_eq!(pick(&v, at(40), Some(0), true), Some(1));
    }

    #[test]
    fn pool_retry_of_the_only_upstream_still_selects_it() {
        let v = pool_view(vec![pool_server(0, 1, (1, 500), (0, 0))]);
        assert_eq!(pick(&v, at(100), Some(0), true), Some(0));
    }

    #[test]
    fn pool_quarantined_upstream_is_skipped_only_inside_its_range() {
        let mut bad = pool_server(0, 0, (1, 500), (0, 0));
        bad.quarantines.push(BlockQuarantine {
            block: 115,
            range: 1,
            until: Instant::now() + NOW_TTL,
        });
        let v = pool_view(vec![bad, pool_server(1, 5, (1, 500), (0, 0))]);
        assert_eq!(pick(&v, at(115), None, false), Some(1));
        assert_eq!(
            pick(&v, at(100), None, false),
            Some(0),
            "a quarantine at 115 must not affect a request starting at 100"
        );
        assert_eq!(
            pick(&v, None, None, false),
            Some(0),
            "initial connect has no block and must ignore range quarantine"
        );
    }

    #[test]
    fn pool_expired_quarantine_does_not_exclude() {
        let mut bad = pool_server(0, 0, (1, 500), (0, 0));
        bad.quarantines.push(BlockQuarantine {
            block: 115,
            range: 1,
            until: Instant::now()
                .checked_sub(Duration::from_secs(1))
                .unwrap_or_else(Instant::now),
        });
        let v = pool_view(vec![bad, pool_server(1, 3, (1, 500), (0, 0))]);
        assert_eq!(pick(&v, at(115), None, false), Some(0));
    }

    #[test]
    fn pool_quarantine_does_not_fall_back_to_the_excluded_upstream() {
        let mut only = pool_server(0, 0, (1, 500), (0, 0));
        only.quarantines.push(BlockQuarantine {
            block: 115,
            range: 2,
            until: Instant::now() + NOW_TTL,
        });
        let v = pool_view(vec![only]);
        assert_eq!(pick(&v, at(116), None, false), None);
        assert_eq!(pick(&v, at(116), Some(0), true), None);
        assert_eq!(pick(&v, at(117), None, false), Some(0));
    }

    #[test]
    fn pool_pick_honours_capacity_hard_exclusion_and_ties_deterministically() {
        let mut full = pool_server(0, 1, (1, 500), (0, 0));
        full.max_sessions = Some(1);
        let v = pool_view(vec![full, pool_server(1, 9, (1, 500), (0, 0))]);
        assert_eq!(pick(&v, at(100), None, false), Some(1));

        let v = pool_view(vec![
            pool_server(0, 0, (1, 500), (0, 0)),
            pool_server(1, 0, (1, 500), (0, 0)),
        ]);
        let mut ex = Excludes::default();
        ex.add_hard(v.up(0).unwrap().endpoint.clone(), NOW_TTL, Instant::now());
        assert_eq!(
            pool_pick(&v, at(100), &ex, false, 1, 1200, Instant::now()),
            Some(1)
        );

        let picks: Vec<Option<usize>> = (0..64)
            .map(|sid| {
                pool_pick(
                    &v,
                    at(100),
                    &Excludes::default(),
                    false,
                    sid,
                    1200,
                    Instant::now(),
                )
            })
            .collect();
        let again: Vec<Option<usize>> = (0..64)
            .map(|sid| {
                pool_pick(
                    &v,
                    at(100),
                    &Excludes::default(),
                    false,
                    sid,
                    1200,
                    Instant::now(),
                )
            })
            .collect();
        assert_eq!(picks, again);
        assert!(picks.contains(&Some(0)) && picks.contains(&Some(1)));
    }

    #[test]
    fn pool_counts_head_plus_one_as_covered_and_ranks_deltas_by_chain_state() {
        // A request at head + 1 is covered by a live node: it beats a node
        // that starts above the block, despite more connections, with or
        // without the lag rule.
        let mut tip = pool_server(0, 5, (1, 1001), (0, 0));
        tip.head = Some(pos(1000));
        let mut other = pool_server(1, 0, (2000, 3001), (0, 0));
        other.head = Some(pos(3000));
        let v = pool_view(vec![tip, other]);
        assert!(eligible(
            &v,
            v.up(0).unwrap(),
            Need::default(),
            1001,
            Union::Online,
            Instant::now()
        ));
        for lag in [0, 1200] {
            assert_eq!(
                pool_pick(
                    &v,
                    at(1001),
                    &Excludes::default(),
                    false,
                    1,
                    lag,
                    Instant::now()
                ),
                Some(0)
            );
        }

        // Deltas-only requests use the chain_state range.
        let mut deltas = pool_server(0, 5, (0, 0), (0, 0));
        deltas.chain_state_begin_block = 1;
        deltas.chain_state_end_block = 1001;
        let traces_only = pool_server(1, 0, (1, 1001), (0, 0));
        let v = pool_view(vec![deltas, traces_only]);
        let need = Need {
            deltas: true,
            ..Default::default()
        };
        assert_eq!(pick(&v, Some((need, 500)), None, false), Some(0));
    }

    // Upstream preference (`priority` and `weight`): the routing tests that
    // came with it, ported to `pool_pick`, and the same order in `next_step`.

    /// A pool upstream whose traces cover 1..500, with `priority` and
    /// `weight`.
    fn tiered(index: usize, conns: usize, priority: u32, weight: u32) -> ServerState {
        let mut s = pool_server(index, conns, (1, 500), (0, 0));
        s.priority = priority;
        s.weight = weight;
        s
    }

    #[test]
    fn pool_lower_priority_tier_wins_over_fewer_connections() {
        let v = pool_view(vec![tiered(0, 9, 0, 1), tiered(1, 0, 1, 1)]);
        assert_eq!(pick(&v, at(100), None, false), Some(0));
        assert_eq!(
            pick(&v, None, None, false),
            Some(0),
            "priority also applies on initial connect, before any block is known"
        );
    }

    #[test]
    fn pool_backup_tier_serves_when_the_preferred_tier_is_offline() {
        let mut primary = tiered(0, 0, 0, 1);
        primary.online = false;
        let v = pool_view(vec![primary, tiered(1, 3, 100, 1)]);
        assert_eq!(pick(&v, at(100), None, false), Some(1));
        assert_eq!(pick(&v, at(100), None, true), Some(1));
    }

    #[test]
    fn pool_coverage_and_freshness_outrank_priority() {
        // A live range is open-ended above its begin, so the preferred node
        // that lacks block 100 is a pruned one whose traces begin above it.
        let mut short = tiered(0, 0, 0, 1);
        short.trace_begin_block = 200;
        let v = pool_view(vec![short, tiered(1, 0, 5, 1)]);
        assert_eq!(pick(&v, at(100), None, false), Some(1));

        let mut stale = tiered(0, 0, 0, 1);
        stale.stale = true;
        let v = pool_view(vec![stale, tiered(1, 0, 5, 1)]);
        assert_eq!(pick(&v, at(100), None, false), Some(1));
    }

    #[test]
    fn pool_weight_splits_sessions_within_a_tier() {
        let v = pool_view(vec![
            tiered(0, 0, 0, 2),
            tiered(1, 0, 0, 1),
            tiered(2, 0, 1, 10),
        ]);
        for _ in 0..30 {
            let picked = pick(&v, at(100), None, false).unwrap();
            v.up(picked)
                .unwrap()
                .connections
                .fetch_add(1, Ordering::Relaxed);
        }
        let conns = |i: usize| v.up(i).unwrap().connections.load(Ordering::Relaxed);
        assert_eq!(conns(0), 20);
        assert_eq!(conns(1), 10);
        assert_eq!(conns(2), 0);
    }

    #[test]
    fn pool_equal_load_prefers_the_higher_weight() {
        let v = pool_view(vec![tiered(0, 1, 0, 1), tiered(1, 2, 0, 2)]);
        assert_eq!(pick(&v, at(100), None, false), Some(1));
    }

    #[test]
    fn pool_retry_leaves_a_failed_preferred_upstream_for_a_backup() {
        let v = pool_view(vec![tiered(0, 0, 0, 1), tiered(1, 0, 5, 1)]);
        assert_eq!(pick(&v, at(100), Some(0), true), Some(1));
    }

    #[test]
    fn preference_compares_load_per_weight_exactly() {
        let v = pool_view(vec![tiered(0, 0, 0, 3), tiered(1, 0, 0, 2)]);
        let (a, b) = (v.up(0).unwrap(), v.up(1).unwrap());
        // 2/3 < 1/2 is false; 1/3 < 1/2 is true; 3/3 == 2/2 goes to weight 3.
        assert_eq!(preference(a, 2, b, 1), CmpOrdering::Greater);
        assert_eq!(preference(a, 1, b, 1), CmpOrdering::Less);
        assert_eq!(preference(a, 3, b, 2), CmpOrdering::Less);
        assert_eq!(preference(b, 2, a, 3), CmpOrdering::Greater);
        // Huge counts do not overflow: MAX/3 < MAX/2.
        assert_eq!(preference(a, usize::MAX, b, usize::MAX), CmpOrdering::Less);
    }

    /// `next_step` at `from` for `v` with `policy`, `ex` and `own` slots.
    fn plan_with(
        v: &FleetView,
        from: u32,
        policy: &StitchingPolicy,
        ex: &Excludes,
        own: &[usize],
    ) -> Step {
        next_step(
            v,
            tip_of(v),
            &shape(all(), u32::MAX),
            from,
            ex,
            own,
            1,
            policy,
            1200,
            false,
            now(),
        )
    }

    fn tier(mut s: ServerState, priority: u32, weight: u32) -> ServerState {
        s.priority = priority;
        s.weight = weight;
        s
    }

    #[test]
    fn stitched_priority_ranks_after_role_and_capacity_before_the_longest_run() {
        let pol = StitchingPolicy::default();
        let none = Excludes::default();
        // Priority beats fewer connections among replicas.
        let busy = tier(frozen(0, 2, 1001), 0, 1);
        busy.connections.store(5, Ordering::Relaxed);
        let v = view(vec![busy, tier(frozen(1, 2, 1001), 1, 1)]);
        assert_eq!(served_by(&plan_with(&v, 10, &pol, &none, &[])).0, 0);

        // Weight splits sessions inside a tier; a backup tier gets none.
        let v = view(vec![
            tier(frozen(0, 2, 1001), 0, 2),
            tier(frozen(1, 2, 1001), 0, 1),
            tier(frozen(2, 2, 1001), 1, 10),
        ]);
        for _ in 0..30 {
            let up = served_by(&plan_with(&v, 10, &pol, &none, &[])).0;
            v.upstreams[up].connections.fetch_add(1, Ordering::Relaxed);
        }
        let conns = |i: usize| v.upstreams[i].connections.load(Ordering::Relaxed);
        assert_eq!((conns(0), conns(1), conns(2)), (20, 10, 0));

        // Role preference outranks priority: B and C overlap on [1900, 2001).
        let fleet = || {
            vec![
                frozen(0, 2, 1001),
                tier(frozen(1, 1001, 2001), 5, 1),
                tier(live(2, 1900, 2300), 0, 1),
            ]
        };
        let v = view(fleet());
        assert_eq!(served_by(&plan_with(&v, 1950, &pol, &none, &[])).0, 1);

        // Without a role preference, priority outranks the longest run...
        let any = StitchingPolicy {
            history_preference: HistoryPreference::Any,
            ..Default::default()
        };
        assert_eq!(
            served_by(&plan_with(&v, 1950, &any, &none, &[])),
            (2, 1950, u32::MAX)
        );
        let mut swapped = fleet();
        swapped[1].priority = 0;
        swapped[2].priority = 5;
        let v = view(swapped);
        assert_eq!(
            served_by(&plan_with(&v, 1950, &any, &none, &[])),
            (1, 1950, 2001),
            "the preferred tier serves although the live run is longer"
        );
        // ...and at equal priority the longest run still wins (PL3).
        let v = view(vec![
            frozen(0, 2, 1001),
            frozen(1, 1001, 2001),
            live(2, 1900, 2300),
        ]);
        assert_eq!(served_by(&plan_with(&v, 1950, &any, &none, &[])).0, 2);

        // Capacity outranks priority: a full preferred replica loses.
        let mut full = tier(frozen(0, 2, 1001), 0, 1);
        full.max_sessions = Some(1);
        full.connections.store(1, Ordering::Relaxed);
        let v = view(vec![full, tier(frozen(1, 2, 1001), 9, 1)]);
        assert_eq!(served_by(&plan_with(&v, 10, &pol, &none, &[])).0, 1);

        // A failover leaves a failed preferred upstream for a backup tier.
        let v = view(vec![
            tier(frozen(0, 2, 1001), 0, 1),
            tier(frozen(1, 2, 1001), 5, 1),
        ]);
        let soft = Excludes {
            soft: Some(v.upstreams[0].endpoint.clone()),
            ..Default::default()
        };
        assert_eq!(served_by(&plan_with(&v, 10, &pol, &none, &[])).0, 0);
        assert_eq!(served_by(&plan_with(&v, 10, &pol, &soft, &[])).0, 1);
    }

    #[test]
    fn stitched_the_longest_run_outranks_load_and_weight_within_a_tier() {
        let none = Excludes::default();
        let pol = StitchingPolicy::default();
        // Overlapping archives: B's longer run saves a handoff at 1001 although
        // B is the busier one.
        let overlap = || vec![frozen(0, 2, 1001), frozen(1, 2, 2001), live(2, 1900, 2300)];
        let v = view(overlap());
        v.upstreams[1].connections.store(1, Ordering::Relaxed);
        assert_eq!(
            served_by(&plan_with(&v, 10, &pol, &none, &[])),
            (1, 10, 2001)
        );
        // Nor does a higher weight pull the session onto the shorter run.
        let mut weighted = overlap();
        weighted[0].weight = 10;
        let v = view(weighted);
        v.upstreams[1].connections.store(5, Ordering::Relaxed);
        assert_eq!(
            served_by(&plan_with(&v, 10, &pol, &none, &[])),
            (1, 10, 2001)
        );

        // PL3: without a role preference the longest run wins although the
        // live node already has a tip reader.
        let any = StitchingPolicy {
            history_preference: HistoryPreference::Any,
            ..Default::default()
        };
        let v = view(fleet());
        v.upstreams[2].connections.store(1, Ordering::Relaxed);
        assert_eq!(
            served_by(&plan_with(&v, 1950, &any, &none, &[])),
            (2, 1950, u32::MAX)
        );
    }

    #[test]
    fn stitched_load_does_not_count_the_sessions_own_slots() {
        // One connection on each replica: on 0 it is the session's own link.
        let a = frozen(0, 2, 1001);
        let b = frozen(1, 2, 1001);
        a.connections.store(1, Ordering::Relaxed);
        b.connections.store(1, Ordering::Relaxed);
        let v = view(vec![a, b]);
        let pol = StitchingPolicy::default();
        let none = Excludes::default();
        assert_eq!(served_by(&plan_with(&v, 10, &pol, &none, &[0])).0, 0);
        assert_eq!(served_by(&plan_with(&v, 10, &pol, &none, &[1])).0, 1);
    }

    // The control API: operator-disabled upstreams and operator moves.

    #[test]
    fn pool_operator_disabled_upstream_is_skipped_by_new_sessions_and_failover() {
        let mut drained = tiered(0, 0, 0, 1);
        drained.operator_disabled = true;
        let v = pool_view(vec![drained, tiered(1, 5, 9, 1)]);
        assert_eq!(pick(&v, at(100), None, false), Some(1));
        assert_eq!(pick(&v, at(100), None, true), Some(1));

        let mut drained = tiered(0, 0, 0, 1);
        drained.operator_disabled = true;
        let v = pool_view(vec![drained]);
        assert_eq!(pick(&v, at(100), None, false), None);
        assert_eq!(pick(&v, at(100), None, true), None);
        assert!(!eligible(
            &v,
            v.up(0).unwrap(),
            Need::default(),
            100,
            Union::Online,
            Instant::now()
        ));
    }

    #[test]
    fn pool_move_needs_another_upstream_whose_range_starts_by_the_next_block() {
        // The archive holds old history; the pruned node starts at 400.
        let fleet =
            |pruned: ServerState| pool_view(vec![pool_server(0, 0, (1, 500), (0, 0)), pruned]);
        let alternative = |v: &FleetView, block: Option<u32>| {
            v.upstreams
                .iter()
                .any(|u| u.index != 0 && can_take_over(u, block))
        };
        let v = fleet(pool_server(1, 0, (400, 500), (0, 0)));
        // A reader catching up at block 100 has nowhere to go.
        assert!(!alternative(&v, Some(100)));
        // A session near the tip, even past the pruned node's end, can move.
        assert!(alternative(&v, Some(450)));
        assert!(alternative(&v, Some(500)));
        // Before any block is requested, any online upstream will do.
        assert!(alternative(&v, None));

        let mut down = pool_server(1, 0, (400, 500), (0, 0));
        down.online = false;
        assert!(!alternative(&fleet(down), Some(450)));
        let mut drained = pool_server(1, 0, (400, 500), (0, 0));
        drained.operator_disabled = true;
        assert!(!alternative(&fleet(drained), None));

        // A frozen upstream must hold the block itself.
        let v = view(vec![frozen(0, 2, 1001), frozen(1, 1001, 2001)]);
        assert!(can_take_over(&v.upstreams[1], Some(1500)));
        assert!(!can_take_over(&v.upstreams[1], Some(500)));
        assert!(!can_take_over(&v.upstreams[1], Some(2001)));
    }

    #[test]
    fn a_move_never_counts_on_a_full_or_quarantined_upstream() {
        // The failover after a move skips both, so a drain must leave such
        // a session where it is.
        let mut full = pool_server(1, 1, (1, 500), (0, 0));
        full.max_sessions = Some(1);
        let v = pool_view(vec![pool_server(0, 0, (1, 500), (0, 0)), full]);
        assert!(!can_take_over(v.up(1).unwrap(), Some(450)));
        assert!(!can_take_over(v.up(1).unwrap(), None));
        assert_eq!(
            pick(&v, at(450), Some(0), true),
            Some(0),
            "the pick skips it"
        );
        // A slot frees up: it can take the session again.
        v.up(1).unwrap().connections.store(0, Ordering::Relaxed);
        assert!(can_take_over(v.up(1).unwrap(), Some(450)));

        let mut quarantined = pool_server(1, 0, (1, 500), (0, 0));
        quarantined.quarantines.push(BlockQuarantine {
            block: 400,
            range: 100,
            until: Instant::now() + NOW_TTL,
        });
        let v = pool_view(vec![pool_server(0, 0, (1, 500), (0, 0)), quarantined]);
        let u = v.up(1).unwrap();
        assert!(!can_take_over(u, Some(450)));
        assert!(can_take_over(u, Some(350)), "only inside the quarantine");
        assert!(can_take_over(u, None));

        // Stitched: a full replica of the archive the session is on.
        let mut full = frozen(1, 2, 1001);
        full.max_sessions = Some(1);
        full.connections.store(1, Ordering::Relaxed);
        let v = view(vec![frozen(0, 2, 1001), full]);
        assert!(!can_take_over(&v.upstreams[1], Some(500)));
    }

    #[test]
    fn stitched_sessions_leave_an_operator_disabled_upstream() {
        let pol = StitchingPolicy::default();
        let none = Excludes::default();
        // A disabled replica: its twin serves.
        let mut drained = frozen(0, 2, 1001);
        drained.operator_disabled = true;
        let v = view(vec![drained, frozen(1, 2, 1001)]);
        assert_eq!(served_by(&plan_with(&v, 10, &pol, &none, &[])).0, 1);
        // The only archive of a range: the session holds as if it were
        // offline (bounded), and its coverage stays.
        let mut drained = frozen(0, 2, 1001);
        drained.operator_disabled = true;
        let v = view(vec![drained, frozen(1, 1001, 2001)]);
        assert!(v.ids.has(500));
        assert_eq!(
            plan_with(&v, 500, &pol, &none, &[]),
            Step::Hold {
                block: 500,
                why: HoldWhy::Offline,
                bounded: true
            }
        );
        assert_eq!(v.online, 1, "a disabled upstream is not counted online");
    }
}
