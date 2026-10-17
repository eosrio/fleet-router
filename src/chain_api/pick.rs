//! Candidate choice for chain API requests.
//!
//! Pure, like the SHiP planner: the caller snapshots its directory of
//! upstream APIs into [`ApiView`]s for each request, asks for an attempt
//! order, runs the attempts and classifies every answer with [`classify`].
//!
//! Block requests are split by region. Block `n` is irreversible when it is
//! at or below the fleet LIB (the SHiP fleet LIB, or the highest LIB a live
//! API reports, which covers a cold start), or when a verified frozen range
//! server holds it, because a block log holds only irreversible blocks.
//! Reversible blocks go to live nodes only, highest head first, and their
//! first HTTP answer is final: fork answers must come from one node.
//! Irreversible blocks go through candidate tiers, and an Unknown Block or a
//! 404 moves on to the next candidate. When a live node is a candidate, one
//! attempt is kept for it, so a real nodeos writes the final error.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::time::Duration;

use serde::Deserialize;

use crate::fleet::planner::rendezvous;
use crate::fleet::MetricLabel;
use crate::models::{BlockSource, Role};
use crate::ship::Range;

use super::errors::UNKNOWN_BLOCK_CODE;
use super::route::BlockEndpoint;

/// How long an upstream API that answered 503 is skipped.
pub const BUSY_TTL: Duration = Duration::from_secs(2);

/// Largest non-2xx body buffered to classify it. A larger answer is final.
pub const PEEK_LIMIT: usize = 64 * 1024;

/// Blocks `n` with the same `n >> 12` share a rendezvous key, so nearby
/// blocks prefer the same frozen replica and its page cache stays warm.
const RENDEZVOUS_CHUNK_SHIFT: u32 = 12;

const GET_INFO: &str = "/v1/chain/get_info";

/// What the planner knows about one upstream API at the time of a request.
#[derive(Clone, Copy, Debug)]
pub struct ApiView<'a> {
    /// `ServerState::index` of the server.
    pub index: usize,
    /// Canonical `host:port` of the API: the rendezvous key.
    pub endpoint: &'a str,
    pub role: Role,
    /// Lower is preferred within a tier.
    pub priority: u32,
    /// Share within a priority tier. Treated as at least 1.
    pub weight: u32,
    /// Up per its probes, with a matching chain_id.
    pub up: bool,
    /// Answered 503 less than [`BUSY_TTL`] ago.
    pub busy: bool,
    /// Requests in flight to this API.
    pub in_flight: usize,
    /// The paths the API advertises. `None` is unknown: a live API is
    /// assumed to serve every path, a frozen one none.
    pub apis: Option<&'a BTreeSet<String>>,
    /// Live: effective head (the probed head, raised by its SHiP head).
    pub head: Option<u32>,
    /// Live: probed `last_irreversible_block_num`.
    pub lib: Option<u32>,
    /// Live: probed `earliest_available_block_num`, when the node reports it.
    pub earliest: Option<u32>,
    /// Frozen: its identity was verified at least once (sticky).
    pub verified: bool,
    /// Frozen: the blocks its chain API serves, `[begin, end)`.
    pub blocks: Option<Range>,
}

impl ApiView<'_> {
    fn is_live(&self) -> bool {
        self.role.is_live()
    }

    fn lists(&self, path: &str) -> bool {
        match self.apis {
            Some(apis) => apis.contains(path),
            None => self.is_live(),
        }
    }

    /// Up, not busy, and serving `path`.
    fn usable(&self, path: &str) -> bool {
        self.up && !self.busy && self.lists(path)
    }

    /// A verified frozen API whose blocks include `n`, up or not.
    fn archives(&self, n: u32) -> bool {
        self.role == Role::Frozen && self.verified && self.blocks.is_some_and(|r| r.contains(n))
    }
}

/// Weighted least-load: `in_flight / weight` ascending, compared without
/// dividing; equal load goes to the higher weight.
fn by_load(a: &ApiView<'_>, b: &ApiView<'_>) -> Ordering {
    let (wa, wb) = (a.weight.max(1), b.weight.max(1));
    let load = |n: usize, w: u32| (n as u128) * u128::from(w);
    load(a.in_flight, wb)
        .cmp(&load(b.in_flight, wa))
        .then(wb.cmp(&wa))
}

/// Order inside a candidate tier: priority, weighted load, then rendezvous
/// on the block's chunk (highest first), then index.
fn tier_cmp(n: u32) -> impl Fn(&&ApiView<'_>, &&ApiView<'_>) -> Ordering {
    let chunk = u64::from(n >> RENDEZVOUS_CHUNK_SHIFT);
    move |a, b| {
        a.priority
            .cmp(&b.priority)
            .then_with(|| by_load(a, b))
            .then_with(|| rendezvous(chunk, b.endpoint).cmp(&rendezvous(chunk, a.endpoint)))
            .then(a.index.cmp(&b.index))
    }
}

/// Whether a block is final on every node.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Region {
    /// At or below the fleet LIB, or held by a verified frozen range server.
    Irreversible,
    /// Above the fleet LIB: live nodes only, the first answer is final.
    Reversible,
}

impl Region {
    /// `region` label of `fleet_router_api_block_requests_total`.
    pub fn as_str(self) -> &'static str {
        match self {
            Region::Irreversible => "irreversible",
            Region::Reversible => "reversible",
        }
    }
}

/// The fleet LIB: the higher of the SHiP fleet LIB and every up live API's
/// probed LIB. The probed LIB answers before any SHiP link has reported.
pub fn fleet_lib(ship_lib: Option<u32>, apis: &[ApiView<'_>]) -> Option<u32> {
    apis.iter()
        .filter(|a| a.up && a.is_live())
        .filter_map(|a| a.lib)
        .chain(ship_lib)
        .max()
}

/// The region of block `n`.
pub fn region(n: u32, fleet_lib: Option<u32>, apis: &[ApiView<'_>]) -> Region {
    if fleet_lib.is_some_and(|lib| n <= lib) || apis.iter().any(|a| a.archives(n)) {
        Region::Irreversible
    } else {
        Region::Reversible
    }
}

/// A block request, as the planner needs it.
#[derive(Clone, Copy, Debug)]
pub struct BlockQuery {
    pub endpoint: BlockEndpoint,
    /// The body's routing hint ([`super::hint::hint`]).
    pub hint: Option<u32>,
    /// The fleet LIB of the listener's SHiP view.
    pub ship_lib: Option<u32>,
    pub block_source: BlockSource,
    pub max_attempts: u32,
}

/// Where a block request goes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlockPlan {
    /// No routing hint: the body goes unchanged to one live API, chosen as
    /// for a state call, and never to a frozen one.
    Unhinted,
    /// Attempt order, as [`ApiView::index`] values, at most `max_attempts`
    /// long. Empty when no API is a candidate: the client gets Busy.
    Routed {
        n: u32,
        region: Region,
        order: Vec<usize>,
    },
}

/// Plan a block request.
pub fn plan_block(q: &BlockQuery, apis: &[ApiView<'_>]) -> BlockPlan {
    let Some(n) = q.hint else {
        return BlockPlan::Unhinted;
    };
    let cap = usize::try_from(q.max_attempts.max(1)).unwrap_or(usize::MAX);
    let region = region(n, fleet_lib(q.ship_lib, apis), apis);
    let order = match region {
        Region::Reversible => reversible_order(n, q.endpoint.path(), apis, cap),
        Region::Irreversible => irreversible_order(n, q, apis, cap),
    };
    BlockPlan::Routed { n, region, order }
}

/// Up live APIs serving `path`: those whose head reached `n` first, by
/// priority, head (highest first) and load; then the rest by head.
fn reversible_order(n: u32, path: &str, apis: &[ApiView<'_>], cap: usize) -> Vec<usize> {
    let (mut ahead, mut behind): (Vec<&ApiView<'_>>, Vec<&ApiView<'_>>) = apis
        .iter()
        .filter(|a| a.is_live() && a.usable(path))
        .partition(|a| a.head.is_some_and(|h| h >= n));
    ahead.sort_by(|a, b| {
        a.priority
            .cmp(&b.priority)
            .then(b.head.cmp(&a.head))
            .then_with(|| by_load(a, b))
            .then(a.index.cmp(&b.index))
    });
    behind.sort_by(|a, b| {
        b.head
            .cmp(&a.head)
            .then_with(|| by_load(a, b))
            .then(a.index.cmp(&b.index))
    });
    ahead
        .into_iter()
        .chain(behind)
        .take(cap)
        .map(|a| a.index)
        .collect()
}

/// The candidate tiers of an irreversible block, in order, each sorted.
///
/// - F: verified frozen APIs whose blocks hold `n`.
/// - L: live APIs whose block log holds `n` (`earliest <= n <= head`).
/// - U: live APIs that do not report `earliest`.
/// - X: live APIs whose block log begins above `n`. They answer a genuine
///   Unknown Block.
///
/// `frozen_first` tries F, L, U, X; `live_first` L, F, U, X. `get_block`
/// (decoded action data) is served by live nodes only: L, U, X.
fn irreversible_order(n: u32, q: &BlockQuery, apis: &[ApiView<'_>], cap: usize) -> Vec<usize> {
    let path = q.endpoint.path();
    let live = |pred: &dyn Fn(&ApiView<'_>) -> bool| -> Vec<&ApiView<'_>> {
        let mut tier: Vec<&ApiView<'_>> = apis
            .iter()
            .filter(|a| a.is_live() && a.usable(path) && pred(a))
            .collect();
        tier.sort_by(tier_cmp(n));
        tier
    };
    let l = live(&|a| a.earliest.is_some_and(|e| e <= n) && a.head.is_some_and(|h| n <= h));
    let u = live(&|a| a.earliest.is_none());
    let x = live(&|a| a.earliest.is_some_and(|e| e > n));
    let mut f: Vec<&ApiView<'_>> = if q.endpoint == BlockEndpoint::Block {
        Vec::new()
    } else {
        apis.iter()
            .filter(|a| a.archives(n) && a.usable(path))
            .collect()
    };
    f.sort_by(tier_cmp(n));

    let tiers: [&[&ApiView<'_>]; 4] = match (q.endpoint, q.block_source) {
        (BlockEndpoint::Block, _) | (_, BlockSource::LiveFirst) => [&l, &f, &u, &x],
        (_, BlockSource::FrozenFirst) => [&f, &l, &u, &x],
    };
    let mut order: Vec<&ApiView<'_>> = tiers
        .iter()
        .flat_map(|t| t.iter().copied())
        .take(cap)
        .collect();

    // Keep one attempt for a live node, so a real nodeos writes the final
    // error. Not with a single attempt: it would send every block to a live
    // node, even one whose log begins above the block.
    if cap >= 2 && !order.iter().any(|a| a.is_live()) {
        let first_live = [&l, &u, &x].into_iter().flat_map(|t| t.first()).next();
        if let (Some(&first_live), Some(last)) = (first_live, order.last_mut()) {
            *last = first_live;
        }
    }
    order.into_iter().map(|a| a.index).collect()
}

/// A state (or exec, or unhinted block) request.
#[derive(Clone, Copy, Debug)]
pub struct StateQuery<'a> {
    /// The request path, checked against each API's advertised set.
    pub path: &'a str,
    /// The fleet head of the listener's SHiP view.
    pub ship_head: Option<u32>,
    /// `live_lag_blocks`; 0 disables the laggard rule.
    pub lag: u32,
    /// Round-robin position; the caller advances it once per request.
    pub rr: u64,
    pub max_attempts: u32,
}

/// Attempt order for a state call: up live APIs serving the path, minus
/// those more than `lag` blocks behind the fleet head (all of them when all
/// lag), by priority, weighted load, then round-robin.
pub fn state_order(q: &StateQuery<'_>, apis: &[ApiView<'_>]) -> Vec<usize> {
    let mut cands: Vec<&ApiView<'_>> = apis
        .iter()
        .filter(|a| a.is_live() && a.usable(q.path))
        .collect();
    let fleet_head = cands.iter().filter_map(|a| a.head).chain(q.ship_head).max();
    if let (true, Some(head)) = (q.lag > 0, fleet_head) {
        let floor = head.saturating_sub(q.lag);
        let fresh: Vec<&ApiView<'_>> = cands
            .iter()
            .copied()
            .filter(|a| a.head.is_some_and(|h| h >= floor))
            .collect();
        if !fresh.is_empty() {
            cands = fresh;
        }
    }
    cands.sort_by_key(|a| a.index);
    let k = cands.len().max(1);
    let shift = usize::try_from(q.rr % k as u64).unwrap_or(0);
    let mut ranked: Vec<(usize, &ApiView<'_>)> = cands
        .into_iter()
        .enumerate()
        .map(|(i, a)| ((i + k - shift) % k, a))
        .collect();
    ranked.sort_by(|(ra, a), (rb, b)| {
        a.priority
            .cmp(&b.priority)
            .then_with(|| by_load(a, b))
            .then(ra.cmp(rb))
    });
    let cap = usize::try_from(q.max_attempts.max(1)).unwrap_or(usize::MAX);
    ranked.into_iter().take(cap).map(|(_, a)| a.index).collect()
}

/// The live API that answers `get_info`.
///
/// Head, LIB and TAPOS values must come from one node, and a client must not
/// see the head step back between calls. So the current primary stays while
/// it is up, until another up API's head reaches the primary's head + 2.
/// Otherwise the up live API with the highest head is chosen (ties by
/// priority, then index).
pub fn info_primary(apis: &[ApiView<'_>], current: Option<usize>) -> Option<usize> {
    let up = || {
        apis.iter()
            .filter(|a| a.is_live() && a.up && a.lists(GET_INFO))
    };
    let best = up().min_by(|a, b| {
        b.head
            .cmp(&a.head)
            .then(a.priority.cmp(&b.priority))
            .then(a.index.cmp(&b.index))
    })?;
    if let Some(primary) = current.and_then(|i| up().find(|a| a.index == i)) {
        let overtaken = match (best.head, primary.head) {
            (Some(best), Some(head)) => best >= head.saturating_add(2),
            (Some(_), None) => true,
            (None, _) => false,
        };
        if !overtaken {
            return Some(primary.index);
        }
    }
    Some(best.index)
}

/// Attempt order for `get_info`: the primary, then the other up live APIs
/// by head (highest first). A busy API is skipped.
pub fn info_order(apis: &[ApiView<'_>], primary: Option<usize>, max_attempts: u32) -> Vec<usize> {
    let mut rest: Vec<&ApiView<'_>> = apis
        .iter()
        .filter(|a| a.is_live() && a.usable(GET_INFO) && Some(a.index) != primary)
        .collect();
    rest.sort_by(|a, b| {
        b.head
            .cmp(&a.head)
            .then(a.priority.cmp(&b.priority))
            .then(a.index.cmp(&b.index))
    });
    let first = primary.filter(|&p| {
        apis.iter()
            .any(|a| a.index == p && a.is_live() && a.usable(GET_INFO))
    });
    let cap = usize::try_from(max_attempts.max(1)).unwrap_or(usize::MAX);
    first
        .into_iter()
        .chain(rest.into_iter().map(|a| a.index))
        .take(cap)
        .collect()
}

/// Targets of a push, in order: the up `push_servers` (configured order),
/// or the `get_info` primary when none is configured. A push is written to
/// one target; the next is tried only if the connection failed before any
/// byte was written.
pub fn push_targets(
    apis: &[ApiView<'_>],
    path: &str,
    push_servers: &[usize],
    primary: Option<usize>,
) -> Vec<usize> {
    let usable = |i: usize| {
        apis.iter()
            .any(|a| a.index == i && a.is_live() && a.usable(path))
    };
    if push_servers.is_empty() {
        return primary.filter(|&p| usable(p)).into_iter().collect();
    }
    push_servers
        .iter()
        .copied()
        .filter(|&i| usable(i))
        .collect()
}

/// What the next attempt does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NextAttempt {
    /// Send to this API (its slot is free).
    Try(usize),
    /// Every remaining candidate is full: wait up to `upstream_queue_ms` for
    /// a slot on this one, the first remaining, then answer Busy.
    Wait(usize),
    /// Every candidate was tried.
    Exhausted,
}

/// The next attempt over `order`: the first untried candidate whose slot is
/// free. A full candidate is skipped without using an attempt.
pub fn next_attempt(order: &[usize], tried: &[usize], full: impl Fn(usize) -> bool) -> NextAttempt {
    let mut remaining = order.iter().copied().filter(|i| !tried.contains(i));
    let Some(first) = remaining.next() else {
        return NextAttempt::Exhausted;
    };
    if !full(first) {
        return NextAttempt::Try(first);
    }
    match remaining.find(|&i| !full(i)) {
        Some(i) => NextAttempt::Try(i),
        None => NextAttempt::Wait(first),
    }
}

/// `reason` label of `fleet_router_api_retries_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RetryReason {
    /// 400 with nodeos's `unknown_block_exception` (irreversible only).
    UnknownBlock,
    /// 404 (irreversible only).
    NotFound,
    /// 503.
    Busy,
    /// The connection failed before a response head.
    Connect,
    /// No response head within `upstream_timeout_ms`.
    Timeout,
}

impl MetricLabel for RetryReason {
    const ALL: &'static [RetryReason] = &[
        RetryReason::UnknownBlock,
        RetryReason::NotFound,
        RetryReason::Busy,
        RetryReason::Connect,
        RetryReason::Timeout,
    ];

    fn as_str(self) -> &'static str {
        match self {
            RetryReason::UnknownBlock => "unknown_block",
            RetryReason::NotFound => "not_found",
            RetryReason::Busy => "busy",
            RetryReason::Connect => "connect",
            RetryReason::Timeout => "timeout",
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

impl RetryReason {
    /// The API is down until its next good probe.
    pub fn marks_down(self) -> bool {
        matches!(self, RetryReason::Connect | RetryReason::Timeout)
    }

    /// The API is skipped for [`BUSY_TTL`].
    pub fn marks_busy(self) -> bool {
        self == RetryReason::Busy
    }
}

/// One attempt's result.
#[derive(Clone, Copy, Debug)]
pub enum Answer<'a> {
    /// The connection failed before a response head.
    Connect,
    /// No response head within `upstream_timeout_ms`.
    Timeout,
    /// A response head. `body` is the buffered body when [`needs_peek`]
    /// asked for it, else `None`.
    Response { status: u16, body: Option<&'a [u8]> },
}

/// What to do with an attempt's result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Send this response to the client.
    Final,
    /// Try the next candidate. With `keep`, the answer is kept: when every
    /// attempt is used, the client gets the last kept answer from a live
    /// API, else the last kept one ([`exhausted`]).
    Next { reason: RetryReason, keep: bool },
}

/// Whether a response must be buffered (up to [`PEEK_LIMIT`]) before it is
/// classified: only a 400 or 404 on an irreversible block, which may move on
/// to the next candidate. A larger declared body is not buffered and is
/// final, as is every 2xx (streamed).
pub fn needs_peek(region: Region, status: u16, content_length: Option<u64>) -> bool {
    region == Region::Irreversible
        && matches!(status, 400 | 404)
        && content_length.is_none_or(|len| len <= PEEK_LIMIT as u64)
}

/// Classify an attempt's result.
///
/// | Result | Reversible | Irreversible |
/// |---|---|---|
/// | connect error, timeout | next, mark down | next, mark down |
/// | 503 | next, busy | next, busy |
/// | 400 Unknown Block | final | next, keep |
/// | 404 | final | next, keep |
/// | anything else | final | final |
///
/// State calls follow the reversible column.
pub fn classify(region: Region, answer: Answer<'_>) -> Outcome {
    let next = |reason, keep| Outcome::Next { reason, keep };
    match answer {
        Answer::Connect => next(RetryReason::Connect, false),
        Answer::Timeout => next(RetryReason::Timeout, false),
        Answer::Response { status: 503, .. } => next(RetryReason::Busy, false),
        Answer::Response { .. } if region == Region::Reversible => Outcome::Final,
        Answer::Response {
            status: 400,
            body: Some(body),
        } if is_unknown_block(body) => next(RetryReason::UnknownBlock, true),
        Answer::Response {
            status: 404,
            body: Some(_),
        } => next(RetryReason::NotFound, true),
        Answer::Response { .. } => Outcome::Final,
    }
}

/// Whether `body` is a nodeos error with `error.code` 3100002.
fn is_unknown_block(body: &[u8]) -> bool {
    #[derive(Deserialize)]
    struct ErrorBody {
        error: ErrorInfo,
    }
    #[derive(Deserialize)]
    struct ErrorInfo {
        code: i64,
    }
    serde_json::from_slice::<ErrorBody>(body).is_ok_and(|b| b.error.code == UNKNOWN_BLOCK_CODE)
}

/// The answer once every attempt is used: the last kept answer from a live
/// API, else the last kept answer. `None` (nothing kept) means Busy.
pub fn exhausted<T>(kept: &[(Role, T)]) -> Option<&T> {
    kept.iter()
        .rev()
        .find(|(role, _)| role.is_live())
        .or_else(|| kept.last())
        .map(|(_, answer)| answer)
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use super::*;

    const ENDPOINTS: [&str; 8] = [
        "10.0.3.10:8890",
        "10.0.3.11:8890",
        "10.0.3.12:8890",
        "10.0.3.13:8890",
        "10.0.1.10:8888",
        "10.0.1.11:8888",
        "10.0.1.12:8888",
        "10.0.1.13:8888",
    ];

    /// What a frozen range server advertises.
    static FROZEN_APIS: LazyLock<BTreeSet<String>> = LazyLock::new(|| {
        [
            "/v1/chain/get_block_header",
            "/v1/chain/get_block_info",
            "/v1/chain/get_raw_block",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    });

    /// A live node with only state endpoints.
    static STATE_ONLY: LazyLock<BTreeSet<String>> = LazyLock::new(|| {
        ["/v1/chain/get_info", "/v1/chain/get_account"]
            .into_iter()
            .map(String::from)
            .collect()
    });

    fn base(index: usize, role: Role) -> ApiView<'static> {
        ApiView {
            index,
            endpoint: ENDPOINTS[index],
            role,
            priority: 0,
            weight: 1,
            up: true,
            busy: false,
            in_flight: 0,
            apis: None,
            head: None,
            lib: None,
            earliest: None,
            verified: false,
            blocks: None,
        }
    }

    /// A verified frozen API serving `[begin, end)`. Indices 0-3.
    fn frozen(index: usize, begin: u32, end: u32) -> ApiView<'static> {
        ApiView {
            apis: Some(&FROZEN_APIS),
            verified: true,
            blocks: Some(Range { begin, end }),
            ..base(index, Role::Frozen)
        }
    }

    /// A live API. Indices 4-7.
    fn live(index: usize, earliest: Option<u32>, lib: u32, head: u32) -> ApiView<'static> {
        ApiView {
            head: Some(head),
            lib: Some(lib),
            earliest,
            ..base(index, Role::Live)
        }
    }

    fn query(endpoint: BlockEndpoint, n: u32) -> BlockQuery {
        BlockQuery {
            endpoint,
            hint: Some(n),
            ship_lib: None,
            block_source: BlockSource::FrozenFirst,
            max_attempts: 3,
        }
    }

    fn raw(n: u32) -> BlockQuery {
        query(BlockEndpoint::RawBlock, n)
    }

    fn order(q: &BlockQuery, apis: &[ApiView<'_>]) -> (Region, Vec<usize>) {
        match plan_block(q, apis) {
            BlockPlan::Routed { region, order, .. } => (region, order),
            BlockPlan::Unhinted => panic!("unexpected unhinted plan"),
        }
    }

    // CU4: region rule
    #[test]
    fn region_uses_the_ship_lib_the_probed_live_lib_and_verified_archives() {
        let live_node = live(4, Some(1), 900, 1000);
        // Cold start: no SHiP LIB, the probed live LIB alone.
        assert_eq!(fleet_lib(None, &[live_node]), Some(900));
        assert_eq!(region(900, Some(900), &[live_node]), Region::Irreversible);
        assert_eq!(region(901, Some(900), &[live_node]), Region::Reversible);
        let (r, o) = order(&raw(900), &[live_node]);
        assert_eq!((r, o), (Region::Irreversible, vec![4]));
        let (r, _) = order(&raw(901), &[live_node]);
        assert_eq!(r, Region::Reversible);

        // The SHiP LIB counts too, the higher one wins.
        assert_eq!(fleet_lib(Some(950), &[live_node]), Some(950));
        assert_eq!(fleet_lib(Some(850), &[live_node]), Some(900));
        let q = BlockQuery {
            ship_lib: Some(950),
            ..raw(940)
        };
        assert_eq!(order(&q, &[live_node]).0, Region::Irreversible);

        // A down live API's LIB does not count.
        let down = ApiView {
            up: false,
            ..live_node
        };
        assert_eq!(fleet_lib(None, &[down]), None);

        // No LIB at all: a block in a verified archive is still irreversible,
        // whether or not that archive is up right now.
        let archive = frozen(0, 1, 1001);
        assert_eq!(region(1, None, &[archive]), Region::Irreversible);
        assert_eq!(region(1000, None, &[archive]), Region::Irreversible);
        assert_eq!(region(1001, None, &[archive]), Region::Reversible);
        let down_archive = ApiView {
            up: false,
            ..archive
        };
        assert_eq!(region(5, None, &[down_archive]), Region::Irreversible);
        let unverified = ApiView {
            verified: false,
            ..archive
        };
        assert_eq!(region(5, None, &[unverified]), Region::Reversible);
    }

    // CU4: reversible ordering
    #[test]
    fn reversible_blocks_go_to_live_nodes_highest_head_first() {
        let apis = [
            frozen(0, 1, 2001),
            live(4, Some(1), 900, 1004),
            live(5, Some(1), 900, 1010),
            live(6, Some(1), 900, 998),
            ApiView {
                busy: true,
                ..live(7, Some(1), 900, 1020)
            },
        ];
        // n = 2500 is above every head: by head.
        assert_eq!(
            order(&raw(2500), &apis),
            (Region::Reversible, vec![5, 4, 6])
        );
        // n = 1003: the nodes whose head reached it first, then the rest.
        let (r, o) = order(&raw(1003), &[apis[1], apis[2], apis[3]]);
        assert_eq!(r, Region::Reversible);
        assert_eq!(o, vec![5, 4, 6]);

        // Priority ranks first among the nodes that reached n.
        let preferred = ApiView {
            priority: 0,
            ..live(4, Some(1), 900, 1004)
        };
        let backup = ApiView {
            priority: 1,
            ..live(5, Some(1), 900, 1010)
        };
        let behind = ApiView {
            priority: 0,
            ..live(6, Some(1), 900, 1000)
        };
        assert_eq!(
            order(&raw(1003), &[backup, preferred, behind]).1,
            vec![4, 5, 6]
        );

        // Equal priority and head: the lighter load first.
        let busy_node = ApiView {
            in_flight: 5,
            ..live(4, Some(1), 900, 1010)
        };
        let idle_node = live(5, Some(1), 900, 1010);
        assert_eq!(order(&raw(1003), &[busy_node, idle_node]).1, vec![5, 4]);

        // max_attempts caps the list; frozen APIs are never used.
        let q = BlockQuery {
            max_attempts: 2,
            ..raw(2500)
        };
        assert_eq!(order(&q, &apis).1, vec![5, 4]);
    }

    // CU4: the three tier orders
    #[test]
    fn irreversible_tiers_follow_block_source() {
        let f = frozen(0, 1, 1001);
        let l = live(4, Some(1), 5000, 6000);
        let u = live(5, None, 5000, 6000);
        let x = live(6, Some(3000), 5000, 6000);
        let apis = [x, u, l, f];
        let q = BlockQuery {
            max_attempts: 8,
            ..raw(500)
        };
        assert_eq!(order(&q, &apis), (Region::Irreversible, vec![0, 4, 5, 6]));
        let live_first = BlockQuery {
            block_source: BlockSource::LiveFirst,
            ..q
        };
        assert_eq!(order(&live_first, &apis).1, vec![4, 0, 5, 6]);
    }

    // CU4: get_block
    #[test]
    fn get_block_is_served_by_live_nodes_only() {
        let f = ApiView {
            // Even an archive that would list it is skipped.
            apis: None,
            ..frozen(0, 1, 1001)
        };
        let l = live(4, Some(1), 5000, 6000);
        let u = live(5, None, 5000, 6000);
        let x = live(6, Some(3000), 5000, 6000);
        let q = BlockQuery {
            max_attempts: 8,
            ..query(BlockEndpoint::Block, 500)
        };
        assert_eq!(order(&q, &[f, x, u, l]).1, vec![4, 5, 6]);
        // Only an archive holds the block: no candidate, the client gets Busy.
        assert_eq!(order(&q, &[frozen(0, 1, 1001)]).1, Vec::<usize>::new());
        // The same block over get_raw_block goes to the archive.
        assert_eq!(order(&raw(500), &[frozen(0, 1, 1001)]).1, vec![0]);
    }

    // CU4: reserved live slot
    #[test]
    fn one_attempt_is_kept_for_a_live_node() {
        let replicas = [frozen(0, 1, 1001), frozen(1, 1, 1001), frozen(2, 1, 1001)];
        let x = live(6, Some(3000), 5000, 6000);
        let mut apis = replicas.to_vec();
        apis.push(x);
        let (r, o) = order(&raw(500), &apis);
        assert_eq!(r, Region::Irreversible);
        assert_eq!(o.len(), 3);
        assert_eq!(o[2], 6, "the last slot goes to the live node");
        assert!(o[..2].iter().all(|i| *i < 3));

        // Room for every candidate: nothing to replace.
        let q = BlockQuery {
            max_attempts: 4,
            ..raw(500)
        };
        let o = order(&q, &apis).1;
        assert_eq!(o.len(), 4);
        assert_eq!(o[3], 6);

        // The first live candidate in tier order: L before U before X.
        let l = live(4, Some(1), 5000, 6000);
        let u = live(5, None, 5000, 6000);
        let mut apis = replicas.to_vec();
        apis.extend([x, u, l]);
        let q = BlockQuery {
            max_attempts: 2,
            ..raw(500)
        };
        assert_eq!(order(&q, &apis).1[1], 4);
        let mut apis = replicas.to_vec();
        apis.extend([x, u]);
        assert_eq!(order(&q, &apis).1[1], 5);

        // A single attempt goes to the first candidate.
        let q = BlockQuery {
            max_attempts: 1,
            ..raw(500)
        };
        let o = order(&q, &[replicas[0], x]).1;
        assert_eq!(o, vec![0]);

        // No live candidate at all: archives only.
        assert_eq!(order(&raw(500), &replicas).1.len(), 3);
    }

    // CU4: priority and weight inside a tier
    #[test]
    fn priority_then_weighted_load_order_a_tier() {
        let primary = ApiView {
            priority: 0,
            in_flight: 9,
            ..frozen(0, 1, 1001)
        };
        let backup = ApiView {
            priority: 1,
            ..frozen(1, 1, 1001)
        };
        assert_eq!(order(&raw(500), &[backup, primary]).1, vec![0, 1]);

        // Equal priority: in_flight / weight, lower first.
        let big = ApiView {
            weight: 4,
            in_flight: 6,
            ..frozen(0, 1, 1001)
        };
        let small = ApiView {
            weight: 1,
            in_flight: 2,
            ..frozen(1, 1, 1001)
        };
        assert_eq!(order(&raw(500), &[small, big]).1, vec![0, 1]);
        // Equal load: the higher weight first.
        let big = ApiView {
            weight: 2,
            in_flight: 2,
            ..frozen(0, 1, 1001)
        };
        let small = ApiView {
            weight: 1,
            in_flight: 1,
            ..frozen(1, 1, 1001)
        };
        assert_eq!(order(&raw(500), &[small, big]).1, vec![0, 1]);
        // A zero weight counts as 1.
        let zero = ApiView {
            weight: 0,
            in_flight: 1,
            ..frozen(0, 1, 1001)
        };
        let one = ApiView {
            weight: 1,
            in_flight: 2,
            ..frozen(1, 1, 1001)
        };
        assert_eq!(order(&raw(500), &[one, zero]).1, vec![0, 1]);

        // Priority never moves a candidate across tiers.
        let preferred_live = ApiView {
            priority: 0,
            ..live(4, Some(1), 5000, 6000)
        };
        let backup_archive = ApiView {
            priority: 9,
            ..frozen(0, 1, 1001)
        };
        assert_eq!(
            order(&raw(500), &[preferred_live, backup_archive]).1,
            vec![0, 4]
        );
    }

    // CU4: skipped candidates
    #[test]
    fn busy_unverified_down_unlisted_and_uncovering_apis_are_skipped() {
        let good = frozen(0, 1, 1001);
        let busy = ApiView {
            busy: true,
            ..frozen(1, 1, 1001)
        };
        let unverified = ApiView {
            verified: false,
            ..frozen(2, 1, 1001)
        };
        // A chain_id mismatch marks the API down.
        let mismatched = ApiView {
            up: false,
            ..frozen(3, 1, 1001)
        };
        let q = BlockQuery {
            max_attempts: 8,
            ..raw(500)
        };
        assert_eq!(order(&q, &[busy, unverified, mismatched, good]).1, vec![0]);

        // Outside the archive's blocks.
        assert_eq!(
            order(
                &raw(1001),
                &[frozen(0, 1, 1001), live(4, Some(1), 5000, 6000)]
            )
            .1,
            vec![4]
        );
        // An archive that does not list the endpoint, or lists nothing known.
        let header_state = query(BlockEndpoint::BlockHeaderState, 500);
        assert_eq!(order(&header_state, &[good]).1, Vec::<usize>::new());
        let unknown_set = ApiView {
            apis: None,
            ..frozen(0, 1, 1001)
        };
        assert_eq!(order(&raw(500), &[unknown_set]).1, Vec::<usize>::new());
        // A live node whose advertised set lacks the endpoint; an unknown set
        // is assumed to serve it.
        let state_only = ApiView {
            apis: Some(&STATE_ONLY),
            ..live(4, Some(1), 5000, 6000)
        };
        let unknown = live(5, Some(1), 5000, 6000);
        assert_eq!(order(&raw(500), &[state_only, unknown]).1, vec![5]);
        // Busy and down live nodes.
        let busy_live = ApiView {
            busy: true,
            ..live(4, Some(1), 5000, 6000)
        };
        let down_live = ApiView {
            up: false,
            ..live(5, Some(1), 5000, 6000)
        };
        assert_eq!(
            order(&raw(500), &[busy_live, down_live]).1,
            Vec::<usize>::new()
        );
        // A live node whose head is below the block is no candidate.
        let lagging = live(4, Some(1), 5000, 499);
        let q = BlockQuery {
            ship_lib: Some(600),
            ..raw(500)
        };
        assert_eq!(order(&q, &[lagging]).1, Vec::<usize>::new());
    }

    // CU4: full candidates
    #[test]
    fn full_candidates_are_skipped_without_using_an_attempt() {
        let order = [0, 1, 2];
        assert_eq!(next_attempt(&order, &[], |_| false), NextAttempt::Try(0));
        assert_eq!(next_attempt(&order, &[], |i| i == 0), NextAttempt::Try(1));
        assert_eq!(next_attempt(&order, &[1], |i| i == 0), NextAttempt::Try(2));
        assert_eq!(
            next_attempt(&order, &[1, 2], |i| i == 0),
            NextAttempt::Wait(0)
        );
        assert_eq!(next_attempt(&order, &[], |_| true), NextAttempt::Wait(0));
        assert_eq!(
            next_attempt(&order, &[0, 1, 2], |_| false),
            NextAttempt::Exhausted
        );
        assert_eq!(next_attempt(&[], &[], |_| false), NextAttempt::Exhausted);
    }

    // CU4: rendezvous
    #[test]
    fn rendezvous_keeps_a_chunk_on_one_replica_and_spreads_chunks() {
        let replicas = [
            frozen(0, 1, 1 << 24),
            frozen(1, 1, 1 << 24),
            frozen(2, 1, 1 << 24),
        ];
        let q = |n| BlockQuery {
            max_attempts: 3,
            ..raw(n)
        };
        // Deterministic, and the same within a 4096-block chunk.
        let first = order(&q(8192), &replicas).1;
        assert_eq!(order(&q(8192), &replicas).1, first);
        assert_eq!(order(&q(8192 + 4095), &replicas).1, first);
        // Input order does not matter.
        let reversed = [replicas[2], replicas[1], replicas[0]];
        assert_eq!(order(&q(8192), &reversed).1, first);
        // Different chunks use every replica first at some point.
        let mut leaders = BTreeSet::new();
        for chunk in 1..64u32 {
            leaders.insert(order(&q(chunk << 12), &replicas).1[0]);
        }
        assert_eq!(leaders.len(), 3);
    }

    #[test]
    fn unhinted_requests_have_no_block_plan() {
        let q = BlockQuery {
            hint: None,
            ..raw(0)
        };
        assert_eq!(plan_block(&q, &[frozen(0, 1, 1001)]), BlockPlan::Unhinted);
        // No candidate at all: an empty order (Busy), never an invented answer.
        assert_eq!(
            plan_block(&raw(5), &[]),
            BlockPlan::Routed {
                n: 5,
                region: Region::Reversible,
                order: vec![]
            }
        );
    }

    #[test]
    fn state_calls_skip_laggards_and_rotate_among_equals() {
        let path = "/v1/chain/get_account";
        let q = |rr, lag| StateQuery {
            path,
            ship_head: None,
            lag,
            rr,
            max_attempts: 3,
        };
        let apis = [
            frozen(0, 1, 1001),
            live(4, Some(1), 900, 5000),
            live(5, Some(1), 900, 5001),
            live(6, Some(1), 900, 1000),
        ];
        // The laggard (4001 behind) is dropped; the other two rotate.
        assert_eq!(state_order(&q(0, 1200), &apis), vec![4, 5]);
        assert_eq!(state_order(&q(1, 1200), &apis), vec![5, 4]);
        assert_eq!(state_order(&q(2, 1200), &apis), vec![4, 5]);
        // Lag rule off: everyone, rotating.
        assert_eq!(state_order(&q(0, 0), &apis), vec![4, 5, 6]);
        assert_eq!(state_order(&q(1, 0), &apis), vec![5, 6, 4]);
        // The SHiP head counts for the fleet head; when every node lags they
        // are all kept.
        let behind = StateQuery {
            ship_head: Some(100_000),
            ..q(0, 1200)
        };
        assert_eq!(state_order(&behind, &apis), vec![4, 5, 6]);
        // Priority, then weighted load, before the rotation.
        let backup = ApiView {
            priority: 1,
            ..live(4, Some(1), 900, 5000)
        };
        let loaded = ApiView {
            in_flight: 3,
            ..live(5, Some(1), 900, 5000)
        };
        let idle = live(6, Some(1), 900, 5000);
        assert_eq!(
            state_order(&q(1, 0), &[backup, loaded, idle]),
            vec![6, 5, 4]
        );
        // Unlisted, busy and down nodes are skipped.
        let unlisted = ApiView {
            apis: Some(&FROZEN_APIS),
            ..live(4, Some(1), 900, 5000)
        };
        let busy = ApiView {
            busy: true,
            ..live(5, Some(1), 900, 5000)
        };
        let down = ApiView {
            up: false,
            ..live(6, Some(1), 900, 5000)
        };
        assert_eq!(
            state_order(&q(0, 0), &[unlisted, busy, down]),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn get_info_primary_is_sticky_until_overtaken_by_two() {
        let a = live(4, Some(1), 900, 1000);
        let b = live(5, Some(1), 900, 1001);
        assert_eq!(info_primary(&[a, b], None), Some(5));
        // b is ahead by one only: a stays primary.
        assert_eq!(info_primary(&[a, b], Some(4)), Some(4));
        let b2 = ApiView {
            head: Some(1002),
            ..b
        };
        assert_eq!(info_primary(&[a, b2], Some(4)), Some(5));
        // The primary going down hands over.
        let a_down = ApiView { up: false, ..a };
        assert_eq!(info_primary(&[a_down, b], Some(4)), Some(5));
        // Never a frozen API; nothing up gives None.
        assert_eq!(info_primary(&[frozen(0, 1, 1001)], None), None);
        // Ties by priority, then index.
        let c = ApiView {
            priority: 1,
            ..live(6, Some(1), 900, 1001)
        };
        assert_eq!(info_primary(&[c, b], None), Some(5));

        // Order: the primary, then the others by head.
        let d = live(7, Some(1), 900, 990);
        assert_eq!(info_order(&[a, b, d], Some(4), 3), vec![4, 5, 7]);
        assert_eq!(info_order(&[a, b, d], Some(4), 2), vec![4, 5]);
        let a_busy = ApiView { busy: true, ..a };
        assert_eq!(info_order(&[a_busy, b, d], Some(4), 3), vec![5, 7]);
        assert_eq!(info_order(&[a, b, d], None, 3), vec![5, 4, 7]);
    }

    #[test]
    fn pushes_go_to_push_servers_in_order_or_to_the_primary() {
        let path = "/v1/chain/send_transaction2";
        let a = live(4, Some(1), 900, 1000);
        let b = live(5, Some(1), 900, 1000);
        let c = ApiView {
            up: false,
            ..live(6, Some(1), 900, 1000)
        };
        assert_eq!(
            push_targets(&[a, b, c], path, &[6, 5, 4], Some(4)),
            vec![5, 4]
        );
        assert_eq!(push_targets(&[a, b, c], path, &[], Some(5)), vec![5]);
        assert_eq!(
            push_targets(&[a, b, c], path, &[], Some(6)),
            Vec::<usize>::new()
        );
        assert_eq!(
            push_targets(&[a, b, c], path, &[], None),
            Vec::<usize>::new()
        );
        // A frozen API is never a push target.
        assert_eq!(
            push_targets(&[frozen(0, 1, 9)], path, &[0], Some(0)),
            Vec::<usize>::new()
        );
    }

    const UNKNOWN_BLOCK: &str = r#"{"code":400,"message":"Unknown Block","error":{"code":3100002,"name":"unknown_block_exception","what":"Unknown block","details":[{"message":"Could not find block: 123","file":"chain_plugin.cpp","line_number":1966,"method":"get_raw_block"}]}}"#;
    const PARSE_ERROR: &str = r#"{"code":400,"message":"Invalid Request","error":{"code":3200006,"name":"invalid_http_request","what":"invalid http request","details":[{"message":"Unable to parse valid input from POST body","file":"http_plugin.hpp","line_number":278,"method":"parse_params"}]}}"#;
    const BAD_ID: &str = r#"{"code":500,"message":"Internal Service Error","error":{"code":3010008,"name":"block_id_type_exception","what":"Invalid block ID","details":[{"message":"Invalid block ID: zz","file":"chain_plugin.cpp","line_number":1963,"method":"get_raw_block"}]}}"#;
    const NOT_FOUND: &str = r#"{"code":404,"message":"Not Found","error":{"code":0,"name":"exception","what":"unspecified","details":[{"message":"Unknown Endpoint","file":"beast_http_session.hpp","line_number":186,"method":"handle_request"}]}}"#;

    fn response(status: u16, body: &str) -> Answer<'_> {
        Answer::Response {
            status,
            body: Some(body.as_bytes()),
        }
    }

    // CU3
    #[test]
    fn answers_are_classified_per_region() {
        use Region::{Irreversible as I, Reversible as R};
        let next = |reason, keep| Outcome::Next { reason, keep };

        assert_eq!(
            classify(I, response(400, UNKNOWN_BLOCK)),
            next(RetryReason::UnknownBlock, true)
        );
        assert_eq!(classify(R, response(400, UNKNOWN_BLOCK)), Outcome::Final);
        assert_eq!(
            classify(I, response(404, NOT_FOUND)),
            next(RetryReason::NotFound, true)
        );
        assert_eq!(classify(R, response(404, NOT_FOUND)), Outcome::Final);
        for region in [I, R] {
            assert_eq!(
                classify(region, response(503, "")),
                next(RetryReason::Busy, false)
            );
            assert_eq!(
                classify(
                    region,
                    Answer::Response {
                        status: 503,
                        body: None
                    }
                ),
                next(RetryReason::Busy, false)
            );
            assert_eq!(
                classify(region, Answer::Connect),
                next(RetryReason::Connect, false)
            );
            assert_eq!(
                classify(region, Answer::Timeout),
                next(RetryReason::Timeout, false)
            );
            // Genuine parameter errors and successes are final.
            assert_eq!(classify(region, response(400, PARSE_ERROR)), Outcome::Final);
            assert_eq!(classify(region, response(500, BAD_ID)), Outcome::Final);
            assert_eq!(classify(region, response(200, "{}")), Outcome::Final);
            assert_eq!(classify(region, response(502, "")), Outcome::Final);
            assert_eq!(classify(region, response(401, "")), Outcome::Final);
        }
        // A 400 that is not nodeos's Unknown Block shape is final.
        for body in [
            "",
            "not json",
            r#"{"error":{"code":"3100002"}}"#,
            r#"{"code":3100002}"#,
            r#"{"error":{"code":3100002},"error":{"code":1}}"#,
        ] {
            assert_eq!(classify(I, response(400, body)), Outcome::Final, "{body}");
        }
        assert_eq!(
            classify(I, response(400, r#"{"error":{"code":3100002}}"#)),
            next(RetryReason::UnknownBlock, true)
        );
        // Not peeked: final.
        for status in [400, 404] {
            assert_eq!(
                classify(I, Answer::Response { status, body: None }),
                Outcome::Final
            );
        }
        assert!(RetryReason::Connect.marks_down());
        assert!(RetryReason::Timeout.marks_down());
        assert!(!RetryReason::Busy.marks_down());
        assert!(RetryReason::Busy.marks_busy());
        assert!(!RetryReason::UnknownBlock.marks_busy());
        assert_eq!(BUSY_TTL, Duration::from_secs(2));
        assert_eq!(RetryReason::UnknownBlock.as_str(), "unknown_block");
        for (i, reason) in RetryReason::ALL.iter().enumerate() {
            assert_eq!(reason.index(), i);
        }
    }

    // CU3: the peek limit
    #[test]
    fn only_retryable_irreversible_errors_are_peeked_up_to_64_kib() {
        use Region::{Irreversible as I, Reversible as R};
        assert!(needs_peek(I, 400, Some(300)));
        assert!(needs_peek(I, 404, None));
        assert!(needs_peek(I, 400, Some(PEEK_LIMIT as u64)));
        assert!(!needs_peek(I, 400, Some(PEEK_LIMIT as u64 + 1)));
        assert!(!needs_peek(I, 200, Some(10)));
        assert!(!needs_peek(I, 503, Some(10)));
        assert!(!needs_peek(I, 500, Some(10)));
        assert!(!needs_peek(R, 400, Some(10)));
        assert!(!needs_peek(R, 404, None));
    }

    #[test]
    fn exhausted_requests_get_the_last_live_answer() {
        let kept = [
            (Role::Frozen, "f1"),
            (Role::Live, "l1"),
            (Role::Frozen, "f2"),
        ];
        assert_eq!(exhausted(&kept), Some(&"l1"));
        let frozen_only = [(Role::Frozen, "f1"), (Role::Frozen, "f2")];
        assert_eq!(exhausted(&frozen_only), Some(&"f2"));
        assert_eq!(exhausted::<&str>(&[]), None);
        assert_eq!(Region::Irreversible.as_str(), "irreversible");
        assert_eq!(Region::Reversible.as_str(), "reversible");
    }
}
