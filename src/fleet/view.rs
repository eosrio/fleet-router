//! Immutable snapshot of a listener's upstreams: per-log sticky coverage, the
//! fleet head/LIB and the canonical ABI that sessions and status replies use.
//!
//! [`build_view`] is a pure function of the state map, the observed tips and
//! the hub's [`HubMemory`] (the listener LIB high-water mark, the remembered
//! tip and the canonical ABI), so every rule here is unit-tested without a
//! running listener.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::abi::AbiEntry;
use crate::models::{AbiMismatch, BlockQuarantine, ResolvedTopology, Role, Server, ServerState};
use crate::ship::{BlockPos, Range};

use super::ObservedTip;

/// One of the three SHiP logs whose ranges a status result reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Log {
    Trace,
    ChainState,
    Finality,
}

impl Log {
    pub const ALL: [Log; 3] = [Log::Trace, Log::ChainState, Log::Finality];

    /// Label value on `/metrics` and key in `/coverage`.
    pub fn as_str(self) -> &'static str {
        match self {
            Log::Trace => "trace",
            Log::ChainState => "chain_state",
            Log::Finality => "finality",
        }
    }
}

/// Sticky coverage of one log (or of the union of the three) over the
/// listener's enabled upstreams.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogCoverage {
    /// Merged segments: sorted, disjoint and not touching.
    pub components: Vec<Range>,
    /// The component status reports, `(0,0)` if none.
    pub top: Range,
    /// Holes between components, all below `top`.
    pub gaps: Vec<Range>,
    /// Parts of `top` that no online upstream covers.
    pub offline_only: Vec<Range>,
    /// Lowest begin over the known enabled live upstreams that have this
    /// log. Live coverage is open-ended above it.
    pub live_tail_begin: Option<u32>,
}

impl LogCoverage {
    /// Membership used by the planner: inside a component, or at or above a
    /// live tail (live nodes keep growing, so their coverage is open-ended).
    pub fn has(&self, n: u32) -> bool {
        self.live_tail_begin.is_some_and(|begin| n >= begin)
            || self.components.iter().any(|c| c.contains(n))
    }

    /// Number of blocks in [`Self::offline_only`].
    pub fn offline_blocks(&self) -> u64 {
        self.offline_only
            .iter()
            .map(|r| u64::from(r.end.saturating_sub(r.begin)))
            .sum()
    }
}

/// Merge `segs` into sorted, disjoint, non-touching ranges. Empty ranges are
/// dropped.
fn merge(segs: Vec<Range>) -> Vec<Range> {
    let mut sorted: Vec<Range> = segs.into_iter().filter(|r| !r.is_empty()).collect();
    sorted.sort_by_key(|r| (r.begin, r.end));
    let mut merged: Vec<Range> = Vec::with_capacity(sorted.len());
    for r in sorted {
        match merged.last_mut() {
            Some(last) if r.begin <= last.end => last.end = last.end.max(r.end),
            _ => merged.push(r),
        }
    }
    merged
}

/// The parts of `top` outside `covered`, which must be merged.
fn subtract(top: Range, covered: &[Range]) -> Vec<Range> {
    let mut out = Vec::new();
    if top.is_empty() {
        return out;
    }
    let mut cursor = top.begin;
    for c in covered {
        if c.end <= cursor {
            continue;
        }
        if c.begin >= top.end {
            break;
        }
        if c.begin > cursor {
            out.push(Range {
                begin: cursor,
                end: c.begin,
            });
        }
        cursor = c.end;
        if cursor >= top.end {
            return out;
        }
    }
    out.push(Range {
        begin: cursor,
        end: top.end,
    });
    out
}

/// Coverage of one log from every enabled known upstream's segment (`segs`),
/// the online ones among them (`online`) and the live tail.
pub fn coverage(segs: Vec<Range>, online: Vec<Range>, live_tail_begin: Option<u32>) -> LogCoverage {
    let components = merge(segs);
    let top = components.last().copied().unwrap_or_default();
    let gaps = components
        .windows(2)
        .map(|w| Range {
            begin: w[0].end,
            end: w[1].begin,
        })
        .collect();
    let offline_only = subtract(top, &merge(online));
    LogCoverage {
        components,
        top,
        gaps,
        offline_only,
        live_tail_begin,
    }
}

/// One upstream as the planner and status aggregation see it.
#[derive(Clone, Debug)]
pub struct UpstreamView {
    /// `ServerState::index`: position among the listener's enabled servers.
    pub index: usize,
    pub endpoint: Arc<str>,
    pub name: Arc<str>,
    /// The URL a session dials.
    pub url: Arc<str>,
    pub role: Role,
    pub enabled: bool,
    pub online: bool,
    pub stale: bool,
    /// Ranges are usable: frozen, reported or declared; live, reported.
    pub known: bool,
    /// Effective ranges: the last reported ones, else (frozen) the declared
    /// range for trace and chain state.
    pub trace: Range,
    pub chain_state: Range,
    pub finality: Range,
    pub head: Option<BlockPos>,
    pub lib: Option<BlockPos>,
    /// The upstream's live link count (the same counter sessions update).
    pub connections: Arc<AtomicUsize>,
    pub max_sessions: Option<u32>,
    pub quarantines: Vec<BlockQuarantine>,
    pub abi_fp: Option<u64>,
    /// False when `abi_mismatch: "exclude"` drops this upstream for an ABI
    /// that differs from the canonical one.
    pub abi_ok: bool,
    /// The upstream only answers `get_status_request_v0`.
    pub legacy_status: bool,
    pub monitor_offline_since: Option<Instant>,
}

impl UpstreamView {
    pub fn range(&self, log: Log) -> Range {
        match log {
            Log::Trace => self.trace,
            Log::ChainState => self.chain_state,
            Log::Finality => self.finality,
        }
    }

    /// Whether this upstream has `log` at all. A live log is present once its
    /// end is non-zero; a frozen one when its range is not empty.
    pub fn has_log(&self, log: Log) -> bool {
        let r = self.range(log);
        match self.role {
            Role::Live => r.end > 0,
            Role::Frozen => !r.is_empty(),
        }
    }

    /// Lowest begin over the logs this upstream has.
    pub fn ids_begin(&self) -> Option<u32> {
        Log::ALL
            .into_iter()
            .filter(|&log| self.has_log(log))
            .map(|log| self.range(log).begin)
            .min()
    }

    /// Highest end over the logs this upstream has (0 when it has none).
    pub fn max_end(&self) -> u32 {
        Log::ALL
            .into_iter()
            .filter(|&log| self.has_log(log))
            .map(|log| self.range(log).end)
            .max()
            .unwrap_or(0)
    }

    /// Live: `end > 0 && begin <= n` (open-ended). Frozen: `begin <= n < end`.
    pub fn covers(&self, log: Log, n: u32) -> bool {
        let r = self.range(log);
        match self.role {
            Role::Live => r.end > 0 && r.begin <= n,
            Role::Frozen => r.contains(n),
        }
    }

    /// Whether this upstream knows the id of block `n` (any of its logs).
    pub fn covers_ids(&self, n: u32) -> bool {
        Log::ALL.into_iter().any(|log| self.covers(log, n))
    }

    pub fn at_capacity(&self) -> bool {
        self.max_sessions.is_some_and(|max| {
            self.connections.load(Ordering::Relaxed) >= usize::try_from(max).unwrap_or(usize::MAX)
        })
    }

    /// Whether an active quarantine excludes a request starting at `n`.
    pub fn quarantined_at(&self, n: u32, now: Instant) -> bool {
        self.quarantines
            .iter()
            .any(|q| q.active(now) && q.covers(n))
    }
}

impl PartialEq for UpstreamView {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
            && self.endpoint == other.endpoint
            && self.name == other.name
            && self.url == other.url
            && self.role == other.role
            && self.enabled == other.enabled
            && self.online == other.online
            && self.stale == other.stale
            && self.known == other.known
            && self.trace == other.trace
            && self.chain_state == other.chain_state
            && self.finality == other.finality
            && self.head == other.head
            && self.lib == other.lib
            && Arc::ptr_eq(&self.connections, &other.connections)
            && self.max_sessions == other.max_sessions
            && self.quarantines == other.quarantines
            && self.abi_fp == other.abi_fp
            && self.abi_ok == other.abi_ok
            && self.legacy_status == other.legacy_status
            && self.monitor_offline_since == other.monitor_offline_since
    }
}

/// Where the canonical ABI of a stitched listener comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AbiSource {
    /// `stitching.abi_file`.
    File,
    /// A live upstream (by index). Sticky while an online live upstream
    /// serves the same ABI.
    Live(usize),
    /// A frozen upstream (by index), used only until a live ABI is seen.
    Provisional(usize),
}

/// A published snapshot of one listener's fleet.
#[derive(Clone, Debug)]
pub struct FleetView {
    /// Increases with every published view.
    pub generation: u64,
    pub topology: ResolvedTopology,
    /// The pinned `chain_id`, else the one an online upstream reported.
    pub chain_id: Option<[u8; 32]>,
    /// Fleet head and LIB when the view was built. `FleetHub::fresh_tip`
    /// merges the tips sessions observed since.
    pub head: Option<BlockPos>,
    pub lib: Option<BlockPos>,
    pub trace: LogCoverage,
    pub chain_state: LogCoverage,
    pub finality: LogCoverage,
    /// Union of the three logs: the blocks whose id some upstream knows.
    pub ids: LogCoverage,
    /// Sorted by index.
    pub upstreams: Vec<UpstreamView>,
    /// The canonical ABI (stitched only).
    pub abi: Option<Arc<AbiEntry>>,
    pub abi_source: Option<AbiSource>,
    /// Enabled frozen upstreams that have neither reported nor declared a
    /// range.
    pub frozen_unknown: usize,
    /// The router can answer `get_status`: fleet head and LIB known and no
    /// frozen upstream unknown.
    pub status_ready: bool,
    pub live_configured: bool,
    pub online: usize,
    pub live_online: usize,
    /// For trace and chain state: max frozen end minus min online live
    /// begin. Negative means a hole between the archives and the live tier.
    pub live_overlap: [Option<i64>; 2],
    /// Positions in `upstreams` of the live upstreams the tip came from.
    pub(crate) anchors: Vec<usize>,
}

impl FleetView {
    /// The view of a listener before its first build.
    pub fn empty(topology: ResolvedTopology, chain_id: Option<[u8; 32]>) -> FleetView {
        FleetView {
            generation: 0,
            topology,
            chain_id,
            head: None,
            lib: None,
            trace: LogCoverage::default(),
            chain_state: LogCoverage::default(),
            finality: LogCoverage::default(),
            ids: LogCoverage::default(),
            upstreams: Vec::new(),
            abi: None,
            abi_source: None,
            frozen_unknown: 0,
            status_ready: false,
            live_configured: false,
            online: 0,
            live_online: 0,
            live_overlap: [None, None],
            anchors: Vec::new(),
        }
    }

    pub fn cov(&self, log: Log) -> &LogCoverage {
        match log {
            Log::Trace => &self.trace,
            Log::ChainState => &self.chain_state,
            Log::Finality => &self.finality,
        }
    }

    /// The upstream with `ServerState::index == index`.
    pub fn up(&self, index: usize) -> Option<&UpstreamView> {
        match self.upstreams.get(index) {
            Some(u) if u.index == index => Some(u),
            _ => self.upstreams.iter().find(|u| u.index == index),
        }
    }

    /// Everything but the generation is equal, so publishing `self` instead
    /// of `other` would change nothing for a reader.
    pub fn same_content(&self, other: &FleetView) -> bool {
        self.topology == other.topology
            && self.chain_id == other.chain_id
            && self.head == other.head
            && self.lib == other.lib
            && self.trace == other.trace
            && self.chain_state == other.chain_state
            && self.finality == other.finality
            && self.ids == other.ids
            && self.upstreams == other.upstreams
            && self.abi.as_ref().map(|a| a.fp) == other.abi.as_ref().map(|a| a.fp)
            && self.abi_source == other.abi_source
            && self.frozen_unknown == other.frozen_unknown
            && self.status_ready == other.status_ready
            && self.live_configured == other.live_configured
            && self.online == other.online
            && self.live_online == other.live_online
            && self.live_overlap == other.live_overlap
            && self.anchors == other.anchors
    }

    /// `log` split into the segments where the set of covering upstreams is
    /// constant, for `/coverage`. Holes are left out (see
    /// [`LogCoverage::gaps`]).
    pub fn segments(&self, log: Log) -> Vec<CoverageSegment> {
        let parts: Vec<(usize, Range)> = self
            .upstreams
            .iter()
            .enumerate()
            .filter(|(_, u)| u.enabled && u.known)
            .filter_map(|(pos, u)| log_segment(u, log, self.head).map(|r| (pos, r)))
            .collect();
        let mut bounds: Vec<u32> = parts.iter().flat_map(|(_, r)| [r.begin, r.end]).collect();
        bounds.sort_unstable();
        bounds.dedup();
        let mut out: Vec<CoverageSegment> = Vec::new();
        for w in bounds.windows(2) {
            let range = Range {
                begin: w[0],
                end: w[1],
            };
            let members: Vec<usize> = parts
                .iter()
                .filter(|(_, r)| r.begin <= range.begin && range.end <= r.end)
                .map(|(pos, _)| *pos)
                .collect();
            if members.is_empty() {
                continue;
            }
            match out.last_mut() {
                Some(last) if last.range.end == range.begin && last.upstreams == members => {
                    last.range.end = range.end;
                }
                _ => out.push(CoverageSegment {
                    range,
                    upstreams: members,
                }),
            }
        }
        out
    }
}

/// A block range of one log and the upstreams that cover all of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoverageSegment {
    pub range: Range,
    /// Positions in [`FleetView::upstreams`].
    pub upstreams: Vec<usize>,
}

/// The segment `u` contributes to the coverage of `log`. A live segment
/// extends to the fleet head + 1, because a live node keeps growing.
pub(crate) fn log_segment(u: &UpstreamView, log: Log, head: Option<BlockPos>) -> Option<Range> {
    if !u.has_log(log) {
        return None;
    }
    let r = u.range(log);
    match u.role {
        Role::Live => Some(Range {
            begin: r.begin,
            end: head.map_or(r.end, |h| r.end.max(h.num.saturating_add(1))),
        }),
        Role::Frozen => Some(r),
    }
}

/// Name, endpoint and dial URL of one enabled server, fixed for the life of
/// the listener.
#[derive(Clone, Debug)]
pub(crate) struct UpstreamStatic {
    pub endpoint: Arc<str>,
    pub name: Arc<str>,
    pub url: Arc<str>,
}

/// What a hub remembers between builds.
#[derive(Debug)]
pub struct HubMemory {
    pub(crate) generation: u64,
    /// The listener LIB never goes below this.
    pub(crate) lib_hwm: Option<BlockPos>,
    /// The last fleet tip, used while every live upstream is offline.
    pub(crate) last_tip: (Option<BlockPos>, Option<BlockPos>),
    pub(crate) canonical: Option<(Arc<AbiEntry>, AbiSource)>,
    /// Highest fleet head number seen, and when it was first seen.
    pub(crate) head_seen: Option<u32>,
    pub(crate) head_advanced_at: Instant,
    /// Upstream index -> the fingerprint last warned as differing from the
    /// canonical ABI.
    pub(crate) warned_abi: HashMap<usize, u64>,
    /// Frozen upstreams already warned for serving blocks above the fleet LIB.
    pub(crate) warned_frozen_end: HashSet<usize>,
}

impl HubMemory {
    pub fn new(now: Instant) -> HubMemory {
        HubMemory {
            generation: 0,
            lib_hwm: None,
            last_tip: (None, None),
            canonical: None,
            head_seen: None,
            head_advanced_at: now,
            warned_abi: HashMap::new(),
            warned_frozen_end: HashSet::new(),
        }
    }

    fn note_head(&mut self, head: Option<BlockPos>, now: Instant) {
        if let Some(head) = head {
            if self.head_seen.is_none_or(|seen| head.num > seen) {
                self.head_seen = Some(head.num);
                self.head_advanced_at = now;
            }
        }
    }
}

/// Inputs of [`build_view`] that come from the listener configuration.
pub(crate) struct BuildCtx<'a> {
    pub topology: ResolvedTopology,
    pub pinned_chain_id: Option<[u8; 32]>,
    pub abi_pin: Option<&'a Arc<AbiEntry>>,
    pub abi_mismatch: AbiMismatch,
    /// Indexed by `ServerState::index`.
    pub statics: &'a [UpstreamStatic],
    /// Listener name for log lines.
    pub listener: Option<&'a str>,
}

/// The pair from the source with the larger number. On equal numbers the
/// first argument wins, so an id is never paired with another source's
/// number.
pub(crate) fn max_by_num(a: Option<BlockPos>, b: Option<BlockPos>) -> Option<BlockPos> {
    match (a, b) {
        (Some(x), Some(y)) if y.num > x.num => Some(y),
        (Some(x), _) => Some(x),
        (None, y) => y,
    }
}

/// Build the view of one listener from its state map.
pub(crate) fn build_view(
    ctx: &BuildCtx<'_>,
    states: &HashMap<String, ServerState>,
    tips: &[Arc<ObservedTip>],
    mem: &mut HubMemory,
    now: Instant,
) -> FleetView {
    let mut sorted: Vec<(&String, &ServerState)> = states.iter().collect();
    sorted.sort_by(|a, b| a.1.index.cmp(&b.1.index).then_with(|| a.0.cmp(b.0)));
    let mut upstreams: Vec<UpstreamView> = sorted
        .iter()
        .map(|(endpoint, s)| upstream_view(ctx, endpoint, s))
        .collect();
    let abis: Vec<Option<Arc<AbiEntry>>> = sorted.iter().map(|(_, s)| s.abi.clone()).collect();
    let chain_ids: Vec<Option<[u8; 32]>> = sorted.iter().map(|(_, s)| s.chain_id).collect();

    let live_configured = upstreams.iter().any(|u| u.enabled && u.role == Role::Live);
    let (head, lib, anchors) = fleet_tip(&upstreams, tips, live_configured, mem);
    mem.note_head(head, now);

    let cov_of = |logs: &[Log]| {
        let mut segs = Vec::new();
        let mut online = Vec::new();
        let mut tail: Option<u32> = None;
        for u in upstreams.iter().filter(|u| u.enabled && u.known) {
            for &log in logs {
                let Some(seg) = log_segment(u, log, head) else {
                    continue;
                };
                segs.push(seg);
                if u.online {
                    online.push(seg);
                }
                if u.role == Role::Live {
                    let begin = u.range(log).begin;
                    tail = Some(tail.map_or(begin, |t| t.min(begin)));
                }
            }
        }
        coverage(segs, online, tail)
    };
    let trace = cov_of(&[Log::Trace]);
    let chain_state = cov_of(&[Log::ChainState]);
    let finality = cov_of(&[Log::Finality]);
    let ids = cov_of(&Log::ALL);

    let live_overlap = [
        live_overlap(&upstreams, Log::Trace),
        live_overlap(&upstreams, Log::ChainState),
    ];
    let frozen_unknown = upstreams
        .iter()
        .filter(|u| u.enabled && u.role == Role::Frozen && !u.known)
        .count();
    let online = upstreams.iter().filter(|u| u.enabled && u.online).count();
    let live_online = upstreams
        .iter()
        .filter(|u| u.enabled && u.online && u.role == Role::Live)
        .count();

    let canonical = match ctx.topology {
        ResolvedTopology::Stitched => canonical_abi(ctx, &upstreams, &abis, mem),
        ResolvedTopology::Pool => None,
    };
    if let (Some((entry, _)), AbiMismatch::Exclude) = (&canonical, ctx.abi_mismatch) {
        for u in &mut upstreams {
            u.abi_ok = u.abi_fp == Some(entry.fp);
        }
    }
    if ctx.topology == ResolvedTopology::Stitched {
        warn_stitched(ctx, &upstreams, canonical.as_ref(), lib, mem);
    }

    let chain_id = ctx.pinned_chain_id.or_else(|| {
        upstreams
            .iter()
            .zip(&chain_ids)
            .find_map(|(u, chain_id)| chain_id.filter(|_| u.online))
    });

    FleetView {
        generation: mem.generation,
        topology: ctx.topology,
        chain_id,
        head,
        lib,
        trace,
        chain_state,
        finality,
        ids,
        upstreams,
        abi: canonical.as_ref().map(|(entry, _)| entry.clone()),
        abi_source: canonical.map(|(_, source)| source),
        frozen_unknown,
        status_ready: head.is_some() && lib.is_some() && frozen_unknown == 0,
        live_configured,
        online,
        live_online,
        live_overlap,
        anchors,
    }
}

fn upstream_view(ctx: &BuildCtx<'_>, endpoint: &str, s: &ServerState) -> UpstreamView {
    let (endpoint_arc, name, url) = match ctx.statics.get(s.index) {
        Some(st) if &*st.endpoint == endpoint => {
            (st.endpoint.clone(), st.name.clone(), st.url.clone())
        }
        _ => {
            let server = Server {
                name: s.name.clone(),
                endpoint: endpoint.to_string(),
                enabled: s.enabled,
                role: s.role,
                range: s.declared,
                max_sessions: s.max_sessions,
            };
            (
                Arc::from(endpoint),
                Arc::from(s.name.as_str()),
                Arc::from(server.ws_url()),
            )
        }
    };
    let reported = |begin: u32, end: u32| Range { begin, end };
    let (trace, chain_state, finality) = if s.ranges_known {
        (
            reported(s.trace_begin_block, s.trace_end_block),
            reported(s.chain_state_begin_block, s.chain_state_end_block),
            reported(s.finality_data_begin_block, s.finality_data_end_block),
        )
    } else {
        match (s.role, s.declared) {
            (Role::Frozen, Some(declared)) => (declared, declared, Range::default()),
            _ => Default::default(),
        }
    };
    UpstreamView {
        index: s.index,
        endpoint: endpoint_arc,
        name,
        url,
        role: s.role,
        enabled: s.enabled,
        online: s.online,
        stale: s.stale,
        known: s.ranges_usable(),
        trace,
        chain_state,
        finality,
        head: s.head,
        lib: s.lib,
        connections: s.connections.clone(),
        max_sessions: s.max_sessions,
        quarantines: s.quarantines.clone(),
        abi_fp: s.abi.as_ref().map(|abi| abi.fp),
        abi_ok: true,
        legacy_status: s.status_v1 == Some(false),
        monitor_offline_since: s.offline_since,
    }
}

/// Maximum head and LIB over `anchors`, each from the monitor's status and
/// the upstream's observed tip.
fn anchor_max(
    upstreams: &[UpstreamView],
    tips: &[Arc<ObservedTip>],
    anchors: &[usize],
) -> (Option<BlockPos>, Option<BlockPos>) {
    anchors
        .iter()
        .filter_map(|&pos| upstreams.get(pos))
        .fold((None, None), |(head, lib), u| {
            let tip = tips.get(u.index).and_then(|t| t.get());
            (
                max_by_num(head, max_by_num(u.head, tip.map(|t| t.0))),
                max_by_num(lib, max_by_num(u.lib, tip.map(|t| t.1))),
            )
        })
}

/// The fleet head and LIB, and the anchors they came from.
///
/// Anchors are the online live upstreams that are not stale (all online live
/// ones when every one is stale). An archive-only listener uses the newest
/// frozen head. With live servers configured but none online, the last tip is
/// kept. The LIB never goes below the listener's high-water mark, and never
/// above the head.
fn fleet_tip(
    upstreams: &[UpstreamView],
    tips: &[Arc<ObservedTip>],
    live_configured: bool,
    mem: &mut HubMemory,
) -> (Option<BlockPos>, Option<BlockPos>, Vec<usize>) {
    let live_on: Vec<usize> = upstreams
        .iter()
        .enumerate()
        .filter(|(_, u)| u.enabled && u.role == Role::Live && u.online)
        .map(|(pos, _)| pos)
        .collect();
    let fresh: Vec<usize> = live_on
        .iter()
        .copied()
        .filter(|&pos| !upstreams[pos].stale)
        .collect();
    let anchors = if fresh.is_empty() { live_on } else { fresh };

    let (mut head, mut lib) = if !anchors.is_empty() {
        anchor_max(upstreams, tips, &anchors)
    } else if !live_configured {
        let newest = upstreams
            .iter()
            .filter(|u| u.enabled && u.known && u.role == Role::Frozen)
            .fold(None, |acc, u| max_by_num(acc, u.head));
        (newest, newest)
    } else {
        mem.last_tip
    };
    if head.is_none() {
        (head, lib) = mem.last_tip;
    }
    let Some(head) = head else {
        return (None, None, anchors);
    };
    let lib = max_by_num(lib, mem.lib_hwm);
    mem.lib_hwm = lib;
    let lib = lib.map(|l| if l.num > head.num { head } else { l });
    mem.last_tip = (Some(head), lib);
    (Some(head), lib, anchors)
}

/// [`FleetView::head`] / [`FleetView::lib`] with the current observed tips
/// of the view's anchors merged in. Raises the LIB high-water mark.
pub(crate) fn fresh_tip(
    v: &FleetView,
    tips: &[Arc<ObservedTip>],
    mem: &mut HubMemory,
    now: Instant,
) -> (Option<BlockPos>, Option<BlockPos>) {
    let (anchor_head, anchor_lib) = anchor_max(&v.upstreams, tips, &v.anchors);
    let Some(head) = max_by_num(v.head, anchor_head) else {
        return (None, None);
    };
    let lib = max_by_num(max_by_num(v.lib, anchor_lib), mem.lib_hwm);
    mem.lib_hwm = lib;
    let lib = lib.map(|l| if l.num > head.num { head } else { l });
    mem.note_head(Some(head), now);
    if mem.last_tip.0.is_none_or(|last| head.num > last.num) {
        mem.last_tip = (Some(head), lib);
    }
    (Some(head), lib)
}

/// Max frozen end minus min online live begin for `log`, when both exist.
fn live_overlap(upstreams: &[UpstreamView], log: Log) -> Option<i64> {
    let frozen_end = upstreams
        .iter()
        .filter(|u| u.enabled && u.known && u.role == Role::Frozen && u.has_log(log))
        .map(|u| u.range(log).end)
        .max()?;
    let live_begin = upstreams
        .iter()
        .filter(|u| u.enabled && u.known && u.online && u.role == Role::Live && u.has_log(log))
        .map(|u| u.range(log).begin)
        .min()?;
    Some(i64::from(frozen_end) - i64::from(live_begin))
}

/// The ABI every client of a stitched listener receives.
///
/// The `abi_file` pin wins. Otherwise a live upstream's ABI, sticky while an
/// online live upstream still serves it, else the ABI of the online live
/// upstream with the highest head. Without any live ABI, the most common
/// frozen ABI is used provisionally until a live one appears.
fn canonical_abi(
    ctx: &BuildCtx<'_>,
    upstreams: &[UpstreamView],
    abis: &[Option<Arc<AbiEntry>>],
    mem: &mut HubMemory,
) -> Option<(Arc<AbiEntry>, AbiSource)> {
    let previous = mem.canonical.clone();
    let next = choose_canonical(ctx, upstreams, abis, previous.clone());
    if let Some((entry, source)) = &next {
        let changed = previous
            .as_ref()
            .is_none_or(|(old, old_source)| old.fp != entry.fp || old_source != source);
        if changed {
            let from = match source {
                AbiSource::File => "abi_file".to_string(),
                AbiSource::Live(i) | AbiSource::Provisional(i) => upstreams
                    .iter()
                    .find(|u| u.index == *i)
                    .map_or_else(|| i.to_string(), |u| u.name.to_string()),
            };
            tracing::info!(
                listener = ctx.listener.unwrap_or("default"),
                fingerprint = %entry.fp_hex(),
                source = %from,
                provisional = matches!(source, AbiSource::Provisional(_)),
                "canonical ABI {}",
                if previous.is_some() { "changed" } else { "chosen" }
            );
        }
    }
    mem.canonical = next.clone();
    next
}

fn choose_canonical(
    ctx: &BuildCtx<'_>,
    upstreams: &[UpstreamView],
    abis: &[Option<Arc<AbiEntry>>],
    current: Option<(Arc<AbiEntry>, AbiSource)>,
) -> Option<(Arc<AbiEntry>, AbiSource)> {
    if let Some(pin) = ctx.abi_pin {
        return Some((pin.clone(), AbiSource::File));
    }
    let online_with_abi = |role: Role| {
        upstreams
            .iter()
            .zip(abis)
            .filter(move |(u, abi)| u.enabled && u.online && u.role == role && abi.is_some())
    };
    if let Some((entry, AbiSource::Live(_))) = &current {
        if online_with_abi(Role::Live).any(|(u, _)| u.abi_fp == Some(entry.fp)) {
            return current;
        }
    }
    // Highest head; on equal heads the lowest index (the first in order).
    let best_live = online_with_abi(Role::Live).fold(
        None,
        |best: Option<(&UpstreamView, &Arc<AbiEntry>)>, (u, abi)| {
            let abi = abi.as_ref().expect("filtered on abi.is_some()");
            match best {
                Some((b, _)) if u.head.map(|h| h.num) <= b.head.map(|h| h.num) => best,
                _ => Some((u, abi)),
            }
        },
    );
    if let Some((u, abi)) = best_live {
        return Some((abi.clone(), AbiSource::Live(u.index)));
    }
    if current.is_some() {
        return current;
    }
    // The most common frozen ABI; ties go to the lowest index.
    let mut counts: Vec<(u64, usize, usize, &Arc<AbiEntry>)> = Vec::new();
    for (u, abi) in online_with_abi(Role::Frozen) {
        let abi = abi.as_ref().expect("filtered on abi.is_some()");
        match counts.iter_mut().find(|(fp, ..)| *fp == abi.fp) {
            Some(slot) => slot.1 += 1,
            None => counts.push((abi.fp, 1, u.index, abi)),
        }
    }
    counts
        .into_iter()
        .max_by(|a, b| a.1.cmp(&b.1).then(b.2.cmp(&a.2)))
        .map(|(_, _, index, abi)| (abi.clone(), AbiSource::Provisional(index)))
}

/// One-time warnings of a stitched listener: an upstream whose ABI differs
/// from the canonical one (once per upstream and fingerprint), and a frozen
/// upstream that serves blocks above the fleet LIB + 1 (once per upstream).
fn warn_stitched(
    ctx: &BuildCtx<'_>,
    upstreams: &[UpstreamView],
    canonical: Option<&(Arc<AbiEntry>, AbiSource)>,
    lib: Option<BlockPos>,
    mem: &mut HubMemory,
) {
    let listener = ctx.listener.unwrap_or("default");
    if let Some((entry, _)) = canonical {
        for u in upstreams {
            match u.abi_fp {
                Some(fp) if fp != entry.fp => {
                    if mem.warned_abi.get(&u.index) != Some(&fp) {
                        mem.warned_abi.insert(u.index, fp);
                        tracing::warn!(
                            listener,
                            name = %u.name,
                            upstream = %u.endpoint,
                            fingerprint = %format!("{fp:016x}"),
                            canonical = %entry.fp_hex(),
                            excluded = !u.abi_ok,
                            "upstream ABI differs from the ABI this listener serves"
                        );
                    }
                }
                _ => {
                    mem.warned_abi.remove(&u.index);
                }
            }
        }
    }
    let Some(lib) = lib else {
        return;
    };
    for u in upstreams
        .iter()
        .filter(|u| u.enabled && u.known && u.role == Role::Frozen)
    {
        if u64::from(u.max_end()) > u64::from(lib.num) + 1 && mem.warned_frozen_end.insert(u.index)
        {
            tracing::warn!(
                listener,
                name = %u.name,
                upstream = %u.endpoint,
                end = u.max_end(),
                fleet_lib = lib.num,
                "frozen upstream serves blocks above the fleet LIB; archived ranges must end at or below LIB"
            );
        }
    }
}

#[cfg(test)]
pub(crate) mod fixtures {
    //! States and views for fleet tests.

    use super::*;

    pub fn id(num: u32, seed: u8) -> [u8; 32] {
        let mut id = [seed; 32];
        id[..4].copy_from_slice(&num.to_be_bytes());
        id
    }

    pub fn pos(num: u32) -> BlockPos {
        BlockPos {
            num,
            id: id(num, 0),
        }
    }

    pub fn endpoint(index: usize) -> String {
        format!("127.0.0.1:{}", 9000 + index)
    }

    fn base(index: usize, role: Role) -> ServerState {
        let mut s = ServerState::new();
        s.index = index;
        s.role = role;
        s.name = format!("up{index}");
        s
    }

    /// An online live upstream with trace and chain state `[begin, head+1)`
    /// and LIB `head - 10`.
    pub fn live(index: usize, begin: u32, head: u32) -> ServerState {
        let mut s = base(index, Role::Live);
        s.online = true;
        s.ranges_known = true;
        s.trace_begin_block = begin;
        s.trace_end_block = head + 1;
        s.chain_state_begin_block = begin;
        s.chain_state_end_block = head + 1;
        s.head = Some(pos(head));
        s.lib = Some(pos(head.saturating_sub(10)));
        s
    }

    /// An online frozen upstream that reported trace and chain state
    /// `[begin, end)`, head = LIB = `end - 1`.
    pub fn frozen(index: usize, begin: u32, end: u32) -> ServerState {
        let mut s = base(index, Role::Frozen);
        s.online = true;
        s.ranges_known = true;
        s.trace_begin_block = begin;
        s.trace_end_block = end;
        s.chain_state_begin_block = begin;
        s.chain_state_end_block = end;
        s.head = Some(pos(end - 1));
        s.lib = Some(pos(end - 1));
        s
    }

    /// An offline frozen upstream that never reported, with an optional
    /// declared range.
    pub fn frozen_unreported(index: usize, declared: Option<(u32, u32)>) -> ServerState {
        let mut s = base(index, Role::Frozen);
        s.declared = declared.map(|(begin, end)| Range { begin, end });
        s
    }

    pub fn states(list: Vec<ServerState>) -> HashMap<String, ServerState> {
        list.into_iter().map(|s| (endpoint(s.index), s)).collect()
    }

    /// A copy of the fields fleet code reads (`ServerState` is not `Clone`).
    pub trait Dup {
        fn dup(&self) -> Self;
    }

    impl Dup for ServerState {
        fn dup(&self) -> Self {
            let mut c = ServerState::new();
            c.name = self.name.clone();
            c.index = self.index;
            c.role = self.role;
            c.declared = self.declared;
            c.max_sessions = self.max_sessions;
            c.enabled = self.enabled;
            c.online = self.online;
            c.stale = self.stale;
            c.ranges_known = self.ranges_known;
            c.trace_begin_block = self.trace_begin_block;
            c.trace_end_block = self.trace_end_block;
            c.chain_state_begin_block = self.chain_state_begin_block;
            c.chain_state_end_block = self.chain_state_end_block;
            c.finality_data_begin_block = self.finality_data_begin_block;
            c.finality_data_end_block = self.finality_data_end_block;
            c.head = self.head;
            c.lib = self.lib;
            c.chain_id = self.chain_id;
            c.status_v1 = self.status_v1;
            c.offline_since = self.offline_since;
            c.quarantines = self.quarantines.clone();
            c.abi = self.abi.clone();
            c.connections = self.connections.clone();
            c
        }
    }

    pub fn ctx(topology: ResolvedTopology) -> BuildCtx<'static> {
        BuildCtx {
            topology,
            pinned_chain_id: Some([7; 32]),
            abi_pin: None,
            abi_mismatch: AbiMismatch::Warn,
            statics: &[],
            listener: None,
        }
    }

    /// A stitched view built with fresh memory and no observed tips.
    pub fn view(list: Vec<ServerState>) -> FleetView {
        let mut mem = HubMemory::new(Instant::now());
        build_view(
            &ctx(ResolvedTopology::Stitched),
            &states(list),
            &[],
            &mut mem,
            Instant::now(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::fixtures::*;
    use super::*;

    fn r(begin: u32, end: u32) -> Range {
        Range { begin, end }
    }

    #[test]
    fn coverage_merges_adjacent_and_overlapping_segments_and_reports_holes() {
        // V1
        let adjacent = coverage(vec![r(1001, 2001), r(2, 1001)], vec![], None);
        assert_eq!(adjacent.components, vec![r(2, 2001)]);
        assert_eq!(adjacent.top, r(2, 2001));
        assert!(adjacent.gaps.is_empty());

        let overlap = coverage(vec![r(2, 1500), r(1000, 2001), r(5, 10)], vec![], None);
        assert_eq!(overlap.components, vec![r(2, 2001)]);

        let hole = coverage(vec![r(2, 1001), r(1101, 2001)], vec![], None);
        assert_eq!(hole.components, vec![r(2, 1001), r(1101, 2001)]);
        assert_eq!(hole.top, r(1101, 2001));
        assert_eq!(hole.gaps, vec![r(1001, 1101)]);

        let empty = coverage(vec![r(5, 5), r(9, 3)], vec![], None);
        assert!(empty.components.is_empty());
        assert_eq!(empty.top, r(0, 0));
        assert!(empty.gaps.is_empty());
        assert!(empty.offline_only.is_empty());
    }

    #[test]
    fn coverage_is_sticky_and_live_segments_reach_the_fleet_head() {
        // V2
        let mut a = frozen(0, 2, 1001);
        a.online = false;
        let declared = frozen_unreported(1, Some((1001, 2001)));
        let mut c = live(2, 1900, 2300);
        let v = view(vec![a.dup(), declared, c.dup()]);
        assert_eq!(
            v.trace.components,
            vec![r(2, 2301)],
            "offline and declared ranges count"
        );
        assert_eq!(v.upstreams[1].trace, r(1001, 2001));
        assert_eq!(v.upstreams[1].finality, r(0, 0));

        // The reported range replaces the declared one.
        let mut reported = frozen(1, 1001, 1801);
        reported.declared = Some(r(1001, 2001));
        let v = view(vec![a.dup(), reported, c.dup()]);
        assert_eq!(v.upstreams[1].trace, r(1001, 1801));
        assert_eq!(v.trace.components, vec![r(2, 1801), r(1900, 2301)]);

        // An offline live upstream keeps its begin; its segment still ends at
        // the fleet head + 1, here the head of the other live upstream.
        c.online = false;
        let other = live(3, 2200, 2400);
        let v = view(vec![a, c, other]);
        assert_eq!(v.head.map(|h| h.num), Some(2400));
        assert_eq!(v.trace.live_tail_begin, Some(1900));
        assert_eq!(v.trace.components, vec![r(2, 1001), r(1900, 2401)]);
    }

    fn mem() -> HubMemory {
        HubMemory::new(Instant::now())
    }

    fn build(list: &[ServerState], tips: &[Arc<ObservedTip>], mem: &mut HubMemory) -> FleetView {
        build_view(
            &ctx(ResolvedTopology::Stitched),
            &states(list.iter().map(Dup::dup).collect()),
            tips,
            mem,
            Instant::now(),
        )
    }

    fn tips(n: usize) -> Vec<Arc<ObservedTip>> {
        (0..n).map(|_| Arc::new(ObservedTip::default())).collect()
    }

    #[test]
    fn fleet_tip_uses_fresh_live_anchors_observed_tips_and_a_monotonic_lib() {
        // V3
        let t = tips(4);
        let mut m = mem();
        let a = frozen(0, 2, 1001);
        let fresh = live(1, 900, 2000);
        let mut stale = live(2, 900, 2100);
        stale.stale = true;

        // Stale anchors are excluded while a fresh one exists.
        let v = build(&[a.dup(), fresh.dup(), stale.dup()], &t, &mut m);
        assert_eq!(v.head, Some(pos(2000)));
        assert_eq!(v.anchors, vec![1]);
        // ...and used when every live upstream is stale.
        let mut both_stale = fresh.dup();
        both_stale.stale = true;
        let v = build(&[a.dup(), both_stale, stale.dup()], &t, &mut mem());
        assert_eq!(v.head, Some(pos(2100)));
        assert_eq!(v.anchors, vec![1, 2]);

        // An observed tip newer than the status wins.
        t[1].observe(pos(2005), pos(1999));
        let v = build(&[a.dup(), fresh.dup(), stale.dup()], &t, &mut m);
        assert_eq!(v.head, Some(pos(2005)));
        assert_eq!(v.lib, Some(pos(1999)));

        // Every live upstream offline: the remembered tip.
        let mut down = fresh.dup();
        down.online = false;
        let mut stale_down = stale.dup();
        stale_down.online = false;
        let v = build(&[a.dup(), down, stale_down], &t, &mut m);
        assert_eq!((v.head, v.lib), (Some(pos(2005)), Some(pos(1999))));
        assert!(v.anchors.is_empty());

        // The LIB high-water mark never decreases, and LIB <= head.
        let mut lagging = live(1, 900, 1500);
        lagging.lib = Some(pos(1490));
        let v = build(&[a.dup(), lagging], &tips(4), &mut m);
        assert_eq!(v.head, Some(pos(1500)));
        assert_eq!(v.lib, Some(pos(1500)), "LIB is clamped to the head");
        assert_eq!(m.lib_hwm, Some(pos(1999)), "the high-water mark is kept");
        let v = build(&[a.dup(), fresh.dup()], &tips(4), &mut m);
        assert_eq!(v.lib, Some(pos(1999)));

        // Archive-only listener: the newest frozen head.
        let b = frozen(1, 1001, 2001);
        let mut b_down = b.dup();
        b_down.online = false;
        let v = build(&[a.dup(), b_down], &[], &mut mem());
        assert_eq!((v.head, v.lib), (Some(pos(2000)), Some(pos(2000))));
        assert!(v.status_ready);
    }

    #[test]
    fn fresh_tip_merges_observed_tips_and_raises_the_high_water_mark() {
        let t = tips(3);
        let mut m = mem();
        let v = build(&[frozen(0, 2, 1001), live(1, 900, 2000)], &t, &mut m);
        assert_eq!(
            fresh_tip(&v, &t, &mut m, Instant::now()),
            (Some(pos(2000)), Some(pos(1990)))
        );
        t[1].observe(pos(2010), pos(2001));
        assert_eq!(
            fresh_tip(&v, &t, &mut m, Instant::now()),
            (Some(pos(2010)), Some(pos(2001)))
        );
        assert_eq!(m.lib_hwm, Some(pos(2001)));
        assert_eq!(m.head_seen, Some(2010));
        // An older view cannot lower the LIB any more.
        let old = build(
            &[frozen(0, 2, 1001), live(1, 900, 1995)],
            &tips(3),
            &mut mem(),
        );
        assert_eq!(
            fresh_tip(&old, &tips(3), &mut m, Instant::now()).1,
            Some(pos(1995))
        );
        let no_head = FleetView::empty(ResolvedTopology::Stitched, None);
        assert_eq!(
            fresh_tip(&no_head, &t, &mut m, Instant::now()),
            (None, None)
        );
    }

    #[test]
    fn status_waits_for_undeclared_frozen_upstreams() {
        // V4
        let c = live(2, 900, 2300);
        let v = view(vec![
            frozen(0, 2, 1001),
            frozen_unreported(1, None),
            c.dup(),
        ]);
        assert_eq!(v.frozen_unknown, 1);
        assert!(!v.status_ready);

        let v = view(vec![frozen(0, 2, 1001), frozen(1, 1001, 2001), c.dup()]);
        assert_eq!(v.frozen_unknown, 0);
        assert!(v.status_ready, "ready once the frozen upstream reports");

        let v = view(vec![
            frozen(0, 2, 1001),
            frozen_unreported(1, Some((1001, 2001))),
            c,
        ]);
        assert_eq!(v.frozen_unknown, 0);
        assert!(v.status_ready, "a declared range never blocks status");

        let v = view(vec![frozen_unreported(0, Some((2, 1001)))]);
        assert!(!v.status_ready, "no head known yet");
    }

    #[test]
    fn offline_only_gaps_and_live_overlap() {
        // V5
        let mut a = frozen(0, 2, 1001);
        a.online = false;
        let b = frozen(1, 1101, 2001);
        let c = live(2, 1900, 2300);
        let v = view(vec![a.dup(), b.dup(), c.dup()]);
        assert_eq!(v.trace.gaps, vec![r(1001, 1101)]);
        assert_eq!(v.trace.top, r(1101, 2301));
        assert!(v.trace.offline_only.is_empty());
        assert_eq!(v.live_overlap, [Some(2001 - 1900), Some(2001 - 1900)]);

        let mut b_down = b.dup();
        b_down.online = false;
        let v = view(vec![a.dup(), b_down, c.dup()]);
        assert_eq!(v.trace.offline_only, vec![r(1101, 1900)]);
        assert_eq!(v.trace.offline_blocks(), 799);
        assert_eq!(v.ids.offline_only, vec![r(1101, 1900)]);

        // A hole between the archives and the live tier.
        let late = live(2, 2500, 3000);
        let v = view(vec![a, b, late]);
        assert_eq!(v.live_overlap[0], Some(2001 - 2500));
        assert_eq!(v.trace.gaps, vec![r(1001, 1101), r(2001, 2500)]);

        // No frozen upstream: no overlap to report.
        let v = view(vec![c]);
        assert_eq!(v.live_overlap, [None, None]);
        assert!(v.trace.gaps.is_empty());
    }

    fn with_abi(mut s: ServerState, text: &str) -> ServerState {
        s.abi = Some(Arc::new(AbiEntry::new(text)));
        s
    }

    fn abi_text(tag: &str) -> String {
        format!(
            r#"{{"version":"eosio::abi/1.1","structs":[{{"name":"{tag}","base":"","fields":[]}}]}}"#
        )
    }

    #[test]
    fn canonical_abi_is_live_derived_sticky_and_overridden_by_the_pin() {
        // V6
        let live_a = abi_text("live_a");
        let live_b = abi_text("live_b");
        let archive = abi_text("archive");
        let mut m = mem();
        let stitched = ctx(ResolvedTopology::Stitched);
        let run = |list: Vec<ServerState>, m: &mut HubMemory| {
            build_view(&stitched, &states(list), &[], m, Instant::now())
        };

        // Only archives online: the most common frozen ABI, provisionally.
        let f0 = with_abi(frozen(0, 2, 1001), &abi_text("other"));
        let f1 = with_abi(frozen(1, 1001, 2001), &archive);
        let f2 = with_abi(frozen(2, 2001, 3001), &archive);
        let mut c = with_abi(live(3, 2900, 4000), &live_a);
        c.online = false;
        let v = run(vec![f0.dup(), f1.dup(), f2.dup(), c.dup()], &mut m);
        assert_eq!(
            v.abi.as_ref().map(|a| a.fp),
            Some(AbiEntry::new(&archive).fp)
        );
        assert_eq!(v.abi_source, Some(AbiSource::Provisional(1)));

        // A live ABI replaces the provisional one.
        c.online = true;
        let v = run(vec![f0.dup(), f1.dup(), f2.dup(), c.dup()], &mut m);
        assert_eq!(
            v.abi.as_ref().map(|a| a.fp),
            Some(AbiEntry::new(&live_a).fp)
        );
        assert_eq!(v.abi_source, Some(AbiSource::Live(3)));

        // Sticky: a higher live node with another ABI does not take over
        // while one online live upstream still serves the chosen ABI.
        let d = with_abi(live(4, 2900, 4100), &live_b);
        let v = run(vec![f1.dup(), c.dup(), d.dup()], &mut m);
        assert_eq!(v.abi_source, Some(AbiSource::Live(3)));
        assert_eq!(
            v.abi.as_ref().map(|a| a.fp),
            Some(AbiEntry::new(&live_a).fp)
        );

        // Once no online live upstream serves it, the highest live head wins.
        let mut c_down = c.dup();
        c_down.online = false;
        let v = run(vec![f1.dup(), c_down.dup(), d.dup()], &mut m);
        assert_eq!(v.abi_source, Some(AbiSource::Live(4)));

        // With no live ABI at all, the last live ABI is kept (no fallback to
        // a frozen one).
        let mut d_down = d.dup();
        d_down.online = false;
        let v = run(vec![f1.dup(), c_down, d_down], &mut m);
        assert_eq!(v.abi_source, Some(AbiSource::Live(4)));

        // abi_file overrides everything.
        let pin = Arc::new(AbiEntry::new(&abi_text("pinned")));
        let pinned = BuildCtx {
            abi_pin: Some(&pin),
            ..ctx(ResolvedTopology::Stitched)
        };
        let v = build_view(
            &pinned,
            &states(vec![f1.dup(), d.dup()]),
            &[],
            &mut m,
            Instant::now(),
        );
        assert_eq!(v.abi_source, Some(AbiSource::File));
        assert_eq!(v.abi.as_ref().map(|a| a.fp), Some(pin.fp));
        assert!(
            v.upstreams.iter().all(|u| u.abi_ok),
            "warn policy never excludes"
        );

        // abi_mismatch = exclude: only upstreams with the canonical ABI are ok.
        let exclude = BuildCtx {
            abi_pin: Some(&pin),
            abi_mismatch: AbiMismatch::Exclude,
            ..ctx(ResolvedTopology::Stitched)
        };
        let same = with_abi(frozen(0, 2, 1001), &abi_text("pinned"));
        let v = build_view(
            &exclude,
            &states(vec![same, f1.dup(), frozen_unreported(2, Some((5, 9)))]),
            &[],
            &mut m,
            Instant::now(),
        );
        assert_eq!(
            v.upstreams.iter().map(|u| u.abi_ok).collect::<Vec<_>>(),
            vec![true, false, false]
        );

        // A pool listener has no canonical ABI.
        let v = build_view(
            &ctx(ResolvedTopology::Pool),
            &states(vec![d]),
            &[],
            &mut mem(),
            Instant::now(),
        );
        assert!(v.abi.is_none());
    }

    #[test]
    fn log_coverage_membership() {
        // V7
        let cov = coverage(vec![r(2, 1001), r(1101, 2001)], vec![], Some(3000));
        assert!(cov.has(2));
        assert!(cov.has(1000));
        assert!(!cov.has(1001), "in the gap");
        assert!(!cov.has(1050), "in the gap");
        assert!(cov.has(1101));
        assert!(!cov.has(2500), "between the top and the live tail");
        assert!(cov.has(3000), "at the live tail");
        assert!(cov.has(u32::MAX), "the live tail is open-ended");
        assert!(!cov.has(1));
        assert!(!coverage(vec![r(2, 1001)], vec![], None).has(1001));
    }

    #[test]
    fn segments_list_the_upstreams_of_each_part() {
        let a = frozen(0, 2, 1001);
        let a2 = frozen(1, 2, 1001);
        let b = frozen(2, 1101, 2001);
        let c = live(3, 1900, 2300);
        let v = view(vec![a, a2, b, c]);
        let segs = v.segments(Log::Trace);
        assert_eq!(
            segs,
            vec![
                CoverageSegment {
                    range: r(2, 1001),
                    upstreams: vec![0, 1]
                },
                CoverageSegment {
                    range: r(1101, 1900),
                    upstreams: vec![2]
                },
                CoverageSegment {
                    range: r(1900, 2001),
                    upstreams: vec![2, 3]
                },
                CoverageSegment {
                    range: r(2001, 2301),
                    upstreams: vec![3]
                },
            ]
        );
        assert!(v.segments(Log::Finality).is_empty());
    }

    #[test]
    fn upstream_view_ranges_by_role() {
        let v = view(vec![frozen(0, 2, 1001), live(1, 900, 2000)]);
        let (f, l) = (&v.upstreams[0], &v.upstreams[1]);
        assert!(f.covers(Log::Trace, 1000) && !f.covers(Log::Trace, 1001));
        assert!(l.covers(Log::Trace, 900) && l.covers(Log::Trace, 5_000_000));
        assert!(!l.covers(Log::Finality, 1000), "no finality log");
        assert_eq!(f.ids_begin(), Some(2));
        assert_eq!(f.max_end(), 1001);
        assert_eq!(l.ids_begin(), Some(900));
        assert!(!f.at_capacity());
        let mut capped = frozen(0, 2, 1001);
        capped.max_sessions = Some(1);
        capped.connections.store(1, Ordering::Relaxed);
        let v = view(vec![capped]);
        assert!(v.upstreams[0].at_capacity());
        assert_eq!(v.up(0).map(|u| u.index), Some(0));
        assert!(v.up(5).is_none());

        let mut q = frozen(0, 2, 1001);
        q.quarantines.push(BlockQuarantine {
            block: 500,
            range: 2,
            until: Instant::now() + Duration::from_secs(60),
        });
        let v = view(vec![q]);
        let now = Instant::now();
        assert!(v.upstreams[0].quarantined_at(501, now));
        assert!(!v.upstreams[0].quarantined_at(502, now));
        assert!(!v.upstreams[0].quarantined_at(501, now + Duration::from_secs(120)));
    }
}
