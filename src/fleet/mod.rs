//! Per-listener fleet state: the hub that rebuilds and publishes fleet view
//! snapshots, the tips observed on live links and the listener statistics.
//!
//! Each listener has one [`FleetHub`]. The upstream monitors call
//! [`FleetHub::rebuild`] after every change to the listener's state map, and
//! the hub publishes a new [`FleetView`] when the routing picture changed.
//! Sessions read the latest view without locking the state map. They report
//! the head and LIB of every live frame into an [`ObservedTip`], which keeps
//! the fleet tip fresher than the monitor's status interval.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::watch;

use crate::abi::AbiEntry;
use crate::models::{
    AbiMismatch, ResolvedListener, ResolvedTopology, ServerConfigDb, ServerState, ServerStateDb,
};
use crate::ship::BlockPos;

/// A metric label with a fixed set of values. The values index counter
/// arrays and are rendered as label values on `/metrics`.
pub trait MetricLabel: Copy + 'static {
    /// Every value, in rendering order.
    const ALL: &'static [Self];
    /// Label value on `/metrics`.
    fn as_str(self) -> &'static str;
    /// Position in [`Self::ALL`].
    fn index(self) -> usize;
}

/// Declares a fieldless enum and its [`MetricLabel`] impl.
macro_rules! metric_label {
    (
        $(#[$meta:meta])*
        $vis:vis enum $name:ident { $($(#[$vmeta:meta])* $variant:ident => $label:literal),+ $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
        $vis enum $name { $($(#[$vmeta])* $variant),+ }

        impl $crate::fleet::MetricLabel for $name {
            const ALL: &'static [$name] = &[$($name::$variant),+];
            fn as_str(self) -> &'static str {
                match self { $($name::$variant => $label),+ }
            }
            fn index(self) -> usize {
                self as usize
            }
        }
    };
}

pub mod planner;
pub mod view;

pub use planner::HoldWhy;
pub use view::{FleetView, Log};

use view::{BuildCtx, HubMemory, UpstreamStatic};

metric_label! {
    /// `fleet_router_sessions{state}`: what a client session is doing.
    pub enum SessionGauge {
        /// No request, or the request is satisfied.
        Idle => "idle",
        Streaming => "streaming",
        Dialing => "dialing",
        Backoff => "backoff",
        Holding => "holding",
        Waiting => "waiting",
    }
}

metric_label! {
    /// `fleet_router_failovers_total{reason}`.
    pub enum FailoverReason {
        Unclean => "unclean",
        Clean => "clean",
        Idle => "idle",
        Stall => "stall",
        Offline => "offline",
        Violation => "violation",
        Uncovered => "uncovered",
        Send => "send",
        /// No frame at all, not even a reply to a liveness ping.
        Silent => "silent",
        /// Moved by an operator through the control API.
        Operator => "operator",
        /// Left a frozen upstream without a block log for one that holds the
        /// blocks a `fetch_block` request asks for.
        NoBlockLog => "no_block_log",
    }
}

metric_label! {
    /// `fleet_router_predials_total{result}`.
    pub enum PredialResult {
        Used => "used",
        Failed => "failed",
        Discarded => "discarded",
    }
}

metric_label! {
    /// `fleet_router_client_protocol_errors_total{kind}`.
    pub enum ProtocolError {
        Malformed => "malformed",
        UnknownVariant => "unknown_variant",
    }
}

metric_label! {
    /// `fleet_router_upstream_links_total{purpose}`: why a session opened a link.
    pub enum LinkPurpose {
        Initial => "initial",
        Handoff => "handoff",
        Failover => "failover",
        Reroute => "reroute",
        Predial => "predial",
    }
}

metric_label! {
    /// `fleet_router_upstream_dial_failures_total{reason}`.
    pub enum DialFailure {
        /// HTTP 503 at the upgrade.
        Busy => "busy",
        Unreachable => "unreachable",
        Handshake => "handshake",
        Abi => "abi",
    }
}

metric_label! {
    /// `fleet_router_upstream_contract_violations_total{kind}`: a frozen
    /// upstream broke the archive contract.
    pub enum ContractViolation {
        Idless => "idless",
        Gap => "gap",
        Rewind => "rewind",
        MissingField => "missing_field",
        Credit => "credit",
        Variant => "variant",
        Discontinuity => "discontinuity",
    }
}

/// One counter per value of the label `L`.
pub struct LabeledCounter<L: MetricLabel> {
    counts: Box<[AtomicU64]>,
    label: PhantomData<fn() -> L>,
}

impl<L: MetricLabel> LabeledCounter<L> {
    pub fn new() -> Self {
        LabeledCounter {
            counts: L::ALL.iter().map(|_| AtomicU64::new(0)).collect(),
            label: PhantomData,
        }
    }

    pub fn inc(&self, label: L) {
        self.add(label, 1);
    }

    pub fn add(&self, label: L, n: u64) {
        self.counts[label.index()].fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self, label: L) -> u64 {
        self.counts[label.index()].load(Ordering::Relaxed)
    }

    /// Every label value with its count, in rendering order.
    pub fn values(&self) -> impl Iterator<Item = (L, u64)> + '_ {
        L::ALL.iter().map(|&label| (label, self.get(label)))
    }
}

impl<L: MetricLabel> Default for LabeledCounter<L> {
    fn default() -> Self {
        Self::new()
    }
}

impl<L: MetricLabel> std::fmt::Debug for LabeledCounter<L> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries(self.values().map(|(label, n)| (label.as_str(), n)))
            .finish()
    }
}

/// Bucket upper bounds of `fleet_router_handoff_latency_seconds`: the `le`
/// label as rendered, and the bound in microseconds.
pub const HANDOFF_BUCKETS: [(&str, u64); 12] = [
    ("0.001", 1_000),
    ("0.005", 5_000),
    ("0.01", 10_000),
    ("0.025", 25_000),
    ("0.05", 50_000),
    ("0.1", 100_000),
    ("0.25", 250_000),
    ("0.5", 500_000),
    ("1", 1_000_000),
    ("2.5", 2_500_000),
    ("5", 5_000_000),
    ("10", 10_000_000),
];

/// A Prometheus histogram over [`HANDOFF_BUCKETS`]. Bucket counts are stored
/// per bucket and summed when rendered.
#[derive(Debug, Default)]
pub struct Histogram {
    buckets: [AtomicU64; HANDOFF_BUCKETS.len()],
    count: AtomicU64,
    sum_us: AtomicU64,
}

/// What a histogram renders: cumulative bucket counts, the observation count
/// and the sum in seconds.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSnapshot {
    pub cumulative: Vec<(&'static str, u64)>,
    pub count: u64,
    pub sum_seconds: f64,
}

impl Histogram {
    pub fn observe(&self, elapsed: Duration) {
        let us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        if let Some(i) = HANDOFF_BUCKETS.iter().position(|&(_, bound)| us <= bound) {
            self.buckets[i].fetch_add(1, Ordering::Relaxed);
        }
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let mut running = 0u64;
        let cumulative = HANDOFF_BUCKETS
            .iter()
            .zip(&self.buckets)
            .map(|(&(le, _), n)| {
                running = running.saturating_add(n.load(Ordering::Relaxed));
                (le, running)
            })
            .collect();
        HistogramSnapshot {
            cumulative,
            // An observation that is bucketed but not counted yet must not
            // make `+Inf` smaller than the last bucket.
            count: count.max(running),
            sum_seconds: self.sum_us.load(Ordering::Relaxed) as f64 / 1e6,
        }
    }
}

/// Session counters for one upstream of a listener.
#[derive(Debug, Default)]
pub struct UpstreamStats {
    pub links: LabeledCounter<LinkPurpose>,
    pub dial_failures: LabeledCounter<DialFailure>,
    pub contract_violations: LabeledCounter<ContractViolation>,
    pub stalls: AtomicU64,
    /// Live forward jumps that a session adopted.
    pub sequence_jumps: AtomicU64,
    /// Frozen results forwarded without the `block` the request asked for,
    /// because no upstream that holds blocks could serve them.
    pub results_without_block: AtomicU64,
    /// Frozen results that lacked `prev_block` right after a block the
    /// session had delivered, filled in by the router from that block.
    pub prev_blocks_filled: AtomicU64,
}

/// Session counters and gauges of one listener (`fleet_router_*` per-listener
/// series), plus the session counters of each of its upstreams.
#[derive(Debug)]
pub struct ListenerStats {
    /// Indexed by [`SessionGauge::index`].
    pub sessions_by_state: [AtomicI64; SessionGauge::ALL.len()],
    /// `[v0 router, v0 upstream, v1 router, v1 upstream]`.
    status_replies: [AtomicU64; 4],
    pub handoffs: AtomicU64,
    /// From the last frame of the old link to the first frame of the new one.
    pub handoff_latency: Histogram,
    pub failovers: LabeledCounter<FailoverReason>,
    /// Pool first-request reroutes.
    pub reroutes: AtomicU64,
    /// Equals the refund acks sent upstream.
    pub duplicates_dropped: AtomicU64,
    pub fork_redeliveries: AtomicU64,
    /// Legacy head keep-alive results forwarded.
    pub keepalives: AtomicU64,
    /// Results forwarded while the client had no credit. Stays 0 with Spring
    /// upstreams.
    pub credit_overruns: AtomicU64,
    pub patched_results: AtomicU64,
    pub holds: LabeledCounter<HoldWhy>,
    pub hold_closes: LabeledCounter<HoldWhy>,
    pub predials: LabeledCounter<PredialResult>,
    pub client_protocol_errors: LabeledCounter<ProtocolError>,
    pub too_large_closes: AtomicU64,
    /// Indexed by `ServerState::index`.
    pub upstreams: Box<[UpstreamStats]>,
}

impl ListenerStats {
    pub fn new(upstreams: usize) -> Self {
        ListenerStats {
            sessions_by_state: Default::default(),
            status_replies: Default::default(),
            handoffs: AtomicU64::new(0),
            handoff_latency: Histogram::default(),
            failovers: LabeledCounter::new(),
            reroutes: AtomicU64::new(0),
            duplicates_dropped: AtomicU64::new(0),
            fork_redeliveries: AtomicU64::new(0),
            keepalives: AtomicU64::new(0),
            credit_overruns: AtomicU64::new(0),
            patched_results: AtomicU64::new(0),
            holds: LabeledCounter::new(),
            hold_closes: LabeledCounter::new(),
            predials: LabeledCounter::new(),
            client_protocol_errors: LabeledCounter::new(),
            too_large_closes: AtomicU64::new(0),
            upstreams: (0..upstreams).map(|_| UpstreamStats::default()).collect(),
        }
    }

    /// Counters of the upstream with `ServerState::index == index`.
    pub fn upstream(&self, index: usize) -> Option<&UpstreamStats> {
        self.upstreams.get(index)
    }

    /// Move one session between `fleet_router_sessions` states. `None` is
    /// "not counted": a session enters with `from = None` and leaves with
    /// `to = None`.
    pub fn session_moved(&self, from: Option<SessionGauge>, to: Option<SessionGauge>) {
        if from == to {
            return;
        }
        if let Some(from) = from {
            self.sessions_by_state[from.index()].fetch_sub(1, Ordering::Relaxed);
        }
        if let Some(to) = to {
            self.sessions_by_state[to.index()].fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn sessions(&self, state: SessionGauge) -> i64 {
        self.sessions_by_state[state.index()].load(Ordering::Relaxed)
    }

    /// Count one status reply sent to a client. `from_router` is true for a
    /// reply the router encoded itself, false for a relayed upstream reply.
    pub fn status_reply(&self, v1: bool, from_router: bool) {
        self.status_replies[status_slot(v1, from_router)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn status_replies(&self, v1: bool, from_router: bool) -> u64 {
        self.status_replies[status_slot(v1, from_router)].load(Ordering::Relaxed)
    }
}

fn status_slot(v1: bool, from_router: bool) -> usize {
    usize::from(v1) * 2 + usize::from(!from_router)
}

/// Highest head and LIB seen on session links to one live upstream.
///
/// Sessions call [`Self::observe`] for every live frame, so the fleet tip
/// does not wait for the next monitor status reply.
#[derive(Debug, Default)]
pub struct ObservedTip {
    head_num: AtomicU32,
    lib_num: AtomicU32,
    inner: Mutex<Option<(BlockPos, BlockPos)>>,
}

impl ObservedTip {
    /// Record a frame's head and LIB. Locks only when head or LIB grows.
    pub fn observe(&self, head: BlockPos, lib: BlockPos) {
        if head.num <= self.head_num.load(Ordering::Relaxed)
            && lib.num <= self.lib_num.load(Ordering::Relaxed)
        {
            return;
        }
        let mut inner = lock(&self.inner);
        let (head, lib) = match *inner {
            Some((seen_head, seen_lib)) => (
                if head.num > seen_head.num {
                    head
                } else {
                    seen_head
                },
                if lib.num > seen_lib.num {
                    lib
                } else {
                    seen_lib
                },
            ),
            None => (head, lib),
        };
        *inner = Some((head, lib));
        self.head_num.store(head.num, Ordering::Relaxed);
        self.lib_num.store(lib.num, Ordering::Relaxed);
    }

    /// The highest `(head, lib)` observed so far.
    pub fn get(&self) -> Option<(BlockPos, BlockPos)> {
        *lock(&self.inner)
    }
}

/// The fleet state of one listener: its upstream state map, the latest
/// published [`FleetView`], the observed tips and the listener statistics.
pub struct FleetHub {
    /// `None` in single-listener mode.
    pub name: Option<Arc<str>>,
    pub state: ServerStateDb,
    /// The listener's enabled `Server` entries, by endpoint.
    pub config: ServerConfigDb,
    pub topology: ResolvedTopology,
    pub pinned_chain_id: Option<[u8; 32]>,
    /// The `stitching.abi_file` text of a stitched listener.
    pub abi_pin: Option<Arc<AbiEntry>>,
    /// Indexed by `ServerState::index`.
    pub tips: Arc<[Arc<ObservedTip>]>,
    pub stats: Arc<ListenerStats>,
    abi_mismatch: AbiMismatch,
    statics: Vec<UpstreamStatic>,
    view_tx: watch::Sender<Arc<FleetView>>,
    mem: Mutex<HubMemory>,
}

impl FleetHub {
    /// Build the hub of `resolved` and publish its first view. `state` and
    /// `config` are the listener's maps, built from `resolved.servers`.
    pub fn new(
        resolved: &ResolvedListener,
        state: ServerStateDb,
        config: ServerConfigDb,
    ) -> Arc<Self> {
        let statics: Vec<UpstreamStatic> = resolved
            .servers
            .iter()
            .filter(|s| s.enabled)
            .map(|s| UpstreamStatic {
                endpoint: Arc::from(s.endpoint.as_str()),
                name: Arc::from(s.name.as_str()),
                url: Arc::from(s.ws_url()),
            })
            .collect();
        let tips: Vec<Arc<ObservedTip>> = statics
            .iter()
            .map(|_| Arc::new(ObservedTip::default()))
            .collect();
        let (view_tx, _) = watch::channel(Arc::new(FleetView::empty(
            resolved.topology,
            resolved.expected_chain_id,
        )));
        let hub = Arc::new(FleetHub {
            name: resolved.name.as_deref().map(Arc::from),
            state,
            config,
            topology: resolved.topology,
            pinned_chain_id: resolved.expected_chain_id,
            abi_pin: resolved
                .abi_pin
                .as_deref()
                .map(|text| Arc::new(AbiEntry::new(text))),
            tips: tips.into(),
            stats: Arc::new(ListenerStats::new(statics.len())),
            abi_mismatch: resolved.stitching.abi_mismatch,
            statics,
            view_tx,
            mem: Mutex::new(HubMemory::new(Instant::now())),
        });
        // The maps were just built, so nothing else holds the lock.
        if let Ok(states) = hub.state.try_lock() {
            hub.rebuild(&states);
        }
        hub
    }

    /// Rebuild the view from `states` and publish it when it differs from the
    /// current one.
    ///
    /// Called with the state map borrowed under its lock, after every
    /// change: a status update, an upstream going offline, a stale flip, a
    /// quarantine change. Holding the lock keeps publications in the order of
    /// the state changes.
    pub fn rebuild(&self, states: &HashMap<String, ServerState>) {
        let mut mem = lock(&self.mem);
        let ctx = BuildCtx {
            topology: self.topology,
            pinned_chain_id: self.pinned_chain_id,
            abi_pin: self.abi_pin.as_ref(),
            abi_mismatch: self.abi_mismatch,
            statics: &self.statics,
            listener: self.name.as_deref(),
        };
        let mut view = view::build_view(&ctx, states, &self.tips, &mut mem, Instant::now());
        if view.same_content(&self.view_tx.borrow()) {
            return;
        }
        mem.generation += 1;
        view.generation = mem.generation;
        self.view_tx.send_replace(Arc::new(view));
    }

    /// Lock the state map and [`Self::rebuild`] from it.
    pub async fn refresh(&self) {
        let states = self.state.lock().await;
        self.rebuild(&states);
    }

    /// A receiver that wakes on every published view.
    pub fn subscribe(&self) -> watch::Receiver<Arc<FleetView>> {
        self.view_tx.subscribe()
    }

    /// The latest published view.
    pub fn view(&self) -> Arc<FleetView> {
        self.view_tx.borrow().clone()
    }

    /// Fleet head and LIB of `v` with the current observed tips merged in,
    /// which is fresher than the view. The listener LIB never decreases.
    pub fn fresh_tip(&self, v: &FleetView) -> (Option<BlockPos>, Option<BlockPos>) {
        view::fresh_tip(v, &self.tips, &mut lock(&self.mem), Instant::now())
    }

    /// Time since the fleet head last advanced, or since the hub was built
    /// when no head is known yet.
    pub fn head_age(&self, now: Instant) -> Duration {
        now.saturating_duration_since(lock(&self.mem).head_advanced_at)
    }
}

/// Lock a std mutex, recovering the data if a panicking holder poisoned it.
/// Every critical section here leaves the data consistent.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{build_config_db, build_state_db, Role, Server, StitchingPolicy};
    use crate::ship::Range;

    fn pos(num: u32, tag: u8) -> BlockPos {
        BlockPos { num, id: [tag; 32] }
    }

    #[test]
    fn observed_tip_keeps_the_highest_head_and_lib_separately() {
        let tip = ObservedTip::default();
        assert_eq!(tip.get(), None);
        tip.observe(pos(100, 1), pos(90, 1));
        tip.observe(pos(99, 2), pos(95, 2));
        assert_eq!(tip.get(), Some((pos(100, 1), pos(95, 2))));
        // An equal number keeps the first id.
        tip.observe(pos(100, 3), pos(95, 3));
        assert_eq!(tip.get(), Some((pos(100, 1), pos(95, 2))));
        tip.observe(pos(120, 4), pos(80, 4));
        assert_eq!(tip.get(), Some((pos(120, 4), pos(95, 2))));
    }

    #[test]
    fn histogram_buckets_are_cumulative_and_sum_is_in_seconds() {
        let h = Histogram::default();
        h.observe(Duration::from_micros(500));
        h.observe(Duration::from_millis(30));
        h.observe(Duration::from_secs(20));
        let snap = h.snapshot();
        assert_eq!(snap.count, 3);
        assert_eq!(snap.cumulative[0], ("0.001", 1));
        assert_eq!(snap.cumulative[3], ("0.025", 1));
        assert_eq!(snap.cumulative[4], ("0.05", 2));
        assert_eq!(snap.cumulative[11], ("10", 2));
        assert!((snap.sum_seconds - 20.0305).abs() < 1e-9);
    }

    #[test]
    fn listener_stats_count_by_label() {
        let stats = ListenerStats::new(2);
        stats.failovers.inc(FailoverReason::Clean);
        stats.failovers.add(FailoverReason::Stall, 2);
        assert_eq!(stats.failovers.get(FailoverReason::Clean), 1);
        assert_eq!(stats.failovers.get(FailoverReason::Stall), 2);
        assert_eq!(stats.failovers.get(FailoverReason::Unclean), 0);
        assert_eq!(stats.failovers.values().count(), FailoverReason::ALL.len());

        stats.session_moved(None, Some(SessionGauge::Dialing));
        stats.session_moved(Some(SessionGauge::Dialing), Some(SessionGauge::Streaming));
        stats.session_moved(Some(SessionGauge::Streaming), Some(SessionGauge::Streaming));
        assert_eq!(stats.sessions(SessionGauge::Dialing), 0);
        assert_eq!(stats.sessions(SessionGauge::Streaming), 1);
        stats.session_moved(Some(SessionGauge::Streaming), None);
        assert_eq!(stats.sessions(SessionGauge::Streaming), 0);

        stats.status_reply(true, true);
        stats.status_reply(false, false);
        stats.status_reply(false, false);
        assert_eq!(stats.status_replies(true, true), 1);
        assert_eq!(stats.status_replies(false, false), 2);
        assert_eq!(stats.status_replies(false, true), 0);

        stats
            .upstream(1)
            .unwrap()
            .dial_failures
            .inc(DialFailure::Busy);
        assert_eq!(
            stats
                .upstream(1)
                .unwrap()
                .dial_failures
                .get(DialFailure::Busy),
            1
        );
        assert!(stats.upstream(2).is_none());
        assert_eq!(ContractViolation::MissingField.as_str(), "missing_field");
        assert_eq!(HoldWhy::AtCapacity.as_str(), "at_capacity");
    }

    fn listener(servers: Vec<Server>) -> ResolvedListener {
        ResolvedListener {
            name: Some("main".to_string()),
            listen_address: "127.0.0.1".to_string(),
            listen_port: 17000,
            max_connections: 10,
            expected_chain_id: Some([7; 32]),
            require_chain_id: false,
            block_failure_quarantine: None,
            topology: ResolvedTopology::Stitched,
            stitching: StitchingPolicy::default(),
            abi_pin: None,
            servers,
        }
    }

    #[tokio::test]
    async fn hub_publishes_only_changed_views_and_merges_observed_tips() {
        let servers = vec![
            Server {
                name: "archive".to_string(),
                endpoint: "127.0.0.1:9000".to_string(),
                enabled: true,
                role: Role::Frozen,
                range: Some(Range {
                    begin: 2,
                    end: 1001,
                }),
                ..Default::default()
            },
            Server {
                name: "off".to_string(),
                endpoint: "127.0.0.1:9002".to_string(),
                enabled: false,
                ..Default::default()
            },
            Server {
                name: "live".to_string(),
                endpoint: "ws://127.0.0.1:9001".to_string(),
                enabled: true,
                ..Default::default()
            },
        ];
        let resolved = listener(servers.clone());
        let state = build_state_db(servers.clone());
        let hub = FleetHub::new(&resolved, state.clone(), build_config_db(servers));
        let mut rx = hub.subscribe();

        assert_eq!(hub.config.lock().await.len(), 2, "enabled servers only");
        let first = hub.view();
        assert_eq!(first.generation, 1, "the first view is published by new");
        assert_eq!(
            first.upstreams.len(),
            2,
            "disabled servers are not part of the view"
        );
        assert_eq!(&*first.upstreams[1].url, "ws://127.0.0.1:9001");
        assert_eq!(
            first.trace.top,
            Range {
                begin: 2,
                end: 1001
            }
        );
        assert_eq!(first.head, None);
        assert!(!first.status_ready);

        // Nothing changed: no new generation, no wake-up.
        hub.refresh().await;
        assert_eq!(hub.view().generation, 1);
        assert!(!rx.has_changed().unwrap());

        {
            let mut db = state.lock().await;
            let live = db.get_mut("ws://127.0.0.1:9001").unwrap();
            live.online = true;
            live.ranges_known = true;
            live.trace_begin_block = 900;
            live.trace_end_block = 2001;
            live.chain_state_begin_block = 900;
            live.chain_state_end_block = 2001;
            live.head = Some(pos(2000, 1));
            live.lib = Some(pos(1990, 1));
            hub.rebuild(&db);
        }
        assert!(rx.has_changed().unwrap());
        let view = rx.borrow_and_update().clone();
        assert_eq!(view.generation, 2);
        assert_eq!(view.head, Some(pos(2000, 1)));
        assert!(view.status_ready);

        // A session sees a newer frame: fresh_tip has it before any rebuild.
        hub.tips[1].observe(pos(2010, 2), pos(2000, 2));
        assert_eq!(
            hub.fresh_tip(&view),
            (Some(pos(2010, 2)), Some(pos(2000, 2)))
        );
        // The LIB high-water mark now holds 2000 even against the old view.
        {
            let mut db = state.lock().await;
            let live = db.get_mut("ws://127.0.0.1:9001").unwrap();
            live.lib = Some(pos(1980, 3));
            hub.rebuild(&db);
        }
        let view = hub.view();
        assert_eq!(view.head, Some(pos(2010, 2)));
        assert_eq!(view.lib, Some(pos(2000, 2)));
        assert!(hub.head_age(Instant::now()) < Duration::from_secs(5));
    }
}
