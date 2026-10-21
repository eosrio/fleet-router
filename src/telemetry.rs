//! In-process state behind the dashboard: an event log, a short history of
//! per-upstream samples, per-listener counters, and the registry of active
//! client sessions.
//!
//! The registry and counters are always maintained (a few relaxed atomics per
//! frame). Events and history are only read by the dashboard API.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::{broadcast, Notify};

/// Events kept in memory for `GET /api/v1/events`.
pub const MAX_EVENTS: usize = 500;
/// One sample per second for 15 minutes.
pub const HISTORY_SAMPLES: usize = 900;
/// Sentinel for an unset block number in [`SessionInfo`].
const NO_BLOCK: u64 = u64::MAX;

/// Wall-clock time in Unix milliseconds.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Lock a std mutex, recovering the data if a holder panicked. Every critical
/// section here is short and leaves the data consistent.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Started,
    UpstreamUp,
    UpstreamDown,
    UpstreamStale,
    UpstreamFresh,
    UpstreamQuarantined,
    SessionFailover,
    SessionFailed,
    Control,
}

/// One entry in the dashboard event log.
#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub id: u64,
    pub t_ms: u64,
    pub level: Level,
    pub kind: EventKind,
    pub listener: Option<String>,
    pub upstream: Option<String>,
    pub session: Option<u64>,
    pub message: String,
}

impl Event {
    /// A new event. `id` and `t_ms` are assigned by [`Telemetry::emit`].
    pub fn new(level: Level, kind: EventKind, message: impl Into<String>) -> Self {
        Event {
            id: 0,
            t_ms: 0,
            level,
            kind,
            listener: None,
            upstream: None,
            session: None,
            message: message.into(),
        }
    }

    pub fn listener(mut self, name: &str) -> Self {
        self.listener = Some(name.to_string());
        self
    }

    pub fn upstream(mut self, endpoint: &str) -> Self {
        self.upstream = Some(endpoint.to_string());
        self
    }

    pub fn session(mut self, id: u64) -> Self {
        self.session = Some(id);
        self
    }
}

/// Per-upstream values recorded once per second.
#[derive(Debug, Clone, Serialize)]
pub struct UpstreamSample {
    pub sessions: usize,
    pub head_block: Option<u32>,
    pub blocks_total: u64,
    pub bytes_total: u64,
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct Sample {
    pub t_ms: u64,
    /// Keyed by `<listener>|<endpoint>`.
    pub upstreams: BTreeMap<String, UpstreamSample>,
}

/// Accept-loop counters for one client listener.
#[derive(Debug, Default)]
pub struct ListenerStats {
    pub accepted_total: AtomicU64,
    pub rejected_total: AtomicU64,
}

/// Live view of one client session, shared between the session task and the
/// control API.
#[derive(Debug)]
pub struct SessionInfo {
    pub id: u64,
    pub listener: Arc<str>,
    pub config_generation: u64,
    pub client: SocketAddr,
    pub started_ms: u64,
    upstream: Mutex<String>,
    start_block: AtomicU64,
    last_block: AtomicU64,
    /// The start of the latest request, then one past each block delivered.
    next_block: AtomicU64,
    finality: AtomicBool,
    blocks: AtomicU64,
    bytes: AtomicU64,
    failovers: AtomicU32,
    move_requested: AtomicBool,
    close_requested: AtomicBool,
    /// Wakes the session loop after [`Self::request_move`] or [`Self::request_close`].
    pub control: Notify,
}

impl SessionInfo {
    fn block(value: &AtomicU64) -> Option<u32> {
        match value.load(Ordering::Relaxed) {
            NO_BLOCK => None,
            block => u32::try_from(block).ok(),
        }
    }

    pub fn upstream(&self) -> String {
        lock(&self.upstream).clone()
    }

    pub fn set_upstream(&self, endpoint: &str) {
        let _change = global().begin_change();
        *lock(&self.upstream) = endpoint.to_string();
    }

    pub fn start_block(&self) -> Option<u32> {
        Self::block(&self.start_block)
    }

    pub fn last_block(&self) -> Option<u32> {
        Self::block(&self.last_block)
    }

    /// The block this session needs next: one past the last block delivered
    /// for its latest request, or that request's start block before any
    /// arrives.
    pub fn next_block(&self) -> Option<u32> {
        Self::block(&self.next_block)
    }

    pub fn finality(&self) -> bool {
        self.finality.load(Ordering::Relaxed)
    }

    pub fn blocks(&self) -> u64 {
        self.blocks.load(Ordering::Relaxed)
    }

    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    pub fn failovers(&self) -> u32 {
        self.failovers.load(Ordering::Relaxed)
    }

    /// Record a client `get_blocks` request.
    pub fn note_request(&self, start_block: u32, finality: bool) {
        let _change = global().begin_change();
        self.start_block
            .store(u64::from(start_block), Ordering::Relaxed);
        self.next_block
            .store(u64::from(start_block), Ordering::Relaxed);
        self.finality.store(finality, Ordering::Relaxed);
    }

    /// Record one frame forwarded to the client.
    pub fn note_frame(&self, bytes: usize, block: Option<u32>) {
        self.bytes
            .fetch_add(u64::try_from(bytes).unwrap_or(u64::MAX), Ordering::Relaxed);
        if let Some(block) = block {
            self.blocks.fetch_add(1, Ordering::Relaxed);
            self.last_block.store(u64::from(block), Ordering::Relaxed);
            self.next_block
                .store(u64::from(block.saturating_add(1)), Ordering::Relaxed);
        }
    }

    pub fn note_failover(&self) {
        let _change = global().begin_change();
        self.failovers.fetch_add(1, Ordering::Relaxed);
    }

    /// Ask the session to fail over to another upstream.
    pub fn request_move(&self) {
        self.move_requested.store(true, Ordering::Release);
        self.control.notify_one();
    }

    /// Ask the session to close with code 1000.
    pub fn request_close(&self) {
        self.close_requested.store(true, Ordering::Release);
        self.control.notify_one();
    }

    /// Consume a pending move request.
    pub fn take_move_request(&self) -> bool {
        self.move_requested.swap(false, Ordering::AcqRel)
    }

    pub fn close_requested(&self) -> bool {
        self.close_requested.load(Ordering::Acquire)
    }
}

/// Removes its session from the registry when dropped.
pub struct SessionRegistration {
    pub info: Arc<SessionInfo>,
}

impl Drop for SessionRegistration {
    fn drop(&mut self) {
        let _change = global().begin_change();
        lock(&global().sessions).remove(&self.info.id);
    }
}

#[derive(Default)]
struct Lifecycle {
    generation: u64,
    writers: usize,
}

/// Marks a lifecycle transaction without holding a lock across its work. Nested
/// and concurrent writers are allowed; capture retries while any are active.
pub(crate) struct LifecycleChange<'a>(&'a Telemetry);

impl Drop for LifecycleChange<'_> {
    fn drop(&mut self) {
        let mut lifecycle = lock(&self.0.lifecycle);
        lifecycle.writers -= 1;
        lifecycle.generation = lifecycle.generation.wrapping_add(1);
        if lifecycle.writers == 0 {
            self.0.changed.notify_one();
        }
    }
}

pub struct Telemetry {
    pub started_ms: u64,
    next_event_id: AtomicU64,
    events: Mutex<VecDeque<Event>>,
    events_tx: broadcast::Sender<Event>,
    history: Mutex<VecDeque<Sample>>,
    next_session_id: AtomicU64,
    sessions: Mutex<HashMap<u64, Arc<SessionInfo>>>,
    lifecycle: Mutex<Lifecycle>,
    /// Coalesced lifecycle changes for the single dashboard publisher. Block
    /// counters are sampled separately; forwarding never wakes this task.
    pub changed: Notify,
}

static TELEMETRY: OnceLock<Telemetry> = OnceLock::new();

/// The process-wide telemetry store.
pub fn global() -> &'static Telemetry {
    TELEMETRY.get_or_init(|| Telemetry {
        started_ms: now_ms(),
        next_event_id: AtomicU64::new(1),
        events: Mutex::new(VecDeque::with_capacity(MAX_EVENTS)),
        events_tx: broadcast::channel(256).0,
        history: Mutex::new(VecDeque::with_capacity(HISTORY_SAMPLES)),
        next_session_id: AtomicU64::new(1),
        sessions: Mutex::new(HashMap::new()),
        lifecycle: Mutex::default(),
        changed: Notify::new(),
    })
}

impl Telemetry {
    /// Use around state changes and their associated events. Frame counters do
    /// not enter this path: their samples remain independent relaxed atomics.
    pub(crate) fn begin_change(&self) -> LifecycleChange<'_> {
        let mut lifecycle = lock(&self.lifecycle);
        lifecycle.generation = lifecycle.generation.wrapping_add(1);
        lifecycle.writers += 1;
        LifecycleChange(self)
    }

    pub(crate) fn capture_version(&self) -> Option<u64> {
        let lifecycle = lock(&self.lifecycle);
        (lifecycle.writers == 0).then_some(lifecycle.generation)
    }

    /// Complete a capture only if no lifecycle writer overlapped it. Derived
    /// status transitions join the event log under the same lock as its final
    /// watermark, so intervening external events cannot be skipped.
    pub(crate) fn capture_events(
        &self,
        generation: u64,
        after: u64,
        transitions: Vec<Event>,
    ) -> Option<Vec<Event>> {
        let lifecycle = lock(&self.lifecycle);
        if lifecycle.writers != 0 || lifecycle.generation != generation {
            return None;
        }
        let mut events = lock(&self.events);
        for event in transitions {
            let event = self.append_event(&mut events, event);
            let _ = self.events_tx.send(event);
        }
        Some(
            events
                .iter()
                .filter(|event| event.id > after)
                .cloned()
                .collect(),
        )
    }

    fn append_event(&self, events: &mut VecDeque<Event>, mut event: Event) -> Event {
        event.t_ms = now_ms();
        // Assigned under the event lock so ids are in log order.
        event.id = self.next_event_id.fetch_add(1, Ordering::Relaxed);
        if events.len() == MAX_EVENTS {
            events.pop_front();
        }
        events.push_back(event.clone());
        event
    }

    /// Assign an id and timestamp, keep the event, and publish it to live streams.
    pub fn emit(&self, event: Event) {
        let _change = self.begin_change();
        let mut events = lock(&self.events);
        let event = self.append_event(&mut events, event);
        let _ = self.events_tx.send(event);
    }

    /// Events with an id greater than `after`, oldest first.
    pub fn events_after(&self, after: u64) -> Vec<Event> {
        lock(&self.events)
            .iter()
            .filter(|event| event.id > after)
            .cloned()
            .collect()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events_tx.subscribe()
    }

    pub fn record_sample(&self, sample: Sample) {
        let mut history = lock(&self.history);
        if history.len() == HISTORY_SAMPLES {
            history.pop_front();
        }
        history.push_back(sample);
    }

    pub fn history(&self) -> Vec<Sample> {
        lock(&self.history).iter().cloned().collect()
    }

    /// Add a session to the registry. It is removed when the returned guard drops.
    #[cfg(test)]
    pub fn register_session(
        &self,
        listener: Arc<str>,
        client: SocketAddr,
        upstream: &str,
    ) -> SessionRegistration {
        self.register_generation(listener, client, upstream, 0)
    }

    pub fn register_generation(
        &self,
        listener: Arc<str>,
        client: SocketAddr,
        upstream: &str,
        config_generation: u64,
    ) -> SessionRegistration {
        let _change = self.begin_change();
        let info = Arc::new(SessionInfo {
            config_generation,
            id: self.next_session_id.fetch_add(1, Ordering::Relaxed),
            listener,
            client,
            started_ms: now_ms(),
            upstream: Mutex::new(upstream.to_string()),
            start_block: AtomicU64::new(NO_BLOCK),
            last_block: AtomicU64::new(NO_BLOCK),
            next_block: AtomicU64::new(NO_BLOCK),
            finality: AtomicBool::new(false),
            blocks: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            failovers: AtomicU32::new(0),
            move_requested: AtomicBool::new(false),
            close_requested: AtomicBool::new(false),
            control: Notify::new(),
        });
        lock(&self.sessions).insert(info.id, info.clone());
        SessionRegistration { info }
    }

    pub fn session(&self, id: u64) -> Option<Arc<SessionInfo>> {
        lock(&self.sessions).get(&id).cloned()
    }

    /// Active sessions, oldest first.
    pub fn sessions(&self) -> Vec<Arc<SessionInfo>> {
        let mut sessions: Vec<_> = lock(&self.sessions).values().cloned().collect();
        sessions.sort_by_key(|session| session.id);
        sessions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_get_increasing_ids_and_after_filters() {
        let telemetry = global();
        let before = telemetry.events_after(0).last().map_or(0, |event| event.id);
        telemetry.emit(Event::new(Level::Info, EventKind::Control, "first"));
        telemetry.emit(Event::new(Level::Warn, EventKind::Control, "second").listener("eos"));
        let newer: Vec<_> = telemetry
            .events_after(before)
            .into_iter()
            .filter(|event| event.message == "first" || event.message == "second")
            .collect();
        assert_eq!(newer.len(), 2);
        assert!(newer[0].id < newer[1].id);
        assert_eq!(newer[1].listener.as_deref(), Some("eos"));
    }

    #[test]
    fn registration_is_removed_on_drop_and_tracks_frames() {
        let telemetry = global();
        let client: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let registration = telemetry.register_session(Arc::from("t"), client, "up:1");
        let id = registration.info.id;
        let info = telemetry.session(id).unwrap();
        assert_eq!(info.start_block(), None);
        assert_eq!(info.next_block(), None);
        info.note_request(100, true);
        assert_eq!(info.next_block(), Some(100));
        info.note_frame(10, Some(100));
        info.note_frame(5, None);
        assert_eq!(info.start_block(), Some(100));
        assert_eq!(info.last_block(), Some(100));
        assert_eq!(info.next_block(), Some(101));
        assert_eq!((info.blocks(), info.bytes()), (1, 15));
        assert!(info.finality());

        info.request_move();
        assert!(info.take_move_request());
        assert!(!info.take_move_request());

        drop(registration);
        assert!(telemetry.session(id).is_none());
    }

    #[test]
    fn a_new_request_restarts_the_next_block_at_its_start() {
        let telemetry = global();
        let client: SocketAddr = "127.0.0.1:5001".parse().unwrap();
        let registration = telemetry.register_session(Arc::from("t"), client, "up:1");
        let info = registration.info.clone();
        info.note_request(4_000, false);
        info.note_frame(10, Some(5_000));
        assert_eq!(info.next_block(), Some(5_001));
        // The client asks for older blocks: the API decides on them, not on
        // where the previous request ended.
        info.note_request(100, false);
        assert_eq!(info.next_block(), Some(100));
        assert_eq!(info.last_block(), Some(5_000), "still the last delivered");
        info.note_frame(10, Some(100));
        assert_eq!(info.next_block(), Some(101));
    }
}
