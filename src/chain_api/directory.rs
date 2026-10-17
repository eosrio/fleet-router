//! What the router knows about each upstream chain API of a listener.
//!
//! The probes write it and requests read it. It is kept apart from the
//! listener's `FleetView`: probe results and request counts change all the
//! time, and publishing them would wake every SHiP session. Each API also
//! has its own in-flight slots, separate from the SHiP connection counts.

use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use hyper::header::HeaderValue;
use tokio::sync::Notify;
use tokio::time::timeout_at;

use crate::fleet::{FleetHub, LabeledCounter};
use crate::models::{ApiUpstream, Role};
use crate::ship::{BlockPos, Range};

use super::pick::{ApiView, BUSY_TTL};
use super::stats::UpstreamResult;

/// Probe failures in a row that take an API down.
const DOWN_AFTER_FAILURES: u8 = 2;

/// Sticky identity verification of a frozen range server's chain API.
///
/// An API is verified when the `last` block its `describe` reports is the
/// head its SHiP endpoint reports, and its own `get_block_info` for that
/// block returns the same id. Until then it is never used. Once verified it
/// stays verified through SHiP and API outages; it is cleared only when the
/// API reports another chain, a `last` that fails the check, or when the
/// SHiP endpoint reports a head other than the verified block.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Verification {
    verified: Option<BlockPos>,
}

/// What to do after a verification event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verify {
    /// Nothing: verified for this `last`, or the SHiP head is not known yet.
    Keep,
    /// Ask the API's `get_block_info` for this block and pass the id it
    /// reports to [`Verification::checked`].
    Check(BlockPos),
    /// The API serves other blocks than its SHiP endpoint, or another
    /// chain. Any verification is cleared.
    Mismatch,
}

impl Verification {
    pub fn verified(&self) -> Option<BlockPos> {
        self.verified
    }

    /// A `describe` answer: its `last`, whether its chain_id is the
    /// listener's, and the SHiP head of the same server when known.
    pub fn described(
        &mut self,
        last: BlockPos,
        chain_ok: bool,
        ship_head: Option<BlockPos>,
    ) -> Verify {
        if !chain_ok {
            self.verified = None;
            return Verify::Mismatch;
        }
        if self.verified == Some(last) {
            return Verify::Keep;
        }
        match ship_head {
            None => Verify::Keep,
            Some(head) if head == last => Verify::Check(last),
            Some(_) => {
                self.verified = None;
                Verify::Mismatch
            }
        }
    }

    /// The SHiP head of the same server, checked on every probe.
    pub fn ship_head(&mut self, head: Option<BlockPos>) -> Verify {
        match (self.verified, head) {
            (Some(verified), Some(head)) if verified != head => {
                self.verified = None;
                Verify::Mismatch
            }
            _ => Verify::Keep,
        }
    }

    /// The id the API's `get_block_info` reported for `last`.
    pub fn checked(&mut self, last: BlockPos, id: [u8; 32]) -> Verify {
        if id == last.id {
            self.verified = Some(last);
            Verify::Keep
        } else {
            self.verified = None;
            Verify::Mismatch
        }
    }
}

/// Probe state of one upstream API.
#[derive(Debug, Default)]
pub struct ApiState {
    /// Answered its last probe (or not twice in a row since) with a
    /// matching chain_id, and no request has failed in transport since.
    pub up: bool,
    /// Probe failures in a row.
    pub fails: u8,
    /// The chain_id the API reported last.
    pub chain_id: Option<[u8; 32]>,
    /// The API reported another chain than the listener's.
    pub chain_mismatch: bool,
    /// The paths the API advertises. `None` is unknown.
    pub apis: Option<Arc<BTreeSet<String>>>,
    /// When `apis` was last asked for (live).
    pub apis_at: Option<Instant>,
    /// Live: probed head, LIB, earliest available block and version.
    pub head: Option<u32>,
    pub lib: Option<u32>,
    pub earliest: Option<u32>,
    pub version: Option<String>,
    /// Frozen: `describe.blocks`, `describe.last`, `describe.get_block`.
    pub blocks: Option<Range>,
    pub last: Option<BlockPos>,
    pub get_block: Option<String>,
    pub verification: Verification,
    /// The verification mismatch was logged; cleared once verified.
    pub mismatch_logged: bool,
    /// Skipped until then after a 503.
    pub busy_until: Option<Instant>,
}

/// One upstream chain API of a listener.
#[derive(Debug)]
pub struct ApiSlot {
    /// `ServerState::index` of the server.
    pub index: usize,
    pub name: Arc<str>,
    /// The server's SHiP endpoint, the key of its `ServerState`.
    pub ship_endpoint: String,
    pub role: Role,
    pub priority: u32,
    pub weight: u32,
    /// Lowercase `host:port`: the rendezvous key.
    pub endpoint: Arc<str>,
    /// `host:port` as configured: the upstream `Host`.
    pub authority: HeaderValue,
    /// `http://host:port`.
    pub base: String,
    /// A frozen server's configured `range`.
    pub declared: Option<Range>,
    /// Requests this API may have in flight (`max_in_flight_per_upstream`,
    /// for a frozen API also its own `describe.max_in_flight`).
    cap: AtomicUsize,
    in_flight: AtomicUsize,
    freed: Notify,
    state: Mutex<ApiState>,
    pub results: LabeledCounter<UpstreamResult>,
}

/// One request's hold on an API slot; dropping it frees the slot.
#[derive(Debug)]
pub struct SlotGuard(Arc<ApiSlot>);

impl Drop for SlotGuard {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.0.freed.notify_waiters();
    }
}

impl ApiSlot {
    fn new(upstream: &ApiUpstream, cap: usize) -> ApiSlot {
        let authority = upstream.api.authority();
        ApiSlot {
            index: upstream.index,
            name: Arc::from(upstream.name.as_str()),
            ship_endpoint: upstream.endpoint.clone(),
            role: upstream.role,
            priority: upstream.priority,
            weight: upstream.weight.max(1),
            endpoint: Arc::from(upstream.api.canonical()),
            authority: HeaderValue::from_str(&authority)
                .expect("a validated api_endpoint is a valid Host value"),
            base: upstream.api.base_url(),
            declared: upstream.range.filter(|_| upstream.role == Role::Frozen),
            cap: AtomicUsize::new(cap.max(1)),
            in_flight: AtomicUsize::new(0),
            freed: Notify::new(),
            state: Mutex::new(ApiState::default()),
            results: LabeledCounter::new(),
        }
    }

    /// The probe state, locked. Held only briefly, never across an await.
    pub fn state(&self) -> MutexGuard<'_, ApiState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    /// Requests this API may have in flight now.
    pub fn cap(&self) -> usize {
        self.cap.load(Ordering::Acquire)
    }

    /// Change the slot count; requests already in flight keep their slots.
    pub fn set_cap(&self, cap: usize) {
        if self.cap.swap(cap.max(1), Ordering::AcqRel) < cap {
            self.freed.notify_waiters();
        }
    }

    pub fn is_full(&self) -> bool {
        self.in_flight() >= self.cap.load(Ordering::Acquire)
    }

    /// A free slot, if there is one.
    pub fn try_acquire(self: &Arc<Self>) -> Option<SlotGuard> {
        let cap = self.cap.load(Ordering::Acquire);
        self.in_flight
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < cap).then_some(n + 1)
            })
            .ok()
            .map(|_| SlotGuard(self.clone()))
    }

    /// A slot, waiting for one to free up until `deadline`.
    pub async fn acquire_until(
        self: &Arc<Self>,
        deadline: tokio::time::Instant,
    ) -> Option<SlotGuard> {
        loop {
            // Registered before the check, so a release in between wakes it.
            let freed = self.freed.notified();
            if let Some(guard) = self.try_acquire() {
                return Some(guard);
            }
            timeout_at(deadline, freed).await.ok()?;
        }
    }

    /// A request failed in transport: the API is down until its next good
    /// probe. Returns true when it was up.
    pub fn mark_down(&self) -> bool {
        std::mem::replace(&mut self.state().up, false)
    }

    /// The API answered 503: it is skipped for [`BUSY_TTL`].
    pub fn mark_busy(&self, now: Instant) {
        self.state().busy_until = Some(now + BUSY_TTL);
    }

    /// Record a good probe. Returns true when the API comes up.
    pub fn probe_ok(state: &mut ApiState) -> bool {
        state.fails = 0;
        let came_up = !state.up;
        state.up = true;
        came_up
    }

    /// Record a failed probe. Returns true when the API goes down.
    pub fn probe_failed(state: &mut ApiState) -> bool {
        state.fails = state.fails.saturating_add(1);
        if state.up && state.fails >= DOWN_AFTER_FAILURES {
            state.up = false;
            return true;
        }
        false
    }
}

/// The upstream chain APIs of one listener.
#[derive(Debug)]
pub struct ApiDirectory {
    slots: Vec<Arc<ApiSlot>>,
    /// `ServerState::index` to a position in `slots`.
    by_index: HashMap<usize, usize>,
    /// The first chain_id any API reported, for listeners with no pinned
    /// chain_id whose SHiP side has not reported one.
    first_chain_id: Mutex<Option<[u8; 32]>>,
}

/// A copy of one API's state, from which a request's [`ApiView`] borrows.
#[derive(Debug, Clone)]
pub struct Snapshot {
    pub index: usize,
    pub endpoint: Arc<str>,
    pub role: Role,
    pub priority: u32,
    pub weight: u32,
    pub up: bool,
    pub busy: bool,
    pub in_flight: usize,
    pub full: bool,
    pub apis: Option<Arc<BTreeSet<String>>>,
    pub head: Option<u32>,
    pub lib: Option<u32>,
    pub earliest: Option<u32>,
    pub verified: bool,
    pub blocks: Option<Range>,
    pub declared: Option<Range>,
}

impl Snapshot {
    pub fn view(&self) -> ApiView<'_> {
        ApiView {
            index: self.index,
            endpoint: &self.endpoint,
            role: self.role,
            priority: self.priority,
            weight: self.weight,
            up: self.up,
            busy: self.busy,
            in_flight: self.in_flight,
            full: self.full,
            apis: self.apis.as_deref(),
            head: self.head,
            lib: self.lib,
            earliest: self.earliest,
            verified: self.verified,
            blocks: self.blocks,
            declared: self.declared,
        }
    }
}

impl ApiDirectory {
    /// One slot per upstream, each with `cap` slots until a frozen API
    /// reports its own limit.
    pub fn new(upstreams: &[ApiUpstream], cap: usize) -> ApiDirectory {
        let slots: Vec<Arc<ApiSlot>> = upstreams
            .iter()
            .map(|u| Arc::new(ApiSlot::new(u, cap)))
            .collect();
        let by_index = slots
            .iter()
            .enumerate()
            .map(|(pos, slot)| (slot.index, pos))
            .collect();
        ApiDirectory {
            slots,
            by_index,
            first_chain_id: Mutex::new(None),
        }
    }

    pub fn slots(&self) -> &[Arc<ApiSlot>] {
        &self.slots
    }

    /// The API of the server with `ServerState::index == index`.
    pub fn slot(&self, index: usize) -> Option<&Arc<ApiSlot>> {
        self.by_index.get(&index).map(|&pos| &self.slots[pos])
    }

    /// Remember `chain_id` if it is the first any API reported; returns the
    /// first one.
    pub fn first_chain_id(&self, chain_id: [u8; 32]) -> [u8; 32] {
        *self
            .first_chain_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_or_insert(chain_id)
    }

    /// Every API as a request sees it now. A live API's head is the highest
    /// of its probed head and the head its SHiP endpoint reported or
    /// streamed, so a block a client got over SHiP is routable at once.
    ///
    /// The control API's changes to the server apply here as they do to
    /// SHiP sessions: the priority and weight in effect, and a server an
    /// operator disabled or drained is not used.
    pub fn snapshot(&self, hub: &FleetHub, now: Instant) -> Vec<Snapshot> {
        let view = hub.view();
        self.slots
            .iter()
            .map(|slot| {
                let upstream = view.up(slot.index);
                let state = slot.state();
                let ship_head = || {
                    let reported = upstream.and_then(|u| u.head).map(|p| p.num);
                    let streamed = hub
                        .tips
                        .get(slot.index)
                        .and_then(|tip| tip.get())
                        .map(|(head, _)| head.num);
                    reported.max(streamed)
                };
                let head = match slot.role {
                    Role::Live => state.head.max(ship_head()),
                    Role::Frozen => None,
                };
                let disabled = upstream.is_some_and(|u| u.operator_disabled);
                Snapshot {
                    index: slot.index,
                    endpoint: slot.endpoint.clone(),
                    role: slot.role,
                    priority: upstream.map_or(slot.priority, |u| u.priority),
                    weight: upstream.map_or(slot.weight, |u| u.weight.max(1)),
                    up: state.up && !state.chain_mismatch && !disabled,
                    busy: state.busy_until.is_some_and(|until| now < until),
                    in_flight: slot.in_flight(),
                    full: slot.is_full(),
                    apis: state.apis.clone(),
                    head,
                    lib: state.lib,
                    earliest: state.earliest,
                    verified: state.verification.verified().is_some(),
                    blocks: state.blocks,
                    declared: slot.declared,
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ApiEndpoint;

    fn pos(num: u32, tag: u8) -> BlockPos {
        BlockPos { num, id: [tag; 32] }
    }

    // CU8
    #[test]
    fn verification_is_sticky_until_the_blocks_differ() {
        let last = pos(1000, 1);
        let mut v = Verification::default();
        // The SHiP side has not reported yet: nothing to check, not verified.
        assert_eq!(v.described(last, true, None), Verify::Keep);
        assert_eq!(v.verified(), None);
        // SHiP reports the same head: ask the API for that block.
        assert_eq!(v.described(last, true, Some(last)), Verify::Check(last));
        assert_eq!(v.checked(last, [1; 32]), Verify::Keep);
        assert_eq!(v.verified(), Some(last));

        // Sticky: SHiP goes away (its last head stays known, or is
        // forgotten), the API flaps, describe repeats itself.
        assert_eq!(v.ship_head(None), Verify::Keep);
        assert_eq!(v.ship_head(Some(last)), Verify::Keep);
        assert_eq!(v.described(last, true, None), Verify::Keep);
        assert_eq!(v.described(last, true, Some(last)), Verify::Keep);
        assert_eq!(v.verified(), Some(last));

        // Cleared by another chain_id.
        assert_eq!(v.described(last, false, Some(last)), Verify::Mismatch);
        assert_eq!(v.verified(), None);

        // Cleared by a `last` that fails the check: SHiP disagrees ...
        let mut v = verified(last);
        assert_eq!(
            v.described(pos(1100, 2), true, Some(last)),
            Verify::Mismatch
        );
        assert_eq!(v.verified(), None);
        // ... or the API's own block has another id.
        let mut v = verified(last);
        let newer = pos(1100, 2);
        assert_eq!(v.described(newer, true, Some(newer)), Verify::Check(newer));
        assert_eq!(v.verified(), Some(last), "kept until the check answers");
        assert_eq!(v.checked(newer, [9; 32]), Verify::Mismatch);
        assert_eq!(v.verified(), None);
        // A new `last` whose check passes moves the verification.
        let mut v = verified(last);
        assert_eq!(v.described(newer, true, Some(newer)), Verify::Check(newer));
        assert_eq!(v.checked(newer, [2; 32]), Verify::Keep);
        assert_eq!(v.verified(), Some(newer));
        // A new `last` while SHiP is unknown keeps the old verification.
        let mut v = verified(last);
        assert_eq!(v.described(newer, true, None), Verify::Keep);
        assert_eq!(v.verified(), Some(last));

        // Cleared by a SHiP head other than the verified block.
        let mut v = verified(last);
        assert_eq!(v.ship_head(Some(pos(1000, 7))), Verify::Mismatch);
        assert_eq!(v.verified(), None);
        let mut v = verified(last);
        assert_eq!(v.ship_head(Some(pos(1001, 1))), Verify::Mismatch);
        // Never verified: a SHiP head that differs from `last` is a mismatch.
        let mut v = Verification::default();
        assert_eq!(v.described(last, true, Some(pos(999, 1))), Verify::Mismatch);
        assert_eq!(v.ship_head(Some(pos(999, 1))), Verify::Keep);
    }

    fn verified(last: BlockPos) -> Verification {
        let mut v = Verification::default();
        assert_eq!(v.described(last, true, Some(last)), Verify::Check(last));
        assert_eq!(v.checked(last, last.id), Verify::Keep);
        v
    }

    fn upstream(index: usize, role: Role) -> ApiUpstream {
        ApiUpstream {
            index,
            name: format!("api-{index}"),
            endpoint: format!("127.0.0.1:{}", 18080 + index),
            role,
            api: ApiEndpoint::parse(&format!("http://Node-{index}.Example:8888")).unwrap(),
            priority: 0,
            weight: 0,
            range: Some(Range {
                begin: 2,
                end: 1001,
            }),
        }
    }

    #[test]
    fn up_after_one_good_probe_down_after_two_failures() {
        let mut state = ApiState::default();
        assert!(!ApiSlot::probe_failed(&mut state));
        assert!(ApiSlot::probe_ok(&mut state), "comes up");
        assert!(!ApiSlot::probe_ok(&mut state));
        assert!(
            !ApiSlot::probe_failed(&mut state),
            "one failure is not enough"
        );
        assert!(state.up);
        assert!(!ApiSlot::probe_ok(&mut state));
        assert!(!ApiSlot::probe_failed(&mut state));
        assert!(ApiSlot::probe_failed(&mut state), "two in a row");
        assert!(!state.up);
        assert!(!ApiSlot::probe_failed(&mut state));
    }

    #[tokio::test]
    async fn slots_follow_the_cap_and_wake_waiters() {
        let dir = ApiDirectory::new(&[upstream(3, Role::Frozen), upstream(5, Role::Live)], 2);
        let slot = dir.slot(3).unwrap().clone();
        assert!(dir.slot(4).is_none());
        assert_eq!(&*slot.endpoint, "node-3.example:8888");
        assert_eq!(slot.authority, "Node-3.Example:8888");
        assert_eq!(slot.base, "http://Node-3.Example:8888");
        assert_eq!(slot.weight, 1, "a zero weight counts as 1");
        assert_eq!(
            slot.declared,
            Some(Range {
                begin: 2,
                end: 1001
            })
        );
        assert_eq!(dir.slot(5).unwrap().declared, None, "live: no range");

        let a = slot.try_acquire().unwrap();
        let b = slot.try_acquire().unwrap();
        assert!(slot.is_full());
        assert!(slot.try_acquire().is_none());
        // A waiter gets the slot a request frees.
        let waiter = {
            let slot = slot.clone();
            tokio::spawn(async move {
                let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
                slot.acquire_until(deadline).await.is_some()
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(a);
        assert!(waiter.await.unwrap());
        // The waiter took a's slot and gave it back.
        assert_eq!(slot.in_flight(), 1);
        // Lowering the cap keeps the requests in flight.
        slot.set_cap(1);
        assert!(slot.is_full(), "b holds the only slot");
        drop(b);
        assert!(!slot.is_full());
        assert_eq!(slot.cap(), 1);
    }

    #[tokio::test]
    async fn a_waiter_gives_up_at_its_deadline() {
        let dir = ApiDirectory::new(&[upstream(0, Role::Frozen)], 1);
        let slot = dir.slot(0).unwrap().clone();
        let _held = slot.try_acquire().unwrap();
        let started = tokio::time::Instant::now();
        let deadline = started + std::time::Duration::from_millis(50);
        assert!(slot.acquire_until(deadline).await.is_none());
        assert!(started.elapsed() >= std::time::Duration::from_millis(50));
        slot.set_cap(2);
        assert!(slot.try_acquire().is_some());
        assert_eq!(dir.first_chain_id([7; 32]), [7; 32]);
        assert_eq!(dir.first_chain_id([8; 32]), [7; 32]);
        slot.mark_busy(Instant::now());
        assert!(slot.state().busy_until.is_some());
        slot.state().up = true;
        assert!(slot.mark_down());
        assert!(!slot.state().up);
        assert!(!slot.mark_down());
    }
}
