//! The chain state one mock server shares across its sessions.
//!
//! Head and LIB live in one atomic so a session never reads a new head with an
//! old LIB. Every change bumps a generation counter and wakes waiting sessions,
//! the way Spring's `block_applied` wakes its session write loops.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use tokio::sync::futures::Notified;
use tokio::sync::Notify;

use crate::protocol::fake_block_id_seeded;

/// Head, LIB and fork history of one mock chain.
#[derive(Debug)]
pub struct ChainClock {
    /// `head << 32 | lib`.
    tip: AtomicU64,
    notify: Notify,
    /// `(from, seed)` in the order they happened: ids for `n >= from` use `seed`
    /// until a later fork overrides them.
    forks: Mutex<Vec<(u32, u8)>>,
    generation: AtomicU64,
}

fn pack(head: u32, lib: u32) -> u64 {
    (u64::from(head) << 32) | u64::from(lib)
}

impl ChainClock {
    pub(crate) fn new(head: u32, lib: u32, fork: Option<(u32, u8)>) -> Self {
        Self {
            tip: AtomicU64::new(pack(head, lib)),
            notify: Notify::new(),
            forks: Mutex::new(fork.into_iter().collect()),
            generation: AtomicU64::new(0),
        }
    }

    /// Current head block number.
    pub fn head(&self) -> u32 {
        (self.tip.load(Ordering::Acquire) >> 32) as u32
    }

    /// Current last irreversible block number.
    pub fn lib(&self) -> u32 {
        self.tip.load(Ordering::Acquire) as u32
    }

    /// The id of block `n` on the current branch.
    pub fn id(&self, n: u32) -> [u8; 32] {
        let seed = self
            .forks()
            .iter()
            .rev()
            .find(|(from, _)| *from <= n)
            .map_or(0, |(_, seed)| *seed);
        fake_block_id_seeded(n, seed)
    }

    pub(crate) fn set(&self, head: u32, lib: u32) {
        self.tip.store(pack(head, lib), Ordering::Release);
        self.changed();
    }

    pub(crate) fn advance(&self, n: u32) {
        let _ = self
            .tip
            .try_update(Ordering::AcqRel, Ordering::Acquire, |tip| {
                let head = (tip >> 32) as u32;
                let lib = tip as u32;
                Some(pack(head.saturating_add(n), lib.saturating_add(n)))
            });
        self.changed();
    }

    pub(crate) fn fork(&self, from: u32, seed: u8) {
        self.forks().push((from, seed));
        self.changed();
    }

    /// Bumped on every change to head, LIB or the fork history.
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// Completes on the next change. Create it before reading
    /// [`generation`](Self::generation): `notify_waiters` reaches a `Notified`
    /// as soon as it exists, so a change in between is not missed.
    pub(crate) fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }

    /// Fork points recorded at or after index `seen`, and the new count.
    pub(crate) fn forks_since(&self, seen: usize) -> (Vec<u32>, usize) {
        let forks = self.forks();
        let points = forks.iter().skip(seen).map(|(from, _)| *from).collect();
        (points, forks.len())
    }

    fn forks(&self) -> std::sync::MutexGuard<'_, Vec<(u32, u8)>> {
        self.forks.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn changed(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::fake_block_id;

    #[test]
    fn later_forks_override_ids_from_their_start() {
        let clock = ChainClock::new(300, 290, Some((100, 1)));
        assert_eq!(clock.id(99), fake_block_id(99));
        assert_eq!(clock.id(150), fake_block_id_seeded(150, 1));
        clock.fork(200, 2);
        assert_eq!(clock.id(150), fake_block_id_seeded(150, 1));
        assert_eq!(clock.id(250), fake_block_id_seeded(250, 2));
        clock.fork(50, 3);
        assert_eq!(clock.id(49), fake_block_id(49));
        assert_eq!(clock.id(250), fake_block_id_seeded(250, 3));
        assert_eq!(clock.forks_since(1), (vec![200, 50], 3));
    }

    #[test]
    fn advance_moves_head_and_lib_together() {
        let clock = ChainClock::new(1000, 990, None);
        let before = clock.generation();
        clock.advance(5);
        assert_eq!((clock.head(), clock.lib()), (1005, 995));
        clock.set(u32::MAX - 1, 7);
        clock.advance(5);
        assert_eq!((clock.head(), clock.lib()), (u32::MAX, 12));
        assert_eq!(clock.generation(), before + 3);
    }
}
