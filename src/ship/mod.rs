//! Value types shared by the SHiP codec, the fleet view and the session actor.
//!
//! [`BlockPos`] is a block number paired with its id, the shape SHiP uses for
//! `head`, `last_irreversible`, `this_block`, `prev_block` and
//! `have_positions`. [`Range`] is a half-open block range, the shape of a log's
//! `[begin, end)` in `get_status_result` and of a declared archive range in the
//! config.

// The types are consumed by the codec, the fleet view and the session actor,
// which are being built on top of this module.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

pub mod wire;

/// A block number and its id (`checksum256`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockPos {
    pub num: u32,
    pub id: [u8; 32],
}

/// A half-open block range `[begin, end)`. Empty when `end <= begin`.
///
/// Serialized as `{ "begin": <u32>, "end": <u32> }`, the form used by a
/// server's `range` in the config.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Range {
    pub begin: u32,
    /// Exclusive.
    pub end: u32,
}

impl Range {
    /// `true` when the range holds no block (`end <= begin`).
    pub const fn is_empty(&self) -> bool {
        self.end <= self.begin
    }

    /// `true` when `begin <= n < end`.
    pub const fn contains(&self, n: u32) -> bool {
        self.begin <= n && n < self.end
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_is_half_open() {
        let r = Range { begin: 2, end: 5 };
        assert!(!r.is_empty());
        assert!(!r.contains(1));
        assert!(r.contains(2));
        assert!(r.contains(4));
        assert!(!r.contains(5));
    }

    #[test]
    fn range_is_empty_when_end_not_above_begin() {
        assert!(Range::default().is_empty());
        assert!(Range { begin: 7, end: 7 }.is_empty());
        assert!(Range { begin: 9, end: 3 }.is_empty());
        assert!(!Range { begin: 9, end: 3 }.contains(5));
    }

    #[test]
    fn range_serde_uses_begin_end_keys() {
        let r = Range {
            begin: 2,
            end: 50_000_001,
        };
        let json = serde_json::to_value(r).unwrap();
        assert_eq!(json, serde_json::json!({ "begin": 2, "end": 50_000_001 }));
        let back: Range = serde_json::from_value(json).unwrap();
        assert_eq!(back, r);
    }
}
