//! Client-side SHiP helpers for tests: request builders and result views.
//!
//! Builders produce exactly the bytes a Spring client sends. Views parse the
//! results a SHiP server (or a router in front of one) returns, and reject any
//! frame that is malformed or has bytes left over.

use crate::protocol::{encode_block_position, encode_varuint32, Reader};

/// A `get_blocks_request_v0` (`v1: false`) or `get_blocks_request_v1`.
/// `fetch_finality_data` is written only for v1.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlocksReq {
    pub v1: bool,
    pub start: u32,
    /// Exclusive.
    pub end: u32,
    pub max_in_flight: u32,
    pub have_positions: Vec<(u32, [u8; 32])>,
    pub irreversible_only: bool,
    pub fetch_block: bool,
    pub fetch_traces: bool,
    pub fetch_deltas: bool,
    pub fetch_finality_data: bool,
}

/// `get_status_request_v0`.
pub fn status_v0() -> Vec<u8> {
    vec![0]
}

/// `get_status_request_v1`.
pub fn status_v1() -> Vec<u8> {
    vec![4]
}

/// `get_blocks_request_v0` (index 1) or `get_blocks_request_v1` (index 3).
pub fn blocks(r: &BlocksReq) -> Vec<u8> {
    let mut buf = encode_varuint32(if r.v1 { 3 } else { 1 });
    buf.extend_from_slice(&r.start.to_le_bytes());
    buf.extend_from_slice(&r.end.to_le_bytes());
    buf.extend_from_slice(&r.max_in_flight.to_le_bytes());
    buf.extend_from_slice(&encode_varuint32(r.have_positions.len() as u32));
    for (num, id) in &r.have_positions {
        buf.extend_from_slice(&encode_block_position(*num, id));
    }
    buf.push(u8::from(r.irreversible_only));
    buf.push(u8::from(r.fetch_block));
    buf.push(u8::from(r.fetch_traces));
    buf.push(u8::from(r.fetch_deltas));
    if r.v1 {
        buf.push(u8::from(r.fetch_finality_data));
    }
    buf
}

/// `get_blocks_ack_request_v0`.
pub fn ack(n: u32) -> Vec<u8> {
    let mut buf = vec![2];
    buf.extend_from_slice(&n.to_le_bytes());
    buf
}

/// A parsed `get_status_result_v0` (index 0) or `get_status_result_v1` (index 3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusView {
    pub variant: u8,
    pub head: (u32, [u8; 32]),
    pub lib: (u32, [u8; 32]),
    pub trace: (u32, u32),
    pub chain_state: (u32, u32),
    /// v1 only.
    pub finality: Option<(u32, u32)>,
    /// Always present in v1; a binary extension in v0.
    pub chain_id: Option<[u8; 32]>,
    /// Frame length: 121 for v0 with `chain_id`, 89 without, 129 for v1.
    pub len: usize,
}

impl StatusView {
    /// Parse a status frame. Anything else, a truncated frame or trailing
    /// bytes give `None`.
    pub fn parse(b: &[u8]) -> Option<Self> {
        let mut r = Reader::new(b);
        let variant = r.u8().ok()?;
        if variant != 0 && variant != 3 {
            return None;
        }
        let head = r.position().ok()?;
        let lib = r.position().ok()?;
        let trace = (r.u32().ok()?, r.u32().ok()?);
        let chain_state = (r.u32().ok()?, r.u32().ok()?);
        let chain_id = if variant == 3 || r.remaining() > 0 {
            Some(r.take(32).ok()?.try_into().unwrap())
        } else {
            None
        };
        let finality = if variant == 3 {
            Some((r.u32().ok()?, r.u32().ok()?))
        } else {
            None
        };
        if r.remaining() != 0 {
            return None;
        }
        Some(Self {
            variant,
            head,
            lib,
            trace,
            chain_state,
            finality,
            chain_id,
            len: b.len(),
        })
    }
}

/// A parsed `get_blocks_result_v0` (index 1) or `get_blocks_result_v1` (index 2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockView {
    pub variant: u8,
    pub head: (u32, [u8; 32]),
    pub lib: (u32, [u8; 32]),
    pub this: Option<(u32, [u8; 32])>,
    pub prev: Option<(u32, [u8; 32])>,
    pub block: Option<Vec<u8>>,
    pub traces: Option<Vec<u8>>,
    pub deltas: Option<Vec<u8>>,
    /// v1 only.
    pub finality: Option<Vec<u8>>,
}

impl BlockView {
    /// Parse a blocks result frame (variants 1 and 2). Anything else, a
    /// truncated frame, a presence byte other than 0/1 or trailing bytes give
    /// `None`.
    pub fn parse(b: &[u8]) -> Option<Self> {
        let mut r = Reader::new(b);
        let variant = r.u8().ok()?;
        if variant != 1 && variant != 2 {
            return None;
        }
        let head = r.position().ok()?;
        let lib = r.position().ok()?;
        let this = optional(&mut r, |r| r.position().ok())?;
        let prev = optional(&mut r, |r| r.position().ok())?;
        let block = optional(&mut r, |r| {
            let len = r.varuint32().ok()? as usize;
            Some(r.take(len).ok()?.to_vec())
        })?;
        let log_entry = |r: &mut Reader<'_>| {
            let len = usize::try_from(r.varuint64().ok()?).ok()?;
            Some(r.take(len).ok()?.to_vec())
        };
        let traces = optional(&mut r, log_entry)?;
        let deltas = optional(&mut r, log_entry)?;
        let finality = if variant == 2 {
            optional(&mut r, log_entry)?
        } else {
            None
        };
        if r.remaining() != 0 {
            return None;
        }
        Some(Self {
            variant,
            head,
            lib,
            this,
            prev,
            block,
            traces,
            deltas,
            finality,
        })
    }

    /// `this_block.block_num`, or `None` for an id-less (head-only) result.
    pub fn num(&self) -> Option<u32> {
        self.this.map(|(num, _)| num)
    }
}

/// `optional<T>`: `Some(None)` when absent, `Some(Some(v))` when present,
/// `None` on a bad presence byte or a failed inner read.
fn optional<'a, T>(
    r: &mut Reader<'a>,
    read: impl FnOnce(&mut Reader<'a>) -> Option<T>,
) -> Option<Option<T>> {
    match r.u8().ok()? {
        0 => Some(None),
        1 => read(r).map(Some),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        decode_request_strict, encode_blocks_result, encode_status, fake_block_id, BlockFields,
        ShipRequest, StatusFields,
    };

    #[test]
    fn builders_round_trip_through_the_strict_decoder() {
        let req = BlocksReq {
            v1: true,
            start: 5,
            end: 9,
            max_in_flight: 3,
            have_positions: vec![(4, fake_block_id(4)), (3, fake_block_id(3))],
            irreversible_only: true,
            fetch_block: true,
            fetch_traces: false,
            fetch_deltas: true,
            fetch_finality_data: true,
        };
        assert_eq!(
            decode_request_strict(&blocks(&req)),
            Ok(ShipRequest::GetBlocksV1 {
                start_block_num: 5,
                end_block_num: 9,
                max_messages_in_flight: 3,
                have_positions: req.have_positions.clone(),
                irreversible_only: true,
                fetch_block: true,
                fetch_traces: false,
                fetch_deltas: true,
                fetch_finality_data: true,
            })
        );
        let v0 = blocks(&BlocksReq {
            v1: false,
            ..req.clone()
        });
        assert_eq!(v0[0], 1);
        assert_eq!(v0.len(), blocks(&req).len() - 1);
        assert_eq!(status_v0(), vec![0]);
        assert_eq!(status_v1(), vec![4]);
        assert_eq!(ack(258), vec![2, 2, 1, 0, 0]);
    }

    #[test]
    fn status_view_parses_both_versions() {
        let fields = StatusFields {
            head: (10, fake_block_id(10)),
            lib: (8, fake_block_id(8)),
            trace: (2, 11),
            chain_state: (4, 11),
            chain_id: [7; 32],
            finality: (6, 11),
        };
        let v0 = StatusView::parse(&encode_status(false, &fields)).unwrap();
        assert_eq!((v0.variant, v0.len), (0, 121));
        assert_eq!((v0.head, v0.lib), (fields.head, fields.lib));
        assert_eq!((v0.trace, v0.chain_state), ((2, 11), (4, 11)));
        assert_eq!((v0.chain_id, v0.finality), (Some([7; 32]), None));
        let v1 = StatusView::parse(&encode_status(true, &fields)).unwrap();
        assert_eq!((v1.variant, v1.len, v1.finality), (3, 129, Some((6, 11))));
        // v0 without the chain_id extension.
        let short = &encode_status(false, &fields)[..89];
        assert_eq!(StatusView::parse(short).unwrap().chain_id, None);
        assert!(StatusView::parse(&encode_status(false, &fields)[..100]).is_none());
        assert!(StatusView::parse(&[1; 121]).is_none());
    }

    #[test]
    fn block_view_parses_every_field_and_rejects_leftovers() {
        let fields = BlockFields {
            v1: true,
            head: (10, fake_block_id(10)),
            lib: (8, fake_block_id(8)),
            this_block: Some((9, fake_block_id(9))),
            prev_block: Some((8, fake_block_id(8))),
            block: Some(&[1; 200]),
            traces: None,
            deltas: Some(&[3; 5]),
            finality: Some(&[4; 2]),
        };
        let frame = encode_blocks_result(&fields);
        let view = BlockView::parse(&frame).unwrap();
        assert_eq!(view.variant, 2);
        assert_eq!(view.num(), Some(9));
        assert_eq!(view.prev, fields.prev_block);
        assert_eq!(view.block.as_deref(), Some(&[1u8; 200][..]));
        assert_eq!(view.traces, None);
        assert_eq!(view.deltas.as_deref(), Some(&[3u8; 5][..]));
        assert_eq!(view.finality.as_deref(), Some(&[4u8; 2][..]));

        let mut padded = frame.clone();
        padded.push(0);
        assert!(BlockView::parse(&padded).is_none());
        for cut in 0..frame.len() {
            assert!(BlockView::parse(&frame[..cut]).is_none(), "cut at {cut}");
        }
        let v0 = encode_blocks_result(&BlockFields {
            v1: false,
            ..fields
        });
        let view = BlockView::parse(&v0).unwrap();
        assert_eq!((view.variant, view.finality), (1, None));
    }
}
