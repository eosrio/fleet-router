//! SHiP wire codec: decoding client requests with fc semantics, encoding
//! upstream requests and router status replies, classifying upstream results
//! and patching their head/LIB in place.
//!
//! The layouts are the `request` and `result` variants of the SHiP ABI a node
//! serves. Every variant index is below 128, so a node always writes it as one
//! byte. `head` and `last_irreversible` are then the first two fields of both
//! `get_blocks_result_v0/v1` and `get_status_result_v0/v1`, at the fixed
//! offsets `[1..37)` and `[37..73)`.
//!
//! Client requests are decoded the way the node's `fc::raw::unpack` reads
//! them:
//! - the variant index and the `have_positions` count are varuint32, and
//!   non-canonical encodings (`[0x81, 0x00]` for 1) are accepted;
//! - a bool must be 0 or 1;
//! - bytes after the last field are ignored;
//! - `have_positions` may hold at most [`MAX_POSITIONS`] entries.
//!
//! fc stops reading a varuint32 after five bytes and drops any bits above 32.
//! This codec rejects such an encoding instead, so the router never acts on a
//! request whose meaning depends on that truncation.
//!
//! Every decoder is bounds-checked and returns an error on short or hostile
//! input. None of them panics or allocates more than the input can back.

use std::fmt::Display;

use bytes::{Bytes, BytesMut};

pub use crate::ship::BlockPos;
use crate::ship::Range;

// `state_request` variant indices.
const REQ_STATUS_V0: u32 = 0;
const REQ_BLOCKS_V0: u32 = 1;
const REQ_ACK: u32 = 2;
const REQ_BLOCKS_V1: u32 = 3;
const REQ_STATUS_V1: u32 = 4;

// `state_result` variant indices, as the single byte a node writes.
const RES_STATUS_V0: u8 = 0;
const RES_BLOCKS_V0: u8 = 1;
const RES_BLOCKS_V1: u8 = 2;
const RES_STATUS_V1: u8 = 3;

/// An encoded `block_position`: `uint32 block_num` then `checksum256 block_id`.
const POS_LEN: usize = 36;

/// One past the LIB position of a result frame: the index byte plus two
/// `block_position`s.
const HEAD_LIB_END: usize = 1 + 2 * POS_LEN;

/// A `get_status_result_v0` message, `chain_id` included.
pub const STATUS_V0_LEN: usize = 121;

/// A `get_status_result_v0` message without its `chain_id` binary extension.
const STATUS_V0_MIN_LEN: usize = STATUS_V0_LEN - 32;

/// A `get_status_result_v1` message.
pub const STATUS_V1_LEN: usize = 129;

/// fc's `MAX_NUM_ARRAY_ELEMENTS`. A node rejects a request whose
/// `have_positions` is longer.
pub const MAX_POSITIONS: u32 = 1024 * 1024;

/// A `get_blocks_request_v0` (`v1 == false`) or `get_blocks_request_v1`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlocksRequest {
    pub v1: bool,
    pub start: u32,
    /// Exclusive.
    pub end: u32,
    pub max_in_flight: u32,
    pub have_positions: Vec<BlockPos>,
    pub irreversible_only: bool,
    pub fetch_block: bool,
    pub fetch_traces: bool,
    pub fetch_deltas: bool,
    /// Only exists in v1. It is `false` for a decoded v0 request and is not
    /// written when a v0 request is encoded.
    pub fetch_finality_data: bool,
}

/// A decoded client `state_request`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientMsg {
    /// `get_status_request_v0` (index 0) or `get_status_request_v1` (index 4).
    Status { v1: bool },
    /// `get_blocks_request_v0` (index 1) or `get_blocks_request_v1` (index 3).
    Blocks(BlocksRequest),
    /// `get_blocks_ack_request_v0` (index 2) with its `num_messages`.
    Ack(u32),
}

/// Why a client request or a result's payload fields could not be decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// The message has no bytes.
    Empty,
    /// A varuint is longer than its width allows, or carries bits above it.
    BadVarint,
    /// The message ends before a field, or a declared length, is complete.
    Truncated,
    /// A bool or an optional's presence byte is neither 0 nor 1.
    BadBool(u8),
    /// A request variant index past `get_status_request_v1`.
    UnknownVariant(u32),
    /// `have_positions` declares more than [`MAX_POSITIONS`] entries.
    TooManyPositions(u32),
}

impl Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Empty => write!(f, "empty SHiP message"),
            DecodeError::BadVarint => write!(f, "invalid varuint in SHiP message"),
            DecodeError::Truncated => write!(f, "truncated SHiP message"),
            DecodeError::BadBool(b) => write!(f, "invalid bool byte {b} in SHiP message"),
            DecodeError::UnknownVariant(i) => write!(f, "unknown SHiP request variant {i}"),
            DecodeError::TooManyPositions(n) => write!(
                f,
                "have_positions declares {n} entries, more than the {MAX_POSITIONS} a node accepts"
            ),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Decode a client `state_request` with fc semantics.
///
/// The variant index is a varuint32. Non-canonical encodings are accepted;
/// more than five bytes, or bits above 32, give [`DecodeError::BadVarint`].
/// Bools must be 0 or 1, and trailing bytes are ignored. The
/// `have_positions` count is checked against [`MAX_POSITIONS`] and against
/// the bytes left in the message before anything is allocated.
pub fn decode_client(b: &[u8]) -> Result<ClientMsg, DecodeError> {
    if b.is_empty() {
        return Err(DecodeError::Empty);
    }
    let mut r = Reader::new(b, 0);
    match r.varuint32()? {
        REQ_STATUS_V0 => Ok(ClientMsg::Status { v1: false }),
        REQ_BLOCKS_V0 => decode_blocks_request(&mut r, false).map(ClientMsg::Blocks),
        REQ_ACK => Ok(ClientMsg::Ack(r.u32()?)),
        REQ_BLOCKS_V1 => decode_blocks_request(&mut r, true).map(ClientMsg::Blocks),
        REQ_STATUS_V1 => Ok(ClientMsg::Status { v1: true }),
        other => Err(DecodeError::UnknownVariant(other)),
    }
}

/// The body of a `get_blocks_request_v0/v1`, after its variant index.
fn decode_blocks_request(r: &mut Reader<'_>, v1: bool) -> Result<BlocksRequest, DecodeError> {
    let start = r.u32()?;
    let end = r.u32()?;
    let max_in_flight = r.u32()?;

    let count = r.varuint32()?;
    if count > MAX_POSITIONS {
        return Err(DecodeError::TooManyPositions(count));
    }
    // Bound the allocation by what the message can actually hold.
    let count = count as usize;
    if r.remaining() / POS_LEN < count {
        return Err(DecodeError::Truncated);
    }
    let mut have_positions = Vec::with_capacity(count);
    for _ in 0..count {
        have_positions.push(r.block_pos()?);
    }

    let irreversible_only = r.bool()?;
    let fetch_block = r.bool()?;
    let fetch_traces = r.bool()?;
    let fetch_deltas = r.bool()?;
    let fetch_finality_data = if v1 { r.bool()? } else { false };

    Ok(BlocksRequest {
        v1,
        start,
        end,
        max_in_flight,
        have_positions,
        irreversible_only,
        fetch_block,
        fetch_traces,
        fetch_deltas,
        fetch_finality_data,
    })
}

/// Encode `r` as `get_blocks_request_v0` (index 1) or, when `r.v1`,
/// `get_blocks_request_v1` (index 3). A v0 request does not carry
/// `fetch_finality_data`.
pub fn encode_blocks_request(r: &BlocksRequest) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + 12 + 5 + POS_LEN * r.have_positions.len() + 5);
    put_varuint32(&mut out, if r.v1 { REQ_BLOCKS_V1 } else { REQ_BLOCKS_V0 });
    put_u32(&mut out, r.start);
    put_u32(&mut out, r.end);
    put_u32(&mut out, r.max_in_flight);
    // Positions come from a decoded request (at most MAX_POSITIONS) or from
    // the router's own bounded ring, so the count always fits in a u32.
    put_varuint32(&mut out, r.have_positions.len() as u32);
    for p in &r.have_positions {
        put_pos(&mut out, p);
    }
    out.push(u8::from(r.irreversible_only));
    out.push(u8::from(r.fetch_block));
    out.push(u8::from(r.fetch_traces));
    out.push(u8::from(r.fetch_deltas));
    if r.v1 {
        out.push(u8::from(r.fetch_finality_data));
    }
    out
}

/// Encode `get_blocks_ack_request_v0`: `[2, n as u32 LE]`.
pub fn encode_ack(n: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(5);
    put_varuint32(&mut out, REQ_ACK);
    put_u32(&mut out, n);
    out
}

/// Encode `get_status_request_v0` (`[0]`) or `get_status_request_v1` (`[4]`).
pub fn encode_status_request(v1: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(1);
    put_varuint32(&mut out, if v1 { REQ_STATUS_V1 } else { REQ_STATUS_V0 });
    out
}

/// The fixed part of a `get_blocks_result_v0/v1`, through `prev_block`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ResultHead {
    /// Result index 2, `get_blocks_result_v1`, which ends with `finality_data`.
    pub v1: bool,
    pub head: BlockPos,
    pub lib: BlockPos,
    pub this_block: Option<BlockPos>,
    pub prev_block: Option<BlockPos>,
    /// Offset of the `block` presence byte, where [`result_presence`] starts.
    pub fields_at: usize,
}

/// What an upstream binary message is, as far as the router needs to know.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpstreamMsg {
    /// `get_status_result_v0` (index 0, at least 89 bytes, since `chain_id`
    /// is a binary extension) or `get_status_result_v1` (index 3, at least
    /// 129 bytes).
    Status { v1: bool },
    /// `get_blocks_result_v0` (index 1) or `get_blocks_result_v1` (index 2).
    Blocks(ResultHead),
    /// Any other index, a short status, a block result whose head does not
    /// decode, or an index written in more than one byte.
    Other,
}

/// Classify an upstream binary message by its result index.
///
/// The index is read from the first byte only: a node writes every result
/// index in one byte, and the fixed head/LIB offsets assume it. A block result
/// is decoded through `prev_block`, so it must hold both positions and both
/// presence bytes (at least 75 bytes). Its payload fields are walked only on
/// demand, by [`result_presence`].
pub fn classify_upstream(b: &[u8]) -> UpstreamMsg {
    match b.first() {
        Some(&RES_STATUS_V0) if b.len() >= STATUS_V0_MIN_LEN => UpstreamMsg::Status { v1: false },
        Some(&RES_STATUS_V1) if b.len() >= STATUS_V1_LEN => UpstreamMsg::Status { v1: true },
        Some(&(RES_BLOCKS_V0 | RES_BLOCKS_V1)) => {
            decode_result_head(b).map_or(UpstreamMsg::Other, UpstreamMsg::Blocks)
        }
        _ => UpstreamMsg::Other,
    }
}

fn decode_result_head(b: &[u8]) -> Result<ResultHead, DecodeError> {
    let mut r = Reader::new(b, 0);
    let v1 = r.u8()? == RES_BLOCKS_V1;
    let head = r.block_pos()?;
    let lib = r.block_pos()?;
    let this_block = r.optional_pos()?;
    let prev_block = r.optional_pos()?;
    Ok(ResultHead {
        v1,
        head,
        lib,
        this_block,
        prev_block,
        fields_at: r.pos,
    })
}

/// Which optional payload fields a block result carries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Presence {
    pub block: bool,
    pub traces: bool,
    pub deltas: bool,
    /// Always `false` for a v0 result, which has no `finality_data`.
    pub finality: bool,
}

/// Walk the `bytes?` fields of a block result from `h.fields_at`.
///
/// `block` has a varuint32 length, because the node packs it with fc.
/// `traces`, `deltas` and (v1 only) `finality_data` have varuint64 lengths,
/// because the node writes them from its logs. A present field counts as
/// present even when its length is 0. Each declared length must fit in the
/// message; trailing bytes are ignored.
pub fn result_presence(b: &[u8], h: &ResultHead) -> Result<Presence, DecodeError> {
    let mut r = Reader::new(b, h.fields_at);
    let block = r.optional_bytes(LenWidth::U32)?;
    let traces = r.optional_bytes(LenWidth::U64)?;
    let deltas = r.optional_bytes(LenWidth::U64)?;
    let finality = if h.v1 {
        r.optional_bytes(LenWidth::U64)?
    } else {
        false
    };
    Ok(Presence {
        block,
        traces,
        deltas,
        finality,
    })
}

/// The fields of a router-built `get_status_result_v0/v1`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StatusBody {
    pub head: BlockPos,
    pub lib: BlockPos,
    pub trace: Range,
    pub chain_state: Range,
    /// Only written to v1.
    pub finality: Range,
    pub chain_id: [u8; 32],
}

/// Encode a status result. v0 is index 0 and 121 bytes, always with
/// `chain_id`. v1 is index 3 and 129 bytes, with the finality range last.
pub fn encode_status(v1: bool, s: &StatusBody) -> Vec<u8> {
    let mut out = Vec::with_capacity(if v1 { STATUS_V1_LEN } else { STATUS_V0_LEN });
    out.push(if v1 { RES_STATUS_V1 } else { RES_STATUS_V0 });
    put_pos(&mut out, &s.head);
    put_pos(&mut out, &s.lib);
    put_u32(&mut out, s.trace.begin);
    put_u32(&mut out, s.trace.end);
    put_u32(&mut out, s.chain_state.begin);
    put_u32(&mut out, s.chain_state.end);
    out.extend_from_slice(&s.chain_id);
    if v1 {
        put_u32(&mut out, s.finality.begin);
        put_u32(&mut out, s.finality.end);
    }
    out
}

/// Read head (`[1..37)`) and LIB (`[37..73)`) from a block or status result.
/// `None` when the frame is shorter than 73 bytes. The index byte is not
/// checked; classify the frame first.
pub fn read_head_lib(frame: &[u8]) -> Option<(BlockPos, BlockPos)> {
    let mut r = Reader::new(frame.get(..HEAD_LIB_END)?, 1);
    Some((r.block_pos().ok()?, r.block_pos().ok()?))
}

/// Writes head at [1..37) and LIB at [37..73). Zero-copy when the Bytes is
/// unique (`try_into_mut`), else one copy.
///
/// Valid for get_blocks_result_v0/v1 and get_status_result_v0/v1 (identical
/// offsets; variant indices < 128). A frame shorter than 73 bytes cannot hold
/// both positions and is returned unchanged.
pub fn patch_head_lib(frame: Bytes, head: BlockPos, lib: BlockPos) -> Bytes {
    if frame.len() < HEAD_LIB_END {
        return frame;
    }
    // A fragmented message is reassembled into its own Vec, so it is unique
    // and is patched in place. A single-frame payload still shares the read
    // buffer, and only then is it copied.
    let mut buf = frame
        .try_into_mut()
        .unwrap_or_else(|shared| BytesMut::from(&shared[..]));
    write_pos(&mut buf[1..1 + POS_LEN], &head);
    write_pos(&mut buf[1 + POS_LEN..HEAD_LIB_END], &lib);
    buf.freeze()
}

/// The block result `frame` with its absent `prev_block` set to `prev`, and
/// the head of the new message.
///
/// Only the `prev_block` optional changes: its presence byte becomes 1 and
/// the position follows it; every other byte is kept. The message is copied
/// once. `None` when the result has no `this_block` (Spring never sends a
/// `prev_block` without one) or already has a `prev_block`.
pub fn insert_prev_block(
    frame: &[u8],
    h: &ResultHead,
    prev: BlockPos,
) -> Option<(Bytes, ResultHead)> {
    if h.this_block.is_none() || h.prev_block.is_some() {
        return None;
    }
    let flag = h.fields_at.checked_sub(1)?;
    if frame.get(flag) != Some(&0) {
        return None;
    }
    let mut out = BytesMut::with_capacity(frame.len() + POS_LEN);
    out.extend_from_slice(&frame[..flag]);
    out.extend_from_slice(&[1]);
    let mut slot = [0u8; POS_LEN];
    write_pos(&mut slot, &prev);
    out.extend_from_slice(&slot);
    out.extend_from_slice(&frame[h.fields_at..]);
    let head = ResultHead {
        prev_block: Some(prev),
        fields_at: h.fields_at + POS_LEN,
        ..*h
    };
    Some((out.freeze(), head))
}

/// Width of a `bytes?` length prefix.
#[derive(Clone, Copy)]
enum LenWidth {
    U32,
    U64,
}

/// Bounds-checked cursor over an untrusted message. A read past the end is
/// [`DecodeError::Truncated`], never a panic.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8], pos: usize) -> Self {
        Reader { buf, pos }
    }

    fn remaining(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        let end = self.pos.checked_add(n).ok_or(DecodeError::Truncated)?;
        let bytes = self.buf.get(self.pos..end).ok_or(DecodeError::Truncated)?;
        self.pos = end;
        Ok(bytes)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        self.take(N)?.try_into().map_err(|_| DecodeError::Truncated)
    }

    fn u8(&mut self) -> Result<u8, DecodeError> {
        let [byte] = self.array()?;
        Ok(byte)
    }

    fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    /// fc bool, also used for an optional's presence byte.
    fn bool(&mut self) -> Result<bool, DecodeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(DecodeError::BadBool(other)),
        }
    }

    fn block_pos(&mut self) -> Result<BlockPos, DecodeError> {
        Ok(BlockPos {
            num: self.u32()?,
            id: self.array()?,
        })
    }

    fn optional_pos(&mut self) -> Result<Option<BlockPos>, DecodeError> {
        if self.bool()? {
            self.block_pos().map(Some)
        } else {
            Ok(None)
        }
    }

    /// LEB128 in at most five bytes. The fifth byte may only carry the top
    /// four bits of a u32, and no continuation bit.
    fn varuint32(&mut self) -> Result<u32, DecodeError> {
        let mut value = 0u32;
        for shift in (0..28).step_by(7) {
            let byte = self.u8()?;
            value |= u32::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        let last = self.u8()?;
        if last > 0x0f {
            return Err(DecodeError::BadVarint);
        }
        Ok(value | (u32::from(last) << 28))
    }

    /// LEB128 in at most ten bytes. The tenth byte may only carry bit 63.
    fn varuint64(&mut self) -> Result<u64, DecodeError> {
        let mut value = 0u64;
        for shift in (0..63).step_by(7) {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        let last = self.u8()?;
        if last > 0x01 {
            return Err(DecodeError::BadVarint);
        }
        Ok(value | (u64::from(last) << 63))
    }

    /// `bytes?`: a presence byte, then a length and that many bytes, which
    /// are skipped. Returns whether the field is present.
    fn optional_bytes(&mut self, width: LenWidth) -> Result<bool, DecodeError> {
        if !self.bool()? {
            return Ok(false);
        }
        let len = match width {
            LenWidth::U32 => u64::from(self.varuint32()?),
            LenWidth::U64 => self.varuint64()?,
        };
        let len = usize::try_from(len).map_err(|_| DecodeError::Truncated)?;
        self.take(len)?;
        Ok(true)
    }
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_pos(out: &mut Vec<u8>, p: &BlockPos) {
    put_u32(out, p.num);
    out.extend_from_slice(&p.id);
}

/// Canonical LEB128, as fc packs an `unsigned_int`.
fn put_varuint32(out: &mut Vec<u8>, mut value: u32) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

/// Write `p` into a 36-byte slot.
fn write_pos(slot: &mut [u8], p: &BlockPos) {
    slot[..4].copy_from_slice(&p.num.to_le_bytes());
    slot[4..POS_LEN].copy_from_slice(&p.id);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::zcd::{self, ZCDValues};

    const SHIP_ABI_JSON: &str = include_str!("../../mock-ship/src/ship_abi.json");

    /// Deterministic id whose first four bytes are `num`, like a real block id.
    fn id_of(num: u32) -> [u8; 32] {
        let mut id = [0u8; 32];
        id[..4].copy_from_slice(&num.to_le_bytes());
        for (i, byte) in id.iter_mut().enumerate().skip(4) {
            *byte = (num.wrapping_mul(31).wrapping_add(i as u32) & 0xff) as u8;
        }
        id
    }

    fn pos(num: u32) -> BlockPos {
        BlockPos {
            num,
            id: id_of(num),
        }
    }

    fn sample_request(v1: bool, positions: u32) -> BlocksRequest {
        BlocksRequest {
            v1,
            start: 100,
            end: 0xffff_ffff,
            max_in_flight: 50,
            have_positions: (0..positions).map(|i| pos(90 + i)).collect(),
            irreversible_only: false,
            fetch_block: true,
            fetch_traces: false,
            fetch_deltas: true,
            fetch_finality_data: v1,
        }
    }

    /// Request bytes assembled field by field in the node's reflection order,
    /// independently of [`encode_blocks_request`]. `count` is the encoded
    /// `have_positions` count, so a test can write it non-canonically.
    fn request_bytes(index: &[u8], r: &BlocksRequest, count: &[u8], flags: &[u8]) -> Vec<u8> {
        let mut b = index.to_vec();
        b.extend_from_slice(&r.start.to_le_bytes());
        b.extend_from_slice(&r.end.to_le_bytes());
        b.extend_from_slice(&r.max_in_flight.to_le_bytes());
        b.extend_from_slice(count);
        for p in &r.have_positions {
            b.extend_from_slice(&p.num.to_le_bytes());
            b.extend_from_slice(&p.id);
        }
        b.extend_from_slice(flags);
        b
    }

    fn canonical(value: u32) -> Vec<u8> {
        let mut out = Vec::new();
        put_varuint32(&mut out, value);
        out
    }

    /// A length prefix written in exactly five LEB128 bytes (non-canonical
    /// for values below 2^28).
    fn five_byte_len(len: usize) -> Vec<u8> {
        let len = len as u64;
        let mut out: Vec<u8> = (0..4)
            .map(|i| 0x80 | ((len >> (7 * i)) & 0x7f) as u8)
            .collect();
        out.push(((len >> 28) & 0x0f) as u8);
        out
    }

    /// One `bytes?` field of a block result.
    #[derive(Clone, Copy)]
    enum Field<'a> {
        Absent,
        /// Payload with a canonical length prefix.
        Present(&'a [u8]),
        /// Payload with a five-byte length prefix.
        Wide(&'a [u8]),
    }

    fn push_field(out: &mut Vec<u8>, field: Field<'_>) {
        match field {
            Field::Absent => out.push(0),
            Field::Present(data) => {
                out.push(1);
                out.extend_from_slice(&canonical(data.len() as u32));
                out.extend_from_slice(data);
            }
            Field::Wide(data) => {
                out.push(1);
                out.extend_from_slice(&five_byte_len(data.len()));
                out.extend_from_slice(data);
            }
        }
    }

    struct ResultSpec<'a> {
        v1: bool,
        head: BlockPos,
        lib: BlockPos,
        this_block: Option<BlockPos>,
        prev_block: Option<BlockPos>,
        block: Field<'a>,
        traces: Field<'a>,
        deltas: Field<'a>,
        finality: Field<'a>,
    }

    impl Default for ResultSpec<'_> {
        fn default() -> Self {
            ResultSpec {
                v1: false,
                head: pos(1000),
                lib: pos(990),
                this_block: Some(pos(500)),
                prev_block: Some(pos(499)),
                block: Field::Absent,
                traces: Field::Absent,
                deltas: Field::Absent,
                finality: Field::Absent,
            }
        }
    }

    /// A `get_blocks_result_v0/v1` laid out the way a node writes it.
    fn result_frame(s: &ResultSpec<'_>) -> Vec<u8> {
        let mut out = vec![if s.v1 { RES_BLOCKS_V1 } else { RES_BLOCKS_V0 }];
        put_pos(&mut out, &s.head);
        put_pos(&mut out, &s.lib);
        for p in [s.this_block, s.prev_block] {
            match p {
                Some(p) => {
                    out.push(1);
                    put_pos(&mut out, &p);
                }
                None => out.push(0),
            }
        }
        push_field(&mut out, s.block);
        push_field(&mut out, s.traces);
        push_field(&mut out, s.deltas);
        if s.v1 {
            push_field(&mut out, s.finality);
        }
        out
    }

    fn blocks_head(frame: &[u8]) -> ResultHead {
        match classify_upstream(frame) {
            UpstreamMsg::Blocks(h) => h,
            other => panic!("expected a block result, got {other:?}"),
        }
    }

    fn sample_status() -> StatusBody {
        StatusBody {
            head: pos(2300),
            lib: pos(2290),
            trace: Range {
                begin: 2,
                end: 2301,
            },
            chain_state: Range {
                begin: 5,
                end: 2301,
            },
            finality: Range {
                begin: 950,
                end: 2301,
            },
            chain_id: [0xAB; 32],
        }
    }

    fn zcd_u32(parsed: &zcd::ZCD<'_>, field: &str) -> u32 {
        match parsed.get(field).expect("field width") {
            Some(ZCDValues::U32(value)) => value,
            other => panic!("expected u32 {field}, got {other:?}"),
        }
    }

    fn zcd_id(parsed: &zcd::ZCD<'_>, field: &str) -> [u8; 32] {
        match parsed.get(field).expect("field width") {
            Some(ZCDValues::Checksum256(id)) => id,
            other => panic!("expected checksum256 {field}, got {other:?}"),
        }
    }

    // ----- decode_client -----

    #[test]
    fn decode_client_handles_every_request_variant() {
        assert_eq!(decode_client(&[0]), Ok(ClientMsg::Status { v1: false }));
        assert_eq!(decode_client(&[4]), Ok(ClientMsg::Status { v1: true }));
        assert_eq!(
            decode_client(&[2, 0x78, 0x56, 0x34, 0x12]),
            Ok(ClientMsg::Ack(0x1234_5678))
        );

        let v0 = sample_request(false, 2);
        let bytes = request_bytes(&[1], &v0, &[2], &[0, 1, 0, 1]);
        assert_eq!(decode_client(&bytes), Ok(ClientMsg::Blocks(v0)));

        let v1 = sample_request(true, 2);
        let bytes = request_bytes(&[3], &v1, &[2], &[0, 1, 0, 1, 1]);
        assert_eq!(decode_client(&bytes), Ok(ClientMsg::Blocks(v1)));
    }

    #[test]
    fn decode_client_reads_v0_and_v1_flags_in_order() {
        // Each flag set alone, so a shifted read would land on the wrong field.
        for bit in 0..5 {
            let flags: Vec<u8> = (0..5).map(|i| u8::from(i == bit)).collect();
            let bytes = request_bytes(&[3], &sample_request(true, 0), &[0], &flags);
            let Ok(ClientMsg::Blocks(got)) = decode_client(&bytes) else {
                panic!("v1 request with flag {bit}");
            };
            assert_eq!(
                [
                    got.irreversible_only,
                    got.fetch_block,
                    got.fetch_traces,
                    got.fetch_deltas,
                    got.fetch_finality_data,
                ],
                [bit == 0, bit == 1, bit == 2, bit == 3, bit == 4]
            );
        }

        // v0 has no fetch_finality_data: a fifth byte is trailing and ignored.
        let r = sample_request(false, 0);
        let bytes = request_bytes(&[1], &r, &[0], &[1, 1, 1, 1, 1]);
        let Ok(ClientMsg::Blocks(got)) = decode_client(&bytes) else {
            panic!("v0 request");
        };
        assert!(!got.v1);
        assert!(got.irreversible_only && got.fetch_deltas);
        assert!(!got.fetch_finality_data);
    }

    #[test]
    fn decode_client_accepts_position_counts_across_varint_widths() {
        for count in [0u32, 1, 127, 128, 2000] {
            for v1 in [false, true] {
                let r = sample_request(v1, count);
                let flags: &[u8] = if v1 { &[1, 1, 1, 1, 0] } else { &[1, 1, 1, 1] };
                let index: &[u8] = if v1 { &[3] } else { &[1] };
                let bytes = request_bytes(index, &r, &canonical(count), flags);
                let Ok(ClientMsg::Blocks(got)) = decode_client(&bytes) else {
                    panic!("{count} positions, v1={v1}");
                };
                assert_eq!(got.have_positions.len(), count as usize);
                assert_eq!(got.have_positions, r.have_positions);
                assert!(got.irreversible_only && got.fetch_deltas);
            }
        }
    }

    #[test]
    fn decode_client_accepts_non_canonical_varints() {
        // [0x81, 0x00] is 1: get_blocks_request_v0.
        let r = sample_request(false, 1);
        let mut bytes = vec![0x81, 0x00];
        bytes.extend_from_slice(&request_bytes(&[], &r, &[1], &[0, 1, 0, 1]));
        assert_eq!(decode_client(&bytes), Ok(ClientMsg::Blocks(r.clone())));

        // A five-byte encoding of 0 is get_status_request_v0.
        assert_eq!(
            decode_client(&[0x80, 0x80, 0x80, 0x80, 0x00]),
            Ok(ClientMsg::Status { v1: false })
        );

        // The position count may be non-canonical too.
        let bytes = request_bytes(&[1], &r, &[0x81, 0x80, 0x00], &[0, 1, 0, 1]);
        assert_eq!(decode_client(&bytes), Ok(ClientMsg::Blocks(r)));

        // The largest five-byte value is u32::MAX: an unknown variant, not an error.
        assert_eq!(
            decode_client(&[0xff, 0xff, 0xff, 0xff, 0x0f]),
            Err(DecodeError::UnknownVariant(u32::MAX))
        );
    }

    #[test]
    fn decode_client_rejects_overlong_varints() {
        // Six bytes.
        assert_eq!(
            decode_client(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x00]),
            Err(DecodeError::BadVarint)
        );
        // Five bytes with bits above 32.
        assert_eq!(
            decode_client(&[0x80, 0x80, 0x80, 0x80, 0x10]),
            Err(DecodeError::BadVarint)
        );
        // Same rules for the position count.
        let r = sample_request(false, 0);
        let bytes = request_bytes(&[1], &r, &[0x80, 0x80, 0x80, 0x80, 0x80, 0x00], &[0; 4]);
        assert_eq!(decode_client(&bytes), Err(DecodeError::BadVarint));
    }

    #[test]
    fn decode_client_rejects_bools_other_than_zero_or_one() {
        let r = sample_request(true, 1);
        for at in 0..5 {
            let mut flags = [0u8; 5];
            flags[at] = 2;
            let bytes = request_bytes(&[3], &r, &[1], &flags);
            assert_eq!(
                decode_client(&bytes),
                Err(DecodeError::BadBool(2)),
                "flag {at}"
            );
        }
        let bytes = request_bytes(&[1], &sample_request(false, 0), &[0], &[0, 0, 0xff, 0]);
        assert_eq!(decode_client(&bytes), Err(DecodeError::BadBool(0xff)));
    }

    #[test]
    fn decode_client_errors_on_every_truncation_without_panicking() {
        let r = sample_request(true, 3);
        let bytes = encode_blocks_request(&r);
        assert_eq!(decode_client(&bytes), Ok(ClientMsg::Blocks(r)));
        assert_eq!(decode_client(&[]), Err(DecodeError::Empty));
        for cut in 1..bytes.len() {
            assert_eq!(
                decode_client(&bytes[..cut]),
                Err(DecodeError::Truncated),
                "cut at {cut} of {}",
                bytes.len()
            );
        }

        // An ack needs all four bytes of num_messages.
        for cut in 1..5 {
            assert_eq!(
                decode_client(&encode_ack(7)[..cut]),
                Err(DecodeError::Truncated)
            );
        }
        // A varint index that never ends.
        assert_eq!(decode_client(&[0x80, 0x80]), Err(DecodeError::Truncated));
    }

    #[test]
    fn decode_client_ignores_trailing_bytes() {
        let mut status = vec![0u8];
        status.resize(1024, 0);
        assert_eq!(decode_client(&status), Ok(ClientMsg::Status { v1: false }));

        let mut ack = encode_ack(3);
        ack.extend_from_slice(&[0xff; 9]);
        assert_eq!(decode_client(&ack), Ok(ClientMsg::Ack(3)));

        let r = sample_request(true, 2);
        let mut bytes = encode_blocks_request(&r);
        bytes.extend_from_slice(&[0xee; 40]);
        assert_eq!(decode_client(&bytes), Ok(ClientMsg::Blocks(r)));
    }

    #[test]
    fn decode_client_reports_unknown_variants() {
        assert_eq!(decode_client(&[5]), Err(DecodeError::UnknownVariant(5)));
        assert_eq!(
            decode_client(&[9, 1, 2]),
            Err(DecodeError::UnknownVariant(9))
        );
        // 300 needs two bytes.
        assert_eq!(
            decode_client(&[0xac, 0x02]),
            Err(DecodeError::UnknownVariant(300))
        );
    }

    #[test]
    fn decode_client_bounds_position_counts_before_allocating() {
        // A count far above what the message holds is Truncated, not an allocation.
        let r = sample_request(false, 0);
        let bytes = request_bytes(&[1], &r, &canonical(1_000_000), &[0; 4]);
        assert_eq!(decode_client(&bytes), Err(DecodeError::Truncated));

        // Above fc's array limit the request is rejected however long it is.
        let bytes = request_bytes(&[1], &r, &canonical(MAX_POSITIONS + 1), &[0; 4]);
        assert_eq!(
            decode_client(&bytes),
            Err(DecodeError::TooManyPositions(MAX_POSITIONS + 1))
        );
        let bytes = request_bytes(&[3], &r, &[0xff, 0xff, 0xff, 0xff, 0x0f], &[0; 5]);
        assert_eq!(
            decode_client(&bytes),
            Err(DecodeError::TooManyPositions(u32::MAX))
        );

        // One position short of the declared count.
        let r = sample_request(false, 2);
        let bytes = request_bytes(&[1], &r, &[3], &[0; 4]);
        assert_eq!(decode_client(&bytes), Err(DecodeError::Truncated));
    }

    #[test]
    fn decoders_never_panic_on_arbitrary_bytes() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..4096 {
            let len = (next() % 192) as usize;
            let mut buf: Vec<u8> = (0..len).map(|_| next() as u8).collect();
            // Bias the first byte toward real indices so the bodies get exercised.
            if let Some(first) = buf.first_mut() {
                *first %= 6;
            }
            let _ = decode_client(&buf);
            if let UpstreamMsg::Blocks(h) = classify_upstream(&buf) {
                let _ = result_presence(&buf, &h);
            }
            let _ = read_head_lib(&buf);
            let fake = ResultHead {
                v1: next() & 1 == 1,
                head: pos(1),
                lib: pos(1),
                this_block: None,
                prev_block: None,
                fields_at: (next() % 256) as usize,
            };
            let _ = result_presence(&buf, &fake);
        }
    }

    // ----- request encoders -----

    #[test]
    fn encode_blocks_request_round_trips_v0_and_v1() {
        for v1 in [false, true] {
            for count in [0, 1, 3, 128] {
                let r = sample_request(v1, count);
                let bytes = encode_blocks_request(&r);
                assert_eq!(decode_client(&bytes), Ok(ClientMsg::Blocks(r)));
            }
        }

        let all_set = BlocksRequest {
            v1: true,
            start: u32::MAX,
            end: 0,
            max_in_flight: u32::MAX,
            have_positions: vec![pos(7)],
            irreversible_only: true,
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: true,
            fetch_finality_data: true,
        };
        assert_eq!(
            decode_client(&encode_blocks_request(&all_set)),
            Ok(ClientMsg::Blocks(all_set))
        );
    }

    #[test]
    fn encode_blocks_request_matches_the_node_layout() {
        let v0 = sample_request(false, 2);
        assert_eq!(
            encode_blocks_request(&v0),
            request_bytes(&[1], &v0, &[2], &[0, 1, 0, 1])
        );
        let v1 = sample_request(true, 2);
        assert_eq!(
            encode_blocks_request(&v1),
            request_bytes(&[3], &v1, &[2], &[0, 1, 0, 1, 1])
        );
        // A count of 128 takes two bytes.
        let wide = sample_request(false, 128);
        assert_eq!(
            encode_blocks_request(&wide),
            request_bytes(&[1], &wide, &[0x80, 0x01], &[0, 1, 0, 1])
        );

        // A v0 request never carries fetch_finality_data.
        let v0_with_finality = BlocksRequest {
            fetch_finality_data: true,
            ..sample_request(false, 0)
        };
        let bytes = encode_blocks_request(&v0_with_finality);
        assert_eq!(bytes.len(), 1 + 12 + 1 + 4);
        assert_eq!(
            decode_client(&bytes),
            Ok(ClientMsg::Blocks(BlocksRequest {
                fetch_finality_data: false,
                ..v0_with_finality
            }))
        );

        // Exact bytes for a small v1 request.
        let small = BlocksRequest {
            v1: true,
            start: 2,
            end: 1001,
            max_in_flight: 0,
            have_positions: Vec::new(),
            irreversible_only: true,
            fetch_block: true,
            fetch_traces: true,
            fetch_deltas: false,
            fetch_finality_data: false,
        };
        assert_eq!(
            encode_blocks_request(&small),
            [3, 2, 0, 0, 0, 0xe9, 0x03, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0]
        );
    }

    #[test]
    fn encoded_requests_decode_with_the_ship_abi() {
        let mut abi = rs_abieos::AbiHandle::from_json(SHIP_ABI_JSON).expect("SHiP ABI parses");
        for v1 in [false, true] {
            let r = BlocksRequest {
                irreversible_only: true,
                ..sample_request(v1, 3)
            };
            let json = abi
                .bin_to_json("request", &encode_blocks_request(&r))
                .expect("request decodes");
            let value: serde_json::Value = serde_json::from_str(&json).expect("json");
            let name = if v1 {
                "get_blocks_request_v1"
            } else {
                "get_blocks_request_v0"
            };
            assert_eq!(value[0], name, "{json}");
            let body = &value[1];
            assert_eq!(body["start_block_num"], r.start);
            assert_eq!(body["end_block_num"], r.end);
            assert_eq!(body["max_messages_in_flight"], r.max_in_flight);
            assert_eq!(body["irreversible_only"], r.irreversible_only);
            assert_eq!(body["fetch_block"], r.fetch_block);
            assert_eq!(body["fetch_traces"], r.fetch_traces);
            assert_eq!(body["fetch_deltas"], r.fetch_deltas);
            if v1 {
                assert_eq!(body["fetch_finality_data"], r.fetch_finality_data);
            } else {
                assert!(body.get("fetch_finality_data").is_none(), "{json}");
            }
            let positions = body["have_positions"].as_array().expect("positions");
            assert_eq!(positions.len(), r.have_positions.len());
            for (got, want) in positions.iter().zip(&r.have_positions) {
                assert_eq!(got["block_num"], want.num);
                let hex: String = want.id.iter().map(|b| format!("{b:02x}")).collect();
                assert_eq!(
                    got["block_id"].as_str().expect("id").to_ascii_lowercase(),
                    hex
                );
            }
        }

        let json = abi.bin_to_json("request", &encode_ack(42)).expect("ack");
        let value: serde_json::Value = serde_json::from_str(&json).expect("json");
        assert_eq!(value[0], "get_blocks_ack_request_v0");
        assert_eq!(value[1]["num_messages"], 42);

        for (v1, name) in [
            (false, "get_status_request_v0"),
            (true, "get_status_request_v1"),
        ] {
            let json = abi
                .bin_to_json("request", &encode_status_request(v1))
                .expect("status request");
            let value: serde_json::Value = serde_json::from_str(&json).expect("json");
            assert_eq!(value[0], name);
        }
    }

    #[test]
    fn ack_and_status_requests_have_fixed_bytes() {
        assert_eq!(encode_ack(0x0102_0304), [2, 4, 3, 2, 1]);
        assert_eq!(encode_status_request(false), [0]);
        assert_eq!(encode_status_request(true), [4]);
        assert_eq!(
            decode_client(&encode_ack(u32::MAX)),
            Ok(ClientMsg::Ack(u32::MAX))
        );
        assert_eq!(
            decode_client(&encode_status_request(true)),
            Ok(ClientMsg::Status { v1: true })
        );
    }

    // ----- classify_upstream -----

    #[test]
    fn classify_upstream_recognises_status_results() {
        let v0 = encode_status(false, &sample_status());
        assert_eq!(v0.len(), 121);
        assert_eq!(classify_upstream(&v0), UpstreamMsg::Status { v1: false });
        // chain_id is a binary extension: 89 bytes is still a status.
        assert_eq!(
            classify_upstream(&v0[..89]),
            UpstreamMsg::Status { v1: false }
        );
        assert_eq!(classify_upstream(&v0[..88]), UpstreamMsg::Other);

        let v1 = encode_status(true, &sample_status());
        assert_eq!(v1.len(), 129);
        assert_eq!(classify_upstream(&v1), UpstreamMsg::Status { v1: true });
        assert_eq!(classify_upstream(&v1[..128]), UpstreamMsg::Other);
        // A v1 index on a v0-sized body is not a status.
        let mut short_v1 = v0.clone();
        short_v1[0] = RES_STATUS_V1;
        assert_eq!(classify_upstream(&short_v1), UpstreamMsg::Other);
    }

    #[test]
    fn classify_upstream_decodes_block_result_heads() {
        for v1 in [false, true] {
            for (this_block, prev_block) in [
                (Some(pos(500)), Some(pos(499))),
                (Some(pos(2)), None),
                (None, None),
                (None, Some(pos(41))),
            ] {
                let spec = ResultSpec {
                    v1,
                    this_block,
                    prev_block,
                    traces: Field::Present(b"traces"),
                    ..ResultSpec::default()
                };
                let frame = result_frame(&spec);
                let expected_at = 73
                    + 1
                    + usize::from(this_block.is_some()) * 36
                    + 1
                    + usize::from(prev_block.is_some()) * 36;
                assert_eq!(
                    classify_upstream(&frame),
                    UpstreamMsg::Blocks(ResultHead {
                        v1,
                        head: pos(1000),
                        lib: pos(990),
                        this_block,
                        prev_block,
                        fields_at: expected_at,
                    })
                );
            }
        }

        // The id-less result a node sends for a block it has no id for.
        let idless = result_frame(&ResultSpec {
            this_block: None,
            prev_block: None,
            ..ResultSpec::default()
        });
        assert_eq!(idless.len(), 78);
        let h = blocks_head(&idless);
        assert_eq!((h.this_block, h.prev_block, h.fields_at), (None, None, 75));
        let idless_v1 = result_frame(&ResultSpec {
            v1: true,
            this_block: None,
            prev_block: None,
            ..ResultSpec::default()
        });
        assert_eq!(idless_v1.len(), 79);
        assert!(blocks_head(&idless_v1).v1);
    }

    #[test]
    fn classify_upstream_rejects_garbage() {
        assert_eq!(classify_upstream(&[]), UpstreamMsg::Other);
        assert_eq!(classify_upstream(&[1, 2, 3]), UpstreamMsg::Other);
        assert_eq!(classify_upstream(&[0, 1, 2]), UpstreamMsg::Other);

        let frame = result_frame(&ResultSpec::default());
        // Indices past get_status_result_v1.
        for index in [4u8, 9, 0x7f] {
            let mut other = frame.clone();
            other[0] = index;
            assert_eq!(
                classify_upstream(&other),
                UpstreamMsg::Other,
                "index {index}"
            );
        }
        // A two-byte index would move head/LIB off their fixed offsets.
        let mut wide_index = vec![0x81, 0x00];
        wide_index.extend_from_slice(&frame[1..]);
        assert_eq!(classify_upstream(&wide_index), UpstreamMsg::Other);

        // A presence byte that is not a bool.
        let mut bad_flag = frame.clone();
        bad_flag[73] = 2;
        assert_eq!(classify_upstream(&bad_flag), UpstreamMsg::Other);

        // Every cut through head, LIB, this_block or the prev_block flag.
        let fields_at = blocks_head(&frame).fields_at;
        for cut in 0..fields_at {
            assert_eq!(
                classify_upstream(&frame[..cut]),
                UpstreamMsg::Other,
                "cut {cut}"
            );
        }
    }

    // ----- result_presence -----

    /// `data` as a present field (canonical or five-byte length), or absent.
    fn pick(on: bool, wide: bool, data: &[u8]) -> Field<'_> {
        match (on, wide) {
            (false, _) => Field::Absent,
            (true, false) => Field::Present(data),
            (true, true) => Field::Wide(data),
        }
    }

    #[test]
    fn result_presence_covers_every_combination() {
        let block = [0x5a; 5];
        let traces = [0x7c; 200]; // two-byte canonical length
        let deltas: [u8; 0] = []; // present but empty
        let finality = [0x0f; 17];
        for wide in [false, true] {
            for mask in 0u8..16 {
                let want = Presence {
                    block: mask & 1 != 0,
                    traces: mask & 2 != 0,
                    deltas: mask & 4 != 0,
                    finality: mask & 8 != 0,
                };
                let spec = ResultSpec {
                    v1: true,
                    block: pick(want.block, wide, &block),
                    traces: pick(want.traces, wide, &traces),
                    deltas: pick(want.deltas, wide, &deltas),
                    finality: pick(want.finality, wide, &finality),
                    ..ResultSpec::default()
                };
                let frame = result_frame(&spec);
                let h = blocks_head(&frame);
                assert_eq!(
                    result_presence(&frame, &h),
                    Ok(want),
                    "v1 mask {mask} wide {wide}"
                );

                // v0 has no finality_data, so its bit never shows.
                let v0 = result_frame(&ResultSpec { v1: false, ..spec });
                let h0 = blocks_head(&v0);
                assert_eq!(
                    result_presence(&v0, &h0),
                    Ok(Presence {
                        finality: false,
                        ..want
                    }),
                    "v0 mask {mask} wide {wide}"
                );

                // Every strict prefix of the payload fields is an error.
                for cut in h.fields_at..frame.len() {
                    assert!(
                        result_presence(&frame[..cut], &h).is_err(),
                        "v1 mask {mask} wide {wide} cut {cut}"
                    );
                }
            }
        }
    }

    #[test]
    fn result_presence_handles_long_length_prefixes() {
        // A ten-byte varuint64 length (the full width), non-canonical for 3.
        let mut frame = result_frame(&ResultSpec::default());
        frame.truncate(blocks_head(&frame).fields_at);
        frame.push(0); // block absent
        frame.push(1); // traces present
        frame.extend_from_slice(&[0x83, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00]);
        frame.extend_from_slice(b"abc");
        frame.push(0); // deltas absent
        let h = blocks_head(&frame);
        assert_eq!(
            result_presence(&frame, &h),
            Ok(Presence {
                traces: true,
                ..Presence::default()
            })
        );

        // Eleven bytes overflow a varuint64.
        let mut overlong = frame[..h.fields_at + 2].to_vec();
        overlong.extend_from_slice(&[0x80; 10]);
        overlong.push(0x00);
        assert_eq!(result_presence(&overlong, &h), Err(DecodeError::BadVarint));

        // The block length is a varuint32: six bytes overflow it.
        let mut block_len = frame[..h.fields_at].to_vec();
        block_len.push(1);
        block_len.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x00]);
        assert_eq!(result_presence(&block_len, &h), Err(DecodeError::BadVarint));

        // A length larger than the frame is Truncated, not an allocation:
        // u32::MAX for the block, u64::MAX for traces.
        let mut huge = frame[..h.fields_at].to_vec();
        huge.extend_from_slice(&[1, 0xff, 0xff, 0xff, 0xff, 0x0f]);
        assert_eq!(result_presence(&huge, &h), Err(DecodeError::Truncated));
        let mut huge64 = frame[..h.fields_at].to_vec();
        huge64.extend_from_slice(&[0, 1]);
        huge64.extend_from_slice(&[0xff; 9]);
        huge64.push(0x01);
        assert_eq!(result_presence(&huge64, &h), Err(DecodeError::Truncated));
    }

    #[test]
    fn result_presence_rejects_bad_flags_and_ignores_trailing_bytes() {
        let frame = result_frame(&ResultSpec {
            block: Field::Present(b"blk"),
            ..ResultSpec::default()
        });
        let h = blocks_head(&frame);
        let mut bad = frame.clone();
        bad[h.fields_at] = 7;
        assert_eq!(result_presence(&bad, &h), Err(DecodeError::BadBool(7)));

        let mut trailing = frame.clone();
        trailing.extend_from_slice(&[0xff; 16]);
        assert_eq!(
            result_presence(&trailing, &h),
            Ok(Presence {
                block: true,
                ..Presence::default()
            })
        );

        // A head that points past the frame.
        let past = ResultHead {
            fields_at: frame.len() + 10,
            ..h
        };
        assert_eq!(result_presence(&frame, &past), Err(DecodeError::Truncated));
    }

    // ----- encode_status -----

    #[test]
    fn encode_status_lays_out_v0_and_v1() {
        let s = sample_status();
        let v0 = encode_status(false, &s);
        assert_eq!(v0.len(), STATUS_V0_LEN);
        assert_eq!(v0[0], 0);
        assert_eq!(&v0[1..5], &2300u32.to_le_bytes());
        assert_eq!(&v0[5..37], &id_of(2300));
        assert_eq!(&v0[37..41], &2290u32.to_le_bytes());
        assert_eq!(&v0[41..73], &id_of(2290));
        assert_eq!(&v0[73..77], &2u32.to_le_bytes());
        assert_eq!(&v0[77..81], &2301u32.to_le_bytes());
        assert_eq!(&v0[81..85], &5u32.to_le_bytes());
        assert_eq!(&v0[85..89], &2301u32.to_le_bytes());
        assert_eq!(&v0[89..121], &[0xAB; 32]);

        let v1 = encode_status(true, &s);
        assert_eq!(v1.len(), STATUS_V1_LEN);
        assert_eq!(v1[0], 3);
        assert_eq!(&v1[1..121], &v0[1..121]);
        assert_eq!(&v1[121..125], &950u32.to_le_bytes());
        assert_eq!(&v1[125..129], &2301u32.to_le_bytes());

        assert_eq!(read_head_lib(&v0), Some((s.head, s.lib)));
        assert_eq!(read_head_lib(&v1), Some((s.head, s.lib)));
    }

    #[test]
    fn encode_status_round_trips_through_the_status_decoders() {
        let s = sample_status();

        let v0 = encode_status(false, &s);
        let parsed = zcd::deserialize_status_result(&v0[1..]).expect("v0");
        assert_eq!(zcd_u32(&parsed, "head_block_num"), s.head.num);
        assert_eq!(zcd_id(&parsed, "head_block_id"), s.head.id);
        assert_eq!(zcd_u32(&parsed, "last_irreversible_block_num"), s.lib.num);
        assert_eq!(zcd_id(&parsed, "last_irreversible_block_id"), s.lib.id);
        assert_eq!(zcd_u32(&parsed, "trace_begin_block"), s.trace.begin);
        assert_eq!(zcd_u32(&parsed, "trace_end_block"), s.trace.end);
        assert_eq!(
            zcd_u32(&parsed, "chain_state_begin_block"),
            s.chain_state.begin
        );
        assert_eq!(zcd_u32(&parsed, "chain_state_end_block"), s.chain_state.end);
        assert_eq!(zcd_id(&parsed, "chain_id"), s.chain_id);

        let v1 = encode_status(true, &s);
        let parsed = zcd::deserialize_status_result_v1(&v1[1..]).expect("v1");
        assert_eq!(zcd_u32(&parsed, "head_block_num"), s.head.num);
        assert_eq!(zcd_id(&parsed, "chain_id"), s.chain_id);
        assert_eq!(
            zcd_u32(&parsed, "finality_data_begin_block"),
            s.finality.begin
        );
        assert_eq!(zcd_u32(&parsed, "finality_data_end_block"), s.finality.end);
    }

    // ----- head/LIB -----

    #[test]
    fn patch_head_lib_is_zero_copy_on_a_unique_frame() {
        let head = pos(5000);
        let lib = pos(4990);
        let original = result_frame(&ResultSpec {
            traces: Field::Present(&[0x33; 64]),
            ..ResultSpec::default()
        });

        // Exact-capacity and spare-capacity vectors use different Bytes
        // representations; both are unique.
        let mut spare = Vec::with_capacity(original.len() + 100);
        spare.extend_from_slice(&original);
        for vec in [original.clone(), spare] {
            let frame = Bytes::from(vec);
            let ptr = frame.as_ptr();
            let patched = patch_head_lib(frame, head, lib);
            assert_eq!(patched.as_ptr(), ptr, "patched in place");
            assert_eq!(read_head_lib(&patched), Some((head, lib)));
            assert_eq!(&patched[73..], &original[73..]);
            assert_eq!(patched.len(), original.len());
            let h = blocks_head(&patched);
            assert_eq!((h.head, h.lib), (head, lib));
            assert_eq!(h.this_block, Some(pos(500)));
        }
    }

    #[test]
    fn patch_head_lib_copies_a_shared_frame() {
        let original = result_frame(&ResultSpec::default());
        let frame = Bytes::from(original.clone());
        let keep = frame.clone();
        let patched = patch_head_lib(frame, pos(7000), pos(6000));
        assert_ne!(patched.as_ptr(), keep.as_ptr());
        assert_eq!(keep, original, "the other handle still sees the old bytes");
        assert_eq!(read_head_lib(&patched), Some((pos(7000), pos(6000))));
        assert_eq!(&patched[73..], &original[73..]);

        // A slice of a larger buffer is shared too.
        let mut big = vec![0xee; 10];
        big.extend_from_slice(&original);
        let whole = Bytes::from(big);
        let part = whole.slice(10..);
        let patched = patch_head_lib(part, pos(8000), pos(7000));
        assert_eq!(read_head_lib(&patched), Some((pos(8000), pos(7000))));
        assert_eq!(&whole[10..], &original[..]);

        // Static bytes are never unique.
        let fixed: &'static [u8] = &[1u8; 80];
        let patched = patch_head_lib(Bytes::from_static(fixed), pos(9), pos(8));
        assert_eq!(read_head_lib(&patched), Some((pos(9), pos(8))));
        assert_eq!(&patched[73..], &fixed[73..]);
        assert!(fixed.iter().all(|&b| b == 1));
    }

    #[test]
    fn patch_head_lib_works_on_status_frames() {
        let s = sample_status();
        for v1 in [false, true] {
            let original = encode_status(v1, &s);
            let frame = Bytes::from(original.clone());
            let ptr = frame.as_ptr();
            let patched = patch_head_lib(frame, pos(3000), pos(2999));
            assert_eq!(patched.as_ptr(), ptr);
            assert_eq!(classify_upstream(&patched), UpstreamMsg::Status { v1 });
            assert_eq!(&patched[73..], &original[73..]);
            let parsed = if v1 {
                zcd::deserialize_status_result_v1(&patched[1..])
            } else {
                zcd::deserialize_status_result(&patched[1..])
            }
            .expect("status");
            assert_eq!(zcd_u32(&parsed, "head_block_num"), 3000);
            assert_eq!(zcd_id(&parsed, "head_block_id"), id_of(3000));
            assert_eq!(zcd_u32(&parsed, "last_irreversible_block_num"), 2999);
            assert_eq!(zcd_id(&parsed, "last_irreversible_block_id"), id_of(2999));
            assert_eq!(zcd_u32(&parsed, "trace_begin_block"), 2);
            assert_eq!(zcd_id(&parsed, "chain_id"), s.chain_id);
        }
    }

    /// A result without `prev_block` (a frozen range server's first block
    /// when the block before its range is in no log it has) becomes, byte
    /// for byte, the result a node that knows that block sends, in v0 and
    /// v1, and head/LIB patching still applies in place afterwards.
    #[test]
    fn insert_prev_block_matches_the_result_with_prev_block_byte_for_byte() {
        use mock_ship::protocol::{encode_blocks_result, BlockFields};

        let block = vec![7u8; 300];
        let traces = vec![8u8; 200];
        let deltas = vec![9u8; 5];
        let finality = [1u8; 130];
        let pair = |p: BlockPos| (p.num, p.id);
        for v1 in [false, true] {
            let result = |head: u32, prev: Option<BlockPos>| {
                encode_blocks_result(&BlockFields {
                    v1,
                    head: pair(pos(head)),
                    lib: pair(pos(head - 10)),
                    this_block: Some(pair(pos(1001))),
                    prev_block: prev.map(pair),
                    block: Some(&block),
                    traces: Some(&traces),
                    deltas: Some(&deltas),
                    finality: v1.then_some(&finality[..]),
                })
            };
            let seam = result(2000, None);
            let unsplit = result(2000, Some(pos(1000)));
            let UpstreamMsg::Blocks(h) = classify_upstream(&seam) else {
                panic!("a block result");
            };
            let (filled, fh) = insert_prev_block(&seam, &h, pos(1000)).expect("inserted");
            assert_eq!(&filled[..], &unsplit[..], "v1 = {v1}");
            assert_eq!(classify_upstream(&filled), UpstreamMsg::Blocks(fh));
            assert_eq!(fh.prev_block, Some(pos(1000)));
            assert_eq!(
                result_presence(&filled, &fh),
                result_presence(&seam, &h),
                "the payload fields are untouched"
            );

            let ptr = filled.as_ptr();
            let patched = patch_head_lib(filled, pos(2300), pos(2290));
            assert_eq!(patched.as_ptr(), ptr, "the new message is patched in place");
            assert_eq!(&patched[..], &result(2300, Some(pos(1000)))[..]);

            // Nothing to insert: the result has a prev_block already.
            let UpstreamMsg::Blocks(uh) = classify_upstream(&unsplit) else {
                panic!("a block result");
            };
            assert!(insert_prev_block(&unsplit, &uh, pos(1000)).is_none());
        }
        // Nor into an id-less result.
        let idless = encode_blocks_result(&BlockFields {
            head: pair(pos(2000)),
            lib: pair(pos(1990)),
            ..BlockFields::default()
        });
        let UpstreamMsg::Blocks(h) = classify_upstream(&idless) else {
            panic!("a block result");
        };
        assert!(insert_prev_block(&idless, &h, pos(1000)).is_none());
    }

    #[test]
    fn short_frames_are_left_alone() {
        let short = Bytes::from(vec![1u8; 72]);
        let patched = patch_head_lib(short.clone(), pos(1), pos(1));
        assert_eq!(patched, short);
        assert_eq!(read_head_lib(&short), None);
        assert_eq!(read_head_lib(&[]), None);
        assert!(read_head_lib(&[1u8; 73]).is_some());
    }
}
