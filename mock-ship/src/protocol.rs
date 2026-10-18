//! SHiP binary protocol encoding/decoding compatible with fc::raw (EOSIO packed format).
//!
//! Variant indices (from Spring v1.2.2 abi.cpp):
//!
//! Request variants (client → server):
//!   0 = get_status_request_v0 (empty)
//!   1 = get_blocks_request_v0
//!   2 = get_blocks_ack_request_v0
//!   3 = get_blocks_request_v1
//!   4 = get_status_request_v1 (empty)
//!
//! Result variants (server → client):
//!   0 = get_status_result_v0
//!   1 = get_blocks_result_v0
//!   2 = get_blocks_result_v1
//!   3 = get_status_result_v1
// --- Encoding helpers (fc::raw compatible) ---

/// Encode a varuint32 (used as variant index prefix)
pub fn encode_varuint32(val: u32) -> Vec<u8> {
    encode_varuint64(u64::from(val))
}

/// Encode a varuint64. Spring writes the length of a SHiP log payload
/// (`traces`, `deltas`, `finality_data`) this way (`history_pack_varuint64`).
/// Below 2^32 the bytes are identical to [`encode_varuint32`].
pub fn encode_varuint64(mut val: u64) -> Vec<u8> {
    let mut buf = Vec::new();
    loop {
        let mut b = (val & 0x7f) as u8;
        val >>= 7;
        if val > 0 {
            b |= 0x80;
        }
        buf.push(b);
        if val == 0 {
            break;
        }
    }
    buf
}

/// Decode a varuint32, returning (value, bytes_consumed)
pub fn decode_varuint32(data: &[u8]) -> (u32, usize) {
    let mut val: u32 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        // Bits past the 32nd are dropped rather than panicking on the shift.
        val |= ((byte & 0x7f) as u32).checked_shl(shift).unwrap_or(0);
        shift += 7;
        if byte & 0x80 == 0 {
            return (val, i + 1);
        }
    }
    (val, data.len())
}

/// Generate a deterministic fake block ID from a block number
pub fn fake_block_id(block_num: u32) -> [u8; 32] {
    let mut id = [0u8; 32];
    id[0..4].copy_from_slice(&block_num.to_le_bytes());
    // Fill rest with a pattern derived from block_num for uniqueness
    for (i, byte) in id.iter_mut().enumerate().skip(4) {
        *byte = ((block_num.wrapping_mul(31).wrapping_add(i as u32)) & 0xff) as u8;
    }
    id
}

/// Generate a deterministic fake block ID for `block_num` on branch `seed`.
///
/// Seed 0 is the main branch and equals [`fake_block_id`]. Any other seed
/// flips every byte after the block number, so ids on different branches never
/// collide while `id[0..4]` still carries the block number.
pub fn fake_block_id_seeded(block_num: u32, seed: u8) -> [u8; 32] {
    let mut id = fake_block_id(block_num);
    for byte in id.iter_mut().skip(4) {
        *byte ^= seed;
    }
    id
}

// --- block_position encoding ---

/// Encode a block_position: uint32_le block_num + checksum256 block_id
pub fn encode_block_position(block_num: u32, block_id: &[u8; 32]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(36);
    buf.extend_from_slice(&block_num.to_le_bytes());
    buf.extend_from_slice(block_id);
    buf
}

// --- Result encoding ---

/// Every field of a status result. `finality` is written only for v1.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatusFields {
    pub head: (u32, [u8; 32]),
    pub lib: (u32, [u8; 32]),
    /// `[trace_begin_block, trace_end_block)`.
    pub trace: (u32, u32),
    /// `[chain_state_begin_block, chain_state_end_block)`.
    pub chain_state: (u32, u32),
    pub chain_id: [u8; 32],
    /// `[finality_data_begin_block, finality_data_end_block)`.
    pub finality: (u32, u32),
}

/// Encode a status result: `get_status_result_v0` (index 0, 121 bytes, the
/// `chain_id` extension always present) or `get_status_result_v1` (index 3,
/// 129 bytes).
pub fn encode_status(v1: bool, s: &StatusFields) -> Vec<u8> {
    let mut buf = Vec::with_capacity(129);
    buf.extend_from_slice(&encode_varuint32(if v1 { 3 } else { 0 }));
    buf.extend_from_slice(&encode_block_position(s.head.0, &s.head.1));
    buf.extend_from_slice(&encode_block_position(s.lib.0, &s.lib.1));
    for value in [s.trace.0, s.trace.1, s.chain_state.0, s.chain_state.1] {
        buf.extend_from_slice(&value.to_le_bytes());
    }
    buf.extend_from_slice(&s.chain_id);
    if v1 {
        buf.extend_from_slice(&s.finality.0.to_le_bytes());
        buf.extend_from_slice(&s.finality.1.to_le_bytes());
    }
    buf
}

/// Encode a get_status_result_v0 as the `result` variant (index 0).
///
/// Fields: head, last_irreversible, trace_begin_block, trace_end_block,
///         chain_state_begin_block, chain_state_end_block, chain_id
pub fn encode_status_result_v0(
    head_block: u32,
    lib_block: u32,
    chain_id: &[u8; 32],
    trace_begin: u32,
    trace_end: u32,
) -> Vec<u8> {
    encode_status(
        false,
        &StatusFields {
            head: (head_block, fake_block_id(head_block)),
            lib: (lib_block, fake_block_id(lib_block)),
            trace: (trace_begin, trace_end),
            chain_state: (trace_begin, trace_end),
            chain_id: *chain_id,
            finality: (0, 0),
        },
    )
}

/// Encode a get_status_result_v1 as the `result` variant (index 3).
///
/// Same fields as v0, with `chain_id` required, followed by
/// `finality_data_begin_block` and `finality_data_end_block` (exclusive end).
pub fn encode_status_result_v1(
    head_block: u32,
    lib_block: u32,
    chain_id: &[u8; 32],
    trace_begin: u32,
    trace_end: u32,
    finality_begin: u32,
    finality_end: u32,
) -> Vec<u8> {
    encode_status(
        true,
        &StatusFields {
            head: (head_block, fake_block_id(head_block)),
            lib: (lib_block, fake_block_id(lib_block)),
            trace: (trace_begin, trace_end),
            chain_state: (trace_begin, trace_end),
            chain_id: *chain_id,
            finality: (finality_begin, finality_end),
        },
    )
}

/// Every field of a blocks result. `finality` is written only for v1.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockFields<'a> {
    /// `get_blocks_result_v1` (index 2) instead of v0 (index 1).
    pub v1: bool,
    pub head: (u32, [u8; 32]),
    pub lib: (u32, [u8; 32]),
    pub this_block: Option<(u32, [u8; 32])>,
    pub prev_block: Option<(u32, [u8; 32])>,
    pub block: Option<&'a [u8]>,
    pub traces: Option<&'a [u8]>,
    pub deltas: Option<&'a [u8]>,
    pub finality: Option<&'a [u8]>,
}

/// Encode a `get_blocks_result_v0` or `_v1` the way Spring's session does.
///
/// `block` carries a varuint32 length (fc reflection). `traces`, `deltas` and
/// `finality_data` carry a varuint64 length (`history_pack_varuint64`). An
/// absent field is a single `00` byte.
pub fn encode_blocks_result(r: &BlockFields<'_>) -> Vec<u8> {
    let payload: usize = [r.block, r.traces, r.deltas, r.finality]
        .iter()
        .flatten()
        .map(|d| d.len() + 10)
        .sum();
    let mut buf = Vec::with_capacity(160 + payload);
    buf.extend_from_slice(&encode_varuint32(if r.v1 { 2 } else { 1 }));
    buf.extend_from_slice(&encode_block_position(r.head.0, &r.head.1));
    buf.extend_from_slice(&encode_block_position(r.lib.0, &r.lib.1));
    for position in [r.this_block, r.prev_block] {
        match position {
            Some((num, id)) => {
                buf.push(1);
                buf.extend_from_slice(&encode_block_position(num, &id));
            }
            None => buf.push(0),
        }
    }
    encode_optional_bytes(&mut buf, r.block);
    encode_optional_log_entry(&mut buf, r.traces);
    encode_optional_log_entry(&mut buf, r.deltas);
    if r.v1 {
        encode_optional_log_entry(&mut buf, r.finality);
    }
    buf
}

/// Encode a get_blocks_result_v0 with optional data payloads.
///
/// When `block_data`, `traces`, or `deltas` are `Some`, the optional is encoded as
/// present (flag byte 1) followed by a length-prefixed byte array (fc::raw `bytes`).
pub fn encode_blocks_result_v0_with_data(
    head_block: u32,
    lib_block: u32,
    this_block_num: u32,
    block_data: Option<&[u8]>,
    traces: Option<&[u8]>,
    deltas: Option<&[u8]>,
) -> Vec<u8> {
    let prev_num = this_block_num.saturating_sub(1);
    encode_blocks_result(&BlockFields {
        v1: false,
        head: (head_block, fake_block_id(head_block)),
        lib: (lib_block, fake_block_id(lib_block)),
        this_block: Some((this_block_num, fake_block_id(this_block_num))),
        prev_block: Some((prev_num, fake_block_id(prev_num))),
        block: block_data,
        traces,
        deltas,
        finality: None,
    })
}

/// Encode an optional<bytes> field: flag byte + varuint32 length + data
fn encode_optional_bytes(buf: &mut Vec<u8>, data: Option<&[u8]>) {
    match data {
        Some(d) => {
            buf.push(1); // present
            buf.extend_from_slice(&encode_varuint32(d.len() as u32));
            buf.extend_from_slice(d);
        }
        None => {
            buf.push(0); // absent
        }
    }
}

/// Encode an optional log entry: flag byte + varuint64 length + data
fn encode_optional_log_entry(buf: &mut Vec<u8>, data: Option<&[u8]>) {
    match data {
        Some(d) => {
            buf.push(1);
            buf.extend_from_slice(&encode_varuint64(d.len() as u64));
            buf.extend_from_slice(d);
        }
        None => buf.push(0),
    }
}

/// Generate deterministic fake data of `size` bytes for a given block number.
/// Uses bulk allocation for fast debug-mode performance.
pub fn generate_fake_data(block_num: u32, size: usize) -> Vec<u8> {
    // Bulk-fill with a seed byte derived from block_num (fast memset)
    let seed = (block_num.wrapping_mul(7) & 0xff) as u8;
    let mut data = vec![seed; size];
    // Stamp the first 4 bytes with block_num for uniqueness verification
    if size >= 4 {
        data[0..4].copy_from_slice(&block_num.to_le_bytes());
    }
    data
}

// --- Request decoding ---

/// Parsed request from a client
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShipRequest {
    GetStatusV0,
    GetStatusV1,
    GetBlocksV0 {
        start_block_num: u32,
        end_block_num: u32,
        max_messages_in_flight: u32,
        have_positions: Vec<(u32, [u8; 32])>,
        irreversible_only: bool,
        fetch_block: bool,
        fetch_traces: bool,
        fetch_deltas: bool,
    },
    GetBlocksV1 {
        start_block_num: u32,
        end_block_num: u32,
        max_messages_in_flight: u32,
        have_positions: Vec<(u32, [u8; 32])>,
        irreversible_only: bool,
        fetch_block: bool,
        fetch_traces: bool,
        fetch_deltas: bool,
        fetch_finality_data: bool,
    },
    GetBlocksAckV0 {
        num_messages: u32,
    },
    Unknown(u32),
}

/// Why [`decode_request_strict`] rejected a message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    Empty,
    /// More than 5 bytes, or a value above `u32::MAX`.
    BadVarint,
    Truncated,
    /// A bool byte other than 0 or 1.
    BadBool(u8),
    UnknownVariant(u32),
}

/// Decode a binary request message from the client, leniently.
///
/// This is the [`Legacy`](crate::Fidelity::Legacy) decoder: a missing bool reads
/// as `false`, any non-zero bool byte as `true`, and a known variant whose fixed
/// fields are short comes back as [`ShipRequest::Unknown`] with its index.
pub fn decode_request(data: &[u8]) -> ShipRequest {
    if data.is_empty() {
        return ShipRequest::Unknown(u32::MAX);
    }

    let (variant_index, consumed) = decode_varuint32(data);
    let payload = &data[consumed..];

    match variant_index {
        0 => ShipRequest::GetStatusV0,
        1 | 3 => decode_blocks_lenient(payload, variant_index == 3)
            .unwrap_or(ShipRequest::Unknown(variant_index)),
        2 => {
            // get_blocks_ack_request_v0
            if payload.len() >= 4 {
                let num = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                ShipRequest::GetBlocksAckV0 { num_messages: num }
            } else {
                ShipRequest::Unknown(2)
            }
        }
        4 => ShipRequest::GetStatusV1,
        _ => ShipRequest::Unknown(variant_index),
    }
}

/// get_blocks_request_v0 / v1 after the variant index. v1 appends
/// `fetch_finality_data`.
fn decode_blocks_lenient(payload: &[u8], v1: bool) -> Option<ShipRequest> {
    if payload.len() < 12 {
        return None;
    }
    let start = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    let end = u32::from_le_bytes(payload[4..8].try_into().unwrap());
    let max_msg = u32::from_le_bytes(payload[8..12].try_into().unwrap());
    // After the 3 u32s: varuint32 have_positions count, then 36 bytes per
    // position (u32 block_num + checksum256 block_id), then the bools.
    let (count, pos_consumed) = decode_varuint32(&payload[12..]);
    let positions_at = 12 + pos_consumed;
    let mut have_positions = Vec::new();
    let (complete, _) = payload[positions_at.min(payload.len())..].as_chunks::<36>();
    for raw in complete.iter().take(count as usize) {
        have_positions.push(read_position(raw));
    }
    let bool_offset = positions_at.saturating_add((count as usize).saturating_mul(36));
    let flag = |i: usize| {
        payload
            .get(bool_offset.saturating_add(i))
            .copied()
            .unwrap_or(0)
            != 0
    };
    Some(if v1 {
        ShipRequest::GetBlocksV1 {
            start_block_num: start,
            end_block_num: end,
            max_messages_in_flight: max_msg,
            have_positions,
            irreversible_only: flag(0),
            fetch_block: flag(1),
            fetch_traces: flag(2),
            fetch_deltas: flag(3),
            fetch_finality_data: flag(4),
        }
    } else {
        ShipRequest::GetBlocksV0 {
            start_block_num: start,
            end_block_num: end,
            max_messages_in_flight: max_msg,
            have_positions,
            irreversible_only: flag(0),
            fetch_block: flag(1),
            fetch_traces: flag(2),
            fetch_deltas: flag(3),
        }
    })
}

/// Decode a request with fc::raw semantics, as Spring does.
///
/// The variant index is a varuint32 (non-canonical encodings accepted), bools
/// must be 0 or 1, positions are bounds-checked before allocation, and trailing
/// bytes are ignored. Spring drops the socket without a Close frame on any
/// error. [`ShipRequest::Unknown`] is never returned.
pub fn decode_request_strict(data: &[u8]) -> Result<ShipRequest, DecodeError> {
    if data.is_empty() {
        return Err(DecodeError::Empty);
    }
    let mut r = Reader::new(data);
    let variant = r.varuint32()?;
    match variant {
        0 => Ok(ShipRequest::GetStatusV0),
        4 => Ok(ShipRequest::GetStatusV1),
        2 => Ok(ShipRequest::GetBlocksAckV0 {
            num_messages: r.u32()?,
        }),
        1 | 3 => {
            let start_block_num = r.u32()?;
            let end_block_num = r.u32()?;
            let max_messages_in_flight = r.u32()?;
            let count = r.varuint32()? as usize;
            if count.saturating_mul(36) > r.remaining() {
                return Err(DecodeError::Truncated);
            }
            let mut have_positions = Vec::with_capacity(count);
            for _ in 0..count {
                have_positions.push(read_position(r.take(36)?));
            }
            let irreversible_only = r.bool()?;
            let fetch_block = r.bool()?;
            let fetch_traces = r.bool()?;
            let fetch_deltas = r.bool()?;
            if variant == 1 {
                return Ok(ShipRequest::GetBlocksV0 {
                    start_block_num,
                    end_block_num,
                    max_messages_in_flight,
                    have_positions,
                    irreversible_only,
                    fetch_block,
                    fetch_traces,
                    fetch_deltas,
                });
            }
            Ok(ShipRequest::GetBlocksV1 {
                start_block_num,
                end_block_num,
                max_messages_in_flight,
                have_positions,
                irreversible_only,
                fetch_block,
                fetch_traces,
                fetch_deltas,
                fetch_finality_data: r.bool()?,
            })
        }
        other => Err(DecodeError::UnknownVariant(other)),
    }
}

fn read_position(raw: &[u8]) -> (u32, [u8; 32]) {
    let num = u32::from_le_bytes(raw[0..4].try_into().unwrap());
    let id: [u8; 32] = raw[4..36].try_into().unwrap();
    (num, id)
}

/// A bounds-checked little-endian reader over one message.
pub(crate) struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if n > self.remaining() {
            return Err(DecodeError::Truncated);
        }
        let out = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(out)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub(crate) fn bool(&mut self) -> Result<bool, DecodeError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(DecodeError::BadBool(other)),
        }
    }

    pub(crate) fn position(&mut self) -> Result<(u32, [u8; 32]), DecodeError> {
        Ok(read_position(self.take(36)?))
    }

    /// Up to 5 bytes; a value above `u32::MAX` is [`DecodeError::BadVarint`].
    pub(crate) fn varuint32(&mut self) -> Result<u32, DecodeError> {
        let value = self.varuint(5)?;
        u32::try_from(value).map_err(|_| DecodeError::BadVarint)
    }

    /// Up to 10 bytes, for `history_pack_varuint64` lengths.
    pub(crate) fn varuint64(&mut self) -> Result<u64, DecodeError> {
        self.varuint(10)
    }

    fn varuint(&mut self, max_bytes: usize) -> Result<u64, DecodeError> {
        let mut value: u64 = 0;
        for i in 0..max_bytes {
            let byte = self.u8()?;
            value |= u64::from(byte & 0x7f)
                .checked_shl(7 * i as u32)
                .unwrap_or(0);
            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }
        Err(DecodeError::BadVarint)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_varuint32_roundtrip() {
        for val in [0, 1, 127, 128, 255, 16383, 16384, u32::MAX] {
            let encoded = encode_varuint32(val);
            let (decoded, consumed) = decode_varuint32(&encoded);
            assert_eq!(val, decoded);
            assert_eq!(consumed, encoded.len());
        }
    }

    #[test]
    fn test_status_request_v0_decode() {
        // variant index 0 = get_status_request_v0 (empty body)
        let data = encode_varuint32(0);
        match decode_request(&data) {
            ShipRequest::GetStatusV0 => {}
            other => panic!("Expected GetStatusV0, got {:?}", other),
        }
    }

    #[test]
    fn test_blocks_request_v0_decode() {
        let mut data = encode_varuint32(1);
        data.extend_from_slice(&100u32.to_le_bytes()); // start_block_num
        data.extend_from_slice(&200u32.to_le_bytes()); // end_block_num
        data.extend_from_slice(&10u32.to_le_bytes()); // max_messages_in_flight
                                                      // have_positions: empty array (varuint32 0)
        data.extend_from_slice(&encode_varuint32(0));
        // bools
        data.push(0); // irreversible_only
        data.push(0); // fetch_block
        data.push(0); // fetch_traces
        data.push(0); // fetch_deltas

        match decode_request(&data) {
            ShipRequest::GetBlocksV0 {
                start_block_num,
                end_block_num,
                max_messages_in_flight,
                ..
            } => {
                assert_eq!(start_block_num, 100);
                assert_eq!(end_block_num, 200);
                assert_eq!(max_messages_in_flight, 10);
            }
            other => panic!("Expected GetBlocksV0, got {:?}", other),
        }
    }

    #[test]
    fn test_ack_request_decode() {
        let mut data = encode_varuint32(2);
        data.extend_from_slice(&5u32.to_le_bytes());
        match decode_request(&data) {
            ShipRequest::GetBlocksAckV0 { num_messages } => {
                assert_eq!(num_messages, 5);
            }
            other => panic!("Expected GetBlocksAckV0, got {:?}", other),
        }
    }

    #[test]
    fn test_status_result_v0_encoding() {
        let chain_id = [0xABu8; 32];
        let result = encode_status_result_v0(1000, 990, &chain_id, 1, 1001);
        // Should start with varuint32(0)
        assert_eq!(result[0], 0);
        // head block_num should be 1000 LE at offset 1
        assert_eq!(u32::from_le_bytes(result[1..5].try_into().unwrap()), 1000);
    }

    #[test]
    fn test_status_result_v1_encoding_appends_finality_range() {
        let chain_id = [0xABu8; 32];
        let result = encode_status_result_v1(1000, 990, &chain_id, 1, 1001, 10, 500);
        assert_eq!(result[0], 3);
        assert_eq!(u32::from_le_bytes(result[1..5].try_into().unwrap()), 1000);
        let n = result.len();
        assert!(n >= 8);
        assert_eq!(
            u32::from_le_bytes(result[n - 8..n - 4].try_into().unwrap()),
            10
        );
        assert_eq!(
            u32::from_le_bytes(result[n - 4..n].try_into().unwrap()),
            500
        );
        // v0 frame is the same prefix without the trailing 8 bytes, and variant 0.
        let v0 = encode_status_result_v0(1000, 990, &chain_id, 1, 1001);
        assert_eq!(result.len(), v0.len() + 8);
    }

    #[test]
    fn test_blocks_request_v1_decode_finality_flag() {
        let mut data = encode_varuint32(3);
        data.extend_from_slice(&100u32.to_le_bytes());
        data.extend_from_slice(&200u32.to_le_bytes());
        data.extend_from_slice(&10u32.to_le_bytes());
        data.extend_from_slice(&encode_varuint32(0));
        data.push(0); // irreversible_only
        data.push(1); // fetch_block
        data.push(0); // fetch_traces
        data.push(0); // fetch_deltas
        data.push(1); // fetch_finality_data

        match decode_request(&data) {
            ShipRequest::GetBlocksV1 {
                start_block_num,
                fetch_block,
                fetch_finality_data,
                ..
            } => {
                assert_eq!(start_block_num, 100);
                assert!(fetch_block);
                assert!(fetch_finality_data);
            }
            other => panic!("Expected GetBlocksV1, got {:?}", other),
        }
    }

    #[test]
    fn test_blocks_result_v0_encoding() {
        let result = encode_blocks_result_v0_with_data(1000, 990, 500, None, None, None);
        // Should start with varuint32(1)
        assert_eq!(result[0], 1);
    }

    /// A get_blocks request with `positions`, `irreversible_only` set and the
    /// fetch flags `[block, traces, deltas, finality]`.
    fn blocks_request(v1: bool, positions: &[(u32, [u8; 32])], flags: [u8; 4]) -> Vec<u8> {
        let mut data = encode_varuint32(if v1 { 3 } else { 1 });
        data.extend_from_slice(&100u32.to_le_bytes());
        data.extend_from_slice(&200u32.to_le_bytes());
        data.extend_from_slice(&10u32.to_le_bytes());
        data.extend_from_slice(&encode_varuint32(positions.len() as u32));
        for (num, id) in positions {
            data.extend_from_slice(&encode_block_position(*num, id));
        }
        data.push(1); // irreversible_only
        data.extend_from_slice(&flags[..3]);
        if v1 {
            data.push(flags[3]);
        }
        data
    }

    #[test]
    fn positions_are_skipped_and_irreversible_only_is_read() {
        let positions: Vec<(u32, [u8; 32])> = (90..93).map(|n| (n, fake_block_id(n))).collect();
        // fetch_block off, traces on, deltas off: a decoder that does not skip
        // the 108 position bytes reads id bytes as flags instead.
        let lenient = decode_request(&blocks_request(false, &positions, [0, 1, 0, 0]));
        let strict = decode_request_strict(&blocks_request(false, &positions, [0, 1, 0, 0]));
        let expected = ShipRequest::GetBlocksV0 {
            start_block_num: 100,
            end_block_num: 200,
            max_messages_in_flight: 10,
            have_positions: positions.clone(),
            irreversible_only: true,
            fetch_block: false,
            fetch_traces: true,
            fetch_deltas: false,
        };
        assert_eq!(lenient, expected);
        assert_eq!(strict, Ok(expected));

        let v1 = decode_request_strict(&blocks_request(true, &positions, [1, 0, 1, 1])).unwrap();
        assert_eq!(
            v1,
            ShipRequest::GetBlocksV1 {
                start_block_num: 100,
                end_block_num: 200,
                max_messages_in_flight: 10,
                have_positions: positions,
                irreversible_only: true,
                fetch_block: true,
                fetch_traces: false,
                fetch_deltas: true,
                fetch_finality_data: true,
            }
        );
    }

    #[test]
    fn strict_decoder_follows_fc_semantics() {
        let positions: Vec<(u32, [u8; 32])> = (1..4).map(|n| (n, fake_block_id(n))).collect();
        let valid = blocks_request(true, &positions, [0, 0, 0, 0]);

        // Every truncation of a valid request is an error, never a panic.
        for cut in 0..valid.len() {
            assert!(
                decode_request_strict(&valid[..cut]).is_err(),
                "cut at {cut}"
            );
        }
        // Trailing bytes are ignored.
        let mut padded = valid.clone();
        padded.extend_from_slice(&[0xEE; 7]);
        assert_eq!(
            decode_request_strict(&padded),
            decode_request_strict(&valid)
        );
        // A non-canonical varuint index is accepted.
        assert_eq!(
            decode_request_strict(&[0x84, 0x00]),
            Ok(ShipRequest::GetStatusV1)
        );
        // A bool byte other than 0 or 1 is rejected.
        let mut bad_bool = valid.clone();
        let last = bad_bool.len() - 1;
        bad_bool[last] = 2;
        assert_eq!(
            decode_request_strict(&bad_bool),
            Err(DecodeError::BadBool(2))
        );
        // A six-byte varuint and an unknown variant are rejected.
        assert_eq!(
            decode_request_strict(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x00]),
            Err(DecodeError::BadVarint)
        );
        assert_eq!(
            decode_request_strict(&[9]),
            Err(DecodeError::UnknownVariant(9))
        );
        // A positions count larger than the message is rejected before allocating.
        let mut huge = encode_varuint32(1);
        huge.extend_from_slice(&[0; 12]);
        huge.extend_from_slice(&encode_varuint32(u32::MAX));
        assert_eq!(decode_request_strict(&huge), Err(DecodeError::Truncated));
        assert_eq!(decode_request_strict(&[]), Err(DecodeError::Empty));
    }

    #[test]
    fn seeded_ids_differ_from_the_main_branch_and_keep_the_number() {
        assert_eq!(fake_block_id_seeded(42, 0), fake_block_id(42));
        let forked = fake_block_id_seeded(42, 7);
        assert_ne!(forked, fake_block_id(42));
        assert_ne!(forked, fake_block_id_seeded(42, 8));
        assert_eq!(&forked[0..4], &42u32.to_le_bytes());
    }

    #[test]
    fn blocks_result_uses_v1_variant_and_finality_field() {
        let id = fake_block_id(7);
        let fields = BlockFields {
            v1: true,
            head: (9, fake_block_id(9)),
            lib: (8, fake_block_id(8)),
            this_block: Some((7, id)),
            prev_block: None,
            block: None,
            traces: Some(&[1, 2, 3]),
            deltas: None,
            finality: Some(&[4]),
        };
        let v1 = encode_blocks_result(&fields);
        assert_eq!(v1[0], 2);
        // idx + head + lib + this(1+36) + prev(1) + block(1) + traces(1+1+3) + deltas(1) + finality(1+1+1)
        assert_eq!(v1.len(), 1 + 36 + 36 + 37 + 1 + 1 + 5 + 1 + 3);
        let v0 = encode_blocks_result(&BlockFields {
            v1: false,
            ..fields
        });
        assert_eq!(v0[0], 1);
        assert_eq!(v0.len(), v1.len() - 3);
        // A head-only result is 78 bytes.
        let keepalive = encode_blocks_result(&BlockFields {
            head: (9, fake_block_id(9)),
            lib: (8, fake_block_id(8)),
            ..BlockFields::default()
        });
        assert_eq!(keepalive.len(), 78);
    }
}
