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
pub fn encode_varuint32(mut val: u32) -> Vec<u8> {
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
        val |= ((byte & 0x7f) as u32) << shift;
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

// --- block_position encoding ---

/// Encode a block_position: uint32_le block_num + checksum256 block_id
pub fn encode_block_position(block_num: u32, block_id: &[u8; 32]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(36);
    buf.extend_from_slice(&block_num.to_le_bytes());
    buf.extend_from_slice(block_id);
    buf
}

// --- Result encoding ---

/// Encode a get_status_result_v0 as the `result` variant (index 0).
///
/// Fields: head, last_irreversible, trace_begin_block, trace_end_block,
///         chain_state_begin_block, chain_state_end_block, chain_id
pub fn encode_status_result_v0(head_block: u32, lib_block: u32, chain_id: &[u8; 32]) -> Vec<u8> {
    let head_id = fake_block_id(head_block);
    let lib_id = fake_block_id(lib_block);

    let mut buf = Vec::with_capacity(128);
    // variant index 0 = get_status_result_v0
    buf.extend_from_slice(&encode_varuint32(0));
    // head block_position
    buf.extend_from_slice(&encode_block_position(head_block, &head_id));
    // last_irreversible block_position
    buf.extend_from_slice(&encode_block_position(lib_block, &lib_id));
    // trace_begin_block
    buf.extend_from_slice(&1u32.to_le_bytes());
    // trace_end_block
    buf.extend_from_slice(&(head_block + 1).to_le_bytes());
    // chain_state_begin_block
    buf.extend_from_slice(&1u32.to_le_bytes());
    // chain_state_end_block
    buf.extend_from_slice(&(head_block + 1).to_le_bytes());
    // chain_id (binary extension — present but optional in v0, always send it)
    buf.extend_from_slice(chain_id);
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
    let head_id = fake_block_id(head_block);
    let lib_id = fake_block_id(lib_block);
    let this_id = fake_block_id(this_block_num);
    let prev_num = if this_block_num > 0 {
        this_block_num - 1
    } else {
        0
    };
    let prev_id = fake_block_id(prev_num);

    let mut buf = Vec::with_capacity(200);
    // variant index 1 = get_blocks_result_v0
    buf.extend_from_slice(&encode_varuint32(1));
    // head
    buf.extend_from_slice(&encode_block_position(head_block, &head_id));
    // last_irreversible
    buf.extend_from_slice(&encode_block_position(lib_block, &lib_id));
    // this_block: optional present
    buf.push(1);
    buf.extend_from_slice(&encode_block_position(this_block_num, &this_id));
    // prev_block: optional present
    buf.push(1);
    buf.extend_from_slice(&encode_block_position(prev_num, &prev_id));
    // block: optional
    encode_optional_bytes(&mut buf, block_data);
    // traces: optional
    encode_optional_bytes(&mut buf, traces);
    // deltas: optional
    encode_optional_bytes(&mut buf, deltas);
    buf
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
#[derive(Debug, Clone)]
pub enum ShipRequest {
    GetStatusV0,
    GetStatusV1,
    GetBlocksV0 {
        start_block_num: u32,
        end_block_num: u32,
        max_messages_in_flight: u32,
        fetch_block: bool,
        fetch_traces: bool,
        fetch_deltas: bool,
    },
    GetBlocksV1 {
        start_block_num: u32,
        end_block_num: u32,
        max_messages_in_flight: u32,
        fetch_block: bool,
        fetch_traces: bool,
        fetch_deltas: bool,
    },
    GetBlocksAckV0 {
        num_messages: u32,
    },
    Unknown(u32),
}

/// Decode a binary request message from the client
pub fn decode_request(data: &[u8]) -> ShipRequest {
    if data.is_empty() {
        return ShipRequest::Unknown(u32::MAX);
    }

    let (variant_index, consumed) = decode_varuint32(data);
    let payload = &data[consumed..];

    match variant_index {
        0 => ShipRequest::GetStatusV0,
        1 => {
            // get_blocks_request_v0
            if payload.len() >= 12 {
                let start = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                let end = u32::from_le_bytes(payload[4..8].try_into().unwrap());
                let max_msg = u32::from_le_bytes(payload[8..12].try_into().unwrap());
                // After the 3 u32s: varuint32 have_positions count, then bools
                let (_, pos_consumed) = decode_varuint32(&payload[12..]);
                let bool_offset = 12 + pos_consumed;
                let fetch_block = payload.get(bool_offset + 1).copied().unwrap_or(0) != 0;
                let fetch_traces = payload.get(bool_offset + 2).copied().unwrap_or(0) != 0;
                let fetch_deltas = payload.get(bool_offset + 3).copied().unwrap_or(0) != 0;
                ShipRequest::GetBlocksV0 {
                    start_block_num: start,
                    end_block_num: end,
                    max_messages_in_flight: max_msg,
                    fetch_block,
                    fetch_traces,
                    fetch_deltas,
                }
            } else {
                ShipRequest::Unknown(1)
            }
        }
        2 => {
            // get_blocks_ack_request_v0
            if payload.len() >= 4 {
                let num = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                ShipRequest::GetBlocksAckV0 { num_messages: num }
            } else {
                ShipRequest::Unknown(2)
            }
        }
        3 => {
            // get_blocks_request_v1 (same layout as v0 + fetch_finality_data bool)
            if payload.len() >= 12 {
                let start = u32::from_le_bytes(payload[0..4].try_into().unwrap());
                let end = u32::from_le_bytes(payload[4..8].try_into().unwrap());
                let max_msg = u32::from_le_bytes(payload[8..12].try_into().unwrap());
                let (_, pos_consumed) = decode_varuint32(&payload[12..]);
                let bool_offset = 12 + pos_consumed;
                let fetch_block = payload.get(bool_offset + 1).copied().unwrap_or(0) != 0;
                let fetch_traces = payload.get(bool_offset + 2).copied().unwrap_or(0) != 0;
                let fetch_deltas = payload.get(bool_offset + 3).copied().unwrap_or(0) != 0;
                ShipRequest::GetBlocksV1 {
                    start_block_num: start,
                    end_block_num: end,
                    max_messages_in_flight: max_msg,
                    fetch_block,
                    fetch_traces,
                    fetch_deltas,
                }
            } else {
                ShipRequest::Unknown(3)
            }
        }
        4 => ShipRequest::GetStatusV1,
        _ => ShipRequest::Unknown(variant_index),
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
        let result = encode_status_result_v0(1000, 990, &chain_id);
        // Should start with varuint32(0)
        assert_eq!(result[0], 0);
        // head block_num should be 1000 LE at offset 1
        assert_eq!(u32::from_le_bytes(result[1..5].try_into().unwrap()), 1000);
    }

    #[test]
    fn test_blocks_result_v0_encoding() {
        let result = encode_blocks_result_v0_with_data(1000, 990, 500, None, None, None);
        // Should start with varuint32(1)
        assert_eq!(result[0], 1);
    }
}
