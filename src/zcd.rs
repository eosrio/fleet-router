// ZCD Deserialization (Zero Copy Deserialization)
// This module is used to deserialize the binary data received from the WebSocket
// server without copying the data to a new buffer
// Deserialized objects are composed of references to the original buffer
#![allow(dead_code)]

use std::collections::HashMap;
use std::fmt::Display;
use ZCDType::{Bytes, Checksum256, U32, U8};

use crate::functions::buffer_to_hex;
use crate::zcd::ZCDType::Array;

/// Failure while decoding an untrusted SHiP frame.
///
/// A frame that ends on a field boundary before a required field is
/// [`Truncated`](ZcdError::Truncated). A frame that supplies some, but not all,
/// of a field's bytes is [`Malformed`](ZcdError::Malformed).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZcdError {
    /// Input ended on a field boundary before `field` was present.
    Truncated {
        field: &'static str,
        expected: usize,
        remaining: usize,
    },
    /// `field` was present but its width does not match the type.
    Malformed {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    /// Lookup of a field the frame did not contain.
    MissingField,
    /// Decoded value was a different type than the conversion requested.
    TypeMismatch,
}

impl Display for ZcdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ZcdError::Truncated {
                field,
                expected,
                remaining,
            } => write!(
                f,
                "truncated SHiP field '{field}': need {expected} bytes, {remaining} remaining"
            ),
            ZcdError::Malformed {
                field,
                expected,
                actual,
            } => write!(
                f,
                "malformed SHiP field '{field}': expected {expected} bytes, got {actual}"
            ),
            ZcdError::MissingField => write!(f, "SHiP field is not present in the frame"),
            ZcdError::TypeMismatch => {
                write!(f, "SHiP value has a different type than requested")
            }
        }
    }
}

impl std::error::Error for ZcdError {}

/// Read exactly `N` bytes, or report a malformed field. Never panics.
fn read_exact<const N: usize>(slice: &[u8], field: &'static str) -> Result<[u8; N], ZcdError> {
    let bytes: &[u8; N] = slice.try_into().map_err(|_| ZcdError::Malformed {
        field,
        expected: N,
        actual: slice.len(),
    })?;
    Ok(*bytes)
}

fn type_width(ty: &ZCDType) -> Option<usize> {
    match ty {
        ZCDType::Bool | ZCDType::U8 => Some(1),
        ZCDType::U32 => Some(4),
        ZCDType::U64 => Some(8),
        ZCDType::U128 => Some(16),
        ZCDType::Checksum256 => Some(32),
        ZCDType::String | ZCDType::Bytes | ZCDType::Array => None,
    }
}

#[derive(Debug)]
pub enum ZCDType {
    Bool,
    U8,
    U32,
    U64,
    U128,
    Checksum256,
    String,
    Bytes,
    Array,
}

#[derive(Debug)]
pub enum ZCDValues {
    Bool(bool),
    U8(u8),
    U32(u32),
    U64(u64),
    U128(u128),
    Checksum256([u8; 32]),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<u8>),
}

impl Display for ZCDValues {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ZCDValues::Bool(v) => write!(f, "{}", v),
            ZCDValues::U8(v) => write!(f, "{}", v),
            ZCDValues::U32(v) => write!(f, "{}", v),
            ZCDValues::U64(v) => write!(f, "{}", v),
            ZCDValues::U128(v) => write!(f, "{}", v),
            ZCDValues::String(v) => write!(f, "{}", v),
            ZCDValues::Checksum256(v) => write!(f, "{}", buffer_to_hex(v.to_vec())),
            ZCDValues::Bytes(v) => write!(f, "{}", buffer_to_hex(v.to_vec())),
            ZCDValues::Array(v) => write!(f, "{}", buffer_to_hex(v.to_vec())),
        }
    }
}

impl TryFrom<ZCDValues> for u8 {
    type Error = ZcdError;

    fn try_from(value: ZCDValues) -> Result<Self, Self::Error> {
        match value {
            ZCDValues::U8(v) => Ok(v),
            _ => Err(ZcdError::TypeMismatch),
        }
    }
}

impl TryFrom<ZCDValues> for u32 {
    type Error = ZcdError;

    fn try_from(value: ZCDValues) -> Result<Self, Self::Error> {
        match value {
            ZCDValues::U32(v) => Ok(v),
            _ => Err(ZcdError::TypeMismatch),
        }
    }
}

impl TryFrom<ZCDValues> for u64 {
    type Error = ZcdError;

    fn try_from(value: ZCDValues) -> Result<Self, Self::Error> {
        match value {
            ZCDValues::U64(v) => Ok(v),
            _ => Err(ZcdError::TypeMismatch),
        }
    }
}

impl TryFrom<ZCDValues> for u128 {
    type Error = ZcdError;

    fn try_from(value: ZCDValues) -> Result<Self, Self::Error> {
        match value {
            ZCDValues::U128(v) => Ok(v),
            _ => Err(ZcdError::TypeMismatch),
        }
    }
}

impl TryFrom<ZCDValues> for [u8; 32] {
    type Error = ZcdError;

    fn try_from(value: ZCDValues) -> Result<Self, Self::Error> {
        match value {
            ZCDValues::Checksum256(v) => Ok(v),
            _ => Err(ZcdError::TypeMismatch),
        }
    }
}

impl TryFrom<ZCDValues> for String {
    type Error = ZcdError;

    fn try_from(value: ZCDValues) -> Result<Self, Self::Error> {
        match value {
            ZCDValues::String(v) => Ok(v),
            _ => Err(ZcdError::TypeMismatch),
        }
    }
}

impl From<u8> for ZCDValues {
    fn from(val: u8) -> Self {
        ZCDValues::U8(val)
    }
}

#[derive(Debug)]
pub struct ZCDField<'a> {
    pub slice: &'a [u8],
    pub f_type: &'a ZCDType,
    pub size: usize,
}

impl ZCDField<'_> {
    pub fn as_bool(&self) -> Result<bool, ZcdError> {
        let [byte] = read_exact(self.slice, "bool")?;
        Ok(byte != 0)
    }

    pub fn as_u8(&self) -> Result<u8, ZcdError> {
        let [byte] = read_exact(self.slice, "u8")?;
        Ok(byte)
    }

    pub fn as_u32(&self) -> Result<u32, ZcdError> {
        Ok(u32::from_le_bytes(read_exact(self.slice, "u32")?))
    }

    pub fn as_u64(&self) -> Result<u64, ZcdError> {
        Ok(u64::from_le_bytes(read_exact(self.slice, "u64")?))
    }

    pub fn as_u128(&self) -> Result<u128, ZcdError> {
        Ok(u128::from_le_bytes(read_exact(self.slice, "u128")?))
    }

    pub fn as_checksum256(&self) -> Result<[u8; 32], ZcdError> {
        read_exact(self.slice, "checksum256")
    }

    pub fn as_checksum256_hex(&self) -> String {
        buffer_to_hex(self.slice.to_vec())
    }

    pub fn as_string(&self) -> String {
        String::from_utf8_lossy(self.slice).to_string()
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug)]
pub struct ZCD<'a> {
    pub buffer: &'a [u8],
    pub hash_map: HashMap<String, ZCDField<'a>>,
}

impl ZCD<'_> {
    pub fn from<'a>(buffer: &'a [u8], hash_map: HashMap<String, ZCDField<'a>>) -> ZCD<'a> {
        ZCD { buffer, hash_map }
    }

    pub fn get_type(&self, field: &str) -> Option<&ZCDType> {
        self.hash_map.get(field).map(|entry| entry.f_type)
    }

    pub fn get(&self, field: &str) -> Result<Option<ZCDValues>, ZcdError> {
        let Some(f) = self.hash_map.get(field) else {
            return Ok(None);
        };
        let value = match f.f_type {
            ZCDType::Bool => ZCDValues::Bool(f.as_bool()?),
            ZCDType::U8 => ZCDValues::U8(f.as_u8()?),
            ZCDType::U32 => ZCDValues::U32(f.as_u32()?),
            ZCDType::U64 => ZCDValues::U64(f.as_u64()?),
            ZCDType::U128 => ZCDValues::U128(f.as_u128()?),
            ZCDType::Checksum256 => ZCDValues::Checksum256(f.as_checksum256()?),
            ZCDType::String => ZCDValues::String(f.as_string()),
            ZCDType::Bytes => ZCDValues::Bytes(f.slice.to_vec()),
            ZCDType::Array => ZCDValues::Array(f.slice.to_vec()),
        };
        Ok(Some(value))
    }
}

// Field Name, Field Size, is bounded
pub const RESULT_V0_FIELDS: [(&ZCDType, &str, usize); 2] =
    [(&U8, "variant", 1), (&Bytes, "data", 0)];

pub const STATUS_RESULT_V0_FIELDS: [(&ZCDType, &str, usize); 9] = [
    (&U32, "head_block_num", 4),
    (&Checksum256, "head_block_id", 32),
    (&U32, "last_irreversible_block_num", 4),
    (&Checksum256, "last_irreversible_block_id", 32),
    (&U32, "trace_begin_block", 4),
    (&U32, "trace_end_block", 4),
    (&U32, "chain_state_begin_block", 4),
    (&U32, "chain_state_end_block", 4),
    (&Checksum256, "chain_id", 32),
];

/// `get_status_result_v1` (result variant 3). Same prefix as v0, plus the
/// finality-data block range. `finality_data_end_block` is one past the last
/// block that has finality data, matching `trace_end_block`.
///
/// Unlike v0, `chain_id` and both finality bounds are required.
pub const STATUS_RESULT_V1_FIELDS: [(&ZCDType, &str, usize); 11] = [
    (&U32, "head_block_num", 4),
    (&Checksum256, "head_block_id", 32),
    (&U32, "last_irreversible_block_num", 4),
    (&Checksum256, "last_irreversible_block_id", 32),
    (&U32, "trace_begin_block", 4),
    (&U32, "trace_end_block", 4),
    (&U32, "chain_state_begin_block", 4),
    (&U32, "chain_state_end_block", 4),
    (&Checksum256, "chain_id", 32),
    (&U32, "finality_data_begin_block", 4),
    (&U32, "finality_data_end_block", 4),
];

fn zcd_builder<'a>(buffer: &'a [u8], fields: &'a [(&ZCDType, &str, usize)]) -> ZCD<'a> {
    let mut offset = 0;
    let mut hash_map = HashMap::new();
    for (index, (f_type, field, size)) in fields.iter().enumerate() {
        match f_type {
            Array => {
                // Count byte plus `size` bytes per element. A truncated frame,
                // or a count whose span does not fit in `usize`, leaves the
                // field absent instead of panicking. The count is one byte,
                // not fc's varuint32, so SHiP requests are decoded by
                // `ship::wire` instead.
                let Some(&elements) = buffer.get(offset) else {
                    break;
                };
                let Some(payload) = size.checked_mul(usize::from(elements)) else {
                    break;
                };
                let Some(full_size) = payload.checked_add(1) else {
                    break;
                };
                let Some(end) = offset.checked_add(full_size) else {
                    break;
                };
                let Some(field_buffer) = buffer.get(offset..end) else {
                    break;
                };
                hash_map.insert(
                    field.to_string(),
                    ZCDField {
                        slice: field_buffer,
                        size: full_size,
                        f_type,
                    },
                );
                offset = end;
            }
            _ => {
                // unbounded fields are the last fields in the array
                if *size == 0 {
                    // unbounded fields at last index
                    if index == fields.len() - 1 {
                        // get data until the end of the buffer
                        let Some(field_buffer) = buffer.get(offset..) else {
                            break;
                        };
                        hash_map.insert(
                            field.to_string(),
                            ZCDField {
                                slice: field_buffer,
                                size: field_buffer.len(),
                                f_type,
                            },
                        );
                        break;
                    }
                } else {
                    // bounded fields
                    let Some(end) = offset.checked_add(*size) else {
                        break;
                    };
                    if end > buffer.len() {
                        break;
                    }
                    let Some(field_buffer) = buffer.get(offset..end) else {
                        break;
                    };
                    hash_map.insert(
                        field.to_string(),
                        ZCDField {
                            slice: field_buffer,
                            size: *size,
                            f_type,
                        },
                    );
                    offset = end;
                }
            }
        }
    }
    ZCD::from(buffer, hash_map)
}

pub fn deserialize_with<'a, 'b: 'a>(
    fields: &'b [(&ZCDType, &str, usize)],
    buffer: &'a [u8],
) -> ZCD<'a> {
    zcd_builder(buffer, fields)
}

/// Walk fixed-width status fields.
///
/// `optional_extension` names a trailing field that may be omitted entirely
/// (`get_status_result_v0`'s `chain_id`). A partial copy of that field is still
/// [`ZcdError::Malformed`]. Bytes past the last field are ignored.
fn decode_fixed_fields<'a>(
    buffer: &'a [u8],
    fields: &'static [(&'static ZCDType, &'static str, usize)],
    optional_extension: Option<&str>,
) -> Result<ZCD<'a>, ZcdError> {
    let mut offset = 0usize;
    let mut hash_map = HashMap::new();
    let last = fields.len().saturating_sub(1);

    for (index, &(f_type, field, size)) in fields.iter().enumerate() {
        let width = type_width(f_type).ok_or(ZcdError::Malformed {
            field,
            expected: size,
            actual: 0,
        })?;
        if width != size {
            return Err(ZcdError::Malformed {
                field,
                expected: width,
                actual: size,
            });
        }

        let remaining = buffer.len().saturating_sub(offset);
        if remaining == 0 && optional_extension == Some(field) && index == last {
            break;
        }
        if remaining < size {
            return Err(if remaining == 0 {
                ZcdError::Truncated {
                    field,
                    expected: size,
                    remaining,
                }
            } else {
                ZcdError::Malformed {
                    field,
                    expected: size,
                    actual: remaining,
                }
            });
        }

        let end = offset + size;
        hash_map.insert(
            field.to_string(),
            ZCDField {
                slice: &buffer[offset..end],
                size,
                f_type,
            },
        );
        offset = end;
    }

    Ok(ZCD::from(buffer, hash_map))
}

/// Decode a `get_status_result_v0` body (no leading result-variant byte).
///
/// `chain_id` is a binary extension, so a frame that ends immediately after
/// `chain_state_end_block` is valid. A frame that stops before a required
/// field, or that includes only part of a field, is returned as an error.
pub fn deserialize_status_result(buffer: &[u8]) -> Result<ZCD<'_>, ZcdError> {
    decode_fixed_fields(buffer, &STATUS_RESULT_V0_FIELDS, Some("chain_id"))
}

/// Decode a `get_status_result_v1` body (no leading result-variant byte).
///
/// `chain_id` and both finality-data bounds are required. A truncated or
/// partial field is an error; trailing bytes past the last field are ignored.
pub fn deserialize_status_result_v1(buffer: &[u8]) -> Result<ZCD<'_>, ZcdError> {
    decode_fixed_fields(buffer, &STATUS_RESULT_V1_FIELDS, None)
}

pub fn deserialize_result(buffer: &[u8]) -> ZCD<'_> {
    zcd_builder(buffer, &RESULT_V0_FIELDS)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn push_u32(buf: &mut Vec<u8>, value: u32) {
        buf.extend_from_slice(&value.to_le_bytes());
    }

    fn push_id(buf: &mut Vec<u8>, byte: u8) {
        buf.extend_from_slice(&[byte; 32]);
    }

    /// Fixed `get_status_result_v0` body. `chain_id` is optional.
    fn status_body(include_chain_id: bool) -> Vec<u8> {
        let mut buf = Vec::with_capacity(120);
        push_u32(&mut buf, 1000);
        push_id(&mut buf, 0x11);
        push_u32(&mut buf, 990);
        push_id(&mut buf, 0x22);
        push_u32(&mut buf, 10);
        push_u32(&mut buf, 1001);
        push_u32(&mut buf, 5);
        push_u32(&mut buf, 1000);
        if include_chain_id {
            push_id(&mut buf, 0xAB);
        }
        buf
    }

    /// v0 body plus both finality bounds.
    fn status_body_v1(begin: u32, end: u32) -> Vec<u8> {
        let mut buf = status_body(true);
        push_u32(&mut buf, begin);
        push_u32(&mut buf, end);
        buf
    }

    fn expect_u32(parsed: &ZCD<'_>, field: &str) -> u32 {
        match parsed.get(field).expect("field width") {
            Some(ZCDValues::U32(value)) => value,
            other => panic!("expected u32 {field}, got {other:?}"),
        }
    }

    #[test]
    fn status_frame_with_chain_id_parses() {
        let body = status_body(true);
        let parsed = deserialize_status_result(&body).expect("valid frame");
        assert_eq!(expect_u32(&parsed, "head_block_num"), 1000);
        assert_eq!(expect_u32(&parsed, "last_irreversible_block_num"), 990);
        assert_eq!(expect_u32(&parsed, "trace_begin_block"), 10);
        assert_eq!(expect_u32(&parsed, "trace_end_block"), 1001);
        assert_eq!(expect_u32(&parsed, "chain_state_begin_block"), 5);
        assert_eq!(expect_u32(&parsed, "chain_state_end_block"), 1000);
        match parsed.get("head_block_id").expect("id") {
            Some(ZCDValues::Checksum256(id)) => assert_eq!(id, [0x11; 32]),
            other => panic!("expected checksum, got {other:?}"),
        }
        match parsed.get("chain_id").expect("chain id") {
            Some(ZCDValues::Checksum256(id)) => assert_eq!(id, [0xAB; 32]),
            other => panic!("expected checksum, got {other:?}"),
        }
        assert!(matches!(
            parsed.get_type("head_block_num").expect("type"),
            ZCDType::U32
        ));
    }

    #[test]
    fn status_frame_without_chain_id_extension_parses() {
        let body = status_body(false);
        assert_eq!(body.len(), 88);
        let parsed = deserialize_status_result(&body).expect("extension may be omitted");
        assert_eq!(expect_u32(&parsed, "head_block_num"), 1000);
        assert_eq!(expect_u32(&parsed, "chain_state_end_block"), 1000);
        assert!(parsed.get("chain_id").expect("lookup").is_none());
    }

    #[test]
    fn trailing_bytes_after_complete_status_frame_are_ignored() {
        let mut body = status_body(true);
        body.push(0xFF);
        let parsed = deserialize_status_result(&body).expect("future extension bytes");
        assert_eq!(expect_u32(&parsed, "head_block_num"), 1000);
    }

    #[test]
    fn truncated_status_frames_return_errors() {
        let empty = deserialize_status_result(&[]).expect_err("empty");
        assert_eq!(
            empty,
            ZcdError::Truncated {
                field: "head_block_num",
                expected: 4,
                remaining: 0,
            }
        );

        // Ends on the head_block_num boundary, so head_block_id is missing entirely.
        let only_head = deserialize_status_result(&1000u32.to_le_bytes()).expect_err("cut");
        assert_eq!(
            only_head,
            ZcdError::Truncated {
                field: "head_block_id",
                expected: 32,
                remaining: 0,
            }
        );

        // Required prefix without the final chain-state word.
        let short = deserialize_status_result(&status_body(false)[..84]).expect_err("short");
        assert_eq!(
            short,
            ZcdError::Truncated {
                field: "chain_state_end_block",
                expected: 4,
                remaining: 0,
            }
        );
    }

    #[test]
    fn malformed_status_frames_return_errors() {
        // Three bytes of a four-byte block number.
        let partial_num = deserialize_status_result(&[1, 2, 3]).expect_err("partial u32");
        assert_eq!(
            partial_num,
            ZcdError::Malformed {
                field: "head_block_num",
                expected: 4,
                actual: 3,
            }
        );

        // Full head number plus a partial block id.
        let mut partial_id = 1000u32.to_le_bytes().to_vec();
        partial_id.extend_from_slice(&[0x11; 10]);
        let err = deserialize_status_result(&partial_id).expect_err("partial id");
        assert_eq!(
            err,
            ZcdError::Malformed {
                field: "head_block_id",
                expected: 32,
                actual: 10,
            }
        );

        // chain_id extension started but not finished.
        let mut partial_chain = status_body(false);
        partial_chain.extend_from_slice(&[0xAB; 12]);
        let err = deserialize_status_result(&partial_chain).expect_err("partial chain id");
        assert_eq!(
            err,
            ZcdError::Malformed {
                field: "chain_id",
                expected: 32,
                actual: 12,
            }
        );
    }

    #[test]
    fn short_slices_and_missing_fields_do_not_panic() {
        let short = ZCDField {
            slice: &[0, 1, 2],
            f_type: &U32,
            size: 3,
        };
        assert_eq!(
            short.as_u32().expect_err("u32"),
            ZcdError::Malformed {
                field: "u32",
                expected: 4,
                actual: 3,
            }
        );
        assert_eq!(
            ZCDField {
                slice: &[0; 31],
                f_type: &Checksum256,
                size: 31,
            }
            .as_checksum256()
            .expect_err("checksum"),
            ZcdError::Malformed {
                field: "checksum256",
                expected: 32,
                actual: 31,
            }
        );
        assert_eq!(
            ZCDField {
                slice: &[],
                f_type: &ZCDType::Bool,
                size: 0,
            }
            .as_bool()
            .expect_err("bool"),
            ZcdError::Malformed {
                field: "bool",
                expected: 1,
                actual: 0,
            }
        );

        let bytes = [0u8, 1, 2];
        let mut hash_map = HashMap::new();
        hash_map.insert(
            "head_block_num".to_string(),
            ZCDField {
                slice: &bytes,
                f_type: &U32,
                size: bytes.len(),
            },
        );
        let parsed = ZCD::from(&bytes, hash_map);
        assert_eq!(
            parsed.get("head_block_num").expect_err("get"),
            ZcdError::Malformed {
                field: "u32",
                expected: 4,
                actual: 3,
            }
        );
        assert!(parsed.get_type("missing").is_none());
    }

    #[test]
    fn status_v1_frame_parses_finality_range() {
        let body = status_body_v1(10, 500);
        let parsed = deserialize_status_result_v1(&body).expect("valid v1");
        assert_eq!(expect_u32(&parsed, "head_block_num"), 1000);
        assert_eq!(expect_u32(&parsed, "trace_begin_block"), 10);
        assert_eq!(expect_u32(&parsed, "trace_end_block"), 1001);
        assert_eq!(expect_u32(&parsed, "chain_state_begin_block"), 5);
        assert_eq!(expect_u32(&parsed, "chain_state_end_block"), 1000);
        assert_eq!(expect_u32(&parsed, "finality_data_begin_block"), 10);
        assert_eq!(expect_u32(&parsed, "finality_data_end_block"), 500);
        match parsed.get("chain_id").expect("chain id") {
            Some(ZCDValues::Checksum256(id)) => assert_eq!(id, [0xAB; 32]),
            other => panic!("expected checksum, got {other:?}"),
        }
    }

    #[test]
    fn status_v1_missing_finality_is_truncated() {
        let err = deserialize_status_result_v1(&status_body(true)).expect_err("missing finality");
        assert_eq!(
            err,
            ZcdError::Truncated {
                field: "finality_data_begin_block",
                expected: 4,
                remaining: 0,
            }
        );
    }

    #[test]
    fn status_v1_partial_finality_u32_is_malformed() {
        let mut body = status_body_v1(10, 500);
        body.truncate(body.len() - 2);
        let err = deserialize_status_result_v1(&body).expect_err("partial u32");
        assert_eq!(
            err,
            ZcdError::Malformed {
                field: "finality_data_end_block",
                expected: 4,
                actual: 2,
            }
        );
    }

    #[test]
    fn value_conversions_reject_mismatched_types() {
        assert_eq!(u8::try_from(ZCDValues::U8(3)).expect("u8"), 3);
        assert_eq!(
            u8::try_from(ZCDValues::U32(3)).expect_err("mismatch"),
            ZcdError::TypeMismatch
        );
        assert_eq!(u32::try_from(ZCDValues::U32(7)).expect("u32"), 7);
        assert_eq!(
            u32::try_from(ZCDValues::U8(7)).expect_err("mismatch"),
            ZcdError::TypeMismatch
        );
        assert_eq!(u64::try_from(ZCDValues::U64(9)).expect("u64"), 9);
        assert!(u64::try_from(ZCDValues::U32(9)).is_err());
        assert_eq!(u128::try_from(ZCDValues::U128(11)).expect("u128"), 11);
        assert!(u128::try_from(ZCDValues::U64(11)).is_err());
        assert_eq!(
            <[u8; 32]>::try_from(ZCDValues::Checksum256([4; 32])).expect("id"),
            [4; 32]
        );
        assert!(<[u8; 32]>::try_from(ZCDValues::Bytes(vec![4; 32])).is_err());
        assert_eq!(
            String::try_from(ZCDValues::String("ok".to_string())).expect("string"),
            "ok"
        );
        assert!(String::try_from(ZCDValues::Bytes(b"ok".to_vec())).is_err());
    }

    #[test]
    fn numeric_accessors_reject_wrong_widths() {
        assert_eq!(
            ZCDField {
                slice: &[0; 5],
                f_type: &U32,
                size: 5,
            }
            .as_u32()
            .expect_err("long u32"),
            ZcdError::Malformed {
                field: "u32",
                expected: 4,
                actual: 5,
            }
        );
        assert_eq!(
            ZCDField {
                slice: &[0; 7],
                f_type: &ZCDType::U64,
                size: 7,
            }
            .as_u64()
            .expect_err("short u64"),
            ZcdError::Malformed {
                field: "u64",
                expected: 8,
                actual: 7,
            }
        );
        assert_eq!(
            ZCDField {
                slice: &[9; 8],
                f_type: &ZCDType::U64,
                size: 8,
            }
            .as_u64()
            .expect("u64"),
            u64::from_le_bytes([9; 8])
        );
        assert_eq!(
            ZCDField {
                slice: &[0; 15],
                f_type: &ZCDType::U128,
                size: 15,
            }
            .as_u128()
            .expect_err("short u128"),
            ZcdError::Malformed {
                field: "u128",
                expected: 16,
                actual: 15,
            }
        );
        assert_eq!(
            ZCDField {
                slice: &[3; 16],
                f_type: &ZCDType::U128,
                size: 16,
            }
            .as_u128()
            .expect("u128"),
            u128::from_le_bytes([3; 16])
        );
        assert_eq!(
            ZCDField {
                slice: &[0; 33],
                f_type: &Checksum256,
                size: 33,
            }
            .as_checksum256()
            .expect_err("long checksum"),
            ZcdError::Malformed {
                field: "checksum256",
                expected: 32,
                actual: 33,
            }
        );
    }

    #[test]
    fn huge_element_counts_do_not_panic() {
        // 255 elements of 36 bytes each, with almost no payload.
        let table: [(&ZCDType, &str, usize); 2] =
            [(&U32, "prefix", 4), (&ZCDType::Array, "items", 36)];
        let mut short = 3u32.to_le_bytes().to_vec();
        short.extend_from_slice(&[255, 0, 0, 0, 0]);
        let parsed = deserialize_with(&table, &short);
        assert!(parsed.get("items").expect("lookup").is_none());
        assert_eq!(expect_u32(&parsed, "prefix"), 3);

        // Per-element size that overflows `size * elements` and `1 + payload`.
        let overflowing: [(&ZCDType, &str, usize); 1] =
            [(&ZCDType::Array, "items", usize::MAX / 2)];
        let parsed = deserialize_with(&overflowing, &[2, 1, 2, 3]);
        assert!(parsed.get("items").expect("lookup").is_none());

        let max_width: [(&ZCDType, &str, usize); 1] = [(&ZCDType::Array, "items", usize::MAX)];
        let parsed = deserialize_with(&max_width, &[1]);
        assert!(parsed.get("items").expect("lookup").is_none());

        // A consumed prefix plus a span of `usize::MAX` must not wrap the end.
        let shifted: [(&ZCDType, &str, usize); 2] = [
            (&ZCDType::U8, "prefix", 1),
            (&ZCDType::Array, "items", usize::MAX),
        ];
        let parsed = deserialize_with(&shifted, &[7, 1, 2, 3, 4]);
        match parsed.get("prefix").expect("prefix") {
            Some(ZCDValues::U8(value)) => assert_eq!(value, 7),
            other => panic!("expected u8, got {other:?}"),
        }
        assert!(parsed.get("items").expect("lookup").is_none());
    }

    #[test]
    fn arbitrary_bytes_never_panic() {
        let samples: &[&[u8]] = &[
            &[],
            &[0],
            &[255],
            &[255, 255, 255, 255],
            &[1, 2, 3],
            &[0; 12],
            &[255; 8],
        ];
        for sample in samples {
            exercise_decoder(sample);
        }

        let mut state = 0x1234_5678_9abc_def0u64;
        for _ in 0..256 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            let len = (state % 128) as usize;
            let mut buf = Vec::with_capacity(len);
            for _ in 0..len {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                buf.push((state & 0xff) as u8);
            }
            exercise_decoder(&buf);
        }
    }

    fn exercise_decoder(buf: &[u8]) {
        let tables: [&[(&ZCDType, &str, usize)]; 2] = [&RESULT_V0_FIELDS, &STATUS_RESULT_V0_FIELDS];
        for fields in tables {
            let parsed = deserialize_with(fields, buf);
            for (_, name, _) in fields {
                let _ = parsed.get(name);
                let _ = parsed.get_type(name);
            }
        }
        let _ = deserialize_result(buf);
        let _ = deserialize_status_result(buf);
        let _ = deserialize_status_result_v1(buf);

        let hostile: [(&ZCDType, &str, usize); 3] = [
            (&U8, "prefix", 1),
            (&Array, "items", usize::MAX / 2),
            (&U32, "tail", usize::MAX),
        ];
        let parsed = deserialize_with(&hostile, buf);
        let _ = parsed.get("prefix");
        let _ = parsed.get("items");
        let _ = parsed.get("tail");
        let _ = parsed.get_type("items");
    }
}
