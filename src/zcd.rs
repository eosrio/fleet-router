// ZCD Deserialization (Zero Copy Deserialization)
// This module is used to deserialize the binary data received from the WebSocket
// server without copying the data to a new buffer
// Deserialized objects are composed of references to the original buffer
#![allow(dead_code)]

use std::collections::HashMap;
use std::fmt::Display;
use ZCDType::{Bytes, Checksum256, U32, U8};

use crate::functions::buffer_to_hex;
use crate::zcd::ZCDType::{Array, Bool};

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

impl From<ZCDValues> for u8 {
    fn from(value: ZCDValues) -> u8 {
        match value {
            ZCDValues::U8(v) => v,
            _ => panic!("Invalid conversion"),
        }
    }
}

impl From<ZCDValues> for u32 {
    fn from(value: ZCDValues) -> u32 {
        match value {
            ZCDValues::U32(v) => v,
            _ => panic!("Invalid conversion"),
        }
    }
}

impl From<ZCDValues> for u64 {
    fn from(value: ZCDValues) -> u64 {
        match value {
            ZCDValues::U64(v) => v,
            _ => panic!("Invalid conversion"),
        }
    }
}

impl From<ZCDValues> for u128 {
    fn from(value: ZCDValues) -> u128 {
        match value {
            ZCDValues::U128(v) => v,
            _ => panic!("Invalid conversion"),
        }
    }
}

impl From<ZCDValues> for [u8; 32] {
    fn from(value: ZCDValues) -> [u8; 32] {
        match value {
            ZCDValues::Checksum256(v) => v,
            _ => panic!("Invalid conversion"),
        }
    }
}

impl From<ZCDValues> for String {
    fn from(value: ZCDValues) -> String {
        match value {
            ZCDValues::String(v) => v,
            _ => panic!("Invalid conversion"),
        }
    }
}

impl Into<ZCDValues> for u8 {
    fn into(self) -> ZCDValues {
        ZCDValues::U8(self)
    }
}

#[derive(Debug)]
pub struct ZCDField<'a> {
    pub slice: &'a [u8],
    pub f_type: &'a ZCDType,
    pub size: usize,
}

impl ZCDField<'_> {
    pub fn as_bool(&self) -> bool {
        self.slice[0] != 0
    }

    pub fn as_u8(&self) -> u8 {
        self.slice[0]
    }

    pub fn as_u32(&self) -> u32 {
        u32::from_le_bytes(self.slice.try_into().unwrap())
    }

    pub fn as_u64(&self) -> u64 {
        u64::from_le_bytes(self.slice.try_into().unwrap())
    }

    pub fn as_u128(&self) -> u128 {
        u128::from_le_bytes(self.slice.try_into().unwrap())
    }

    pub fn as_checksum256(&self) -> [u8; 32] {
        self.slice.try_into().unwrap()
    }

    pub fn as_checksum256_hex(&self) -> String {
        buffer_to_hex(self.slice.to_vec())
    }

    pub fn as_string(&self) -> String {
        String::from_utf8_lossy(self.slice).to_string()
    }
}

#[derive(Debug)]
pub struct ZCD<'a> {
    pub buffer: &'a [u8],
    pub hash_map: HashMap<String, ZCDField<'a>>,
}

impl ZCD<'_> {
    pub fn from<'a>(buffer: &'a [u8], hash_map: HashMap<String, ZCDField<'a>>) -> ZCD<'a> {
        ZCD { buffer, hash_map }
    }

    pub fn get_type(&self, field: &str) -> &ZCDType {
        self.hash_map.get(field).unwrap().f_type
    }

    pub fn get(&self, field: &str) -> Option<ZCDValues> {
        match self.hash_map.get(field) {
            None => None,
            Some(f) => match f.f_type {
                ZCDType::Bool => Some(ZCDValues::Bool(f.as_bool())),
                ZCDType::U8 => Some(ZCDValues::U8(f.as_u8())),
                ZCDType::U32 => Some(ZCDValues::U32(f.as_u32())),
                ZCDType::U64 => Some(ZCDValues::U64(f.as_u64())),
                ZCDType::U128 => Some(ZCDValues::U128(f.as_u128())),
                ZCDType::Checksum256 => Some(ZCDValues::Checksum256(f.as_checksum256())),
                ZCDType::String => Some(ZCDValues::String(f.as_string())),
                ZCDType::Bytes => Some(ZCDValues::Bytes(f.slice.to_vec())),
                ZCDType::Array => Some(ZCDValues::Array(f.slice.to_vec()))
            },
        }
    }
}

pub const GET_BLOCKS_REQUEST_V0_FIELDS: [(&ZCDType, &str, usize); 8] = [
    (&U32, "start_block_num", 4),
    (&U32, "end_block_num", 4),
    (&U32, "max_messages_in_flight", 4),
    (&Array, "have_positions", 4 + 32),
    (&Bool, "irreversible_only", 1),
    (&Bool, "fetch_block", 1),
    (&Bool, "fetch_traces", 1),
    (&Bool, "fetch_deltas", 1),
];

pub const GET_BLOCKS_ACK_REQUEST_V0_FIELDS: [(&ZCDType, &str, usize); 1] =
    [(&U32, "num_messages", 4)];

// Field Name, Field Size, is bounded
pub const RESULT_V0_FIELDS: [(&'static ZCDType, &str, usize); 2] =
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

fn zcd_builder<'a>(buffer: &'a [u8], fields: &'a [(&ZCDType, &str, usize)]) -> ZCD<'a> {
    let mut offset = 0;
    let mut hash_map = HashMap::new();
    let mut index = 0;
    for (f_type, field, size) in fields.iter() {
        match f_type {
            Array => {
                // unbounded fields in the middle of the array
                let elements = buffer[offset];
                let full_size = 1 + size * elements as usize;
                let field_buffer = &buffer[offset..offset + full_size];
                hash_map.insert(
                    field.to_string(),
                    ZCDField {
                        slice: field_buffer,
                        size: full_size,
                        f_type: *f_type,
                    },
                );
                offset += full_size;
            }
            _ => {
                // unbounded fields are the last fields in the array
                if *size == 0 {
                    // unbounded fields at last index
                    if index == fields.len() - 1 {
                        // get data until the end of the buffer
                        let field_buffer = &buffer[offset..];
                        hash_map.insert(
                            field.to_string(),
                            ZCDField {
                                slice: field_buffer,
                                size: buffer.len() - offset,
                                f_type: *f_type,
                            },
                        );
                        break;
                    }
                } else {
                    // bounded fields
                    let field_buffer = &buffer[offset..offset + size];
                    hash_map.insert(
                        field.to_string(),
                        ZCDField {
                            slice: field_buffer,
                            size: *size,
                            f_type: *f_type,
                        },
                    );
                    offset += size;
                }
            }
        }
        index += 1;
    }
    ZCD::from(buffer, hash_map)
}

pub fn deserialize_with<'a, 'b: 'a>(
    fields: &'b [(&ZCDType, &str, usize)],
    buffer: &'a [u8],
) -> ZCD<'a> {
    zcd_builder(buffer, fields)
}

pub fn deserialize_status_result(buffer: &[u8]) -> ZCD {
    zcd_builder(buffer, &STATUS_RESULT_V0_FIELDS)
}

pub fn deserialize_result(buffer: &[u8]) -> ZCD {
    zcd_builder(buffer, &RESULT_V0_FIELDS)
}
