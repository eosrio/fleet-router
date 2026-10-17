//! Per-check validation of an upstream SHiP ABI.
//!
//! Client handshakes, failovers, and upstream monitors used to load every ABI
//! into one process-wide [`rs_abieos::Abieos`] under account `"0"`. That type
//! is `Send` but not `Sync`, so the context sat in an `Arc<Mutex<_>>` cloned
//! onto every connection and every monitor. Upstreams that serve different
//! ABIs could overwrite slot `0` between the unlock and any later use.
//!
//! Fleet-router never decodes against the loaded ABI; the data path reads raw
//! byte offsets, and the handshake only checks that the JSON parses. On the
//! rust backend, [`rs_abieos::AbiHandle::from_json`] is that check: it runs
//! `AbiDef::from_json_str` and `Abi::from_def`, the same steps as `set_abi`,
//! and it does not need a context or an account slot.
//!
//! A fresh [`Abieos`] per connection would also remove the mutex, but it would
//! keep a context plus the parsed model for the life of the session. A SHiP
//! ABI is tens of kilobytes of JSON and a full type table. The handle is
//! dropped as soon as the parse result is known, so a connection does not
//! retain it.
//!
//! Monitors also record each upstream's ABI as an [`AbiEntry`]: the text, a
//! fingerprint that ignores key order and whitespace, and whether its
//! `request` / `result` variants line up with Spring's. Two upstreams with the
//! same fingerprint send the same ABI. A mismatch is reported, never rejected.

use std::ffi::CString;
use std::sync::Arc;

use rs_abieos::{AbiHandle, AbieosError};
use serde_json::Value;

/// Spring's `request` variant, in index order. Older nodes serve a prefix.
const SPRING_REQUEST_TYPES: [&str; 5] = [
    "get_status_request_v0",
    "get_blocks_request_v0",
    "get_blocks_ack_request_v0",
    "get_blocks_request_v1",
    "get_status_request_v1",
];

/// Spring's `result` variant, in index order. Older nodes serve a prefix.
const SPRING_RESULT_TYPES: [&str; 4] = [
    "get_status_result_v0",
    "get_blocks_result_v0",
    "get_blocks_result_v1",
    "get_status_result_v1",
];

/// One upstream's handshake ABI, as recorded by its monitor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AbiEntry {
    /// The exact text the upstream sent.
    pub text: Arc<str>,
    /// [`abi_fingerprint`] of `text`.
    pub fp: u64,
    /// [`abi_compatible`] of `text`.
    pub compatible: bool,
}

impl AbiEntry {
    /// Fingerprint and check `text`, parsing the JSON once.
    pub fn new(text: &str) -> AbiEntry {
        let parsed = serde_json::from_str::<Value>(text).ok();
        let (fp, compatible) = match &parsed {
            Some(value) => (fingerprint_value(value), compatible_value(value)),
            None => (fnv1a64(text.as_bytes()), false),
        };
        AbiEntry {
            text: Arc::from(text),
            fp,
            compatible,
        }
    }

    /// The fingerprint as the 16 lowercase hex digits used in logs and labels.
    pub fn fp_hex(&self) -> String {
        format!("{:016x}", self.fp)
    }
}

/// 64-bit FNV-1a.
pub(crate) fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(OFFSET, |hash, &byte| {
        (hash ^ u64::from(byte)).wrapping_mul(PRIME)
    })
}

/// Fingerprint of an ABI text that ignores key order and whitespace.
///
/// The text is parsed as JSON and written back with object keys sorted, then
/// hashed with [`fnv1a64`]. Text that is not JSON is hashed as raw bytes.
// For a session that fingerprints a dialed upstream; the monitor uses AbiEntry.
#[cfg_attr(not(test), allow(dead_code))]
pub fn abi_fingerprint(text: &str) -> u64 {
    match serde_json::from_str::<Value>(text) {
        Ok(value) => fingerprint_value(&value),
        Err(_) => fnv1a64(text.as_bytes()),
    }
}

/// Whether the ABI's `request` and `result` variants are prefixes of Spring's.
///
/// Every node from EOSIO 2.0 to Spring appends to these lists and never
/// reorders them, so a prefix keeps the variant indices the router sends and
/// reads. A missing, empty or reordered list is not compatible.
// See abi_fingerprint.
#[cfg_attr(not(test), allow(dead_code))]
pub fn abi_compatible(text: &str) -> bool {
    serde_json::from_str::<Value>(text).is_ok_and(|value| compatible_value(&value))
}

fn fingerprint_value(value: &Value) -> u64 {
    let mut canonical = Vec::new();
    write_canonical(value, &mut canonical);
    fnv1a64(&canonical)
}

/// Compact JSON with object keys in byte order, whatever map type serde_json
/// was built with.
fn write_canonical(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            out.push(b'{');
            for (i, (key, item)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                // Serializing a string or a scalar into a Vec cannot fail.
                serde_json::to_writer(&mut *out, key).expect("string key");
                out.push(b':');
                write_canonical(item, out);
            }
            out.push(b'}');
        }
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canonical(item, out);
            }
            out.push(b']');
        }
        scalar => serde_json::to_writer(&mut *out, scalar).expect("scalar"),
    }
}

fn compatible_value(value: &Value) -> bool {
    variant_is_prefix(value, "request", &SPRING_REQUEST_TYPES)
        && variant_is_prefix(value, "result", &SPRING_RESULT_TYPES)
}

fn variant_is_prefix(abi: &Value, name: &str, spring: &[&str]) -> bool {
    let Some(variants) = abi.get("variants").and_then(Value::as_array) else {
        return false;
    };
    let Some(types) = variants
        .iter()
        .find(|v| v.get("name").and_then(Value::as_str) == Some(name))
        .and_then(|v| v.get("types"))
        .and_then(Value::as_array)
    else {
        return false;
    };
    !types.is_empty()
        && types.len() <= spring.len()
        && types
            .iter()
            .zip(spring)
            .all(|(have, want)| have.as_str() == Some(*want))
}

/// Parse `abi_json` into a handle owned by the caller.
///
/// Production checks use [`accept_upstream_abi`], which drops the handle.
/// Tests hold the handle to show that two parses do not share a slot.
pub(crate) fn parse_upstream_abi(abi_json: &str) -> Result<AbiHandle, AbieosError> {
    AbiHandle::from_json(abi_json)
}

/// Parse `abi_json` and drop the model before returning.
///
/// An interior NUL is rejected before the parse, matching `set_abi_json` /
/// `set_abi_json_native` (`CString::new` fails, and those methods return
/// [`AbieosError::SetAbi`]). On the rust backend a failed `set_abi*` is
/// `Err`, never `Ok(false)`.
pub(crate) fn accept_upstream_abi(abi_json: &str) -> Result<(), AbieosError> {
    CString::new(abi_json).map_err(|_| AbieosError::SetAbi("interior NUL byte in input".into()))?;
    parse_upstream_abi(abi_json).map(|_| ())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::{
        abi_compatible, abi_fingerprint, accept_upstream_abi, fnv1a64, parse_upstream_abi, AbiEntry,
    };

    const SHIP_ABI_JSON: &str = include_str!("../mock-ship/src/ship_abi.json");

    fn eosio_name(s: &str) -> u64 {
        fn symbol(c: u8) -> u64 {
            match c {
                b'a'..=b'z' => (c - b'a') as u64 + 6,
                b'1'..=b'5' => (c - b'1') as u64 + 1,
                _ => 0,
            }
        }
        let bytes = s.as_bytes();
        let mut name = 0u64;
        for (i, &c) in bytes.iter().take(12).enumerate() {
            name |= (symbol(c) & 0x1f) << (64 - 5 * (i + 1));
        }
        if bytes.len() >= 13 {
            name |= symbol(bytes[12]) & 0x0f;
        }
        name
    }

    fn abi_with_action(action: &str) -> String {
        format!(
            r#"{{"version":"eosio::abi/1.1","actions":[{{"name":"{action}","type":"uint32","ricardian_contract":""}}]}}"#
        )
    }

    #[test]
    fn ship_abi_parses_and_garbage_does_not() {
        accept_upstream_abi(SHIP_ABI_JSON).expect("Spring SHiP ABI");
        assert!(accept_upstream_abi("{").is_err());
        assert!(accept_upstream_abi(r#"{"version":"nope"}"#).is_err());
        assert!(accept_upstream_abi("not json").is_err());
    }

    #[test]
    fn interior_nul_is_rejected_like_set_abi() {
        let err = accept_upstream_abi("{\"version\":\"eosio::abi/1.1\"\0}").unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("interior NUL"),
            "unexpected error: {message}"
        );
    }

    /// Each parse owns its ABI. A shared context keyed by account `0` could
    /// not keep both action names visible at once.
    #[test]
    fn concurrent_parses_each_own_their_abi() {
        const N: usize = 8;
        let barrier = Arc::new(Barrier::new(N));
        let mut threads = Vec::with_capacity(N);
        for i in 0..N {
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                let action = if i % 2 == 0 { "acta" } else { "actb" };
                let json = abi_with_action(action);
                barrier.wait();
                let handle = parse_upstream_abi(&json).expect("abi");
                (action, handle)
            }));
        }
        let owned: Vec<_> = threads
            .into_iter()
            .map(|t| t.join().expect("thread"))
            .collect();
        for (action, handle) in &owned {
            let other = if *action == "acta" { "actb" } else { "acta" };
            assert_eq!(
                handle.type_for_action(eosio_name(action)),
                Some("uint32"),
                "{action} must still resolve on its own handle"
            );
            assert_eq!(handle.type_for_action(eosio_name(other)), None);
        }
    }

    #[test]
    fn accept_does_not_keep_a_handle_and_matches_parse() {
        let json = abi_with_action("acta");
        accept_upstream_abi(&json).expect("valid");
        let handle = parse_upstream_abi(&json).expect("valid");
        assert_eq!(handle.type_for_action(eosio_name("acta")), Some("uint32"));
    }

    #[test]
    fn fnv1a64_matches_reference_vectors() {
        assert_eq!(fnv1a64(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a64(b"foobar"), 0x8594_4171_f739_67e8);
    }

    #[test]
    fn fingerprint_ignores_key_order_and_whitespace() {
        let compact = r#"{"version":"eosio::abi/1.1","structs":[{"name":"a","fields":[]}]}"#;
        let reordered = "{\n  \"structs\" : [ { \"fields\": [], \"name\": \"a\" } ],\n  \"version\": \"eosio::abi/1.1\"\n}";
        assert_eq!(abi_fingerprint(compact), abi_fingerprint(reordered));
        // The canonical form is compact JSON with sorted keys.
        assert_eq!(
            abi_fingerprint(compact),
            fnv1a64(br#"{"structs":[{"fields":[],"name":"a"}],"version":"eosio::abi/1.1"}"#)
        );

        let changed = r#"{"version":"eosio::abi/1.1","structs":[{"name":"b","fields":[]}]}"#;
        assert_ne!(abi_fingerprint(compact), abi_fingerprint(changed));
        // Array order is content, not formatting.
        let two = r#"{"types":["x","y"]}"#;
        let swapped = r#"{"types":["y","x"]}"#;
        assert_ne!(abi_fingerprint(two), abi_fingerprint(swapped));
    }

    #[test]
    fn fingerprint_of_non_json_hashes_the_raw_text() {
        assert_eq!(abi_fingerprint("not json"), fnv1a64(b"not json"));
        assert_eq!(abi_fingerprint("{"), fnv1a64(b"{"));
    }

    #[test]
    fn spring_and_older_ship_abis_are_compatible() {
        assert!(abi_compatible(SHIP_ABI_JSON));

        // EOSIO 2.0 / Leap 3: no v1 request or result types yet.
        let legacy = r#"{"version":"eosio::abi/1.1","variants":[
            {"name":"request","types":["get_status_request_v0","get_blocks_request_v0","get_blocks_ack_request_v0"]},
            {"name":"result","types":["get_status_result_v0","get_blocks_result_v0"]}]}"#;
        assert!(abi_compatible(legacy));
    }

    #[test]
    fn reordered_missing_or_extended_variants_are_not_compatible() {
        let variants = |request: &str, result: &str| {
            format!(
                r#"{{"version":"eosio::abi/1.1","variants":[{{"name":"request","types":[{request}]}},{{"name":"result","types":[{result}]}}]}}"#
            )
        };
        let request = r#""get_status_request_v0","get_blocks_request_v0""#;
        let result = r#""get_status_result_v0","get_blocks_result_v0""#;
        assert!(abi_compatible(&variants(request, result)));

        let swapped = r#""get_blocks_request_v0","get_status_request_v0""#;
        assert!(!abi_compatible(&variants(swapped, result)));
        assert!(!abi_compatible(&variants(request, "")));
        let unknown = r#""get_status_result_v0","get_blocks_result_v0","get_blocks_result_v1","get_status_result_v1","get_status_result_v2""#;
        assert!(!abi_compatible(&variants(request, unknown)));

        assert!(!abi_compatible(r#"{"version":"eosio::abi/1.1"}"#));
        assert!(!abi_compatible(
            r#"{"version":"eosio::abi/1.1","variants":[{"name":"request","types":["get_status_request_v0"]}]}"#
        ));
        assert!(!abi_compatible("not json"));
    }

    #[test]
    fn abi_entry_keeps_the_text_and_matches_the_free_functions() {
        let entry = AbiEntry::new(SHIP_ABI_JSON);
        assert_eq!(&*entry.text, SHIP_ABI_JSON);
        assert_eq!(entry.fp, abi_fingerprint(SHIP_ABI_JSON));
        assert!(entry.compatible);
        assert_eq!(entry.fp_hex().len(), 16);
        assert_eq!(entry.fp_hex(), format!("{:016x}", entry.fp));

        let garbage = AbiEntry::new("{");
        assert_eq!(garbage.fp, fnv1a64(b"{"));
        assert!(!garbage.compatible);
    }
}
