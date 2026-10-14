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
//! retain it. Monitors keep the ABI *text* only as the "already saw the
//! handshake" flag, which they already did.

use std::ffi::CString;

use rs_abieos::{AbiHandle, AbieosError};

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

    use super::{accept_upstream_abi, parse_upstream_abi};

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
}
