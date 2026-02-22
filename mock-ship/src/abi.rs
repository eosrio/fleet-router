/// The SHiP ABI JSON from Spring v1.2.2 (AntelopeIO/spring abi.cpp).
/// This is the exact string that a real nodeos sends as the first Text frame
/// upon WebSocket connection. The mock server sends this verbatim.
pub const SHIP_ABI_JSON: &str = include_str!("ship_abi.json");
