// Mock SHiP (State History Plugin) Server
// Implements the Spring v1.2.2 SHiP WebSocket protocol for testing fleet-router.
//
// Protocol flow:
//   1. Server accepts WebSocket connection
//   2. Server sends ABI JSON as a Text frame
//   3. Server switches to Binary mode
//   4. Client sends `state_request` variants (fc::raw packed)
//   5. Server responds with `state_result` variants (fc::raw packed)
//
// `MockShipConfig::live` and `MockShipConfig::frozen` give Spring-faithful
// live nodes and frozen range servers. `client` builds requests and parses
// results for tests.
//
// `chain_api::MockChainApi::live` and `::frozen` give the HTTP chain API of
// a nodeos live node and of a frozen range server.

pub mod chain_api;
pub mod client;
pub mod protocol;

mod abi;
pub use abi::SHIP_ABI_JSON;
mod clock;
pub use clock::ChainClock;
mod server;

pub use protocol::{fake_block_id, fake_block_id_seeded};
pub use server::{
    serve_websocket, EndOfRange, Fidelity, MockShipConfig, MockShipHandle, MockShipServer,
    Observed, StatusFault,
};
