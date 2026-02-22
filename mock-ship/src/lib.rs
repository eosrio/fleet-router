// Mock SHiP (State History Plugin) Server
// Implements the Spring v1.2.2 SHiP WebSocket protocol for testing fleet-router.
//
// Protocol flow:
//   1. Server accepts WebSocket connection
//   2. Server sends ABI JSON as a Text frame
//   3. Server switches to Binary mode
//   4. Client sends `state_request` variants (fc::raw packed)
//   5. Server responds with `state_result` variants (fc::raw packed)

mod abi;
mod protocol;
mod server;

pub use server::{MockShipConfig, MockShipServer};
