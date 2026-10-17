//! An opt-in, nodeos-compatible HTTP chain API in front of a listener's
//! servers.
//!
//! Block endpoints are routed by block number. An irreversible block goes to
//! the frozen range server whose block log holds it, falling back to live
//! nodes; a reversible block goes to live nodes only, because fork answers
//! must come from one node's fork database. `get_info` and read-only state
//! calls are proxied unchanged to live nodes; push and exec are off unless
//! allowed. The router never rewrites a response body and never invents a
//! parameter error or an Unknown Block: when no upstream can answer it says
//! Busy, in nodeos's error shape.
//!
//! The decisions are pure functions, kept apart from the runtime:
//! - [`route`] classifies the raw request-target the way nodeos dispatches it;
//! - [`hint`] reads the block number from a body, only when nodeos would read
//!   the same block from it;
//! - [`pick`] decides a block's region, orders the candidate upstream APIs and
//!   classifies each attempt's answer;
//! - [`errors`] holds the bodies the router writes itself.

// The listener that consumes these modules is built on top of them; until it
// is wired, only the tests use them.
#![cfg_attr(not(test), allow(dead_code))]

pub mod errors;
pub mod hint;
pub mod pick;
pub mod route;
