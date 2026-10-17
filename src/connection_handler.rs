//! The client session moved to [`crate::session`]. This module keeps the
//! paths the accept loop and the `ServerState` docs still use.

pub use crate::session::handle_client;
// Only referenced from the `ServerState::connections` doc link.
#[allow(unused_imports)]
pub use crate::session::ConnectionGuard;
