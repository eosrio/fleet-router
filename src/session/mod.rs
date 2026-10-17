//! Client session actor: one task per client connection that owns the client
//! socket, the upstream link, dials and timers, and mirrors the credit and
//! cursor state of a single SHiP node.
