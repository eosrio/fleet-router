//! Client-facing session helpers: the byte-capped buffer for client frames
//! that arrive while no upstream link is up, and the block number read from
//! upstream results for replay de-duplication.

use std::collections::VecDeque;

use tokio_tungstenite::tungstenite::Message;

/// Maximum payload bytes of client frames buffered per session while an upstream
/// is mid-failover. A frame counts as at least one byte, so an empty-frame flood
/// cannot grow the queue without bound. Exceeding the cap closes the session
/// (close code 1008) instead of dropping frames or pinning unbounded memory.
pub(super) const MAX_PENDING_CLIENT_BYTES: usize = 64 * 1024;

/// Client frames held during a failover window, accounted in bytes.
pub(super) struct PendingClientFrames {
    frames: VecDeque<Message>,
    bytes: usize,
}

impl PendingClientFrames {
    pub(super) fn new() -> Self {
        Self {
            frames: VecDeque::new(),
            bytes: 0,
        }
    }

    /// Bytes this frame contributes toward [`MAX_PENDING_CLIENT_BYTES`].
    pub(super) fn accounted(msg: &Message) -> usize {
        msg.len().max(1)
    }

    /// Bytes currently held.
    pub(super) fn buffered(&self) -> usize {
        self.bytes
    }

    /// Queue `msg` when it fits. Returns `false` without modifying the queue
    /// when the byte cap would be exceeded (the message is dropped).
    pub(super) fn try_push(&mut self, msg: Message) -> bool {
        let n = Self::accounted(&msg);
        if self.bytes.saturating_add(n) > MAX_PENDING_CLIENT_BYTES {
            return false;
        }
        self.bytes += n;
        self.frames.push_back(msg);
        true
    }

    pub(super) fn pop_front(&mut self) -> Option<Message> {
        let msg = self.frames.pop_front()?;
        self.bytes = self.bytes.saturating_sub(Self::accounted(&msg));
        Some(msg)
    }
}

/// Extract the `this_block.block_num` from a `get_blocks_result_v0/v1` frame, if
/// present. Layout: variant(1) + head(36) + lib(36) + this_block_flag(1) @73 +
/// block_num(4) @74. Returns `None` for any other frame (status, head keep-alive).
pub(super) fn extract_block_num(msg: &Message) -> Option<u32> {
    if let Message::Binary(bin) = msg {
        if bin.len() >= 78 && (bin[0] == 1 || bin[0] == 2) && bin[73] == 1 {
            if let Ok(bytes) = bin[74..78].try_into() {
                return Some(u32::from_le_bytes(bytes));
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binary(n: usize) -> Message {
        Message::binary(vec![0u8; n])
    }

    #[test]
    fn pending_buffer_accepts_frames_until_the_byte_cap() {
        let mut pending = PendingClientFrames::new();
        assert!(pending.try_push(binary(1024)));
        assert!(pending.try_push(binary(MAX_PENDING_CLIENT_BYTES - 1024)));
        assert_eq!(pending.bytes, MAX_PENDING_CLIENT_BYTES);
        assert_eq!(pending.frames.len(), 2);
        assert!(!pending.try_push(binary(1)));
        assert_eq!(pending.frames.len(), 2);
        assert_eq!(pending.bytes, MAX_PENDING_CLIENT_BYTES);
    }

    #[test]
    fn pending_buffer_rejects_a_single_oversized_frame() {
        let mut pending = PendingClientFrames::new();
        assert!(!pending.try_push(binary(MAX_PENDING_CLIENT_BYTES + 1)));
        assert!(pending.frames.is_empty());
        assert_eq!(pending.bytes, 0);
        assert!(pending.try_push(binary(1)));
    }

    #[test]
    fn pending_buffer_exact_cap_fits_and_pop_frees_budget() {
        let mut pending = PendingClientFrames::new();
        assert!(pending.try_push(binary(MAX_PENDING_CLIENT_BYTES)));
        assert!(!pending.try_push(binary(1)));
        let got = pending.pop_front().unwrap();
        assert_eq!(got.len(), MAX_PENDING_CLIENT_BYTES);
        assert_eq!(pending.bytes, 0);
        assert!(pending.pop_front().is_none());
        assert!(pending.try_push(binary(MAX_PENDING_CLIENT_BYTES)));
    }

    #[test]
    fn pending_buffer_counts_empty_frames_as_one_byte() {
        let mut pending = PendingClientFrames::new();
        assert!(pending.try_push(binary(MAX_PENDING_CLIENT_BYTES - 1)));
        assert!(pending.try_push(Message::binary(Vec::<u8>::new())));
        assert_eq!(pending.bytes, MAX_PENDING_CLIENT_BYTES);
        assert!(!pending.try_push(Message::text("")));
        assert_eq!(pending.frames.len(), 2);
    }

    #[test]
    fn pending_buffer_counts_text_and_binary_payloads() {
        let mut pending = PendingClientFrames::new();
        assert!(pending.try_push(Message::text("abcd")));
        assert!(pending.try_push(Message::binary(vec![1, 2, 3])));
        assert_eq!(pending.bytes, 7);
        let first = pending.pop_front().unwrap();
        assert_eq!(first.len(), 4);
        assert_eq!(pending.bytes, 3);
    }

    #[test]
    fn pending_buffer_reports_its_buffered_bytes() {
        let mut pending = PendingClientFrames::new();
        assert_eq!(pending.buffered(), 0);
        assert!(pending.try_push(binary(10)));
        assert!(pending.try_push(Message::binary(Vec::<u8>::new())));
        assert_eq!(pending.buffered(), 11);
        pending.pop_front();
        assert_eq!(pending.buffered(), 1);
    }

    /// Result frame with `variant`, zeroed head/LIB and an optional `this_block`.
    fn result_frame(variant: u8, this_block: Option<u32>) -> Message {
        let mut buf = vec![variant];
        buf.extend_from_slice(&[0u8; 72]); // head + last_irreversible
        match this_block {
            Some(num) => {
                buf.push(1);
                buf.extend_from_slice(&num.to_le_bytes());
                buf.extend_from_slice(&[0u8; 32]);
            }
            None => buf.push(0),
        }
        buf.extend_from_slice(&[0u8; 4]); // prev_block, block, traces, deltas absent
        Message::binary(buf)
    }

    #[test]
    fn block_num_is_read_from_v0_and_v1_results() {
        assert_eq!(extract_block_num(&result_frame(1, Some(115))), Some(115));
        assert_eq!(extract_block_num(&result_frame(2, Some(7))), Some(7));
    }

    #[test]
    fn frames_without_this_block_have_no_block_num() {
        // Head keep-alive: a result with no this_block.
        assert_eq!(extract_block_num(&result_frame(1, None)), None);
        // Status results and other variants are not block results.
        assert_eq!(extract_block_num(&result_frame(0, Some(115))), None);
        assert_eq!(extract_block_num(&result_frame(3, Some(115))), None);
        // Too short to hold this_block.
        assert_eq!(extract_block_num(&Message::binary(vec![1u8; 77])), None);
        assert_eq!(extract_block_num(&Message::text("{}")), None);
    }
}
