//! Zero-copy, bounded-buffer, vectored RFC 6455 WebSocket data-plane stream.
//!
//! Replaces `tungstenite::protocol::frame::FrameCodec` on the hot SHiP proxy
//! data path after the HTTP 101 Upgrade handshake completes:
//! - Reads into uninitialized `BytesMut` spare capacity via `AsyncReadExt::read_buf`
//!   (zero `memset(0)` on `WouldBlock` or partial reads).
//! - 100% cancel-safe `read_message()` inside `tokio::select!` (small frames
//!   keep their header intact in `in_buf` until complete; spike frames resume
//!   via `self.large_read`; no async writes occur inside `read_message()`).
//! - Adaptive read buffer: starts at `<= 4 KiB` while idle and scales up to
//!   `read_buf_cap` (`64 KiB` upstream) once saturated. Frames `<= 2 MiB` reuse
//!   `in_buf` across back-to-back reads (avoiding per-block `VirtualAlloc`/`mmap`
//!   and page faults) and shrink back to `read_buf_cap` after 2 s idle. Larger
//!   spike frames (`> 2 MiB`) read into a dedicated exact-sized `BytesMut` that
//!   frees immediately on drop (zero permanent high-water-mark retention).
//! - Writes frames via `write_vectored` (`IoSlice`) with inline 72-byte SHiP
//!   `head`/`lib` patching (zero payload copy), plus bounded 64 KiB small-frame
//!   coalescing with cursor advancement (zero `Vec::drain` memmove).

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{ready, Context, Poll};
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::error::{CapacityError, ProtocolError};
use tokio_tungstenite::tungstenite::protocol::frame::coding::{CloseCode, Data};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

use crate::ship::BlockPos;

/// Maximum coalesced write batch size for small Server -> Client frames.
const MAX_COALESCE_BUF: usize = 64 * 1024;
/// Frames up to this size are coalesced into `out_buf`; larger frames are
/// written directly via zero-copy `write_vectored`.
const SMALL_COALESCE_THRESHOLD: usize = 16 * 1024;
/// Frames up to 2 MiB reuse `in_buf` across back-to-back reads and shrink back
/// to `read_buf_cap` once idle for [`LARGE_BUF_IDLE_SHRINK`]. Larger spike
/// frames (e.g. 6 MiB) use a dedicated exact-sized `BytesMut` freed immediately
/// on drop.
const MAX_REUSABLE_READ_CAP: usize = 2 * 1024 * 1024;
const LARGE_BUF_IDLE_SHRINK: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WsRole {
    Server,
    Client,
}

#[derive(Clone, Copy, Debug)]
pub struct FastWsConfig {
    pub role: WsRole,
    pub read_buf_cap: usize,
    pub max_message_size: usize,
    pub max_frame_size: usize,
}

#[derive(Clone, Copy, Debug)]
struct FrameHdr {
    fin: bool,
    opcode: u8,
    masked: bool,
    mask: [u8; 4],
    payload_len: usize,
    hdr_len: usize,
}

/// Generate a non-zero per-connection initial masking seed without syscalls.
fn initial_mask_seed() -> u32 {
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let mixed = RandomState::new().hash_one(n);
    let seed = (mixed as u32) ^ ((mixed >> 32) as u32);
    if seed == 0 {
        0x9e37_79b9
    } else {
        seed
    }
}

/// Zero-copy WebSocket framing wrapper over any `AsyncRead + AsyncWrite + Unpin`.
pub struct FastWs<S> {
    stream: S,
    cfg: FastWsConfig,
    /// Current target read buffer size: starts at `<= 4 KiB` while idle and
    /// ramps up to `cfg.read_buf_cap` automatically once a stream saturates.
    active_read_cap: usize,
    in_buf: BytesMut,
    /// In-progress spike frame (`> MAX_REUSABLE_READ_CAP`), stored on `self`
    /// so `read_message()` is 100% cancel-safe inside `tokio::select!`.
    large_read: Option<(FrameHdr, BytesMut)>,
    /// Deadline at which an expanded `in_buf` (`> 2 * read_buf_cap`) will
    /// shrink back to `read_buf_cap` if no large frames arrived since.
    shrink_at: Option<Instant>,
    /// Small-frame coalescing buffer (bounded `<= MAX_COALESCE_BUF`, starts empty).
    out_buf: Vec<u8>,
    out_pos: usize,
    /// True if a direct vectored write occurred since the last `stream.flush()`.
    needs_flush: bool,
    /// Reassembly state for fragmented data messages (`FIN = 0`).
    frag_opcode: Option<u8>,
    frag_buf: Vec<u8>,
    /// Per-connection masking PRNG state (xorshift32) for `Client` frames.
    mask_state: u32,
}

impl<S> FastWs<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(stream: S, cfg: FastWsConfig) -> Self {
        let initial = cfg.read_buf_cap.clamp(1024, 4096);
        Self {
            stream,
            cfg,
            active_read_cap: initial,
            in_buf: BytesMut::with_capacity(initial),
            large_read: None,
            shrink_at: None,
            out_buf: Vec::new(),
            out_pos: 0,
            needs_flush: false,
            frag_opcode: None,
            frag_buf: Vec::new(),
            mask_state: initial_mask_seed(),
        }
    }

    #[inline]
    fn next_mask(&mut self) -> [u8; 4] {
        let mut x = self.mask_state;
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        self.mask_state = if x == 0 { 0x6c8e_9cf5 } else { x };
        self.mask_state.to_ne_bytes()
    }

    #[inline]
    pub fn has_buffered_write(&self) -> bool {
        self.out_pos < self.out_buf.len()
    }

    #[inline]
    pub fn buffered_write_len(&self) -> usize {
        self.out_buf.len().saturating_sub(self.out_pos)
    }

    #[inline]
    pub fn shrink_deadline(&self) -> Option<Instant> {
        self.shrink_at
    }

    /// Shrink an oversized `in_buf` (from `<= 2 MiB` streaming) back to
    /// `read_buf_cap` once idle past `shrink_at`.
    ///
    /// Note: after `split_to(len)`, `BytesMut::capacity()` reports only the
    /// remaining tail capacity (often 0) while still owning the full backing
    /// allocation, so we replace `in_buf` whenever `shrink_at` is due and
    /// `in_buf` is empty rather than gating on `in_buf.capacity()`.
    pub fn shrink_if_idle(&mut self, now: Instant) {
        if self.shrink_at.is_some_and(|at| now >= at) {
            if self.in_buf.is_empty() {
                self.in_buf = BytesMut::with_capacity(self.cfg.read_buf_cap);
            }
            self.shrink_at = None;
        }
    }

    /// Try to parse a complete unfragmented data frame already in `in_buf`
    /// without performing any socket read. Used for coalesced batch draining.
    pub fn try_read_buffered(&mut self) -> Option<Result<Message, WsError>> {
        if self.large_read.is_some() || self.frag_opcode.is_some() || self.in_buf.len() < 2 {
            return None;
        }
        let hdr = match parse_header(&self.in_buf, self.cfg.max_frame_size) {
            Ok(Some(h)) => h,
            Ok(None) => return None,
            Err(e) => return Some(Err(e)),
        };
        if !hdr.fin || (hdr.opcode != 1 && hdr.opcode != 2) {
            return None;
        }
        if self.in_buf.len() < hdr.hdr_len + hdr.payload_len {
            return None;
        }
        if let Err(e) = check_mask_role(self.cfg.role, hdr.masked) {
            return Some(Err(e));
        }
        self.in_buf.advance(hdr.hdr_len);
        let mut payload = self.in_buf.split_to(hdr.payload_len);
        if hdr.masked {
            apply_mask(&mut payload, hdr.mask);
        }
        Some(make_data_message(hdr.opcode, payload.freeze()))
    }

    /// Read the next WebSocket message. 100% cancel-safe (performs no writes and
    /// preserves partial small/large reads across `tokio::select!` cancellation).
    pub async fn read_message(&mut self) -> Option<Result<Message, WsError>> {
        loop {
            let (hdr, payload) = match self.read_next_frame().await {
                Ok(Some(pair)) => pair,
                Ok(None) => return None,
                Err(e) => return Some(Err(e)),
            };

            match hdr.opcode {
                // Control frames (may arrive interleaved between fragments).
                0x8 => match parse_close_frame(&payload) {
                    Ok(frame) => return Some(Ok(Message::Close(frame))),
                    Err(e) => return Some(Err(e)),
                },
                0x9 => return Some(Ok(Message::Ping(payload))),
                0xA => return Some(Ok(Message::Pong(payload))),
                // Data frames: Text (1) or Binary (2)
                0x1 | 0x2 => {
                    if self.frag_opcode.is_some() {
                        return Some(Err(WsError::Protocol(ProtocolError::ExpectedFragment(
                            Data::Continue,
                        ))));
                    }
                    if hdr.fin {
                        if payload.len() > self.cfg.max_message_size {
                            return Some(Err(WsError::Capacity(CapacityError::MessageTooLong {
                                size: payload.len(),
                                max_size: self.cfg.max_message_size,
                            })));
                        }
                        return Some(make_data_message(hdr.opcode, payload));
                    }
                    if payload.len() > self.cfg.max_message_size {
                        return Some(Err(WsError::Capacity(CapacityError::MessageTooLong {
                            size: payload.len(),
                            max_size: self.cfg.max_message_size,
                        })));
                    }
                    self.frag_opcode = Some(hdr.opcode);
                    self.frag_buf.clear();
                    self.frag_buf.extend_from_slice(&payload);
                }
                // Continuation (0)
                0x0 => {
                    let Some(op) = self.frag_opcode else {
                        return Some(Err(WsError::Protocol(
                            ProtocolError::UnexpectedContinueFrame,
                        )));
                    };
                    let total = self.frag_buf.len().saturating_add(payload.len());
                    if total > self.cfg.max_message_size {
                        self.frag_opcode = None;
                        self.frag_buf.clear();
                        self.frag_buf.shrink_to_fit();
                        return Some(Err(WsError::Capacity(CapacityError::MessageTooLong {
                            size: total,
                            max_size: self.cfg.max_message_size,
                        })));
                    }
                    self.frag_buf.extend_from_slice(&payload);
                    if hdr.fin {
                        self.frag_opcode = None;
                        let full = std::mem::take(&mut self.frag_buf);
                        return Some(make_data_message(op, Bytes::from(full)));
                    }
                }
                _ => {
                    return Some(Err(WsError::Protocol(ProtocolError::InvalidOpcode(
                        hdr.opcode,
                    ))));
                }
            }
        }
    }

    async fn read_next_frame(&mut self) -> Result<Option<(FrameHdr, Bytes)>, WsError> {
        // Resume an in-progress spike frame if previously cancelled mid-read.
        if self.large_read.is_some() {
            return self.finish_large_read().await;
        }

        // 1. Read header bytes (without advancing `in_buf` yet).
        let hdr = loop {
            if let Some(hdr) = parse_header(&self.in_buf, self.cfg.max_frame_size)? {
                break hdr;
            }
            if self.in_buf.capacity() - self.in_buf.len() < 16 {
                self.in_buf.reserve(self.active_read_cap);
            }
            let avail_cap = self.in_buf.capacity() - self.in_buf.len();
            let n = self.stream.read_buf(&mut self.in_buf).await?;
            if n == 0 {
                if self.in_buf.is_empty() {
                    return Ok(None);
                }
                return Err(WsError::Protocol(
                    ProtocolError::ResetWithoutClosingHandshake,
                ));
            }
            if n == avail_cap && self.active_read_cap < self.cfg.read_buf_cap {
                self.active_read_cap = (self.active_read_cap * 2).min(self.cfg.read_buf_cap);
            }
        };

        check_mask_role(self.cfg.role, hdr.masked)?;
        validate_control_hdr(&hdr)?;

        let len = hdr.payload_len;
        if len == 0 {
            self.in_buf.advance(hdr.hdr_len);
            return Ok(Some((hdr, Bytes::new())));
        }

        // Fast path 1: complete frame already buffered in `in_buf`.
        let total = hdr.hdr_len + len;
        if self.in_buf.len() >= total {
            self.in_buf.advance(hdr.hdr_len);
            let mut payload = self.in_buf.split_to(len);
            if hdr.masked {
                apply_mask(&mut payload, hdr.mask);
            }
            return Ok(Some((hdr, payload.freeze())));
        }

        // Fast path 2 (Spike frame > MAX_REUSABLE_READ_CAP): read into a
        // dedicated exact-sized `BytesMut` stored on `self.large_read` so
        // `in_buf` never bloats on spikes and frees immediately on drop.
        if len > MAX_REUSABLE_READ_CAP {
            self.in_buf.advance(hdr.hdr_len);
            let mut dedicated = BytesMut::with_capacity(len);
            if !self.in_buf.is_empty() {
                dedicated.extend_from_slice(&self.in_buf);
                self.in_buf.clear();
            }
            self.maybe_shrink_in_buf();
            self.large_read = Some((hdr, dedicated));
            return self.finish_large_read().await;
        }

        // Normal & medium/large streaming frame (<= 2 MiB): keep header intact
        // in `in_buf` while reading (100% cancel-safe) and reuse the buffer
        // across back-to-back frames without per-block VirtualAlloc/mmap.
        if total > self.active_read_cap && self.active_read_cap < self.cfg.read_buf_cap {
            self.active_read_cap = self.cfg.read_buf_cap;
        }
        self.in_buf.reserve(total - self.in_buf.len());
        while self.in_buf.len() < total {
            let n = self.stream.read_buf(&mut self.in_buf).await?;
            if n == 0 {
                return Err(WsError::Protocol(
                    ProtocolError::ResetWithoutClosingHandshake,
                ));
            }
        }
        self.in_buf.advance(hdr.hdr_len);
        let mut payload = self.in_buf.split_to(len);
        if hdr.masked {
            apply_mask(&mut payload, hdr.mask);
        }
        if total > self.cfg.read_buf_cap * 2 {
            self.shrink_at = Some(Instant::now() + LARGE_BUF_IDLE_SHRINK);
        }
        Ok(Some((hdr, payload.freeze())))
    }

    async fn finish_large_read(&mut self) -> Result<Option<(FrameHdr, Bytes)>, WsError> {
        loop {
            let (target_len, cur_len) = {
                let (hdr, buf) = self.large_read.as_ref().expect("checked");
                (hdr.payload_len, buf.len())
            };
            if cur_len >= target_len {
                let (hdr, mut buf) = self.large_read.take().expect("checked");
                if hdr.masked {
                    apply_mask(&mut buf, hdr.mask);
                }
                return Ok(Some((hdr, buf.freeze())));
            }
            let remaining = target_len - cur_len;
            let (_, buf) = self.large_read.as_mut().expect("checked");
            let n = read_buf_limited(&mut self.stream, buf, remaining).await?;
            if n == 0 {
                self.large_read = None;
                return Err(WsError::Protocol(
                    ProtocolError::ResetWithoutClosingHandshake,
                ));
            }
        }
    }

    #[inline]
    fn maybe_shrink_in_buf(&mut self) {
        if self.in_buf.is_empty()
            && (self.shrink_at.is_some() || self.in_buf.capacity() > self.cfg.read_buf_cap * 2)
        {
            self.in_buf = BytesMut::with_capacity(self.cfg.read_buf_cap);
            self.shrink_at = None;
        }
    }

    /// Coalesce a small Server -> Client binary frame (with optional inline
    /// 72-byte SHiP head/LIB patch) into `out_buf`, or write a large frame
    /// directly via zero-copy `write_vectored`.
    ///
    /// Call [`Self::flush_writes`] at the end of a batch.
    pub async fn queue_binary_patched(
        &mut self,
        payload: &[u8],
        patch: Option<(BlockPos, BlockPos)>,
    ) -> Result<(), WsError> {
        debug_assert_eq!(self.cfg.role, WsRole::Server);
        let mut hdr_buf = [0u8; 10];
        let hdr_len = encode_header(0x2, payload.len(), None, &mut hdr_buf);
        let frame_total = hdr_len + payload.len();

        // Small frame: coalesce into bounded `out_buf` (inline patch) so the
        // caller can immediately drop `payload` Bytes and let `in_buf` reclaim.
        if frame_total <= SMALL_COALESCE_THRESHOLD {
            if self.out_buf.len() + frame_total > MAX_COALESCE_BUF {
                self.flush_writes().await?;
            }
            if self.out_buf.capacity() == 0 {
                self.out_buf.reserve(16 * 1024);
            }
            self.out_buf.extend_from_slice(&hdr_buf[..hdr_len]);
            if let (Some((head, lib)), true) = (patch, payload.len() >= 73) {
                self.out_buf.push(payload[0]);
                self.out_buf.extend_from_slice(&head.num.to_le_bytes());
                self.out_buf.extend_from_slice(&head.id);
                self.out_buf.extend_from_slice(&lib.num.to_le_bytes());
                self.out_buf.extend_from_slice(&lib.id);
                self.out_buf.extend_from_slice(&payload[73..]);
            } else {
                self.out_buf.extend_from_slice(payload);
            }
            return Ok(());
        }

        // Large frame: flush any queued small frames first, then write directly
        // via vectored I/O with zero payload copy and zero `out_buf` growth.
        if self.has_buffered_write() {
            self.flush_writes().await?;
        }
        if let (Some((head, lib)), true) = (patch, payload.len() >= 73) {
            let mut head_buf = [0u8; 73];
            head_buf[0] = payload[0];
            head_buf[1..5].copy_from_slice(&head.num.to_le_bytes());
            head_buf[5..37].copy_from_slice(&head.id);
            head_buf[37..41].copy_from_slice(&lib.num.to_le_bytes());
            head_buf[41..73].copy_from_slice(&lib.id);
            write_all_vectored_3(
                &mut self.stream,
                &hdr_buf[..hdr_len],
                &head_buf[..],
                &payload[73..],
            )
            .await?;
        } else {
            write_all_vectored_2(&mut self.stream, &hdr_buf[..hdr_len], payload).await?;
        }
        self.needs_flush = true;
        Ok(())
    }

    /// Flush all coalesced small frames in `out_buf` using cursor advancement
    /// (zero `Vec::drain` memmove) and flush the underlying transport if needed.
    pub async fn flush_writes(&mut self) -> Result<(), WsError> {
        if !self.has_buffered_write() && !self.needs_flush {
            return Ok(());
        }
        while self.out_pos < self.out_buf.len() {
            let n = self.stream.write(&self.out_buf[self.out_pos..]).await?;
            if n == 0 {
                return Err(WsError::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "write zero",
                )));
            }
            self.out_pos += n;
        }
        self.out_buf.clear();
        self.out_pos = 0;
        self.needs_flush = false;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn send_pong(&mut self, payload: &[u8]) -> Result<(), WsError> {
        if self.has_buffered_write() {
            self.flush_writes().await?;
        }
        self.write_framed(0xA, payload).await?;
        self.stream.flush().await?;
        self.needs_flush = false;
        Ok(())
    }

    /// Send any WebSocket `Message` and flush immediately.
    pub async fn send_message(&mut self, msg: Message) -> Result<(), WsError> {
        if self.has_buffered_write() {
            self.flush_writes().await?;
        }
        match msg {
            Message::Binary(b) => self.write_framed(0x2, &b).await?,
            Message::Text(t) => self.write_framed(0x1, t.as_bytes()).await?,
            Message::Ping(p) => self.write_framed(0x9, &p).await?,
            Message::Pong(p) => self.write_framed(0xA, &p).await?,
            Message::Close(frame) => {
                let mut payload = [0u8; 125];
                let mut len = 0usize;
                if let Some(cf) = frame {
                    payload[0..2].copy_from_slice(&u16::from(cf.code).to_be_bytes());
                    let r = cf.reason.as_bytes();
                    let take = r.len().min(123);
                    payload[2..2 + take].copy_from_slice(&r[..take]);
                    len = 2 + take;
                }
                self.write_framed(0x8, &payload[..len]).await?;
            }
            Message::Frame(f) => {
                let b = f.into_payload();
                self.write_framed(0x2, &b).await?;
            }
        }
        self.stream.flush().await?;
        self.needs_flush = false;
        Ok(())
    }

    /// Send a binary slice directly without wrapping in `Bytes`.
    pub async fn send_binary_slice(&mut self, payload: &[u8]) -> Result<(), WsError> {
        if self.has_buffered_write() {
            self.flush_writes().await?;
        }
        self.write_framed(0x2, payload).await?;
        self.stream.flush().await?;
        self.needs_flush = false;
        Ok(())
    }

    async fn write_framed(&mut self, opcode: u8, payload: &[u8]) -> Result<(), WsError> {
        let mask = match self.cfg.role {
            WsRole::Client => Some(self.next_mask()),
            WsRole::Server => None,
        };
        let mut hdr_buf = [0u8; 14];
        let hdr_len = encode_header(opcode, payload.len(), mask, &mut hdr_buf);
        if let Some(m) = mask {
            let total = hdr_len + payload.len();
            if total <= 256 {
                // Zero-allocation stack path for SHiP ACKs, pings, and small requests.
                let mut stack = [0u8; 256];
                stack[..hdr_len].copy_from_slice(&hdr_buf[..hdr_len]);
                stack[hdr_len..total].copy_from_slice(payload);
                apply_mask(&mut stack[hdr_len..total], m);
                self.stream.write_all(&stack[..total]).await?;
            } else {
                let mut buf = Vec::with_capacity(total);
                buf.extend_from_slice(&hdr_buf[..hdr_len]);
                let start = buf.len();
                buf.extend_from_slice(payload);
                apply_mask(&mut buf[start..], m);
                self.stream.write_all(&buf).await?;
            }
        } else {
            write_all_vectored_2(&mut self.stream, &hdr_buf[..hdr_len], payload).await?;
        }
        Ok(())
    }

    pub async fn close_quietly_now(&mut self) {
        let _ = self
            .send_message(Message::Close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "".into(),
            })))
            .await;
        let _ = self.stream.shutdown().await;
    }
}

/// Read at most `limit` bytes directly into `buf`'s uninitialized spare capacity.
async fn read_buf_limited<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    limit: usize,
) -> io::Result<usize> {
    struct ReadLimited<'a, S> {
        stream: &'a mut S,
        buf: &'a mut BytesMut,
        limit: usize,
    }
    impl<S: AsyncRead + Unpin> std::future::Future for ReadLimited<'_, S> {
        type Output = io::Result<usize>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            let this = self.get_mut();
            let spare = this.buf.spare_capacity_mut();
            let cap = spare.len().min(this.limit);
            let mut rbuf = tokio::io::ReadBuf::uninit(&mut spare[..cap]);
            ready!(Pin::new(&mut *this.stream).poll_read(cx, &mut rbuf))?;
            let n = rbuf.filled().len();
            // SAFETY: `poll_read` initialized `n` bytes of `spare[..cap]`,
            // which is within `this.buf`'s allocated spare capacity.
            unsafe {
                this.buf.advance_mut(n);
            }
            Poll::Ready(Ok(n))
        }
    }
    ReadLimited { stream, buf, limit }.await
}

async fn write_all_vectored_2<S: AsyncWrite + Unpin>(
    stream: &mut S,
    a: &[u8],
    b: &[u8],
) -> io::Result<()> {
    let mut off_a = 0usize;
    let mut off_b = 0usize;
    while off_a < a.len() || off_b < b.len() {
        let n = if off_a < a.len() && off_b < b.len() {
            let bufs = [IoSlice::new(&a[off_a..]), IoSlice::new(&b[off_b..])];
            stream.write_vectored(&bufs).await?
        } else if off_a < a.len() {
            stream.write(&a[off_a..]).await?
        } else {
            stream.write(&b[off_b..]).await?
        };
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "write zero"));
        }
        let take_a = n.min(a.len() - off_a);
        off_a += take_a;
        off_b += n - take_a;
    }
    Ok(())
}

async fn write_all_vectored_3<S: AsyncWrite + Unpin>(
    stream: &mut S,
    a: &[u8],
    b: &[u8],
    c: &[u8],
) -> io::Result<()> {
    let mut off_a = 0usize;
    let mut off_b = 0usize;
    let mut off_c = 0usize;
    while off_a < a.len() || off_b < b.len() || off_c < c.len() {
        let n = if off_a < a.len() {
            let bufs = [
                IoSlice::new(&a[off_a..]),
                IoSlice::new(&b[off_b..]),
                IoSlice::new(&c[off_c..]),
            ];
            stream.write_vectored(&bufs).await?
        } else if off_b < b.len() {
            let bufs = [IoSlice::new(&b[off_b..]), IoSlice::new(&c[off_c..])];
            stream.write_vectored(&bufs).await?
        } else {
            stream.write(&c[off_c..]).await?
        };
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "write zero"));
        }
        let mut rem = n;
        let ta = rem.min(a.len() - off_a);
        off_a += ta;
        rem -= ta;
        let tb = rem.min(b.len() - off_b);
        off_b += tb;
        rem -= tb;
        off_c += rem;
    }
    Ok(())
}

fn encode_header(opcode: u8, len: usize, mask: Option<[u8; 4]>, out: &mut [u8]) -> usize {
    out[0] = 0x80 | (opcode & 0x0f);
    let mask_bit = if mask.is_some() { 0x80 } else { 0x00 };
    let mut pos = if len < 126 {
        out[1] = mask_bit | (len as u8);
        2
    } else if len <= 65_535 {
        out[1] = mask_bit | 126;
        out[2..4].copy_from_slice(&(len as u16).to_be_bytes());
        4
    } else {
        out[1] = mask_bit | 127;
        out[2..10].copy_from_slice(&(len as u64).to_be_bytes());
        10
    };
    if let Some(m) = mask {
        out[pos..pos + 4].copy_from_slice(&m);
        pos += 4;
    }
    pos
}

fn validate_control_hdr(hdr: &FrameHdr) -> Result<(), WsError> {
    if hdr.opcode >= 0x8 {
        if !hdr.fin {
            return Err(WsError::Protocol(ProtocolError::FragmentedControlFrame));
        }
        if hdr.payload_len > 125 {
            return Err(WsError::Protocol(ProtocolError::ControlFrameTooBig));
        }
    }
    Ok(())
}

fn parse_header(buf: &[u8], max_frame_size: usize) -> Result<Option<FrameHdr>, WsError> {
    if buf.len() < 2 {
        return Ok(None);
    }
    let b0 = buf[0];
    let b1 = buf[1];
    if (b0 & 0x70) != 0 {
        return Err(WsError::Protocol(ProtocolError::NonZeroReservedBits));
    }
    let fin = (b0 & 0x80) != 0;
    let opcode = b0 & 0x0f;
    let masked = (b1 & 0x80) != 0;
    let len_code = b1 & 0x7f;

    let (payload_len_u64, mut hdr_len) = match len_code {
        0..=125 => (u64::from(len_code), 2usize),
        126 => {
            if buf.len() < 4 {
                return Ok(None);
            }
            (u64::from(u16::from_be_bytes([buf[2], buf[3]])), 4usize)
        }
        _ => {
            if buf.len() < 10 {
                return Ok(None);
            }
            let v = u64::from_be_bytes(buf[2..10].try_into().unwrap());
            // RFC 6455 §5.2: the most significant bit MUST be 0.
            if (v & (1u64 << 63)) != 0 {
                return Err(WsError::Capacity(CapacityError::MessageTooLong {
                    size: usize::MAX,
                    max_size: max_frame_size,
                }));
            }
            (v, 10usize)
        }
    };

    let mask = if masked {
        if buf.len() < hdr_len + 4 {
            return Ok(None);
        }
        let m = [
            buf[hdr_len],
            buf[hdr_len + 1],
            buf[hdr_len + 2],
            buf[hdr_len + 3],
        ];
        hdr_len += 4;
        m
    } else {
        [0; 4]
    };

    if payload_len_u64 > max_frame_size as u64 {
        return Err(WsError::Capacity(CapacityError::MessageTooLong {
            size: payload_len_u64 as usize,
            max_size: max_frame_size,
        }));
    }

    Ok(Some(FrameHdr {
        fin,
        opcode,
        masked,
        mask,
        payload_len: payload_len_u64 as usize,
        hdr_len,
    }))
}

#[inline]
fn check_mask_role(role: WsRole, masked: bool) -> Result<(), WsError> {
    match (role, masked) {
        (WsRole::Server, false) => Err(WsError::Protocol(ProtocolError::UnmaskedFrameFromClient)),
        (WsRole::Client, true) => Err(WsError::Protocol(ProtocolError::MaskedFrameFromServer)),
        _ => Ok(()),
    }
}

fn make_data_message(opcode: u8, payload: Bytes) -> Result<Message, WsError> {
    if opcode == 2 {
        Ok(Message::Binary(payload))
    } else {
        match String::from_utf8(payload.to_vec()) {
            Ok(s) => Ok(Message::Text(s.into())),
            Err(_) => Err(WsError::Utf8("invalid utf8".into())),
        }
    }
}

fn parse_close_frame(payload: &[u8]) -> Result<Option<CloseFrame>, WsError> {
    if payload.is_empty() {
        return Ok(None);
    }
    if payload.len() == 1 {
        return Err(WsError::Protocol(ProtocolError::InvalidCloseSequence));
    }
    let code = CloseCode::from(u16::from_be_bytes([payload[0], payload[1]]));
    let reason = std::str::from_utf8(&payload[2..])
        .map_err(|_| WsError::Utf8("invalid utf8 in close reason".into()))?
        .to_string();
    Ok(Some(CloseFrame {
        code,
        reason: reason.into(),
    }))
}

/// Word-at-a-time (8 bytes/iter) RFC 6455 XOR masking.
#[inline]
fn apply_mask(buf: &mut [u8], mask: [u8; 4]) {
    let mask64 = u64::from_ne_bytes([
        mask[0], mask[1], mask[2], mask[3], mask[0], mask[1], mask[2], mask[3],
    ]);
    let mut i = 0usize;
    let len = buf.len();
    while i + 8 <= len {
        let chunk = u64::from_ne_bytes(buf[i..i + 8].try_into().unwrap()) ^ mask64;
        buf[i..i + 8].copy_from_slice(&chunk.to_ne_bytes());
        i += 8;
    }
    while i < len {
        buf[i] ^= mask[i & 3];
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ship::wire;
    use tokio::io::{duplex, AsyncWriteExt};

    fn cfg(role: WsRole, max: usize) -> FastWsConfig {
        FastWsConfig {
            role,
            read_buf_cap: 64 * 1024,
            max_message_size: max,
            max_frame_size: max,
        }
    }

    fn sample_block_frame(size: usize) -> Bytes {
        let mut v = vec![0u8; size.max(73)];
        v[0] = 1; // get_blocks_result_v0
        for (i, b) in v.iter_mut().enumerate().skip(1) {
            *b = (i as u8).wrapping_mul(31).wrapping_add(7);
        }
        Bytes::from(v)
    }

    #[tokio::test]
    async fn round_trips_all_length_encodings_in_both_directions() {
        let (a, b) = duplex(256 * 1024);
        let mut server = FastWs::new(a, cfg(WsRole::Server, 4 << 20));
        let mut client = FastWs::new(b, cfg(WsRole::Client, 4 << 20));

        // Cover 7-bit (<126), 16-bit (126..=65535), and 64-bit (>65535) lengths.
        for len in [0usize, 5, 125, 126, 900, 16_384, 65_535, 70_000] {
            let payload: Vec<u8> = (0..len).map(|i| (i & 0xff) as u8).collect();
            let p_clone = payload.clone();
            let send_task = tokio::spawn(async move {
                client.send_binary_slice(&p_clone).await.unwrap();
                client
            });
            let got = server.read_message().await.unwrap().unwrap();
            client = send_task.await.unwrap();
            assert_eq!(got, Message::Binary(Bytes::from(payload.clone())));

            // Server -> Client (unmasked)
            let p_clone = payload.clone();
            let srv_task = tokio::spawn(async move {
                server.queue_binary_patched(&p_clone, None).await.unwrap();
                server.flush_writes().await.unwrap();
                server
            });
            let got = client.read_message().await.unwrap().unwrap();
            server = srv_task.await.unwrap();
            assert_eq!(got, Message::Binary(Bytes::from(payload)));
        }
    }

    #[tokio::test]
    async fn inline_head_lib_patch_matches_wire_patch_for_small_and_large_frames() {
        let (a, b) = duplex(512 * 1024);
        let mut server = FastWs::new(a, cfg(WsRole::Server, 4 << 20));
        let mut client = FastWs::new(b, cfg(WsRole::Client, 4 << 20));

        let head = BlockPos {
            num: 123_456,
            id: [0xaa; 32],
        };
        let lib = BlockPos {
            num: 123_000,
            id: [0xbb; 32],
        };

        // Test both coalesced small path (900 B <= 16 KiB) and vectored large path (128 KiB > 16 KiB).
        for size in [73usize, 900, 12_000, 128 * 1024] {
            let raw = sample_block_frame(size);
            let expected = wire::patch_head_lib(raw.clone(), head, lib);
            let raw_send = raw.clone();
            let srv = tokio::spawn(async move {
                server
                    .queue_binary_patched(&raw_send, Some((head, lib)))
                    .await
                    .unwrap();
                server.flush_writes().await.unwrap();
                server
            });
            let msg = client.read_message().await.unwrap().unwrap();
            server = srv.await.unwrap();
            assert_eq!(msg, Message::Binary(expected), "size={size}");
        }
    }

    #[tokio::test]
    async fn fragmented_message_reassembles_across_interleaved_ping() {
        let (mut raw_tx, rx) = duplex(4096);
        let mut client = FastWs::new(rx, cfg(WsRole::Client, 64 * 1024));

        // Fragment 1: FIN=0, opcode=Text (1), "hello "
        let mut wire_bytes = Vec::new();
        wire_bytes.extend_from_slice(&[0x01, 6]);
        wire_bytes.extend_from_slice(b"hello ");
        // Interleaved control frame: FIN=1, opcode=Ping (9), "pi"
        wire_bytes.extend_from_slice(&[0x89, 2]);
        wire_bytes.extend_from_slice(b"pi");
        // Fragment 2: FIN=1, opcode=Continue (0), "world"
        wire_bytes.extend_from_slice(&[0x80, 5]);
        wire_bytes.extend_from_slice(b"world");
        raw_tx.write_all(&wire_bytes).await.unwrap();

        assert_eq!(
            client.read_message().await.unwrap().unwrap(),
            Message::Ping(Bytes::from_static(b"pi"))
        );
        assert_eq!(
            client.read_message().await.unwrap().unwrap(),
            Message::Text("hello world".into())
        );
    }

    #[tokio::test]
    async fn read_message_is_cancel_safe_for_small_and_spike_frames() {
        // 1. Small frame trickled in 2 parts with cancellation in between.
        let (mut tx, rx) = duplex(16 * 1024);
        let mut client = FastWs::new(rx, cfg(WsRole::Client, 8 << 20));
        let small = vec![0x42u8; 500];
        let mut hdr = [0u8; 10];
        let hlen = encode_header(0x2, small.len(), None, &mut hdr);
        tx.write_all(&hdr[..hlen]).await.unwrap();
        tx.write_all(&small[..100]).await.unwrap();

        let (notify_tx, mut notify_rx) = tokio::sync::oneshot::channel::<()>();
        notify_tx.send(()).unwrap();
        tokio::select! {
            biased;
            _ = &mut notify_rx => {}
            _ = client.read_message() => panic!("should not complete before tail arrives"),
        }
        // Poll once to make sure partial bytes are pulled into `in_buf`, then cancel.
        let _ = tokio::time::timeout(Duration::from_millis(10), client.read_message()).await;
        tx.write_all(&small[100..]).await.unwrap();
        assert_eq!(
            client.read_message().await.unwrap().unwrap(),
            Message::Binary(Bytes::from(small))
        );

        // 2. Spike frame (> 2 MiB) trickled in 2 parts with cancellation mid-payload.
        let spike_len = MAX_REUSABLE_READ_CAP + 4096;
        let spike = vec![0x7fu8; spike_len];
        let hlen = encode_header(0x2, spike.len(), None, &mut hdr);
        let writer = tokio::spawn(async move {
            tx.write_all(&hdr[..hlen]).await.unwrap();
            tx.write_all(&spike[..8192]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(30)).await;
            tx.write_all(&spike[8192..]).await.unwrap();
            (tx, spike)
        });
        // Cancel mid-spike read after partial bytes have moved into `self.large_read`.
        let timed = tokio::time::timeout(Duration::from_millis(10), client.read_message()).await;
        assert!(
            timed.is_err(),
            "expected cancellation before spike finishes"
        );
        let got = client.read_message().await.unwrap().unwrap();
        let (_tx, spike) = writer.await.unwrap();
        assert_eq!(got, Message::Binary(Bytes::from(spike)));
        // Spike frame must never permanently expand `in_buf`.
        assert!(client.in_buf.capacity() <= client.cfg.read_buf_cap * 2);
    }

    #[tokio::test]
    async fn idle_shrink_reclaims_reusable_large_buffer() {
        let (a, b) = duplex(512 * 1024);
        let mut server = FastWs::new(a, cfg(WsRole::Server, 4 << 20));
        let mut client = FastWs::new(b, cfg(WsRole::Client, 4 << 20));

        let payload = vec![0x55u8; 256 * 1024];
        let p_send = payload.clone();
        let srv = tokio::spawn(async move {
            server.queue_binary_patched(&p_send, None).await.unwrap();
            server.flush_writes().await.unwrap();
            server
        });
        let msg = client.read_message().await.unwrap().unwrap();
        let _server = srv.await.unwrap();
        assert_eq!(msg, Message::Binary(Bytes::from(payload)));
        drop(msg);
        // After `drop(msg)`, `BytesMut` reclaims its 256 KiB backing allocation
        // in place on the next reserve (proving zero-alloc reuse during streaming).
        client.in_buf.reserve(1);
        assert!(client.in_buf.capacity() >= 256 * 1024);
        let deadline = client.shrink_deadline().expect("shrink scheduled");
        client.shrink_if_idle(deadline);
        assert!(client.shrink_deadline().is_none());
        client.in_buf.reserve(1);
        assert!(client.in_buf.capacity() <= client.cfg.read_buf_cap);
    }

    #[tokio::test]
    async fn try_read_buffered_drains_coalesced_batch() {
        let (a, b) = duplex(64 * 1024);
        let mut server = FastWs::new(a, cfg(WsRole::Server, 1 << 20));
        let mut client = FastWs::new(b, cfg(WsRole::Client, 1 << 20));

        for i in 0u8..5 {
            server.queue_binary_patched(&[i; 64], None).await.unwrap();
        }
        server.flush_writes().await.unwrap();

        let first = client.read_message().await.unwrap().unwrap();
        assert_eq!(first, Message::Binary(Bytes::from(vec![0u8; 64])));
        let mut rest = Vec::new();
        while let Some(Ok(m)) = client.try_read_buffered() {
            rest.push(m);
        }
        assert_eq!(rest.len(), 4);
        for (idx, m) in rest.into_iter().enumerate() {
            assert_eq!(m, Message::Binary(Bytes::from(vec![(idx + 1) as u8; 64])));
        }
    }

    #[tokio::test]
    async fn enforces_rfc6455_masking_capacity_and_control_rules() {
        // Unmasked frame rejected by Server
        let (mut tx, rx) = duplex(1024);
        let mut server = FastWs::new(rx, cfg(WsRole::Server, 1024));
        tx.write_all(&[0x82, 0x02, 1, 2]).await.unwrap();
        assert!(matches!(
            server.read_message().await,
            Some(Err(WsError::Protocol(
                ProtocolError::UnmaskedFrameFromClient
            )))
        ));

        // Oversized frame rejected with CapacityError::MessageTooLong
        let (mut tx, rx) = duplex(1024);
        let mut client = FastWs::new(rx, cfg(WsRole::Client, 100));
        tx.write_all(&[0x82, 120]).await.unwrap();
        assert!(matches!(
            client.read_message().await,
            Some(Err(WsError::Capacity(CapacityError::MessageTooLong { .. })))
        ));

        // Fragmented control frame rejected
        let (mut tx, rx) = duplex(1024);
        let mut client = FastWs::new(rx, cfg(WsRole::Client, 1024));
        tx.write_all(&[0x09, 0x00]).await.unwrap();
        assert!(matches!(
            client.read_message().await,
            Some(Err(WsError::Protocol(
                ProtocolError::FragmentedControlFrame
            )))
        ));

        // 1-byte Close frame rejected
        let (mut tx, rx) = duplex(1024);
        let mut client = FastWs::new(rx, cfg(WsRole::Client, 1024));
        tx.write_all(&[0x88, 0x01, 0x03]).await.unwrap();
        assert!(matches!(
            client.read_message().await,
            Some(Err(WsError::Protocol(ProtocolError::InvalidCloseSequence)))
        ));
    }
}
