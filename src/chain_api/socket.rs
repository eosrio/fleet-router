//! The socket of one client connection. hyper has no write timeout: a client
//! that stops reading (or vanishes with its receive window closed) would
//! keep its answer, the request's in-flight permit and the upstream slot
//! for as long as TCP keeps the connection. Here a write that waits the
//! listener's read timeout with no byte taken by the client fails, and
//! hyper closes the connection, which gives all of them back.
//!
//! The limit is on progress, not on the whole answer: a client that keeps
//! reading, however slowly, still gets all of it.

use std::future::Future;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::{sleep, Sleep};

use crate::tls::ClientStream;

use super::stats::{ApiStats, Rejection};

/// Why a client connection was closed while an answer was being written.
#[derive(Debug)]
pub struct Stalled;

impl std::fmt::Display for Stalled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the client took none of its answer in time")
    }
}

impl std::error::Error for Stalled {}

/// A client's stream (plain TCP or TLS) whose writes fail once they have waited
/// `stall` with no progress. A cut counts as `rejected{reason="timeout"}`.
pub struct Socket {
    stream: ClientStream,
    stall: Duration,
    stats: Arc<ApiStats>,
    timer: Option<Pin<Box<Sleep>>>,
    /// A write is waiting and `timer` runs.
    waiting: bool,
}

impl Socket {
    pub fn new(stream: ClientStream, stall: Duration, stats: Arc<ApiStats>) -> Socket {
        Socket {
            stream,
            stall,
            stats,
            timer: None,
            waiting: false,
        }
    }

    /// `write`, the outcome of a write to the stream, or [`Stalled`] once
    /// the writes have waited `stall` with no byte taken.
    fn progress<T>(
        &mut self,
        cx: &mut Context<'_>,
        write: Poll<io::Result<T>>,
    ) -> Poll<io::Result<T>> {
        if write.is_ready() {
            self.waiting = false;
            return write;
        }
        let stall = self.stall;
        let timer = self.timer.get_or_insert_with(|| Box::pin(sleep(stall)));
        if !self.waiting {
            self.waiting = true;
            timer.as_mut().reset(tokio::time::Instant::now() + stall);
        }
        ready!(timer.as_mut().poll(cx));
        self.stats.rejected.inc(Rejection::Timeout);
        tracing::debug!(
            peer = ?self.stream.peer_addr().ok(),
            "chain API client took none of its answer in time; closing"
        );
        Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, Stalled)))
    }
}

impl AsyncRead for Socket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let write = Pin::new(&mut this.stream).poll_write(cx, buf);
        this.progress(cx, write)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let write = Pin::new(&mut this.stream).poll_write_vectored(cx, bufs);
        this.progress(cx, write)
    }

    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let flush = Pin::new(&mut this.stream).poll_flush(cx);
        this.progress(cx, flush)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
