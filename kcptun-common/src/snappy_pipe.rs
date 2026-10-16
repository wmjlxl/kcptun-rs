//! Snappy session codec as an `AsyncRead + AsyncWrite` transport adapter.
//!
//! [`SnappyPipe`] wraps any `knet::AsyncRead + AsyncWrite` stream (e.g. a
//! [`kcp_rs::KcpStream`]) and transparently compresses / decompresses the byte
//! stream with Go-compatible snappy *framing* (the `snap` crate, CRC32C).
//!
//! Placement matches kcptun production: the whole SMUX frame byte stream is
//! compressed as **one** snappy stream (stream header written once) on the KCP
//! user data — see `AGENTS.md` "Snappy compression at SMUX session level".
//!
//! `compress = false` is a byte passthrough (`--nocomp`), preserving the exact
//! wire bytes.

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, BytesMut};

use crate::SnappyStreamDecoder;

/// `io::Write` adapter over a [`BytesMut`] — `bytes` does not implement
/// `io::Write`, so this lets `snap::FrameEncoder` compress straight into the
/// staged buffer instead of an intermediate `Vec` that had to be taken and
/// reallocated on every write.
///
/// NOTE: the write side deliberately keeps one flush per `poll_write` (small
/// writes each become their own frame) rather than accumulating to a block
/// threshold: upper layers (`SmuxConn` write task, session keepalive NOPs)
/// call `write_all` without `poll_flush`, so deferring the flush would park
/// small frames — including keepalives — until 32 KiB accumulates.
struct BytesMutWriter(BytesMut);

impl std::io::Write for BytesMutWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Snappy session codec adapter (`AsyncRead + AsyncWrite`).
///
/// - **Write side** buffers compressed bytes until the inner transport accepts
///   them; backpressure propagates — when inner `poll_write` returns `Pending`,
///   this adapter returns `Pending` before accepting more input.
/// - **Read side** keeps a persistent [`SnappyStreamDecoder`] so snappy frames
///   split across multiple KCP `recv` calls reassemble correctly.
pub struct SnappyPipe<T> {
    inner: T,
    compress: bool,
    // ── write side ──
    /// Compressed output goes straight into the encoder's writer (a
    /// `BytesMut`), never through an intermediate `Vec` that had to be
    /// `mem::take`-en and reallocated on every write. In passthrough mode
    /// this is `None` and `pending` carries the bytes instead.
    encoder: Option<snap::write::FrameEncoder<BytesMutWriter>>,
    /// Bytes staged for the inner transport in passthrough mode. When
    /// compressing, the staged bytes live in the encoder's writer
    /// (`BytesMutWriter`) instead.
    pending: BytesMut,
    // ── read side ──
    decoder: SnappyStreamDecoder,
    dec_out: VecDeque<u8>,
    read_buf: Vec<u8>,
    read_off: usize,
    read_len: usize,
    eof: bool,
}

impl<T> SnappyPipe<T>
where
    T: knet::AsyncRead + knet::AsyncWrite + Unpin,
{
    /// Wrap `inner`. When `compress` is `true`, writes are snappy-compressed
    /// (session level) and reads decompressed; `false` = byte passthrough.
    pub fn new(inner: T, compress: bool) -> Self {
        SnappyPipe {
            inner,
            compress,
            encoder: compress.then(|| {
                snap::write::FrameEncoder::new(BytesMutWriter(BytesMut::with_capacity(64 * 1024)))
            }),
            pending: BytesMut::new(),
            decoder: SnappyStreamDecoder::new(),
            dec_out: VecDeque::new(),
            read_buf: vec![0u8; 64 * 1024],
            read_off: 0,
            read_len: 0,
            eof: false,
        }
    }

    /// Mutably borrow the inner transport.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    // ── read side ───────────────────────────────────────────────────────────

    fn poll_read_into(&mut self, cx: &mut Context<'_>, out: &mut [u8]) -> Poll<io::Result<usize>> {
        if out.is_empty() {
            return Poll::Ready(Ok(0));
        }
        // Serve buffered decompressed bytes first.
        if !self.dec_out.is_empty() {
            return Poll::Ready(Ok(self.take_decoded(out)));
        }
        if self.eof {
            return Poll::Ready(Ok(0));
        }

        loop {
            if self.compress {
                // 1. Feed any buffered raw bytes into the decoder.
                if self.read_off < self.read_len {
                    // Keep decoded data queued by this poll bounded even when
                    // a 64 KiB transport read contains many compressed blocks.
                    let end = (self.read_off + 8 * 1024).min(self.read_len);
                    let data = &self.read_buf[self.read_off..end];
                    let dec_out = &mut self.dec_out;
                    match self
                        .decoder
                        .feed_chunks(data, |chunk| dec_out.extend(chunk))
                    {
                        Ok(()) => {
                            // feed_chunks() consumes the whole slice into its internal buffer.
                            self.read_off = end;
                        }
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                    if !self.dec_out.is_empty() {
                        return Poll::Ready(Ok(self.take_decoded(out)));
                    }
                    if self.read_off < self.read_len {
                        continue;
                    }
                }
            } else {
                // Passthrough (`--nocomp`): return raw inner bytes directly — the
                // wire is not snappy-framed, so the decoder would never emit.
                if self.read_off < self.read_len {
                    let avail = self.read_len - self.read_off;
                    let n = out.len().min(avail);
                    out[..n].copy_from_slice(&self.read_buf[self.read_off..self.read_off + n]);
                    self.read_off += n;
                    if self.read_off == self.read_len {
                        self.read_off = 0;
                        self.read_len = 0;
                    }
                    return Poll::Ready(Ok(n));
                }
            }

            // 2. Reset the read buffer (fully consumed).
            self.read_off = 0;
            self.read_len = 0;

            // 3. Make room if a single inner read filled the buffer without
            //    producing decompressed output (incompressible large chunks).
            if self.read_len == self.read_buf.len() {
                let extra = self.read_buf.len();
                self.read_buf.resize(self.read_buf.len() + extra, 0);
            }

            // 4. Read more raw bytes from the inner transport.
            match inner_poll_read(&mut self.inner, cx, &mut self.read_buf[self.read_len..]) {
                Poll::Ready(Ok(0)) => {
                    self.eof = true;
                    return Poll::Ready(Ok(0));
                }
                Poll::Ready(Ok(n)) => self.read_len += n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn take_decoded(&mut self, out: &mut [u8]) -> usize {
        let n = out.len().min(self.dec_out.len());
        // Two memcpy operations instead of a per-byte drain loop.
        // VecDeque may wrap around the ring buffer, so we copy in
        // up to two contiguous chunks.
        let (front, back) = self.dec_out.as_slices();
        let first = n.min(front.len());
        out[..first].copy_from_slice(&front[..first]);
        if first < n {
            let rest = n - first;
            out[first..n].copy_from_slice(&back[..rest]);
        }
        self.dec_out.drain(..n);
        n
    }

    // ── write side ──────────────────────────────────────────────────────────

    fn poll_write_impl(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        // 1. Drain any staged bytes to the inner transport first; propagate
        //    backpressure (Pending) so we don't buffer unboundedly.
        if !self.is_staged_empty() {
            match self.drain_pending(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(_)) => {}
            }
        }

        // 2. Accept new input into `buf` (compress or passthrough).
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        if self.compress {
            use std::io::Write;
            let enc = self.encoder.as_mut().expect("encoder set when compress");
            // One flush per poll_write (see `BytesMutWriter`): the compressed
            // frame lands directly in the writer's `BytesMut`, staged for the
            // drain below — no intermediate `Vec`, no take/realloc, one copy
            // fewer than the old pending-extend path.
            if enc.write_all(buf).is_err() || enc.flush().is_err() {
                return Poll::Ready(Err(io::Error::other("snappy encode failed")));
            }
        } else {
            self.pending.extend_from_slice(buf);
        }

        // 3. Best-effort drain; we've already accepted `buf` (buffered), so a
        //    blocked inner transport defers to the next poll_write call.
        match self.drain_pending(cx) {
            Poll::Pending | Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
        }
        Poll::Ready(Ok(buf.len()))
    }

    /// Flush the encoder and drain everything to the inner transport.
    fn finish_write_side(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.compress {
            use std::io::Write;
            let enc = self.encoder.as_mut().expect("encoder set when compress");
            let _ = enc.flush();
        }
        match self.drain_pending(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        }
    }

    fn poll_flush_impl(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.finish_write_side(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        inner_poll_flush(&mut self.inner, cx)
    }

    fn poll_shutdown_impl(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.finish_write_side(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }
        inner_poll_shutdown(&mut self.inner, cx)
    }

    /// True when nothing is staged for the inner transport.
    fn is_staged_empty(&self) -> bool {
        match self.encoder {
            Some(ref enc) => enc.get_ref().0.is_empty(),
            None => self.pending.is_empty(),
        }
    }

    /// Drain staged bytes to the inner transport.
    ///
    /// Compressing: the staged buffer is the encoder's own writer — the
    /// compressed frames land there directly (no intermediate `Vec` hop).
    /// Passthrough: `pending`. After a partial write the remainder stays
    /// staged; the next call resumes from the front.
    fn drain_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            // Destructure so the staged buffer (`encoder`/`pending`) and the
            // transport (`inner`) are disjoint field borrows in one call.
            let Self {
                inner,
                encoder,
                pending,
                ..
            } = self;
            let staged: &mut BytesMut = match encoder {
                Some(enc) => &mut enc.get_mut().0,
                None => pending,
            };
            if staged.is_empty() {
                return Poll::Ready(Ok(()));
            }
            match inner_poll_write(inner, cx, staged) {
                Poll::Ready(Ok(0)) => return Poll::Ready(Ok(())),
                Poll::Ready(Ok(n)) => {
                    let staged: &mut BytesMut = match encoder {
                        Some(enc) => &mut enc.get_mut().0,
                        None => pending,
                    };
                    staged.advance(n);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

// ─── Inner transport poll helpers (trait signature differs per runtime) ───────

fn inner_poll_read<T: knet::AsyncRead + Unpin>(
    inner: &mut T,
    cx: &mut Context<'_>,
    buf: &mut [u8],
) -> Poll<io::Result<usize>> {
    let mut rb = knet::ReadBuf::new(buf);
    match Pin::new(inner).poll_read(cx, &mut rb) {
        Poll::Ready(Ok(())) => Poll::Ready(Ok(rb.filled().len())),
        Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
        Poll::Pending => Poll::Pending,
    }
}

fn inner_poll_write<T: knet::AsyncWrite + Unpin>(
    inner: &mut T,
    cx: &mut Context<'_>,
    buf: &[u8],
) -> Poll<io::Result<usize>> {
    Pin::new(inner).poll_write(cx, buf)
}

fn inner_poll_flush<T: knet::AsyncWrite + Unpin>(
    inner: &mut T,
    cx: &mut Context<'_>,
) -> Poll<io::Result<()>> {
    Pin::new(inner).poll_flush(cx)
}

fn inner_poll_shutdown<T: knet::AsyncWrite + Unpin>(
    inner: &mut T,
    cx: &mut Context<'_>,
) -> Poll<io::Result<()>> {
    Pin::new(inner).poll_shutdown(cx)
}

// ─── knet::AsyncRead / AsyncWrite impls (feature-gated signatures) ─────────────

#[cfg(feature = "tokio")]
impl<T> knet::AsyncRead for SnappyPipe<T>
where
    T: knet::AsyncRead + knet::AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut knet::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let space = buf.initialize_unfilled();
        match this.poll_read_into(cx, space) {
            Poll::Ready(Ok(0)) => Poll::Ready(Ok(())),
            Poll::Ready(Ok(n)) => {
                buf.advance(n);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(feature = "tokio")]
impl<T> knet::AsyncWrite for SnappyPipe<T>
where
    T: knet::AsyncRead + knet::AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().poll_write_impl(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_flush_impl(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.get_mut().poll_shutdown_impl(cx)
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(all(test, feature = "tokio"))]
mod tests;
