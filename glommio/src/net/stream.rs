// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
#[cfg(not(feature = "trust-unmanaged-buffer"))]
use crate::ByteSliceMutExt;
use crate::{
    reactor::{Reactor, RecvBuffer},
    sys::{self, Source},
};
use futures_lite::ready;
use nix::sys::socket::MsgFlags;
use std::{
    cell::Cell,
    io,
    net::Shutdown,
    os::unix::io::{AsRawFd, FromRawFd, RawFd},
    rc::{Rc, Weak},
    task::{Context, Poll},
    time::Duration,
};

type Result<T> = crate::Result<T, ()>;

pub(crate) trait RxBuf: Default {
    type ReadResult;

    fn len(&self) -> usize;
    fn read(&mut self, buf: &mut [u8]) -> usize;
    fn resize(&mut self);
    fn is_empty(&mut self) -> bool;
    fn as_bytes(&self) -> &[u8];
    fn recv_buffer(&mut self, buf: Option<&mut [u8]>) -> RecvBuffer;
    fn consume(&mut self, amt: usize);
    fn buffer_size(&self) -> usize;
    fn set_buffer_size(&mut self, buffer_size: usize);
    fn handle_result(&mut self, result: usize);
    fn prefer_external_buffer(&self, buf: &mut [u8]) -> bool;
}

#[derive(Debug)]
pub(crate) struct Preallocated {
    buf: Vec<u8>,
    head: usize,
    tail: usize,
    cap: usize,
}

impl Preallocated {
    fn new(size: usize) -> Self {
        Self {
            buf: vec![0; size],
            tail: 0,
            head: 0,
            cap: size,
        }
    }
}

impl Default for Preallocated {
    fn default() -> Self {
        Self::new(DEFAULT_BUFFER_SIZE)
    }
}

impl RxBuf for Preallocated {
    type ReadResult = usize;

    fn len(&self) -> usize {
        self.tail - self.head
    }

    fn read(&mut self, buf: &mut [u8]) -> usize {
        let sz = std::cmp::min(self.len(), buf.len());
        if sz > 0 {
            buf[..sz].copy_from_slice(&self.buf[self.head..self.head + sz]);
            self.head += sz;
        }
        sz
    }

    fn resize(&mut self) {
        if self.cap > self.buf.len() {
            self.buf.reserve(self.cap - self.buf.len());
        } else {
            assert!(self.tail == 0);
            unsafe { self.buf.set_len(self.cap) };
            self.buf.shrink_to_fit();
        }
    }

    fn is_empty(&mut self) -> bool {
        if self.tail > self.head {
            return false;
        } else if self.tail > 0 {
            self.tail = 0;
            self.head = 0;
        }
        true
    }

    fn as_bytes(&self) -> &[u8] {
        &self.buf[self.head..self.tail]
    }

    fn recv_buffer(&mut self, buf: Option<&mut [u8]>) -> RecvBuffer {
        RecvBuffer::allocated(buf.unwrap_or_else(|| self.buf.as_mut()))
    }

    fn consume(&mut self, amt: usize) {
        self.head += std::cmp::min(self.len(), amt);
    }

    fn buffer_size(&self) -> usize {
        self.cap
    }

    fn set_buffer_size(&mut self, buffer_size: usize) {
        self.cap = buffer_size;
    }

    fn handle_result(&mut self, result: usize) {
        self.tail = result;
    }

    fn prefer_external_buffer(&self, buf: &mut [u8]) -> bool {
        buf.len() >= self.cap
    }
}

const DEFAULT_BUFFER_SIZE: usize = 8192;

#[derive(Debug)]
pub(crate) struct GlommioStream<S, B = Preallocated> {
    reactor: Weak<Reactor>,
    stream: S,
    source_tx: Option<Source>,
    source_rx: Option<Source>,
    write_timeout: Cell<Option<Duration>>,
    read_timeout: Cell<Option<Duration>>,
    rx_buf: B,
    rx_buf_size: usize,
}

impl<S: From<socket2::Socket>, B: RxBuf> From<socket2::Socket> for GlommioStream<S, B> {
    fn from(socket: socket2::Socket) -> GlommioStream<S, B> {
        let stream = socket.into();
        GlommioStream {
            reactor: Rc::downgrade(&crate::executor().reactor()),
            stream,
            source_tx: None,
            source_rx: None,
            write_timeout: Cell::new(None),
            read_timeout: Cell::new(None),
            rx_buf: B::default(),
            rx_buf_size: DEFAULT_BUFFER_SIZE,
        }
    }
}

impl<S: AsRawFd, B> AsRawFd for GlommioStream<S, B> {
    fn as_raw_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }
}

impl<S: FromRawFd + From<socket2::Socket>, B: RxBuf> FromRawFd for GlommioStream<S, B> {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        let socket = socket2::Socket::from_raw_fd(fd);
        GlommioStream::from(socket)
    }
}

impl<S: AsRawFd + Unpin, B: RxBuf> GlommioStream<S, B> {
    /// Receives data on the socket from the remote address to which it is
    /// connected, without removing that data from the queue.
    ///
    /// On success, returns the number of bytes peeked.
    /// Successive calls return the same data. This is accomplished by passing
    /// `MSG_PEEK` as a flag to the underlying `recv` system call.
    pub(crate) async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let source = self.reactor.upgrade().unwrap().recv(
            self.as_raw_fd(),
            RecvBuffer::allocated(buf),
            MsgFlags::MSG_PEEK,
        );
        source.collect_rw().await
    }

    pub(crate) fn poll_replenish_buffer(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.source_rx.is_none() {
            self.rx_buf.resize();
        }
        let result = poll_err!(ready!(self.poll_read(cx, None)));
        self.rx_buf.handle_result(result);
        Poll::Ready(Ok(()))
    }

    pub(crate) fn poll_fill_buf(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        if self.rx_buf.is_empty() {
            poll_err!(ready!(self.poll_replenish_buffer(cx)));
        }
        Poll::Ready(Ok(self.rx_buf.as_bytes()))
    }

    pub(crate) fn poll_buffered_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.rx_buf.is_empty() {
            // If we don't have any buffered data and we're doing a massive read
            // (larger than our internal buffer), bypass our internal buffer
            // entirely.
            #[cfg(feature = "trust-unmanaged-buffer")]
            if self.rx_buf.prefer_external_buffer(buf) {
                return self.poll_read(cx, Some(buf));
            }
            poll_err!(ready!(self.poll_replenish_buffer(cx)));
        }
        Poll::Ready(Ok(self.rx_buf.read(buf)))
    }

    fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: Option<&mut [u8]>,
    ) -> Poll<io::Result<usize>> {
        if self.source_rx.is_none() {
            let buf = self.rx_buf.recv_buffer(buf);
            if let Some(buf) = buf.as_mut() {
                poll_some!(super::yolo_recv(self.stream.as_raw_fd(), buf));
            }
            let reactor = self.reactor.upgrade().unwrap();
            self.source_rx =
                Some(reactor.rushed_recv(self.stream.as_raw_fd(), buf, self.read_timeout.get())?);
        }
        Self::poll_result(cx, &mut self.source_rx)
    }

    pub(crate) fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.source_tx.is_none() {
            poll_some!(super::yolo_send(self.stream.as_raw_fd(), buf));
            let reactor = self.reactor.upgrade().unwrap();
            #[cfg(not(feature = "trust-unmanaged-buffer"))]
            let buf = {
                let mut dma = reactor.alloc_dma_buffer(buf.len());
                dma.write_at(0, &buf);
                dma
            };
            self.source_tx = Some(reactor.rushed_send(
                self.stream.as_raw_fd(),
                buf.into(),
                self.read_timeout.get(),
            )?);
        }
        Self::poll_result(cx, &mut self.source_tx)
    }

    pub(crate) fn poll_close(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.source_tx.take();
        Poll::Ready(sys::shutdown(self.stream.as_raw_fd(), Shutdown::Write))
    }

    fn poll_result(cx: &mut Context<'_>, source: &mut Option<Source>) -> Poll<io::Result<usize>> {
        let src = source.take().unwrap();
        match src.result() {
            None => {
                src.add_waiter_single(cx.waker().clone());
                *source = Some(src);
                Poll::Pending
            }
            Some(result) => Poll::Ready(result),
        }
    }

    pub(crate) fn poll_flush(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// io_uring has support for shutdown now, but it is not in any released
    /// kernel. Even with my "let's use latest" policy it would be crazy to
    /// mandate a kernel that doesn't even exist. So in preparation for that
    /// we'll sync-emulate this but already on an async wrapper
    pub(crate) fn poll_shutdown(
        &self,
        _cx: &mut Context<'_>,
        how: Shutdown,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(sys::shutdown(self.as_raw_fd(), how))
    }

    pub(crate) fn consume(&mut self, amt: usize) {
        self.rx_buf.consume(amt);
    }

    pub(crate) fn set_write_timeout(&self, dur: Option<Duration>) -> Result<()> {
        if let Some(dur) = dur.as_ref() {
            if dur.as_nanos() == 0 {
                return Err(io::Error::from_raw_os_error(libc::EINVAL).into());
            }
        }
        self.write_timeout.set(dur);
        Ok(())
    }

    pub(crate) fn set_read_timeout(&self, dur: Option<Duration>) -> Result<()> {
        if let Some(dur) = dur.as_ref() {
            if dur.as_nanos() == 0 {
                return Err(io::Error::from_raw_os_error(libc::EINVAL).into());
            }
        }
        self.read_timeout.set(dur);
        Ok(())
    }

    pub(crate) fn write_timeout(&self) -> Option<Duration> {
        self.write_timeout.get()
    }

    pub(crate) fn read_timeout(&self) -> Option<Duration> {
        self.read_timeout.get()
    }

    pub(crate) fn buffer_size(&self) -> usize {
        self.rx_buf.buffer_size()
    }

    pub(crate) fn set_buffer_size(&mut self, buffer_size: usize) {
        self.rx_buf.set_buffer_size(buffer_size);
    }

    pub(crate) fn stream(&self) -> &S {
        &self.stream
    }
}
