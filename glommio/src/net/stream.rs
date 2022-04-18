// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::{
    reactor::Reactor,
    sys::{self, Source, SourceType},
};
use futures_lite::ready;
use nix::sys::socket::MsgFlags;
use std::{
    cell::Cell,
    io,
    net::Shutdown,
    os::unix::io::{AsRawFd, FromRawFd, RawFd},
    rc::{Rc, Weak},
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

type Result<T> = crate::Result<T, ()>;

/// Root trait for socket stream receive buffer
pub trait RxBuf {
    fn read(&mut self, buf: &mut [u8]) -> usize;
    fn peek(&self, buf: &mut [u8]) -> usize;
    fn is_empty(&self) -> bool;
    fn as_bytes(&self) -> &[u8];
    fn consume(&mut self, amt: usize);
    fn buffer_size(&self) -> usize;
    fn handle_result(&mut self, result: usize);
    fn unfilled(&mut self) -> &mut [u8];
}

#[derive(Debug, Default)]
pub struct NonBuffered;

impl RxBuf for NonBuffered {
    fn read(&mut self, _buf: &mut [u8]) -> usize {
        0
    }

    fn peek(&self, _buf: &mut [u8]) -> usize {
        0
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn as_bytes(&self) -> &[u8] {
        &[]
    }

    fn consume(&mut self, _amt: usize) {}

    fn buffer_size(&self) -> usize {
        0
    }

    fn handle_result(&mut self, _result: usize) {}

    fn unfilled(&mut self) -> &mut [u8] {
        &mut []
    }
}

/// Trait for receive buffer implementations
pub trait Buffered: RxBuf {}

/// Non-shared fixed sized receive buffer allocated
/// when buffered stream is created
#[derive(Debug)]
pub struct Preallocated {
    buf: Vec<u8>,
    head: usize,
    tail: usize,
    cap: usize,
}

impl Preallocated {
    const DEFAULT_BUFFER_SIZE: usize = 8192;

    /// Creates a fixed sized receive buffer
    pub fn new(size: usize) -> Self {
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
        Self::new(Self::DEFAULT_BUFFER_SIZE)
    }
}

impl Preallocated {
    fn len(&self) -> usize {
        self.tail - self.head
    }
}

impl Buffered for Preallocated {}

impl RxBuf for Preallocated {
    fn read(&mut self, buf: &mut [u8]) -> usize {
        let sz = std::cmp::min(self.len(), buf.len());
        if sz > 0 {
            buf[..sz].copy_from_slice(&self.buf[self.head..self.head + sz]);
            self.head += sz;
        }
        sz
    }

    fn peek(&self, buf: &mut [u8]) -> usize {
        let sz = std::cmp::min(self.len(), buf.len());
        if sz > 0 {
            buf[..sz].copy_from_slice(&self.buf[self.head..self.head + sz]);
        }
        sz
    }

    fn is_empty(&self) -> bool {
        self.head >= self.tail
    }

    fn as_bytes(&self) -> &[u8] {
        &self.buf[self.head..self.tail]
    }

    fn consume(&mut self, amt: usize) {
        self.head += std::cmp::min(self.len(), amt);
    }

    fn buffer_size(&self) -> usize {
        self.cap
    }

    fn handle_result(&mut self, result: usize) {
        self.tail += result;
    }

    fn unfilled(&mut self) -> &mut [u8] {
        if self.len() == 0 {
            self.head = 0;
            self.tail = 0;
        }
        &mut self.buf[self.tail..]
    }
}

#[derive(Debug)]
struct Timeout {
    id: u64,
    timeout: Cell<Option<Duration>>,
    timer: Cell<Option<Instant>>,
}

impl Timeout {
    fn new(id: u64) -> Self {
        Self {
            id,
            timeout: Cell::new(None),
            timer: Cell::new(None),
        }
    }

    fn get(&self) -> Option<Duration> {
        self.timeout.get()
    }

    fn set(&self, dur: Option<Duration>) -> Result<()> {
        if let Some(dur) = dur.as_ref() {
            if dur.as_nanos() == 0 {
                return Err(io::Error::from_raw_os_error(libc::EINVAL).into());
            }
        }
        self.timeout.set(dur);
        Ok(())
    }

    fn maybe_set_timer(&self, reactor: &Reactor, waker: &Waker) {
        if let Some(timeout) = self.timeout.get() {
            if self.timer.get().is_none() {
                let deadline = Instant::now() + timeout;
                reactor.insert_timer(self.id, deadline, waker.clone());
                self.timer.set(Some(deadline));
            }
        }
    }

    fn cancel_timer(&self, reactor: &Reactor) {
        if self.timer.take().is_some() {
            reactor.remove_timer(self.id);
        }
    }

    fn check(&self, reactor: &Reactor) -> io::Result<()> {
        if let Some(deadline) = self.timer.get() {
            if !reactor.timer_exists(&(deadline, self.id)) {
                reactor.remove_timer(self.id);
                self.timer.take();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Operation timed out",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct NonBufferedStream<S> {
    reactor: Weak<Reactor>,
    stream: S,
    source_tx: Option<Source>,
    source_rx: Option<Source>,
    write_timeout: Timeout,
    read_timeout: Timeout,
}

impl<S: AsRawFd> NonBufferedStream<S> {
    fn init(&mut self) {
        let reactor = self.reactor.upgrade().unwrap();
        let stream_fd = self.stream.as_raw_fd();
        self.source_rx = Some(reactor.poll_read_ready(stream_fd));
    }

    pub(crate) fn try_peek(&self, buf: &mut [u8]) -> Option<io::Result<usize>> {
        super::yolo_peek(self.stream.as_raw_fd(), buf)
    }

    pub(crate) async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let source = self.reactor.upgrade().unwrap().recv(
            self.stream.as_raw_fd(),
            buf.len(),
            MsgFlags::MSG_PEEK,
        );

        let sz = source.collect_rw().await?;
        match source.extract_source_type() {
            SourceType::SockRecv(mut src) => {
                buf[0..sz].copy_from_slice(&src.take().unwrap().as_bytes()[0..sz]);
            }
            _ => unreachable!(),
        }
        Ok(sz)
    }

    pub(crate) fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let reactor = self.reactor.upgrade().unwrap();
        let reactor = reactor.as_ref();

        let no_pending_poll = self
            .source_rx
            .as_ref()
            .map(|src| src.result().is_some())
            .unwrap_or(true);

        if no_pending_poll {
            if let Some(result) = super::yolo_recv(self.stream.as_raw_fd(), buf) {
                self.source_rx.take();
                self.read_timeout.cancel_timer(reactor);
                let result = poll_err!(result);
                // Start an early poll if the buffer is not fully filled. So when
                // the next time `poll_read` is called, it will be known immediately
                // whether the underlying stream is ready for reading.
                if result > 0 && result < buf.len() {
                    self.source_rx = Some(reactor.poll_read_ready(self.stream.as_raw_fd()));
                    // The `rush_dispatch`s here and after could be removed to
                    // improve performance if #458 is handled appropriately.
                    // reactor.rush_dispatch(self.source_rx.as_ref().unwrap());
                }
                return Poll::Ready(Ok(result));
            }
        }

        poll_err!(self.read_timeout.check(reactor));

        if no_pending_poll {
            self.source_rx = Some(reactor.poll_read_ready(self.stream.as_raw_fd()));
            // reactor.rush_dispatch(self.source_rx.as_ref().unwrap());
        }

        let source = self.source_rx.as_ref().unwrap();
        source.add_waiter_single(cx.waker());
        self.read_timeout.maybe_set_timer(reactor, cx.waker());
        Poll::Pending
    }

    pub(crate) fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // On the write path, we always start with calling `yolo_send`, because
        // it is very likely to success. It could be a waste if it already timed
        // out since the last `poll_write`, but it would not cost much more to
        // give it one last chance in this case.
        if let Some(result) = super::yolo_send(self.stream.as_raw_fd(), buf) {
            let reactor = self.reactor.upgrade().unwrap();
            self.write_timeout.cancel_timer(reactor.as_ref());
            self.source_tx.take();
            return Poll::Ready(result);
        }

        let reactor = self.reactor.upgrade().unwrap();
        let reactor = reactor.as_ref();
        poll_err!(self.write_timeout.check(reactor));

        let no_pending_poll = self
            .source_tx
            .as_ref()
            .map(|src| src.result().is_some())
            .unwrap_or(true);

        if no_pending_poll {
            self.source_tx = Some(reactor.poll_write_ready(self.stream.as_raw_fd()));
        }

        let source = self.source_tx.as_ref().unwrap();
        source.add_waiter_single(cx.waker());
        self.write_timeout.maybe_set_timer(reactor, cx.waker());
        Poll::Pending
    }

    pub(crate) fn poll_close(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.source_tx.take();
        Poll::Ready(sys::shutdown(self.stream.as_raw_fd(), Shutdown::Write))
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
        Poll::Ready(sys::shutdown(self.stream.as_raw_fd(), how))
    }
}

#[derive(Debug)]
pub(crate) struct GlommioStream<S, B> {
    stream: NonBufferedStream<S>,
    rx_buf: B,
    rx_done: Cell<bool>,
}

impl<S> From<socket2::Socket> for GlommioStream<S, NonBuffered>
where
    S: AsRawFd + From<socket2::Socket> + Unpin,
{
    fn from(socket: socket2::Socket) -> Self {
        let reactor = crate::executor().reactor();
        let mut stream = NonBufferedStream {
            reactor: Rc::downgrade(&reactor),
            stream: socket.into(),
            source_tx: None,
            source_rx: None,
            write_timeout: Timeout::new(reactor.register_timer()),
            read_timeout: Timeout::new(reactor.register_timer()),
        };
        stream.init();
        GlommioStream {
            stream,
            rx_buf: NonBuffered,
            rx_done: Cell::new(false),
        }
    }
}

impl<S: AsRawFd> AsRawFd for GlommioStream<S, NonBuffered> {
    fn as_raw_fd(&self) -> RawFd {
        self.stream.stream.as_raw_fd()
    }
}

impl<S> FromRawFd for GlommioStream<S, NonBuffered>
where
    S: AsRawFd + FromRawFd + From<socket2::Socket> + Unpin,
{
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        let socket = socket2::Socket::from_raw_fd(fd);
        GlommioStream::from(socket)
    }
}

impl<S> GlommioStream<S, NonBuffered> {
    pub(crate) fn buffered_with<B: Buffered>(self, rx_buf: B) -> GlommioStream<S, B> {
        GlommioStream {
            stream: self.stream,
            rx_buf,
            rx_done: self.rx_done,
        }
    }
}

impl<S: AsRawFd, B: RxBuf> GlommioStream<S, B> {
    /// Receives data on the socket from the remote address to which it is
    /// connected, without removing that data from the queue.
    ///
    /// On success, returns the number of bytes peeked.
    /// Successive calls return the same data. This is accomplished by passing
    /// `MSG_PEEK` as a flag to the underlying `recv` system call.
    pub(crate) async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let mut pos = self.rx_buf.peek(buf);
        if pos < buf.len() && !self.rx_done.get() {
            if let Some(result) = self.stream.try_peek(&mut buf[pos..]) {
                match result {
                    Err(e) => return Err(e),
                    Ok(len) => {
                        pos += len;
                        if len == 0 {
                            self.rx_done.set(true);
                        }
                    }
                }
            }
        }
        if pos > 0 || self.rx_done.get() {
            return Ok(pos);
        }
        self.stream.peek(buf).await
    }

    pub(crate) fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        if self.rx_buf.is_empty() {
            if buf.len() >= self.rx_buf.buffer_size() {
                return self.stream.poll_read(cx, buf);
            }
            if !self.rx_done.get() {
                poll_err!(ready!(self.poll_replenish_buffer(cx)));
            }
        }
        Poll::Ready(Ok(self.rx_buf.read(buf)))
    }

    fn poll_replenish_buffer(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        let result = poll_err!(ready!(self.stream.poll_read(cx, self.rx_buf.unfilled())));
        self.rx_buf.handle_result(result);
        if result == 0 {
            self.rx_done.set(true);
        }
        Poll::Ready(Ok(result))
    }

    pub(crate) fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.stream.poll_write(cx, buf)
    }

    pub(crate) fn poll_flush(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    pub(crate) fn poll_close(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.stream.poll_close(cx)
    }

    pub(crate) fn poll_shutdown(
        &self,
        cx: &mut Context<'_>,
        how: Shutdown,
    ) -> Poll<io::Result<()>> {
        self.stream.poll_shutdown(cx, how)
    }

    pub(crate) fn set_write_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.stream.write_timeout.set(dur)
    }

    pub(crate) fn set_read_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.stream.read_timeout.set(dur)
    }

    pub(crate) fn write_timeout(&self) -> Option<Duration> {
        self.stream.write_timeout.get()
    }

    pub(crate) fn read_timeout(&self) -> Option<Duration> {
        self.stream.read_timeout.get()
    }

    pub(crate) fn stream(&self) -> &S {
        &self.stream.stream
    }
}

impl<S: AsRawFd, B: Buffered> GlommioStream<S, B> {
    pub(crate) fn poll_fill_buf(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<&[u8]>> {
        if self.rx_buf.is_empty() {
            poll_err!(ready!(self.poll_replenish_buffer(cx)));
        }
        Poll::Ready(Ok(self.rx_buf.as_bytes()))
    }

    pub(crate) fn consume(&mut self, amt: usize) {
        self.rx_buf.consume(amt);
    }
}
