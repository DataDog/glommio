// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::{
    parking::Reactor,
    sys::{self, DmaBuffer, Source, SourceType},
    ByteSliceMutExt,
    Local,
};
use futures_lite::ready;
use nix::sys::socket::MsgFlags;
use std::{
    cell::Cell,
    convert::TryFrom,
    io,
    net::Shutdown,
    os::unix::io::{AsRawFd, FromRawFd, RawFd},
    rc::{Rc, Weak},
    task::{Context, Poll},
    time::Duration,
};

type Result<T> = crate::Result<T, ()>;

struct RecvBuffer {
    buf: DmaBuffer,
}

impl TryFrom<Source> for RecvBuffer {
    type Error = io::Error;

    fn try_from(source: Source) -> io::Result<RecvBuffer> {
        match source.extract_source_type() {
            SourceType::SockRecv(mut buf) => {
                let sz = source.take_result().unwrap()?;
                let mut buf = buf.take().unwrap();
                buf.trim_to_size(sz);
                Ok(RecvBuffer { buf })
            }
            _ => unreachable!(),
        }
    }
}

const DEFAULT_BUFFER_SIZE: usize = 8192;

#[derive(Debug)]
pub(crate) struct GlommioStream<S: AsRawFd + FromRawFd + From<socket2::Socket>> {
    pub(crate) reactor: Weak<Reactor>,
    pub(crate) stream: S,
    pub(crate) source_tx: Option<Source>,
    pub(crate) source_rx: Option<Source>,

    pub(crate) write_timeout: Cell<Option<Duration>>,
    pub(crate) read_timeout: Cell<Option<Duration>>,

    // you only live once, you've got no time to block! if this is set to true try a direct
    // non-blocking syscall otherwise schedule for sending later over the ring
    //
    // If you are familiar with high throughput networking code you might have seen similar
    // techniques with names such as "optimistic" "speculative" or things like that. But frankly
    // "yolo" is such a better name. Calling this "yolo" is likely glommio's biggest
    // contribution to humankind.
    pub(crate) tx_yolo: bool,
    pub(crate) rx_yolo: bool,

    pub(crate) rx_buf: Option<DmaBuffer>,
    pub(crate) tx_buf: Option<DmaBuffer>,

    pub(crate) rx_buf_size: usize,
}

impl<S: AsRawFd + FromRawFd + From<socket2::Socket>> From<socket2::Socket> for GlommioStream<S> {
    fn from(socket: socket2::Socket) -> GlommioStream<S> {
        let stream = socket.into();
        GlommioStream {
            reactor: Rc::downgrade(&Local::get_reactor()),
            stream,
            source_tx: None,
            source_rx: None,
            write_timeout: Cell::new(None),
            read_timeout: Cell::new(None),
            tx_yolo: true,
            rx_yolo: true,
            rx_buf: None,
            tx_buf: None,
            rx_buf_size: DEFAULT_BUFFER_SIZE,
        }
    }
}

impl<S: AsRawFd + FromRawFd + From<socket2::Socket>> AsRawFd for GlommioStream<S> {
    fn as_raw_fd(&self) -> RawFd {
        self.stream.as_raw_fd()
    }
}

impl<S: FromRawFd + AsRawFd + From<socket2::Socket>> FromRawFd for GlommioStream<S> {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        let socket = socket2::Socket::from_raw_fd(fd);
        GlommioStream::from(socket)
    }
}

impl<S: FromRawFd + AsRawFd + From<socket2::Socket>> GlommioStream<S> {
    /// Receives data on the socket from the remote address to which it is
    /// connected, without removing that data from the queue.
    ///
    /// On success, returns the number of bytes peeked.
    /// Successive calls return the same data. This is accomplished by passing
    /// MSG_PEEK as a flag to the underlying recv system call.
    pub(crate) async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let source = self.reactor.upgrade().unwrap().recv(
            self.stream.as_raw_fd(),
            buf.len(),
            MsgFlags::MSG_PEEK,
        );

        let sz = source.collect_rw().await?;
        match source.extract_source_type() {
            SourceType::SockRecv(mut src) => {
                let mut src = src.take().unwrap();
                src.trim_to_size(sz);
                buf[0..sz].copy_from_slice(&src.as_bytes()[0..sz]);
            }
            _ => unreachable!(),
        }
        Ok(sz)
    }

    fn consume_receive_buffer(&mut self, buf: &mut [u8]) -> Option<io::Result<usize>> {
        if let Some(src) = self.rx_buf.as_mut() {
            let sz = std::cmp::min(src.len(), buf.len());
            buf[0..sz].copy_from_slice(&src.as_bytes()[0..sz]);
            src.trim_front(sz);
            if src.is_empty() {
                self.rx_buf.take();
            }
            Some(Ok(sz))
        } else {
            None
        }
    }

    // io_uring has support for shutdown now but it is not in any released kernel.
    // Even with my "let's use latest" policy it would be crazy to mandate a kernel
    // that doesn't even exist. So in preparation for that we'll sync-emulate this
    // but already on an async wrapper
    pub(crate) fn poll_shutdown(
        &self,
        _cx: &mut Context<'_>,
        how: Shutdown,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(sys::shutdown(self.stream.as_raw_fd(), how))
    }

    pub(crate) fn allocate_buffer(&self, size: usize) -> DmaBuffer {
        self.reactor.upgrade().unwrap().alloc_dma_buffer(size)
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

    pub(crate) fn yolo_rx(&mut self, buf: &mut [u8]) -> Option<io::Result<usize>> {
        if self.rx_yolo {
            super::yolo_recv(self.stream.as_raw_fd(), buf)
        } else {
            None
        }
        .or_else(|| {
            self.rx_yolo = false;
            None
        })
    }

    pub(crate) fn yolo_tx(&mut self, buf: &[u8]) -> Option<io::Result<usize>> {
        if self.tx_yolo {
            super::yolo_send(self.stream.as_raw_fd(), buf)
        } else {
            None
        }
        .or_else(|| {
            self.tx_yolo = false;
            None
        })
    }

    pub(crate) fn poll_replenish_buffer(
        &mut self,
        cx: &mut Context<'_>,
        size: usize,
    ) -> Poll<io::Result<usize>> {
        let source = match self.source_rx.take() {
            Some(source) => source,
            None => poll_err!(self.reactor.upgrade().unwrap().rushed_recv(
                self.stream.as_raw_fd(),
                size,
                self.read_timeout.get()
            )),
        };

        if !source.has_result() {
            source.add_waiter(cx.waker().clone());
            self.source_rx = Some(source);
            Poll::Pending
        } else {
            let buf = poll_err!(RecvBuffer::try_from(source));
            self.rx_yolo = true;
            self.rx_buf = Some(buf.buf);
            Poll::Ready(Ok(self.rx_buf.as_ref().unwrap().len()))
        }
    }

    pub(crate) fn write_dma(
        &mut self,
        cx: &mut Context<'_>,
        buf: DmaBuffer,
    ) -> Poll<io::Result<usize>> {
        let source = match self.source_tx.take() {
            Some(source) => source,
            None => poll_err!(self.reactor.upgrade().unwrap().rushed_send(
                self.stream.as_raw_fd(),
                buf,
                self.write_timeout.get()
            )),
        };

        match source.take_result() {
            None => {
                source.add_waiter(cx.waker().clone());
                self.source_tx = Some(source);
                Poll::Pending
            }
            Some(res) => {
                self.tx_yolo = true;
                Poll::Ready(res)
            }
        }
    }

    pub(crate) fn poll_read(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        poll_some!(self.consume_receive_buffer(buf));
        poll_some!(self.yolo_rx(buf));
        poll_err!(ready!(self.poll_replenish_buffer(cx, buf.len())));
        poll_some!(self.consume_receive_buffer(buf));
        unreachable!();
    }

    pub(crate) fn consume(&mut self, amt: usize) {
        let buf_ref = self.rx_buf.as_mut().unwrap();
        let amt = std::cmp::min(amt, buf_ref.len());
        buf_ref.trim_front(amt);
        if buf_ref.is_empty() {
            self.rx_buf.take();
        }
    }

    pub(crate) fn poll_write(
        &mut self,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        poll_some!(self.yolo_tx(buf));
        let mut dma = self.allocate_buffer(buf.len());
        assert_eq!(dma.write_at(0, buf), buf.len());
        self.write_dma(cx, dma)
    }

    pub(crate) fn poll_flush(&self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    pub(crate) fn poll_close(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.source_tx.take();
        Poll::Ready(sys::shutdown(self.stream.as_raw_fd(), Shutdown::Write))
    }
}
