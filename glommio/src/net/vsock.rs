use super::stream::GlommioStream;
use crate::{
    net::{
        stream::{Buffered, NonBuffered, Preallocated, RxBuf},
        yolo_accept,
    },
    reactor::Reactor,
    GlommioError,
};
use futures_lite::{
    io::{AsyncBufRead, AsyncRead, AsyncWrite},
    stream,
};
use nix::sys::socket::SockAddr;
use pin_project_lite::pin_project;
use socket2::{Domain, Socket, Type};
use std::{
    io,
    os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context, Poll},
};

type Result<T> = crate::Result<T, ()>;

#[derive(Debug)]
pub struct VsockListener {
    reactor: Weak<Reactor>,
    listener: vsock::VsockListener,
}

impl VsockListener {
    pub fn bind_with_cid_port(cid: u32, port: u32) -> Result<VsockListener> {
        let listener = vsock::VsockListener::bind_with_cid_port(cid, port)?;

        Ok(VsockListener {
            reactor: Rc::downgrade(&crate::executor().reactor()),
            listener,
        })
    }

    pub async fn shared_accept(&self) -> Result<AcceptedVsockStream> {
        let reactor = self.reactor.upgrade().unwrap();
        let raw_fd = self.listener.as_raw_fd();
        if let Some(r) = yolo_accept(raw_fd) {
            match r {
                Ok(fd) => {
                    return Ok(AcceptedVsockStream { fd });
                }
                Err(err) => return Err(GlommioError::IoError(err)),
            }
        }
        let source = reactor.accept(self.listener.as_raw_fd());
        let fd = source.collect_rw().await?;
        Ok(AcceptedVsockStream { fd: fd as RawFd })
    }

    pub async fn accept(&self) -> Result<VsockStream> {
        Ok(self.shared_accept().await?.bind_to_executor())
    }

    pub fn incoming(&self) -> impl stream::Stream<Item = Result<VsockStream>> + Unpin + '_ {
        Box::pin(stream::unfold(self, |listener| async move {
            Some((listener.accept().await, listener))
        }))
    }
}

#[derive(Copy, Clone, Debug)]
pub struct AcceptedVsockStream {
    fd: RawFd,
}

impl AcceptedVsockStream {
    pub fn bind_to_executor(self) -> VsockStream {
        VsockStream {
            stream: unsafe { GlommioStream::from_raw_fd(self.fd) },
        }
    }
}

pin_project! {
    #[derive(Debug)]
    pub struct VsockStream<B: RxBuf = NonBuffered> {
        stream: GlommioStream<Stream, B>,
    }
}

impl VsockStream {
    pub async fn connect_with_cid_port(cid: u32, port: u32) -> Result<VsockStream> {
        let socket = Socket::new(Domain::VSOCK, Type::STREAM, None)?;
        let addr = SockAddr::new_vsock(cid, port);
        let reactor = crate::executor().reactor();
        let source = reactor.connect(socket.as_raw_fd(), addr);
        source.collect_rw().await?;

        Ok(VsockStream {
            stream: GlommioStream::from(socket),
        })
    }

    pub fn buffered(self) -> VsockStream<Preallocated> {
        self.buffered_with(Preallocated::default())
    }

    pub fn buffered_with<B: Buffered>(self, buf: B) -> VsockStream<B> {
        VsockStream {
            stream: self.stream.buffered_with(buf),
        }
    }
}

impl<B: Buffered + Unpin> AsyncBufRead for VsockStream<B> {
    fn poll_fill_buf<'a>(
        self: Pin<&'a mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<&'a [u8]>> {
        self.project().stream.poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.stream.consume(amt);
    }
}

impl<B: RxBuf + Unpin> AsyncRead for VsockStream<B> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl<B: RxBuf + Unpin> AsyncWrite for VsockStream<B> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_close(cx)
    }
}

#[derive(Debug)]
struct Stream(vsock::VsockStream);

impl From<socket2::Socket> for Stream {
    fn from(socket: socket2::Socket) -> Stream {
        Self(unsafe { vsock::VsockStream::from_raw_fd(socket.into_raw_fd()) })
    }
}

impl AsRawFd for Stream {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

impl FromRawFd for Stream {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        Self(vsock::VsockStream::from_raw_fd(fd))
    }
}
