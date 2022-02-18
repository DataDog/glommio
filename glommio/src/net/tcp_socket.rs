// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
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
    future::poll_fn,
    io::{AsyncBufRead, AsyncRead, AsyncWrite},
    stream::{self, Stream},
};
use nix::sys::socket::{InetAddr, SockAddr};
use pin_project_lite::pin_project;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    io,
    net::{self, Shutdown, SocketAddr, ToSocketAddrs},
    os::unix::io::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context, Poll},
    time::Duration,
};

type Result<T> = crate::Result<T, ()>;

#[derive(Debug)]
/// A TCP socket server, listening for connections.
///
/// After creating a TcpListener by binding it to a socket address, it listens
/// for incoming TCP connections. These can be accepted by calling [`accept`] or
/// [`shared_accept`], or by iterating over the Incoming iterator returned by
/// [`incoming`].
///
/// A good networking architecture within a thread-per-core model needs to take
/// into account parallelism and spawn work into multiple executors. If
/// everything happens inside the same Executor, then at most one thread is
/// used. Sometimes this is what you want: you may want to dedicate a CPU
/// entirely for networking, or even use specialized ports for each CPU of the
/// application, but most likely it isn't.
///
/// There are two approaches to load balancing possible with the `TcpListener`:
///
/// * By default, the ReusePort flag is set in the socket automatically. The OS
///   already provides some load balancing capabilities with that so you can
///   simply [`bind`] to the same address from many executors.
///
/// * If that is insufficient or otherwise not desirable, it is possible to use
///   [`shared_accept`] instead of [`accept`]: that returns an object that
///   implements [`Send`]. You can then use a [`shared_channel`] to send the
///   accepted connection into multiple executors. The object returned by
///   [`shared_accept`] can then be bound to its executor with
///   [`bind_to_executor`], at which point it becomes a standard [`TcpStream`].
///
/// Relying on the OS is definitely simpler, but which approach is better
/// depends on the specific needs of your application.
///
/// The socket will be closed when the value is dropped.
///
/// [`accept`]: TcpListener::accept
/// [`shared_accept`]: TcpListener::shared_accept
/// [`bind`]: TcpListener::bind
/// [`incoming`]: TcpListener::incoming
/// [`bind_to_executor`]: AcceptedTcpStream::bind_to_executor
/// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
/// [`shared_channel`]: ../channels/shared_channel/index.html
pub struct TcpListener {
    reactor: Weak<Reactor>,
    listener: net::TcpListener,
}

impl FromRawFd for TcpListener {
    /// Convert an already bound and listening RawFd into a TcpListener
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        let sk = Socket::from_raw_fd(fd);
        let listener = sk.into();

        TcpListener {
            reactor: Rc::downgrade(&crate::executor().reactor()),
            listener,
        }
    }
}

impl TcpListener {
    /// Creates a TCP listener bound to the specified address.
    ///
    /// Binding with port number 0 will request an available port from the OS.
    ///
    /// This method sets the ReusePort option in the bound socket, so it is
    /// designed to be called from multiple executors to achieve
    /// parallelism.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     println!("Listening on {}", listener.local_addr().unwrap());
    /// });
    /// ```
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<TcpListener> {
        let addr = addr
            .to_socket_addrs()
            .unwrap()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "empty address"))?;

        let domain = if addr.is_ipv6() {
            Domain::IPV6
        } else {
            Domain::IPV4
        };
        let sk = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        let addr = socket2::SockAddr::from(addr);
        sk.set_reuse_port(true)?;
        sk.bind(&addr)?;
        sk.listen(1024)?;
        let listener = sk.into();

        Ok(TcpListener {
            reactor: Rc::downgrade(&crate::executor().reactor()),
            listener,
        })
    }

    /// Accepts a new incoming TCP connection and allows the result to be sent
    /// to a foreign executor
    ///
    /// This is similar to [`accept`], except it returns an
    /// [`AcceptedTcpStream`] instead of a [`TcpStream`].
    /// [`AcceptedTcpStream`] implements [`Send`], so it can be safely sent
    /// for processing over a shared channel to a different executor.
    ///
    /// This is useful when the user wants to do her own load balancing across
    /// multiple executors instead of relying on the load balancing the OS
    /// would do with the ReusePort property of the bound socket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     let stream = listener.shared_accept().await.unwrap();
    /// });
    /// ```
    ///
    /// [`accept`]: TcpListener::accept
    /// [`AcceptedTcpStream`]: struct.AcceptedTcpStream.html
    /// [`TcpStream`]: struct.TcpStream.html
    /// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
    pub async fn shared_accept(&self) -> Result<AcceptedTcpStream> {
        let reactor = self.reactor.upgrade().unwrap();
        let raw_fd = self.listener.as_raw_fd();
        if let Some(r) = yolo_accept(raw_fd) {
            match r {
                Ok(fd) => {
                    return Ok(AcceptedTcpStream { fd });
                }
                Err(err) => return Err(GlommioError::IoError(err)),
            }
        }
        let source = reactor.accept(self.listener.as_raw_fd());
        let fd = source.collect_rw().await?;
        Ok(AcceptedTcpStream { fd: fd as RawFd })
    }

    /// Accepts a new incoming TCP connection in this executor
    ///
    /// This is similar to calling [`shared_accept`] and [`bind_to_executor`] in
    /// a single operation.
    ///
    /// If this connection once accepted is to be handled by the same executor
    /// in which it was accepted, this version is preferred.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use futures_lite::stream::StreamExt;
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     let stream = listener.accept().await.unwrap();
    ///     println!("Accepted client: {:?}", stream.local_addr());
    /// });
    /// ```
    ///
    /// [`shared_accept`]: TcpListener::accept
    /// [`bind_to_executor`]: AcceptedTcpStream::bind_to_executor
    pub async fn accept(&self) -> Result<TcpStream> {
        let a = self.shared_accept().await?;
        Ok(a.bind_to_executor())
    }

    /// Creates a stream of incoming connections
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use futures_lite::stream::StreamExt;
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     let mut incoming = listener.incoming();
    ///     while let Some(conn) = incoming.next().await {
    ///         println!("Accepted client: {:?}", conn);
    ///     }
    /// });
    /// ```
    pub fn incoming(&self) -> impl Stream<Item = Result<TcpStream>> + Unpin + '_ {
        Box::pin(stream::unfold(self, |listener| async move {
            let res = listener.accept().await;
            Some((res, listener))
        }))
    }

    /// Returns the socket address of the local half of this TCP connection.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     println!("Listening on {}", listener.local_addr().unwrap());
    /// });
    /// ```
    pub fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.listener.local_addr()?)
    }

    /// Gets the value of the `IP_TTL` option for this socket.
    ///
    /// This option configures the time-to-live field that is used in every
    /// packet sent from this socket.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///
    ///     listener.set_ttl(100).expect("could not set TTL");
    ///     assert_eq!(listener.ttl().unwrap(), 100);
    /// });
    /// ```
    pub fn ttl(&self) -> Result<u32> {
        Ok(self.listener.ttl()?)
    }

    /// Sets the value of the `IP_TTL` option for this socket.
    ///
    /// This option configures the time-to-live field that is used in every
    /// packet sent from this socket.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{net::TcpListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///
    ///     listener.set_ttl(100).expect("could not set TTL");
    ///     assert_eq!(listener.ttl().unwrap(), 100);
    /// });
    /// ```
    pub fn set_ttl(&self, ttl: u32) -> Result<()> {
        Ok(self.listener.set_ttl(ttl)?)
    }
}

#[derive(Copy, Clone, Debug)]
/// An Accepted Tcp connection that can be moved to a different executor
///
/// This is useful in situations where the load balancing provided by the
/// Operating System through ReusePort is not desirable. The user can accept the
/// connection in one executor through [`shared_accept`] which returns an
/// AcceptedTcpStream.
///
/// Once the `AcceptedTcpStream` arrives at its destination it can then be made
/// active with [`bind_to_executor`]
///
/// [`shared_accept`]: TcpListener::shared_accept
/// [`bind_to_executor`]: AcceptedTcpStream::bind_to_executor
pub struct AcceptedTcpStream {
    fd: RawFd,
}

impl AcceptedTcpStream {
    /// Returns the socket address of the remote peer
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        let socket = unsafe { Socket::from_raw_fd(self.fd) };
        let sock_addr = socket.peer_addr()?;
        socket.into_raw_fd();
        Ok(sock_addr.as_socket().unwrap())
    }

    /// Binds this `AcceptedTcpStream` to the current executor
    ///
    /// This returns a [`TcpStream`] that can then be used normally
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{
    ///     channels::shared_channel,
    ///     net::TcpListener,
    ///     LocalExecutor,
    ///     LocalExecutorBuilder,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let (sender, receiver) = shared_channel::new_bounded(1);
    ///     let sender = sender.connect().await;
    ///
    ///     let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    ///
    ///     let accepted = listener.shared_accept().await.unwrap();
    ///     sender.try_send(accepted).unwrap();
    ///
    ///     let ex1 = LocalExecutorBuilder::default()
    ///         .spawn(move || async move {
    ///             let receiver = receiver.connect().await;
    ///             let accepted = receiver.recv().await.unwrap();
    ///             let _ = accepted.bind_to_executor();
    ///         })
    ///         .unwrap();
    ///
    ///     ex1.join().unwrap();
    /// });
    /// ```
    pub fn bind_to_executor(self) -> TcpStream {
        TcpStream {
            stream: unsafe { GlommioStream::from_raw_fd(self.fd) },
        }
    }
}

pin_project! {
    #[derive(Debug)]
    /// A Tcp Stream of bytes. This can be used with [`AsyncRead`], [`AsyncBufRead`] and
    /// [`AsyncWrite`]
    ///
    /// [`AsyncRead`]: https://docs.rs/futures-io/0.3.8/futures_io/trait.AsyncRead.html
    /// [`AsyncBufRead`]: https://docs.rs/futures-io/0.3.8/futures_io/trait.AsyncBufRead.html
    /// [`AsyncWrite`]: https://docs.rs/futures-io/0.3.8/futures_io/trait.AsyncWrite.html

    pub struct TcpStream<B: RxBuf = NonBuffered> {
        stream: GlommioStream<net::TcpStream, B>
    }
}

impl From<socket2::Socket> for TcpStream {
    fn from(socket: socket2::Socket) -> TcpStream {
        Self {
            stream: GlommioStream::from(socket),
        }
    }
}

impl<B: RxBuf> AsRawFd for TcpStream<B> {
    fn as_raw_fd(&self) -> RawFd {
        self.stream.stream().as_raw_fd()
    }
}

impl FromRawFd for TcpStream {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        let socket = socket2::Socket::from_raw_fd(fd);
        TcpStream::from(socket)
    }
}

fn make_tcp_socket(addr: &SocketAddr) -> io::Result<(SockAddr, Socket)> {
    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
    let inet = InetAddr::from_std(addr);
    let addr = SockAddr::new_inet(inet);
    Ok((addr, socket))
}

impl TcpStream {
    /// Creates a TCP connection to the specified address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     TcpStream::connect("127.0.0.1:10000").await.unwrap();
    /// })
    /// ```
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<TcpStream> {
        let addr = addr.to_socket_addrs()?.next().unwrap();
        let (addr, socket) = make_tcp_socket(&addr)?;
        let reactor = crate::executor().reactor();
        let source = reactor.connect(socket.as_raw_fd(), addr);
        source.collect_rw().await?;

        Ok(TcpStream {
            stream: GlommioStream::from(socket),
        })
    }

    /// Creates a TCP connection to the specified address with a timeout.
    ///
    /// It is an error to pass a zero `Duration` to this function.
    ///
    /// Timeouts are implemented using `io_uring`'s `IORING_OP_LINK_TIMEOUT`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// use std::time::Duration;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     TcpStream::connect_timeout("127.0.0.1:10000", Duration::from_secs(10))
    ///         .await
    ///         .unwrap();
    /// })
    /// ```
    pub async fn connect_timeout<A: ToSocketAddrs>(
        addr: A,
        duration: Duration,
    ) -> Result<TcpStream> {
        if duration.as_secs() == 0 && duration.subsec_nanos() == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot set a 0 duration timeout",
            )
            .into());
        }

        let addr = addr.to_socket_addrs()?.next().unwrap();
        let (addr, socket) = make_tcp_socket(&addr)?;
        let reactor = crate::executor().reactor();
        let source = reactor.connect_timeout(socket.as_raw_fd(), addr, duration);

        // connect_timeout submits two sqes to io_uring: a connect sqe soft-linked
        // with a LINK_TIMEOUT sqe. If the timeout fires, the connect sqe fails with
        // ECANCELED. We map that error to TimedOut to match the standard library's API.
        source
            .collect_rw()
            .await
            .map_err(|err| match err.raw_os_error() {
                Some(libc::ECANCELED) => {
                    io::Error::new(io::ErrorKind::TimedOut, "connection timed out")
                }
                _ => err,
            })?;

        Ok(TcpStream {
            stream: GlommioStream::from(socket),
        })
    }

    /// Creates a buffered TCP connection with default receive buffer.
    pub fn buffered(self) -> TcpStream<Preallocated> {
        self.buffered_with(Preallocated::default())
    }

    /// Creates a buffered TCP connection with custom receive buffer.
    pub fn buffered_with<B: Buffered>(self, buf: B) -> TcpStream<B> {
        TcpStream {
            stream: self.stream.buffered_with(buf),
        }
    }
}

impl<B: RxBuf> TcpStream<B> {
    /// Sets the read timeout to the timeout specified.
    ///
    /// If the value specified is [`None`], then read calls will block
    /// indefinitely. An [`Err`] is returned if the zero [`Duration`] is
    /// passed to this method.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::TcpStream, LocalExecutor};
    /// # use std::time::Duration;
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    /// stream
    ///     .set_read_timeout(Some(Duration::from_secs(1)))
    ///     .unwrap();
    /// # })
    /// ```
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.stream.set_read_timeout(dur)
    }

    /// Sets the write timeout to the timeout specified.
    ///
    /// If the value specified is [`None`], then write calls will block
    /// indefinitely. An [`Err`] is returned if the zero [`Duration`] is
    /// passed to this method.
    ///
    /// ```no_run
    /// # use glommio::{net::TcpStream, LocalExecutor};
    /// # use std::time::Duration;
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    /// stream
    ///     .set_write_timeout(Some(Duration::from_secs(1)))
    ///     .unwrap();
    /// # })
    /// ```
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.stream.set_write_timeout(dur)
    }

    /// Returns the read timeout of this socket.
    pub fn read_timeout(&self) -> Option<Duration> {
        self.stream.read_timeout()
    }

    /// Returns the write timeout of this socket.
    pub fn write_timeout(&self) -> Option<Duration> {
        self.stream.write_timeout()
    }

    /// Shuts down the read, write, or both halves of this connection.
    pub async fn shutdown(&self, how: Shutdown) -> Result<()> {
        poll_fn(|cx| self.stream.poll_shutdown(cx, how))
            .await
            .map_err(Into::into)
    }

    /// Sets the value of the `TCP_NODELAY` option on this socket.
    ///
    /// If set, this option disables the Nagle algorithm. This means that
    /// segments are always sent as soon as possible, even if there is only a
    /// small amount of data. When not set, data is buffered until there is a
    /// sufficient amount to send out, thereby avoiding the frequent sending of
    /// small packets.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     stream.set_nodelay(true).expect("set_nodelay call failed");
    /// });
    /// ```
    pub fn set_nodelay(&self, value: bool) -> Result<()> {
        self.stream.stream().set_nodelay(value).map_err(Into::into)
    }

    /// Gets the `TCP_NODELAY` option on this socket.
    ///
    /// For more information about this option, see [`TcpStream::set_nodelay`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     stream.set_nodelay(true).expect("set_nodelay call failed");
    ///     assert_eq!(stream.nodelay().unwrap(), true);
    /// });
    /// ```
    pub fn nodelay(&self) -> Result<bool> {
        self.stream.stream().nodelay().map_err(Into::into)
    }

    /// Gets the value of the `IP_TTL` option for this socket.
    ///
    /// This option configures the time-to-live field that is used in every
    /// packet sent from this socket.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     stream.set_ttl(100).expect("could not set TTL");
    ///     assert_eq!(stream.ttl().unwrap(), 100);
    /// });
    /// ```
    pub fn ttl(&self) -> Result<u32> {
        Ok(self.stream.stream().ttl()?)
    }

    /// Sets the value of the `IP_TTL` option for this socket.
    ///
    /// This option configures the time-to-live field that is used in every
    /// packet sent from this socket.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     stream.set_ttl(100).expect("could not set TTL");
    ///     assert_eq!(stream.ttl().unwrap(), 100);
    /// });
    /// ```
    pub fn set_ttl(&self, ttl: u32) -> Result<()> {
        Ok(self.stream.stream().set_ttl(ttl)?)
    }

    /// Receives data on the socket from the remote address to which it is
    /// connected, without removing that data from the queue.
    ///
    /// On success, returns the number of bytes peeked.
    /// Successive calls return the same data. This is accomplished by passing
    /// MSG_PEEK as a flag to the underlying `recv` system call.
    pub async fn peek(&self, buf: &mut [u8]) -> Result<usize> {
        self.stream.peek(buf).await.map_err(Into::into)
    }

    /// Returns the socket address of the remote peer of this TCP connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     println!("My peer: {:?}", stream.peer_addr());
    /// })
    /// ```
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        self.stream.stream().peer_addr().map_err(Into::into)
    }

    /// Returns the socket address of the local half of this TCP connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::TcpStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     println!("My peer: {:?}", stream.local_addr());
    /// })
    /// ```
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.stream.stream().local_addr().map_err(Into::into)
    }
}

impl<B: Buffered + Unpin> AsyncBufRead for TcpStream<B> {
    fn poll_fill_buf<'a>(
        self: Pin<&'a mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<&'a [u8]>> {
        let this = self.project();
        this.stream.poll_fill_buf(cx)
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.stream.consume(amt);
    }
}

impl<B: RxBuf + Unpin> AsyncRead for TcpStream<B> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl<B: RxBuf + Unpin> AsyncWrite for TcpStream<B> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{channels::shared_channel, enclose, timer::Timer, LocalExecutorBuilder};
    use futures_lite::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt},
        StreamExt,
    };
    use std::{
        cell::Cell,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    };

    #[test]
    fn tcp_listener_ttl() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_ttl(100).unwrap();
            assert_eq!(listener.ttl().unwrap(), 100);
        });
    }

    #[test]
    fn tcp_stream_ttl() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let stream = TcpStream::connect(addr).await.unwrap();
            stream.set_ttl(100).unwrap();
            assert_eq!(stream.ttl().unwrap(), 100);
        });
    }

    #[test]
    fn tcp_stream_nodelay() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let stream = TcpStream::connect(addr).await.unwrap();
            stream.set_nodelay(true).expect("set_nodelay call failed");
            assert!(stream.nodelay().unwrap());
        });
    }

    #[test]
    fn connect_local_server() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let coord = Rc::new(Cell::new(0));

            let listener_handle = crate::spawn_local(enclose! { (coord) async move {
                coord.set(1);
                let stream = listener.accept().await?;
                stream.peer_addr()
            }});

            while coord.get() != 1 {
                crate::executor().yield_task_queue_now().await;
            }
            let stream = TcpStream::connect(addr).await.unwrap();
            assert_eq!(listener_handle.await.unwrap(), stream.local_addr().unwrap());
        });
    }

    #[test]
    fn multi_executor_bind_works() {
        test_executor!(async move {
            let addr_getter = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = addr_getter.local_addr().unwrap();
            let (first_sender, first_receiver) = shared_channel::new_bounded(1);
            let (second_sender, second_receiver) = shared_channel::new_bounded(1);

            let ex1 = LocalExecutorBuilder::default()
                .spawn(move || async move {
                    let receiver = first_receiver.connect().await;
                    let _ = TcpListener::bind(addr).unwrap();
                    receiver.recv().await.unwrap();
                })
                .unwrap();

            let ex2 = LocalExecutorBuilder::default()
                .spawn(move || async move {
                    let receiver = second_receiver.connect().await;
                    let _ = TcpListener::bind(addr).unwrap();
                    receiver.recv().await.unwrap();
                })
                .unwrap();

            Timer::new(Duration::from_millis(100)).await;

            let sender = first_sender.connect().await;
            sender.try_send(0).unwrap();
            let sender = second_sender.connect().await;
            sender.try_send(0).unwrap();

            ex1.join().unwrap();
            ex2.join().unwrap();
        });
    }

    #[test]
    fn multi_executor_accept() {
        let (sender, receiver) = shared_channel::new_bounded(1);
        let (addr_sender, addr_receiver) = shared_channel::new_bounded(1);
        let connected = Arc::new(AtomicUsize::new(0));

        let status = connected.clone();
        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                let addr_sender = addr_sender.connect().await;
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = listener.local_addr().unwrap();
                addr_sender.try_send(addr).unwrap();

                status.store(1, Ordering::Relaxed);
                let accepted = listener.shared_accept().await.unwrap();
                sender.try_send(accepted).unwrap();
            })
            .unwrap();

        let status = connected.clone();
        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let accepted = receiver.recv().await.unwrap();
                let _ = accepted.bind_to_executor();
                status.store(2, Ordering::Relaxed);
            })
            .unwrap();

        let ex3 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = addr_receiver.connect().await;
                let addr = receiver.recv().await.unwrap();
                TcpStream::connect(addr).await.unwrap();
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
        ex3.join().unwrap();
        assert_eq!(connected.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn stream_of_connections() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let coord = Rc::new(Cell::new(0));

            let listener_handle = crate::spawn_local(enclose! { (coord) async move {
                coord.set(1);
                listener.incoming().take(4).try_for_each(|addr| {
                    addr.map(|_| ())
                }).await
            }});

            while coord.get() != 1 {
                crate::executor().yield_task_queue_now().await;
            }
            let mut handles = Vec::with_capacity(4);
            for _ in 0..4 {
                handles.push(
                    crate::spawn_local(async move { TcpStream::connect(addr).await }).detach(),
                );
            }

            for handle in handles.drain(..) {
                handle.await.unwrap().unwrap();
            }
            listener_handle.await.unwrap();

            let res = TcpStream::connect(addr).await;
            // server is now dead, connection must fail
            assert!(res.is_err())
        });
    }

    #[test]
    fn parallel_accept() {
        test_executor!(async move {
            let listener = Rc::new(TcpListener::bind("127.0.0.1:0").unwrap());
            let addr = listener.local_addr().unwrap();

            let mut handles = Vec::new();

            for _ in 0..128 {
                handles.push(
                    crate::spawn_local(enclose! { (listener) async move {
                        let _accept = listener.accept().await.unwrap();
                    }})
                    .detach(),
                );
            }
            // give it some time to make sure that all tasks above were sent down to
            // the ring
            Timer::new(Duration::from_millis(100)).await;

            // Now we should be able to establish 128 connections and all of that would
            // accept
            for _ in 0..128 {
                handles.push(
                    crate::spawn_local(async move {
                        let _stream = TcpStream::connect(addr).await.unwrap();
                    })
                    .detach(),
                );
            }

            for handle in handles {
                handle.await.unwrap();
            }
        });
    }

    #[test]
    fn connect_and_ping_pong() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let coord = Rc::new(Cell::new(0));

            let listener_handle = crate::spawn_local(enclose! { (coord) async move {
                coord.set(1);
                let mut stream = listener.accept().await?;
                let mut byte = [0u8; 1];
                let read = stream.read(&mut byte).await?;
                assert_eq!(read, 1);
                io::Result::Ok(byte[0])
            }})
            .detach();

            while coord.get() != 1 {
                crate::executor().yield_task_queue_now().await;
            }
            let mut stream = TcpStream::connect(addr).await.unwrap();

            let byte = [65u8; 1];
            let b = stream.write(&byte).await.unwrap();
            assert_eq!(b, 1);
            assert_eq!(listener_handle.await.unwrap().unwrap(), 65u8);
        });
    }

    #[test]
    fn test_read_until() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let mut stream = listener.accept().await?.buffered();
                let mut buf = Vec::new();
                stream.read_until(10, &mut buf).await?;
                io::Result::Ok(buf.len())
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();

            let vec = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
            let b = stream.write(&vec).await.unwrap();
            assert_eq!(b, 10);
            assert_eq!(listener_handle.await.unwrap().unwrap(), 10);
        });
    }

    #[test]
    fn test_read_line() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let mut stream = listener.accept().await?.buffered();
                let mut buf = String::new();
                stream.read_line(&mut buf).await?;
                io::Result::Ok(buf.len())
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();

            let b = stream.write(b"line\n").await.unwrap();
            assert_eq!(b, 5);
            assert_eq!(listener_handle.await.unwrap().unwrap(), 5);
        });
    }

    #[test]
    fn test_lines() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let stream = listener.accept().await?.buffered();
                io::Result::Ok(stream.lines().count().await)
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();

            stream.write(b"line1\nline2\nline3\n").await.unwrap();
            stream.write(b"line4\nline5\nline6\n").await.unwrap();
            stream.close().await.unwrap();
            assert_eq!(listener_handle.await.unwrap().unwrap(), 6);
        });
    }

    #[test]
    fn multibuf_fill() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let mut stream = listener.accept().await?.buffered();
                let buf = stream.fill_buf().await?;
                // likely both messages were coalesced together
                assert_eq!(&buf[0..4], b"msg1");
                stream.consume(4);
                let buf = stream.fill_buf().await?;
                assert_eq!(buf, b"msg2");
                stream.consume(4);
                let buf = stream.fill_buf().await?;
                assert_eq!(buf.len(), 0);
                io::Result::Ok(())
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();

            let b = stream.write(b"msg1").await.unwrap();
            assert_eq!(b, 4);
            stream.write(b"msg2").await.unwrap();
            assert_eq!(b, 4);
            stream.close().await.unwrap();
            listener_handle.await.unwrap().unwrap();
        });
    }

    #[test]
    fn overconsume() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let mut stream = listener.accept().await?.buffered();
                let buf = stream.fill_buf().await?;
                assert_eq!(buf.len(), 4);
                stream.consume(100);
                let buf = stream.fill_buf().await?;
                assert_eq!(buf.len(), 0);
                io::Result::Ok(())
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();

            stream.write(b"msg1").await.unwrap();
            stream.close().await.unwrap();
            listener_handle.await.unwrap().unwrap();
        });
    }

    #[test]
    fn repeated_fill_before_consume() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let mut stream = listener.accept().await?.buffered();
                let buf = stream.fill_buf().await?;
                assert_eq!(buf, b"msg1");
                let buf = stream.fill_buf().await?;
                assert_eq!(buf, b"msg1");
                stream.consume(4);
                let buf = stream.fill_buf().await?;
                assert!(buf.is_empty());
                io::Result::Ok(())
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();

            stream.write(b"msg1").await.unwrap();
            stream.close().await.unwrap();
            listener_handle.await.unwrap().unwrap();
        });
    }

    #[test]
    fn peek() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let listener_handle = crate::spawn_local(async move {
                let mut stream = listener.accept().await?;
                let mut buf = [0u8; 64];
                for _ in 0..10 {
                    let b = stream.peek(&mut buf).await?;
                    assert_eq!(b, 4);
                    assert_eq!(&buf[0..4], b"msg1");
                }
                stream.read(&mut buf).await?;
                stream.peek(&mut buf).await
            })
            .detach();

            let mut stream = TcpStream::connect(addr).await.unwrap();
            stream.write(b"msg1").await.unwrap();
            stream.close().await.unwrap();
            let res = listener_handle.await.unwrap().unwrap();
            assert_eq!(res, 0);
        });
    }

    #[test]
    fn tcp_connect_timeout() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            match TcpStream::connect_timeout(addr, Duration::from_millis(250)).await {
                Ok(_) => {}
                Err(e) => panic!("unexpected error {}", e),
            }
        });
    }

    // adapted from socket2 test:
    // https://docs.rs/socket2/0.3.19/src/socket2/socket.rs.html#971-982
    #[test]
    fn tcp_connect_timeout_error() {
        test_executor!(async move {
            // this IP is unroutable, so connections should always time out
            match TcpStream::connect_timeout("10.255.255.1:80", Duration::from_millis(250)).await {
                Ok(_) => panic!("unexpected success"),
                Err(GlommioError::IoError(ref e)) if e.kind() == io::ErrorKind::TimedOut => {}
                Err(e) => panic!("unexpected error {}", e),
            }
        });
    }

    #[test]
    fn tcp_read_timeout() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let ltask = crate::spawn_local(async move {
                let mut stream = listener.accept().await?;
                stream
                    .set_read_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                let mut buf = [0u8; 64];
                let now = Instant::now();
                match stream.read(&mut buf).await {
                    Ok(_) => unreachable!(),
                    Err(x) => {
                        assert_eq!(x.kind(), io::ErrorKind::TimedOut);
                    }
                };
                assert!(now.elapsed().as_secs() >= 1);
                io::Result::Ok(0)
            });

            let _s = TcpStream::connect(addr).await.unwrap();
            ltask.await.unwrap();
        });
    }

    #[test]
    fn tcp_force_poll() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let ltask = crate::spawn_local(async move {
                let mut stream = listener.accept().await?;
                poll_fn(|cx| {
                    let mut buf = [0u8; 64];
                    // try to overflow the amount of wakers possible
                    for _ in 0..64_000 {
                        if Pin::new(&mut stream).poll_read(cx, &mut buf).is_ready() {
                            panic!("should be pending");
                        }
                    }
                    Poll::Ready(())
                })
                .await;
                io::Result::Ok(0)
            });

            let _s = TcpStream::connect(addr).await.unwrap();
            ltask.await.unwrap();
        });
    }

    #[test]
    fn tcp_invalid_timeout() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let ltask = crate::spawn_local(async move {
                let stream = listener.accept().await?;
                stream
                    .set_write_timeout(Some(Duration::from_nanos(0)))
                    .unwrap_err();
                assert!(stream.write_timeout().is_none());
                stream
                    .set_write_timeout(Some(Duration::from_secs(1)))
                    .unwrap();
                assert_eq!(stream.write_timeout(), Some(Duration::from_secs(1)));
                io::Result::Ok(0)
            });

            let _s = TcpStream::connect(addr).await.unwrap();
            ltask.await.unwrap();
        });
    }

    #[test]
    fn accepted_tcp_stream_peer_addr() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();

            let peer_addr = crate::spawn_local(async move {
                let accepted = listener.shared_accept().await.unwrap();
                let peer_addr = accepted.peer_addr().unwrap();
                let stream = accepted.bind_to_executor();
                assert_eq!(peer_addr, stream.peer_addr().unwrap());
                peer_addr
            });

            let s = TcpStream::connect(addr).await.unwrap();
            assert_eq!(s.local_addr().unwrap(), peer_addr.await);
        });
    }
}
