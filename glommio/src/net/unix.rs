// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use super::{datagram::GlommioDatagram, stream::GlommioStream};
use crate::{
    net::stream::{Buffered, NonBuffered, Preallocated, RxBuf},
    reactor::Reactor,
};
use futures_lite::{
    future::poll_fn,
    io::{AsyncBufRead, AsyncRead, AsyncWrite},
    stream::{self, Stream},
};
use nix::sys::socket::{SockAddr, UnixAddr};
use pin_project_lite::pin_project;
use socket2::{Domain, Socket, Type};
use std::{
    io,
    net::Shutdown,
    os::unix::{
        io::{AsRawFd, FromRawFd, RawFd},
        net::{self, SocketAddr},
    },
    path::Path,
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context, Poll},
};

type Result<T> = crate::Result<T, ()>;

#[derive(Debug)]
/// A Unix socket server, listening for connections.
///
/// After creating a UnixListener by binding it to a socket address, it listens
/// for incoming Unix connections. These can be accepted by calling [`accept`]
/// or [`shared_accept`], or by iterating over the Incoming iterator returned by
/// [`incoming`].
///
/// A good networking architecture within a thread-per-core model needs to take
/// into account parallelism and spawn work into multiple executors. If
/// everything happens inside the same Executor, then at most one thread is
/// used. Sometimes this is what you want: you may want to dedicate a CPU
/// entirely for networking, or even use specialized ports for each CPU of the
/// application, but most likely it isn't.
///
/// There are so far only one approach to load balancing possible with the
/// `UnixListener`:
///
/// * It is possible to use [`shared_accept`] instead of [`accept`]: that
///   returns an object that implements [`Send`]. You can then use a
///   [`shared_channel`] to send the accepted connection into multiple
///   executors. The object returned by [`shared_accept`] can then be bound to
///   its executor with [`bind_to_executor`], at which point it becomes a
///   standard [`UnixStream`].
///
///
/// The socket will be closed when the value is dropped.
///
/// [`accept`]: UnixListener::accept
/// [`shared_accept`]: UnixListener::shared_accept
/// [`bind`]: UnixListener::bind
/// [`incoming`]: UnixListener::incoming
/// [`bind_to_executor`]: AcceptedUnixStream::bind_to_executor
/// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
/// [`shared_channel`]: ../channels/shared_channel/index.html
pub struct UnixListener {
    reactor: Weak<Reactor>,
    listener: net::UnixListener,
}

impl UnixListener {
    /// Creates a Unix listener bound to the specified address.
    ///
    /// Binding with port number 0 will request an available port from the OS.
    ///
    /// This method sets the ReusePort option in the bound socket, so it is
    /// designed to be called from multiple executors to achieve
    /// parallelism.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::UnixListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let _listener = UnixListener::bind("/tmp/named").unwrap();
    /// });
    /// ```
    pub fn bind<A: AsRef<Path>>(addr: A) -> Result<UnixListener> {
        let sk = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        let addr = socket2::SockAddr::unix(addr.as_ref())?;

        sk.bind(&addr)?;
        sk.listen(128)?;
        let listener = sk.into();

        Ok(UnixListener {
            reactor: Rc::downgrade(&crate::executor().reactor()),
            listener,
        })
    }

    /// Accepts a new incoming Unix connection and allows the result to be sent
    /// to a foreign executor
    ///
    /// This is similar to [`accept`], except it returns an
    /// [`AcceptedUnixStream`] instead of a [`UnixStream`].
    /// [`AcceptedUnixStream`] implements [`Send`], so it can be safely sent
    /// for processing over a shared channel to a different executor.
    ///
    /// This is useful when the user wants to do her own load balancing across
    /// multiple executors instead of relying on the load balancing the OS
    /// would do with the ReusePort property of the bound socket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::UnixListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = UnixListener::bind("/tmp/named").unwrap();
    ///     let _stream = listener.shared_accept().await.unwrap();
    /// });
    /// ```
    ///
    /// [`accept`]: UnixListener::accept
    /// [`AcceptedUnixStream`]: struct.AcceptedUnixStream.html
    /// [`UnixStream`]: struct.UnixStream.html
    /// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
    pub async fn shared_accept(&self) -> Result<AcceptedUnixStream> {
        let reactor = self.reactor.upgrade().unwrap();
        let source = reactor.accept(self.listener.as_raw_fd());
        let fd = source.collect_rw().await?;
        Ok(AcceptedUnixStream { fd: fd as RawFd })
    }

    /// Accepts a new incoming Unix connection in this executor
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
    /// use glommio::{net::UnixListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = UnixListener::bind("/tmp/named").unwrap();
    ///     let _stream = listener.accept().await.unwrap();
    /// });
    /// ```
    ///
    /// [`shared_accept`]: UnixListener::accept
    /// [`bind_to_executor`]: AcceptedUnixStream::bind_to_executor
    pub async fn accept(&self) -> Result<UnixStream> {
        let a = self.shared_accept().await?;
        Ok(a.bind_to_executor())
    }

    /// Creates a stream of incoming connections
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use futures_lite::stream::StreamExt;
    /// use glommio::{net::UnixListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = UnixListener::bind("/tmp/named").unwrap();
    ///     let mut incoming = listener.incoming();
    ///     while let Some(conn) = incoming.next().await {
    ///         // handle
    ///     }
    /// });
    /// ```
    pub fn incoming(&self) -> impl Stream<Item = Result<UnixStream>> + Unpin + '_ {
        Box::pin(stream::unfold(self, |listener| async move {
            let res = listener.accept().await.map_err(Into::into);
            Some((res, listener))
        }))
    }

    /// Returns the socket address of the local half of this Unix connection.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{net::UnixListener, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = UnixListener::bind("/tmp/named").unwrap();
    ///     println!("Listening on {:?}", listener.local_addr().unwrap());
    /// });
    /// ```
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener.local_addr().map_err(Into::into)
    }
}

#[derive(Copy, Clone, Debug)]
/// An Accepted Unix connection that can be moved to a different executor
///
/// This is useful in situations where the load balancing provided by the
/// Operating System through ReusePort is not desirable. The user can accept the
/// connection in one executor through [`shared_accept`] which returns an
/// AcceptedUnixStream.
///
/// Once the `AcceptedUnixStream` arrives at its destination it can then be made
/// active with [`bind_to_executor`]
///
/// [`shared_accept`]: UnixListener::shared_accept
/// [`bind_to_executor`]: AcceptedUnixStream::bind_to_executor
pub struct AcceptedUnixStream {
    fd: RawFd,
}

impl AcceptedUnixStream {
    /// Binds this `AcceptedUnixStream` to the current executor
    ///
    /// This returns a [`UnixStream`] that can then be used normally
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{
    ///     channels::shared_channel,
    ///     net::UnixListener,
    ///     LocalExecutor,
    ///     LocalExecutorBuilder,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let (sender, receiver) = shared_channel::new_bounded(1);
    ///     let sender = sender.connect().await;
    ///
    ///     let listener = UnixListener::bind("/tmp/named").unwrap();
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
    pub fn bind_to_executor(self) -> UnixStream {
        let stream = unsafe { GlommioStream::from_raw_fd(self.fd as _) };
        UnixStream { stream }
    }
}

pin_project! {
    #[derive(Debug)]
    /// A Unix Stream of bytes. This can be used with [`AsyncRead`], [`AsyncBufRead`] and
    /// [`AsyncWrite`]
    ///
    /// [`AsyncRead`]: https://docs.rs/futures-io/0.3.8/futures_io/trait.AsyncRead.html
    /// [`AsyncBufRead`]: https://docs.rs/futures-io/0.3.8/futures_io/trait.AsyncBufRead.html
    /// [`AsyncWrite`]: https://docs.rs/futures-io/0.3.8/futures_io/trait.AsyncWrite.html
    pub struct UnixStream<B: RxBuf = NonBuffered> {
        stream: GlommioStream<net::UnixStream, B>
    }
}

impl FromRawFd for UnixStream {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        UnixStream {
            stream: GlommioStream::from_raw_fd(fd as _),
        }
    }
}

impl UnixStream {
    /// Creates an unnamed pair of connected Unix stream sockets.
    ///
    /// # Examples
    ///
    /// ```
    /// use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
    /// use glommio::{net::UnixStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let (mut p1, mut p2) = UnixStream::pair().unwrap();
    ///     let sz = p1.write(&[65u8; 1]).await.unwrap();
    ///     let mut buf = [0u8; 1];
    ///     let sz = p2.read(&mut buf).await.unwrap();
    /// })
    /// ```
    pub fn pair() -> Result<(UnixStream, UnixStream)> {
        let (stream1, stream2) = net::UnixStream::pair()?;
        let stream1 = GlommioStream::from(socket2::Socket::from(stream1));
        let stream2 = GlommioStream::from(socket2::Socket::from(stream2));
        let stream1 = Self { stream: stream1 };
        let stream2 = Self { stream: stream2 };
        Ok((stream1, stream2))
    }

    /// Creates a Unix connection to the specified endpoint.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::UnixStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     UnixStream::connect("/tmp/named").await.unwrap();
    /// })
    /// ```
    pub async fn connect<A: AsRef<Path>>(addr: A) -> Result<UnixStream> {
        let reactor = crate::executor().reactor();

        let socket = Socket::new(Domain::UNIX, Type::STREAM, None)?;
        let addr = SockAddr::new_unix(addr.as_ref())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let source = reactor.connect(socket.as_raw_fd(), addr);
        source.collect_rw().await?;

        Ok(Self {
            stream: GlommioStream::from(socket),
        })
    }

    /// Creates a buffered Unix connection with default receive buffer.
    pub fn buffered(self) -> UnixStream<Preallocated> {
        self.buffered_with(Preallocated::default())
    }

    /// Creates a buffered Unix connection with custom receive buffer.
    pub fn buffered_with<B: Buffered>(self, buf: B) -> UnixStream<B> {
        UnixStream {
            stream: self.stream.buffered_with(buf),
        }
    }
}

impl<B: RxBuf> UnixStream<B> {
    /// Shuts down the read, write, or both halves of this connection.
    pub async fn shutdown(&self, how: Shutdown) -> Result<()> {
        poll_fn(|cx| self.stream.poll_shutdown(cx, how))
            .await
            .map_err(Into::into)
    }

    /// Receives data on the socket from the remote address to which it is
    /// connected, without removing that data from the queue.
    ///
    /// On success, returns the number of bytes peeked.
    /// Successive calls return the same data. This is accomplished by passing
    /// MSG_PEEK as a flag to the underlying recv system call.
    pub async fn peek(&self, buf: &mut [u8]) -> Result<usize> {
        self.stream.peek(buf).await.map_err(Into::into)
    }

    /// Returns the socket address of the remote peer of this Unix connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::UnixStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = UnixStream::connect("/tmp/named").await.unwrap();
    ///     println!("My peer: {:?}", stream.peer_addr());
    /// })
    /// ```
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        self.stream.stream().peer_addr().map_err(Into::into)
    }

    /// Returns the socket address of the local half of this Unix connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::UnixStream, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = UnixStream::connect("/tmp/named").await.unwrap();
    ///     println!("My peer: {:?}", stream.local_addr());
    /// })
    /// ```
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.stream.stream().local_addr().map_err(Into::into)
    }
}

impl<B: Buffered + Unpin> AsyncBufRead for UnixStream<B> {
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

impl<B: RxBuf + Unpin> AsyncRead for UnixStream<B> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl<B: RxBuf + Unpin> AsyncWrite for UnixStream<B> {
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
/// A Unix Datagram Socket.
pub struct UnixDatagram {
    socket: GlommioDatagram<net::UnixDatagram>,
}

impl UnixDatagram {
    /// Creates an unnamed pair of connected Unix Datagram sockets.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{net::UnixDatagram, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let (mut p1, mut p2) = UnixDatagram::pair().unwrap();
    ///     let sz = p1.send(&[65u8; 1]).await.unwrap();
    ///     let mut buf = [0u8; 1];
    ///     let sz = p2.recv(&mut buf).await.unwrap();
    /// })
    /// ```
    pub fn pair() -> Result<(UnixDatagram, UnixDatagram)> {
        let (socket1, socket2) = net::UnixDatagram::pair()?;
        let socket1 = GlommioDatagram::from(socket2::Socket::from(socket1));
        let socket2 = GlommioDatagram::from(socket2::Socket::from(socket2));
        let socket1 = Self { socket: socket1 };
        let socket2 = Self { socket: socket2 };
        Ok((socket1, socket2))
    }

    /// Creates a Unix Datagram socket bound to the specified address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::UnixDatagram, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = UnixDatagram::bind("/tmp/named_dgram").unwrap();
    ///     println!("Listening on {:?}", listener.local_addr().unwrap());
    /// });
    /// ```
    pub fn bind<A: AsRef<Path>>(addr: A) -> Result<UnixDatagram> {
        let sk = Socket::new(Domain::UNIX, Type::DGRAM, None)?;
        let addr = socket2::SockAddr::unix(addr.as_ref())?;
        sk.bind(&addr)?;
        Ok(Self {
            socket: GlommioDatagram::from(sk),
        })
    }

    /// Creates a Unix Datagram socket which is not bound to any address.
    pub fn unbound() -> Result<UnixDatagram> {
        let sk = Socket::new(Domain::UNIX, Type::DGRAM, None)?;
        Ok(Self {
            socket: GlommioDatagram::from(sk),
        })
    }

    /// Connects an unbounded Unix Datagram socket to a remote address, allowing
    /// the [`send`] and [`recv`] methods to be used to send data and also
    /// applies filters to only receive data from the specified address.
    ///
    /// If `addr` yields multiple addresses, connect will be attempted with each
    /// of the addresses until the underlying OS function returns no error.
    /// Note that usually, a successful connect call does not specify that
    /// there is a remote server listening on the port, rather, such an
    /// error would only be detected after the first send. If the OS returns an
    /// error for each of the specified addresses, the error returned from
    /// the last connection attempt (the last address) is returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::UnixDatagram, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UnixDatagram::bind("/tmp/dgram").unwrap();
    ///     let sender = UnixDatagram::unbound().unwrap();
    ///     sender.connect("/tmp/dgram").await.unwrap();
    /// });
    /// ```
    ///
    /// [`send`]: UnixDatagram::send
    /// [`recv`]: UnixDatagram::recv
    pub async fn connect<A: AsRef<Path>>(&self, addr: A) -> Result<()> {
        let addr = SockAddr::new_unix(addr.as_ref())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let reactor = self.socket.reactor.upgrade().unwrap();
        let source = reactor.connect(self.socket.as_raw_fd(), addr);
        source.collect_rw().await.map(|_| {}).map_err(Into::into)
    }

    /// Sets the buffer size used on the receive path
    pub fn set_buffer_size(&mut self, buffer_size: usize) {
        self.socket.rx_buf_size = buffer_size;
    }

    /// gets the buffer size used
    pub fn buffer_size(&mut self) -> usize {
        self.socket.rx_buf_size
    }

    /// Receives single datagram on the socket from the remote address to which
    /// it is connected, without removing the message from input queue. On
    /// success, returns the number of bytes peeked.
    ///
    /// The function must be called with valid byte array buf of sufficient size
    /// to hold the message bytes. If a message is too long to fit in the
    /// supplied buffer, excess bytes may be discarded.
    ///
    /// To use this function, [`connect`] must have been called
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::UnixDatagram, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UnixDatagram::bind("/tmp/dgram").unwrap();
    ///     let sender = UnixDatagram::unbound().unwrap();
    ///     sender.connect("/tmp/dgram").await.unwrap();
    ///     sender.send(&[1; 1]).await.unwrap();
    ///     let mut buf = vec![0; 32];
    ///     let sz = receiver.peek(&mut buf).await.unwrap();
    ///     assert_eq!(sz, 1);
    /// })
    /// ```
    ///
    /// [`connect`]: UnixDatagram::connect
    pub async fn peek(&self, buf: &mut [u8]) -> Result<usize> {
        let _ = self.peer_addr()?;
        self.socket.peek(buf).await.map_err(Into::into)
    }

    /// Receives a single datagram message on the socket, without removing it
    /// from the queue. On success, returns the number of bytes read and the
    /// origin.
    ///
    /// The function must be called with valid byte array buf of sufficient size
    /// to hold the message bytes. If a message is too long to fit in the
    /// supplied buffer, excess bytes may be discarded.
    #[track_caller]
    pub async fn peek_from(&self, buf: &mut [u8]) -> Result<(usize, UnixAddr)> {
        let (sz, addr) = self.socket.peek_from(buf).await?;

        let addr = match addr {
            nix::sys::socket::SockAddr::Unix(addr) => addr,
            x => panic!("invalid socket addr for this family!: {:?}", x),
        };
        Ok((sz, addr))
    }

    /// Returns the socket address of the remote peer this socket was connected
    /// to.
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        self.socket.socket.peer_addr().map_err(Into::into)
    }

    /// Returns the socket address of the local half of this Unix Datagram
    /// connection.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.socket.socket.local_addr().map_err(Into::into)
    }

    /// Receives a single datagram message on the socket from the remote address
    /// to which it is connected.
    ///
    /// On success, returns the number of bytes read.  The function must be
    /// called with valid byte array buf of sufficient size to hold the
    /// message bytes. If a message is too long to fit in the supplied
    /// buffer, excess bytes may be discarded.
    ///
    ///
    /// To use this function, [`connect`] must have been called
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::UnixDatagram, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UnixDatagram::bind("/tmp/dgram").unwrap();
    ///     let sender = UnixDatagram::unbound().unwrap();
    ///     sender.connect("/tmp/dgram").await.unwrap();
    ///     sender.send(&[1; 1]).await.unwrap();
    ///     let mut buf = vec![0; 32];
    ///     let sz = receiver.recv(&mut buf).await.unwrap();
    ///     assert_eq!(sz, 1);
    /// })
    /// ```
    ///
    /// [`connect`]: UnixDatagram::connect
    pub async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        self.socket.recv(buf).await.map_err(Into::into)
    }

    /// Receives a single datagram message on the socket. On success, returns
    /// the number of bytes read and the origin.
    ///
    /// The function must be called with valid byte array buf of sufficient size
    /// to hold the message bytes. If a message is too long to fit in the
    /// supplied buffer, excess bytes may be discarded.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::UnixDatagram, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UnixDatagram::bind("/tmp/dgram").unwrap();
    ///     let sender = UnixDatagram::unbound().unwrap();
    ///     sender.send_to(&[1; 1], "/tmp/dgram").await.unwrap();
    ///     let mut buf = vec![0; 32];
    ///     let (sz, _addr) = receiver.recv_from(&mut buf).await.unwrap();
    ///     assert_eq!(sz, 1);
    /// })
    /// ```
    #[track_caller]
    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, UnixAddr)> {
        let (sz, addr) = self.socket.recv_from(buf).await?;
        let addr = match addr {
            nix::sys::socket::SockAddr::Unix(addr) => addr,
            x => panic!("invalid socket addr for this family!: {:?}", x),
        };
        Ok((sz, addr))
    }

    /// Sends data on the socket to the given address. On success, returns the
    /// number of bytes written.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::UnixDatagram, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let sender = UnixDatagram::unbound().unwrap();
    ///     sender.send_to(&[1; 1], "/tmp/dgram").await.unwrap();
    /// })
    /// ```
    pub async fn send_to<A: AsRef<Path>>(&self, buf: &[u8], addr: A) -> Result<usize> {
        let addr = nix::sys::socket::SockAddr::new_unix(addr.as_ref())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        self.socket.send_to(buf, addr).await.map_err(Into::into)
    }

    /// Sends data on the socket to the remote address to which it is connected.
    ///
    /// [`UnixDatagram::connect`] will connect this socket to a remote address.
    /// This method will fail if the socket is not connected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{net::UnixDatagram, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UnixDatagram::bind("/tmp/dgram").unwrap();
    ///     let sender = UnixDatagram::unbound().unwrap();
    ///     sender.connect("/tmp/dgram").await.unwrap();
    ///     sender.send(&[1; 1]).await.unwrap();
    /// })
    /// ```
    ///
    /// `[UnixDatagram::connect`]: UnixDatagram::connect
    pub async fn send(&self, buf: &[u8]) -> Result<usize> {
        self.socket.send(buf).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{enclose, test_utils::*};
    use futures_lite::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    use std::cell::Cell;

    macro_rules! unix_socket_test {
        ( $name:ident, $dir:ident, $code:block) => {
            #[test]
            fn $name() {
                let td = make_tmp_test_directory(&format!("uds-{}", stringify!($name)));
                let $dir = td.path.clone();
                test_executor!(async move { $code });
            }
        };
    }

    unix_socket_test!(connect_local_server, dir, {
        let mut file = dir.clone();
        file.push("name");

        let listener = UnixListener::bind(&file).unwrap();
        let addr = listener.local_addr().unwrap();
        let addr = addr.as_pathname().unwrap();
        let coord = Rc::new(Cell::new(0));

        let listener_handle = crate::spawn_local(enclose! { (coord) async move {
            coord.set(1);
            listener.accept().await.unwrap();
        }})
        .detach();

        while coord.get() != 1 {
            crate::executor().yield_task_queue_now().await;
        }
        UnixStream::connect(&addr).await.unwrap();
        listener_handle.await.unwrap();
    });

    unix_socket_test!(pair, _dir, {
        let (mut p1, mut p2) = UnixStream::pair().unwrap();
        let sz = p1.write(&[65u8; 1]).await.unwrap();
        assert_eq!(sz, 1);
        let mut buf = [0u8; 1];
        let sz = p2.read(&mut buf).await.unwrap();
        assert_eq!(sz, 1);
        assert_eq!(buf[0], 65);
    });

    unix_socket_test!(read_until, dir, {
        let mut file = dir.clone();
        file.push("name");

        let listener = UnixListener::bind(&file).unwrap();

        let listener_handle = crate::spawn_local(async move {
            let mut stream = listener.accept().await?.buffered();
            let mut buf = Vec::new();
            stream.read_until(10, &mut buf).await?;
            io::Result::Ok(buf.len())
        })
        .detach();

        let mut stream = UnixStream::connect(&file).await.unwrap();

        let vec = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let b = stream.write(&vec).await.unwrap();
        assert_eq!(b, 10);
        assert_eq!(listener_handle.await.unwrap().unwrap(), 10);
    });

    unix_socket_test!(datagram_pair_ping_pong, _dir, {
        let (p1, p2) = UnixDatagram::pair().unwrap();
        let sz = p1.send(&[65u8; 1]).await.unwrap();
        assert_eq!(sz, 1);
        let mut buf = [0u8; 1];
        let sz = p2.recv(&mut buf).await.unwrap();
        assert_eq!(sz, 1);
        assert_eq!(buf[0], 65);
    });

    unix_socket_test!(datagram_send_recv, dir, {
        let mut file = dir.clone();
        file.push("name");

        let p1 = UnixDatagram::bind(&file).unwrap();
        let p2 = UnixDatagram::unbound().unwrap();
        p2.connect(&file).await.unwrap();
        p2.send(b"msg1").await.unwrap();

        let mut buf = [0u8; 10];
        let sz = p1.recv(&mut buf).await.unwrap();
        assert_eq!(sz, 4);
    });

    unix_socket_test!(datagram_send_to_recv_from, dir, {
        let mut file = dir.clone();
        file.push("name");

        let p1 = UnixDatagram::bind(&file).unwrap();
        let p2 = UnixDatagram::unbound().unwrap();
        p2.send_to(b"msg1", &file).await.unwrap();

        let mut buf = [0u8; 10];
        let (sz, addr) = p1.recv_from(&mut buf).await.unwrap();
        assert_eq!(sz, 4);
        assert!(addr.path().is_none());
    });

    unix_socket_test!(datagram_connect_unbounded, dir, {
        let mut file = dir.clone();
        file.push("name");
        let _p1 = UnixDatagram::bind(&file).unwrap();
        let p2 = UnixDatagram::unbound().unwrap();
        p2.connect(&file).await.unwrap();
    });
}
