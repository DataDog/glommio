use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::{
    os::unix::net::{SocketAddr as UnixSocketAddr, UnixDatagram, UnixListener, UnixStream},
    path::Path,
};

use futures_lite::stream::{self, Stream};
use socket2::{Domain, Protocol, Socket, Type};

use crate::pollable::Async;

impl Async<TcpListener> {
    /// Creates a TCP listener bound to the specified address.
    ///
    /// Binding with port number 0 will request an available port from the OS.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::Async;
    /// use std::net::TcpListener;
    ///
    /// # futures_lite::future::block_on(async {
    /// let listener = Async::<TcpListener>::bind(([127, 0, 0, 1], 0))?;
    /// println!("Listening on {}", listener.get_ref().local_addr()?);
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn bind<A: Into<SocketAddr>>(addr: A) -> io::Result<Async<TcpListener>> {
        let addr = addr.into();
        Ok(Async::new(TcpListener::bind(addr)?)?)
    }

    /// Accepts a new incoming TCP connection.
    ///
    /// When a connection is established, it will be returned as a TCP stream together with its
    /// remote address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::net::TcpListener;
    ///
    /// # futures_lite::future::block_on(async {
    /// let listener = Async::<TcpListener>::bind(([127, 0, 0, 1], 8000))?;
    /// let (stream, addr) = listener.accept().await?;
    /// println!("Accepted client: {}", addr);
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn accept(&self) -> io::Result<(Async<TcpStream>, SocketAddr)> {
        let (stream, addr) = self.read_with(|io| io.accept()).await?;
        Ok((Async::new(stream)?, addr))
    }

    /// Returns a stream of incoming TCP connections.
    ///
    /// The stream is infinite, i.e. it never stops with a [`None`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use futures_lite::stream::StreamExt;
    /// use std::net::TcpListener;
    ///
    /// # futures_lite::future::block_on(async {
    /// let listener = Async::<TcpListener>::bind(([127, 0, 0, 1], 8000))?;
    /// let mut incoming = listener.incoming();
    ///
    /// while let Some(stream) = incoming.next().await {
    ///     let stream = stream?;
    ///     println!("Accepted client: {}", stream.get_ref().peer_addr()?);
    /// }
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn incoming(&self) -> impl Stream<Item = io::Result<Async<TcpStream>>> + Unpin + '_ {
        Box::pin(stream::unfold(self, |listener| async move {
            let res = listener.accept().await.map(|(stream, _)| stream);
            Some((res, listener))
        }))
    }
}

impl Async<TcpStream> {
    /// Creates a TCP connection to the specified address.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::Async;
    /// use std::net::{TcpStream, ToSocketAddrs};
    ///
    /// # futures_lite::future::block_on(async {
    /// let addr = "example.com:80".to_socket_addrs()?.next().unwrap();
    /// let stream = Async::<TcpStream>::connect(addr).await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn connect<A: Into<SocketAddr>>(addr: A) -> io::Result<Async<TcpStream>> {
        let addr = addr.into();

        // Create a socket.
        let domain = if addr.is_ipv6() {
            Domain::ipv6()
        } else {
            Domain::ipv4()
        };
        let socket = Socket::new(domain, Type::stream(), Some(Protocol::tcp()))?;

        // Begin async connect and ignore the inevitable "in progress" error.
        socket.set_nonblocking(true)?;
        socket.connect(&addr.into()).or_else(|err| {
            // Check for EINPROGRESS on Unix and WSAEWOULDBLOCK on Windows.
            let in_progress = err.raw_os_error() == Some(libc::EINPROGRESS);

            // If connect results with an "in progress" error, that's not an error.
            if in_progress {
                Ok(())
            } else {
                Err(err)
            }
        })?;
        let stream = Async::new(socket.into_tcp_stream())?;

        // The stream becomes writable when connected.
        stream.writable().await?;

        // Check if there was an error while connecting.
        match stream.get_ref().take_error()? {
            None => Ok(stream),
            Some(err) => Err(err),
        }
    }

    /// Reads data from the stream without removing it from the buffer.
    ///
    /// Returns the number of bytes read. Successive calls of this method read the same data.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::Async;
    /// use futures_lite::{io::AsyncWriteExt, stream::StreamExt};
    /// use std::net::{TcpStream, ToSocketAddrs};
    ///
    /// # futures_lite::future::block_on(async {
    /// let addr = "example.com:80".to_socket_addrs()?.next().unwrap();
    /// let mut stream = Async::<TcpStream>::connect(addr).await?;
    ///
    /// stream
    ///     .write_all(b"GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")
    ///     .await?;
    ///
    /// let mut buf = [0u8; 1024];
    /// let len = stream.peek(&mut buf).await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_with(|io| io.peek(buf)).await
    }
}

impl Async<UdpSocket> {
    /// Creates a UDP socket bound to the specified address.
    ///
    /// Binding with port number 0 will request an available port from the OS.
    ///
    /// # Examples
    ///
    /// ```
    /// use async_io::Async;
    /// use std::net::UdpSocket;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UdpSocket>::bind(([127, 0, 0, 1], 0))?;
    /// println!("Bound to {}", socket.get_ref().local_addr()?);
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn bind<A: Into<SocketAddr>>(addr: A) -> io::Result<Async<UdpSocket>> {
        let addr = addr.into();
        Ok(Async::new(UdpSocket::bind(addr)?)?)
    }

    /// Receives a single datagram message.
    ///
    /// Returns the number of bytes read and the address the message came from.
    ///
    /// This method must be called with a valid byte slice of sufficient size to hold the message.
    /// If the message is too long to fit, excess bytes may get discarded.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::net::UdpSocket;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UdpSocket>::bind(([127, 0, 0, 1], 8000))?;
    ///
    /// let mut buf = [0u8; 1024];
    /// let (len, addr) = socket.recv_from(&mut buf).await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.read_with(|io| io.recv_from(buf)).await
    }

    /// Receives a single datagram message without removing it from the queue.
    ///
    /// Returns the number of bytes read and the address the message came from.
    ///
    /// This method must be called with a valid byte slice of sufficient size to hold the message.
    /// If the message is too long to fit, excess bytes may get discarded.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::net::UdpSocket;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UdpSocket>::bind(([127, 0, 0, 1], 8000))?;
    ///
    /// let mut buf = [0u8; 1024];
    /// let (len, addr) = socket.peek_from(&mut buf).await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn peek_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.read_with(|io| io.peek_from(buf)).await
    }

    /// Sends data to the specified address.
    ///
    /// Returns the number of bytes writen.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::net::UdpSocket;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UdpSocket>::bind(([127, 0, 0, 1], 0))?;
    /// let addr = socket.get_ref().local_addr()?;
    ///
    /// let msg = b"hello";
    /// let len = socket.send_to(msg, addr).await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn send_to<A: Into<SocketAddr>>(&self, buf: &[u8], addr: A) -> io::Result<usize> {
        let addr = addr.into();
        self.write_with(|io| io.send_to(buf, addr)).await
    }

    /// Receives a single datagram message from the connected peer.
    ///
    /// Returns the number of bytes read.
    ///
    /// This method must be called with a valid byte slice of sufficient size to hold the message.
    /// If the message is too long to fit, excess bytes may get discarded.
    ///
    /// The [`connect`][`UdpSocket::connect()`] method connects this socket to a remote address.
    /// This method will fail if the socket is not connected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::net::UdpSocket;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UdpSocket>::bind(([127, 0, 0, 1], 8000))?;
    /// socket.get_ref().connect("127.0.0.1:9000")?;
    ///
    /// let mut buf = [0u8; 1024];
    /// let len = socket.recv(&mut buf).await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_with(|io| io.recv(buf)).await
    }

    /// Receives a single datagram message from the connected peer without removing it from the
    /// queue.
    ///
    /// Returns the number of bytes read and the address the message came from.
    ///
    /// This method must be called with a valid byte slice of sufficient size to hold the message.
    /// If the message is too long to fit, excess bytes may get discarded.
    ///
    /// The [`connect`][`UdpSocket::connect()`] method connects this socket to a remote address.
    /// This method will fail if the socket is not connected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::net::UdpSocket;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UdpSocket>::bind(([127, 0, 0, 1], 8000))?;
    /// socket.get_ref().connect("127.0.0.1:9000")?;
    ///
    /// let mut buf = [0u8; 1024];
    /// let len = socket.peek(&mut buf).await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_with(|io| io.peek(buf)).await
    }

    /// Sends data to the connected peer.
    ///
    /// Returns the number of bytes written.
    ///
    /// The [`connect`][`UdpSocket::connect()`] method connects this socket to a remote address.
    /// This method will fail if the socket is not connected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::net::UdpSocket;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UdpSocket>::bind(([127, 0, 0, 1], 8000))?;
    /// socket.get_ref().connect("127.0.0.1:9000")?;
    ///
    /// let msg = b"hello";
    /// let len = socket.send(msg).await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        self.write_with(|io| io.send(buf)).await
    }
}

impl Async<UnixListener> {
    /// Creates a UDS listener bound to the specified path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::os::unix::net::UnixListener;
    ///
    /// # futures_lite::future::block_on(async {
    /// let listener = Async::<UnixListener>::bind("/tmp/socket")?;
    /// println!("Listening on {:?}", listener.get_ref().local_addr()?);
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn bind<P: AsRef<Path>>(path: P) -> io::Result<Async<UnixListener>> {
        let path = path.as_ref().to_owned();
        Ok(Async::new(UnixListener::bind(path)?)?)
    }

    /// Accepts a new incoming UDS stream connection.
    ///
    /// When a connection is established, it will be returned as a stream together with its remote
    /// address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::os::unix::net::UnixListener;
    ///
    /// # futures_lite::future::block_on(async {
    /// let listener = Async::<UnixListener>::bind("/tmp/socket")?;
    /// let (stream, addr) = listener.accept().await?;
    /// println!("Accepted client: {:?}", addr);
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn accept(&self) -> io::Result<(Async<UnixStream>, UnixSocketAddr)> {
        let (stream, addr) = self.read_with(|io| io.accept()).await?;
        Ok((Async::new(stream)?, addr))
    }

    /// Returns a stream of incoming UDS connections.
    ///
    /// The stream is infinite, i.e. it never stops with a [`None`] item.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use futures_lite::stream::StreamExt;
    /// use std::os::unix::net::UnixListener;
    ///
    /// # futures_lite::future::block_on(async {
    /// let listener = Async::<UnixListener>::bind("/tmp/socket")?;
    /// let mut incoming = listener.incoming();
    ///
    /// while let Some(stream) = incoming.next().await {
    ///     let stream = stream?;
    ///     println!("Accepted client: {:?}", stream.get_ref().peer_addr()?);
    /// }
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn incoming(&self) -> impl Stream<Item = io::Result<Async<UnixStream>>> + Unpin + '_ {
        Box::pin(stream::unfold(self, |listener| async move {
            let res = listener.accept().await.map(|(stream, _)| stream);
            Some((res, listener))
        }))
    }
}

impl Async<UnixStream> {
    /// Creates a UDS stream connected to the specified path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::os::unix::net::UnixStream;
    ///
    /// # futures_lite::future::block_on(async {
    /// let stream = Async::<UnixStream>::connect("/tmp/socket").await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn connect<P: AsRef<Path>>(path: P) -> io::Result<Async<UnixStream>> {
        // Create a socket.
        let socket = Socket::new(Domain::unix(), Type::stream(), None)?;

        // Begin async connect and ignore the inevitable "in progress" error.
        socket.set_nonblocking(true)?;
        socket
            .connect(&socket2::SockAddr::unix(path)?)
            .or_else(|err| {
                if err.raw_os_error() == Some(libc::EINPROGRESS) {
                    Ok(())
                } else {
                    Err(err)
                }
            })?;
        let stream = Async::new(socket.into_unix_stream())?;

        // The stream becomes writable when connected.
        stream.writable().await?;

        Ok(stream)
    }

    /// Creates an unnamed pair of connected UDS stream sockets.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::os::unix::net::UnixStream;
    ///
    /// # futures_lite::future::block_on(async {
    /// let (stream1, stream2) = Async::<UnixStream>::pair()?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn pair() -> io::Result<(Async<UnixStream>, Async<UnixStream>)> {
        let (stream1, stream2) = UnixStream::pair()?;
        Ok((Async::new(stream1)?, Async::new(stream2)?))
    }
}

impl Async<UnixDatagram> {
    /// Creates a UDS datagram socket bound to the specified path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::os::unix::net::UnixDatagram;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UnixDatagram>::bind("/tmp/socket")?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn bind<P: AsRef<Path>>(path: P) -> io::Result<Async<UnixDatagram>> {
        let path = path.as_ref().to_owned();
        Ok(Async::new(UnixDatagram::bind(path)?)?)
    }

    /// Creates a UDS datagram socket not bound to any address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::os::unix::net::UnixDatagram;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UnixDatagram>::unbound()?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn unbound() -> io::Result<Async<UnixDatagram>> {
        Ok(Async::new(UnixDatagram::unbound()?)?)
    }

    /// Creates an unnamed pair of connected Unix datagram sockets.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::os::unix::net::UnixDatagram;
    ///
    /// # futures_lite::future::block_on(async {
    /// let (socket1, socket2) = Async::<UnixDatagram>::pair()?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub fn pair() -> io::Result<(Async<UnixDatagram>, Async<UnixDatagram>)> {
        let (socket1, socket2) = UnixDatagram::pair()?;
        Ok((Async::new(socket1)?, Async::new(socket2)?))
    }

    /// Receives data from the socket.
    ///
    /// Returns the number of bytes read and the address the message came from.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::os::unix::net::UnixDatagram;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UnixDatagram>::bind("/tmp/socket")?;
    ///
    /// let mut buf = [0u8; 1024];
    /// let (len, addr) = socket.recv_from(&mut buf).await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, UnixSocketAddr)> {
        self.read_with(|io| io.recv_from(buf)).await
    }

    /// Sends data to the specified address.
    ///
    /// Returns the number of bytes written.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::os::unix::net::UnixDatagram;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UnixDatagram>::unbound()?;
    ///
    /// let msg = b"hello";
    /// let addr = "/tmp/socket";
    /// let len = socket.send_to(msg, addr).await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn send_to<P: AsRef<Path>>(&self, buf: &[u8], path: P) -> io::Result<usize> {
        self.write_with(|io| io.send_to(buf, &path)).await
    }

    /// Receives data from the connected peer.
    ///
    /// Returns the number of bytes read and the address the message came from.
    ///
    /// The [`connect`][`UnixDatagram::connect()`] method connects this socket to a remote address.
    /// This method will fail if the socket is not connected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::os::unix::net::UnixDatagram;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UnixDatagram>::bind("/tmp/socket1")?;
    /// socket.get_ref().connect("/tmp/socket2")?;
    ///
    /// let mut buf = [0u8; 1024];
    /// let len = socket.recv(&mut buf).await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        self.read_with(|io| io.recv(buf)).await
    }

    /// Sends data to the connected peer.
    ///
    /// Returns the number of bytes written.
    ///
    /// The [`connect`][`UnixDatagram::connect()`] method connects this socket to a remote address.
    /// This method will fail if the socket is not connected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use async_io::Async;
    /// use std::os::unix::net::UnixDatagram;
    ///
    /// # futures_lite::future::block_on(async {
    /// let socket = Async::<UnixDatagram>::bind("/tmp/socket1")?;
    /// socket.get_ref().connect("/tmp/socket2")?;
    ///
    /// let msg = b"hello";
    /// let len = socket.send(msg).await?;
    /// # std::io::Result::Ok(()) });
    /// ```
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        self.write_with(|io| io.send(buf)).await
    }
}
