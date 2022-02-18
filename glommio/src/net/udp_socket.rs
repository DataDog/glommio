// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use super::datagram::GlommioDatagram;
use nix::sys::socket::{InetAddr, SockAddr};
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    io,
    net::{self, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs},
    os::unix::io::{AsRawFd, FromRawFd, RawFd},
    time::Duration,
};

type Result<T> = crate::Result<T, ()>;

#[derive(Debug)]
/// An Udp Socket.
pub struct UdpSocket {
    socket: GlommioDatagram<net::UdpSocket>,
}

impl From<socket2::Socket> for UdpSocket {
    fn from(socket: socket2::Socket) -> UdpSocket {
        Self {
            socket: GlommioDatagram::<net::UdpSocket>::from(socket),
        }
    }
}

impl AsRawFd for UdpSocket {
    fn as_raw_fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }
}

impl FromRawFd for UdpSocket {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        let socket = socket2::Socket::from_raw_fd(fd);
        UdpSocket::from(socket)
    }
}

impl UdpSocket {
    /// Creates a UDP socket bound to the specified address.
    ///
    /// Binding with port number 0 will request an available port from the OS.
    ///
    /// This sets the ReusePort option on the socket, so if the OS-provided load
    /// balancing is enough, it is possible to just bind to the same address
    /// from multiple executors.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{net::UdpSocket, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = UdpSocket::bind("127.0.0.1:8000").unwrap();
    ///     println!("Listening on {}", listener.local_addr().unwrap());
    /// });
    /// ```
    pub fn bind<A: ToSocketAddrs>(addr: A) -> Result<UdpSocket> {
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
        let sk = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
        let addr = socket2::SockAddr::from(addr);
        sk.set_reuse_port(true)?;
        sk.bind(&addr)?;
        Ok(Self {
            socket: GlommioDatagram::from(sk),
        })
    }

    /// Connects this UDP socket to a remote address, allowing the [`send`] and
    /// [`recv`] methods to be used to send data and also applies filters to
    /// only receive data from the specified address.
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
    /// ```
    /// use glommio::{net::UdpSocket, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     receiver
    ///         .connect(sender.local_addr().unwrap())
    ///         .await
    ///         .unwrap();
    ///     sender
    ///         .connect(receiver.local_addr().unwrap())
    ///         .await
    ///         .unwrap();
    /// });
    /// ```
    ///
    /// [`send`]: UdpSocket::send
    /// [`recv`]: UdpSocket::recv
    pub async fn connect<A: ToSocketAddrs>(&self, addr: A) -> Result<()> {
        let iter = addr.to_socket_addrs()?;
        let mut err = io::Error::new(io::ErrorKind::Other, "No Valid addresses");
        for addr in iter {
            let inet = InetAddr::from_std(&addr);
            let addr = SockAddr::new_inet(inet);
            let reactor = self.socket.reactor.upgrade().unwrap();
            let source = reactor.connect(self.socket.as_raw_fd(), addr);
            match source.collect_rw().await {
                Ok(_) => return Ok(()),
                Err(x) => {
                    err = x;
                }
            };
        }
        Err(err.into())
    }

    /// Sets the buffer size used on the receive path
    pub fn set_buffer_size(&mut self, buffer_size: usize) {
        self.socket.rx_buf_size = buffer_size;
    }

    /// gets the buffer size used
    pub fn buffer_size(&mut self) -> usize {
        self.socket.rx_buf_size
    }

    /// Gets the value of the `SO_BROADCAST` option for this socket.
    ///
    /// For more information about this option, see
    /// [`UdpSocket::set_broadcast`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::UdpSocket, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let s = UdpSocket::bind("127.0.0.1:10000").unwrap();
    /// s.set_broadcast(false).expect("set_broadcast call failed");
    /// assert_eq!(s.broadcast().unwrap(), false);
    /// # })
    /// ```
    pub fn broadcast(&self) -> Result<bool> {
        Ok(self.socket.socket.broadcast()?)
    }

    /// Sets the value of the `SO_BROADCAST` option for this socket.
    ///
    /// When enabled, this socket is allowed to send packets to a broadcast
    /// address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::UdpSocket, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let s = UdpSocket::bind("127.0.0.1:10000").unwrap();
    /// s.set_broadcast(false).expect("set_broadcast call failed");
    /// # })
    /// ```
    pub fn set_broadcast(&self, broadcast: bool) -> Result<()> {
        Ok(self.socket.socket.set_broadcast(broadcast)?)
    }

    /// Executes an operation of the `IP_ADD_MEMBERSHIP` type.
    ///
    /// This function specifies a new multicast group for this socket to join.
    /// The address must be a valid multicast address, and `interface` is the
    /// address of the local interface with which the system should join the
    /// multicast group. If it's equal to `INADDR_ANY` then an appropriate
    /// interface is chosen by the system.
    pub fn join_multicast_v4(&self, multiaddr: &Ipv4Addr, interface: &Ipv4Addr) -> Result<()> {
        Ok(self.socket.socket.join_multicast_v4(multiaddr, interface)?)
    }

    /// Executes an operation of the `IPV6_ADD_MEMBERSHIP` type.
    ///
    /// This function specifies a new multicast group for this socket to join.
    /// The address must be a valid multicast address, and `interface` is the
    /// index of the interface to join/leave (or 0 to indicate any interface).
    pub fn join_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> Result<()> {
        Ok(self.socket.socket.join_multicast_v6(multiaddr, interface)?)
    }

    /// Executes an operation of the `IP_DROP_MEMBERSHIP` type.
    ///
    /// For more information about this option, see
    /// [`UdpSocket::join_multicast_v4`].
    pub fn leave_multicast_v4(&self, multiaddr: &Ipv4Addr, interface: &Ipv4Addr) -> Result<()> {
        Ok(self
            .socket
            .socket
            .leave_multicast_v4(multiaddr, interface)?)
    }

    /// Executes an operation of the `IPV6_DROP_MEMBERSHIP` type.
    ///
    /// For more information about this option, see
    /// [`UdpSocket::join_multicast_v6`].
    pub fn leave_multicast_v6(&self, multiaddr: &Ipv6Addr, interface: u32) -> Result<()> {
        Ok(self
            .socket
            .socket
            .leave_multicast_v6(multiaddr, interface)?)
    }

    /// Gets the value of the `IP_MULTICAST_LOOP` option for this socket.
    ///
    /// For more information about this option, see
    /// [`UdpSocket::set_multicast_loop_v4`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::UdpSocket, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let s = UdpSocket::bind("127.0.0.1:10000").unwrap();
    /// s.set_multicast_loop_v4(false)
    ///     .expect("set_multicast_loop_v4 call failed");
    /// assert_eq!(s.multicast_loop_v4().unwrap(), false);
    /// # })
    /// ```
    pub fn multicast_loop_v4(&self) -> Result<bool> {
        Ok(self.socket.socket.multicast_loop_v4()?)
    }

    /// Sets the value of the `IP_MULTICAST_LOOP` option for this socket.
    ///
    /// If enabled, multicast packets will be looped back to the local socket.
    /// Note that this may not have any effect on IPv6 sockets.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::UdpSocket, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let s = UdpSocket::bind("127.0.0.1:10000").unwrap();
    /// s.set_multicast_loop_v4(false)
    ///     .expect("set_multicast_loop_v4 call failed");
    /// # })
    /// ```
    pub fn set_multicast_loop_v4(&self, multicast_loop_v4: bool) -> Result<()> {
        Ok(self
            .socket
            .socket
            .set_multicast_loop_v4(multicast_loop_v4)?)
    }

    /// Gets the value of the `IPV6_MULTICAST_LOOP` option for this socket.
    ///
    /// For more information about this option, see
    /// [`UdpSocket::set_multicast_loop_v6`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::UdpSocket, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let s = UdpSocket::bind("127.0.0.1:10000").unwrap();
    /// s.set_multicast_loop_v6(false)
    ///     .expect("set_multicast_loop_v6 call failed");
    /// assert_eq!(s.multicast_loop_v6().unwrap(), false);
    /// # })
    /// ```
    pub fn multicast_loop_v6(&self) -> Result<bool> {
        Ok(self.socket.socket.multicast_loop_v6()?)
    }

    /// Sets the value of the `IPV6_MULTICAST_LOOP` option for this socket.
    ///
    /// Controls whether this socket sees the multicast packets it sends itself.
    /// Note that this may not have any affect on IPv4 sockets.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::UdpSocket, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let s = UdpSocket::bind("127.0.0.1:10000").unwrap();
    /// s.set_multicast_loop_v6(false)
    ///     .expect("set_multicast_loop_v6 call failed");
    /// # })
    /// ```
    pub fn set_multicast_loop_v6(&self, multicast_loop_v6: bool) -> Result<()> {
        Ok(self
            .socket
            .socket
            .set_multicast_loop_v6(multicast_loop_v6)?)
    }

    /// Gets the value of the `IP_MULTICAST_TTL` option for this socket.
    ///
    /// For more information about this option, see
    /// [`UdpSocket::set_multicast_ttl_v4`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::UdpSocket, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let s = UdpSocket::bind("127.0.0.1:10000").unwrap();
    /// s.set_multicast_ttl_v4(42)
    ///     .expect("set_multicast_ttl_v4 call failed");
    /// assert_eq!(s.multicast_ttl_v4().unwrap(), 42);
    /// # })
    /// ```
    pub fn multicast_ttl_v4(&self) -> Result<u32> {
        Ok(self.socket.socket.multicast_ttl_v4()?)
    }

    /// Sets the value of the `IP_MULTICAST_TTL` option for this socket.
    ///
    /// Indicates the time-to-live value of outgoing multicast packets for
    /// this socket. The default value is 1 which means that multicast packets
    /// don't leave the local network unless explicitly requested.
    ///
    /// Note that this may not have any effect on IPv6 sockets.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::UdpSocket, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let s = UdpSocket::bind("127.0.0.1:10000").unwrap();
    /// s.set_multicast_ttl_v4(42)
    ///     .expect("set_multicast_ttl_v4 call failed");
    /// # })
    /// ```
    pub fn set_multicast_ttl_v4(&self, multicast_ttl_v4: u32) -> Result<()> {
        Ok(self.socket.socket.set_multicast_ttl_v4(multicast_ttl_v4)?)
    }

    /// Gets the value of the `IP_TTL` option for this socket.
    ///
    /// For more information about this option, see [`UdpSocket::set_ttl`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::UdpSocket, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let s = UdpSocket::bind("127.0.0.1:10000").unwrap();
    /// s.set_ttl(42).expect("set_ttl call failed");
    /// assert_eq!(s.ttl().unwrap(), 42);
    /// # })
    /// ```
    pub fn ttl(&self) -> Result<u32> {
        Ok(self.socket.socket.ttl()?)
    }

    /// Sets the value for the `IP_TTL` option on this socket.
    ///
    /// This value sets the time-to-live field that is used in every packet sent
    /// from this socket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::UdpSocket, LocalExecutor};
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let s = UdpSocket::bind("127.0.0.1:10000").unwrap();
    /// s.set_ttl(42).expect("set_ttl call failed");
    /// # })
    /// ```
    pub fn set_ttl(&self, ttl: u32) -> Result<()> {
        Ok(self.socket.socket.set_ttl(ttl)?)
    }

    /// Sets the read timeout to the timeout specified.
    ///
    /// If the value specified is [`None`], then read calls will block
    /// indefinitely. An [`Err`] is returned if the zero [`Duration`] is
    /// passed to this method.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use glommio::{net::UdpSocket, LocalExecutor};
    /// # use std::time::Duration;
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let s = UdpSocket::bind("127.0.0.1:10000").unwrap();
    /// s.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    /// # })
    /// ```
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.socket.set_read_timeout(dur)
    }

    /// Sets the write timeout to the timeout specified.
    ///
    /// If the value specified is [`None`], then write calls will block
    /// indefinitely. An [`Err`] is returned if the zero [`Duration`] is
    /// passed to this method.
    ///
    /// ```no_run
    /// # use glommio::{net::UdpSocket, LocalExecutor};
    /// # use std::time::Duration;
    /// # let ex = LocalExecutor::default();
    /// # ex.run(async move {
    /// let s = UdpSocket::bind("127.0.0.1:10000").unwrap();
    /// s.set_write_timeout(Some(Duration::from_secs(1))).unwrap();
    /// # })
    /// ```
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> Result<()> {
        self.socket.set_write_timeout(dur)
    }

    /// Returns the read timeout of this socket.
    pub fn read_timeout(&self) -> Option<Duration> {
        self.socket.read_timeout()
    }

    /// Returns the write timeout of this socket.
    pub fn write_timeout(&self) -> Option<Duration> {
        self.socket.write_timeout()
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
    /// ```
    /// use glommio::{net::UdpSocket, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     receiver
    ///         .connect(sender.local_addr().unwrap())
    ///         .await
    ///         .unwrap();
    ///     sender
    ///         .send_to(&[1; 1], receiver.local_addr().unwrap())
    ///         .await
    ///         .unwrap();
    ///     let mut buf = vec![0; 32];
    ///     let sz = receiver.peek(&mut buf).await.unwrap();
    ///     assert_eq!(sz, 1);
    /// })
    /// ```
    ///
    /// [`connect`]: UdpSocket::connect
    pub async fn peek(&self, buf: &mut [u8]) -> Result<usize> {
        let _ = self.peer_addr()?;
        self.socket.peek(buf).await.map_err(Into::into)
    }

    ///Receives a single datagram message on the socket, without removing it
    /// from the queue. On success, returns the number of bytes read and the
    /// origin.
    ///
    /// The function must be called with valid byte array buf of sufficient size
    /// to hold the message bytes. If a message is too long to fit in the
    /// supplied buffer, excess bytes may be discarded.
    #[track_caller]
    pub async fn peek_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let (sz, addr) = self.socket.peek_from(buf).await?;

        let addr = match addr {
            nix::sys::socket::SockAddr::Inet(addr) => addr,
            x => panic!("invalid socket addr for this family!: {:?}", x),
        };
        Ok((sz, addr.to_std()))
    }

    /// Returns the socket address of the remote peer this socket was connected
    /// to.
    pub fn peer_addr(&self) -> Result<SocketAddr> {
        self.socket.socket.peer_addr().map_err(Into::into)
    }

    /// Returns the socket address of the local half of this UDP connection.
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
    /// ```
    /// use glommio::{net::UdpSocket, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     receiver
    ///         .connect(sender.local_addr().unwrap())
    ///         .await
    ///         .unwrap();
    ///     sender
    ///         .send_to(&[1; 1], receiver.local_addr().unwrap())
    ///         .await
    ///         .unwrap();
    ///     let mut buf = vec![0; 32];
    ///     let sz = receiver.recv(&mut buf).await.unwrap();
    ///     assert_eq!(sz, 1);
    /// })
    /// ```
    ///
    /// [`connect`]: UdpSocket::connect
    pub async fn recv(&self, buf: &mut [u8]) -> Result<usize> {
        let _ = self.peer_addr()?;
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
    /// ```
    /// use glommio::{net::UdpSocket, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     sender
    ///         .send_to(&[1; 1], receiver.local_addr().unwrap())
    ///         .await
    ///         .unwrap();
    ///     let mut buf = vec![0; 32];
    ///     let (sz, addr) = receiver.recv_from(&mut buf).await.unwrap();
    ///     assert_eq!(sz, 1);
    ///     assert_eq!(addr, sender.local_addr().unwrap());
    /// })
    /// ```
    #[track_caller]
    pub async fn recv_from(&self, buf: &mut [u8]) -> Result<(usize, SocketAddr)> {
        let (sz, addr) = self.socket.recv_from(buf).await?;
        let addr = match addr {
            nix::sys::socket::SockAddr::Inet(addr) => addr,
            x => panic!("invalid socket addr for this family!: {:?}", x),
        };
        Ok((sz, addr.to_std()))
    }

    /// Sends data on the socket to the given address. On success, returns the
    /// number of bytes written. Address type can be any implementor of
    /// [`ToSocketAddrs`] trait. See its documentation for concrete examples.
    /// It is possible for `addr` to yield multiple addresses, but send_to will
    /// only send data to the first address yielded by `addr`.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{net::UdpSocket, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     sender
    ///         .send_to(&[1; 1], receiver.local_addr().unwrap())
    ///         .await
    ///         .unwrap();
    ///     let mut buf = vec![0; 32];
    ///     let (sz, addr) = receiver.recv_from(&mut buf).await.unwrap();
    ///     assert_eq!(sz, 1);
    ///     assert_eq!(addr, sender.local_addr().unwrap());
    /// })
    /// ```
    ///
    /// [`ToSocketAddrs`]: https://doc.rust-lang.org/stable/std/net/trait.ToSocketAddrs.html
    pub async fn send_to<A: ToSocketAddrs>(&self, buf: &[u8], addr: A) -> Result<usize> {
        let addr = addr
            .to_socket_addrs()
            .unwrap()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "empty address"))?;

        let inet = nix::sys::socket::InetAddr::from_std(&addr);
        let sockaddr = nix::sys::socket::SockAddr::new_inet(inet);
        self.socket.send_to(buf, sockaddr).await.map_err(Into::into)
    }

    /// Sends data on the socket to the remote address to which it is connected.
    ///
    /// [`UdpSocket::connect`] will connect this socket to a remote address.
    /// This method will fail if the socket is not connected.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::{net::UdpSocket, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     sender
    ///         .connect(receiver.local_addr().unwrap())
    ///         .await
    ///         .unwrap();
    ///     sender.send(&[1; 1]).await.unwrap();
    /// })
    /// ```
    ///
    /// `[UdpSocket::connect`]: UdpSocket::connect
    pub async fn send(&self, buf: &[u8]) -> Result<usize> {
        self.socket.send(buf).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{timer::Timer, LocalExecutorBuilder};
    use nix::sys::socket::MsgFlags;
    use std::time::Duration;

    macro_rules! connected_pair {
        () => {{
            let s1 = UdpSocket::bind("127.0.0.1:0").unwrap();
            let s2 = UdpSocket::bind("127.0.0.1:0").unwrap();
            s1.connect(s2.local_addr().unwrap()).await.unwrap();
            s2.connect(s1.local_addr().unwrap()).await.unwrap();
            (s1, s2)
        }};
    }

    #[should_panic]
    #[test]
    fn udp_unconnected_recv() {
        test_executor!(async move {
            let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
            let mut buf = [0u8; 1];
            receiver.recv(&mut buf).await.unwrap();
        });
    }

    #[should_panic]
    #[test]
    fn udp_unconnected_peek() {
        test_executor!(async move {
            let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
            let mut buf = [0u8; 1];
            receiver.peek(&mut buf).await.unwrap();
        });
    }

    #[should_panic]
    #[test]
    fn udp_unconnected_send() {
        test_executor!(async move {
            let conn = UdpSocket::bind("127.0.0.1:0").unwrap();
            conn.send(&[1]).await.unwrap();
        });
    }

    #[test]
    fn multi_executor_bind_works() {
        test_executor!(async move {
            let addr_picker = UdpSocket::bind("127.0.0.1:0").unwrap();
            let addr = addr_picker.local_addr().unwrap();

            let ex1 = LocalExecutorBuilder::default()
                .spawn(move || async move {
                    let socket = UdpSocket::bind(addr).unwrap();
                    let mut buf = [0u8; 1];
                    println!("will receive");
                    let (sz, _) = socket.recv_from(&mut buf).await.unwrap();
                    assert_eq!(sz, 1);
                    assert_eq!(buf[0], 65);
                    println!("received1");
                })
                .unwrap();

            let ex2 = LocalExecutorBuilder::default()
                .spawn(move || async move {
                    let socket = UdpSocket::bind(addr).unwrap();
                    let mut buf = [0u8; 1];
                    let (sz, _) = socket.recv_from(&mut buf).await.unwrap();
                    assert_eq!(sz, 1);
                    assert_eq!(buf[0], 65);
                })
                .unwrap();

            Timer::new(Duration::from_millis(100)).await;

            // Because we can't rely on how the load balancing will happen,
            // we just send a bunch. There seems to be affinity, so every time
            // we send we create a new source address
            for _ in 0..1000 {
                let client = UdpSocket::bind("127.0.0.1:0").unwrap();
                client.send_to(&[65; 1], addr).await.unwrap();
            }

            ex1.join().unwrap();
            ex2.join().unwrap();
        });
    }

    #[test]
    fn udp_connect_peers() {
        test_executor!(async move {
            let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
            let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
            receiver
                .connect(sender.local_addr().unwrap())
                .await
                .unwrap();
            sender
                .connect(receiver.local_addr().unwrap())
                .await
                .unwrap();

            assert_eq!(receiver.peer_addr().unwrap(), sender.local_addr().unwrap());
            assert_eq!(sender.peer_addr().unwrap(), receiver.local_addr().unwrap());
        });
    }

    #[test]
    fn udp_connected_ping_pong() {
        test_executor!(async move {
            let (receiver, sender) = connected_pair!();
            sender.send(&[65u8; 1]).await.unwrap();

            let mut buf = [0u8; 1];
            assert_eq!(1, receiver.recv(&mut buf).await.unwrap());
            assert_eq!(buf[0], 65);

            receiver.send(&[64u8; 1]).await.unwrap();
            let mut buf = [0u8; 1];
            assert_eq!(1, sender.recv(&mut buf).await.unwrap());
            assert_eq!(buf[0], 64);
        });
    }

    #[test]
    fn udp_connected_recv_filter() {
        test_executor!(async move {
            let (receiver, sender) = connected_pair!();
            let other_sender = UdpSocket::bind("127.0.0.1:0").unwrap();
            for _ in 0..10 {
                sender.send(&[65u8; 1]).await.unwrap();
                // because we are connected, those messages will never arrive.
                other_sender
                    .send_to(&[64u8; 1], receiver.local_addr().unwrap())
                    .await
                    .unwrap();
            }

            for _ in 0..10 {
                let mut buf = [0u8; 1];
                assert_eq!(1, receiver.recv(&mut buf).await.unwrap());
                assert_eq!(buf[0], 65);
            }
        });
    }

    #[test]
    fn zero_sized_send() {
        test_executor!(async move {
            let (receiver, sender) = connected_pair!();

            let recv_handle = crate::spawn_local(async move {
                let mut buf = [0u8; 10];
                // try to receive 10 bytes, but will assert that none comes back.
                let sz = receiver.recv(&mut buf).await.unwrap();
                assert_eq!(sz, 0);
            })
            .detach();

            Timer::new(Duration::from_millis(100)).await;

            sender.send(&[]).await.unwrap();
            recv_handle.await.unwrap();
        });
    }

    #[test]
    fn peek() {
        test_executor!(async move {
            let (receiver, sender) = connected_pair!();

            let receiver_handle = crate::spawn_local(async move {
                for _ in 0..10 {
                    let mut buf = [0u8; 40];
                    let sz = receiver.peek(&mut buf).await.unwrap();
                    assert_eq!(sz, 4);
                    assert_eq!(&buf[0..4], b"msg1");
                }

                let mut buf = [0u8; 40];
                let sz = receiver.recv(&mut buf).await.unwrap();
                assert_eq!(sz, 4);
                assert_eq!(&buf[0..4], b"msg1");
            })
            .detach();

            sender.send(b"msg1").await.unwrap();

            receiver_handle.await.unwrap();
        });
    }

    // sends first and then receive, so will hit the nonblocking path.
    #[test]
    fn peekfrom_non_blocking() {
        test_executor!(async move {
            let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
            let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
            let addr = receiver.local_addr().unwrap();

            sender.send_to(&[1], addr).await.unwrap();

            let mut buf = [0u8; 1];
            let (sz, from) = receiver.peek_from(&mut buf).await.unwrap();
            assert_eq!(sz, 1);
            assert_eq!(from, sender.local_addr().unwrap());
        });
    }

    // like the previous test, but recvs first so likely hits the io_uring path
    #[test]
    fn peekfrom_blocking() {
        test_executor!(async move {
            let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
            let sender = UdpSocket::bind("127.0.0.1:0").unwrap();

            let addr = receiver.local_addr().unwrap();

            let receive_handle = crate::spawn_local(async move {
                let mut buf = [0u8; 1];
                for _ in 0..10 {
                    let (sz, _) = receiver
                        .socket
                        .recv_from_blocking(&mut buf, MsgFlags::MSG_PEEK)
                        .await
                        .unwrap();
                    assert_eq!(sz, 1);
                }
                let (_, from) = receiver
                    .socket
                    .recv_from_blocking(&mut buf, MsgFlags::MSG_PEEK)
                    .await
                    .unwrap();
                let addr = match from {
                    nix::sys::socket::SockAddr::Inet(addr) => addr,
                    x => panic!("invalid socket addr for this family!: {:?}", x),
                };
                addr.to_std()
            })
            .detach();

            Timer::new(Duration::from_millis(100)).await;
            let sender_addr = sender.local_addr().unwrap();
            sender.send_to(&[1], addr).await.unwrap();

            let from = receive_handle.await.unwrap();
            assert_eq!(from, sender_addr);
        });
    }

    // sends first and then receive, so will hit the nonblocking path.
    #[test]
    fn recvfrom_non_blocking() {
        test_executor!(async move {
            let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
            let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
            let addr = receiver.local_addr().unwrap();

            sender.send_to(&[1], addr).await.unwrap();

            let mut buf = [0u8; 1];
            let (sz, from) = receiver.recv_from(&mut buf).await.unwrap();
            assert_eq!(sz, 1);
            assert_eq!(from, sender.local_addr().unwrap());
        });
    }

    // like the previous test, but recvs first so likely hits the io_uring path
    #[test]
    fn recvfrom_blocking() {
        test_executor!(async move {
            let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
            let sender = UdpSocket::bind("127.0.0.1:0").unwrap();

            let addr = receiver.local_addr().unwrap();

            let receive_handle = crate::spawn_local(async move {
                let mut buf = [0u8; 1];
                let (sz, from) = receiver
                    .socket
                    .recv_from_blocking(&mut buf, MsgFlags::empty())
                    .await
                    .unwrap();
                assert_eq!(sz, 1);
                let addr = match from {
                    nix::sys::socket::SockAddr::Inet(addr) => addr,
                    x => panic!("invalid socket addr for this family!: {:?}", x),
                };
                addr.to_std()
            })
            .detach();

            Timer::new(Duration::from_millis(100)).await;
            let sender_addr = sender.local_addr().unwrap();
            sender.send_to(&[1], addr).await.unwrap();

            let from = receive_handle.await.unwrap();
            assert_eq!(from, sender_addr);
        });
    }

    #[test]
    fn sendto() {
        test_executor!(async move {
            let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
            let me = UdpSocket::bind("127.0.0.1:0").unwrap();
            receiver.connect(me.local_addr().unwrap()).await.unwrap();

            let addr = receiver.local_addr().unwrap();
            me.send_to(&[65u8; 1], addr).await.unwrap();

            let mut buf = [0u8; 1];
            let sz = receiver.recv(&mut buf).await.unwrap();
            assert_eq!(sz, 1);
            assert_eq!(buf[0], 65u8);
        });
    }

    #[test]
    fn sendto_blocking() {
        test_executor!(async move {
            let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
            let addr = receiver.local_addr().unwrap();

            let inet = nix::sys::socket::InetAddr::from_std(&addr);
            let sockaddr = nix::sys::socket::SockAddr::new_inet(inet);
            let me = UdpSocket::bind("127.0.0.1:0").unwrap();
            me.socket
                .send_to_blocking(&[65u8; 1], sockaddr)
                .await
                .unwrap();

            receiver.connect(me.local_addr().unwrap()).await.unwrap();
            let mut buf = [0u8; 1];
            let sz = receiver.recv(&mut buf).await.unwrap();
            assert_eq!(sz, 1);
            assert_eq!(buf[0], 65u8);
        });
    }

    #[test]
    fn broadcast() {
        test_executor!(async move {
            let s = UdpSocket::bind("127.0.0.1:0").unwrap();
            s.set_broadcast(false).expect("set_broadcast call failed");
            assert!(!s.broadcast().unwrap());
        });
    }

    #[test]
    fn multicast_v4() {
        test_executor!(async move {
            let multicast_addr = Ipv4Addr::new(239, 0, 0, 0);
            assert!(multicast_addr.is_multicast());
            let interface_addr = Ipv4Addr::new(0, 0, 0, 0);
            let s = UdpSocket::bind("127.0.0.1:0").unwrap();
            s.join_multicast_v4(&multicast_addr, &interface_addr)
                .unwrap();
            s.leave_multicast_v4(&multicast_addr, &interface_addr)
                .unwrap();
        });
    }

    #[test]
    fn multicast_v6() {
        test_executor!(async move {
            let multicast_addr = Ipv6Addr::new(0xff00, 0, 0, 0, 0, 0, 0, 0);
            assert!(multicast_addr.is_multicast());
            let s = UdpSocket::bind("::1:0").unwrap();
            s.join_multicast_v6(&multicast_addr, 0).unwrap();
            s.leave_multicast_v6(&multicast_addr, 0).unwrap();
        });
    }

    #[test]
    fn set_multicast_loop_v4() {
        test_executor!(async move {
            let s = UdpSocket::bind("127.0.0.1:0").unwrap();
            s.set_multicast_loop_v4(false)
                .expect("set_multicast_loop_v4 call failed");
            assert!(!s.multicast_loop_v4().unwrap());
        });
    }

    #[test]
    fn set_multicast_loop_v6() {
        test_executor!(async move {
            let s = UdpSocket::bind("::1:0").unwrap();
            s.set_multicast_loop_v6(false)
                .expect("set_multicast_loop_v6 call failed");
            assert!(!s.multicast_loop_v6().unwrap());
        });
    }

    #[test]
    fn set_multicast_ttl_v4() {
        test_executor!(async move {
            let s = UdpSocket::bind("127.0.0.1:0").unwrap();
            s.set_multicast_ttl_v4(42)
                .expect("set_multicast_ttl_v4 call failed");
            assert_eq!(s.multicast_ttl_v4().unwrap(), 42);
        });
    }

    #[test]
    fn set_ttl() {
        test_executor!(async move {
            let s = UdpSocket::bind("127.0.0.1:0").unwrap();
            s.set_ttl(42).expect("set_ttl call failed");
            assert_eq!(s.ttl().unwrap(), 42);
        });
    }
}
