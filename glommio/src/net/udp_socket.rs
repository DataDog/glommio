// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::parking::Reactor;
use crate::sys::{self, DmaBuffer, Source, SourceType};
use crate::{ByteSliceMutExt, Local};
use iou::{InetAddr, SockAddr};
use socket2::{Domain, Protocol, Socket, Type};
use std::cell::Cell;
use std::io;
use std::net::{self, SocketAddr, ToSocketAddrs};
use std::os::unix::io::AsRawFd;
use std::rc::{Rc, Weak};

const DEFAULT_BUFFER_SIZE: usize = 8192;

#[derive(Debug)]
/// An Udp Socket.
pub struct UdpSocket {
    reactor: Weak<Reactor>,
    socket: net::UdpSocket,

    // you only live once, you've got no time to block! if this is set to true try a direct non-blocking syscall otherwise schedule for sending later over the ring
    //
    // If you are familiar with high throughput networking code you might have seen similar
    // techniques with names such as "optimistic" "speculative" or things like that. But frankly "yolo" is such a
    // better name. Calling this "yolo" is likely glommio's biggest contribution to humankind.
    tx_yolo: Cell<bool>,
    rx_yolo: Cell<bool>,

    rx_buf_size: usize,
}

impl UdpSocket {
    /// Creates a UDP socket bound to the specified address.
    ///
    /// Binding with port number 0 will request an available port from the OS.
    ///
    /// This sets the ReusePort option on the socket, so if the OS-provided load
    /// balancing is enough, it is possible to just bind to the same address from
    /// multiple executors.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::net::UdpSocket;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = UdpSocket::bind("127.0.0.1:8000").unwrap();
    ///     println!("Listening on {}", listener.local_addr().unwrap());
    /// });
    /// ```
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<UdpSocket> {
        let addr = addr
            .to_socket_addrs()
            .unwrap()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "empty address"))?;

        let domain = if addr.is_ipv6() {
            Domain::ipv6()
        } else {
            Domain::ipv4()
        };
        let sk = Socket::new(domain, Type::dgram(), Some(Protocol::udp()))?;
        let addr = socket2::SockAddr::from(addr);
        sk.set_reuse_port(true)?;
        sk.bind(&addr)?;
        let socket = sk.into_udp_socket();

        //let socket = net::UdpSocket::bind(addr)?;
        let reactor = Local::get_reactor();
        Ok(Self {
            reactor: Rc::downgrade(&reactor),
            socket,
            tx_yolo: Cell::new(true),
            rx_yolo: Cell::new(true),
            rx_buf_size: DEFAULT_BUFFER_SIZE,
        })
    }

    /// Connects this UDP socket to a remote address, allowing the [`send`] and [`recv`] methods to be
    /// used to send data and also applies filters to only receive data from the specified address.
    ///
    /// If addr yields multiple addresses, connect will be attempted with each of the addresses
    /// until the underlying OS function returns no error. Note that usually, a successful connect
    /// call does not specify that there is a remote server listening on the port, rather, such an
    /// error would only be detected after the first send. If the OS returns an error for each of
    /// the specified addresses, the error returned from the last connection attempt (the last
    /// address) is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::net::UdpSocket;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     receiver.connect(sender.local_addr().unwrap()).await.unwrap();
    ///     sender.connect(receiver.local_addr().unwrap()).await.unwrap();
    /// });
    /// ```
    ///
    /// [`send`]: UdpSocket::send
    /// [`recv`]: UdpSocket::recv
    pub async fn connect<A: ToSocketAddrs>(&self, addr: A) -> io::Result<()> {
        let iter = addr.to_socket_addrs()?;
        let mut err = io::Error::new(io::ErrorKind::Other, "No Valid addresses");
        for addr in iter {
            let inet = InetAddr::from_std(&addr);
            let addr = SockAddr::new_inet(inet);
            let reactor = self.reactor.upgrade().unwrap();
            let source = reactor.connect(self.socket.as_raw_fd(), addr);
            match source.collect_rw().await {
                Ok(_) => return Ok(()),
                Err(x) => {
                    err = x;
                }
            };
        }
        Err(err)
    }

    async fn consume_receive_buffer(&self, source: &Source, buf: &mut [u8]) -> io::Result<usize> {
        let sz = source.collect_rw().await?;
        let src = match source.extract_source_type() {
            SourceType::SockRecv(mut buf) => {
                let mut buf = buf.take().unwrap();
                buf.trim_to_size(sz);
                buf
            }
            _ => unreachable!(),
        };
        buf[0..sz].copy_from_slice(&src.as_bytes()[0..sz]);
        self.rx_yolo.set(true);
        Ok(sz)
    }

    /// Sets the buffer size used on the receive path
    pub fn set_buffer_size(&mut self, buffer_size: usize) {
        self.rx_buf_size = buffer_size;
    }

    /// gets the buffer size used
    pub fn buffer_size(&mut self) -> usize {
        self.rx_buf_size
    }

    /// Receives single datagram on the socket from the remote address to which it is connected,
    /// without removing the message from input queue. On success, returns the number of bytes
    /// peeked.
    ///
    /// The function must be called with valid byte array buf of sufficient size to hold the
    /// message bytes. If a message is too long to fit in the supplied buffer, excess bytes may be
    /// discarded.
    ///
    /// To use this function, [`connect`] must have been called
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::net::UdpSocket;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     receiver.connect(sender.local_addr().unwrap()).await.unwrap();
    ///     sender.send_to(&[1; 1], receiver.local_addr().unwrap()).await.unwrap();
    ///     let mut buf = vec![0; 32];
    ///     let sz = receiver.peek(&mut buf).await.unwrap();
    ///     assert_eq!(sz, 1);
    /// })
    /// ```
    ///
    /// [`connect`]: UdpSocket::connect
    pub async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let _ = self.peer_addr()?;
        let source = self.reactor.upgrade().unwrap().recv(
            self.socket.as_raw_fd(),
            buf.len(),
            iou::MsgFlags::MSG_PEEK,
        );

        self.consume_receive_buffer(&source, buf).await
    }

    ///Receives a single datagram message on the socket, without removing it from the queue. On
    ///success, returns the number of bytes read and the origin.
    ///
    /// The function must be called with valid byte array buf of sufficient size to hold the
    /// message bytes. If a message is too long to fit in the supplied buffer, excess bytes may be
    /// discarded.
    pub async fn peek_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (sz, addr) = match self.yolo_recvmsg(buf, iou::MsgFlags::MSG_PEEK) {
            Some(res) => res?,
            None => {
                self.recv_from_blocking(buf, iou::MsgFlags::MSG_PEEK)
                    .await?
            }
        };

        let addr = match addr {
            nix::sys::socket::SockAddr::Inet(addr) => addr,
            x => panic!("invalid socket addr for this family!: {:?}", x),
        };
        Ok((sz, addr.to_std()))
    }

    /// Returns the socket address of the remote peer this socket was connected to.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.socket.peer_addr()
    }

    /// Returns the socket address of the local half of this UDP connection.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Receives a single datagram message on the socket from the remote address to which it is
    /// connected.
    ///
    /// On success, returns the number of bytes read.  The function must be called with
    /// valid byte array buf of sufficient size to hold the message bytes. If a message is too long
    /// to fit in the supplied buffer, excess bytes may be discarded.
    ///
    ///
    /// To use this function, [`connect`] must have been called
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::net::UdpSocket;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     receiver.connect(sender.local_addr().unwrap()).await.unwrap();
    ///     sender.send_to(&[1; 1], receiver.local_addr().unwrap()).await.unwrap();
    ///     let mut buf = vec![0; 32];
    ///     let sz = receiver.recv(&mut buf).await.unwrap();
    ///     assert_eq!(sz, 1);
    /// })
    /// ```
    ///
    /// [`connect`]: UdpSocket::connect
    pub async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
        let _ = self.peer_addr()?;

        match self.yolo_rx(buf) {
            Some(x) => x,
            None => {
                let source = self
                    .reactor
                    .upgrade()
                    .unwrap()
                    .rushed_recv(self.socket.as_raw_fd(), buf.len())?;
                self.consume_receive_buffer(&source, buf).await
            }
        }
    }

    async fn recv_from_blocking(
        &self,
        buf: &mut [u8],
        flags: iou::MsgFlags,
    ) -> io::Result<(usize, nix::sys::socket::SockAddr)> {
        let source = self.reactor.upgrade().unwrap().rushed_recvmsg(
            self.socket.as_raw_fd(),
            buf.len(),
            flags,
        )?;
        let sz = source.collect_rw().await?;
        match source.extract_source_type() {
            SourceType::SockRecvMsg(mut src, _iov, hdr, addr) => {
                let mut src = src.take().unwrap();
                src.trim_to_size(sz);
                buf[0..sz].copy_from_slice(&src.as_bytes()[0..sz]);
                let addr = unsafe { sys::ssptr_to_sockaddr(addr, hdr.msg_namelen as _)? };
                self.rx_yolo.set(true);
                Ok((sz, addr))
            }
            _ => unreachable!(),
        }
    }

    /// Receives a single datagram message on the socket. On success, returns the number of bytes read and the origin.
    ///
    /// The function must be called with valid byte array buf of sufficient size to hold the message bytes.
    /// If a message is too long to fit in the supplied buffer, excess bytes may be discarded.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::net::UdpSocket;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     sender.send_to(&[1; 1], receiver.local_addr().unwrap()).await.unwrap();
    ///     let mut buf = vec![0; 32];
    ///     let (sz, addr) = receiver.recv_from(&mut buf).await.unwrap();
    ///     assert_eq!(sz, 1);
    ///     assert_eq!(addr, sender.local_addr().unwrap());
    /// })
    /// ```
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        let (sz, addr) = match self.yolo_recvmsg(buf, iou::MsgFlags::empty()) {
            Some(res) => res?,
            None => self.recv_from_blocking(buf, iou::MsgFlags::empty()).await?,
        };

        let addr = match addr {
            nix::sys::socket::SockAddr::Inet(addr) => addr,
            x => panic!("invalid socket addr for this family!: {:?}", x),
        };
        Ok((sz, addr.to_std()))
    }

    async fn send_to_blocking(
        &self,
        buf: &[u8],
        sockaddr: nix::sys::socket::SockAddr,
    ) -> io::Result<usize> {
        let mut dma = self.allocate_buffer(buf.len());
        assert_eq!(dma.write_at(0, buf), buf.len());
        let source = self.reactor.upgrade().unwrap().rushed_sendmsg(
            self.socket.as_raw_fd(),
            dma,
            sockaddr,
        )?;
        let ret = source.collect_rw().await?;
        self.tx_yolo.set(true);
        Ok(ret)
    }

    /// Sends data on the socket to the given address. On success, returns the number of bytes written.
    /// Address type can be any implementor of [`ToSocketAddrs`] trait. See its documentation for concrete examples.
    /// It is possible for addr to yield multiple addresses, but send_to will only send data to the first address yielded by addr.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::net::UdpSocket;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     sender.send_to(&[1; 1], receiver.local_addr().unwrap()).await.unwrap();
    ///     let mut buf = vec![0; 32];
    ///     let (sz, addr) = receiver.recv_from(&mut buf).await.unwrap();
    ///     assert_eq!(sz, 1);
    ///     assert_eq!(addr, sender.local_addr().unwrap());
    /// })
    /// ```
    ///
    /// [`ToSocketAddrs`]: https://doc.rust-lang.org/stable/std/net/trait.ToSocketAddrs.html
    pub async fn send_to<A: ToSocketAddrs>(&self, buf: &[u8], addr: A) -> io::Result<usize> {
        let addr = addr
            .to_socket_addrs()
            .unwrap()
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "empty address"))?;

        let inet = nix::sys::socket::InetAddr::from_std(&addr);
        let mut sockaddr = nix::sys::socket::SockAddr::new_inet(inet);

        match self.yolo_sendmsg(buf, &mut sockaddr) {
            Some(res) => res,
            None => self.send_to_blocking(buf, sockaddr).await,
        }
    }

    /// Sends data on the socket to the remote address to which it is connected.
    ///
    /// [`UdpSocket::connect`] will connect this socket to a remote address. This method will fail if the socket is not connected.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::net::UdpSocket;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
    ///     sender.connect(receiver.local_addr().unwrap()).await.unwrap();
    ///     sender.send(&[1; 1]).await.unwrap();
    /// })
    /// ```
    ///
    /// `[UdpSocket::connect`]: UdpSocket::connect
    pub async fn send(&self, buf: &[u8]) -> io::Result<usize> {
        match self.yolo_tx(buf) {
            Some(r) => r,
            None => {
                let mut dma = self.allocate_buffer(buf.len());
                assert_eq!(dma.write_at(0, buf), buf.len());
                let source = self
                    .reactor
                    .upgrade()
                    .unwrap()
                    .rushed_send(self.socket.as_raw_fd(), dma)?;
                let ret = source.collect_rw().await?;
                self.tx_yolo.set(true);
                Ok(ret)
            }
        }
    }

    fn allocate_buffer(&self, size: usize) -> DmaBuffer {
        self.reactor.upgrade().unwrap().alloc_dma_buffer(size)
    }

    fn yolo_rx(&self, buf: &mut [u8]) -> Option<io::Result<usize>> {
        if self.rx_yolo.get() {
            super::yolo_recv(self.socket.as_raw_fd(), buf)
        } else {
            None
        }
        .or_else(|| {
            self.rx_yolo.set(false);
            None
        })
    }

    fn yolo_recvmsg(
        &self,
        buf: &mut [u8],
        flags: iou::MsgFlags,
    ) -> Option<io::Result<(usize, nix::sys::socket::SockAddr)>> {
        if self.rx_yolo.get() {
            super::yolo_recvmsg(self.socket.as_raw_fd(), buf, flags)
        } else {
            None
        }
        .or_else(|| {
            self.rx_yolo.set(false);
            None
        })
    }

    fn yolo_tx(&self, buf: &[u8]) -> Option<io::Result<usize>> {
        if self.tx_yolo.get() {
            super::yolo_send(self.socket.as_raw_fd(), buf)
        } else {
            None
        }
        .or_else(|| {
            self.tx_yolo.set(false);
            None
        })
    }

    fn yolo_sendmsg(
        &self,
        buf: &[u8],
        addr: &mut nix::sys::socket::SockAddr,
    ) -> Option<io::Result<usize>> {
        if self.tx_yolo.get() {
            super::yolo_sendmsg(self.socket.as_raw_fd(), buf, addr)
        } else {
            None
        }
        .or_else(|| {
            self.tx_yolo.set(false);
            None
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timer::Timer;
    use crate::LocalExecutorBuilder;
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

            let ex1 = LocalExecutorBuilder::new()
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

            let ex2 = LocalExecutorBuilder::new()
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

            let recv_handle = Local::local(async move {
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

            let receiver_handle = Task::local(async move {
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

            let receive_handle = Task::local(async move {
                let mut buf = [0u8; 1];
                for _ in 0..10 {
                    let (sz, _) = receiver
                        .recv_from_blocking(&mut buf, iou::MsgFlags::MSG_PEEK)
                        .await
                        .unwrap();
                    assert_eq!(sz, 1);
                }
                let (_, from) = receiver
                    .recv_from_blocking(&mut buf, iou::MsgFlags::MSG_PEEK)
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

            let receive_handle = Task::local(async move {
                let mut buf = [0u8; 1];
                let (sz, from) = receiver
                    .recv_from_blocking(&mut buf, iou::MsgFlags::empty())
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
            me.send_to_blocking(&[65u8; 1], sockaddr).await.unwrap();

            receiver.connect(me.local_addr().unwrap()).await.unwrap();
            let mut buf = [0u8; 1];
            let sz = receiver.recv(&mut buf).await.unwrap();
            assert_eq!(sz, 1);
            assert_eq!(buf[0], 65u8);
        });
    }
}
