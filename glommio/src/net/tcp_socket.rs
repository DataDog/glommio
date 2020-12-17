// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::io::ByteSliceMutExt;
use crate::parking::Reactor;
use crate::sys::{self, DmaBuffer, Source, SourceType};
use crate::Local;
use futures_lite::future::poll_fn;
use futures_lite::io::{AsyncBufRead, AsyncRead, AsyncWrite};
use futures_lite::ready;
use futures_lite::stream::{self, Stream};
use iou::{InetAddr, SockAddr};
use pin_project_lite::pin_project;
use socket2::{Domain, Protocol, Socket, Type};
use std::io;
use std::net::{self, Shutdown, SocketAddr, ToSocketAddrs};
use std::os::unix::io::RawFd;
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll};

const DEFAULT_BUFFER_SIZE: usize = 8192;

#[derive(Debug)]
/// A TCP socket server, listening for connections.
///
/// After creating a TcpListener by binding it to a socket address, it listens for incoming TCP connections.
/// These can be accepted by calling [`accept`] or [`shared_accept`], or by iterating over the Incoming iterator returned by [`incoming`].
///
/// A good networking architecture within a thread-per-core model needs to take into account
/// parallelism and spawn work into multiple executors. If everything happens inside the same
/// Executor, then at most one thread is used. Sometimes this is what you want: you may want to
/// dedicate a CPU entirely for networking, or even use specialized ports for each CPU of the
/// application, but most likely it isn't.
///
/// There are two approaches to load balancing possible with the `TcpListener`:
///
/// * By default, the ReusePort flag is set in the socket automatically. The OS already provides
///   some load balancing capabilities with that so you can simply [`bind`] to the same address
///   from many executors.
///
/// * If that is insufficient or otherwise not desirable, it is possible to use [`shared_accept`]
///   instead of [`accept`]: that returns an object that implements [`Send`]. You can then use a
///   [`shared_channel`] to send the accepted connection into multiple executors. The object
///   returned by [`shared_accept`] can then be bound to its executor with [`bind_to_executor`], at
///   which point it becomes a standard [`TcpStream`].
///
/// Relying on the OS is definitely simpler, but which approach is better depends on the specific
/// needs of your application.
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

impl TcpListener {
    /// Creates a TCP listener bound to the specified address.
    ///
    /// Binding with port number 0 will request an available port from the OS.
    ///
    /// This method sets the ReusePort option in the bound socket, so it is designed to
    /// be called from multiple executors to achieve parallelism.
    ///
    /// # Examples
    ///
    /// ```
    /// use glommio::net::TcpListener;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     println!("Listening on {}", listener.local_addr().unwrap());
    /// });
    /// ```
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<TcpListener> {
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
        let sk = Socket::new(domain, Type::stream(), Some(Protocol::tcp()))?;
        let addr = socket2::SockAddr::from(addr);
        sk.set_reuse_port(true)?;
        sk.bind(&addr)?;
        sk.listen(128)?;
        let listener = sk.into_tcp_listener();

        Ok(TcpListener {
            reactor: Rc::downgrade(&Local::get_reactor()),
            listener,
        })
    }

    /// Accepts a new incoming TCP connection and allows the result to be sent to a foreign
    /// executor
    ///
    /// This is similar to [`accept`], except it returns an [`AcceptedTcpStream`] instead of
    /// a [`TcpStream`]. [`AcceptedTcpStream`] implements [`Send`], so it can be safely sent
    /// for processing over a shared channel to a different executor.
    ///
    /// This is useful when the user wants to do her own load balancing across multiple executors
    /// instead of relying on the load balancing the OS would do with the ReusePort property of
    /// the bound socket.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::net::TcpListener;
    /// use glommio::LocalExecutor;
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
    pub async fn shared_accept(&self) -> io::Result<AcceptedTcpStream> {
        let reactor = self.reactor.upgrade().unwrap();
        let source = reactor.accept(self.listener.as_raw_fd());
        let fd = source.collect_rw().await?;
        Ok(AcceptedTcpStream { fd: fd as RawFd })
    }

    /// Accepts a new incoming TCP connection in this executor
    ///
    /// This is similar to calling [`shared_accept`] and [`bind_to_executor`] in a single
    /// operation.
    ///
    /// If this connection once accepted is to be handled by the same executor in which it
    /// was accepted, this version is preferred.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::net::TcpListener;
    /// use glommio::LocalExecutor;
    /// use futures_lite::stream::StreamExt;
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
    pub async fn accept(&self) -> io::Result<TcpStream> {
        let a = self.shared_accept().await?;
        Ok(a.bind_to_executor())
    }

    /// Creates a stream of incoming connections
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::net::TcpListener;
    /// use glommio::LocalExecutor;
    /// use futures_lite::stream::StreamExt;
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
    pub fn incoming(&self) -> impl Stream<Item = io::Result<TcpStream>> + Unpin + '_ {
        Box::pin(stream::unfold(self, |listener| async move {
            let res = listener.accept().await;
            Some((res, listener))
        }))
    }

    /// Returns the socket address of the local half of this TCP connection.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::net::TcpListener;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let listener = TcpListener::bind("127.0.0.1:8000").unwrap();
    ///     println!("Listening on {}", listener.local_addr().unwrap());
    /// });
    /// ```
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

#[derive(Copy, Clone, Debug)]
/// An Accepted Tcp connection that can be moved to a different executor
///
/// This is useful in situations where the load balancing provided by the Operating System
/// through ReusePort is not desirable. The user can accept the connection in one executor
/// through [`shared_accept`] which returns an AcceptedTcpStream.
///
/// Once the `AcceptedTcpStream` arrives at its destination it can then be made active with
/// [`bind_to_executor`]
///
/// [`shared_accept`]: TcpListener::shared_accept
/// [`bind_to_executor`]: AcceptedTcpStream::bind_to_executor
pub struct AcceptedTcpStream {
    fd: RawFd,
}

impl AcceptedTcpStream {
    /// Binds this `AcceptedTcpStream` to the current executor
    ///
    /// This returns a [`TcpStream`] that can then be used normally
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::net::TcpListener;
    /// use glommio::{LocalExecutorBuilder, LocalExecutor};
    /// use glommio::channels::shared_channel;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///
    ///    let (sender, receiver) = shared_channel::new_bounded(1);
    ///    let sender = sender.connect();
    ///
    ///    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    ///
    ///    let accepted = listener.shared_accept().await.unwrap();
    ///    sender.try_send(accepted).unwrap();
    ///
    ///   let ex1 = LocalExecutorBuilder::new().spawn(move || async move {
    ///       let receiver = receiver.connect();
    ///       let accepted = receiver.recv().await.unwrap();
    ///       let _ = accepted.bind_to_executor();
    ///   }).unwrap();
    ///
    ///   ex1.join().unwrap();
    /// });
    /// ```
    pub fn bind_to_executor(self) -> TcpStream {
        let reactor = Local::get_reactor();

        let socket = unsafe { Socket::from_raw_fd(self.fd as _) };
        let stream = socket.into_tcp_stream();
        let source_tx = None;
        let source_rx = None;
        TcpStream {
            reactor: Rc::downgrade(&reactor),
            stream,
            source_tx,
            source_rx,
            tx_yolo: true,
            rx_yolo: true,
            rx_buf: None,
            tx_buf: None,
            rx_buf_size: DEFAULT_BUFFER_SIZE,
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
    pub struct TcpStream {
        reactor: Weak<Reactor>,
        stream: net::TcpStream,
        source_tx: Option<Source>,
        source_rx: Option<Source>,

        // you only live once, you've got no time to block! if this is set to true try a direct non-blocking syscall otherwise schedule for sending later over the ring
        //
        // If you are familiar with high throughput networking code you might have seen similar
        // techniques with names such as "optimistic" "speculative" or things like that. But frankly "yolo" is such a
        // better name. Calling this "yolo" is likely glommio's biggest contribution to humankind.
        tx_yolo: bool,
        rx_yolo: bool,

        rx_buf: Option<DmaBuffer>,
        tx_buf: Option<DmaBuffer>,

        rx_buf_size: usize,
    }
}

use std::convert::TryFrom;

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

impl TcpStream {
    /// Creates a TCP connection to the specified address.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::net::TcpStream;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     TcpStream::connect("127.0.0.1:10000").await.unwrap();
    /// })
    /// ```
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<TcpStream> {
        let addr = addr.to_socket_addrs()?.next().unwrap();

        let reactor = Local::get_reactor();

        // Create a socket.
        let domain = if addr.is_ipv6() {
            Domain::ipv6()
        } else {
            Domain::ipv4()
        };
        let socket = Socket::new(domain, Type::stream(), Some(Protocol::tcp()))?;
        let inet = InetAddr::from_std(&addr);
        let addr = SockAddr::new_inet(inet);

        let source = reactor.connect(socket.as_raw_fd(), addr);
        source.collect_rw().await?;
        let stream = socket.into_tcp_stream();

        let source_tx = None;
        let source_rx = None;
        Ok(Self {
            reactor: Rc::downgrade(&reactor),
            stream,
            source_rx,
            source_tx,
            tx_yolo: true,
            rx_yolo: true,
            rx_buf: None,
            tx_buf: None,
            rx_buf_size: DEFAULT_BUFFER_SIZE,
        })
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
    // that doesn't even exist. So in preparation for that we'll sync-emulate this but
    // already on an async wrapper
    fn poll_shutdown(&self, _cx: &mut Context<'_>, how: Shutdown) -> Poll<io::Result<()>> {
        Poll::Ready(sys::shutdown(self.stream.as_raw_fd(), how))
    }

    /// Shuts down the read, write, or both halves of this connection.
    pub async fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        poll_fn(|cx| self.poll_shutdown(cx, how)).await
    }

    /// Sets the `TCP_NODELAY` option to this socket.
    ///
    /// Setting this to true disabled the Nagle algorithm.
    pub fn set_nodelay(&mut self, value: bool) -> io::Result<()> {
        self.stream.set_nodelay(value)
    }

    /// Sets the buffer size used on the receive path
    pub fn set_buffer_size(&mut self, buffer_size: usize) {
        self.rx_buf_size = buffer_size;
    }

    /// gets the buffer size used
    pub fn buffer_size(&mut self) -> usize {
        self.rx_buf_size
    }

    /// Receives data on the socket from the remote address to which it is connected, without removing that data from the queue.
    ///
    /// On success, returns the number of bytes peeked.
    /// Successive calls return the same data. This is accomplished by passing MSG_PEEK as a flag to the underlying recv system call.
    pub async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let source = self.reactor.upgrade().unwrap().recv(
            self.stream.as_raw_fd(),
            buf.len(),
            iou::MsgFlags::MSG_PEEK,
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

    /// Returns the socket address of the remote peer of this TCP connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::net::TcpStream;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     println!("My peer: {:?}", stream.peer_addr());
    /// })
    /// ```
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.peer_addr()
    }

    /// Returns the socket address of the local half of this TCP connection.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::net::TcpStream;
    /// use glommio::LocalExecutor;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let stream = TcpStream::connect("127.0.0.1:10000").await.unwrap();
    ///     println!("My peer: {:?}", stream.local_addr());
    /// })
    /// ```
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.stream.local_addr()
    }

    fn allocate_buffer(&self, size: usize) -> DmaBuffer {
        self.reactor.upgrade().unwrap().alloc_dma_buffer(size)
    }

    fn yolo_rx(&mut self, buf: &mut [u8]) -> Option<io::Result<usize>> {
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

    fn yolo_tx(&mut self, buf: &[u8]) -> Option<io::Result<usize>> {
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

    fn poll_replenish_buffer(
        &mut self,
        cx: &mut Context<'_>,
        size: usize,
    ) -> Poll<io::Result<usize>> {
        let source = match self.source_rx.take() {
            Some(source) => source,
            None => poll_err!(self
                .reactor
                .upgrade()
                .unwrap()
                .rushed_recv(self.stream.as_raw_fd(), size)),
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

    fn write_dma(&mut self, cx: &mut Context<'_>, buf: DmaBuffer) -> Poll<io::Result<usize>> {
        let source = match self.source_tx.take() {
            Some(source) => source,
            None => poll_err!(self
                .reactor
                .upgrade()
                .unwrap()
                .rushed_send(self.stream.as_raw_fd(), buf)),
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
}

impl AsyncBufRead for TcpStream {
    fn poll_fill_buf<'a>(
        mut self: Pin<&'a mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<&'a [u8]>> {
        let buf_size = self.rx_buf_size;
        if self.rx_buf.as_ref().is_none() {
            poll_err!(ready!(self.poll_replenish_buffer(cx, buf_size)));
        }
        let this = self.project();
        Poll::Ready(Ok(this.rx_buf.as_ref().unwrap().as_bytes()))
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        let buf_ref = self.rx_buf.as_mut().unwrap();
        let amt = std::cmp::min(amt, buf_ref.len());
        buf_ref.trim_front(amt);
        if buf_ref.is_empty() {
            self.rx_buf.take();
        }
    }
}

impl AsyncRead for TcpStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        poll_some!(self.consume_receive_buffer(buf));
        poll_some!(self.yolo_rx(buf));
        poll_err!(ready!(self.poll_replenish_buffer(cx, buf.len())));
        poll_some!(self.consume_receive_buffer(buf));
        unreachable!();
    }
}

impl AsyncWrite for TcpStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        poll_some!(self.yolo_tx(buf));
        let mut dma = self.allocate_buffer(buf.len());
        assert_eq!(dma.write_at(0, buf), buf.len());
        self.write_dma(cx, dma)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.source_tx.take();
        Poll::Ready(sys::shutdown(self.stream.as_raw_fd(), Shutdown::Write))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channels::shared_channel;
    use crate::enclose;
    use crate::timer::Timer;
    use crate::LocalExecutorBuilder;
    use futures_lite::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    use futures_lite::StreamExt;
    use std::cell::Cell;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn connect_local_server() {
        test_executor!(async move {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let addr = listener.local_addr().unwrap();
            let coord = Rc::new(Cell::new(0));

            let listener_handle: Task<io::Result<SocketAddr>> =
                Task::local(enclose! { (coord) async move {
                    coord.set(1);
                    let stream = listener.accept().await?;
                    Ok(stream.peer_addr()?)
                }});

            while coord.get() != 1 {
                Local::later().await;
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

            let ex1 = LocalExecutorBuilder::new()
                .spawn(move || async move {
                    let receiver = first_receiver.connect();
                    let _ = TcpListener::bind(addr).unwrap();
                    receiver.recv().await.unwrap();
                })
                .unwrap();

            let ex2 = LocalExecutorBuilder::new()
                .spawn(move || async move {
                    let receiver = second_receiver.connect();
                    let _ = TcpListener::bind(addr).unwrap();
                    receiver.recv().await.unwrap();
                })
                .unwrap();

            Timer::new(Duration::from_millis(100)).await;

            let sender = first_sender.connect();
            sender.try_send(0).unwrap();
            let sender = second_sender.connect();
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
        let ex1 = LocalExecutorBuilder::new()
            .spawn(move || async move {
                let sender = sender.connect();
                let addr_sender = addr_sender.connect();
                let listener = TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = listener.local_addr().unwrap();
                addr_sender.try_send(addr).unwrap();

                status.store(1, Ordering::Relaxed);
                let accepted = listener.shared_accept().await.unwrap();
                sender.try_send(accepted).unwrap();
            })
            .unwrap();

        let status = connected.clone();
        let ex2 = LocalExecutorBuilder::new()
            .spawn(move || async move {
                let receiver = receiver.connect();
                let accepted = receiver.recv().await.unwrap();
                let _ = accepted.bind_to_executor();
                status.store(2, Ordering::Relaxed);
            })
            .unwrap();

        let ex3 = LocalExecutorBuilder::new()
            .spawn(move || async move {
                let receiver = addr_receiver.connect();
                let addr = receiver.recv().await.unwrap();
                TcpStream::connect(addr).await.unwrap()
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

            let listener_handle: Task<io::Result<()>> =
                Task::local(enclose! { (coord) async move {
                    coord.set(1);
                    listener.incoming().take(4).try_for_each(|addr| {
                        addr.map(|_| ())
                    }).await
                }});

            while coord.get() != 1 {
                Local::later().await;
            }
            let mut handles = Vec::with_capacity(4);
            for _ in 0..4 {
                handles.push(Task::local(async move { TcpStream::connect(addr).await }).detach());
            }

            for handle in handles.drain(..) {
                handle.await.unwrap().unwrap();
            }
            listener_handle.await.unwrap();

            let res = TcpStream::connect(addr).await;
            // server is now dead, connection must fail
            assert_eq!(res.is_err(), true)
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
                    Local::local(enclose! { (listener) async move {
                        let _accept = listener.accept().await.unwrap();
                    }})
                    .detach(),
                );
            }
            // give it some time to make sure that all tasks above were sent down to
            // the ring
            Timer::new(Duration::from_millis(100)).await;

            // Now we should be able to establish 128 connections and all of that would accept
            for _ in 0..128 {
                handles.push(
                    Local::local(async move {
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

            let listener_handle = Task::<io::Result<u8>>::local(enclose! { (coord) async move {
                coord.set(1);
                let mut stream = listener.accept().await?;
                let mut byte = [0u8; 1];
                let read = stream.read(&mut byte).await?;
                assert_eq!(read, 1);
                Ok(byte[0])
            }})
            .detach();

            while coord.get() != 1 {
                Local::later().await;
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

            let listener_handle = Task::<io::Result<usize>>::local(async move {
                let mut stream = listener.accept().await?;
                let mut buf = Vec::new();
                stream.read_until(10, &mut buf).await?;
                Ok(buf.len())
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

            let listener_handle = Task::<io::Result<usize>>::local(async move {
                let mut stream = listener.accept().await?;
                let mut buf = String::new();
                stream.read_line(&mut buf).await?;
                Ok(buf.len())
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

            let listener_handle = Task::<io::Result<usize>>::local(async move {
                let stream = listener.accept().await?;
                Ok(stream.lines().count().await)
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

            let listener_handle = Task::<io::Result<()>>::local(async move {
                let mut stream = listener.accept().await?;
                let buf = stream.fill_buf().await?;
                // likely both messages were coalesced together
                assert_eq!(&buf[0..4], b"msg1");
                stream.consume(4);
                let buf = stream.fill_buf().await?;
                assert_eq!(buf, b"msg2");
                stream.consume(4);
                let buf = stream.fill_buf().await?;
                assert_eq!(buf.len(), 0);
                Ok(())
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

            let listener_handle = Task::<io::Result<()>>::local(async move {
                let mut stream = listener.accept().await?;
                let buf = stream.fill_buf().await?;
                assert_eq!(buf.len(), 4);
                stream.consume(100);
                let buf = stream.fill_buf().await?;
                assert_eq!(buf.len(), 0);
                Ok(())
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

            let listener_handle = Task::<io::Result<()>>::local(async move {
                let mut stream = listener.accept().await?;
                let buf = stream.fill_buf().await?;
                assert_eq!(buf, b"msg1");
                let buf = stream.fill_buf().await?;
                assert_eq!(buf, b"msg1");
                stream.consume(4);
                let buf = stream.fill_buf().await?;
                assert_eq!(buf.is_empty(), true);
                Ok(())
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

            let listener_handle = Task::<io::Result<usize>>::local(async move {
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
}
