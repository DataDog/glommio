// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::{
    sys::{self, DmaBuffer, Source, SourceType},
    ByteSliceMutExt,
    Local,
    Reactor,
};
use nix::sys::socket::MsgFlags;
use std::{
    cell::Cell,
    io,
    os::unix::io::{AsRawFd, FromRawFd, RawFd},
    rc::{Rc, Weak},
};

const DEFAULT_BUFFER_SIZE: usize = 8192;

#[derive(Debug)]
pub struct GlommioDatagram<S: AsRawFd + FromRawFd + From<socket2::Socket>> {
    pub(crate) reactor: Weak<Reactor>,
    pub(crate) socket: S,

    // you only live once, you've got no time to block! if this is set to true try a direct
    // non-blocking syscall otherwise schedule for sending later over the ring
    //
    // If you are familiar with high throughput networking code you might have seen similar
    // techniques with names such as "optimistic" "speculative" or things like that. But frankly
    // "yolo" is such a better name. Calling this "yolo" is likely glommio's biggest
    // contribution to humankind.
    pub(crate) tx_yolo: Cell<bool>,
    pub(crate) rx_yolo: Cell<bool>,

    pub(crate) rx_buf_size: usize,
}

impl<S: AsRawFd + FromRawFd + From<socket2::Socket>> From<socket2::Socket> for GlommioDatagram<S> {
    fn from(socket: socket2::Socket) -> GlommioDatagram<S> {
        let socket = socket.into();
        GlommioDatagram {
            reactor: Rc::downgrade(&Local::get_reactor()),
            socket,
            tx_yolo: Cell::new(true),
            rx_yolo: Cell::new(true),
            rx_buf_size: DEFAULT_BUFFER_SIZE,
        }
    }
}

impl<S: AsRawFd + FromRawFd + From<socket2::Socket>> AsRawFd for GlommioDatagram<S> {
    fn as_raw_fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }
}

impl<S: FromRawFd + AsRawFd + From<socket2::Socket>> FromRawFd for GlommioDatagram<S> {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        let socket = socket2::Socket::from_raw_fd(fd);
        GlommioDatagram::from(socket)
    }
}

impl<S: AsRawFd + FromRawFd + From<socket2::Socket>> GlommioDatagram<S> {
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

    pub(crate) async fn peek(&self, buf: &mut [u8]) -> io::Result<usize> {
        let source = self.reactor.upgrade().unwrap().recv(
            self.socket.as_raw_fd(),
            buf.len(),
            MsgFlags::MSG_PEEK,
        );

        self.consume_receive_buffer(&source, buf).await
    }

    pub(crate) async fn peek_from(
        &self,
        buf: &mut [u8],
    ) -> io::Result<(usize, nix::sys::socket::SockAddr)> {
        match self.yolo_recvmsg(buf, MsgFlags::MSG_PEEK) {
            Some(res) => res,
            None => self.recv_from_blocking(buf, MsgFlags::MSG_PEEK).await,
        }
    }

    pub(crate) async fn recv(&self, buf: &mut [u8]) -> io::Result<usize> {
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

    pub(crate) async fn recv_from_blocking(
        &self,
        buf: &mut [u8],
        flags: MsgFlags,
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

    pub(crate) async fn recv_from(
        &self,
        buf: &mut [u8],
    ) -> io::Result<(usize, nix::sys::socket::SockAddr)> {
        match self.yolo_recvmsg(buf, MsgFlags::empty()) {
            Some(res) => res,
            None => self.recv_from_blocking(buf, MsgFlags::empty()).await,
        }
    }

    pub(crate) async fn send_to_blocking(
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

    pub(crate) async fn send_to(
        &self,
        buf: &[u8],
        mut addr: nix::sys::socket::SockAddr,
    ) -> io::Result<usize> {
        match self.yolo_sendmsg(buf, &mut addr) {
            Some(res) => res,
            None => self.send_to_blocking(buf, addr).await,
        }
    }

    pub(crate) async fn send(&self, buf: &[u8]) -> io::Result<usize> {
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
        flags: MsgFlags,
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
