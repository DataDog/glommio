// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! This module provide glommio's networking support.
use crate::sys;
use std::io;
use std::os::unix::io::RawFd;

fn yolo_accept(fd: RawFd) -> Option<io::Result<RawFd>> {
    let flags =
        nix::fcntl::OFlag::from_bits(nix::fcntl::fcntl(fd, nix::fcntl::F_GETFL).unwrap()).unwrap();
    nix::fcntl::fcntl(
        fd,
        nix::fcntl::F_SETFL(flags | nix::fcntl::OFlag::O_NONBLOCK),
    )
    .unwrap();
    let r = sys::accept_syscall(fd);
    nix::fcntl::fcntl(fd, nix::fcntl::F_SETFL(flags)).unwrap();
    match r {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

fn yolo_send(fd: RawFd, buf: &[u8]) -> Option<io::Result<usize>> {
    match sys::send_syscall(
        fd,
        buf.as_ptr(),
        buf.len(),
        iou::MsgFlags::MSG_DONTWAIT.bits(),
    ) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

fn yolo_recv(fd: RawFd, buf: &mut [u8]) -> Option<io::Result<usize>> {
    match sys::recv_syscall(
        fd,
        buf.as_mut_ptr(),
        buf.len(),
        iou::MsgFlags::MSG_DONTWAIT.bits(),
    ) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

fn yolo_recvmsg(
    fd: RawFd,
    buf: &mut [u8],
    flags: iou::MsgFlags,
) -> Option<io::Result<(usize, nix::sys::socket::SockAddr)>> {
    match sys::recvmsg_syscall(
        fd,
        buf.as_mut_ptr(),
        buf.len(),
        (flags | iou::MsgFlags::MSG_DONTWAIT).bits(),
    ) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

fn yolo_sendmsg(
    fd: RawFd,
    buf: &[u8],
    addr: &mut nix::sys::socket::SockAddr,
) -> Option<io::Result<usize>> {
    match sys::sendmsg_syscall(
        fd,
        buf.as_ptr(),
        buf.len(),
        addr,
        iou::MsgFlags::MSG_DONTWAIT.bits(),
    ) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

mod datagram;
mod stream;
mod tcp_socket;
mod udp_socket;
mod unix;
pub use self::tcp_socket::{AcceptedTcpStream, TcpListener, TcpStream};
pub use self::udp_socket::UdpSocket;
pub use self::unix::{AcceptedUnixStream, UnixDatagram, UnixListener, UnixStream};
