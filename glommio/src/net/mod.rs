// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! This module provides glommio's networking support.
use crate::sys;
use nix::sys::socket::{MsgFlags, SockaddrLike};
use std::{io, os::unix::io::RawFd};

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
    match sys::send_syscall(fd, buf.as_ptr(), buf.len(), MsgFlags::MSG_DONTWAIT.bits()) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

fn yolo_peek(fd: RawFd, buf: &mut [u8]) -> Option<io::Result<usize>> {
    match sys::recv_syscall(
        fd,
        buf.as_mut_ptr(),
        buf.len(),
        (MsgFlags::MSG_DONTWAIT | MsgFlags::MSG_PEEK).bits(),
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
        MsgFlags::MSG_DONTWAIT.bits(),
    ) {
        Ok(x) => Some(Ok(x)),
        Err(err) => match err.kind() {
            io::ErrorKind::WouldBlock => None,
            _ => Some(Err(err)),
        },
    }
}

fn yolo_recvmsg<T: SockaddrLike>(
    fd: RawFd,
    buf: &mut [u8],
    flags: MsgFlags,
) -> Option<io::Result<(usize, T)>> {
    match sys::recvmsg_syscall(
        fd,
        buf.as_mut_ptr(),
        buf.len(),
        (flags | MsgFlags::MSG_DONTWAIT).bits(),
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
    addr: &impl nix::sys::socket::SockaddrLike,
) -> Option<io::Result<usize>> {
    match sys::sendmsg_syscall(
        fd,
        buf.as_ptr(),
        buf.len(),
        addr,
        MsgFlags::MSG_DONTWAIT.bits(),
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
pub use self::{
    stream::{Buffered, Preallocated},
    tcp_socket::{AcceptedTcpStream, TcpListener, TcpStream},
    udp_socket::UdpSocket,
    unix::{AcceptedUnixStream, UnixDatagram, UnixListener, UnixStream},
};
