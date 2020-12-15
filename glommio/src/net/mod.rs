// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//! This module provide glommio's networking support.
use crate::sys;
use std::io;
use std::os::unix::io::RawFd;

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

mod tcp_socket;
pub use self::tcp_socket::{AcceptedTcpStream, TcpListener, TcpStream};
