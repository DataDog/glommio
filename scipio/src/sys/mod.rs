// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use std::cell::RefCell;
use std::ffi::CString;
use std::io;
use std::marker::PhantomPinned;
use std::mem::ManuallyDrop;
use std::net::{Shutdown, TcpStream};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{FromRawFd, RawFd};
use std::path::Path;
use std::pin::Pin;
use std::task::Waker;

macro_rules! syscall {
    ($fn:ident $args:tt) => {{
        let res = unsafe { libc::$fn $args };
        if res == -1 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(res)
        }
    }};
}

const FS_XFLAG_EXTSIZE: u32 = 0x00000800;

#[repr(C, packed)]
pub struct Fsxattr {
    fsx_xflags: u32,
    fsx_extsize: u32,
    fsx_nextents: u32,
    fsx_projid: u32,
    fsx_cowextsize: u32,
    fsx_pad: u64,
}

const FS_SETXATTR_MAGIC: u8 = b'X';
const FS_SETXATTR_TYPE_MODE: u8 = 32;
ioctl_write_ptr!(
    set_fsxattr,
    FS_SETXATTR_MAGIC,
    FS_SETXATTR_TYPE_MODE,
    Fsxattr
);

pub(crate) fn fs_hint_extentsize(fd: RawFd, size: usize) -> nix::Result<i32> {
    let attr = Fsxattr {
        fsx_xflags: FS_XFLAG_EXTSIZE,
        fsx_extsize: size as u32,
        fsx_nextents: 0,
        fsx_projid: 0,
        fsx_cowextsize: 0,
        fsx_pad: 0,
    };
    unsafe { set_fsxattr(fd, &attr) }
}

pub(crate) fn remove_file(path: &Path) -> io::Result<()> {
    let path = CString::new(path.as_os_str().as_bytes())?;
    syscall!(unlink(path.as_c_str().as_ptr()))?;
    Ok(())
}

pub(crate) fn rename_file(old_path: &Path, new_path: &Path) -> io::Result<()> {
    let old = CString::new(old_path.as_os_str().as_bytes())?;
    let new = CString::new(new_path.as_os_str().as_bytes())?;

    syscall!(rename(old.as_c_str().as_ptr(), new.as_c_str().as_ptr()))?;
    Ok(())
}

pub(crate) fn truncate_file(fd: RawFd, size: u64) -> io::Result<()> {
    syscall!(ftruncate(fd, size as i64))?;
    Ok(())
}

pub(crate) fn duplicate_file(fd: RawFd) -> io::Result<RawFd> {
    syscall!(dup(fd))
}

pub(crate) fn sync_open(path: &Path, flags: libc::c_int, mode: libc::c_int) -> io::Result<RawFd> {
    let path = path.as_os_str().as_bytes().as_ptr();
    syscall!(open(path as _, flags, mode))
}

mod posix_buffers;
mod uring;

pub use self::posix_buffers::*;
pub use self::uring::*;
use crate::IoRequirements;

/// A buffer that can be used with DmaFile.
pub type DmaBuffer = PosixDmaBuffer;

#[derive(Debug, Copy, Clone)]
pub(crate) enum PollableStatus {
    Pollable,
    NonPollable,
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum LinkStatus {
    Freestanding,
    Linked,
}

#[derive(Debug)]
pub(crate) enum SourceType {
    DmaWrite(PollableStatus),
    DmaRead(PollableStatus, Option<DmaBuffer>),
    PollableFd,
    Open(CString),
    FdataSync,
    Fallocate,
    Close,
    LinkRings(LinkStatus),
    Statx(CString, Box<RefCell<libc::statx>>),
    Timeout(bool),
    Invalid,
}

/// Tasks interested in events on a source.
#[derive(Debug)]
pub(crate) struct Wakers {
    /// Raw result of the operation.
    pub(crate) result: Option<io::Result<usize>>,

    /// Tasks waiting for the next event.
    pub(crate) waiters: Vec<Waker>,
}

impl Wakers {
    pub(crate) fn new() -> Self {
        Wakers {
            result: None,
            waiters: Vec::new(),
        }
    }
}

/// A registered source of I/O events.
#[derive(Debug)]
pub struct Source {
    /// Raw file descriptor on Unix platforms.
    pub(crate) raw: RawFd,

    /// Tasks interested in events on this source.
    pub(crate) wakers: RefCell<Wakers>,

    pub(crate) source_type: SourceType,

    io_requirements: IoRequirements,

    _pin: PhantomPinned,
}

impl Source {
    /// Registers an I/O source in the reactor.
    pub(crate) fn new(
        ioreq: IoRequirements,
        raw: RawFd,
        source_type: SourceType,
    ) -> Pin<Box<Source>> {
        let b = Box::new(Source {
            _pin: PhantomPinned,
            raw,
            wakers: RefCell::new(Wakers::new()),
            source_type,
            io_requirements: ioreq,
        });
        b.into()
    }

    pub(crate) fn as_ptr(self: Pin<&Self>) -> *const Self {
        self.get_ref() as *const Self
    }

    pub(crate) fn update_source_type(self: Pin<&mut Self>, source_type: SourceType) {
        unsafe {
            let source = self.get_unchecked_mut();
            source.source_type = source_type;
        }
    }

    pub(crate) fn extract_source_type(self: Pin<&mut Self>) -> SourceType {
        unsafe {
            let source = self.get_unchecked_mut();
            let invalid = SourceType::Invalid;
            std::mem::replace(&mut source.source_type, invalid)
        }
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        let w = self.wakers.get_mut();
        if !w.waiters.is_empty() {
            panic!("Attempting to release a source with pending waiters!");
            // This cancellation will be problematic, because
            // the operation may still be in-flight. If it returns
            // later it will point to garbage memory.
            //    crate::parking::Reactor::get().cancel_io(self);
        }
    }
}

/// Shuts down the write side of a socket.
///
/// If this source is not a socket, the `shutdown()` syscall error is ignored.
pub fn shutdown_write(raw: RawFd) -> io::Result<()> {
    // This may not be a TCP stream, but that's okay. All we do is call `shutdown()` on the raw
    // descriptor and ignore errors if it's not a socket.
    let res = unsafe {
        let stream = ManuallyDrop::new(TcpStream::from_raw_fd(raw));
        stream.shutdown(Shutdown::Write)
    };

    // The only actual error may be ENOTCONN, ignore everything else.
    match res {
        Err(err) if err.kind() == io::ErrorKind::NotConnected => Err(err),
        _ => Ok(()),
    }
}
