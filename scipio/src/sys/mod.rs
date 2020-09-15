// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::Latency;
use std::cell::{RefCell, UnsafeCell};
use std::ffi::CString;
use std::io;
use std::mem::ManuallyDrop;
use std::net::{Shutdown, TcpStream};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{FromRawFd, RawFd};
use std::path::Path;
use std::rc::Rc;
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
    Timeout(Option<u64>),
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
pub struct InnerSource {
    /// Raw file descriptor on Unix platforms.
    pub(crate) raw: RawFd,

    /// Tasks interested in events on this source.
    pub(crate) wakers: RefCell<Wakers>,

    pub(crate) source_type: SourceType,

    io_requirements: IoRequirements,

    id: Option<u64>,

    queue: Option<ReactorQueue>,
}

#[derive(Debug)]
pub struct Source {
    pub(crate) inner: Rc<UnsafeCell<InnerSource>>,
}

impl Source {
    /// Registers an I/O source in the reactor.
    pub(crate) fn new(ioreq: IoRequirements, raw: RawFd, source_type: SourceType) -> Source {
        Source {
            inner: Rc::new(UnsafeCell::new(InnerSource {
                raw,
                wakers: RefCell::new(Wakers::new()),
                source_type,
                io_requirements: ioreq,
                id: None,
                queue: None,
            })),
        }
    }
}

impl InnerSource {
    pub(crate) fn update_source_type(&mut self, source_type: SourceType) -> SourceType {
        std::mem::replace(&mut self.source_type, source_type)
    }
}

pub(crate) fn mut_source(source: &Rc<UnsafeCell<InnerSource>>) -> &mut InnerSource {
    unsafe { &mut *source.get() }
}

impl Source {
    fn inner(&self) -> &mut InnerSource {
        mut_source(&self.inner)
    }

    pub(crate) fn update_reactor_info(&self, id: u64, queue: ReactorQueue) {
        self.inner().id = Some(id);
        self.inner().queue = Some(queue);
    }

    pub(crate) fn consume_id(&self) -> Option<u64> {
        self.inner().id.take()
    }

    pub(crate) fn latency_req(&self) -> Latency {
        self.inner().io_requirements.latency_req
    }

    pub(crate) fn source_type(&self) -> &SourceType {
        &self.inner().source_type
    }

    pub(crate) fn raw(&self) -> RawFd {
        self.inner().raw
    }

    pub(crate) fn wakers(&self) -> &RefCell<Wakers> {
        &self.inner().wakers
    }

    pub(crate) fn update_source_type(&mut self, source_type: SourceType) -> SourceType {
        self.inner().update_source_type(source_type)
    }

    pub(crate) fn extract_source_type(&mut self) -> SourceType {
        self.inner().update_source_type(SourceType::Invalid)
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        if let Some(id) = self.consume_id() {
            let queue = self.inner().queue.take();
            crate::sys::uring::cancel_source(id, queue.unwrap());
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
