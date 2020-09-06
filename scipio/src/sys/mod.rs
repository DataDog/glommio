// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use std::cell::{Cell, RefCell};
use std::convert::TryFrom;
use std::ffi::CString;
use std::mem::ManuallyDrop;
use std::net::{Shutdown, TcpStream};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::{FromRawFd, RawFd};
use std::path::Path;
use std::rc::Rc;
use std::task::Waker;
use std::time::Duration;
use std::{fmt, io};
use crate::uring_sys::uring_sys;

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
    let path = cstr(path)?;
    syscall!(unlink(path.as_ptr()))?;
    Ok(())
}

pub(crate) fn rename_file(old_path: &Path, new_path: &Path) -> io::Result<()> {
    let old = cstr(old_path)?;
    let new = cstr(new_path)?;

    syscall!(rename(old.as_ptr(), new.as_ptr()))?;
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
    let path = cstr(path)?;
    syscall!(open(path.as_ptr(), flags, mode))
}

fn cstr(path: &Path) -> io::Result<CString> {
    Ok(CString::new(path.as_os_str().as_bytes())?)
}

mod posix_buffers;
pub(crate) mod sysfs;
mod uring;

pub use self::posix_buffers::*;
pub use self::uring::*;
use crate::IoRequirements;

/// A buffer that can be used with DmaFile.
pub type DmaBuffer = PosixDmaBuffer;

#[derive(Debug)]
pub(crate) enum IOBuffer {
    Dma(PosixDmaBuffer),
    Buffered(Vec<u8>),
}

// You can be NonPollable and Buffered: that is the case for a Direct I/O file
// dispatched, say, on a RAID array (RAID do not currently support Poll, but it
// happily supports Direct I/O). So this is a 2 x 2 = 4 Matrix of possibilibies
// meaning we can't conflate Pollable and the buffer type.
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
    Write(PollableStatus, IOBuffer),
    Read(PollableStatus, Option<IOBuffer>),
    PollableFd,
    Open(CString),
    FdataSync,
    Fallocate,
    Close,
    LinkRings(LinkStatus),
    Statx(CString, Box<RefCell<libc::statx>>),
    Timeout(Option<u64>, TimeSpec64),
    Invalid,
}

impl TryFrom<SourceType> for libc::statx {
    type Error = io::Error;

    fn try_from(value: SourceType) -> Result<Self, Self::Error> {
        match value {
            SourceType::Statx(_, buf) => Ok(buf.into_inner()),
            _ => Err(io::Error::new(io::ErrorKind::Other, "Wrong source Type!")),
        }
    }
}

pub(crate) struct TimeSpec64 {
    raw: uring_sys::__kernel_timespec,
}

impl Default for TimeSpec64 {
    fn default() -> TimeSpec64 {
        TimeSpec64::from(Duration::default())
    }
}

impl fmt::Debug for TimeSpec64 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let duration = Duration::from(self);
        fmt::Debug::fmt(&duration, f)
    }
}

impl From<&'_ TimeSpec64> for Duration {
    fn from(ts: &TimeSpec64) -> Self {
        Duration::new(ts.raw.tv_sec as u64, ts.raw.tv_nsec as u32)
    }
}

impl From<Duration> for TimeSpec64 {
    fn from(dur: Duration) -> Self {
        TimeSpec64 {
            raw: uring_sys::__kernel_timespec {
                tv_sec: dur.as_secs() as i64,
                tv_nsec: dur.subsec_nanos() as libc::c_longlong,
            },
        }
    }
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
pub struct InnerSource {
    /// Raw file descriptor on Unix platforms.
    raw: RawFd,

    /// Tasks interested in events on this source.
    wakers: RefCell<Wakers>,

    source_type: RefCell<SourceType>,

    io_requirements: IoRequirements,

    enqueued: Cell<Option<EnqueuedSource>>,
}

pub struct EnqueuedSource {
    pub(crate) id: SourceId,
    pub(crate) queue: ReactorQueue,
}

impl fmt::Debug for InnerSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InnerSource")
            .field("raw", &self.raw)
            .field("wakers", &self.wakers)
            .field("source_type", &self.source_type)
            .field("io_requirements", &self.io_requirements)
            .finish()
    }
}

#[derive(Debug)]
pub struct Source {
    pub(crate) inner: Rc<InnerSource>,
}

impl Source {
    /// Registers an I/O source in the reactor.
    pub(crate) fn new(ioreq: IoRequirements, raw: RawFd, source_type: SourceType) -> Source {
        Source {
            inner: Rc::new(InnerSource {
                raw,
                wakers: RefCell::new(Wakers::new()),
                source_type: RefCell::new(source_type),
                io_requirements: ioreq,
                enqueued: Cell::new(None),
            }),
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
