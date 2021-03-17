// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::{iou::sqe::SockAddrStorage, uring_sys};
use ahash::AHashMap;
use nix::sys::socket::SockAddr;
use std::{
    cell::{Cell, RefCell},
    convert::TryFrom,
    ffi::CString,
    fmt,
    io,
    mem::{ManuallyDrop, MaybeUninit},
    net::{Shutdown, TcpStream},
    os::unix::{
        ffi::OsStrExt,
        io::{AsRawFd, FromRawFd, RawFd},
    },
    path::Path,
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
        RwLock,
    },
    task::Waker,
    time::Duration,
};

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

pub(crate) fn create_eventfd() -> io::Result<RawFd> {
    syscall!(eventfd(0, libc::O_CLOEXEC))
}

pub(crate) fn write_eventfd(eventfd: RawFd) {
    let buf = [1u64; 1];
    let ret = syscall!(write(eventfd, &buf as *const u64 as _, 8)).unwrap();
    assert_eq!(ret, 8);
}

pub(crate) fn send_syscall(fd: RawFd, buf: *const u8, len: usize, flags: i32) -> io::Result<usize> {
    syscall!(send(fd, buf as _, len, flags)).map(|x| x as usize)
}

pub(crate) fn recv_syscall(fd: RawFd, buf: *mut u8, len: usize, flags: i32) -> io::Result<usize> {
    syscall!(recv(fd, buf as _, len, flags)).map(|x| x as usize)
}

pub(crate) fn accept_syscall(fd: RawFd) -> io::Result<RawFd> {
    let mut addr: MaybeUninit<libc::sockaddr_storage> = MaybeUninit::uninit();
    let mut length = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    syscall!(accept(fd, addr.as_mut_ptr() as *mut _, &mut length))
}

// This essentially converts the nix errors into something we can integrate with
// the rest of the crate.
pub(crate) unsafe fn ssptr_to_sockaddr(
    ss: MaybeUninit<nix::sys::socket::sockaddr_storage>,
    len: usize,
) -> io::Result<nix::sys::socket::SockAddr> {
    let storage = ss.assume_init();
    // unnamed unix sockets have a len of 0. Technically we should make sure this
    // has family = AF_UNIX, but if len == 0 the OS may not have written
    // anything here. If this is not supposed to be unix, the upper layers will
    // complain.
    if len == 0 {
        nix::sys::socket::SockAddr::new_unix("")
    } else {
        nix::sys::socket::sockaddr_storage_to_addr(&storage, len)
    }
    .map_err(|e| {
        let err_no = e.as_errno();
        match err_no {
            Some(err_no) => io::Error::from_raw_os_error(err_no as _),
            None => io::Error::new(io::ErrorKind::Other, "Unknown error"),
        }
    })
}

pub(crate) fn recvmsg_syscall(
    fd: RawFd,
    buf: *mut u8,
    len: usize,
    flags: i32,
) -> io::Result<(usize, nix::sys::socket::SockAddr)> {
    let mut iov = libc::iovec {
        iov_base: buf as *mut libc::c_void,
        iov_len: len,
    };

    let mut msg_name = MaybeUninit::<nix::sys::socket::sockaddr_storage>::uninit();
    let msg_namelen = std::mem::size_of::<nix::sys::socket::sockaddr_storage>() as libc::socklen_t;

    let mut hdr = libc::msghdr {
        msg_name: msg_name.as_mut_ptr() as *mut libc::c_void,
        msg_namelen,
        msg_iov: &mut iov as *mut libc::iovec,
        msg_iovlen: 1,
        msg_control: std::ptr::null_mut(),
        msg_controllen: 0,
        msg_flags: 0,
    };

    let x = syscall!(recvmsg(fd, &mut hdr, flags)).map(|x| x as usize)?;
    let addr = unsafe { ssptr_to_sockaddr(msg_name, hdr.msg_namelen as _)? };
    Ok((x, addr))
}

pub(crate) fn sendmsg_syscall(
    fd: RawFd,
    buf: *const u8,
    len: usize,
    addr: &mut nix::sys::socket::SockAddr,
    flags: i32,
) -> io::Result<usize> {
    let mut iov = libc::iovec {
        iov_base: buf as *mut libc::c_void,
        iov_len: len,
    };

    let (msg_name, msg_namelen) = addr.as_ffi_pair();
    let msg_name = msg_name as *const nix::sys::socket::sockaddr as *mut libc::c_void;

    let hdr = libc::msghdr {
        msg_name,
        msg_namelen,
        msg_iov: &mut iov as *mut libc::iovec,
        msg_iovlen: 1,
        msg_control: std::ptr::null_mut(),
        msg_controllen: 0,
        msg_flags: 0,
    };

    syscall!(sendmsg(fd, &hdr, flags)).map(|x| x as usize)
}

fn cstr(path: &Path) -> io::Result<CString> {
    Ok(CString::new(path.as_os_str().as_bytes())?)
}

mod dma_buffer;
pub(crate) mod sysfs;
mod uring;

pub use self::dma_buffer::DmaBuffer;
pub(crate) use self::uring::*;
use crate::{
    error::{ExecutorErrorKind, GlommioError, ReactorErrorKind},
    IoRequirements,
};

#[derive(Debug, Default)]
pub(crate) struct ReactorGlobalState {
    idgen: usize,
    // reactor_id -> notifier
    sleep_notifiers: AHashMap<usize, std::sync::Weak<SleepNotifier>>,
}

impl ReactorGlobalState {
    fn new_local_state(&mut self) -> io::Result<Arc<SleepNotifier>> {
        // note how this starts from 1. 0 means "no notifier present"
        self.idgen += 1;
        let id = self.idgen;
        if id == 0 || id == usize::MAX {
            return Err(GlommioError::<()>::ExecutorError(ExecutorErrorKind::InvalidId(id)).into());
        }

        let notifier = SleepNotifier::new(id)?;
        let res = self.sleep_notifiers.insert(id, Arc::downgrade(&notifier));
        assert_eq!(res.is_none(), true);
        Ok(notifier)
    }
}

#[derive(Debug)]
pub(crate) struct SleepNotifier {
    id: usize,
    eventfd: std::fs::File,
    memory: Arc<AtomicUsize>,
}

lazy_static! {
    static ref REACTOR_GLOBAL_STATE: RwLock<ReactorGlobalState> =
        RwLock::new(ReactorGlobalState::default());
    static ref REACTOR_DISCONNECTED: Arc<SleepNotifier> = SleepNotifier::new(usize::MAX).unwrap();
}

pub(super) fn new_sleep_notifier() -> io::Result<Arc<SleepNotifier>> {
    let mut state = REACTOR_GLOBAL_STATE.write().unwrap();
    state.new_local_state()
}

pub(crate) fn get_sleep_notifier_for(id: usize) -> Option<Arc<SleepNotifier>> {
    if id == usize::MAX {
        Some(REACTOR_DISCONNECTED.clone())
    } else {
        let state = REACTOR_GLOBAL_STATE.read().unwrap();
        state.sleep_notifiers.get(&id).and_then(|x| x.upgrade())
    }
}

impl SleepNotifier {
    pub(crate) fn new(id: usize) -> io::Result<Arc<Self>> {
        let eventfd = unsafe { std::fs::File::from_raw_fd(create_eventfd()?) };
        Ok(Arc::new(Self {
            eventfd,
            id,
            memory: Arc::new(AtomicUsize::new(0)),
        }))
    }

    pub(crate) fn eventfd_fd(&self) -> RawFd {
        self.eventfd.as_raw_fd()
    }

    pub(crate) fn id(&self) -> usize {
        self.id
    }

    pub(crate) fn must_notify(&self) -> Option<RawFd> {
        match self.memory.load(Ordering::Acquire) {
            0 => None,
            x => Some(x as _),
        }
    }

    pub(super) fn prepare_to_sleep(&self) {
        // This will allow this eventfd to be notified. This should not happen
        // for the placeholder (disconnected) case.
        assert_ne!(self.id, usize::MAX);
        self.memory
            .store(self.eventfd.as_raw_fd() as _, Ordering::SeqCst);
    }

    pub(super) fn wake_up(&self) {
        self.memory.store(0, Ordering::Release);
    }
}

impl Drop for SleepNotifier {
    fn drop(&mut self) {
        let mut state = REACTOR_GLOBAL_STATE.write().unwrap();
        // The other side may still be holding a reference in which case the notifier
        // will be freed later. However we can't receive notifications anymore
        // so memory must be zeroed here.
        self.wake_up();
        state.sleep_notifiers.remove(&self.id).unwrap();
    }
}

#[derive(Debug)]
pub(crate) enum IOBuffer {
    Dma(DmaBuffer),
    Buffered(Vec<u8>),
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum DirectIO {
    Enabled,
    Disabled,
}

// You can be NonPollable and Buffered: that is the case for a Direct I/O file
// dispatched, say, on a RAID array (RAID do not currently support Poll, but it
// happily supports Direct I/O). So this is a 2 x 2 = 4 Matrix of possibilibies
// meaning we can't conflate Pollable and the buffer type.
#[derive(Debug, Copy, Clone)]
pub(crate) enum PollableStatus {
    // The pollable ring only supports Direct I/O, so always true.
    Pollable,
    // Non pollable can go either way
    NonPollable(DirectIO),
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
    SockSend(DmaBuffer),
    SockRecv(Option<DmaBuffer>),
    SockRecvMsg(
        Option<DmaBuffer>,
        libc::iovec,
        libc::msghdr,
        MaybeUninit<nix::sys::socket::sockaddr_storage>,
    ),
    SockSendMsg(
        DmaBuffer,
        libc::iovec,
        libc::msghdr,
        nix::sys::socket::SockAddr,
    ),
    Open(CString),
    FdataSync,
    Fallocate,
    Close,
    LinkRings(LinkStatus),
    Statx(CString, Box<RefCell<libc::statx>>),
    Timeout(TimeSpec64),
    Connect(SockAddr),
    ConnectTimeout(SockAddr, TimeSpec64),
    Accept(SockAddrStorage),
    Invalid,
    #[cfg(feature = "bench")]
    Noop,
}

impl TryFrom<SourceType> for libc::statx {
    type Error = GlommioError<()>;

    fn try_from(value: SourceType) -> Result<Self, Self::Error> {
        match value {
            SourceType::Statx(_, buf) => Ok(buf.into_inner()),
            _ => Err(GlommioError::ReactorError(
                ReactorErrorKind::IncorrectSourceType,
            )),
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
    pub(crate) waiter: Option<Waker>,
}

impl Wakers {
    pub(crate) fn new() -> Self {
        Wakers {
            result: None,
            waiter: None,
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

/// Shuts down the requested side of a socket.
///
/// If this source is not a socket, the `shutdown()` syscall error is ignored.
pub(crate) fn shutdown(raw: RawFd, how: Shutdown) -> io::Result<()> {
    // This may not be a TCP stream, but that's okay. All we do is call `shutdown()`
    // on the raw descriptor and ignore errors if it's not a socket.
    let res = unsafe {
        let stream = ManuallyDrop::new(TcpStream::from_raw_fd(raw));
        stream.shutdown(how)
    };

    // The only actual error may be ENOTCONN, ignore everything else.
    match res {
        Err(err) if err.kind() == io::ErrorKind::NotConnected => Err(err),
        _ => Ok(()),
    }
}
