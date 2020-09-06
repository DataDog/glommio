use std::ffi::CStr;
use std::io;
use std::marker::PhantomData;
use std::mem;
use std::os::unix::io::RawFd;
use std::ptr::{self, NonNull};
use std::time::Duration;

use crate::iou::iou::IoUring;
use crate::iou::iou::{PollFlags, SockAddr, SockFlag};
use crate::uring_sys::uring_sys;

macro_rules! resultify {
    ($ret:expr) => {{
        let ret = $ret;
        match ret >= 0 {
            true => Ok(ret as _),
            false => Err(std::io::Error::from_raw_os_error(-ret)),
        }
    }};
}

/// The queue of pending IO events.
///
/// Each element is a [`SubmissionQueueEvent`](crate::sqe::SubmissionQueueEvent).
/// By default, events are processed in parallel after being submitted.
/// You can modify this behavior for specific events using event [`SubmissionFlags`](crate::sqe::SubmissionFlags).
///
/// # Examples
/// Consider a read event that depends on a successful write beforehand.
///
/// We reify this relationship by using `IO_LINK` to link these events.
/// ```compile_fail
/// # use std::error::Error;
/// # use std::fs::File;
/// # use std::os::unix::io::{AsRawFd, RawFd};
/// # use iou::{IoUring, SubmissionFlags};
/// #
/// # fn main() -> Result<(), Box<dyn Error>> {
/// # let mut ring = IoUring::new(2)?;
/// # let mut sq = ring.sq();
/// #
/// let mut write_event = sq.next_sqe().unwrap();
///
/// // -- write event prep elided
///
/// // set IO_LINK to link the next event to this one
/// write_event.set_flags(SubmissionFlags::IO_LINK);
///
/// let mut read_event = sq.next_sqe().unwrap();
///
/// // -- read event prep elided
///
/// // read_event only occurs if write_event was successful
/// sq.submit()?;
/// # Ok(())
/// # }
/// ```
pub(crate) struct SubmissionQueue<'ring> {
    ring: NonNull<uring_sys::io_uring>,
    _marker: PhantomData<&'ring mut IoUring>,
}

impl<'ring> SubmissionQueue<'ring> {
    pub(crate) fn new(ring: &'ring IoUring) -> SubmissionQueue<'ring> {
        SubmissionQueue {
            ring: NonNull::from(&ring.ring),
            _marker: PhantomData,
        }
    }

    /// Returns new [`SubmissionQueueEvent`s](crate::sqe::SubmissionQueueEvent) until the queue size is reached. After that, will return `None`.
    /// ```compile_fail
    /// # use iou::IoUring;
    /// # use std::error::Error;
    /// # fn main() -> std::io::Result<()> {
    /// # let ring_size = 2;
    /// let mut ring = IoUring::new(ring_size)?;
    ///
    /// let mut counter = 0;
    ///
    /// while let Some(event) = ring.next_sqe() {
    ///     counter += 1;
    /// }
    ///
    /// assert_eq!(counter, ring_size);
    /// assert!(ring.next_sqe().is_none());
    /// # Ok(())
    /// # }
    ///
    pub(crate) fn next_sqe<'a>(&'a mut self) -> Option<SubmissionQueueEvent<'a>> {
        unsafe {
            let sqe = uring_sys::io_uring_get_sqe(self.ring.as_ptr());
            if sqe != ptr::null_mut() {
                let mut sqe = SubmissionQueueEvent::new(&mut *sqe);
                sqe.clear();
                Some(sqe)
            } else {
                None
            }
        }
    }

    /// Submit all events in the queue. Returns the number of submitted events.
    ///
    /// If this function encounters any IO errors an [`io::Error`](std::io::Result) variant is returned.
    pub(crate) fn submit(&mut self) -> io::Result<usize> {
        resultify!(unsafe { uring_sys::io_uring_submit(self.ring.as_ptr()) })
    }

    pub(crate) fn submit_and_wait(&mut self, wait_for: u32) -> io::Result<usize> {
        resultify!(unsafe {
            uring_sys::io_uring_submit_and_wait(self.ring.as_ptr(), wait_for as _)
        })
    }

    pub(crate) fn submit_and_wait_with_timeout(
        &mut self,
        wait_for: u32,
        duration: Duration,
    ) -> io::Result<usize> {
        let ts = uring_sys::__kernel_timespec {
            tv_sec: duration.as_secs() as _,
            tv_nsec: duration.subsec_nanos() as _,
        };

        loop {
            if let Some(mut sqe) = self.next_sqe() {
                sqe.clear();
                unsafe {
                    sqe.prep_timeout(&ts);
                    sqe.set_user_data(uring_sys::LIBURING_UDATA_TIMEOUT);
                    return resultify!(uring_sys::io_uring_submit_and_wait(
                        self.ring.as_ptr(),
                        wait_for as _
                    ));
                }
            }

            self.submit()?;
        }
    }
}

unsafe impl<'ring> Send for SubmissionQueue<'ring> {}
unsafe impl<'ring> Sync for SubmissionQueue<'ring> {}

/// A pending IO event.
///
/// Can be configured with a set of [`SubmissionFlags`](crate::sqe::SubmissionFlags).
///
pub(crate) struct SubmissionQueueEvent<'a> {
    sqe: &'a mut uring_sys::io_uring_sqe,
}

impl<'a> SubmissionQueueEvent<'a> {
    pub(crate) fn new(sqe: &'a mut uring_sys::io_uring_sqe) -> SubmissionQueueEvent<'a> {
        SubmissionQueueEvent { sqe }
    }

    /// Get this event's user data.
    pub(crate) fn user_data(&self) -> u64 {
        self.sqe.user_data as u64
    }

    /// Set this event's user data. User data is intended to be used by the application after completion.
    /// ```compile_fail
    /// # use iou::IoUring;
    /// # fn main() -> std::io::Result<()> {
    /// # let mut ring = IoUring::new(2)?;
    /// # let mut sq_event = ring.next_sqe().unwrap();
    /// #
    /// sq_event.set_user_data(0xB00);
    /// ring.submit_sqes()?;
    ///
    /// let cq_event = ring.wait_for_cqe()?;
    /// assert_eq!(cq_event.user_data(), 0xB00);
    /// # Ok(())
    /// # }
    /// ```
    pub(crate) fn set_user_data(&mut self, user_data: u64) {
        self.sqe.user_data = user_data as _;
    }

    /// Get this event's flags.
    pub(crate) fn flags(&self) -> SubmissionFlags {
        unsafe { SubmissionFlags::from_bits_unchecked(self.sqe.flags as _) }
    }

    /// Set this event's flags.
    pub(crate) fn set_flags(&mut self, flags: SubmissionFlags) {
        self.sqe.flags = flags.bits() as _;
    }

    #[inline]
    pub(crate) unsafe fn prep_statx(
        &mut self,
        dirfd: RawFd,
        path: &CStr,
        flags: StatxFlags,
        mask: StatxMode,
        buf: &mut libc::statx,
    ) {
        uring_sys::io_uring_prep_statx(
            self.sqe,
            dirfd,
            path.as_ptr() as _,
            flags.bits() as _,
            mask.bits() as _,
            buf as _,
        );
    }

    #[inline]
    pub(crate) unsafe fn prep_openat(
        &mut self,
        fd: RawFd,
        path: &CStr,
        flags: libc::c_int,
        mode: OpenMode,
    ) {
        uring_sys::io_uring_prep_openat(self.sqe, fd, path.as_ptr() as _, flags as _, mode.bits());
    }

    #[inline]
    pub(crate) unsafe fn prep_close(&mut self, fd: RawFd) {
        uring_sys::io_uring_prep_close(self.sqe, fd);
    }

    #[inline]
    pub(crate) unsafe fn prep_read_vectored(
        &mut self,
        fd: RawFd,
        bufs: &mut [io::IoSliceMut<'_>],
        offset: usize,
    ) {
        let len = bufs.len();
        let addr = bufs.as_mut_ptr();
        uring_sys::io_uring_prep_readv(self.sqe, fd, addr as _, len as _, offset as _);
    }

    #[inline]
    pub(crate) unsafe fn prep_read_fixed(
        &mut self,
        fd: RawFd,
        buf: &mut [u8],
        offset: u64,
        buf_index: usize,
    ) {
        let len = buf.len();
        let addr = buf.as_mut_ptr();
        uring_sys::io_uring_prep_read_fixed(
            self.sqe,
            fd,
            addr as _,
            len as _,
            offset as _,
            buf_index as _,
        );
    }

    #[inline]
    pub(crate) unsafe fn prep_read(&mut self, fd: RawFd, buf: &mut [u8], offset: u64) {
        let len = buf.len();
        let addr = buf.as_mut_ptr();
        uring_sys::io_uring_prep_read(self.sqe, fd, addr as _, len as _, offset as _);
    }

    #[inline]
    pub(crate) unsafe fn prep_write_vectored(
        &mut self,
        fd: RawFd,
        bufs: &[io::IoSlice<'_>],
        offset: usize,
    ) {
        let len = bufs.len();
        let addr = bufs.as_ptr();
        uring_sys::io_uring_prep_writev(self.sqe, fd, addr as _, len as _, offset as _);
    }

    #[inline]
    pub(crate) unsafe fn prep_write(&mut self, fd: RawFd, buf: &[u8], offset: u64) {
        let len = buf.len();
        let addr = buf.as_ptr();
        uring_sys::io_uring_prep_write(self.sqe, fd, addr as _, len as _, offset as _);
    }

    #[inline]
    pub(crate) unsafe fn prep_write_fixed(
        &mut self,
        fd: RawFd,
        buf: &[u8],
        offset: u64,
        buf_index: usize,
    ) {
        let len = buf.len();
        let addr = buf.as_ptr();
        uring_sys::io_uring_prep_write_fixed(
            self.sqe,
            fd,
            addr as _,
            len as _,
            offset as _,
            buf_index as _,
        );
    }

    #[inline]
    pub(crate) unsafe fn prep_fsync(&mut self, fd: RawFd, flags: FsyncFlags) {
        uring_sys::io_uring_prep_fsync(self.sqe, fd, flags.bits() as _);
    }

    #[inline]
    pub(crate) unsafe fn prep_fallocate(
        &mut self,
        fd: RawFd,
        offset: u64,
        size: u64,
        flags: FallocateFlags,
    ) {
        uring_sys::io_uring_prep_fallocate(self.sqe, fd, flags.bits() as _, offset as _, size as _);
    }

    /// Prepare a timeout event.
    /// ```compile_fail
    /// # use iou::IoUring;
    /// # fn main() -> std::io::Result<()> {
    /// # let mut ring = IoUring::new(1)?;
    /// # let mut sqe = ring.next_sqe().unwrap();
    /// #
    /// // make a one-second timeout
    /// let timeout_spec: _ = uring_sys::__kernel_timespec {
    ///     tv_sec:  1 as _,
    ///     tv_nsec: 0 as _,
    /// };
    ///
    /// unsafe { sqe.prep_timeout(&timeout_spec); }
    ///
    /// ring.submit_sqes()?;
    /// # Ok(())
    /// # }
    ///```
    #[inline]
    pub(crate) unsafe fn prep_timeout(&mut self, ts: &uring_sys::__kernel_timespec) {
        self.prep_timeout_with_flags(ts, 0, TimeoutFlags::empty());
    }

    #[inline]
    pub(crate) unsafe fn prep_timeout_with_flags(
        &mut self,
        ts: &uring_sys::__kernel_timespec,
        count: usize,
        flags: TimeoutFlags,
    ) {
        uring_sys::io_uring_prep_timeout(
            self.sqe,
            ts as *const _ as *mut _,
            count as _,
            flags.bits() as _,
        );
    }

    #[inline]
    pub(crate) unsafe fn prep_timeout_remove(&mut self, user_data: u64) {
        uring_sys::io_uring_prep_timeout_remove(self.sqe, user_data as _, 0);
    }

    #[inline]
    pub(crate) unsafe fn prep_cancel(&mut self, user_data: u64) {
        uring_sys::io_uring_prep_cancel(self.sqe, user_data as _, 0);
    }

    #[inline]
    pub(crate) unsafe fn prep_link_timeout(&mut self, ts: &uring_sys::__kernel_timespec) {
        uring_sys::io_uring_prep_link_timeout(self.sqe, ts as *const _ as *mut _, 0);
    }

    #[inline]
    pub(crate) unsafe fn prep_poll_add(&mut self, fd: RawFd, poll_flags: PollFlags) {
        uring_sys::io_uring_prep_poll_add(self.sqe, fd, poll_flags.bits())
    }

    #[inline]
    pub(crate) unsafe fn prep_poll_remove(&mut self, user_data: u64) {
        uring_sys::io_uring_prep_poll_remove(self.sqe, user_data as _)
    }

    #[inline]
    pub(crate) unsafe fn prep_connect(&mut self, fd: RawFd, socket_addr: &SockAddr) {
        let (addr, len) = socket_addr.as_ffi_pair();
        uring_sys::io_uring_prep_connect(self.sqe, fd, addr as *const _ as *mut _, len);
    }

    #[inline]
    pub(crate) unsafe fn prep_accept(
        &mut self,
        fd: RawFd,
        accept: Option<&mut SockAddrStorage>,
        flags: SockFlag,
    ) {
        let (addr, len) = match accept {
            Some(accept) => (
                accept.storage.as_mut_ptr() as *mut _,
                &mut accept.len as *mut _ as *mut _,
            ),
            None => (std::ptr::null_mut(), std::ptr::null_mut()),
        };
        uring_sys::io_uring_prep_accept(self.sqe, fd, addr, len, flags.bits())
    }

    /// Prepare a no-op event.
    /// ```compile_fail
    /// # use iou::{IoUring, SubmissionFlags};
    /// # fn main() -> std::io::Result<()> {
    /// # let mut ring = IoUring::new(1)?;
    /// #
    /// // example: use a no-op to force a drain
    ///
    /// let mut nop = ring.next_sqe().unwrap();
    ///
    /// nop.set_flags(SubmissionFlags::IO_DRAIN);
    /// unsafe { nop.prep_nop(); }
    ///
    /// ring.submit_sqes()?;
    /// # Ok(())
    /// # }
    ///```
    #[inline]
    pub(crate) unsafe fn prep_nop(&mut self) {
        uring_sys::io_uring_prep_nop(self.sqe);
    }

    /// Clear event. Clears user data, flags, and any event setup.
    /// ```compile_fail
    /// # use iou::{IoUring, SubmissionFlags};
    /// #
    /// # fn main() -> std::io::Result<()> {
    /// # let mut ring = IoUring::new(1)?;
    /// # let mut sqe = ring.next_sqe().unwrap();
    /// #
    /// sqe.set_user_data(0x1010);
    /// sqe.set_flags(SubmissionFlags::IO_DRAIN);
    ///
    /// sqe.clear();
    ///
    /// assert_eq!(sqe.user_data(), 0x0);
    /// assert_eq!(sqe.flags(), SubmissionFlags::empty());
    /// # Ok(())
    /// # }
    /// ```
    pub(crate) fn clear(&mut self) {
        *self.sqe = unsafe { mem::zeroed() };
    }

    /// Get a reference to the underlying [`uring_sys::io_uring_sqe`](uring_sys::io_uring_sqe) object.
    ///
    /// You can use this method to inspect the low-level details of an event.
    /// ```compile_fail
    /// # use iou::{IoUring};
    /// #
    /// # fn main() -> std::io::Result<()> {
    /// # let mut ring = IoUring::new(1)?;
    /// # let mut sqe = ring.next_sqe().unwrap();
    /// #
    /// unsafe { sqe.prep_nop(); }
    ///
    /// let sqe_ref = sqe.raw();
    ///
    /// assert_eq!(sqe_ref.len, 0);
    /// # Ok(())
    /// # }
    ///
    /// ```
    pub(crate) fn raw(&self) -> &uring_sys::io_uring_sqe {
        &self.sqe
    }

    pub(crate) fn raw_mut(&mut self) -> &mut uring_sys::io_uring_sqe {
        &mut self.sqe
    }
}

unsafe impl<'a> Send for SubmissionQueueEvent<'a> {}
unsafe impl<'a> Sync for SubmissionQueueEvent<'a> {}

pub(crate) struct SockAddrStorage {
    storage: mem::MaybeUninit<nix::sys::socket::sockaddr_storage>,
    len: usize,
}

impl SockAddrStorage {
    pub(crate) fn uninit() -> Self {
        let storage = mem::MaybeUninit::uninit();
        let len = mem::size_of::<nix::sys::socket::sockaddr_storage>();
        SockAddrStorage { storage, len }
    }

    pub(crate) unsafe fn as_socket_addr(&self) -> io::Result<SockAddr> {
        let storage = &*self.storage.as_ptr();
        nix::sys::socket::sockaddr_storage_to_addr(storage, self.len).map_err(|e| {
            let err_no = e.as_errno();
            match err_no {
                Some(err_no) => io::Error::from_raw_os_error(err_no as _),
                None => io::Error::new(io::ErrorKind::Other, "Unknown error"),
            }
        })
    }
}

bitflags::bitflags! {
    /// [`SubmissionQueueEvent`](SubmissionQueueEvent) configuration flags.
    ///
    /// Use a [`Registrar`](crate::registrar::Registrar) to register files for the `FIXED_FILE` flag.
    pub(crate) struct SubmissionFlags: u8 {
        /// This event's file descriptor is an index into the preregistered set of files.
        const FIXED_FILE    = 1 << 0;   /* use fixed fileset */
        /// Submit this event only after completing all ongoing submission events.
        const IO_DRAIN      = 1 << 1;   /* issue after inflight IO */
        /// Force the next submission event to wait until this event has completed sucessfully.
        ///
        /// An event's link only applies to the next event, but link chains can be
        /// arbitrarily long.
        const IO_LINK       = 1 << 2;   /* next IO depends on this one */
    }
}

bitflags::bitflags! {
    pub(crate) struct OpenMode: u32 {
        const S_IXOTH  = 1 << 0;
        const S_IWOTH  = 1 << 1;
        const S_IROTH  = 1 << 2;
        const S_IXGRP  = 1 << 3;
        const S_IWGRP  = 1 << 4;
        const S_IRGRP  = 1 << 5;
        const S_IXUSR  = 1 << 6;
        const S_IWUSR  = 1 << 7;
        const S_IRUSR  = 1 << 8;
        const S_IRWXO  = Self::S_IROTH.bits | Self::S_IWOTH.bits | Self::S_IXOTH.bits;
        const S_IRWXG  = Self::S_IRGRP.bits | Self::S_IWGRP.bits | Self::S_IXGRP.bits;
        const S_IRWXU  = Self::S_IRUSR.bits | Self::S_IWUSR.bits | Self::S_IXUSR.bits;
        const S_ISVTX  = 1 << 12;
        const S_ISGID  = 1 << 13;
        const S_ISUID  = 1 << 14;
    }
}

bitflags::bitflags! {
    pub(crate) struct FsyncFlags: u32 {
        /// Sync file data without an immediate metadata sync.
        const FSYNC_DATASYNC    = 1 << 0;
    }
}

bitflags::bitflags! {
    pub(crate) struct FallocateFlags: i32 {
        const FALLOC_FL_KEEP_SIZE      = 1 << 0;
        const FALLOC_FL_PUNCH_HOLE     = 1 << 1;
        const FALLOC_FL_NO_HIDE_STALE  = 1 << 2;
        const FALLOC_FL_COLLAPSE_RANGE = 1 << 3;
        const FALLOC_FL_ZERO_RANGE     = 1 << 4;
        const FALLOC_FL_INSERT_RANGE   = 1 << 5;
        const FALLOC_FL_UNSHARE_RANGE  = 1 << 6;
    }
}

bitflags::bitflags! {
    pub(crate) struct StatxFlags: i32 {
        const AT_STATX_SYNC_AS_STAT = 0;
        const AT_SYMLINK_NOFOLLOW   = 1 << 10;
        const AT_NO_AUTOMOUNT       = 1 << 11;
        const AT_EMPTY_PATH         = 1 << 12;
        const AT_STATX_FORCE_SYNC   = 1 << 13;
        const AT_STATX_DONT_SYNC    = 1 << 14;
    }
}

bitflags::bitflags! {
    pub(crate) struct StatxMode: i32 {
        const STATX_TYPE        = 1 << 0;
        const STATX_MODE        = 1 << 1;
        const STATX_NLINK       = 1 << 2;
        const STATX_UID         = 1 << 3;
        const STATX_GID         = 1 << 4;
        const STATX_ATIME       = 1 << 5;
        const STATX_MTIME       = 1 << 6;
        const STATX_CTIME       = 1 << 7;
        const STATX_INO         = 1 << 8;
        const STATX_SIZE        = 1 << 9;
        const STATX_BLOCKS      = 1 << 10;
        const STATX_BTIME       = 1 << 11;
    }
}

bitflags::bitflags! {
    pub(crate) struct TimeoutFlags: u32 {
        const TIMEOUT_ABS   = 1 << 0;
    }
}
