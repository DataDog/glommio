use std::io;
use std::ops::*;
use std::os::unix::io::{AsRawFd, RawFd};

use crate::iou::sqe::SQE;
use crate::uring_sys;

pub const PLACEHOLDER_FD: RawFd = -1;

/// A member of the kernel's registered fileset.
///
/// Valid `RegisteredFd`s can be obtained through a [`Registrar`](crate::registrar::Registrar).
///
/// Registered files handle kernel fileset indexing behind the scenes and can often be used in place
/// of raw file descriptors. Not all IO operations support registered files.
///
/// Submission event prep methods on `RegisteredFd` will ensure that the submission event's
/// `SubmissionFlags::FIXED_FILE` flag is properly set.
pub type RegisteredFd = Registered<RawFd>;
pub type RegisteredBuf = Registered<Box<[u8]>>;
pub type RegisteredBufRef<'a> = Registered<&'a [u8]>;
pub type RegisteredBufMut<'a> = Registered<&'a mut [u8]>;

/// An object registered with an io-uring instance through a [`Registrar`](crate::Registrar).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Registered<T> {
    data: T,
    index: u32,
}

impl<T> Registered<T> {
    pub fn new(index: u32, data: T) -> Registered<T> {
        Registered { data, index }
    }

    pub fn index(&self) -> u32 {
        self.index
    }

    pub fn into_inner(self) -> T {
        self.data
    }
}

impl RegisteredFd {
    pub fn is_placeholder(&self) -> bool {
        self.data == PLACEHOLDER_FD
    }
}

impl AsRawFd for RegisteredFd {
    fn as_raw_fd(&self) -> RawFd {
        self.data
    }
}

impl RegisteredBuf {
    pub fn as_ref(&self) -> RegisteredBufRef<'_> {
        Registered::new(self.index, &self.data[..])
    }

    pub fn as_mut(&mut self) -> RegisteredBufMut<'_> {
        Registered::new(self.index, &mut self.data[..])
    }

    pub fn slice(&self, range: Range<usize>) -> RegisteredBufRef<'_> {
        Registered::new(self.index, &self.data[range])
    }

    pub fn slice_mut(&mut self, range: Range<usize>) -> RegisteredBufMut<'_> {
        Registered::new(self.index, &mut self.data[range])
    }

    pub fn slice_to(&self, index: usize) -> RegisteredBufRef<'_> {
        Registered::new(self.index, &self.data[..index])
    }

    pub fn slice_to_mut(&mut self, index: usize) -> RegisteredBufMut<'_> {
        Registered::new(self.index, &mut self.data[..index])
    }

    pub fn slice_from(&self, index: usize) -> RegisteredBufRef<'_> {
        Registered::new(self.index, &self.data[index..])
    }

    pub fn slice_from_mut(&mut self, index: usize) -> RegisteredBufMut<'_> {
        Registered::new(self.index, &mut self.data[index..])
    }
}

impl<'a> RegisteredBufRef<'a> {
    pub fn as_ref(&self) -> RegisteredBufRef<'_> {
        Registered::new(self.index, &self.data[..])
    }

    pub fn slice(self, range: Range<usize>) -> RegisteredBufRef<'a> {
        Registered::new(self.index, &self.data[range])
    }

    pub fn slice_to(&self, index: usize) -> RegisteredBufRef<'_> {
        Registered::new(self.index, &self.data[..index])
    }

    pub fn slice_from(&self, index: usize) -> RegisteredBufRef<'_> {
        Registered::new(self.index, &self.data[index..])
    }
}

impl<'a> RegisteredBufMut<'a> {
    pub fn as_ref(&self) -> RegisteredBufRef<'_> {
        Registered::new(self.index, &self.data[..])
    }

    pub fn as_mut(&mut self) -> RegisteredBufMut<'_> {
        Registered::new(self.index, &mut self.data[..])
    }

    pub fn slice(self, range: Range<usize>) -> RegisteredBufRef<'a> {
        Registered::new(self.index, &self.data[range])
    }

    pub fn slice_mut(self, range: Range<usize>) -> RegisteredBufMut<'a> {
        Registered::new(self.index, &mut self.data[range])
    }

    pub fn slice_to(&self, index: usize) -> RegisteredBufRef<'_> {
        Registered::new(self.index, &self.data[..index])
    }

    pub fn slice_to_mut(&mut self, index: usize) -> RegisteredBufMut<'_> {
        Registered::new(self.index, &mut self.data[..index])
    }

    pub fn slice_from(&self, index: usize) -> RegisteredBufRef<'_> {
        Registered::new(self.index, &self.data[index..])
    }

    pub fn slice_from_mut(&mut self, index: usize) -> RegisteredBufMut<'_> {
        Registered::new(self.index, &mut self.data[index..])
    }
}

impl Deref for RegisteredBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.data[..]
    }
}

impl Deref for RegisteredBufRef<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.data[..]
    }
}

impl Deref for RegisteredBufMut<'_> {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.data[..]
    }
}

impl DerefMut for RegisteredBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.data[..]
    }
}

impl DerefMut for RegisteredBufMut<'_> {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.data[..]
    }
}
/// A file descriptor that can be used to prepare SQEs.
///
/// The standard library's [`RawFd`] type implements this trait, but so does [`RegisteredFd`], a
/// type which is returned when a user pre-registers file descriptors with an io-uring instance.
pub trait UringFd {
    fn as_raw_fd(&self) -> RawFd;
    fn update_sqe(&self, sqe: &mut SQE<'_>);
}

impl UringFd for RawFd {
    fn as_raw_fd(&self) -> RawFd {
        *self
    }

    fn update_sqe(&self, _: &mut SQE<'_>) {}
}

impl UringFd for RegisteredFd {
    fn as_raw_fd(&self) -> RawFd {
        AsRawFd::as_raw_fd(self)
    }

    fn update_sqe(&self, sqe: &mut SQE<'_>) {
        unsafe {
            sqe.raw_mut().fd = self.index as RawFd;
        }
        sqe.set_fixed_file();
    }
}

/// A buffer that can be used to prepare read events.
pub trait UringReadBuf {
    unsafe fn prep_read(self, fd: impl UringFd, sqe: &mut SQE<'_>, offset: u64);
}

/// A buffer that can be used to prepare write events.
pub trait UringWriteBuf {
    unsafe fn prep_write(self, fd: impl UringFd, sqe: &mut SQE<'_>, offset: u64);
}

impl UringReadBuf for RegisteredBufMut<'_> {
    unsafe fn prep_read(self, fd: impl UringFd, sqe: &mut SQE<'_>, offset: u64) {
        uring_sys::io_uring_prep_read_fixed(
            sqe.raw_mut(),
            fd.as_raw_fd(),
            self.data.as_mut_ptr() as _,
            self.data.len() as _,
            offset as _,
            self.index() as _,
        );
        fd.update_sqe(sqe);
    }
}

impl UringReadBuf for &'_ mut [u8] {
    unsafe fn prep_read(self, fd: impl UringFd, sqe: &mut SQE<'_>, offset: u64) {
        uring_sys::io_uring_prep_read(
            sqe.raw_mut(),
            fd.as_raw_fd(),
            self.as_mut_ptr() as _,
            self.len() as _,
            offset as _,
        );
        fd.update_sqe(sqe);
    }
}

impl UringReadBuf for io::IoSliceMut<'_> {
    unsafe fn prep_read(mut self, fd: impl UringFd, sqe: &mut SQE<'_>, offset: u64) {
        uring_sys::io_uring_prep_read(
            sqe.raw_mut(),
            fd.as_raw_fd(),
            self.as_mut_ptr() as _,
            self.len() as _,
            offset as _,
        );
        fd.update_sqe(sqe);
    }
}

impl UringReadBuf for &'_ mut [&'_ mut [u8]] {
    unsafe fn prep_read(self, fd: impl UringFd, sqe: &mut SQE<'_>, offset: u64) {
        uring_sys::io_uring_prep_readv(
            sqe.raw_mut(),
            fd.as_raw_fd(),
            self.as_mut_ptr() as _,
            self.len() as _,
            offset as _,
        );
        fd.update_sqe(sqe);
    }
}

impl UringReadBuf for &'_ mut [io::IoSliceMut<'_>] {
    unsafe fn prep_read(self, fd: impl UringFd, sqe: &mut SQE<'_>, offset: u64) {
        uring_sys::io_uring_prep_readv(
            sqe.raw_mut(),
            fd.as_raw_fd(),
            self.as_mut_ptr() as _,
            self.len() as _,
            offset as _,
        );
        fd.update_sqe(sqe);
    }
}

impl UringWriteBuf for RegisteredBufRef<'_> {
    unsafe fn prep_write(self, fd: impl UringFd, sqe: &mut SQE<'_>, offset: u64) {
        uring_sys::io_uring_prep_write_fixed(
            sqe.raw_mut(),
            fd.as_raw_fd(),
            self.data.as_ptr() as _,
            self.data.len() as _,
            offset as _,
            self.index() as _,
        );
        fd.update_sqe(sqe);
    }
}

impl UringWriteBuf for &'_ [u8] {
    unsafe fn prep_write(self, fd: impl UringFd, sqe: &mut SQE<'_>, offset: u64) {
        uring_sys::io_uring_prep_write(
            sqe.raw_mut(),
            fd.as_raw_fd(),
            self.as_ptr() as _,
            self.len() as _,
            offset as _,
        );
        fd.update_sqe(sqe);
    }
}

impl UringWriteBuf for io::IoSlice<'_> {
    unsafe fn prep_write(self, fd: impl UringFd, sqe: &mut SQE<'_>, offset: u64) {
        uring_sys::io_uring_prep_write(
            sqe.raw_mut(),
            fd.as_raw_fd(),
            self.as_ptr() as _,
            self.len() as _,
            offset as _,
        );
        fd.update_sqe(sqe);
    }
}

impl UringWriteBuf for &'_ [io::IoSlice<'_>] {
    unsafe fn prep_write(self, fd: impl UringFd, sqe: &mut SQE<'_>, offset: u64) {
        uring_sys::io_uring_prep_writev(
            sqe.raw_mut(),
            fd.as_raw_fd(),
            self.as_ptr() as _,
            self.len() as _,
            offset as _,
        );
        fd.update_sqe(sqe);
    }
}

impl UringWriteBuf for &'_ [&'_ [u8]] {
    unsafe fn prep_write(self, fd: impl UringFd, sqe: &mut SQE<'_>, offset: u64) {
        uring_sys::io_uring_prep_writev(
            sqe.raw_mut(),
            fd.as_raw_fd(),
            self.as_ptr() as _,
            self.len() as _,
            offset as _,
        );
        fd.update_sqe(sqe);
    }
}
