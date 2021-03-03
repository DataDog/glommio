//! Types related to registration and registered resources.
//!
//! The [`Registrar`] type can be used to register resources with the kernel that will be used with
//! a particular [`IoUring`] instance. This can improve performance by avoiding the kernel from
//! reallocating resources for each IO events performed against those resources.
//!
//! When file descriptors and buffers are registered with the kernel, an iterator of the type-safe
//! [`Registered`] wrapper is returned. This wrapper makes it easier to correctly use
//! pre-registered resources. By passing a [`RegisteredFd`] or the correct type of registered
//! buffer to an `SQE`s prep methods, the SQE will be properly prepared to use the
//! pre-registered object.
mod registered;

use std::fmt;
use std::io;
use std::marker::PhantomData;
use std::os::unix::io::RawFd;
use std::ptr::NonNull;

use super::{resultify, IoUring, Probe};
use crate::uring_sys;

pub use registered::*;

/// A `Registrar` creates ahead-of-time kernel references to files and user buffers.
///
/// Preregistration significantly reduces per-IO overhead, so consider registering frequently
/// used files and buffers. For file IO, preregistration lets the kernel skip the atomic acquire and
/// release of a kernel-specific file descriptor. For buffer IO, the kernel can avoid mapping kernel
/// memory for every operation.
///
/// Beware that registration is relatively expensive and should be done before any performance
/// sensitive code.
///
/// If you want to register a file but don't have an open file descriptor yet, you can register
/// a [placeholder](PLACEHOLDER_FD) descriptor and
/// [update](crate::registrar::Registrar::update_registered_files) it later.
pub struct Registrar<'ring> {
    ring: NonNull<uring_sys::io_uring>,
    _marker: PhantomData<&'ring mut IoUring>,
}

impl<'ring> Registrar<'ring> {
    pub(crate) fn new(ring: &'ring IoUring) -> Registrar<'ring> {
        Registrar {
            ring: NonNull::from(&ring.ring),
            _marker: PhantomData,
        }
    }

    pub fn register_buffers(
        &self,
        buffers: Vec<Box<[u8]>>,
    ) -> io::Result<impl Iterator<Item = RegisteredBuf>> {
        let len = buffers.len();
        let addr = buffers.as_ptr() as *const _;
        resultify(unsafe {
            uring_sys::io_uring_register_buffers(self.ring.as_ptr(), addr, len as _)
        })?;
        Ok(buffers
            .into_iter()
            .enumerate()
            .map(|(i, buf)| RegisteredBuf::new(i as u32, buf)))
    }

    pub fn register_buffers_by_ref<'a>(
        &self,
        buffers: &'a [&'a [u8]],
    ) -> io::Result<impl Iterator<Item = RegisteredBufRef<'a>> + 'a> {
        let len = buffers.len();
        let addr = buffers.as_ptr() as *const _;
        resultify(unsafe {
            uring_sys::io_uring_register_buffers(self.ring.as_ptr(), addr, len as _)
        })?;
        Ok(buffers
            .iter()
            .enumerate()
            .map(|(i, buf)| Registered::new(i as u32, &**buf)))
    }

    pub fn register_buffers_by_mut<'a>(
        &self,
        buffers: &'a mut [&'a mut [u8]],
    ) -> io::Result<impl Iterator<Item = RegisteredBufMut<'a>> + 'a> {
        let len = buffers.len();
        let addr = buffers.as_ptr() as *const _;
        resultify(unsafe {
            uring_sys::io_uring_register_buffers(self.ring.as_ptr(), addr, len as _)
        })?;
        Ok(buffers
            .iter_mut()
            .enumerate()
            .map(|(i, buf)| Registered::new(i as u32, &mut **buf)))
    }

    /// Unregister all currently registered buffers. An explicit call to this method is often unecessary,
    /// because all buffers will be unregistered automatically when the ring is dropped.
    pub fn unregister_buffers(&self) -> io::Result<()> {
        resultify(unsafe { uring_sys::io_uring_unregister_buffers(self.ring.as_ptr()) })?;
        Ok(())
    }

    /// Register a set of files with the kernel. Registered files handle kernel fileset indexing
    /// behind the scenes and can often be used in place of raw file descriptors.
    ///
    /// # Errors
    /// Returns an error if
    /// * there is a preexisting set of registered files,
    /// * the `files` slice was empty,
    /// * the inner [`io_uring_register_files`](uring_sys::io_uring_register_files) call failed for
    ///   another reason
    pub fn register_files<'a>(
        &self,
        files: &'a [RawFd],
    ) -> io::Result<impl Iterator<Item = RegisteredFd> + 'a> {
        assert!(files.len() <= u32::MAX as usize);
        resultify(unsafe {
            uring_sys::io_uring_register_files(
                self.ring.as_ptr(),
                files.as_ptr() as *const _,
                files.len() as _,
            )
        })?;
        Ok(files
            .iter()
            .enumerate()
            .map(|(i, &fd)| RegisteredFd::new(i as u32, fd)))
    }

    /// Update the currently registered kernel fileset. It is usually more efficient to reserve space
    /// for files before submitting events, because `IoUring` will wait until the submission queue is
    /// empty before registering files.
    /// # Errors
    /// Returns an error if
    /// * there isn't a registered fileset,
    /// * the `files` slice was empty,
    /// * `offset` is out of bounds,
    /// * the `files` slice was too large,
    /// * the inner [`io_uring_register_files_update`](uring_sys::io_uring_register_files_update) call
    ///   failed for another reason
    pub fn update_registered_files<'a>(
        &mut self,
        offset: usize,
        files: &'a [RawFd],
    ) -> io::Result<impl Iterator<Item = RegisteredFd> + 'a> {
        assert!(files.len() + offset <= u32::MAX as usize);
        resultify(unsafe {
            uring_sys::io_uring_register_files_update(
                self.ring.as_ptr(),
                offset as _,
                files.as_ptr() as *const _,
                files.len() as _,
            )
        })?;
        Ok(files
            .iter()
            .enumerate()
            .map(move |(i, &fd)| RegisteredFd::new((i + offset) as u32, fd)))
    }

    pub fn unregister_files(&self) -> io::Result<()> {
        resultify(unsafe { uring_sys::io_uring_unregister_files(self.ring.as_ptr()) })?;
        Ok(())
    }

    pub fn register_eventfd(&self, eventfd: RawFd) -> io::Result<()> {
        resultify(unsafe { uring_sys::io_uring_register_eventfd(self.ring.as_ptr(), eventfd) })?;
        Ok(())
    }

    pub fn register_eventfd_async(&self, eventfd: RawFd) -> io::Result<()> {
        resultify(unsafe {
            uring_sys::io_uring_register_eventfd_async(self.ring.as_ptr(), eventfd)
        })?;
        Ok(())
    }

    pub fn unregister_eventfd(&self) -> io::Result<()> {
        resultify(unsafe { uring_sys::io_uring_unregister_eventfd(self.ring.as_ptr()) })?;
        Ok(())
    }

    pub fn register_personality(&self) -> io::Result<Personality> {
        let id =
            resultify(unsafe { uring_sys::io_uring_register_personality(self.ring.as_ptr()) })?;
        debug_assert!(id < u16::MAX as u32);
        Ok(Personality { id: id as u16 })
    }

    pub fn unregister_personality(&self, personality: Personality) -> io::Result<()> {
        resultify(unsafe {
            uring_sys::io_uring_unregister_personality(self.ring.as_ptr(), personality.id as _)
        })?;
        Ok(())
    }

    pub fn probe(&self) -> io::Result<Probe> {
        Probe::for_ring(self.ring.as_ptr())
    }
}

impl fmt::Debug for Registrar<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fd = unsafe { self.ring.as_ref().ring_fd };
        f.debug_struct(std::any::type_name::<Self>())
            .field("fd", &fd)
            .finish()
    }
}

unsafe impl<'ring> Send for Registrar<'ring> {}
unsafe impl<'ring> Sync for Registrar<'ring> {}

#[derive(Debug, Eq, PartialEq, Hash, Ord, PartialOrd, Clone, Copy)]
pub struct Personality {
    pub(crate) id: u16,
}

impl From<u16> for Personality {
    fn from(id: u16) -> Personality {
        Personality { id }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::os::unix::io::AsRawFd;

    #[test]
    #[should_panic(expected = "Invalid argument")]
    fn register_empty_slice() {
        let ring = IoUring::new(1).unwrap();
        let _ = ring.registrar().register_files(&[]).unwrap();
    }

    #[test]
    #[should_panic(expected = "Bad file descriptor")]
    fn register_bad_fd() {
        let ring = IoUring::new(1).unwrap();
        let _ = ring.registrar().register_files(&[-100]).unwrap();
    }

    #[test]
    #[should_panic(expected = "Device or resource busy")]
    fn double_register() {
        let ring = IoUring::new(1).unwrap();
        let _ = ring.registrar().register_files(&[1]).unwrap();
        let _ = ring.registrar().register_files(&[1]).unwrap();
    }

    #[test]
    #[should_panic(expected = "No such device or address")]
    fn empty_unregister_err() {
        let ring = IoUring::new(1).unwrap();
        let _ = ring.registrar().unregister_files().unwrap();
    }

    #[test]
    #[should_panic(expected = "No such device or address")]
    fn empty_update_err() {
        let ring = IoUring::new(1).unwrap();
        let _ = ring.registrar().update_registered_files(0, &[1]).unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid argument")]
    fn offset_out_of_bounds_update() {
        let raw_fds = [1, 2];
        let ring = IoUring::new(1).unwrap();
        let _ = ring.registrar().register_files(&raw_fds).unwrap();
        let _ = ring
            .registrar()
            .update_registered_files(2, &raw_fds)
            .unwrap();
    }

    #[test]
    #[should_panic(expected = "Invalid argument")]
    fn slice_len_out_of_bounds_update() {
        let ring = IoUring::new(1).unwrap();
        let _ = ring.registrar().register_files(&[1, 1]).unwrap();
        let _ = ring
            .registrar()
            .update_registered_files(0, &[1, 1, 1])
            .unwrap();
    }

    #[test]
    fn valid_fd_update() {
        let ring = IoUring::new(1).unwrap();

        let file = std::fs::File::create("tmp.txt").unwrap();
        let _ = ring
            .registrar()
            .register_files(&[file.as_raw_fd()])
            .unwrap();

        let new_file = std::fs::File::create("new_tmp.txt").unwrap();
        let _ = ring
            .registrar()
            .update_registered_files(0, &[new_file.as_raw_fd()])
            .unwrap();

        let _ = std::fs::remove_file("tmp.txt");
        let _ = std::fs::remove_file("new_tmp.txt");
    }

    #[test]
    fn placeholder_update() {
        let ring = IoUring::new(1).unwrap();
        let _ = ring.registrar().register_files(&[-1, -1, -1]).unwrap();

        let file = std::fs::File::create("tmp.txt").unwrap();
        let _ = ring
            .registrar()
            .update_registered_files(0, &[file.as_raw_fd()])
            .unwrap();
        let _ = std::fs::remove_file("tmp.txt");
    }
}
