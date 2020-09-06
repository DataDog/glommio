use std::io;
use std::marker::PhantomData;
use std::os::unix::io::RawFd;
use std::ptr::NonNull;

use crate::iou::iou::IoUring;
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
    pub fn register_buffers(&self, buffers: &[io::IoSlice<'_>]) -> io::Result<()> {
        let len = buffers.len();
        let addr = buffers.as_ptr() as *const _;
        let _: i32 = resultify!(unsafe {
            uring_sys::io_uring_register_buffers(self.ring.as_ptr(), addr, len as _)
        })?;
        Ok(())
    }

    pub fn unregister_buffers(&self) -> io::Result<()> {
        let _: i32 =
            resultify!(unsafe { uring_sys::io_uring_unregister_buffers(self.ring.as_ptr()) })?;
        Ok(())
    }

    pub fn register_files(&self, files: &[RawFd]) -> io::Result<()> {
        let len = files.len();
        let addr = files.as_ptr();
        let _: i32 = resultify!(unsafe {
            uring_sys::io_uring_register_files(self.ring.as_ptr(), addr, len as _)
        })?;
        Ok(())
    }

    pub fn unregister_files(&self) -> io::Result<()> {
        let _: i32 =
            resultify!(unsafe { uring_sys::io_uring_unregister_files(self.ring.as_ptr()) })?;
        Ok(())
    }

    pub fn register_eventfd(&self, eventfd: RawFd) -> io::Result<()> {
        let _: i32 = resultify!(unsafe {
            uring_sys::io_uring_register_eventfd(self.ring.as_ptr(), eventfd)
        })?;
        Ok(())
    }

    pub fn unregister_eventfd(&self) -> io::Result<()> {
        let _: i32 =
            resultify!(unsafe { uring_sys::io_uring_unregister_eventfd(self.ring.as_ptr()) })?;
        Ok(())
    }
}

unsafe impl<'ring> Send for Registrar<'ring> {}
unsafe impl<'ring> Sync for Registrar<'ring> {}
