use std::io;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, NonNull};

use super::{resultify, IoUring};
use crate::uring_sys;

/// A completed IO event.
#[derive(Debug)]
pub struct CQE {
    user_data: u64,
    res: i32,
    flags: CompletionFlags,
}

impl CQE {
    pub fn from_raw(cqe: uring_sys::io_uring_cqe) -> CQE {
        CQE {
            user_data: cqe.user_data,
            res: cqe.res,
            flags: CompletionFlags::from_bits_truncate(cqe.flags),
        }
    }

    pub fn from_raw_parts(user_data: u64, res: i32, flags: CompletionFlags) -> CQE {
        CQE {
            user_data,
            res,
            flags,
        }
    }

    pub(crate) fn new(
        ring: NonNull<uring_sys::io_uring>,
        cqe: &mut uring_sys::io_uring_cqe,
    ) -> CQE {
        let user_data = cqe.user_data;
        let res = cqe.res;
        let flags = CompletionFlags::from_bits_truncate(cqe.flags);

        unsafe {
            uring_sys::io_uring_cqe_seen(ring.as_ptr(), cqe);
        }

        CQE::from_raw_parts(user_data, res, flags)
    }

    pub fn user_data(&self) -> u64 {
        self.user_data as u64
    }

    pub fn result(&self) -> io::Result<u32> {
        resultify(self.res)
    }

    pub fn flags(&self) -> CompletionFlags {
        self.flags
    }

    pub fn raw_result(&self) -> i32 {
        self.res
    }

    pub fn raw_flags(&self) -> u32 {
        self.flags.bits()
    }
}

unsafe impl Send for CQE {}
unsafe impl Sync for CQE {}

/// An iterator of [`CQE`]s from the [`CompletionQueue`](crate::CompletionQueue).
///
/// This iterator will be exhausted when there are no `CQE`s ready, and return `None`.
pub struct CQEs<'a> {
    ring: NonNull<uring_sys::io_uring>,
    ready: u32,
    marker: PhantomData<&'a mut IoUring>,
}

impl<'a> CQEs<'a> {
    pub(crate) fn new(ring: NonNull<uring_sys::io_uring>) -> CQEs<'a> {
        CQEs {
            ring,
            ready: 0,
            marker: PhantomData,
        }
    }

    #[inline(always)]
    fn ready(&self) -> u32 {
        unsafe { uring_sys::io_uring_cq_ready(self.ring.as_ptr()) }
    }

    #[inline(always)]
    fn peek_for_cqe(&mut self) -> Option<CQE> {
        unsafe {
            let mut cqe = MaybeUninit::uninit();
            uring_sys::io_uring_peek_cqe(self.ring.as_ptr(), cqe.as_mut_ptr());
            let cqe = cqe.assume_init();
            if !cqe.is_null() {
                Some(CQE::new(self.ring, &mut *cqe))
            } else {
                None
            }
        }
    }
}

impl Iterator for CQEs<'_> {
    type Item = CQE;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ready == 0 {
            self.ready = self.ready();
            if self.ready == 0 {
                return None;
            }
        }

        self.ready -= 1;
        self.peek_for_cqe()
    }
}

/// An iterator of [`CQE`]s from the [`CompletionQueue`](crate::CompletionQueue).
///
/// This iterator will never be exhausted; if there are no `CQE`s ready, it will block until there
/// are.
pub struct CQEsBlocking<'a> {
    ring: NonNull<uring_sys::io_uring>,
    ready: u32,
    wait_for: u32,
    marker: PhantomData<&'a mut IoUring>,
}

impl<'a> CQEsBlocking<'a> {
    pub(crate) fn new(ring: NonNull<uring_sys::io_uring>, wait_for: u32) -> CQEsBlocking<'a> {
        CQEsBlocking {
            ring,
            ready: 0,
            wait_for,
            marker: PhantomData,
        }
    }

    #[inline(always)]
    fn ready(&self) -> u32 {
        unsafe { uring_sys::io_uring_cq_ready(self.ring.as_ptr()) }
    }

    #[inline(always)]
    fn peek_for_cqe(&mut self) -> Option<CQE> {
        unsafe {
            let mut cqe = MaybeUninit::uninit();
            uring_sys::io_uring_peek_cqe(self.ring.as_ptr(), cqe.as_mut_ptr());
            let cqe = cqe.assume_init();
            if !cqe.is_null() {
                Some(CQE::new(self.ring, &mut *cqe))
            } else {
                None
            }
        }
    }

    #[inline(always)]
    fn wait(&mut self) -> io::Result<&mut uring_sys::io_uring_cqe> {
        unsafe {
            let mut cqe = MaybeUninit::uninit();

            resultify(uring_sys::io_uring_wait_cqes(
                self.ring.as_ptr(),
                cqe.as_mut_ptr(),
                self.wait_for as _,
                ptr::null(),
                ptr::null(),
            ))?;

            Ok(&mut *cqe.assume_init())
        }
    }
}

impl Iterator for CQEsBlocking<'_> {
    type Item = io::Result<CQE>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ready == 0 {
            self.ready = self.ready();
            if self.ready == 0 {
                let ring = self.ring;
                return Some(self.wait().map(|cqe| CQE::new(ring, cqe)));
            }
        }

        self.ready -= 1;
        self.peek_for_cqe().map(Ok)
    }
}

bitflags::bitflags! {
    /// Flags that can be returned from the kernel on [`CQE`]s.
    pub struct CompletionFlags: u32 {
        const BUFFER_SHIFT    = 1 << 0;
    }
}
