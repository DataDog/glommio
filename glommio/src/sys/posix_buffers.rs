// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
// Buffers that are friendly to be used with O_DIRECT files.
// For the time being they are really only properly aligned,
// but in the near future they can be coming from memory-areas
// that are pre-registered for I/O uring.

use std::ptr;

use crate::sys::uring::UringBuffer;
use aligned_alloc::{aligned_alloc, aligned_free};

#[derive(Debug)]
pub(crate) struct SysAlloc {
    data: ptr::NonNull<u8>,
}

impl SysAlloc {
    fn new(size: usize) -> Option<Self> {
        let data = aligned_alloc(size, 4 << 10) as *mut u8;
        let data = ptr::NonNull::new(data)?;
        Some(SysAlloc { data })
    }

    fn as_ptr(&self) -> *const u8 {
        self.data.as_ptr()
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.data.as_ptr()
    }
}

impl Drop for SysAlloc {
    fn drop(&mut self) {
        unsafe {
            aligned_free(self.data.as_ptr() as *mut ());
        }
    }
}

#[derive(Debug)]
pub(crate) enum BufferStorage {
    Sys(SysAlloc),
    Uring(UringBuffer),
}

impl BufferStorage {
    fn as_ptr(&self) -> *const u8 {
        match self {
            BufferStorage::Sys(x) => x.as_ptr(),
            BufferStorage::Uring(x) => x.as_ptr(),
        }
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        match self {
            BufferStorage::Sys(x) => x.as_mut_ptr(),
            BufferStorage::Uring(x) => x.as_mut_ptr(),
        }
    }
}

#[derive(Debug)]
pub struct PosixDmaBuffer {
    storage: BufferStorage,
    // Invariant: trim + size are at most one byte past the original allocation.
    trim: usize,
    size: usize,
}

impl PosixDmaBuffer {
    pub(crate) fn new(size: usize) -> Option<PosixDmaBuffer> {
        Some(PosixDmaBuffer {
            storage: BufferStorage::Sys(SysAlloc::new(size)?),
            size,
            trim: 0,
        })
    }

    pub(crate) fn with_storage(size: usize, storage: BufferStorage) -> PosixDmaBuffer {
        PosixDmaBuffer {
            storage,
            size,
            trim: 0,
        }
    }

    pub(crate) fn uring_buffer_id(&self) -> Option<usize> {
        match &self.storage {
            BufferStorage::Uring(x) => x.uring_buffer_id(),
            _ => None,
        }
    }

    pub(crate) fn trim_to_size(&mut self, newsize: usize) {
        assert!(newsize <= self.size);
        self.size = newsize;
    }

    pub(crate) fn trim_front(&mut self, trim: usize) {
        assert!(trim <= self.size);
        self.trim += trim;
        self.size -= trim;
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.size) }
    }

    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.size) }
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        unsafe { self.storage.as_mut_ptr().add(self.trim) }
    }

    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.storage.as_ptr().add(self.trim) }
    }

    pub fn read_at(&self, offset: usize, dst: &mut [u8]) -> usize {
        if offset > self.size {
            return 0;
        }
        let len = std::cmp::min(dst.len(), self.size - offset);
        let me = &self.as_bytes()[offset..offset + len];
        dst[0..len].copy_from_slice(me);
        len
    }

    pub fn write_at(&mut self, offset: usize, src: &[u8]) -> usize {
        if offset > self.size {
            return 0;
        }
        let len = std::cmp::min(src.len(), self.size - offset);
        let me = &mut self.as_bytes_mut()[offset..offset + len];
        me.copy_from_slice(&src[0..len]);
        len
    }

    pub fn memset(&mut self, value: u8) {
        unsafe { std::ptr::write_bytes(self.as_mut_ptr(), value, self.size) }
    }
}
