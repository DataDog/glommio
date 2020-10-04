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

use aligned_alloc::{aligned_alloc, aligned_free};

#[derive(Debug)]
pub struct PosixDmaBuffer {
    data: ptr::NonNull<u8>,
    // Invariant: trim + size are at most one byte past the original allocation.
    trim: usize,
    size: usize,
}

impl PosixDmaBuffer {
    pub(crate) fn new(size: usize) -> Option<PosixDmaBuffer> {
        let data = aligned_alloc(size, 4 << 10) as *mut u8;
        let data = ptr::NonNull::new(data)?;
        Some(PosixDmaBuffer {
            data,
            size,
            trim: 0,
        })
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
        unsafe { self.data.as_ptr().add(self.trim) }
    }

    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.data.as_ptr().add(self.trim) }
    }

    pub fn copy_to_slice(&self, offset: usize, dst: &mut [u8]) -> usize {
        if offset > self.size {
            return 0;
        }
        let len = std::cmp::min(dst.len(), self.size - offset);

        let dst_ptr = dst.as_mut_ptr();
        unsafe { std::ptr::copy_nonoverlapping(self.as_ptr().add(offset), dst_ptr, len) }
        len
    }

    pub fn copy_from_slice(&mut self, offset: usize, src: &[u8]) -> usize {
        if offset > self.size {
            return 0;
        }
        let len = std::cmp::min(src.len(), self.size - offset);

        let src_ptr = src.as_ptr();
        unsafe { std::ptr::copy_nonoverlapping(src_ptr, self.as_mut_ptr().add(offset), len) }
        len
    }

    pub fn memset(&mut self, value: u8) {
        unsafe { std::ptr::write_bytes(self.as_mut_ptr(), value, self.size) }
    }
}

impl Drop for PosixDmaBuffer {
    fn drop(&mut self) {
        unsafe {
            aligned_free(self.data.as_ptr() as *mut ());
        }
    }
}
