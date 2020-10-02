// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
// Buffers that are friendly to be used with O_DIRECT files.
// For the time being they are really only properly aligned,
// but in the near future they can be coming from memory-areas
// that are pre-registered for I/O uring.

use aligned_alloc::{aligned_alloc, aligned_free};

#[derive(Debug)]
pub struct PosixDmaBuffer {
    data: *mut u8,
    // Invariant: trim + size are at most one byte past the original allocation.
    trim: usize,
    size: usize,
}

// Adapted from stdlib's source code, simplified because we only ever need u8.
fn is_nonoverlapping(src: *const u8, dst: *const u8, size: usize) -> bool {
    let src_usize = src as usize;
    let dst_usize = dst as usize;
    let diff = if src_usize > dst_usize {
        src_usize - dst_usize
    } else {
        dst_usize - src_usize
    };
    // If the absolute distance between the ptrs is at least as big as the size of the buffer,
    // they do not overlap.
    diff >= size
}

impl PosixDmaBuffer {
    pub(crate) fn new(size: usize) -> Option<PosixDmaBuffer> {
        let data: *mut u8;
        data = aligned_alloc(size, 4 << 10) as *mut u8;
        if data.is_null() {
            return None;
        }
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

    pub fn as_mut_ptr(&self) -> *mut u8 {
        unsafe { self.data.add(self.trim) }
    }

    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.data.add(self.trim) }
    }

    pub fn copy_to_slice(&self, offset: usize, dst: &mut [u8]) -> usize {
        if offset > self.size {
            return 0;
        }
        let len = std::cmp::min(dst.len(), self.size - offset);

        let dst_ptr = dst.as_mut_ptr() as *mut u8;
        assert_eq!(is_nonoverlapping(self.as_ptr(), dst_ptr, len), true);
        unsafe { std::ptr::copy_nonoverlapping(self.as_ptr().add(offset), dst_ptr, len) }
        len
    }

    pub fn copy_from_slice(&self, offset: usize, src: &[u8]) -> usize {
        if offset > self.size {
            return 0;
        }
        let len = std::cmp::min(src.len(), self.size - offset);

        let src_ptr = src.as_ptr() as *const u8;
        assert_eq!(is_nonoverlapping(src_ptr, self.as_ptr(), len), true);
        unsafe { std::ptr::copy_nonoverlapping(src_ptr, self.as_mut_ptr().add(offset), len) }
        len
    }

    pub fn memset(&self, value: u8) {
        unsafe { std::ptr::write_bytes(self.as_mut_ptr(), value, self.size) }
    }
}

impl Drop for PosixDmaBuffer {
    fn drop(&mut self) {
        if !self.data.is_null() {
            unsafe {
                aligned_free(self.data as *mut ());
            }
        }
    }
}
