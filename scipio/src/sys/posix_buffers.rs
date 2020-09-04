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
    trim: usize,
    size: usize,
}

impl PosixDmaBuffer {
    pub fn new(size: usize) -> Option<PosixDmaBuffer> {
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

    pub fn trim_to_size(&mut self, newsize: usize) {
        self.size = newsize;
    }

    pub fn trim_front(&mut self, trim: usize) {
        self.trim = trim;
    }

    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.size) }
    }

    pub fn as_mut_bytes(&self) -> &mut [u8] {
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
