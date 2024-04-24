// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
// Buffers that are friendly to be used with O_DIRECT files.
// For the time being they are really only properly aligned,
// but in the near future they can be coming from memory-areas
// that are pre-registered for I/O uring.

use std::ptr;

use crate::{io::ReadResult, sys::uring::UringBuffer};
use alloc::alloc::Layout;

#[derive(Debug)]
pub(crate) struct SysAlloc {
    data: ptr::NonNull<u8>,
    layout: Layout,
}

impl SysAlloc {
    fn new(size: usize) -> Option<Self> {
        if size == 0 {
            None
        } else {
            let layout = Layout::from_size_align(size, 4096).unwrap();
            let data = unsafe { alloc::alloc::alloc(layout) };
            let data = ptr::NonNull::new(data)?;
            Some(SysAlloc { data, layout })
        }
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
            alloc::alloc::dealloc(self.data.as_ptr(), self.layout);
        }
    }
}

#[derive(Debug)]
pub(crate) enum BufferStorage {
    Sys(SysAlloc),
    Uring(UringBuffer),
    EventFd(*mut u8),
    ReadResult(ReadResult),
}

impl BufferStorage {
    fn as_ptr(&self) -> *const u8 {
        match self {
            BufferStorage::Sys(x) => x.as_ptr(),
            BufferStorage::Uring(x) => x.as_ptr(),
            BufferStorage::EventFd(x) => *x as *const u8,
            BufferStorage::ReadResult(x) => x.as_ptr(),
        }
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        match self {
            BufferStorage::Sys(x) => x.as_mut_ptr(),
            BufferStorage::Uring(x) => x.as_mut_ptr(),
            BufferStorage::EventFd(x) => *x,
            BufferStorage::ReadResult(_) => {
                unreachable!("Attempt to access immutable ReadResult as a mutable pointer")
            }
        }
    }
}

#[derive(Debug)]
/// A buffer suitable for Direct I/O over io_uring
///
/// Direct I/O has strict requirements about alignment. On top of that, io_uring
/// places additional restrictions on memory placement if we are to use advanced
/// features like registered buffers.
///
/// The `DmaBuffer` is a buffer that adheres to those properties making it
/// suitable for io_uring's Direct I/O.
pub struct DmaBuffer {
    storage: BufferStorage,
    // Invariant: trim + size are at most one byte past the original allocation.
    trim: usize,
    size: usize,
}

impl DmaBuffer {
    pub(crate) fn new(size: usize) -> Option<DmaBuffer> {
        Some(DmaBuffer {
            storage: BufferStorage::Sys(SysAlloc::new(size)?),
            size,
            trim: 0,
        })
    }

    pub(crate) fn with_storage(size: usize, storage: BufferStorage) -> DmaBuffer {
        DmaBuffer {
            storage,
            size,
            trim: 0,
        }
    }

    pub(crate) fn uring_buffer_id(&self) -> Option<u32> {
        match &self.storage {
            BufferStorage::Uring(x) => x.uring_buffer_id(),
            _ => None,
        }
    }

    /// Reduce the length of this buffer to be `newsize`. This value must be <= the current
    /// [`len`](#method.len) and is a destructive operation (length cannot be increased).
    /// NOTE: When using this with DmaFile, you have to make sure that `newsize` is properly
    /// aligned or the write will fail due to O_DIRECT requirements.
    pub fn trim_to_size(&mut self, newsize: usize) {
        assert!(newsize <= self.size);
        self.size = newsize;
    }

    /// Returns a representation of the current addressable contents of this
    /// `DmaBuffer` as a byte slice
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.size) }
    }

    /// Returns a representation of the current addressable contents of this
    /// `DmaBuffer` as a mutable byte slice
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.size) }
    }

    /// Returns the current number of bytes present in the addressable contents
    /// of this `DmaBuffer`
    pub fn len(&self) -> usize {
        self.size
    }

    /// Indicates whether this buffer is empty. An empty buffer can be,
    /// for instance, the result of a read past the end of a file.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Returns a representation of the current addressable contents of this
    /// `DmaBuffer` as mutable pointer
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        unsafe { self.storage.as_mut_ptr().add(self.trim) }
    }

    /// Returns a representation of the current addressable contents of this
    /// `DmaBuffer` as pointer
    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.storage.as_ptr().add(self.trim) }
    }
}

impl AsRef<[u8]> for DmaBuffer {
    #[inline(always)]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsMut<[u8]> for DmaBuffer {
    #[inline(always)]
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_bytes_mut()
    }
}

impl From<ReadResult> for DmaBuffer {
    fn from(value: ReadResult) -> Self {
        Self {
            trim: 0,
            size: value.len(),
            storage: BufferStorage::ReadResult(value),
        }
    }
}
