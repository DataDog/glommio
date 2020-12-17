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
use alloc::alloc::Layout;

#[derive(Debug)]
pub(crate) struct SysAlloc {
    data: ptr::NonNull<u8>,
    layout: Layout,
}

impl SysAlloc {
    fn new(size: usize) -> Option<Self> {
        let layout = Layout::from_size_align(size, 4096).unwrap();
        let data = unsafe { alloc::alloc::alloc(layout) as *mut u8 };
        let data = ptr::NonNull::new(data)?;
        Some(SysAlloc { data, layout })
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
/// A buffer suitable for Direct I/O over io_uring
///
/// Direct I/O has strict requirements about alignment. On top of that, io_uring places additional
/// restrictions on memory placement if we are to use advanced features like registered buffers.
///
/// The `DmaBuffer` is a buffer that adheres to those properties making it suitable for io_uring's
/// Direct I/O.
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

    /// Returns a representation of the current addressable contents of this `DmaBuffer` as a byte
    /// slice
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.size) }
    }

    /// Returns a representation of the current addressable contents of this `DmaBuffer` as a
    /// mutable byte slice
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.as_mut_ptr(), self.size) }
    }

    /// Returns the current number of bytes present in the addressable contents of this `DmaBuffer`
    pub fn len(&self) -> usize {
        self.size
    }

    /// Indicates whether or not this buffer is empty. An empty buffer can be, for instance, the
    /// result of a read past the end of a file.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Returns a representation of the current addressable contents of this `DmaBuffer` as mutable
    /// pointer
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        unsafe { self.storage.as_mut_ptr().add(self.trim) }
    }

    /// Returns a representation of the current addressable contents of this `DmaBuffer` as pointer
    pub fn as_ptr(&self) -> *const u8 {
        unsafe { self.storage.as_ptr().add(self.trim) }
    }

    /// Reads data from this buffer into a user-provided byte slice, starting at a particular
    /// offset
    ///
    /// ## Returns
    ///
    /// The amount of bytes read.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::LocalExecutor;
    /// use glommio::io::DmaFile;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::create("test.txt").await.unwrap();
    ///     let buf = file.alloc_dma_buffer(4096);
    ///     let mut vec = vec![0; 64];
    ///     let n = buf.read_at(0, &mut vec);
    ///     assert_eq!(n, 64); // read 64 bytes, the size of the buffer
    ///
    ///     let n = buf.read_at(4090, &mut vec);
    ///     assert_eq!(n, 6);  // read 6 bytes, as there are only 6 bytes left from this offset
    ///     file.close().await.unwrap();
    /// });
    /// ```
    pub fn read_at(&self, offset: usize, dst: &mut [u8]) -> usize {
        if offset > self.size {
            return 0;
        }
        let len = std::cmp::min(dst.len(), self.size - offset);
        let me = &self.as_bytes()[offset..offset + len];
        dst[0..len].copy_from_slice(me);
        len
    }

    /// Writes data to this buffer from a user-provided byte slice, starting at a particular
    /// offset
    ///
    /// ## Returns
    ///
    /// The amount of bytes written.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::LocalExecutor;
    /// use glommio::io::DmaFile;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::create("test.txt").await.unwrap();
    ///     let mut buf = file.alloc_dma_buffer(4096);
    ///     let vec = vec![1; 64];
    ///     let n = buf.write_at(0, &vec);
    ///     assert_eq!(n, 64); // wrote 64 bytes.
    ///
    ///     let n = buf.write_at(4090, &vec);
    ///     assert_eq!(n, 6);  // wrote 6 bytes, as there are only 6 bytes left from this offset
    ///     file.close().await.unwrap();
    /// });
    /// ```
    pub fn write_at(&mut self, offset: usize, src: &[u8]) -> usize {
        if offset > self.size {
            return 0;
        }
        let len = std::cmp::min(src.len(), self.size - offset);
        let me = &mut self.as_bytes_mut()[offset..offset + len];
        me.copy_from_slice(&src[0..len]);
        len
    }

    /// Writes the specified value into all bytes of this buffer
    pub fn memset(&mut self, value: u8) {
        unsafe { std::ptr::write_bytes(self.as_mut_ptr(), value, self.size) }
    }
}
