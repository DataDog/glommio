// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.

/// Utility methods for working with byte slices/buffers.
pub trait ByteSliceExt {
    /// Copies the contents of `self` into the byte-slice `destination`.
    ///
    /// The copy starts at position `offset` into the `source` and copies
    /// as many bytes as possible until either we don't have more bytes to copy
    /// or `destination` doesn't have more space to hold them.
    ///
    /// ## Returns
    ///
    /// The amount of bytes written.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{io::DmaFile, ByteSliceExt, LocalExecutor};
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
    ///     assert_eq!(n, 6); // read 6 bytes, as there are only 6 bytes left from this offset
    ///     file.close().await.unwrap();
    /// });
    /// ```
    fn read_at<D: AsMut<[u8]> + ?Sized>(&self, offset: usize, destination: &mut D) -> usize;
}

/// Utility methods for working with mutable byte slices/buffers.
pub trait ByteSliceMutExt {
    /// Writes data to this buffer from a user-provided byte slice, starting at
    /// a particular offset
    ///
    /// ## Returns
    ///
    /// The amount of bytes written.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{io::DmaFile, ByteSliceMutExt, LocalExecutor};
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
    ///     assert_eq!(n, 6); // wrote 6 bytes, as there are only 6 bytes left from this offset
    ///     file.close().await.unwrap();
    /// });
    /// ```
    fn write_at<S: AsRef<[u8]> + ?Sized>(&mut self, offset: usize, src: &S) -> usize;

    /// Writes the specified value into all bytes of this buffer
    fn memset(&mut self, value: u8);
}

#[inline]
fn slice_read_at(source: &[u8], offset: usize, destination: &mut [u8]) -> usize {
    if offset > source.len() {
        0
    } else {
        let len = core::cmp::min(destination.len(), source.len() - offset);
        destination[0..len].copy_from_slice(&source[offset..][..len]);
        len
    }
}

#[inline]
fn slice_write_at(destination: &mut [u8], offset: usize, source: &[u8]) -> usize {
    if offset > destination.len() {
        0
    } else {
        let len = std::cmp::min(source.len(), destination.len() - offset);
        destination[offset..offset + len].copy_from_slice(&source[0..len]);
        len
    }
}

#[inline]
fn slice_memset(destination: &mut [u8], value: u8) {
    for b in destination.iter_mut() {
        *b = value;
    }
}

// A NOTE about `#[inline]`: rustc currently only inlines code *across crate
// boundaries* if it is marked with `#[inline]`.

impl<T: AsRef<[u8]> + ?Sized> ByteSliceExt for T {
    #[inline]
    fn read_at<D: AsMut<[u8]> + ?Sized>(&self, offset: usize, destination: &mut D) -> usize {
        slice_read_at(self.as_ref(), offset, destination.as_mut())
    }
}

impl<T: AsMut<[u8]> + ?Sized> ByteSliceMutExt for T {
    #[inline]
    fn write_at<S: AsRef<[u8]> + ?Sized>(&mut self, offset: usize, src: &S) -> usize {
        slice_write_at(self.as_mut(), offset, src.as_ref())
    }

    #[inline]
    fn memset(&mut self, value: u8) {
        slice_memset(self.as_mut(), value)
    }
}
