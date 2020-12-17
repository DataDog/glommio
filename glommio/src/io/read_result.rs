// unless explicitly stated otherwise all files in this repository are licensed under the
// mit/apache-2.0 license, at your convenience
//
// this product includes software developed at datadog (https://www.datadoghq.com/). copyright 2020 datadog, inc.
//
use crate::sys::DmaBuffer;
use std::io;
use std::rc::Rc;

type Result<T> = crate::Result<T, ()>;

#[derive(Debug, Clone)]
/// ReadResult encapsulates a buffer, returned by read operations like [`get_buffer_aligned`] and
/// [`read_at`]
///
/// [`get_buffer_aligned`]: struct.DmaStreamReader.html#method.get_buffer_aligned
/// [`read_at`]: struct.DmaFile.html#method.read_at
pub struct ReadResult {
    buffer: Option<Rc<DmaBuffer>>,
    offset: usize,
    end: usize,
}

static EMPTY: [u8; 0] = [0; 0];

#[allow(clippy::len_without_is_empty)]
impl ReadResult {
    pub(crate) fn empty_buffer() -> ReadResult {
        ReadResult {
            end: 0,
            offset: 0,
            buffer: None,
        }
    }

    pub(crate) fn from_whole_buffer(buffer: DmaBuffer) -> ReadResult {
        ReadResult {
            end: buffer.len(),
            offset: 0,
            buffer: Some(Rc::new(buffer)),
        }
    }

    /// Copies the contents of this [`ReadResult`] into the byte-slice `dst`.
    ///
    /// The copy starts at position `offset` into the [`ReadResult`] and copies
    /// as many bytes as possible until either we don't have more bytes to copy
    /// or `dst` doesn't have more space to hold them.
    ///
    /// [`ReadResult`]: struct.ReadResult.html
    pub fn read_at(&self, offset: usize, dst: &mut [u8]) -> usize {
        let offset = self.offset + offset;
        self.buffer.as_ref().unwrap().read_at(offset, dst)
    }

    /// Creates a slice of this ReadResult with the given offset and length.
    ///
    /// Returns an [`std::io::Error`] with `[std::io::ErrorKind`] `[InvalidInput`] if
    /// either offset or offset + len would not fit in the original buffer.
    ///
    /// [`std::io::Error`]: https://doc.rust-lang.org/std/io/struct.Error.html
    /// [`std::io::ErrorKind`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html
    /// [`InvalidInput`]: https://doc.rust-lang.org/std/io/struct.ErrorKind.html#variant.InvalidInput
    pub fn slice(&self, extra_offset: usize, len: usize) -> Result<ReadResult> {
        let offset = self.offset + extra_offset;
        let end = offset + len;

        if offset > self.buffer.as_ref().unwrap().len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "offset {} ({} + {}) is more than the length of the buffer",
                    offset, self.offset, extra_offset
                ),
            )
            .into());
        }

        if end > self.buffer.as_ref().unwrap().len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "length {} would cross past the end of the buffer ({} + {})",
                    end, offset, len
                ),
            )
            .into());
        }

        Ok(ReadResult {
            buffer: self.buffer.clone(),
            offset,
            end,
        })
    }

    /// The length of this buffer
    pub fn len(&self) -> usize {
        self.end - self.offset
    }

    /// Allows accessing the contents of this buffer as a byte slice
    pub fn as_bytes(&self) -> &[u8] {
        match self.buffer.as_ref() {
            None => &EMPTY,
            Some(buffer) => &buffer.as_bytes()[self.offset..self.end],
        }
    }
}
