// unless explicitly stated otherwise all files in this repository are licensed
// under the mit/apache-2.0 license, at your convenience
//
// this product includes software developed at datadog (https://www.datadoghq.com/). copyright 2020 datadog, inc.
use crate::sys::DmaBuffer;
use core::num::NonZeroUsize;
use std::rc::Rc;

#[derive(Default, Clone, Debug)]
/// ReadResult encapsulates a buffer, returned by read operations like
/// [`get_buffer_aligned`](super::DmaStreamReader::get_buffer_aligned) and
/// [`read_at`](super::DmaFile::read_at)
pub struct ReadResult(Option<ReadResultInner>);

impl core::ops::Deref for ReadResult {
    type Target = [u8];

    /// Allows accessing the contents of this buffer as a byte slice
    fn deref(&self) -> &[u8] {
        self.0.as_deref().unwrap_or(&[])
    }
}

#[derive(Clone, Debug)]
struct ReadResultInner {
    buffer: Rc<DmaBuffer>,
    offset: usize,

    // This (usage of `NonZeroUsize`) is probably needed to make sure that rustc
    // doesn't need to reserve additional memory for the surrounding Option enum tag.
    // see also `self::test::equal_struct_size`
    //
    // The additionally `ReadResultInner` structure is a good idea as it allows
    // a cleaner implementation (offset and end/len don't have any meaning if buffer.is_none()).
    len: NonZeroUsize,
}

impl core::ops::Deref for ReadResultInner {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.buffer.as_bytes()[self.offset..][..self.len.get()]
    }
}

impl ReadResult {
    pub(crate) fn empty_buffer() -> Self {
        Self(None)
    }

    pub(crate) fn from_whole_buffer(buffer: DmaBuffer) -> Self {
        Self(NonZeroUsize::new(buffer.len()).map(|len| ReadResultInner {
            buffer: Rc::new(buffer),
            offset: 0,
            len,
        }))
    }

    /// Creates a slice of this ReadResult with the given offset and length.
    ///
    /// Returns `None` if either offset or offset + len would not fit in the
    /// original buffer.
    pub fn slice(this: &Self, extra_offset: usize, len: usize) -> Option<Self> {
        Some(Self(if let Some(len) = NonZeroUsize::new(len) {
            Some(ReadResultInner::slice(this.0.as_ref()?, extra_offset, len)?)
        } else {
            // This branch is needed to make sure that calls to `slice` with `len = 0` are
            // handled. If they aren't valid, then `len` should be changed to
            // `NonZeroUsize`.
            None
        }))
    }

    /// Creates a slice of this ReadResult with the given offset and length.
    /// Similiar to [`ReadResult::slice`], but does not check if the offset and
    /// length are correct.
    ///
    /// # Safety
    ///
    /// Any user of this function must guarantee that the offset and length are
    /// correct (not out of bounds).
    pub unsafe fn slice_unchecked(this: &Self, extra_offset: usize, len: usize) -> Self {
        Self(NonZeroUsize::new(len).map(|len| {
            ReadResultInner::slice_unchecked(this.0.as_ref().unwrap(), extra_offset, len)
        }))
    }
}

impl ReadResultInner {
    fn check_invariants(this: &Self) {
        if cfg!(debug_assertions) {
            let max_len = this.buffer.len();
            assert!(
                (this.offset + this.len.get()) <= max_len,
                "a ReadResult contains an out-of-range 'end': offset ({} + {}) > buffer length \
                 ({})",
                this.offset,
                this.len,
                max_len,
            );
        }
    }

    fn slice(this: &Self, extra_offset: usize, len: NonZeroUsize) -> Option<Self> {
        Self::check_invariants(this);
        if extra_offset > this.len.get() || len.get() > (this.len.get() - extra_offset) {
            None
        } else {
            Some(ReadResultInner {
                buffer: this.buffer.clone(),
                offset: this.offset + extra_offset,
                len,
            })
        }
    }

    unsafe fn slice_unchecked(this: &Self, extra_offset: usize, len: NonZeroUsize) -> Self {
        Self::check_invariants(this);
        if cfg!(debug_assertions) {
            assert!(
                extra_offset <= this.len.get(),
                "offset {} is more than the length ({}) of the slice",
                extra_offset,
                this.len,
            );
            assert!(
                len.get() <= (this.len.get() - extra_offset),
                "length {} would cross past the end ({}) of the slice",
                len.get() + extra_offset,
                this.len,
            );
        }

        Self {
            buffer: this.buffer.clone(),
            offset: this.offset + extra_offset,
            len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equal_struct_size() {
        use core::mem::size_of;
        assert_eq!(size_of::<ReadResult>(), size_of::<ReadResultInner>());
    }
}
