// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//! `glommio::io` provides data structures targeted towards File I/O.
//!
//! File I/O in Glommio comes in two kinds: Buffered and Direct I/O.
//!
//! Ideally an application would pick one of them according to its needs and not
//! mix both. However, if you do want to mix both, it is recommended that you do
//! not do so in the same device: Kernel settings like I/O schedulers and merge
//! settings that are beneficial to one of them can be detrimental to the
//! others.
//!
//! If you absolutely must use both in the same device, avoid issuing both
//! Direct and Buffered I/O in the same file: at this point you are just trying
//! to drive Linux crazy.
//!
//! Buffered I/O
//! ============
//!
//! Buffered I/O will use the operating system page cache. It is ideal for
//! simpler applications that don't want to deal with caching policies and have
//! I/O performance as a maybe important, but definitely not crucial part of
//! their performance story.
//!
//! Disadvantages of Buffered I/O:
//!  * Hard to know when resources are really used, which make controlled
//!    processes almost impossible (the time of write to device is detached from
//!    the file write time)
//!  * More copies than necessary, as the data has to be copied from the device
//!    to the page cache, from the page cache to the internal file buffers, and
//!    in abstract linear implementations like [`AsyncWriteExt`] and
//!    [`AsyncReadExt`] from user-provided buffers to the file internal buffers.
//!  * Advanced features for io_uring like Non-interrupt mode, registered files,
//!    registered buffers, will not work with Buffered I/O
//!  * Read amplification for small random reads, as the OS is bounded by the
//!    page size (usually 4 KiB), even though modern NVMe devices are perfectly
//!    capable of issuing 512-byte I/O.
//!
//! The main structure to deal with Buffered I/O is the [`BufferedFile`] struct.
//! It is targeted at random I/O. Reads from and writes to it expect a position.
//!
//! Direct I/O
//! ==========
//!
//! Direct I/O will not use the Operating System page cache and will always
//! touch the device directly. That will always work very well for stream-based
//! workloads (scanning a file much larger than memory, writing a buffer that
//! will not be read from in the near future, etc.) but will require a
//! user-provided cache for good random performance.
//!
//! There are advantages to using a user-provided cache: Files usually contain
//! serialized objects and every read have to deserialize them. A user-provided
//! cache can cache the parsed objects, among others. Still, not all
//! applications can or want to deal with that complexity.
//!
//! Disadvantages of Direct I/O:
//! * I/O needs to be aligned. Both the buffers and the file positions need
//!   specific alignments. The [`DmaBuffer`] should hide most of that
//!   complexity, but you may still end up with heavy read amplification if you
//!   are not careful.
//! * Without a user-provided cache, random performance can be bad.
//!
//! At the lowest level, there are two main structs that deal with File Direct
//! I/O:
//!
//! [`DmaFile`] is targeted at random Direct I/O. Reads from and writes to it
//! expect a position.
//!
//! [`DmaStreamWriter`] and [`DmaStreamReader`] perform sequential I/O and their
//! interface is a lot closer to other mainstream rust interfaces in `std::fs`.
//!
//! However, despite being sequential, I/O for the two Stream structs are
//! parallel: [`DmaStreamWriter`] exposes a setting for write-behind, meaning
//! that it will keep accepting writes to its internal buffers even with older
//! writes are still in-flight. In turn, [`DmaStreamReader`] exposes a setting
//! for read-ahead meaning it will initiate I/O for positions you will read into
//! the future sooner.
//!
//! ImmutableFile
//! =============
//!
//! Often times, due to constraints of modern storage systems, files are
//! immutable once written. To safely capture that pattern and provide useful
//! optimizations, Glommio exposes an [`ImmutableFile`]: The [`ImmutableFile`]
//! can be written to once created, but once sealed it is assumed not to change.
//! This allows Glommio to provide some optimizations, like basic caching, that
//! should make Direct I/O more palatable.
//!
//! If this matches your access patterns, consider using an [`ImmutableFile`]
//! instead of the lower level [`DmaFile`]. [`ImmutableFile`]s can be accessed
//! both randomly or sequentially.
//!
//! [`ImmutableFile`]: struct.ImmutableFile.html
//! [`BufferedFile`]: struct.BufferedFile.html
//! [`DmaFile`]: struct.DmaFile.html
//! [`DmaBuffer`]: struct.DmaBuffer.html
//! [`DmaStreamWriter`]: struct.DmaStreamWriter.html
//! [`DmaStreamReader`]: struct.DmaStreamReader.html
//! [`AsyncReadExt`]: https://docs.rs/futures-lite/1.11.2/futures_lite/io/trait.AsyncReadExt.html
//! [`AsyncWriteExt`]: https://docs.rs/futures-lite/1.11.2/futures_lite/io/trait.AsyncWriteExt.html

macro_rules! enhanced_try {
    ($expr:expr, $op:expr, $path:expr, $fd:expr) => {{
        match $expr {
            Ok(val) => Ok(val),
            Err(source) => {
                let enhanced = crate::error::GlommioError::<()>::EnhancedIoError {
                    source,
                    op: $op,
                    path: $path.and_then(|x| Some(x.to_path_buf())),
                    fd: $fd,
                };
                Err(enhanced)
            }
        }
    }};
    ($expr:expr, $op:expr, $obj:expr) => {{ enhanced_try!($expr, $op, $obj.path(), Some($obj.as_raw_fd())) }};
}

mod buffered_file;
mod buffered_file_stream;
mod bulk_io;
mod directory;
mod dma_file;
mod dma_file_stream;
mod glommio_file;
mod immutable_file;
mod open_options;
mod read_result;
mod sched;

use std::path::Path;

pub(super) type Result<T> = crate::Result<T, ()>;

/// rename an existing file.
pub async fn rename<P: AsRef<Path>, Q: AsRef<Path>>(old_path: P, new_path: Q) -> Result<()> {
    let reactor = crate::executor().reactor();
    let source = reactor.rename(old_path.as_ref(), new_path.as_ref()).await;
    source.collect_rw().await?;
    Ok(())
}

/// remove an existing file given its name
pub async fn remove<P: AsRef<Path>>(path: P) -> Result<()> {
    let reactor = crate::executor().reactor();
    let source = reactor.remove_file(path.as_ref()).await;
    source.collect_rw().await?;
    Ok(())
}

pub(crate) use self::sched::{FileScheduler, IoScheduler, ScheduledSource};
pub use self::{
    buffered_file::BufferedFile,
    buffered_file_stream::{
        stdin,
        StreamReader,
        StreamReaderBuilder,
        StreamWriter,
        StreamWriterBuilder,
    },
    bulk_io::{
        BulkIo,
        IoConcurrencyLimiter,
        IoVec,
        MergedBufferLimit,
        OrderedBulkIo,
        ReadAmplificationLimit,
        ReadManyResult,
        UnorderedBulkIo,
    },
    directory::Directory,
    dma_file::{CloseResult, CoalescedReadsMapped, DmaFile},
    dma_file_stream::{
        DmaStreamReader,
        DmaStreamReaderBuilder,
        DmaStreamWriter,
        DmaStreamWriterBuilder,
    },
    immutable_file::{ImmutableFile, ImmutableFileBuilder, ImmutableFilePreSealSink},
    open_options::OpenOptions,
    read_result::ReadResult,
};
pub use crate::sys::DmaBuffer;

#[cfg(test)]
mod test {
    use super::*;
    use crate::LocalExecutor;

    #[test]
    fn remove_nonexistent() {
        let local_ex = LocalExecutor::default();
        local_ex.run(async {
            let x = remove("/tmp/this_file_does_not_exist_and_if_you_created_just_to_mess_with_me_you_deserve_this_test_to_fail_and_I_am_not_even_sorry").await;
            assert_eq!(x.unwrap_err().raw_os_error().unwrap(), libc::ENOENT);
        });
    }
}
