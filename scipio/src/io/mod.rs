// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//! scipio::io provides data structures targeted towards File I/O.
//!
//! There are two main structs that deal with File I/O:
//!
//! [`DmaFile`] is targeted at random I/O, and reads and writes to it
//! expect a position.
//!
//! [`StreamWriter`] and [`StreamReader`] perform sequential I/O and their
//! interface is a lot closer to other mainstream rust interfaces in std::fs.
//!
//! However, despite being sequential, I/O for the two Stream structs are parallel:
//! [`StreamWriter`] exposes a setting for write-behind, meaning that it will keep
//! accepting writes to its internal buffers even with older writes are still in-flight.
//! In turn, [`StreamReader`] exposes a setting for read-ahead meaning it will initiate
//! I/O for positions you will read into the future sooner.
//!
//! Both random and sequential file classes are implemented on top of Direct I/O meaning
//! every call is expected to reach the media. Especially for random I/O, this can be very
//! detrimental without a user-provided cache. This means that those entities are better
//! used in conjunction with your own cache.
macro_rules! enhanced_try {
    ($expr:expr, $op:expr, $path:expr, $fd:expr) => {{
        match $expr {
            Ok(val) => Ok(val),
            Err(inner) => {
                let enhanced: std::io::Error = crate::error::ErrorEnhancer {
                    inner,
                    op: $op,
                    path: $path.and_then(|x| Some(x.to_path_buf())),
                    fd: $fd,
                }
                .into();
                Err(enhanced)
            }
        }
    }};
    ($expr:expr, $op:expr, $obj:expr) => {{
        enhanced_try!(
            $expr,
            $op,
            $obj.path.as_ref().and_then(|x| Some(x.as_path())),
            Some($obj.as_raw_fd())
        )
    }};
}

macro_rules! path_required {
    ($obj:expr, $op:expr) => {{
        $obj.path.as_ref().ok_or(crate::error::ErrorEnhancer {
            inner: std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "operation requires a valid path",
            ),
            op: $op,
            path: None,
            fd: Some($obj.as_raw_fd()),
        })
    }};
}

macro_rules! bad_buffer {
    ($obj:expr) => {{
        let enhanced: std::io::Error = crate::error::ErrorEnhancer {
            inner: std::io::Error::from_raw_os_error(5),
            op: "processing read buffer",
            path: $obj.path.clone(),
            fd: Some($obj.as_raw_fd()),
        }
        .into();
        enhanced
    }};
}

mod dma_file;
mod file_stream;
mod read_result;

use crate::sys;
use std::io;
use std::path::Path;

/// rename an existing file.
///
/// Warning: synchronous operation, will block the reactor
pub async fn rename<P: AsRef<Path>, Q: AsRef<Path>>(old_path: P, new_path: Q) -> io::Result<()> {
    sys::rename_file(&old_path.as_ref(), &new_path.as_ref())
}

/// remove an existing file given its name
///
/// Warning: synchronous operation, will block the reactor
pub async fn remove<P: AsRef<Path>>(path: P) -> io::Result<()> {
    enhanced_try!(
        sys::remove_file(path.as_ref()),
        "Removing",
        Some(path.as_ref()),
        None
    )
}

pub use self::dma_file::{Directory, DmaFile};
pub use self::file_stream::{StreamReader, StreamReaderBuilder, StreamWriter, StreamWriterBuilder};
pub use self::read_result::ReadResult;
pub use crate::sys::DmaBuffer;
