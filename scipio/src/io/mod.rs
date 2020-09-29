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
mod dma_file;
mod file_stream;
mod read_result;

pub use self::dma_file::{Directory, DmaFile};
pub use self::file_stream::{StreamReader, StreamReaderBuilder, StreamWriter, StreamWriterBuilder};
pub use self::read_result::ReadResult;
