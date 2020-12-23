// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use std::path::PathBuf;
use std::{
    fmt::{self, Debug},
    io,
};
use std::{os::unix::io::RawFd, path::Path};
use thiserror::Error;

/// Result type alias that all Glommio public API functions can use.
pub type Result<T, V> = std::result::Result<T, GlommioError<V>>;

/// Resource Type used for errors that `WouldBlock` and includes extra
/// diagnostic data for richer error messages.
#[derive(Debug)]
pub enum ResourceType<T> {
    /// Semaphore resource that includes the requested and available shares
    /// as debugging metadata for the [`Semaphore`](crate::sync::Semaphore) type.
    Semaphore {
        /// Requested shares
        requested: u64,
        /// Available semaphore shares
        available: u64,
    },

    /// Lock variant for reporting errors from the [`RwLock`](crate::sync::RwLock) type.
    RwLock,

    /// Channel variant for reporting errors from [`local_channel`](crate::channels::local_channel) and
    /// [`shared_channel`](crate::channels::shared_channel) channel types.
    Channel(T),

    /// File variant used for reporting errors for the Buffered ([`BufferedFile`](crate::io::BufferedFile))
    /// and Direct ([`DmaFile`](crate::io::DmaFile)) file I/O variants.
    File(String),
}

/// Error variants for executor queues.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum QueueErrorKind {
    /// Queue is still active
    StillActive,
    /// Queue is not found
    NotFound,
}

/// Errors coming from the reactor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReactorErrorKind {
    IncorrectSourceType,
}

impl fmt::Display for ReactorErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReactorErrorKind::IncorrectSourceType => write!(f, "Incorrect source type!"),
        }
    }
}

/// Error types that can be created by the executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorErrorKind {
    QueueError {
        /// index of the queue
        index: usize,

        /// the kind of error encountered
        kind: QueueErrorKind,
    },
}

impl fmt::Display for ExecutorErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutorErrorKind::QueueError { index, kind } => {
                write!(f, "Queue #{} is {}", index, kind)
            }
        }
    }
}

/// Composite error type to encompass all error types glommio produces.
///
/// Single error type that will be produced by any public Glommio API
/// functions. Contains a generic type that is only used for the [ `Channel` ](ResourceType::Channel)
/// variants. In other cases it can just be replaced with the unit type `()`
/// and ignored. The variants are broken up into a few common categories such as
/// [`Closed`](GlommioError::Closed) and [`WouldBlock`](GlommioError::WouldBlock)
/// as well as a generic [`IoError`](GlommioError::IoError) and errors dedicated to
/// the executor and reactor.
///
/// # Examples
///
/// ```
/// use glommio::{GlommioError, ResourceType};
///
/// fn will_error() -> Result<(), GlommioError<()>> {
///     Err(GlommioError::WouldBlock(ResourceType::File("Error reading a file".to_string())))?
/// }
/// assert!(will_error().is_err());
///
/// ```
#[derive(Error)]
pub enum GlommioError<T> {
    /// IO error from standard library functions or libraries that produce
    /// std::io::Error's.
    #[error("IO error occurred: {0}")]
    IoError(#[from] io::Error),

    /// Enhanced IO error that gives more information in the error message
    /// than the basic [`IoError`](GlommioError::IoError). It includes the
    /// operation, path and file descriptor. It also contains the error
    /// from the source IO error from `std::io::*`.
    #[error("{source}, op: {op} path: {path:?} with fd: {fd:?}")]
    EnhancedIoError {
        /// The source error from `std::io::Error`.
        #[source]
        source: io::Error,

        /// The operation that was being attempted.
        op: &'static str,

        /// The path of the file, if relavent.
        path: Option<PathBuf>,

        /// The numeric file descriptor of the relavent resource.
        fd: Option<RawFd>,
    },

    /// Executor error variant(s) for signaling certain error conditions
    /// inside of the executor.
    #[error("Executor error: {0}")]
    ExecutorError(ExecutorErrorKind),

    /// The resource in question is closed. Generic because the channel
    /// variant needs to return the actual item sent into the channel.
    #[error("{0} is closed")]
    Closed(ResourceType<T>),

    /// Error encapsulating the `WouldBlock` error for types that don't have
    /// errors originating in the standard library. Glommio also has
    /// nonblocking types that need to indicate if they are blocking or not.
    /// This type allows for signaling when a function would otherwise block
    /// for a specific `ResourceType`.
    #[error("{0} operation would block")]
    WouldBlock(ResourceType<T>),

    /// Reactor error variants. This includes errors specific to the operation
    /// of the io-uring instances or related.
    #[error("Reactor error: {0}")]
    ReactorError(ReactorErrorKind),
}

impl GlommioError<()> {
    pub(crate) fn create_enhanced<P: AsRef<Path>>(
        source: io::Error,
        op: &'static str,
        path: Option<P>,
        fd: Option<RawFd>,
    ) -> GlommioError<()> {
        GlommioError::EnhancedIoError {
            source,
            op,
            path: path.map(|path| path.as_ref().to_path_buf()),
            fd,
        }
    }
}

impl<T> From<(io::Error, ResourceType<T>)> for GlommioError<T> {
    fn from(tuple: (io::Error, ResourceType<T>)) -> Self {
        match tuple.0.kind() {
            io::ErrorKind::BrokenPipe => GlommioError::Closed(tuple.1),
            io::ErrorKind::WouldBlock => GlommioError::WouldBlock(tuple.1),
            _ => tuple.0.into(),
        }
    }
}

impl<T> GlommioError<T> {
    pub(crate) fn queue_still_active(index: usize) -> GlommioError<T> {
        GlommioError::ExecutorError(ExecutorErrorKind::QueueError {
            index,
            kind: QueueErrorKind::StillActive,
        })
    }

    pub(crate) fn queue_not_found(index: usize) -> GlommioError<T> {
        GlommioError::ExecutorError(ExecutorErrorKind::QueueError {
            index,
            kind: QueueErrorKind::NotFound,
        })
    }
}

impl fmt::Display for QueueErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueueErrorKind::StillActive => f.write_str("still active"),
            QueueErrorKind::NotFound => f.write_str("not found"),
        }
    }
}

impl<T> fmt::Display for ResourceType<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fmt_str = match self {
            ResourceType::Semaphore { .. } => "Semaphore".to_string(),
            ResourceType::RwLock => "RwLock".to_string(),
            ResourceType::Channel(_) => "Channel".to_string(),
            ResourceType::File { .. } => "File".to_string(),
        };
        write!(f, "{}", &fmt_str)
    }
}

#[doc(hidden)]
/// This `Debug` implementation is required, otherwise we'd be required to
/// place a bound on the generic `T` in GlommioError. This causes the `Debug`
/// constraint to be forced onto users of the type, and it's an annoying burden
/// on the type. This gets around that.
impl<T> Debug for GlommioError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GlommioError::IoError(err) => write!(f, "{:?}", err),
            GlommioError::Closed(resource) | GlommioError::WouldBlock(resource) => match resource {
                ResourceType::Semaphore {
                    requested,
                    available,
                } => write!(
                    f,
                    "SemaphoreError {{ requested {}, available: {} }}",
                    requested, available
                ),
                ResourceType::RwLock => write!(f, "RwLockError {{ .. }}"),
                ResourceType::Channel(_) => write!(f, "ChannelError {{ .. }}"),
                ResourceType::File(msg) => write!(f, "File (\"{}\")", msg),
            },
            GlommioError::ExecutorError(ExecutorErrorKind::QueueError { index, kind }) => f
                .write_fmt(format_args!(
                    "QueueError {{ index: {}, kind: {:?} }}",
                    index, kind
                )),
            GlommioError::EnhancedIoError {
                source,
                op,
                path,
                fd,
            } => {
                write!(
                    f,
                    "EnhancedIoError {{ source: {:?}, op: {:?}, path: {:?}, fd: {:?} }}",
                    source, op, path, fd
                )
            }
            GlommioError::ReactorError(kind) => {
                let kind = match kind {
                    ReactorErrorKind::IncorrectSourceType => "IncorrectSourceType",
                };
                write!(f, "ReactorError {{ kind: '{}' }}", kind)
            }
        }
    }
}

impl<T> From<GlommioError<T>> for io::Error {
    fn from(err: GlommioError<T>) -> Self {
        let display_err = err.to_string();
        match err {
            GlommioError::IoError(io_err) => io_err,
            GlommioError::WouldBlock(resource) => io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("{} operation would block", resource),
            ),
            GlommioError::Closed(resource) => {
                io::Error::new(io::ErrorKind::BrokenPipe, format!("{} is closed", resource))
            }
            GlommioError::ExecutorError(ExecutorErrorKind::QueueError { index, kind }) => {
                match kind {
                    QueueErrorKind::StillActive => io::Error::new(
                        io::ErrorKind::Other,
                        format!("Queue #{} still active", index),
                    ),
                    QueueErrorKind::NotFound => io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("Queue #{} not found", index),
                    ),
                }
            }
            GlommioError::EnhancedIoError { source, .. } => {
                io::Error::new(source.kind(), display_err)
            }
            GlommioError::ReactorError(_) => {
                io::Error::new(io::ErrorKind::InvalidData, "Incorrect source type!")
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io;

    #[test]
    #[should_panic(expected = "Queue #0 is still active")]
    fn queue_still_active_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::queue_still_active(0));
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Queue #0 is not found")]
    fn queue_not_found_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::queue_not_found(0));
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "RwLock is closed")]
    fn rwlock_closed_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::Closed(ResourceType::RwLock));
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Semaphore is closed")]
    fn semaphore_closed_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::Closed(ResourceType::Semaphore {
            requested: 0,
            available: 0,
        }));
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Channel is closed")]
    fn channel_closed_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::Closed(ResourceType::Channel(())));
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "RwLock operation would block")]
    fn rwlock_wouldblock_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::WouldBlock(ResourceType::RwLock));
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Semaphore operation would block")]
    fn semaphore_wouldblock_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::WouldBlock(ResourceType::Semaphore {
            requested: 0,
            available: 0,
        }));
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Channel operation would block")]
    fn channel_wouldblock_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::WouldBlock(ResourceType::Channel(())));
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "File operation would block")]
    fn file_wouldblock_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::WouldBlock(ResourceType::File(
            "specific error message here".to_string(),
        )));
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    fn composite_error_from_into() {
        let err: GlommioError<()> =
            io::Error::new(io::ErrorKind::Other, "test other io-error").into();
        let _: io::Error = err.into();

        let err: GlommioError<()> =
            io::Error::new(io::ErrorKind::BrokenPipe, "some error msg").into();
        let _: io::Error = err.into();

        let channel_closed = GlommioError::Closed(ResourceType::Channel(()));
        let _: io::Error = channel_closed.into();

        let lock_closed = GlommioError::Closed(ResourceType::RwLock::<()>);
        let _: io::Error = lock_closed.into();

        let semaphore_closed = GlommioError::Closed(ResourceType::Semaphore::<()> {
            requested: 10,
            available: 0,
        });
        let _: io::Error = semaphore_closed.into();

        let channel_block = GlommioError::WouldBlock(ResourceType::Channel(()));
        let _: io::Error = channel_block.into();

        let lock_block = GlommioError::WouldBlock(ResourceType::RwLock::<()>);
        let _: io::Error = lock_block.into();

        let semaphore_block = GlommioError::WouldBlock(ResourceType::Semaphore::<()> {
            requested: 0,
            available: 0,
        });
        let _: io::Error = semaphore_block.into();
    }

    #[test]
    fn enhance_error() {
        let inner = io::Error::from_raw_os_error(9);
        let enhanced = GlommioError::EnhancedIoError::<()> {
            source: inner,
            op: "testing enhancer",
            path: None,
            fd: Some(32),
        };
        let s = format!("{}", enhanced);
        assert_eq!(
            s,
            "Bad file descriptor (os error 9), op: testing enhancer path: None with fd: Some(32)"
        );
    }

    fn convert_error() -> io::Result<()> {
        let inner = io::Error::from_raw_os_error(9);
        let enhanced = GlommioError::<()>::create_enhanced::<PathBuf>(
            inner,
            "testing enhancer",
            None,
            Some(32),
        );
        Err(enhanced)?;
        Ok(())
    }

    #[test]
    fn enhance_error_converted() {
        let io_error = convert_error().unwrap_err();
        let s = format!("{}", io_error.into_inner().unwrap());
        assert_eq!(
            s,
            "Bad file descriptor (os error 9), op: testing enhancer path: None with fd: Some(32)"
        );
    }
}
