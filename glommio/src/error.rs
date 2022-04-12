// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use std::{
    fmt::{self, Debug},
    io,
    os::unix::io::RawFd,
    path::{Path, PathBuf},
    time::Duration,
};

/// Result type alias that all Glommio public API functions can use.
pub type Result<T, V> = std::result::Result<T, GlommioError<V>>;

/// Resource Type used for errors that `WouldBlock` and includes extra
/// diagnostic data for richer error messages.
#[derive(Debug)]
pub enum ResourceType<T> {
    /// Semaphore resource that includes the requested and available shares
    /// as debugging metadata for the [`Semaphore`](crate::sync::Semaphore)
    /// type.
    Semaphore {
        /// Requested shares
        requested: u64,
        /// Available semaphore shares
        available: u64,
    },

    /// Lock variant for reporting errors from the
    /// [`RwLock`](crate::sync::RwLock) type.
    RwLock,

    /// Channel variant for reporting errors from
    /// [`local_channel`](crate::channels::local_channel) and
    /// [`shared_channel`](crate::channels::shared_channel) channel types.
    Channel(T),

    /// File variant used for reporting errors for the Buffered
    /// ([`BufferedFile`](crate::io::BufferedFile)) and Direct
    /// ([`DmaFile`](crate::io::DmaFile)) file I/O variants.
    File(String),

    /// Gate variant used for reporting errors for the
    /// [`Gate`](crate::sync::Gate) type.
    Gate,
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReactorErrorKind {
    /// Indicates an incorrect source type.
    IncorrectSourceType(String),

    /// Reactor unable to lock memory (max allowed, min required)
    MemLockLimit(u64, u64),
}

impl fmt::Display for ReactorErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReactorErrorKind::IncorrectSourceType(x) => {
                write!(f, "Incorrect source type: {:?}!", x)
            }
            ReactorErrorKind::MemLockLimit(max, min) => write!(
                f,
                "The memlock resource limit is too low: {} (recommended {})",
                max, min
            ),
        }
    }
}

/// Error types that can be created by the executor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorErrorKind {
    /// Error variants for executor queues.
    QueueError {
        /// index of the queue
        index: usize,

        /// the kind of error encountered
        kind: QueueErrorKind,
    },
    /// The executor Id is invalid
    InvalidId(usize),
}

impl fmt::Display for ExecutorErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExecutorErrorKind::QueueError { index, kind } => {
                write!(f, "Queue #{} is {}", index, kind)
            }
            ExecutorErrorKind::InvalidId(x) => {
                write!(f, "indexing executor with id {}, which is invalid", x)
            }
        }
    }
}

/// Error types that can be created when building executors.
#[derive(Debug)]
pub enum BuilderErrorKind {
    /// Error type for specifying a CPU that doesn't exist
    NonExistentCpus {
        /// The CPU that doesn't exist
        cpu: usize,
    },
    /// Error type for using a [`Placement`](crate::PoolPlacement) that requires
    /// more CPUs than available.
    InsufficientCpus {
        /// The number of CPUs required for success.
        required: usize,

        /// The number of CPUs available.
        available: usize,
    },
    /// Error type for using [`Placement::Custom`](crate::PoolPlacement::Custom)
    /// with a number of [`CpuSet`](crate::CpuSet)s that does not match the
    /// number of shards requested.
    NrShards {
        /// The minimum number of shards that can be created
        minimum: usize,

        /// The number of shards requested.
        shards: usize,
    },
    /// Error type returned by
    /// [`PoolThreadHandles::join_all`](crate::PoolThreadHandles::join_all)
    /// for threads that panicked.  The contained error is forwarded from
    /// [`JoinHandle`](std::thread::JoinHandle).
    ThreadPanic(Box<dyn std::any::Any + Send>),
}

impl fmt::Display for BuilderErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonExistentCpus { cpu } => {
                write!(f, "CPU {} doesn't exist on the host", cpu)
            }
            Self::InsufficientCpus {
                available,
                required,
            } => write!(f, "found {} of {} required CPUs", available, required),
            Self::NrShards { minimum, shards } => write!(
                f,
                "requested {} shards but a minimum of {} is required",
                minimum, shards
            ),
            Self::ThreadPanic(_) => write!(f, "thread panicked"),
        }
    }
}

/// Composite error type to encompass all error types glommio produces.
///
/// Single error type that will be produced by any public Glommio API
/// functions. Contains a generic type that is only used for the [ `Channel`
/// ](ResourceType::Channel) variants. In other cases it can just be replaced
/// with the unit type `()` and ignored. The variants are broken up into a few
/// common categories such as [`Closed`](GlommioError::Closed) and
/// [`WouldBlock`](GlommioError::WouldBlock) as well as a generic
/// [`IoError`](GlommioError::IoError) and errors dedicated to the executor and
/// reactor.
///
/// # Examples
///
/// ```
/// use glommio::{GlommioError, ResourceType};
///
/// fn will_error() -> Result<(), GlommioError<()>> {
///     Err(GlommioError::WouldBlock(ResourceType::File(
///         "Error reading a file".to_string(),
///     )))?
/// }
/// assert!(will_error().is_err());
/// ```
pub enum GlommioError<T> {
    /// IO error from standard library functions or libraries that produce
    /// std::io::Error's.
    IoError(io::Error),

    /// Enhanced IO error that gives more information in the error message
    /// than the basic [`IoError`](GlommioError::IoError). It includes the
    /// operation, path and file descriptor. It also contains the error
    /// from the source IO error from `std::io::*`.
    EnhancedIoError {
        /// The source error from [`std::io::Error`].
        source: io::Error,

        /// The operation that was being attempted.
        op: &'static str,

        /// The path of the file, if relevant.
        path: Option<PathBuf>,

        /// The numeric file descriptor of the relevant resource.
        fd: Option<RawFd>,
    },

    /// Executor error variant(s) for signaling certain error conditions
    /// inside the executor.
    ExecutorError(ExecutorErrorKind),

    /// Error variant(s) produced when building executors.
    BuilderError(BuilderErrorKind),

    /// The resource in question is closed. Generic because the channel
    /// variant needs to return the actual item sent into the channel.
    Closed(ResourceType<T>),

    /// The resource can not be closed because certain conditions are not
    /// satisfied
    CanNotBeClosed(ResourceType<T>, &'static str),

    /// Error encapsulating the `WouldBlock` error for types that don't have
    /// errors originating in the standard library. Glommio also has
    /// nonblocking types that need to indicate if they are blocking or not.
    /// This type allows for signaling when a function would otherwise block
    /// for a specific `ResourceType`.
    WouldBlock(ResourceType<T>),

    /// Reactor error variants. This includes errors specific to the operation
    /// of the io-uring instances or related.
    ReactorError(ReactorErrorKind),

    /// Timeout variant used for reporting timed out operations
    TimedOut(Duration),
}

impl<T> From<io::Error> for GlommioError<T> {
    fn from(err: io::Error) -> Self {
        GlommioError::IoError(err)
    }
}

impl<T> fmt::Display for GlommioError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GlommioError::IoError(err) => write!(f, "IO error occurred: {}", err),
            GlommioError::EnhancedIoError {
                source,
                op,
                path,
                fd,
            } => write!(
                f,
                "{}, op: {} path: {:?} with fd: {:?}",
                source, op, path, fd
            ),
            GlommioError::ExecutorError(err) => write!(f, "Executor error: {}", err),
            GlommioError::BuilderError(err) => write!(f, "Executor builder error: {}", err),
            GlommioError::Closed(rt) => match rt {
                ResourceType::Semaphore {
                    requested,
                    available,
                } => write!(
                    f,
                    "Semaphore is closed (requested: {}, available: {})",
                    requested, available
                ),
                ResourceType::RwLock => write!(f, "RwLock is closed"),
                ResourceType::Channel(_) => write!(f, "Channel is closed"),
                // TODO: look at what this format string should be as per bug report..
                ResourceType::File(msg) => write!(f, "File is closed ({})", msg),
                ResourceType::Gate => write!(f, "Gate is closed"),
            },
            GlommioError::CanNotBeClosed(_, s) => write!(
                f,
                "Can not be closed because certain conditions are not satisfied. {}",
                *s
            ),
            GlommioError::WouldBlock(rt) => match rt {
                ResourceType::Semaphore {
                    requested,
                    available,
                } => write!(
                    f,
                    "Semaphore operation would block (requested: {}, available: {})",
                    requested, available
                ),
                ResourceType::RwLock => write!(f, "RwLock operation would block"),
                ResourceType::Channel(_) => write!(f, "Channel operation would block"),
                ResourceType::File(msg) => write!(f, "File operation would block ({})", msg),
                ResourceType::Gate => write!(f, "Gate operation would block"),
            },
            GlommioError::ReactorError(err) => write!(f, "Reactor error: {}", err),
            GlommioError::TimedOut(dur) => write!(f, "Operation timed out after {:#?}", dur),
        }
    }
}

impl<T> std::error::Error for GlommioError<T> {}

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
    /// Returns the OS error that this error represents (if any).
    ///
    /// If this `Error` was constructed from an [`io::Error`] which encapsulates
    /// a raw OS error, then this function will return [`Some`], otherwise
    /// it will return [`None`].
    ///
    /// [`io::Error`]: https://doc.rust-lang.org/std/io/struct.Error.html
    /// [`Some`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.Some
    /// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
    pub fn raw_os_error(&self) -> Option<i32> {
        match self {
            GlommioError::IoError(x) => x.raw_os_error(),
            GlommioError::EnhancedIoError {
                source,
                op: _,
                path: _,
                fd: _,
            } => source.raw_os_error(),
            _ => None,
        }
    }

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

/// Note this is a tricky impl in the sense that you will not get the
/// information you expect from just using this display impl on a value. On the
/// other hand the display impl for the entire error will give correct results.
impl<T> fmt::Display for ResourceType<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fmt_str = match self {
            ResourceType::Semaphore { .. } => "Semaphore".to_string(),
            ResourceType::RwLock => "RwLock".to_string(),
            ResourceType::Channel(_) => "Channel".to_string(),
            ResourceType::File(_) => "File".to_string(),
            ResourceType::Gate => "Gate".to_string(),
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
            GlommioError::Closed(resource) => match resource {
                ResourceType::Semaphore {
                    requested,
                    available,
                } => write!(
                    f,
                    "Semaphore is closed {{ requested {}, available: {} }}",
                    requested, available
                ),
                ResourceType::RwLock => write!(f, "RwLock is closed {{ .. }}"),
                ResourceType::Channel(_) => write!(f, "Channel is closed {{ .. }}"),
                ResourceType::File(msg) => write!(f, "File is closed (\"{}\")", msg),
                ResourceType::Gate => write!(f, "Gate is closed"),
            },
            GlommioError::CanNotBeClosed(resource, str) => match resource {
                ResourceType::RwLock => write!(f, "RwLock can not be closed (\"{}\")", *str),
                ResourceType::Channel(_) => write!(f, "Channel can not be closed (\"{}\")", *str),
                ResourceType::File(msg) => {
                    write!(f, "File can not be closed : (\"{}\"). (\"{}\")", *str, msg)
                }
                ResourceType::Gate => write!(f, "Gate can not be closed: {}", *str),
                ResourceType::Semaphore {
                    requested,
                    available,
                } => write!(
                    f,
                    "Semaphore can not be closed: {}. {{ requested {}, available: {} }}",
                    *str, requested, available
                ),
            },
            GlommioError::WouldBlock(resource) => match resource {
                ResourceType::Semaphore {
                    requested,
                    available,
                } => write!(
                    f,
                    "Semaphore operation would block {{ requested {}, available: {} }}",
                    requested, available
                ),
                ResourceType::RwLock => write!(f, "RwLock operation would block {{ .. }}"),
                ResourceType::Channel(_) => write!(f, "Channel operation  would block {{ .. }}"),
                ResourceType::File(msg) => write!(f, "File operation would block (\"{}\")", msg),
                ResourceType::Gate => write!(f, "Gate operation would block {{ .. }}"),
            },
            GlommioError::ExecutorError(kind) => match kind {
                ExecutorErrorKind::QueueError { index, kind } => f.write_fmt(format_args!(
                    "QueueError {{ index: {}, kind: {:?} }}",
                    index, kind
                )),
                ExecutorErrorKind::InvalidId(x) => {
                    f.write_fmt(format_args!("Invalid Executor ID {{ id: {} }}", x))
                }
            },
            GlommioError::BuilderError(kind) => match kind {
                BuilderErrorKind::NonExistentCpus { cpu } => {
                    f.write_fmt(format_args!("NonExistentCpus {{ cpu: {} }}", cpu))
                }
                BuilderErrorKind::InsufficientCpus {
                    required,
                    available,
                } => f.write_fmt(format_args!(
                    "InsufficientCpus {{ required: {}, available: {} }}",
                    required, available
                )),
                BuilderErrorKind::NrShards { minimum, shards } => f.write_fmt(format_args!(
                    "NrShards {{ minimum: {}, shards: {} }}",
                    minimum, shards
                )),
                BuilderErrorKind::ThreadPanic(_) => write!(f, "Thread panicked {{ .. }}"),
            },
            GlommioError::EnhancedIoError {
                source,
                op,
                path,
                fd,
            } => write!(
                f,
                "EnhancedIoError {{ source: {:?}, op: {:?}, path: {:?}, fd: {:?} }}",
                source, op, path, fd
            ),
            GlommioError::ReactorError(kind) => match kind {
                ReactorErrorKind::IncorrectSourceType(x) => {
                    write!(f, "ReactorError {{ kind: IncorrectSourceType {:?} }}", x)
                }
                ReactorErrorKind::MemLockLimit(a, b) => {
                    write!(f, "ReactorError {{ kind: MemLockLimit({:?}/{:?}) }}", a, b)
                }
            },
            GlommioError::TimedOut(dur) => write!(f, "TimedOut {{ dur {:?} }}", dur),
        }
    }
}

impl<T> From<GlommioError<T>> for io::Error {
    fn from(err: GlommioError<T>) -> Self {
        let display_err = err.to_string();
        match err {
            GlommioError::IoError(io_err) => io_err,
            GlommioError::WouldBlock(_) => io::Error::new(io::ErrorKind::WouldBlock, display_err),
            GlommioError::Closed(_) => io::Error::new(io::ErrorKind::BrokenPipe, display_err),
            GlommioError::CanNotBeClosed(_, _) => {
                io::Error::new(io::ErrorKind::BrokenPipe, display_err)
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
            GlommioError::ExecutorError(ExecutorErrorKind::InvalidId(id)) => io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid executor id {}", id),
            ),
            GlommioError::BuilderError(BuilderErrorKind::NonExistentCpus { .. })
            | GlommioError::BuilderError(BuilderErrorKind::InsufficientCpus { .. })
            | GlommioError::BuilderError(BuilderErrorKind::NrShards { .. })
            | GlommioError::BuilderError(BuilderErrorKind::ThreadPanic(_)) => io::Error::new(
                io::ErrorKind::Other,
                format!("Executor builder error: {}", display_err),
            ),
            GlommioError::EnhancedIoError { source, .. } => {
                io::Error::new(source.kind(), display_err)
            }
            GlommioError::ReactorError(e) => match e {
                ReactorErrorKind::IncorrectSourceType(x) => io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("IncorrectSourceType {:?}", x),
                ),
                ReactorErrorKind::MemLockLimit(a, b) => io::Error::new(
                    io::ErrorKind::Other,
                    format!("MemLockLimit({:?}/{:?})", a, b),
                ),
            },
            GlommioError::TimedOut(dur) => io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out after {:#?}", dur),
            ),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{io, panic::panic_any};

    use super::*;

    #[test]
    #[should_panic(
        expected = "File operation would block (\"Reading 100 bytes from position 90 would cross \
                    a buffer boundary (Buffer size 120)\")"
    )]
    fn extended_file_err_msg_unwrap() {
        let _: () = Err(GlommioError::<()>::WouldBlock(ResourceType::File(format!(
            "Reading {} bytes from position {} would cross a buffer boundary (Buffer size {})",
            100, 90, 120
        ))))
        .unwrap();
    }

    #[test]
    #[should_panic(
        expected = "File operation would block (Reading 100 bytes from position 90 would cross a \
                    buffer boundary (Buffer size 120))"
    )]
    fn extended_file_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::<()>::WouldBlock(ResourceType::File(format!(
            "Reading {} bytes from position {} would cross a buffer boundary (Buffer size {})",
            100, 90, 120
        ))));
        panic_any(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "File is closed (Some specific message here with value: 1001)")]
    fn extended_closed_file_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::<()>::Closed(ResourceType::File(format!(
            "Some specific message here with value: {}",
            1001
        ))));
        panic_any(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Queue #0 is still active")]
    fn queue_still_active_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::queue_still_active(0));
        panic_any(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Queue #0 is not found")]
    fn queue_not_found_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::queue_not_found(0));
        panic_any(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "RwLock is closed")]
    fn rwlock_closed_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::Closed(ResourceType::RwLock));
        panic_any(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Semaphore is closed")]
    fn semaphore_closed_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::Closed(ResourceType::Semaphore {
            requested: 0,
            available: 0,
        }));
        panic_any(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Channel is closed")]
    fn channel_closed_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::Closed(ResourceType::Channel(())));
        panic_any(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "RwLock operation would block")]
    fn rwlock_wouldblock_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::WouldBlock(ResourceType::RwLock));
        panic_any(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Semaphore operation would block")]
    fn semaphore_wouldblock_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::WouldBlock(ResourceType::Semaphore {
            requested: 0,
            available: 0,
        }));
        panic_any(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Channel operation would block")]
    fn channel_wouldblock_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::WouldBlock(ResourceType::Channel(())));
        panic_any(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "File operation would block (specific error message here)")]
    fn file_wouldblock_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::WouldBlock(ResourceType::File(
            "specific error message here".to_string(),
        )));
        panic_any(err.unwrap_err().to_string());
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
        Err(enhanced.into())
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
