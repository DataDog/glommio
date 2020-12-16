// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use std::os::unix::io::RawFd;
use std::path::PathBuf;
use std::{
    fmt::{self, Debug},
    io,
};
use thiserror::Error;

/// Result type alias that all Glommio public API functions can use.
pub type Result<T, V> = std::result::Result<T, GlommioError<V>>;

/// Resource Type used for errors that `WouldBlock` and includes extra
/// diagnostic data for richer error messages.
#[derive(Debug)]
pub enum ResourceType<T> {
    /// Semaphore resource that includes the requested and available shares
    /// as debugging metadata.
    Semaphore {
        /// Requested shares
        requested: u64,
        /// Available semaphore shares
        available: u64,
    },

    /// Lock type
    RwLock,

    /// Channel
    Channel(T),
}

impl<T> fmt::Display for ResourceType<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let fmt_str = match self {
            ResourceType::Semaphore {
                requested,
                available,
            } => format!(
                "Semaphore (requested {} but only {} available)",
                requested, available
            ),
            ResourceType::RwLock => "RwLock".to_string(),
            ResourceType::Channel(_) => "Channel".to_string(),
        };

        f.write_str(&fmt_str)
    }
}

#[derive(Error)]
/// Composite error type to encompass all error types glommio produces.
pub enum GlommioError<T> {
    /// IO error from standard library functions
    #[error("IO error occurred: {0}")]
    IoError(#[from] io::Error),

    /// Queue could not be found error variant
    #[error("Queue #{index} not found")]
    QueueNotFoundError {
        /// index of the queue in question
        index: usize,
    },

    /// Queue still active error variant
    #[error("Queue #{index} is still active")]
    QueueStillActiveError {
        /// index of the queue
        index: usize,
    },

    /// The resource in question is closed. Generic because the channel
    /// variant needs to return the actual item sent into the channel.
    #[error("{0} is closed")]
    Closed(ResourceType<T>),

    /// Error encapsulating the `WouldBlock` error for types that don't have
    /// errors originating in the standard library. Glommio also has
    /// nonblocking types that need to indicate if they are blocking or not.
    /// This type allows for signaling when a function would otherwise block
    /// for a specific `ResourceType`.
    #[error("{0} would block")]
    WouldBlock(ResourceType<T>),
}

#[doc(hidden)]
/// This `Debug` implementation is required, otherwise we'd be required to
/// place a bound on the generic `T` in GlommioError. This causes the `Debug`
/// constraint to be forced onto users of the type, and it's an annoying burden
/// on the type. This gets around that.
impl<T> Debug for GlommioError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GlommioError::IoError(err) => f.write_fmt(format_args!("{:?}", err)),
            GlommioError::QueueNotFoundError { index }
            | GlommioError::QueueStillActiveError { index } => {
                f.write_fmt(format_args!("Queue {{ index: {} }}", index))
            }
            GlommioError::Closed(resource) | GlommioError::WouldBlock(resource) => match resource {
                ResourceType::Semaphore {
                    requested,
                    available,
                } => f.write_fmt(format_args!(
                    "Semaphore {{ requested {}, available: {} }}",
                    requested, available
                )),
                ResourceType::RwLock => f.write_str("RwLock {{ .. }}"),
                ResourceType::Channel(_) => f.write_str("Channel {{ .. }}"),
            },
        }
    }
}

/// TODO: finish
impl<T> From<GlommioError<T>> for io::Error {
    fn from(err: GlommioError<T>) -> Self {
        match err {
            GlommioError::IoError(io_err) => io_err,
            GlommioError::QueueNotFoundError { index } => io::Error::new(
                io::ErrorKind::NotFound,
                format!("Queue #{} not found", index),
            ),
            GlommioError::QueueStillActiveError { index } => io::Error::new(
                io::ErrorKind::Other,
                format!("Queue #{} still active", index),
            ),
            GlommioError::WouldBlock(resource) => io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("{} would block", resource),
            ),
            GlommioError::Closed(resource) => {
                io::Error::new(io::ErrorKind::BrokenPipe, format!("{} is closed", resource))
            }
        }
    }
}

/// Augments an `io::Error` with more information about what was happening
/// and to which file when the error ocurred.
pub(crate) struct ErrorEnhancer {
    pub(crate) inner: io::Error,
    pub(crate) op: &'static str,
    pub(crate) path: Option<PathBuf>,
    pub(crate) fd: Option<RawFd>,
}

impl fmt::Debug for ErrorEnhancer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for ErrorEnhancer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, op: {}", self.inner, self.op)?;
        if let Some(path) = &self.path {
            write!(f, " path {}", path.display())?;
        }

        if let Some(fd) = self.fd {
            write!(f, " with fd {}", fd)?;
        }
        Ok(())
    }
}

impl From<ErrorEnhancer> for io::Error {
    fn from(err: ErrorEnhancer) -> io::Error {
        io::Error::new(err.inner.kind(), format!("{}", err))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io;

    #[test]
    #[should_panic(expected = "Queue #0 is still active")]
    fn queue_still_active_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::QueueStillActiveError { index: 0 });
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Queue #0 not found")]
    fn queue_not_found_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::QueueNotFoundError { index: 0 });
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "RwLock is closed")]
    fn rwlock_closed_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::Closed(ResourceType::RwLock));
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Semaphore (requested 0 but only 0 available) is closed")]
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
    #[should_panic(expected = "RwLock would block")]
    fn rwlock_wouldblock_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::WouldBlock(ResourceType::RwLock));
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Semaphore (requested 0 but only 0 available) would block")]
    fn semaphore_wouldblock_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::WouldBlock(ResourceType::Semaphore {
            requested: 0,
            available: 0,
        }));
        panic!(err.unwrap_err().to_string());
    }

    #[test]
    #[should_panic(expected = "Channel would block")]
    fn channel_wouldblock_err_msg() {
        let err: Result<(), ()> = Err(GlommioError::WouldBlock(ResourceType::Channel(())));
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
        let enhanced = ErrorEnhancer {
            inner,
            op: "testing enhancer",
            path: None,
            fd: Some(32),
        };
        let s = format!("{}", enhanced);
        assert_eq!(
            s,
            "Bad file descriptor (os error 9), op: testing enhancer with fd 32"
        );
    }

    fn convert_error() -> io::Result<()> {
        let inner = io::Error::from_raw_os_error(9);
        let enhanced = ErrorEnhancer {
            inner,
            op: "testing enhancer",
            path: None,
            fd: Some(32),
        };
        Err(enhanced)?;
        Ok(())
    }

    #[test]
    fn enhance_error_converted() {
        let io_error = convert_error().unwrap_err();
        let s = format!("{}", io_error.into_inner().unwrap());
        assert_eq!(
            s,
            "Bad file descriptor (os error 9), op: testing enhancer with fd 32"
        );
    }
}
