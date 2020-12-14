// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::{channels, executor, sync};
use std::fmt::{self, Debug};
use std::os::unix::io::RawFd;
use std::path::PathBuf;
use thiserror::Error;

/// Result type alias that all Glommio public API functions can use.
pub type Result<T, V> = std::result::Result<T, GlommioError<V>>;

/// Resource Type used for errors that `WouldBlock` and includes extra
/// diagnostic data for richer error messages.
#[derive(Debug)]
pub enum ResourceType {
    /// Semaphore resource that includes the requested and available shares
    /// as debugging metadata.
    Semaphore {
        /// Requested shares
        requested: u64,
        /// Available semaphore shares
        available: u64,
    },
}

impl fmt::Display for ResourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResourceType::Semaphore {
                requested,
                available,
            } => f.write_str(&format!(
                "Semaphore requested {} but only {} available",
                requested, available
            )),
        }
    }
}

#[derive(Error, Debug)]
/// Composite error type to encompass all error types glommio produces.
pub enum GlommioError<T: Debug + 'static> {
    /// IO error from standard library functions
    #[error("IO error occurred: {0}")]
    IoError(#[from] std::io::Error),

    /// Error indicating the lock is closed
    #[error("Lock closed error: {0}")]
    LockClosedError(#[from] sync::LockClosedError),

    /// Error encapsulating a channel error
    #[error("Channel error: {0}")]
    ChannelError(#[from] channels::ChannelError<T>),

    /// Queue could not be found error variant
    #[error("Queue not found error: {0}")]
    QueueNotFoundError(#[from] executor::QueueNotFoundError),

    /// Queue still active error variant
    #[error("Queue still active error: {0}")]
    QueueStillActiveError(#[from] executor::QueueStillActiveError),

    /// try_lock error variant
    #[error("Try lock error: {0}")]
    TryLockError(#[from] sync::TryLockError),

    /// Error variant that indicates that the semaphore is closed
    #[error("Semaphore closed")]
    SemaphoreClosed,

    /// Error encapsulating the `WouldBlock` error for types that don't have
    /// errors originating in the standard library. Glommio also has
    /// nonblocking types that need to indicate if they are blocking or not.
    /// This type allows for signaling when a function would otherwise block
    /// for a specific `ResourceType`.
    #[error("Resource would block. {0}")]
    WouldBlock(ResourceType),
}

/// Augments an `io::Error` with more information about what was happening
/// and to which file when the error ocurred.
pub(crate) struct ErrorEnhancer {
    pub(crate) inner: std::io::Error,
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

impl From<ErrorEnhancer> for std::io::Error {
    fn from(err: ErrorEnhancer) -> std::io::Error {
        std::io::Error::new(err.inner.kind(), format!("{}", err))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io;

    #[test]
    fn composite_error_from_into() {
        let _: GlommioError<()> =
            std::io::Error::new(io::ErrorKind::Other, "test other io-error").into();
        let _: GlommioError<()> = sync::LockClosedError {}.into();
        let _: GlommioError<()> = channels::ChannelError::new(io::ErrorKind::NotFound, ()).into();
        let _: GlommioError<()> = executor::QueueNotFoundError { index: 0 }.into();
        let _: GlommioError<()> = executor::QueueStillActiveError { index: 0 }.into();
        let _: GlommioError<()> = sync::TryLockError::Closed(sync::LockClosedError).into();
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
