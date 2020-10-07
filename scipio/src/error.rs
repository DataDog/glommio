// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use std::fmt;
use std::os::unix::io::RawFd;
use std::path::PathBuf;

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
        std::io::Error::new(err.inner.kind(), format!("{}", err.inner))
    }
}
