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
        std::io::Error::new(err.inner.kind(), format!("{}", err))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io;

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
