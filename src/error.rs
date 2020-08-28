use std::fmt;
use std::os::unix::io::RawFd;
use std::path::PathBuf;

/// Augments an io::Error with more information about what was happening
/// and to which file when the error ocurred.
pub struct Error {
    pub(crate) inner: std::io::Error,
    pub(crate) op: &'static str,
    pub(crate) path: Option<PathBuf>,
    pub(crate) fd: Option<RawFd>,
}

impl Error {
    /// Returns the raw OS error, if there is one, associated with the inner io::Error
    pub fn raw_os_error(&self) -> Option<i32> {
        self.inner.raw_os_error()
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, op: {}", self.inner, self.op)?;
        if let Some(path) = &self.path.as_ref().and_then(|x| x.to_str()) {
            write!(f, " path {}", path)?;
        }

        if let Some(fd) = &self.fd {
            write!(f, " with fd {}", fd)?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.inner)
    }
}

impl From<Error> for std::io::Error {
    fn from(err: Error) -> std::io::Error {
        err.inner
    }
}
