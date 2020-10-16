// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::error::ErrorEnhancer;
use crate::parking::Reactor;
use crate::sys;
use crate::sys::SourceType;
use std::io;
use std::mem;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::path::{Path, PathBuf};

/// A wrapper over `std::fs::File` which carries a path (for better error
/// messages) and prints a warning if closed synchronously.
///
/// It also implements some operations that are common among Buffered and non-Buffered files
#[derive(Debug)]
pub(crate) struct ScipioFile {
    pub(crate) file: std::fs::File,
    // A file can appear in many paths, through renaming and linking.
    // If we do that, each path should have its own object. This is to
    // facilitate error displaying.
    pub(crate) path: Option<PathBuf>,
}

impl Drop for ScipioFile {
    fn drop(&mut self) {
        eprintln!(
            "File dropped while still active. Should have been async closed ({:?} / fd {})
I will close it and turn a leak bug into a performance bug. Please investigate",
            self.path,
            self.as_raw_fd()
        );
    }
}

impl AsRawFd for ScipioFile {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

impl FromRawFd for ScipioFile {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        ScipioFile {
            file: std::fs::File::from_raw_fd(fd),
            path: None,
        }
    }
}

impl ScipioFile {
    pub(crate) async fn close(mut self) -> io::Result<()> {
        // Destruct `self` into components skipping Drop.
        let (fd, path) = {
            let fd = self.as_raw_fd();
            let path = self.path.take();
            mem::forget(self);
            (fd, path)
        };

        let source = Reactor::get().close(fd);
        enhanced_try!(source.collect_rw().await, "Closing", path, Some(fd))?;
        Ok(())
    }

    pub(crate) fn with_path(mut self, path: Option<PathBuf>) -> ScipioFile {
        self.path = path;
        self
    }

    pub(crate) fn path_required(&self, op: &'static str) -> io::Result<&Path> {
        self.path.as_deref().ok_or_else(|| {
            ErrorEnhancer {
                inner: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "operation requires a valid path",
                ),
                op,
                path: None,
                fd: Some(self.as_raw_fd()),
            }
            .into()
        })
    }

    pub(crate) async fn pre_allocate(&self, size: u64) -> io::Result<()> {
        let flags = libc::FALLOC_FL_ZERO_RANGE;
        let source = Reactor::get().fallocate(self.as_raw_fd(), 0, size, flags);
        enhanced_try!(source.collect_rw().await, "Pre-allocate space", self)?;
        Ok(())
    }

    pub(crate) async fn hint_extent_size(&self, size: usize) -> nix::Result<i32> {
        sys::fs_hint_extentsize(self.as_raw_fd(), size)
    }

    pub(crate) async fn truncate(&self, size: u64) -> io::Result<()> {
        enhanced_try!(
            sys::truncate_file(self.as_raw_fd(), size),
            "Truncating",
            self
        )
    }

    pub(crate) async fn rename<P: AsRef<Path>>(&mut self, new_path: P) -> io::Result<()> {
        let old_path = self.path_required("rename")?;
        enhanced_try!(
            crate::io::rename(old_path, &new_path).await,
            "Renaming",
            self
        )?;
        self.path = Some(new_path.as_ref().to_owned());
        Ok(())
    }

    pub(crate) async fn fdatasync(&self) -> io::Result<()> {
        let source = Reactor::get().fdatasync(self.as_raw_fd());
        enhanced_try!(source.collect_rw().await, "Syncing", self)?;
        Ok(())
    }

    pub(crate) async fn remove(&self) -> io::Result<()> {
        let path = self.path_required("remove")?;
        enhanced_try!(sys::remove_file(path), "Removing", self)
    }

    // Retrieve file metadata, backed by the statx(2) syscall
    pub(crate) async fn statx(&self) -> io::Result<libc::statx> {
        let path = self.path_required("stat")?;

        let source = Reactor::get().statx(self.as_raw_fd(), path);
        enhanced_try!(source.collect_rw().await, "getting file metadata", self)?;
        let stype = source.extract_source_type();
        let stat_buf = match stype {
            SourceType::Statx(_, buf) => buf,
            _ => panic!("Source type is wrong for describe operation"),
        };
        Ok(stat_buf.into_inner())
    }

    pub(crate) async fn file_size(&self) -> io::Result<u64> {
        let st = self.statx().await?;
        Ok(st.stx_size)
    }
}
