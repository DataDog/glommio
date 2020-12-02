// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::error::ErrorEnhancer;
use crate::parking::Reactor;
use crate::sys;
use crate::Local;
use log::debug;
use std::convert::TryInto;
use std::io;
use std::mem;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::rc::Rc;

/// A wrapper over `std::fs::File` which carries a path (for better error
/// messages) and prints a warning if closed synchronously.
///
/// It also implements some operations that are common among Buffered and non-Buffered files
#[derive(Debug)]
pub(crate) struct GlommioFile {
    pub(crate) file: Option<std::fs::File>,
    // A file can appear in many paths, through renaming and linking.
    // If we do that, each path should have its own object. This is to
    // facilitate error displaying.
    pub(crate) path: Option<PathBuf>,
    pub(crate) inode: u64,
    pub(crate) dev_major: u32,
    pub(crate) dev_minor: u32,
    pub(crate) reactor: Rc<Reactor>,
}

impl Drop for GlommioFile {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            let fd = file.as_raw_fd();
            debug!(
            "File dropped while still active. ({:?} / fd {}).
That means that while the file is already out of scope, the file descriptor is still registered until the next I/O cycle.
This is likely file, but in extreme situations can lead to resource exhaustion. An explicit asynchronous close is still preferred",
            self.path,
            fd);

            std::mem::forget(file);
            self.reactor.close(fd);
        }
    }
}

impl AsRawFd for GlommioFile {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_ref().unwrap().as_raw_fd()
    }
}

impl FromRawFd for GlommioFile {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        GlommioFile {
            file: Some(std::fs::File::from_raw_fd(fd)),
            path: None,
            inode: 0,
            dev_major: 0,
            dev_minor: 0,
            reactor: Local::get_reactor(),
        }
    }
}

impl GlommioFile {
    pub(crate) async fn open_at(
        dir: RawFd,
        path: &Path,
        flags: libc::c_int,
        mode: libc::c_int,
    ) -> io::Result<GlommioFile> {
        let reactor = Local::get_reactor();
        let path = if dir == -1 && path.is_relative() {
            let mut pbuf = std::fs::canonicalize(".")?;
            pbuf.push(path);
            pbuf
        } else {
            path.to_owned()
        };

        let source = reactor.open_at(dir, &path, flags, mode);
        let fd = source.collect_rw().await?;

        let mut file = GlommioFile {
            file: Some(unsafe { std::fs::File::from_raw_fd(fd as _) }),
            path: Some(path),
            inode: 0,
            dev_major: 0,
            dev_minor: 0,
            reactor,
        };

        let st = file.statx().await?;
        file.inode = st.stx_ino;
        file.dev_major = st.stx_dev_major;
        file.dev_minor = st.stx_dev_minor;
        Ok(file)
    }

    pub(crate) fn is_same(&self, other: &GlommioFile) -> bool {
        self.inode == other.inode
            && self.dev_major == other.dev_major
            && self.dev_minor == other.dev_minor
    }

    pub(crate) fn discard(mut self) -> (RawFd, Option<PathBuf>) {
        // Destruct `self` into components skipping Drop.
        let fd = self.as_raw_fd();
        let path = self.path.take();
        mem::forget(self);
        (fd, path)
    }

    pub(crate) async fn close(self) -> io::Result<()> {
        let reactor = self.reactor.clone();
        // Destruct `self` into components skipping Drop.
        let (fd, path) = self.discard();
        let source = reactor.close(fd);
        enhanced_try!(source.collect_rw().await, "Closing", path, Some(fd))?;
        Ok(())
    }

    pub(crate) fn with_path(mut self, path: Option<PathBuf>) -> GlommioFile {
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
        let source = self.reactor.fallocate(self.as_raw_fd(), 0, size, flags);
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
        let source = self.reactor.fdatasync(self.as_raw_fd());
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

        let source = self.reactor.statx(self.as_raw_fd(), path);
        enhanced_try!(source.collect_rw().await, "getting file metadata", self)?;
        let stype = source.extract_source_type();
        stype.try_into()
    }

    pub(crate) async fn file_size(&self) -> io::Result<u64> {
        let st = self.statx().await?;
        Ok(st.stx_size)
    }
}
