// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//

use crate::{io::sched::FileScheduler, reactor::Reactor, sys, GlommioError};
use log::debug;
use std::{
    cell::{Ref, RefCell},
    convert::TryInto,
    io,
    os::unix::io::{AsRawFd, FromRawFd, RawFd},
    path::{Path, PathBuf},
    rc::{Rc, Weak},
};

type Result<T> = crate::Result<T, ()>;

pub(super) type Device = u64;
pub(super) type Inode = u64;
pub(super) type Identity = (Device, Inode);

/// A wrapper over `std::fs::File` which carries a path (for better error
/// messages) and prints a warning if closed synchronously.
///
/// It also implements some operations that are common among Buffered and
/// non-Buffered files
#[derive(Debug)]
pub(crate) struct GlommioFile {
    pub(crate) file: Option<RawFd>,
    // A file can appear in many paths, through renaming and linking.
    // If we do that, each path should have its own object. This is to
    // facilitate error displaying.
    pub(crate) path: RefCell<Option<PathBuf>>,
    pub(crate) inode: u64,
    pub(crate) dev_major: u32,
    pub(crate) dev_minor: u32,
    pub(crate) reactor: Weak<Reactor>,
    pub(crate) scheduler: RefCell<Option<FileScheduler>>,
}

impl Drop for GlommioFile {
    fn drop(&mut self) {
        if let Some(file) = self.file.take() {
            debug!(
                "File dropped while still active. ({:?} / fd {}).
That means that while the file is already out of scope, the file descriptor is still registered \
                 until the next I/O cycle.
This is likely file, but in extreme situations can lead to resource exhaustion. An explicit \
                 asynchronous close is still preferred",
                self.path.borrow(),
                file
            );
            if let Some(r) = self.reactor.upgrade() {
                r.sys.async_close(file);
            }
        }
    }
}

impl AsRawFd for GlommioFile {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_ref().copied().unwrap()
    }
}

impl FromRawFd for GlommioFile {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        GlommioFile {
            file: Some(fd),
            path: RefCell::new(None),
            inode: 0,
            dev_major: 0,
            dev_minor: 0,
            reactor: Rc::downgrade(&crate::executor().reactor()),
            scheduler: RefCell::new(None),
        }
    }
}

impl GlommioFile {
    pub(crate) async fn open_at(
        dir: RawFd,
        path: &Path,
        flags: libc::c_int,
        mode: libc::mode_t,
    ) -> io::Result<GlommioFile> {
        let reactor = crate::executor().reactor();
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
            file: Some(fd as _),
            path: RefCell::new(Some(path)),
            inode: 0,
            dev_major: 0,
            dev_minor: 0,
            reactor: Rc::downgrade(&reactor),
            scheduler: RefCell::new(None),
        };

        let st = file.statx().await?;
        file.inode = st.stx_ino;
        file.dev_major = st.stx_dev_major;
        file.dev_minor = st.stx_dev_minor;
        Ok(file)
    }

    pub(super) fn attach_scheduler(&self) {
        if self.scheduler.borrow().is_none() {
            self.scheduler.replace(Some(
                crate::executor()
                    .reactor()
                    .io_scheduler()
                    .get_file_scheduler(self.identity()),
            ));
        }
    }

    pub(crate) fn identity(&self) -> Identity {
        (
            (self.dev_major as u64) << 32 | self.dev_minor as u64,
            self.inode,
        )
    }

    pub(crate) fn is_same(&self, other: &GlommioFile) -> bool {
        self.identity() == other.identity()
    }

    pub(crate) fn discard(mut self) -> (RawFd, Option<PathBuf>) {
        // Destruct `self` signalling to `Drop` that there is no need to async close
        let fd = self.file.take().unwrap();
        let path = self.path.take();
        (fd, path)
    }

    pub(crate) async fn close(self) -> Result<()> {
        let reactor = self.reactor.upgrade().unwrap();
        // Destruct `self` into components skipping Drop.
        let (fd, path) = self.discard();
        let source = reactor.close(fd);
        source
            .collect_rw()
            .await
            .map_err(|source| GlommioError::create_enhanced(source, "Closing", path, Some(fd)))?;
        Ok(())
    }

    pub(crate) fn with_path(self, path: Option<PathBuf>) -> GlommioFile {
        self.path.replace(path);
        self
    }

    pub(crate) fn path_required(&self, op: &'static str) -> Result<Ref<'_, Path>> {
        match *self.path.borrow() {
            None => Err(GlommioError::EnhancedIoError {
                op,
                path: None,
                fd: Some(self.as_raw_fd()),
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "operation requires a valid path",
                ),
            }),
            Some(_) => Ok(Ref::map(self.path.borrow(), |p| {
                p.as_ref().unwrap().as_path()
            })),
        }
    }

    pub(crate) fn path(&self) -> Option<Ref<'_, Path>> {
        self.path
            .borrow()
            .as_deref()
            .map(|_| Ref::map(self.path.borrow(), |p| p.as_ref().unwrap().as_path()))
    }

    pub(crate) async fn pre_allocate(&self, size: u64) -> Result<()> {
        let flags = libc::FALLOC_FL_ZERO_RANGE;
        let source = self
            .reactor
            .upgrade()
            .unwrap()
            .fallocate(self.as_raw_fd(), 0, size, flags);
        source.collect_rw().await.map_err(|source| {
            GlommioError::create_enhanced(
                source,
                "Pre-allocate space",
                self.path.borrow().as_ref(),
                Some(self.as_raw_fd()),
            )
        })?;
        Ok(())
    }

    pub(crate) async fn hint_extent_size(&self, size: usize) -> Result<i32> {
        match sys::fs_hint_extentsize(self.as_raw_fd(), size) {
            Ok(hint) => Ok(hint),
            Err(err) => Err(io::Error::from_raw_os_error(err as _).into()),
        }
    }

    pub(crate) async fn truncate(&self, size: u64) -> Result<()> {
        let source = self
            .reactor
            .upgrade()
            .unwrap()
            .truncate(self.as_raw_fd(), size)
            .await;

        source.collect_rw().await.map_err(|source| {
            GlommioError::create_enhanced(
                source,
                "Truncating",
                self.path.borrow().as_ref(),
                Some(self.as_raw_fd()),
            )
        })?;
        Ok(())
    }

    pub(crate) async fn rename<P: AsRef<Path>>(&self, new_path: P) -> Result<()> {
        let old_path = self.path_required("rename")?.to_path_buf();
        crate::io::rename(old_path, &new_path).await?;
        self.path.replace(Some(new_path.as_ref().to_owned()));
        Ok(())
    }

    pub(crate) async fn fdatasync(&self) -> Result<()> {
        let source = self.reactor.upgrade().unwrap().fdatasync(self.as_raw_fd());
        source.collect_rw().await.map_err(|source| {
            GlommioError::create_enhanced(
                source,
                "Syncing",
                self.path.borrow().as_ref(),
                Some(self.as_raw_fd()),
            )
        })?;
        Ok(())
    }

    pub(crate) async fn remove(&self) -> Result<()> {
        let path = self.path_required("remove")?;
        let source = self.reactor.upgrade().unwrap().remove_file(&*path).await;

        source.collect_rw().await.map_err(|source| {
            GlommioError::create_enhanced(
                source,
                "Removing",
                self.path.borrow().as_ref(),
                Some(self.as_raw_fd()),
            )
        })?;
        Ok(())
    }

    // Retrieve file metadata, backed by the statx(2) syscall
    pub(crate) async fn statx(&self) -> Result<libc::statx> {
        let path = self.path_required("stat")?;

        let source = self
            .reactor
            .upgrade()
            .unwrap()
            .statx(self.as_raw_fd(), path.as_ref());
        source.collect_rw().await.map_err(|source| {
            GlommioError::create_enhanced(
                source,
                "getting file metadata",
                self.path.borrow().as_ref(),
                Some(self.as_raw_fd()),
            )
        })?;
        let stype = source.extract_source_type();
        stype.try_into()
    }

    pub(crate) async fn file_size(&self) -> Result<u64> {
        let st = self.statx().await?;
        Ok(st.stx_size)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::{test_utils::*, timer::sleep};
    use std::time::Duration;

    #[test]
    fn drop_closes_the_file() {
        test_executor!(async move {
            let dir = make_tmp_test_directory("drop_closes_the_file");
            let path = dir.path.clone();

            let file = path.join("file");
            let gf = GlommioFile::open_at(-1, &file, libc::O_CREAT, 644)
                .await
                .unwrap();
            let gf_fd = gf.path.borrow().as_ref().cloned().unwrap();

            let file_list = || {
                let mut files = vec![];
                for f in std::fs::read_dir("/proc/self/fd").unwrap() {
                    let f = f.unwrap().path();
                    if let Ok(file) = std::fs::canonicalize(&f) {
                        files.push(file);
                    }
                }
                files
            };

            assert!(file_list().iter().any(|x| *x == gf_fd)); // sanity check that file is open
            let _ = { gf }; // moves scope and drops
            sleep(Duration::from_millis(10)).await; // forces the reactor to run, which will drop the file
            assert!(!file_list().iter().any(|x| *x == gf_fd)); // file is gone
        });
    }
}
