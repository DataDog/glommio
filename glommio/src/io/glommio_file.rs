// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//

use crate::{
    io::sched::FileScheduler,
    reactor::Reactor,
    sys::{self, Statx},
    GlommioError, ResourceType,
};
use log::debug;
use std::{
    cell::{Ref, RefCell},
    convert::TryInto,
    io,
    os::unix::io::{AsRawFd, FromRawFd, RawFd},
    path::{Path, PathBuf},
    rc::{Rc, Weak},
    sync::{Arc, Weak as AWeak},
};

type Result<T> = crate::Result<T, ()>;

pub(super) type Device = u64;
pub(super) type Inode = u64;
pub(super) type Identity = (Device, Inode);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AdvisoryLockState {
    Unlocked = 0,
    Locking = 1,
    Locked = 2,
}

struct AdvisoryLockStateGuard<'a>(&'a AdvisoryLockStateHolder, bool);

impl AdvisoryLockStateGuard<'_> {
    fn commit(mut self) {
        self.1 = false;
        self.0.mark_locked();
    }
}

impl Drop for AdvisoryLockStateGuard<'_> {
    fn drop(&mut self) {
        if self.1 {
            self.0.abort_locking();
        }
    }
}

#[derive(Default)]
pub(crate) struct AdvisoryLockStateHolder {
    state: std::sync::atomic::AtomicU8,
}

impl std::fmt::Debug for AdvisoryLockStateHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{:?}", self.state(),))
    }
}

impl AdvisoryLockStateHolder {
    fn map_state(state: u8) -> AdvisoryLockState {
        match state {
            0 => AdvisoryLockState::Unlocked,
            1 => AdvisoryLockState::Locking,
            2 => AdvisoryLockState::Locked,
            rest => {
                panic!("Memory corruption resulting in unexpected state value? state = {rest:?}")
            }
        }
    }

    fn state(&self) -> AdvisoryLockState {
        Self::map_state(self.state.load(std::sync::atomic::Ordering::Relaxed))
    }

    fn mark_locking<'a>(&'a self, file: &Path) -> Result<AdvisoryLockStateGuard<'a>> {
        self.state
            .compare_exchange_weak(
                AdvisoryLockState::Unlocked as u8,
                AdvisoryLockState::Locking as u8,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            )
            .map(|_| AdvisoryLockStateGuard(self, true))
            .map_err(|state| match Self::map_state(state) {
                AdvisoryLockState::Unlocked => crate::GlommioError::WouldBlock(ResourceType::File(
                    format!("Failed to acquire lock but current state is unlocked on {file:?}?"),
                )),
                AdvisoryLockState::Locking => crate::GlommioError::WouldBlock(ResourceType::File(
                    format!("The lock is already trying to be acquired on {file:?}"),
                )),
                AdvisoryLockState::Locked => crate::GlommioError::WouldBlock(ResourceType::File(
                    format!("The lock is already acquired on {file:?}"),
                )),
            })
    }

    fn abort_locking(&self) {
        self.state
            .compare_exchange_weak(
                AdvisoryLockState::Locking as u8,
                AdvisoryLockState::Unlocked as u8,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            )
            .expect("Should only be called if the current state is locking");
    }

    fn mark_locked(&self) {
        self.state
            .compare_exchange_weak(
                AdvisoryLockState::Locking as u8,
                AdvisoryLockState::Locked as u8,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            )
            .expect("Should be in the locking state if we're making the lock as acquired!");
    }

    fn unlock(&self) {
        self.state
            .compare_exchange_weak(
                AdvisoryLockState::Locked as u8,
                AdvisoryLockState::Unlocked as u8,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            )
            .expect("Unlock called when in unexpected state!");
    }
}

/// A wrapper over `std::fs::File` which carries a path (for better error
/// messages) and prints a warning if closed synchronously.
///
/// It also implements some operations that are common among Buffered and
/// non-Buffered files
#[derive(Debug, Clone)]
pub(crate) struct GlommioFile {
    pub(crate) file: Option<Arc<RawFd>>,
    pub(crate) lock_state: Option<Arc<AdvisoryLockStateHolder>>,
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
        if let Some(file) = self.file.take().and_then(Arc::into_inner) {
            debug!(
                "File dropped while still active. ({:?} / fd {}).
That means that while the file is already out of scope, the file descriptor is still registered \
                 until the next I/O cycle.
This is likely fine, but in extreme situations can lead to resource exhaustion. An explicit \
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
        self.file.as_ref().unwrap().as_raw_fd()
    }
}

impl FromRawFd for GlommioFile {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        GlommioFile {
            file: Some(Arc::new(fd)),
            lock_state: Some(Arc::new(AdvisoryLockStateHolder::default())),
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
            file: Some(Arc::new(fd as _)),
            lock_state: Some(Arc::new(Default::default())),
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

    pub(crate) fn dup(&self) -> io::Result<Self> {
        let reactor = crate::executor().reactor();

        let duped = Self {
            file: Some(Arc::new(nix::unistd::dup(
                self.file.as_ref().unwrap().as_raw_fd(),
            )?)),
            lock_state: self.lock_state.clone(),
            path: self.path.clone(),
            inode: self.inode,
            dev_major: self.dev_major,
            dev_minor: self.dev_minor,
            reactor: Rc::downgrade(&reactor),
            scheduler: RefCell::new(None),
        };

        if self.scheduler.borrow().is_some() {
            duped.attach_scheduler();
        }

        Ok(duped)
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

    pub(crate) fn try_take_last_clone(mut self) -> std::result::Result<Self, Self> {
        match Arc::try_unwrap(self.file.take().unwrap()) {
            Ok(took) => {
                self.file = Some(Arc::new(took));
                Ok(self)
            }
            Err(failed) => {
                self.file = Some(failed);
                Err(self)
            }
        }
    }

    pub(crate) async fn try_take_last_clone_unlocking_guard(
        self,
        guard: OwnedGlommioFile,
    ) -> std::result::Result<Self, (Self, OwnedGlommioFile)> {
        if guard.as_raw_fd() == self.as_raw_fd() {
            std::mem::drop(guard);

            match self.try_take_last_clone() {
                Ok(took) => {
                    unsafe { took.funlock().await }.expect("Unlocking advisory lock failed");
                    Ok(took)
                }
                Err(failed) => {
                    let guard = failed.clone().into();
                    Err((failed, guard))
                }
            }
        } else {
            debug_assert!(Arc::ptr_eq(
                self.lock_state.as_ref().unwrap(),
                guard.lock_state.as_ref().unwrap()
            ));

            // Seperate file descriptors but the same file entry. Try taking the fd...
            match self.try_take_last_clone() {
                Ok(took) => {
                    let original: GlommioFile = guard.into();
                    unsafe { original.funlock().await }.expect("Unlocking advisory lock failed");
                    Ok(took)
                }
                Err(failed) => Err((failed, guard)),
            }
        }
    }

    pub(crate) fn discard(mut self) -> (Option<RawFd>, Option<PathBuf>) {
        // Destruct `self` signalling to `Drop` that there is no need to async close.
        // Similarly, because we're about to close, we don't need to do anything with the advisory lock - if this is
        // the last clone of the file, then it'll unlock implicitly by closing the fd. And if it's not (e.g. a dup
        // exists), then we can't safely unlock yet anyway.
        self.lock_state.take().unwrap();

        (Arc::into_inner(self.file.take().unwrap()), self.path.take())
    }

    pub(crate) async fn close(self) -> Result<()> {
        let reactor = self.reactor.upgrade().unwrap();
        // Destruct `self` into components skipping Drop.
        let (fd, path) = self.discard();
        if let Some(fd) = fd {
            let source = reactor.close(fd);
            source.collect_rw().await.map_err(|source| {
                GlommioError::create_enhanced(source, "Closing", path, Some(fd))
            })?;
            Ok(())
        } else {
            Err(GlommioError::CanNotBeClosed(
                ResourceType::File(
                    path.map_or("path has already been taken!".to_string(), |p| {
                        p.to_string_lossy().to_string()
                    }),
                ),
                "Another clone of this file exists somewhere - cannot close fd",
            ))
        }
    }

    async fn flock(&self, op: &'static str, flags: libc::c_int) -> Result<()> {
        let guard = self
            .lock_state
            .as_ref()
            .unwrap()
            .mark_locking(self.path.borrow().as_ref().unwrap())?;

        let res = unsafe { libc::flock(self.as_raw_fd(), flags) };
        if res == -1 {
            return Err(GlommioError::create_enhanced(
                std::io::Error::last_os_error(),
                op,
                self.path.borrow().as_ref(),
                Some(self.as_raw_fd()),
            ));
        }

        guard.commit();

        Ok(())
    }

    pub(crate) async fn try_lock_shared(&self) -> Result<OwnedGlommioFile> {
        // NOTE: The try variant could just do the syscall directly instead of dispatching to a blocking thread.
        // For consistency though it's implemented the same was.
        self.flock("try_lock_shared", libc::LOCK_SH | libc::LOCK_NB)
            .await
            .map(|()| self.clone().into())
    }

    pub(crate) async fn try_lock_exclusive(&self) -> Result<OwnedGlommioFile> {
        // NOTE: The try variant could just do the syscall directly instead of dispatching to a blocking thread.
        // For consistency though it's implemented the same way.
        self.flock("try_lock_exclusive", libc::LOCK_EX | libc::LOCK_NB)
            .await
            .map(|()| self.clone().into())
    }

    pub(crate) async unsafe fn funlock(&self) -> Result<()> {
        let lock_state = self.lock_state.as_ref().unwrap();
        assert_eq!(
            lock_state.state(),
            AdvisoryLockState::Locked,
            "Not holding the lock!"
        );

        let res = unsafe { libc::flock(self.as_raw_fd(), libc::LOCK_UN) };
        if res == -1 {
            return Err(GlommioError::create_enhanced(
                std::io::Error::last_os_error(),
                "funlock",
                self.path.borrow().as_ref(),
                Some(self.as_raw_fd()),
            ));
        }

        lock_state.unlock();

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

    pub(crate) async fn deallocate(&self, offset: u64, size: u64) -> Result<()> {
        let flags = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
        self.fallocate("Deallocate range", flags, offset, size)
            .await
    }

    pub(crate) async fn pre_allocate(&self, size: u64, keep_size: bool) -> Result<()> {
        let flags = if keep_size {
            libc::FALLOC_FL_KEEP_SIZE
        } else {
            0
        };
        self.fallocate("Pre-allocate space", flags, 0, size).await
    }

    async fn fallocate(&self, op: &'static str, flags: i32, offset: u64, size: u64) -> Result<()> {
        let source =
            self.reactor
                .upgrade()
                .unwrap()
                .fallocate(self.as_raw_fd(), offset, size, flags);
        source.collect_rw().await.map_err(|source| {
            GlommioError::create_enhanced(
                source,
                op,
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
        let path = self.path_required("remove")?.to_owned();
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
    pub(crate) async fn statx(&self) -> Result<Statx> {
        let source = self.reactor.upgrade().unwrap().statx(self.as_raw_fd());
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

    pub(crate) fn downgrade(&self) -> WeakGlommioFile {
        WeakGlommioFile {
            fd: self.file.as_ref().map_or(AWeak::new(), Arc::downgrade),
            lock_state: self
                .lock_state
                .as_ref()
                .map_or(AWeak::new(), Arc::downgrade),
            path: self.path.borrow().clone(),
            inode: self.inode,
            dev_major: self.dev_major,
            dev_minor: self.dev_minor,
        }
    }

    pub(crate) fn strong_count(&self) -> usize {
        self.file.as_ref().map_or(0, Arc::strong_count)
    }
}

/// This lets you open a DmaFile on one thread and then send it safely to another thread for processing.
#[derive(Debug, Clone)]
pub(crate) struct OwnedGlommioFile {
    pub(crate) fd: Option<Arc<RawFd>>,
    pub(crate) lock_state: Option<Arc<AdvisoryLockStateHolder>>,
    pub(crate) path: Option<PathBuf>,
    pub(crate) inode: u64,
    pub(crate) dev_major: u32,
    pub(crate) dev_minor: u32,
}

impl OwnedGlommioFile {
    pub(crate) fn dup(&self) -> io::Result<Self> {
        let fd = match self.fd.as_ref() {
            Some(fd) => Some(Arc::new(nix::unistd::dup(fd.as_raw_fd())?)),
            None => None,
        };

        Ok(Self {
            fd,
            lock_state: self.lock_state.clone(),
            path: self.path.clone(),
            inode: self.inode,
            dev_major: self.dev_major,
            dev_minor: self.dev_minor,
        })
    }

    pub(crate) fn strong_count(&self) -> usize {
        self.fd.as_ref().map_or(0, Arc::strong_count)
    }

    pub(crate) fn downgrade(&self) -> WeakGlommioFile {
        WeakGlommioFile {
            fd: self.fd.as_ref().map_or(AWeak::new(), Arc::downgrade),
            lock_state: self
                .lock_state
                .as_ref()
                .map_or(AWeak::new(), Arc::downgrade),
            path: self.path.clone(),
            inode: self.inode,
            dev_major: self.dev_major,
            dev_minor: self.dev_minor,
        }
    }

    pub(crate) unsafe fn funlock_immediately(&mut self) {
        let advisory_lock = self.lock_state.take().unwrap();
        assert_eq!(
            advisory_lock.state(),
            AdvisoryLockState::Locked,
            "Not holding the lock!"
        );

        nix::fcntl::flock(
            self.fd.as_ref().unwrap().as_raw_fd(),
            nix::fcntl::FlockArg::Unlock,
        )
        .unwrap();

        advisory_lock.unlock();
    }
}

impl AsRawFd for OwnedGlommioFile {
    fn as_raw_fd(&self) -> RawFd {
        self.fd.as_ref().unwrap().as_raw_fd()
    }
}

impl Drop for OwnedGlommioFile {
    fn drop(&mut self) {
        if let Some(fd) = self.fd.take().and_then(Arc::into_inner) {
            nix::unistd::close(fd).unwrap();
        }
    }
}

impl From<OwnedGlommioFile> for GlommioFile {
    fn from(mut owned: OwnedGlommioFile) -> Self {
        let reactor = crate::executor().reactor();

        GlommioFile {
            file: owned.fd.take(),
            lock_state: owned.lock_state.take(),
            path: RefCell::new(owned.path.take()),
            inode: owned.inode,
            dev_major: owned.dev_major,
            dev_minor: owned.dev_minor,
            reactor: Rc::downgrade(&reactor),
            scheduler: RefCell::new(None),
        }
    }
}

impl From<GlommioFile> for OwnedGlommioFile {
    fn from(mut value: GlommioFile) -> Self {
        Self {
            fd: value.file.take(),
            lock_state: value.lock_state.take(),
            path: value.path.borrow_mut().take().map(|p| p.to_path_buf()),
            inode: value.inode,
            dev_major: value.dev_major,
            dev_minor: value.dev_minor,
        }
    }
}

#[derive(Default, Debug, Clone)]
pub(crate) struct WeakGlommioFile {
    pub(crate) fd: AWeak<RawFd>,
    pub(crate) lock_state: AWeak<AdvisoryLockStateHolder>,
    pub(crate) path: Option<PathBuf>,
    pub(crate) inode: u64,
    pub(crate) dev_major: u32,
    pub(crate) dev_minor: u32,
}

impl WeakGlommioFile {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn strong_count(&self) -> usize {
        self.fd.strong_count()
    }

    pub(crate) fn upgrade(&self) -> Option<OwnedGlommioFile> {
        self.fd.upgrade().map(|fd| OwnedGlommioFile {
            fd: Some(fd),
            lock_state: Some(
                self.lock_state
                    .upgrade()
                    .expect("If the FD is live then so should the advisory lock state be"),
            ),
            path: self.path.clone(),
            inode: self.inode,
            dev_major: self.dev_major,
            dev_minor: self.dev_minor,
        })
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
                    if let Ok(file) = std::fs::canonicalize(f) {
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
