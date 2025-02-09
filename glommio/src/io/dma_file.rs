// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::{
    io::{
        bulk_io::{
            CoalescedReads, IoVec, MergedBufferLimit, OrderedBulkIo, ReadAmplificationLimit,
            ReadManyArgs, ReadManyResult,
        },
        glommio_file::GlommioFile,
        open_options::OpenOptions,
        read_result::ReadResult,
        ScheduledSource,
    },
    sys::{self, sysfs, DirectIo, DmaBuffer, DmaSource, PollableStatus},
};
use futures_lite::{Stream, StreamExt};
use nix::sys::statfs::*;
use std::{
    cell::Ref,
    io,
    os::unix::io::{AsRawFd, RawFd},
    path::Path,
    rc::Rc,
    sync::{Arc, Weak as AWeak},
};

use super::{
    glommio_file::{OwnedGlommioFile, WeakGlommioFile},
    Stat,
};

pub(super) type Result<T> = crate::Result<T, ()>;

/// Close result of [`DmaFile::close_rc()`] and [`DmaFile::close`]. Indicates which operation is
/// performed on close.
#[derive(Debug)]
pub enum CloseResult {
    /// The file is closed.
    Closed,
    /// There are other references existing, only removed this one.
    Unreferenced,
}

pub(crate) fn align_up(v: u64, align: u64) -> u64 {
    (v + align - 1) & !(align - 1)
}

pub(crate) fn align_down(v: u64, align: u64) -> u64 {
    v & !(align - 1)
}

/// This guard represents that an advisory lock is being held over some file. See for more information about how
/// advisory locks work. This is obtained through [DmaFile::try_lock_exclusive] / [DmaFile::try_lock_shared]. If not
/// explicitly [unlocked](Self::unlock), the advisory lock is released synchronously on Drop.
#[derive(Debug, Clone)]
pub struct AdvisoryLockGuard(Option<Arc<OwnedGlommioFile>>);

impl Drop for AdvisoryLockGuard {
    fn drop(&mut self) {
        if let Some(mut locked) = self.0.take().and_then(Arc::into_inner) {
            unsafe { locked.funlock_immediately() }
        }
    }
}

impl AdvisoryLockGuard {
    /// Explicitly releases the lock. The file can be successfully closed after calling this.
    pub async fn unlock(mut self) -> Result<()> {
        let inner = self.0.take().unwrap();

        match Arc::try_unwrap(inner).map(GlommioFile::from) {
            Ok(lock) => unsafe { lock.funlock() }.await,
            Err(still_locked) => Err(crate::GlommioError::CanNotBeClosed(
                crate::ResourceType::File(
                    still_locked
                        .path
                        .as_ref()
                        .map_or("path has already been taken!".to_string(), |p| {
                            p.to_string_lossy().to_string()
                        }),
                ),
                "Another clone of this file exists somewhere - cannot close fd",
            )),
        }
    }

    /// Downgrade the lock guard to a weak reference that won't keep the file open. The lock will be released when the
    /// the last strong reference is dropped.
    pub fn downgrade(&self) -> WeakAdvisoryLockGuard {
        WeakAdvisoryLockGuard(Arc::downgrade(self.0.as_ref().unwrap()))
    }

    /// Like [DmaFile::try_take_last_clone], this returns `Ok(self)` if and only if it's the last clone of the lock
    /// guard in the program (not including weak references). `Err(self)` is returned if other instances exist.
    pub fn try_take_last_clone(mut self) -> std::result::Result<Self, Self> {
        match Arc::try_unwrap(self.0.take().unwrap()) {
            Ok(unique) => Ok(Self(Some(Arc::new(unique)))),
            Err(shared) => Err(Self(Some(shared))),
        }
    }
}

/// This holds a weak reference to an advisory lock. The advisory lock may be dropped while this reference is held
/// in which case this instance will fail to [upgrade](Self::upgrade).
#[derive(Default, Debug, Clone)]
pub struct WeakAdvisoryLockGuard(AWeak<OwnedGlommioFile>);

impl WeakAdvisoryLockGuard {
    /// Returns a weak lock guard not pointing at anything.
    pub fn new() -> Self {
        Default::default()
    }

    /// Upgrades a weak handle to a held advisory lock back to a strong handle. Returns None if the advisory lock was
    /// dropped.
    pub fn upgrade(&self) -> Option<AdvisoryLockGuard> {
        self.0.upgrade().map(|guard| AdvisoryLockGuard(Some(guard)))
    }
}

#[derive(Debug, Clone)]
/// An asynchronously accessed Direct Memory Access (DMA) file.
///
/// All access uses Direct I/O, and all operations including open and close are
/// asynchronous (with some exceptions noted). Reads from and writes to this
/// struct must come and go through the [`DmaBuffer`] type, which will buffer
/// them in memory; on calling [`DmaFile::write_at`] and [`DmaFile::read_at`],
/// the buffers will be passed to the OS to asynchronously write directly to the
/// file on disk, bypassing page caches.
///
/// See the module-level [documentation](index.html) for more details and
/// examples.
pub struct DmaFile {
    file: GlommioFile,
    o_direct_alignment: u64,
    max_sectors_size: usize,
    max_segment_size: usize,
    pollable: PollableStatus,
}

impl DmaFile {
    /// align a value up to the minimum alignment needed to access this file
    pub fn align_up(&self, v: u64) -> u64 {
        align_up(v, self.o_direct_alignment)
    }

    /// align a value down to the minimum alignment needed to access this file
    pub fn align_down(&self, v: u64) -> u64 {
        align_down(v, self.o_direct_alignment)
    }
}

impl AsRawFd for DmaFile {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

impl DmaFile {
    /// Returns true if the DmaFiles represent the same file on the underlying
    /// device.
    ///
    /// Files are considered to be the same if they live in the same file system
    /// and have the same Linux inode. Note that based on this rule a
    /// symlink is *not* considered to be the same file.
    ///
    /// Files will be considered to be the same if:
    /// * A file is opened multiple times (different file descriptors, but same
    ///   file!)
    /// * they are hard links.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{io::DmaFile, LocalExecutor};
    /// use std::os::unix::io::AsRawFd;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let mut wfile = DmaFile::create("myfile.txt").await.unwrap();
    ///     let mut rfile = DmaFile::open("myfile.txt").await.unwrap();
    ///     // Different objects (OS file descriptors), so they will be different...
    ///     assert_ne!(wfile.as_raw_fd(), rfile.as_raw_fd());
    ///     // However they represent the same object.
    ///     assert!(wfile.is_same(&rfile));
    ///     wfile.close().await;
    ///     rfile.close().await;
    /// });
    /// ```
    pub fn is_same(&self, other: &DmaFile) -> bool {
        self.file.is_same(&other.file)
    }

    async fn open_at(
        dir: RawFd,
        path: &Path,
        flags: libc::c_int,
        mode: libc::mode_t,
    ) -> io::Result<DmaFile> {
        let file = GlommioFile::open_at(dir, path, flags, mode).await?;
        let (major, minor) = (file.dev_major as usize, file.dev_minor as usize);
        let buf = statfs(path).unwrap();
        let fstype = buf.filesystem_type();
        let max_sectors_size = sysfs::BlockDevice::max_sectors_size(major, minor);
        let max_segment_size = sysfs::BlockDevice::max_segment_size(major, minor);
        // make sure the alignment is at least 512 in any case
        let o_direct_alignment =
            sysfs::BlockDevice::logical_block_size(major, minor).max(512) as u64;

        let pollable = if (fstype.0 as u64) == (libc::TMPFS_MAGIC as u64) {
            PollableStatus::NonPollable(DirectIo::Disabled)
        } else {
            // Allow this to work on non direct I/O devices, but only if this is in-memory
            sys::direct_io_ify(file.as_raw_fd(), flags)?;
            let reactor = file.reactor.upgrade().unwrap();
            if reactor
                .probe_iopoll_support(file.as_raw_fd(), o_direct_alignment, major, minor, path)
                .await
            {
                PollableStatus::Pollable
            } else {
                PollableStatus::NonPollable(DirectIo::Enabled)
            }
        };

        Ok(DmaFile {
            file,
            o_direct_alignment,
            max_sectors_size,
            max_segment_size,
            pollable,
        })
    }

    pub(super) async fn open_with_options<'a>(
        dir: RawFd,
        path: &'a Path,
        opdesc: &'static str,
        opts: &'a OpenOptions,
    ) -> Result<DmaFile> {
        let flags = libc::O_CLOEXEC
            | opts.get_access_mode()?
            | opts.get_creation_mode()?
            | (opts.custom_flags as libc::c_int & !libc::O_ACCMODE);

        let res = DmaFile::open_at(dir, path, flags, opts.mode).await;
        Ok(enhanced_try!(res, opdesc, Some(path), None)?)
    }

    pub(super) fn attach_scheduler(&self) {
        self.file.attach_scheduler()
    }

    /// Allocates a buffer that is suitable for using to write to this file.
    pub fn alloc_dma_buffer(&self, size: usize) -> DmaBuffer {
        self.file.reactor.upgrade().unwrap().alloc_dma_buffer(size)
    }

    /// Similar to `create()` in the standard library, but returns a DMA file
    pub async fn create<P: AsRef<Path>>(path: P) -> Result<DmaFile> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .dma_open(path.as_ref())
            .await
    }

    /// Similar to `open()` in the standard library, but returns a DMA file
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<DmaFile> {
        OpenOptions::new().read(true).dma_open(path.as_ref()).await
    }

    /// Creates a duplicate instance pointing to the same file descriptor as self.
    ///
    /// <p style="background:rgba(255,181,77,0.16);padding:0.75em;">
    /// <strong>Warning:</strong> If the file has been opened with `append`,
    /// then the position for writes will get ignored and the buffer will be written at
    /// the current end of file. See the [man page] for `O_APPEND`. All dup'ed files
    /// will share the same offset (i.e. writes to one will affect the other).
    /// </p>
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{
    ///   LocalExecutor,
    ///   io::{
    ///     OpenOptions,
    ///     DmaBuffer,
    ///   }
    /// };
    ///
    /// fn populate(buf: &mut DmaBuffer) {
    ///     buf.as_bytes_mut()[0..5].copy_from_slice(b"hello");
    /// }
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     // A new anonymous file is created within `some_directory/`.
    ///     let file = OpenOptions::new()
    ///       .create_new(true)
    ///       .read(true)
    ///       .write(true)
    ///       .tmpfile(true)
    ///       .dma_open("some_directory")
    ///       .await
    ///       .unwrap();
    ///
    ///     let file2 = file.dup().unwrap();
    ///
    ///     let mut buf = file.alloc_dma_buffer(4096);
    ///     // Write some data into the buffer.
    ///     populate(&mut buf);
    ///
    ///     let written = file.write_at(buf, 0).await.unwrap();
    ///     assert_eq!(written, 4096);
    ///     file.close().await.unwrap();
    ///
    ///     let read = file2.read_at_aligned(0, 4096).await.unwrap();
    ///     assert_eq!(read.len(), 4096);
    ///     assert_eq!(&read[0..6], b"hello\0");
    /// });
    /// ```
    pub fn dup(&self) -> Result<Self> {
        Ok(Self {
            file: enhanced_try!(self.file.dup(), "Duplicating", self.file)?,
            o_direct_alignment: self.o_direct_alignment,
            max_sectors_size: self.max_sectors_size,
            max_segment_size: self.max_segment_size,
            pollable: self.pollable,
        })
    }

    /// Write the buffer in `buf` to a specific position in the file.
    ///
    /// It is expected that the buffer and the position be properly aligned
    /// for Direct I/O. In most platforms that means 4096 bytes. There is no
    /// write_at_aligned, since a nonaligned write would require a
    /// read-modify-write.
    ///
    /// Buffers should be allocated through [`alloc_dma_buffer`], which
    /// guarantees proper alignment, but alignment on position is still up
    /// to the user.
    ///
    /// This method acquires ownership of the buffer so the buffer can be kept
    /// alive while the kernel has it.
    ///
    /// Note that it is legal to return fewer bytes than the buffer size. That
    /// is the situation, for example, when the device runs out of space
    /// (See the man page for write(2) for details)
    ///
    /// <p style="background:rgba(255,181,77,0.16);padding:0.75em;">
    /// <strong>Warning:</strong> If the file has been opened with `append`,
    /// then the position will get ignored and the buffer will be written at
    /// the current end of file. See the [man page] for `O_APPEND`
    /// </p>
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{io::DmaFile, LocalExecutor};
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::create("test.txt").await.unwrap();
    ///
    ///     let mut buf = file.alloc_dma_buffer(4096);
    ///     let res = file.write_at(buf, 0).await.unwrap();
    ///     assert!(res <= 4096);
    ///     file.close().await.unwrap();
    /// });
    /// ```
    ///
    /// [`alloc_dma_buffer`]: struct.DmaFile.html#method.alloc_dma_buffer
    /// [man page]: https://man7.org/linux/man-pages/man2/open.2.html
    pub async fn write_at(&self, buf: DmaBuffer, pos: u64) -> Result<usize> {
        let source = self.file.reactor.upgrade().unwrap().write_dma(
            self.as_raw_fd(),
            DmaSource::Owned(buf),
            pos,
            self.pollable,
        );
        enhanced_try!(source.collect_rw().await, "Writing", self.file).map_err(Into::into)
    }

    /// Equivalent to [`DmaFile::write_at`] except that the caller retains
    /// non-mutable ownership of the underlying buffer. This can be useful if
    /// you want to asynchronously process a page concurrently with writing it.
    ///
    /// # Examples
    /// ```no_run
    /// use futures::join;
    /// use glommio::{
    ///     io::{DmaBuffer, DmaFile},
    ///     timer::sleep,
    ///     LocalExecutor,
    /// };
    /// use std::rc::Rc;
    ///
    /// fn populate(buf: &mut DmaBuffer) {
    ///     buf.as_bytes_mut()[0..5].copy_from_slice(b"hello");
    /// }
    ///
    /// async fn transform(buf: &[u8]) -> Vec<u8> {
    ///     // Dummy implementation that just returns a copy of what was written.
    ///     sleep(std::time::Duration::from_millis(100)).await;
    ///     buf.iter()
    ///         .map(|a| if *a == 0 { 0 } else { *a + 1 })
    ///         .collect()
    /// }
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::create("test.txt").await.unwrap();
    ///
    ///     let mut buf = file.alloc_dma_buffer(4096);
    ///     // Write some data into the buffer.
    ///     populate(&mut buf);
    ///
    ///     // Seal the buffer by moving ownership to a non-threaded reference
    ///     // counter on the heap.
    ///     let buf = Rc::new(buf);
    ///
    ///     let (written, transformed) = join!(
    ///         async { file.write_rc_at(buf.clone(), 0).await.unwrap() },
    ///         transform(buf.as_bytes())
    ///     );
    ///     assert_eq!(written, 4096);
    ///     file.close().await.unwrap();
    ///
    ///     // transformed AND buf can still be used even though the buffer got
    ///     // written. Note that there may be performance issues if buf is large
    ///     // and you remain hanging onto it.
    /// });
    /// ```
    pub async fn write_rc_at(&self, buf: Rc<DmaBuffer>, pos: u64) -> Result<usize> {
        let source = self.file.reactor.upgrade().unwrap().write_dma(
            self.as_raw_fd(),
            DmaSource::Shared(buf),
            pos,
            self.pollable,
        );
        enhanced_try!(source.collect_rw().await, "Writing", self.file).map_err(Into::into)
    }

    /// Reads from a specific position in the file and returns the buffer.
    ///
    /// The position must be aligned to for Direct I/O. In most platforms
    /// that means 512 bytes.
    pub async fn read_at_aligned(&self, pos: u64, size: usize) -> Result<ReadResult> {
        let source = self.file.reactor.upgrade().unwrap().read_dma(
            self.as_raw_fd(),
            pos,
            size,
            self.pollable,
            self.file.scheduler.borrow().as_ref(),
        );
        let read_size = enhanced_try!(source.collect_rw().await, "Reading", self.file)?;
        Ok(ReadResult::from_sliced_buffer(source, 0, read_size))
    }

    /// Reads into buffer in buf from a specific position in the file.
    ///
    /// It is not necessary to respect the `O_DIRECT` alignment of the file, and
    /// this API will internally convert the positions and sizes to match,
    /// at a cost.
    ///
    /// If you can guarantee proper alignment, prefer [`Self::read_at_aligned`]
    /// instead
    pub async fn read_at(&self, pos: u64, size: usize) -> Result<ReadResult> {
        let eff_pos = self.align_down(pos);
        let b = (pos - eff_pos) as usize;

        let eff_size = self.align_up((size + b) as u64) as usize;
        let source = self.file.reactor.upgrade().unwrap().read_dma(
            self.as_raw_fd(),
            eff_pos,
            eff_size,
            self.pollable,
            self.file.scheduler.borrow().as_ref(),
        );

        let read_size = enhanced_try!(source.collect_rw().await, "Reading", self.file)?;
        Ok(ReadResult::from_sliced_buffer(
            source,
            b,
            std::cmp::min(read_size, size),
        ))
    }

    /// Submit many reads and process the results in a stream-like fashion via a
    /// [`ReadManyResult`].
    ///
    /// This API will optimistically coalesce and deduplicate IO requests such
    /// that two overlapping or adjacent reads will result in a single IO
    /// request. This is transparent for the consumer, you will still
    /// receive individual [`ReadResult`]s corresponding to what you asked for.
    ///
    /// The first argument is a stream of [`IoVec`]. The last two
    /// arguments control how aggressive the IO coalescing should be:
    /// * `buffer_limit` controls how large a merged IO request can get;
    /// * `read_amp_limit` controls how much read amplification is acceptable.
    ///
    /// It is not necessary to respect the `O_DIRECT` alignment of the file, and
    /// this API will internally align the reads appropriately.
    pub fn read_many<V, S>(
        self: &Rc<DmaFile>,
        iovs: S,
        buffer_limit: MergedBufferLimit,
        read_amp_limit: ReadAmplificationLimit,
    ) -> ReadManyResult<V, impl Stream<Item = (ScheduledSource, ReadManyArgs<V>)>>
    where
        V: IoVec + Unpin,
        S: Stream<Item = V> + Unpin,
    {
        let max_merged_buffer_size = match buffer_limit {
            MergedBufferLimit::NoMerging => 0,
            MergedBufferLimit::DeviceMaxSingleRequest => self.max_sectors_size,
            MergedBufferLimit::Custom(limit) => {
                self.align_down(limit.min(self.max_segment_size) as u64) as usize
            }
        };

        let max_read_amp = match read_amp_limit {
            ReadAmplificationLimit::NoAmplification => Some(0),
            ReadAmplificationLimit::Custom(limit) => Some(limit),
            ReadAmplificationLimit::NoLimit => None,
        };

        let file = self.clone();
        let reactor = file.file.reactor.upgrade().unwrap();
        let it = CoalescedReads::new(
            max_merged_buffer_size,
            max_read_amp,
            Some(self.o_direct_alignment),
            iovs,
        )
        .map(move |iov| {
            let fd = file.as_raw_fd();
            let pollable = file.pollable;
            let scheduler = file.file.scheduler.borrow();
            (
                reactor.read_dma(fd, iov.pos(), iov.size(), pollable, scheduler.as_ref()),
                ReadManyArgs {
                    user_reads: iov.coalesced_user_iovecs,
                    system_read: (iov.pos, iov.size),
                },
            )
        });
        ReadManyResult {
            inner: OrderedBulkIo::new(self.clone(), crate::executor().reactor().ring_depth(), it),
            current: Default::default(),
        }
    }

    /// Copies a file range from one file to another in kernel space. This is going to have the same performance
    /// characteristic as splice except if both files are on the same filesystem and the filesystem supports reflinks.
    /// In that case, the underlying disk blocks will be CoW linked instead of actually performing a copy.
    /// Since `copy_file_range` is not yet implemented on io_uring <https://github.com/axboe/liburing/issues/831>,
    /// this is just a dispatch to the blocking thread pool to do the syscall.
    pub async fn copy_file_range_aligned(
        &self,
        fd_in: &DmaFile,
        off_in: u64,
        len: usize,
        off_out: u64,
    ) -> Result<usize> {
        let source = self
            .file
            .reactor
            .upgrade()
            .unwrap()
            .copy_file_range(fd_in.as_raw_fd(), off_in, self.as_raw_fd(), off_out, len)
            .await;
        let copy_size = enhanced_try!(source.collect_rw().await, "Copying file range", self.file)?;
        Ok(copy_size)
    }

    /// Issues `fdatasync` for the underlying file, instructing the OS to flush
    /// all writes to the device, providing durability even if the system
    /// crashes or is rebooted.
    ///
    /// As this is a DMA file, the OS will not be caching this file; however,
    /// there may be caches on the drive itself.
    pub async fn fdatasync(&self) -> Result<()> {
        self.file.fdatasync().await.map_err(Into::into)
    }

    /// Returns the alignment required for I/O operations. Typical values will
    /// be 512 (NVME drive is configured in slower compat mode) or 4096
    /// (typical TLC native alignment).
    pub fn alignment(&self) -> u64 {
        self.o_direct_alignment
    }

    /// Erases a range from the file without changing the size. Check the man
    /// page for [`fallocate`] for a list of the supported filesystems.
    /// Partial blocks are zeroed while whole blocks are simply unmapped
    /// from the file. The reported file size (`file_size`) is unchanged but
    /// the allocated file size may if you've erased whole filesystem blocks
    /// ([`allocated_file_size`])
    ///
    /// [`fallocate`]: https://man7.org/linux/man-pages/man2/fallocate.2.html
    /// [`allocated_file_size`]: struct.Stat.html#structfield.alloc_dma_buffer
    pub async fn deallocate(&self, offset: u64, size: u64) -> Result<()> {
        self.file.deallocate(offset, size).await
    }

    /// pre-allocates space in the filesystem to hold a file at least as big as
    /// the size argument. No existing data in the range [0, size) is modified.
    /// If `keep_size` is false, then anything in [current file length, size)
    /// will report zeroed blocks until overwritten and the file size reported
    /// will be `size`. If `keep_size` is true then the existing file size
    /// is unchanged.
    pub async fn pre_allocate(&self, size: u64, keep_size: bool) -> Result<()> {
        self.file.pre_allocate(size, keep_size).await
    }

    /// Hint to the OS the size of increase of this file, to allow more
    /// efficient allocation of blocks.
    ///
    /// Allocating blocks at the filesystem level turns asynchronous writes into
    /// threaded synchronous writes, as we need to first find the blocks to
    /// host the file.
    ///
    /// If the extent is larger, that means many blocks are allocated at a time.
    /// For instance, if the extent size is 1 MiB, that means that only 1 out
    /// of 4 256 KiB writes will be turned synchronous. Combined with diligent
    /// use of `fallocate` we can greatly minimize context switches.
    ///
    /// It is important not to set the extent size too big. Writes can fail
    /// otherwise if the extent can't be allocated.
    pub async fn hint_extent_size(&self, size: usize) -> Result<i32> {
        self.file.hint_extent_size(size).await
    }

    /// Truncates a file to the specified size.
    ///
    /// Note: this syscall might be issued in a background thread depending on
    /// the system's capabilities.
    pub async fn truncate(&self, size: u64) -> Result<()> {
        self.file.truncate(size).await
    }

    /// Rename this file.
    ///
    /// Note: this syscall might be issued in a background thread depending on
    /// the system's capabilities.
    pub async fn rename<P: AsRef<Path>>(&self, new_path: P) -> Result<()> {
        self.file.rename(new_path).await
    }

    /// Remove this file.
    ///
    /// The file does not have to be closed to be removed. Removing removes
    /// the name from the filesystem but the file will still be accessible for
    /// as long as it is open.
    ///
    /// Note: this syscall might be issued in a background thread depending on
    /// the system's capabilities.
    pub async fn remove(&self) -> Result<()> {
        self.file.remove().await
    }

    /// Returns the size of a file, in bytes
    pub async fn file_size(&self) -> Result<u64> {
        self.file.file_size().await
    }

    /// Returns the size of the filesystem cluster, in bytes
    pub async fn stat(&self) -> Result<Stat> {
        self.file.statx().await.map(Into::into)
    }

    /// Tries to acquire a process-wide advisory shared lock on this file instance. If an exclusive advisory lock is
    /// already held for the underlying file (by any process), this immediately returns with an error. Similarly,
    /// if a shared lock is already held on this file instance, this returns an error. To acquire multiple shared locks
    /// in-process on the same file, you must acquire them through files obtained through separate calls to open the
    /// file.
    ///
    /// This is equivalent to [flock](https://linux.die.net/man/2/flock) with `LOCK_SH | LOCK_NB` and requires the
    /// file have [read](OpenOptions::read) allowed on it.
    ///
    /// # File instances and advisory locks.
    ///
    /// For the purpose of advisory locks, clones and [dupes](Self::dup) count as being the same file instance (even
    /// though dupes don't keep the file open). Advisory locks keep the underlying file that they were acquired on open
    /// while they are held.
    ///
    /// # Safety
    ///
    /// The simple mechanism here only allows one advisory lock holder for the process for a given file instance
    /// and that lock must either be [unlocked](AdvisoryLockGuard::unlock).
    ///
    /// The reason only a single advisory lock can be held on a given file instance at a time is for safety and
    /// soundness - the kernel advisory lock is a status bit and any unlock request will unlock the file. That causes
    /// some danger if you acquire two shared locks and then unlock one of them - the file is unlocked from then on. Or
    /// worse, you unlock one of them, acquire an exclusive lock and then release what you thought was the shared lock
    /// but end up releasing the exclusive lock. Similarly, acquiring a shared lock will downgrade if you're holding
    /// an exclusive lock.
    ///
    /// # Usage
    ///
    /// The purpose of advisory locks is to synchronize access to a resource. A common use-case is that if you're
    /// reading to a file, you acquire a shared lock so that any other processes can also continue reading it but you're
    /// guaranteed that no one trying to write holding an exclusive lock is admitted.
    ///
    /// ```no_run
    /// use glommio::{
    ///   LocalExecutor,
    ///   io::{
    ///     OpenOptions,
    ///     DmaBuffer,
    ///   }
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     // A new anonymous file is created within `some_directory/`.
    ///     let file = OpenOptions::new()
    ///       .create_new(true)
    ///       .read(true)
    ///       .write(true)
    ///       .tmpfile(true)
    ///       .dma_open("some_directory")
    ///       .await
    ///       .unwrap();
    ///
    ///     let guard = file.try_lock_shared().await.unwrap();
    ///
    ///     // Until the guard is dropped, no other process on the file system can acquire an advisory read lock
    ///     // on the file.
    ///
    ///     // File won't close until you either unlock the guard, drop it, or give it back as part of closing.
    ///     guard.unlock().await;
    /// });
    /// ```
    ///
    /// NOTE:
    /// [Currently](https://github.com/axboe/liburing/issues/85) flock isn't implemented through io_uring so it requires
    /// dispatching to a background thread.
    pub async fn try_lock_shared(&self) -> Result<AdvisoryLockGuard> {
        self.file
            .try_lock_shared()
            .await
            .map(|f| AdvisoryLockGuard(Some(Arc::new(f))))
    }

    /// Tries to acquire an advisory exclusive lock on this file instance. If successful, then the OS advisory lock
    /// is acquired. If failed, it's either because the OS advisory lock is held as shared OR because this file instance
    /// already handed out a lock (or is in the process of handing one out) - either exclusive or shared.
    ///
    /// This is equivalent to [flock](https://linux.die.net/man/2/flock) with `LOCK_EX | LOCK_NB` and requires the
    /// file have [write](OpenOptions::write) allowed on it.
    ///
    /// See [try_lock_shared](Self::try_lock_shared) for more details.
    pub async fn try_lock_exclusive(&self) -> Result<AdvisoryLockGuard> {
        self.file
            .try_lock_exclusive()
            .await
            .map(|f| AdvisoryLockGuard(Some(Arc::new(f))))
    }

    /// Attempt to confirm no other clones of this file exist. If no clones exist,
    /// Ok(self) is returned. If clones remain, Err(self) is returned. Do not use
    /// this unless you are implementing a polling mechanism to determine when it's
    /// safe to invoke close - dropping the error case is equivalent to just invoking
    /// close and ignoring the result.
    /// NOTE: Clones are allowed to exist on any thread and all share the same underlying
    /// fd safely. try_take_last_clone is also safe to invoke from any thread and will
    /// behave correctly with respect to clones on other threads.
    pub fn try_take_last_clone(mut self) -> std::result::Result<Self, Self> {
        match self.file.try_take_last_clone() {
            Ok(took) => {
                self.file = took;
                Ok(self)
            }
            Err(still_cloned) => {
                self.file = still_cloned;
                Err(self)
            }
        }
    }

    /// This will release the advisory lock if and only if the guard is the last instance AND this is the last DmaFile
    /// instance. On success, self is returned with the guard dropped. Otherwise everything is returned. This will panic
    /// if given a guard that's not for this file.
    ///
    /// NOTE: This will panic if the unlock operation fails.
    pub async fn try_take_last_clone_unlocking_guard(
        mut self,
        guard: AdvisoryLockGuard,
    ) -> std::result::Result<Self, (Self, AdvisoryLockGuard)> {
        let guard_file = guard.0.as_ref().unwrap();
        assert!(
            Arc::ptr_eq(
                guard_file.lock_state.as_ref().unwrap(),
                self.file.lock_state.as_ref().unwrap()
            ),
            "The provided guard is for {:?} ({}) which doesn't share the same file entry as {:?} ({})",
            guard_file.path,
            guard_file.as_raw_fd(),
            self.path(),
            self.as_raw_fd()
        );

        match guard.try_take_last_clone() {
            Ok(mut last_guard) => {
                match self
                    .file
                    .try_take_last_clone_unlocking_guard(
                        Arc::into_inner(last_guard.0.take().unwrap()).unwrap(),
                    )
                    .await
                {
                    Ok(took) => {
                        self.file = took;
                        Ok(self)
                    }
                    Err((not_unique, guard)) => {
                        self.file = not_unique;
                        Err((self, AdvisoryLockGuard(Some(Arc::new(guard)))))
                    }
                }
            }
            Err(still_locked) => Err((self, still_locked)),
        }
    }

    /// Closes this DMA file.
    pub async fn close(self) -> Result<()> {
        self.file.close().await
    }

    /// Returns an `Option` containing the path associated with this open
    /// directory, or `None` if there isn't one.
    pub fn path(&self) -> Option<Ref<'_, Path>> {
        self.file.path()
    }

    /// The inode backing the file. A file with the same inode may appear under multiple
    /// paths due to renaming and linking.
    pub fn inode(&self) -> u64 {
        self.file.inode
    }

    /// The major ID of the device containing the filesystem where the file resides.
    /// The device may be found by issuing a `readlink` on `/sys/dev/block/<major>:<minor>`
    pub fn dev_major(&self) -> u32 {
        self.file.dev_major
    }

    /// The minor ID of the device containing the filesystem where the file resides.
    pub fn dev_minor(&self) -> u32 {
        self.file.dev_minor
    }

    /// Creates a non-owning reference to this file that can be shared to a background thread.
    /// The weak reference may keep some memory alive but does not keep the underlying file descriptor
    /// open if all strong references have been dropped.
    pub fn downgrade(&self) -> WeakDmaFile {
        WeakDmaFile {
            file: self.file.downgrade(),
            o_direct_alignment: self.o_direct_alignment,
            max_sectors_size: self.max_sectors_size,
            max_segment_size: self.max_segment_size,
            pollable: self.pollable,
        }
    }

    /// How many strong references are holding this file open. The returned count will always be at least 1.
    ///
    /// # Safety
    ///
    /// This method by itself is safe, but using it correctly requires extra care. Another thread can change the count
    /// at any time, including potentially between calling this method and acting on the result. You probably want to
    /// use [Self::downgrade] or [DmaFile::try_take_last_clone] to manage the ownership.
    pub fn strong_count(&self) -> usize {
        self.file.strong_count()
    }

    /// Convenience method that closes a DmaFile wrapped inside an Rc.
    ///
    /// Returns [CloseResult] to indicate which operation was performed.
    pub async fn close_rc(self: Rc<DmaFile>) -> Result<CloseResult> {
        match Rc::try_unwrap(self) {
            Err(_) => Ok(CloseResult::Unreferenced),
            Ok(file) => file.close().await.map(|_| CloseResult::Closed),
        }
    }
}

#[derive(Debug, Clone)]
/// Takes ownership of internal state of the [`DmaFile`] that can be sent to an executor running on a different thread.
/// In the other thread, you'd convert this back into a [`DmaFile`].
///
/// # Examples
/// ```no_run
/// use glommio::{
///     io::{DmaFile, OpenOptions, OwnedDmaFile},
///     LocalExecutor,
/// };
/// use std::os::unix::io::AsRawFd;
///
/// let main_ex = LocalExecutor::default();
/// main_ex.run(async move {
///     let file = OpenOptions::new()
///         .create_new(true)
///         .read(true)
///         .write(true)
///         .tmpfile(true)
///         .dma_open("some directory")
///         .await
///         .expect("Failed to open file");
///     let original_fd = file.as_raw_fd();
///     let original_inode = file.inode();
///
///     let alignment = file.alignment();
///     let fs_cluster_size = file.stat().await.unwrap().fs_cluster_size;
///     let buffer_size = alignment.max(fs_cluster_size.into()) as usize;
///
///     let mut buffer = file.alloc_dma_buffer(buffer_size);
///     buffer.as_bytes_mut().fill(1);
///     file.write_at(buffer, 0).await.unwrap();
///
///     let to_send: OwnedDmaFile = file.into();
///     let result: OwnedDmaFile = std::thread::spawn(move || {
///         let background_ex = LocalExecutor::default();
///
///         let result = background_ex.run(async move {
///             let file: DmaFile = to_send.into();
///             assert_eq!(file.as_raw_fd(), original_fd);
///             assert_eq!(file.inode(), original_inode);
///
///             let read = file.read_at_aligned(0, buffer_size).await.unwrap();
///             assert!(read.iter().all(|&b| b == 1));
///
///             let mut buffer = file.alloc_dma_buffer(buffer_size);
///             buffer.as_bytes_mut().fill(2);
///
///             file.write_at(buffer, 0).await.unwrap();
///
///             file.dup().unwrap().into()
///         });
///         // Make sure we cycle through the reactor one final time to process the
///         // implicit close generated by the drop. This is generally true for glommio. However,
///         // Most examples have `.run` return out of main and so the leak that would otherwise
///         // happen is short lived. However, OwnedDmaFile means you're likely to create an executor
///         // for a long-lived background thread and it's worth calling out that caveat here explicitly.
///         background_ex.run(async move {});
///         result
///     })
///     .join()
///     .unwrap();
///
///     assert_eq!(nix::fcntl::fcntl(original_fd, nix::fcntl::FcntlArg::F_GETFD), Err(nix::errno::Errno::EBADF));
///
///     let file: DmaFile = result.into();
///     assert_ne!(file.as_raw_fd(), original_fd);
///     assert_eq!(file.inode(), original_inode);
///
///     let read = file.read_at_aligned(0, buffer_size).await.unwrap();
///     assert!(read.iter().all(|&b| b == 2));
/// });
/// ```
///
/// SAFETY: Make sure to use pread/pwrite APIs if operating on the FD directly to ensure you
/// don't have multiple threads messing with the kernel's internal offset for the descriptor.
/// Additionally, there's no synchronization guarantee about race conditions reading and writing
/// the same file region from different threads. This is also true for a DmaFile that you convert
/// this into.
///
/// For a guarantee of safety of the logical data on disk, you need to provide external
/// synchronization guarantees. For example, immutably writing a given file region, handing out
/// an owned reference after the fact, and then somehow guaranteeing that you never modify that
/// region again as long as there are readers alive.
pub struct OwnedDmaFile {
    file: OwnedGlommioFile,
    o_direct_alignment: u64,
    max_sectors_size: usize,
    max_segment_size: usize,
    pollable: PollableStatus,
}

impl OwnedDmaFile {
    /// Creates a duplicate instance pointing to the same file descriptor as self.
    /// See [`DmaFile::dup`](struct.DmaFile.html#method.dup) for more info. This is
    /// very different from cloning which simply adds a reference to the underlying
    /// fd already open and should be preferred unless you are trying to intentionally
    /// decouple lifetime of the file descriptors.
    pub fn dup(&self) -> Result<Self> {
        Ok(Self {
            file: enhanced_try!(
                self.file.dup(),
                "Duplicating",
                self.file.path.as_ref(),
                self.file.fd.as_ref().map(|fd| fd.as_raw_fd())
            )?,
            o_direct_alignment: self.o_direct_alignment,
            max_sectors_size: self.max_sectors_size,
            max_segment_size: self.max_segment_size,
            pollable: self.pollable,
        })
    }

    /// Returns an `Option` containing the path associated with this open
    /// directory, or `None` if there isn't one.
    pub fn path(&self) -> Option<&Path> {
        self.file.path.as_deref()
    }

    /// The inode backing the file. A file with the same inode may appear under multiple
    /// paths due to renaming and linking.
    pub fn inode(&self) -> u64 {
        self.file.inode
    }

    /// The major ID of the device containing the filesystem where the file resides.
    /// The device may be found by issuing a `readlink` on `/sys/dev/block/<major>:<minor>`
    pub fn dev_major(&self) -> u32 {
        self.file.dev_major
    }

    /// The minor ID of the device containing the filesystem where the file resides.
    pub fn dev_minor(&self) -> u32 {
        self.file.dev_minor
    }

    /// Returns the alignment required for I/O operations. Typical values will
    /// be 512 (NVME drive is configured in slower compat mode) or 4096
    /// (typical TLC native alignment).
    pub fn alignment(&self) -> u64 {
        self.o_direct_alignment
    }

    /// Downgrade to a weak reference that doesn't keep the FD open but can be upgraded at a later
    /// date on any thread to an OwnedDmaFile. There are safety implications if multiple threads
    /// get involved (see the safety note for [OwnedDmaFile]).
    pub fn downgrade(&self) -> WeakDmaFile {
        WeakDmaFile {
            file: self.file.downgrade(),
            o_direct_alignment: self.o_direct_alignment,
            max_sectors_size: self.max_sectors_size,
            max_segment_size: self.max_segment_size,
            pollable: self.pollable,
        }
    }

    /// How many strong references are holding this file open. The returned count will always be at least 1.
    ///
    /// # Safety
    ///
    /// This method by itself is safe, but using it correctly requires extra care. Another thread can change the count
    /// at any time, including potentially between calling this method and acting on the result. You probably want to
    /// use [Self::downgrade] or convert to a [DmaFile] and use [DmaFile::try_take_last_clone].
    pub fn strong_count(&self) -> usize {
        self.file.strong_count()
    }
}

impl From<DmaFile> for OwnedDmaFile {
    fn from(value: DmaFile) -> Self {
        Self {
            file: value.file.into(),
            o_direct_alignment: value.o_direct_alignment,
            max_sectors_size: value.max_sectors_size,
            max_segment_size: value.max_segment_size,
            pollable: value.pollable,
        }
    }
}

impl From<OwnedDmaFile> for DmaFile {
    fn from(value: OwnedDmaFile) -> Self {
        Self {
            file: value.file.into(),
            o_direct_alignment: value.o_direct_alignment,
            max_sectors_size: value.max_sectors_size,
            max_segment_size: value.max_segment_size,
            pollable: value.pollable,
        }
    }
}

impl AsRawFd for OwnedDmaFile {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

/// This holds a weak non-owning reference to the file that can attempt to be upgraded to an owned
/// reference on any thread. Underneath it uses Arc so there is an atomic cost involved in constructing
/// it / cloning. However, it can be useful if you want to grant conditional access at a future date
/// to a different thread or even to provide access to the FD for use outside glommio. For example,
/// upgrade to an OwnedDmaFile which gives you access to the underlying fd via [OwnedDmaFile::as_raw_fd].
///
/// SAFETY: See [OwnedDmaFile].
#[derive(Debug, Clone)]
pub struct WeakDmaFile {
    file: WeakGlommioFile,
    o_direct_alignment: u64,
    max_sectors_size: usize,
    max_segment_size: usize,
    pollable: PollableStatus,
}

impl Default for WeakDmaFile {
    fn default() -> Self {
        Self::new()
    }
}

impl WeakDmaFile {
    /// Creates an empty reference that will never upgrade.
    pub fn new() -> WeakDmaFile {
        Self {
            file: WeakGlommioFile::new(),
            o_direct_alignment: 0,
            max_sectors_size: 0,
            max_segment_size: 0,
            pollable: PollableStatus::Pollable,
        }
    }

    /// The number of strong references that still remain on the file.
    /// If created via [Self::new], this will return 0.
    pub fn strong_count(&self) -> usize {
        self.file.strong_count()
    }

    /// Returns an `Option` containing ownership over the file if the file hasn't been closed yet, otherwise None.
    pub fn upgrade(&self) -> Option<OwnedDmaFile> {
        self.file.upgrade().map(|file| OwnedDmaFile {
            file,
            o_direct_alignment: self.o_direct_alignment,
            max_sectors_size: self.max_sectors_size,
            max_segment_size: self.max_segment_size,
            pollable: self.pollable,
        })
    }

    /// Returns an `Option` containing the path associated with this open
    /// directory, or `None` if there isn't one.
    pub fn path(&self) -> Option<&Path> {
        self.file.path.as_deref()
    }

    /// The inode backing the file. A file with the same inode may appear under multiple
    /// paths due to renaming and linking.
    pub fn inode(&self) -> u64 {
        self.file.inode
    }

    /// The major ID of the device containing the filesystem where the file resides.
    /// The device may be found by issuing a `readlink` on `/sys/dev/block/<major>:<minor>`
    pub fn dev_major(&self) -> u32 {
        self.file.dev_major
    }

    /// The minor ID of the device containing the filesystem where the file resides.
    pub fn dev_minor(&self) -> u32 {
        self.file.dev_minor
    }

    /// Returns the alignment required for I/O operations. Typical values will
    /// be 512 (NVME drive is configured in slower compat mode) or 4096
    /// (typical TLC native alignment).
    pub fn alignment(&self) -> u64 {
        self.o_direct_alignment
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::{
        enclose,
        sync::Semaphore,
        test_utils::make_test_directories,
        timer::{sleep, timeout},
        ByteSliceMutExt, GlommioError, Latency, LocalExecutor, ResourceType, Shares,
    };
    use futures::join;
    use futures_lite::{stream, StreamExt};
    use rand::{seq::SliceRandom, thread_rng};
    use std::{cell::RefCell, convert::TryInto, ops::Deref, path::PathBuf, time::Duration};

    macro_rules! dma_file_test {
        ( $name:ident, $dir:ident, $kind:ident, $code:block) => {
            #[test]
            fn $name() {
                for dir in make_test_directories(&format!("dma-{}", stringify!($name))) {
                    let $dir = dir.path.clone();
                    let $kind = dir.kind;
                    test_executor!(async move { $code });
                }
            }
        };
    }

    dma_file_test!(file_create_close, path, _k, {
        let new_file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");
        new_file.close().await.expect("failed to close file");
        std::assert!(path.join("testfile").exists());
    });

    dma_file_test!(file_open, path, _k, {
        let new_file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");
        new_file.close().await.expect("failed to close file");

        let file = DmaFile::open(path.join("testfile"))
            .await
            .expect("failed to open file");
        file.close().await.expect("failed to close file");

        std::assert!(path.join("testfile").exists());
    });

    dma_file_test!(file_open_nonexistent, path, _k, {
        DmaFile::open(path.join("testfile"))
            .await
            .expect_err("opened nonexistent file");
        std::assert!(!path.join("testfile").exists());
    });

    dma_file_test!(file_rename, path, _k, {
        let new_file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");

        new_file
            .rename(path.join("testfile2"))
            .await
            .expect("failed to rename file");

        std::assert!(!path.join("testfile").exists());
        std::assert!(path.join("testfile2").exists());

        new_file.close().await.expect("failed to close file");
    });

    dma_file_test!(file_rename_noop, path, _k, {
        let new_file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");

        new_file
            .rename(path.join("testfile"))
            .await
            .expect("failed to rename file");
        std::assert!(path.join("testfile").exists());

        new_file.close().await.expect("failed to close file");
    });

    dma_file_test!(file_fallocate_alocatee, path, _kind, {
        let new_file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");

        new_file
            .pre_allocate(4096, false)
            .await
            .expect("fallocate failed");

        std::assert_eq!(
            new_file.file_size().await.unwrap(),
            4096,
            "file doesn't have expected size"
        );
        let metadata = std::fs::metadata(path.join("testfile")).unwrap();
        std::assert_eq!(metadata.len(), 4096);

        // should be noop
        new_file
            .pre_allocate(2048, false)
            .await
            .expect("fallocate failed");

        std::assert_eq!(
            new_file.file_size().await.unwrap(),
            4096,
            "file doesn't have expected size"
        );
        let metadata = std::fs::metadata(path.join("testfile")).unwrap();
        std::assert_eq!(metadata.len(), 4096);

        let mut buf = new_file.alloc_dma_buffer(4096);
        buf.as_bytes_mut()[0] = 1;
        buf.as_bytes_mut()[4095] = 2;
        new_file.write_at(buf, 0).await.expect("failed to write");

        new_file
            .pre_allocate(8192, true)
            .await
            .expect("fallocate failed");
        let metadata = std::fs::metadata(path.join("testfile")).unwrap();
        std::assert_eq!(metadata.len(), 4096);

        new_file
            .pre_allocate(8192, false)
            .await
            .expect("fallocate failed");
        let metadata = std::fs::metadata(path.join("testfile")).unwrap();
        std::assert_eq!(metadata.len(), 8192);

        new_file.close().await.expect("failed to close file");

        let new_file = DmaFile::open(path.join("testfile"))
            .await
            .expect("failed to open file");
        let read = new_file.read_at(0, 8192).await.expect("failed to read");
        assert_eq!(read.len(), 8192);
        assert_eq!(read[0], 1);
        assert_eq!(read[4095], 2);
        assert_eq!(read[4096], 0);
        assert_eq!(read[8191], 0);
    });

    dma_file_test!(file_fallocate_zero, path, _k, {
        let new_file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");
        new_file
            .pre_allocate(0, false)
            .await
            .expect_err("fallocate should fail with len == 0");

        new_file.close().await.expect("failed to close file");
    });

    dma_file_test!(file_path, path, _k, {
        let new_file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");

        assert_eq!(*new_file.path().unwrap(), path.join("testfile"));
        new_file.close().await.expect("failed to close file");
    });

    dma_file_test!(file_simple_readwrite, path, _k, {
        let new_file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");

        let mut buf = new_file.alloc_dma_buffer(4096);
        buf.memset(42);
        let res = new_file.write_at(buf, 0).await.expect("failed to write");
        assert_eq!(res, 4096);

        new_file.close().await.expect("failed to close file");

        let new_file = DmaFile::open(path.join("testfile"))
            .await
            .expect("failed to create file");
        let read_buf = new_file.read_at(0, 500).await.expect("failed to read");
        std::assert_eq!(read_buf.len(), 500);
        let min_read_size = new_file.align_up(500);
        for i in 0..read_buf.len() {
            std::assert_eq!(read_buf[i], 42);
        }

        let read_buf = new_file
            .read_at_aligned(0, 4096)
            .await
            .expect("failed to read");
        std::assert_eq!(read_buf.len(), 4096);
        for i in 0..read_buf.len() {
            std::assert_eq!(read_buf[i], 42);
        }

        new_file.close().await.expect("failed to close file");

        let stats = crate::executor().io_stats();
        assert_eq!(stats.all_rings().files_opened(), 2);
        assert_eq!(stats.all_rings().files_closed(), 2);
        assert_eq!(stats.all_rings().file_reads(), (2, 4096 + min_read_size));
        assert_eq!(stats.all_rings().file_writes(), (1, 4096));
    });

    dma_file_test!(file_invalid_readonly_write, path, _k, {
        let file = std::fs::File::create(path.join("testfile")).expect("failed to create file");
        let mut perms = file
            .metadata()
            .expect("failed to fetch metadata")
            .permissions();
        perms.set_readonly(true);
        file.set_permissions(perms)
            .expect("failed to update file permissions");

        let new_file = DmaFile::open(path.join("testfile"))
            .await
            .expect("open failed");
        let buf = DmaBuffer::new(4096).expect("failed to allocate dma buffer");

        new_file
            .write_at(buf, 0)
            .await
            .expect_err("writes to read-only files should fail");
        new_file
            .pre_allocate(4096, false)
            .await
            .expect_err("pre allocating read-only files should fail");
        new_file.close().await.expect("failed to close file");
        assert_eq!(
            crate::executor().io_stats().all_rings().file_writes(),
            (0, 0)
        );
    });

    dma_file_test!(file_empty_read, path, _k, {
        std::fs::File::create(path.join("testfile")).expect("failed to create file");

        let new_file = DmaFile::open(path.join("testfile"))
            .await
            .expect("failed to open file");
        let buf = new_file.read_at(0, 512).await.expect("failed to read");
        std::assert_eq!(buf.len(), 0);
        new_file.close().await.expect("failed to close file");

        let stats = crate::executor().io_stats();
        assert_eq!(stats.all_rings().files_opened(), 1);
        assert_eq!(stats.all_rings().files_closed(), 1);
        assert_eq!(stats.all_rings().file_reads(), (1, 0));
    });

    // Futures not polled. Should be in the submission queue
    dma_file_test!(cancellation_doest_crash_futures_not_polled, path, _k, {
        let file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");

        let size: usize = 4096;
        file.truncate(size as u64).await.unwrap();
        let mut futs = vec![];
        for _ in 0..200 {
            let mut buf = file.alloc_dma_buffer(size);
            let bytes = buf.as_bytes_mut();
            bytes[0] = b'x';

            let f = file.write_at(buf, 0);
            futs.push(f);
        }
        let mut all = join_all(futs);
        let _ = futures::poll!(&mut all);
        drop(all);
        file.close().await.unwrap();

        let stats = crate::executor().io_stats();
        assert_eq!(stats.all_rings().files_opened(), 1);
        assert_eq!(stats.all_rings().files_closed(), 1);
        assert_eq!(stats.all_rings().file_reads(), (0, 0));
        assert_eq!(stats.all_rings().file_writes(), (0, 0));
    });

    // Futures polled. Should be a mixture of in the ring and in the in the
    // submission queue
    dma_file_test!(cancellation_doest_crash_futures_polled, p, _k, {
        let mut handles = vec![];
        for i in 0..200 {
            let path = p.clone();
            handles.push(
                crate::spawn_local(async move {
                    let mut path = path.join("testfile");
                    path.set_extension(i.to_string());
                    let file = DmaFile::create(&path).await.expect("failed to create file");

                    let size: usize = 4096;
                    file.truncate(size as u64).await.unwrap();

                    let mut buf = file.alloc_dma_buffer(size);
                    let bytes = buf.as_bytes_mut();
                    bytes[0] = b'x';
                    file.write_at(buf, 0).await.unwrap();
                    file.close().await.unwrap();
                })
                .detach(),
            );
        }
        for h in &handles {
            h.cancel();
        }
        for h in handles {
            h.await;
        }
    });

    dma_file_test!(is_same_file, path, _k, {
        let wfile = DmaFile::create(path.join("testfile")).await.unwrap();
        let rfile = DmaFile::open(path.join("testfile")).await.unwrap();
        let wfile_other = DmaFile::create(path.join("testfile_other")).await.unwrap();

        assert_ne!(wfile.as_raw_fd(), rfile.as_raw_fd());
        assert!(wfile.is_same(&rfile));
        assert!(!wfile.is_same(&wfile_other));

        wfile.close().await.unwrap();
        wfile_other.close().await.unwrap();
        rfile.close().await.unwrap();
    });

    async fn write_dma_file(path: PathBuf, bytes: usize) -> DmaFile {
        let new_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .read(true)
            .dma_open(path)
            .await
            .expect("failed to create file");

        let mut buf = new_file.alloc_dma_buffer(bytes);
        for x in 0..bytes {
            buf.as_bytes_mut()[x] = x as u8;
        }
        let res = new_file.write_at(buf, 0).await.expect("failed to write");
        assert_eq!(res, bytes);
        new_file.fdatasync().await.expect("failed to sync disk");
        new_file
    }

    async fn read_write(path: std::path::PathBuf) {
        let new_file = write_dma_file(path, 4096).await;
        let read_buf = new_file.read_at(0, 500).await.expect("failed to read");
        std::assert_eq!(read_buf.len(), 500);
        for i in 0..read_buf.len() {
            std::assert_eq!(read_buf[i], i as u8);
        }

        let read_buf = new_file
            .read_at_aligned(0, 4096)
            .await
            .expect("failed to read");
        std::assert_eq!(read_buf.len(), 4096);
        for i in 0..read_buf.len() {
            std::assert_eq!(read_buf[i], i as u8);
        }

        new_file.close().await.expect("failed to close file");
    }

    dma_file_test!(per_queue_stats, path, _k, {
        let q1 =
            crate::executor().create_task_queue(Shares::default(), Latency::NotImportant, "q1");
        let q2 = crate::executor().create_task_queue(
            Shares::default(),
            Latency::Matters(Duration::from_millis(1)),
            "q2",
        );
        let task1 =
            crate::spawn_local_into(read_write(path.join("q1")), q1).expect("failed to spawn task");
        let task2 =
            crate::spawn_local_into(read_write(path.join("q2")), q2).expect("failed to spawn task");

        join!(task1, task2);

        let stats = crate::executor().io_stats();
        assert_eq!(stats.all_rings().files_opened(), 2);
        assert_eq!(stats.all_rings().files_closed(), 2);
        assert_eq!(stats.all_rings().file_reads().0, 4);
        assert_eq!(stats.all_rings().file_writes().0, 2);

        let stats = crate::executor()
            .task_queue_io_stats(q1)
            .expect("failed to retrieve task queue io stats");
        assert_eq!(stats.all_rings().files_opened(), 1);
        assert_eq!(stats.all_rings().files_closed(), 1);
        assert_eq!(stats.all_rings().file_reads().0, 2);
        assert_eq!(stats.all_rings().file_writes().0, 1);

        let stats = crate::executor()
            .task_queue_io_stats(q2)
            .expect("failed to retrieve task queue io stats");
        assert_eq!(stats.all_rings().files_opened(), 1);
        assert_eq!(stats.main_ring.files_opened(), 1);
        assert_eq!(stats.all_rings().files_closed(), 1);
        assert_eq!(stats.main_ring.files_closed(), 1);
        assert_eq!(stats.all_rings().file_reads().0, 2);
        assert_eq!(stats.all_rings().file_writes().0, 1);
    });

    dma_file_test!(file_many_reads, path, _k, {
        let new_file = Rc::new(write_dma_file(path.join("testfile"), 4096).await);

        println!("{new_file:?}");

        let total_reads = Rc::new(RefCell::new(0));
        let last_read = Rc::new(RefCell::new(-1));

        let mut iovs: Vec<(u64, usize)> = (0..512).map(|x| (x * 8, 8)).collect();
        iovs.shuffle(&mut thread_rng());
        new_file
            .read_many(
                stream::iter(iovs.into_iter()),
                MergedBufferLimit::NoMerging,
                ReadAmplificationLimit::NoAmplification,
            )
            .enumerate()
            .for_each(enclose! {(total_reads, last_read) |x| {
                *total_reads.borrow_mut() += 1;
                let res = x.1.unwrap();
                assert_eq!(res.0.size(), 8);
                assert_eq!(res.1.len(), 8);
                assert_eq!(*last_read.borrow() + 1, x.0 as i64);
                for i in 0..res.1.len() {
                    assert_eq!(res.1[i], (res.0.pos() + i as u64) as u8);
                }
                *last_read.borrow_mut() = x.0 as i64;
            }})
            .await;
        assert_eq!(*total_reads.borrow(), 512);

        let io_stats = crate::executor().io_stats().all_rings();
        assert!(io_stats.file_reads().0 >= 1 && io_stats.file_reads().0 <= 512);
        new_file.close_rc().await.expect("failed to close file");
    });

    dma_file_test!(file_many_reads_unaligned, path, _k, {
        let new_file = Rc::new(write_dma_file(path.join("testfile"), 4096).await);

        let total_reads = Rc::new(RefCell::new(0));
        let last_read = Rc::new(RefCell::new(-1));

        let mut iovs: Vec<(u64, usize)> = (0..511).map(|x| (x * 8 + 1, 7)).collect();
        iovs.shuffle(&mut thread_rng());
        new_file
            .read_many(
                stream::iter(iovs.into_iter()),
                MergedBufferLimit::Custom(4096),
                ReadAmplificationLimit::NoAmplification,
            )
            .enumerate()
            .for_each(enclose! {(total_reads, last_read) |x| {
                *total_reads.borrow_mut() += 1;
                let res = x.1.unwrap();
                assert_eq!(res.0.size(), 7);
                assert_eq!(res.1.len(), 7);
                assert_eq!(*last_read.borrow() + 1, x.0 as i64);
                for i in 0..res.1.len() {
                    assert_eq!(res.1[i], (res.0.pos() + i as u64) as u8);
                }
                *last_read.borrow_mut() = x.0 as i64;
            }})
            .await;
        assert_eq!(*total_reads.borrow(), 511);
        let io_stats = crate::executor().io_stats().all_rings();
        assert!(io_stats.file_reads().0 >= 1 && io_stats.file_reads().0 <= 512);
        new_file.close_rc().await.expect("failed to close file");
    });

    dma_file_test!(file_many_reads_no_coalescing, path, _k, {
        let new_file = Rc::new(write_dma_file(path.join("testfile"), 4096).await);

        let total_reads = Rc::new(RefCell::new(0));
        let last_read = Rc::new(RefCell::new(-1));

        new_file
            .read_many(
                stream::iter((0..511).map(|x| (x * 8 + 1, 7))),
                MergedBufferLimit::NoMerging,
                ReadAmplificationLimit::NoAmplification,
            )
            .enumerate()
            .for_each(enclose! {(total_reads, last_read) |x| {
                *total_reads.borrow_mut() += 1;
                let res = x.1.unwrap();
                assert_eq!(res.0.size(), 7);
                assert_eq!(res.1.len(), 7);
                assert_eq!(res.0.pos(), (x.0 * 8 + 1) as u64);
                assert_eq!(*last_read.borrow() + 1, x.0 as i64);
                for i in 0..res.1.len() {
                    assert_eq!(res.1[i], (res.0.pos() + i as u64) as u8);
                }
                *last_read.borrow_mut() = x.0 as i64;
            }})
            .await;

        assert_eq!(*total_reads.borrow(), 511);
        let io_stats = crate::executor().io_stats().all_rings();
        assert_eq!(io_stats.file_reads().0, 4096 / new_file.o_direct_alignment);
        assert_eq!(
            io_stats.post_reactor_io_scheduler_latency_us().count() as u64,
            4096 / new_file.o_direct_alignment
        );
        assert_eq!(
            io_stats.io_latency_us().count() as u64,
            4096 / new_file.o_direct_alignment
        );
        new_file.close_rc().await.expect("failed to close file");
    });

    dma_file_test!(write_past_end, path, _k, {
        let writer = DmaFile::create(path.join("testfile")).await.unwrap();
        let reader = DmaFile::open(path.join("testfile")).await.unwrap();

        let stat = reader.stat().await.unwrap();
        assert_eq!(stat.file_size, 0);
        assert_eq!(stat.allocated_file_size, 0);

        let cluster_size = stat.fs_cluster_size;
        assert!(cluster_size >= 512, "{}", stat.fs_cluster_size);

        let mut buffer = writer.alloc_dma_buffer(512);
        for (elem, val) in buffer.as_bytes_mut().iter_mut().zip(1..513) {
            *elem = val as u8;
        }
        let r = writer
            .write_at(buffer, (cluster_size * 2).into())
            .await
            .unwrap();
        assert_eq!(r, 512);

        let stat = reader.stat().await.unwrap();
        assert_eq!(stat.file_size, (cluster_size * 2 + 512).into());
        assert_eq!(stat.allocated_file_size, (cluster_size).into());
        assert_eq!(stat.fs_cluster_size, cluster_size);

        let rb = reader
            .read_at_aligned(0, (cluster_size * 2).try_into().unwrap())
            .await
            .unwrap();
        assert_eq!(rb.len(), (cluster_size * 2).try_into().unwrap());
        for i in rb.iter() {
            assert_eq!(*i, 0);
        }

        let rb = reader
            .read_at_aligned((cluster_size * 2).into(), 1024)
            .await
            .unwrap();
        assert_eq!(rb.len(), 512);
        for (idx, i) in rb.iter().enumerate() {
            assert_eq!(*i, (idx + 1) as u8);
        }

        let mut buffer = writer.alloc_dma_buffer(512);
        for (elem, val) in buffer.as_bytes_mut().iter_mut().zip(3..515) {
            *elem = val as u8;
        }
        let r = writer.write_at(buffer, 512).await.unwrap();
        assert_eq!(r, 512);

        let stat = reader.stat().await.unwrap();
        assert_eq!(stat.file_size, (cluster_size * 2 + 512).into());
        assert_eq!(stat.allocated_file_size, (cluster_size * 2).into());
        assert_eq!(stat.fs_cluster_size, cluster_size);

        let rb = reader.read_at_aligned(0, 512).await.unwrap();
        assert_eq!(rb.len(), 512);
        for i in rb.iter() {
            assert_eq!(*i, 0);
        }

        let rb = reader.read_at_aligned(512, 512).await.unwrap();
        assert_eq!(rb.len(), 512);
        for (idx, i) in rb.iter().enumerate() {
            assert_eq!(*i, (idx + 3) as u8);
        }

        let rb = reader
            .read_at_aligned(1024, (cluster_size * 2 - 1024).try_into().unwrap())
            .await
            .unwrap();
        assert_eq!(rb.len(), (cluster_size * 2 - 1024).try_into().unwrap());
        for i in rb.iter() {
            assert_eq!(*i, 0);
        }

        // Deallocating past the end of file doesn't matter. The size doesn't change.
        writer
            .deallocate((cluster_size * 2).into(), (cluster_size * 2).into())
            .await
            .unwrap();
        let stat = reader.stat().await.unwrap();
        // File size remains unchanged.
        assert_eq!(stat.file_size, (cluster_size * 2 + 512).into());
        // Only one allocated cluster remains.
        assert_eq!(stat.allocated_file_size, cluster_size.into());

        // Deallocated range now returns 0s.
        let rb = reader
            .read_at_aligned((cluster_size * 2).into(), 1024)
            .await
            .unwrap();
        assert_eq!(rb.len(), 512);
        for i in rb.iter() {
            assert_eq!(*i, 0);
        }
    });

    dma_file_test!(file_rc_write, path, _k, {
        let new_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .read(true)
            .dma_open(path.join("testfile"))
            .await
            .expect("failed to create file");

        let bytes = 4096;

        let mut buf = new_file.alloc_dma_buffer(bytes);
        for x in 0..bytes {
            buf.as_bytes_mut()[x] = x as u8;
        }
        let buf = Rc::new(buf);
        let res = new_file
            .write_rc_at(buf.clone(), 0)
            .await
            .expect("failed to write");
        assert_eq!(res, bytes);
        new_file.fdatasync().await.expect("failed to sync disk");
        new_file.close().await.expect("failed to close file");

        assert_eq!(buf.as_bytes()[1], 1);
    });

    dma_file_test!(mirror_buffer_to_two_files, path, _k, {
        let (file1, file2) = join!(
            async {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .dma_open(path.join("testfile1"))
                    .await
                    .expect("failed to create file 1")
            },
            async {
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .dma_open(path.join("testfile2"))
                    .await
                    .expect("failed to create file 2")
            }
        );

        let bytes = 4096;

        let mut buf = file1.alloc_dma_buffer(bytes);
        buf.memset(104);

        let buf1 = Rc::new(buf);
        let buf2 = buf1.clone();

        let (written1, written2) = join!(
            async {
                file1
                    .write_rc_at(buf1, 0)
                    .await
                    .expect("failed to write testfile1")
            },
            async {
                file2
                    .write_rc_at(buf2, 0)
                    .await
                    .expect("failed to write testfile2")
            }
        );

        assert_eq!(written1, bytes);
        assert_eq!(written2, bytes);

        let (buf1, buf2) = join!(
            async {
                file1
                    .read_at_aligned(0, bytes)
                    .await
                    .expect("failed to read testfile1")
            },
            async {
                file2
                    .read_at_aligned(0, bytes)
                    .await
                    .expect("failed to read testfile2")
            }
        );

        join!(async move { file1.close().await.unwrap() }, async move {
            file2.close().await.unwrap()
        });

        assert_eq!(buf1.len(), bytes);
        assert_eq!(buf2.len(), bytes);

        assert_eq!(*buf1, *buf2);
    });

    dma_file_test!(send_file_across_threads, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(path)
            .await
            .expect("Failed to open file");
        let original_fd = file.as_raw_fd();
        let original_inode = file.file.inode;

        let alignment = file.alignment();
        let fs_cluster_size = file.stat().await.unwrap().fs_cluster_size;
        let buffer_size = alignment.max(fs_cluster_size.into()) as usize;

        let mut buffer = file.alloc_dma_buffer(buffer_size);
        buffer.as_bytes_mut().fill(1);
        file.write_at(buffer, 0).await.unwrap();

        let to_send: OwnedDmaFile = file.into();
        let result: OwnedDmaFile = std::thread::spawn(move || {
            let local_ex = LocalExecutor::default();

            let result = local_ex.run(async move {
                let file: DmaFile = to_send.into();
                assert_eq!(file.as_raw_fd(), original_fd);
                assert_eq!(file.file.inode, original_inode);

                let read = file.read_at_aligned(0, buffer_size).await.unwrap();
                assert!(read.iter().all(|&b| b == 1));

                let mut buffer = file.alloc_dma_buffer(buffer_size);
                buffer.as_bytes_mut().fill(2);

                file.write_at(buffer, 0).await.unwrap();

                file.dup().unwrap().into()
            });
            // Make sure we cycle through the reactor one final time to process the
            // implicit close generated by the drop.
            local_ex.run(async move {});
            result
        })
        .join()
        .unwrap();

        let file: DmaFile = result.into();
        assert_ne!(file.as_raw_fd(), original_fd);
        assert_eq!(file.file.inode, original_inode);

        let read = file.read_at_aligned(0, buffer_size).await.unwrap();
        assert!(read.iter().all(|&b| b == 2));
    });

    dma_file_test!(dup, path, _k, {
        fn populate(buf: &mut DmaBuffer) {
            buf.as_bytes_mut()[0..5].copy_from_slice(b"hello");
            buf.as_bytes_mut()[5..].fill(0);
        }

        // A new anonymous file is created within `some_directory/`.
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(path)
            .await
            .unwrap();

        let file2 = file.dup().unwrap();
        let buffer_size = file.o_direct_alignment.try_into().unwrap();
        let mut buf = file.alloc_dma_buffer(buffer_size);
        // Write some data into the buffer.
        populate(&mut buf);

        let written = file.write_at(buf, 0).await.unwrap();
        assert_eq!(written, buffer_size);
        file.close().await.unwrap();

        let read = file2.read_at_aligned(0, buffer_size).await.unwrap();
        assert_eq!(read.len(), buffer_size);
        assert_eq!(
            &read[0..6],
            b"hello\0",
            "{}",
            String::from_utf8_lossy(&read[0..6])
        );
    });

    dma_file_test!(tmpfile_fails_if_not_writable, path, _k, {
        OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(false)
            .tmpfile(true)
            .dma_open(path)
            .await
            .expect_err("O_TMPFILE requires opening with write permissions");
    });

    dma_file_test!(deleted_file_still_can_be_stat, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .dma_open(path.join("deleted_file_still_can_be_stat"))
            .await
            .expect("file should open");
        let mut buf = file.alloc_dma_buffer(512);
        buf.as_bytes_mut().fill(2);
        file.write_at(buf, 0)
            .await
            .expect("should be able to write the file");
        file.remove().await.expect("should have removed file");
        let stat = file
            .stat()
            .await
            .expect("should be able to state unlinked but open file");
        assert_eq!(stat.file_size, 512);
    });

    dma_file_test!(tmpfiles_have_unique_inode, path, _k, {
        let f1 = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(&path)
            .await
            .unwrap();

        let f2 = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(&path)
            .await
            .unwrap();

        assert_ne!(f1.inode(), f2.inode());
        assert_eq!(f1.stat().await.unwrap().file_size, 0);
        assert_eq!(f2.stat().await.unwrap().file_size, 0);

        let mut buf = f1.alloc_dma_buffer(512);
        buf.as_bytes_mut().fill(2);
        f1.write_at(buf, 0)
            .await
            .expect("failed to write to temporary file");
        assert_eq!(f1.stat().await.unwrap().file_size, 512);
    });

    dma_file_test!(resize_dma_buf, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(path)
            .await
            .expect("Failed to open file");

        let alignment =
            (file.alignment()).max(file.stat().await.unwrap().fs_cluster_size.into()) as usize;

        let mut buffer = file.alloc_dma_buffer(2 * alignment);
        buffer.as_bytes_mut()[0..alignment].fill(1);
        buffer.as_bytes_mut()[alignment..].fill(2);
        buffer.trim_to_size(alignment);

        assert_eq!(alignment, file.write_at(buffer, 0).await.unwrap());

        let read = file.read_at_aligned(0, 2 * alignment).await.unwrap();
        assert_eq!(read.len(), alignment);
        assert!(read.iter().all(|&b| b == 1));
    });

    dma_file_test!(copy_file_range, path, _k, {
        let file1 = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(&path)
            .await
            .unwrap();

        let file2 = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(&path)
            .await
            .unwrap();

        let buffer_len = file1.alignment().max(4096) as usize;
        let mut buffer = file1.alloc_dma_buffer(buffer_len);
        buffer.as_bytes_mut().fill(0);
        for i in 0..10u8 {
            buffer.as_bytes_mut()[i as usize] = i;
        }
        let original_write_buffer = buffer.as_bytes_mut().to_vec();

        file1.write_at(buffer, 0).await.unwrap();

        assert_eq!(
            buffer_len,
            file2
                .copy_file_range_aligned(&file1, 0, buffer_len, 0)
                .await
                .unwrap()
        );

        let read = file2.read_at_aligned(0, buffer_len).await.unwrap();
        assert_eq!(read.len(), buffer_len);
        assert_eq!(original_write_buffer.as_slice(), &read[..]);
    });

    dma_file_test!(zero_copy_between_files, path, _k, {
        let file1 = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(&path)
            .await
            .unwrap();

        let file2 = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(&path)
            .await
            .unwrap();

        let buffer_len = file1.alignment().max(4096) as usize;
        let mut buffer = file1.alloc_dma_buffer(buffer_len);
        buffer.as_bytes_mut().fill(0);
        for i in 0..10u8 {
            buffer.as_bytes_mut()[i as usize] = i;
        }
        let original_write_buffer = buffer.as_bytes_mut().to_vec();

        file1.write_at(buffer, 0).await.unwrap();

        let to_copy = file1.read_at_aligned(0, buffer_len).await.unwrap();
        assert_eq!(to_copy.deref(), original_write_buffer.as_slice());

        file2.write_at(to_copy.into(), 4096).await.unwrap();
        let file2_data = file2.read_at_aligned(4096, buffer_len).await.unwrap();
        assert_eq!(file2_data.deref(), original_write_buffer.as_slice());
    });

    dma_file_test!(close_fails_if_clone_exists, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(&path)
            .await
            .unwrap();

        let cloned_file = file.clone();
        assert_eq!(cloned_file.as_raw_fd(), file.as_raw_fd());

        let cloned_file = cloned_file
            .try_take_last_clone()
            .expect_err("Can't take when a clone still exists");

        match file.close().await {
            Err(GlommioError::CanNotBeClosed(ResourceType::File(failed_path), reason)) => {
                assert_eq!(Path::new(&failed_path), path);
                assert_eq!(
                    reason,
                    "Another clone of this file exists somewhere - cannot close fd"
                );
            }
            Ok(()) => panic!("Cannot close file while a clone exists"),
            Err(e) => panic!("File close failed with an unexpected error {e:?}"),
        }

        cloned_file
            .close()
            .await
            .expect("Last reference to file should close successfully");
    });

    dma_file_test!(close_succeeds_on_background_thread, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(&path)
            .await
            .unwrap();

        let cloned_file: OwnedDmaFile = file.clone().into();
        assert_eq!(cloned_file.as_raw_fd(), file.as_raw_fd());

        let (clone_registered_on_background_thread, waiting_for_registration) =
            tokio::sync::oneshot::channel();
        let (test_thread_dropped_file, waiting_for_file_drop) = tokio::sync::oneshot::channel();

        let test_thread = std::thread::current().id();

        let spawned_task = crate::executor()
            .spawn_local(async move {
                crate::executor()
                    .spawn_blocking(move || {
                        let local_ex = crate::executor::LocalExecutorBuilder::new(
                            crate::executor::Placement::Unbound,
                        )
                        .record_io_latencies(true)
                        .make()
                        .unwrap();
                        local_ex.run(async move {
                            assert_ne!(
                                std::thread::current().id(),
                                test_thread,
                                "Blocking thread and test thread shouldn't be the same thread"
                            );

                            let cloned_file: DmaFile = cloned_file.into();

                            clone_registered_on_background_thread.send(()).unwrap();
                            waiting_for_file_drop.await.unwrap();

                            cloned_file
                                .try_take_last_clone()
                                .expect("No more clones should exist")
                                .close()
                                .await
                                .expect("Close on background thread should succeed");
                        })
                    })
                    .await
            })
            .detach();

        waiting_for_registration.await.unwrap();
        let _ = file
            .try_take_last_clone()
            .expect_err("A clone is expected to exist");
        test_thread_dropped_file.send(()).unwrap();
        spawned_task.await.expect("Task wasn't cancelled");
    });

    dma_file_test!(share_file_between_threads, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(&path)
            .await
            .unwrap();

        let cloned_file = file.clone();
        assert_eq!(cloned_file.as_raw_fd(), file.as_raw_fd());

        let owned_clone: OwnedDmaFile = cloned_file.into();

        let test_thread = std::thread::current().id();

        let expected_buffer: Vec<u8> = crate::executor()
            .spawn_blocking(move || {
                let local_ex =
                    crate::executor::LocalExecutorBuilder::new(crate::executor::Placement::Unbound)
                        .record_io_latencies(true)
                        .make()
                        .unwrap();
                local_ex.run(async move {
                    assert_ne!(
                        std::thread::current().id(),
                        test_thread,
                        "Blocking thread and test thread shouldn't be the same thread"
                    );

                    let cloned_file: DmaFile = owned_clone.into();

                    let buffer_len = cloned_file.alignment().max(4096) as usize;
                    let mut buffer = cloned_file.alloc_dma_buffer(buffer_len);
                    buffer.as_bytes_mut().fill(0);
                    for i in 0..10u8 {
                        buffer.as_bytes_mut()[i as usize] = i;
                    }
                    let original_write_buffer = buffer.as_bytes_mut().to_vec();

                    cloned_file
                        .write_at(buffer, cloned_file.alignment())
                        .await
                        .expect("File should be written from background thread successfully");

                    // See close_fails_if_clone_exists - error should be returned by close if a clone
                    // exists & that includes clones materialized on other threads.
                    if let GlommioError::CanNotBeClosed(ResourceType::File(_), _) =
                        cloned_file.close().await.expect_err(
                            "File shouldn't close because clone still exists back on main thread",
                        )
                    {
                    } else {
                        panic!("Unexpected error closing file");
                    }

                    original_write_buffer
                })
            })
            .await;

        let found_buffer = file
            .read_at_aligned(file.alignment(), expected_buffer.len())
            .await
            .expect("File should be read from foreground thread successfully");
        assert_eq!(&*found_buffer, expected_buffer.as_slice());
    });

    dma_file_test!(basic_weak_file, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(&path)
            .await
            .unwrap();

        let weak: WeakDmaFile = file.downgrade();
        assert!(weak.upgrade().is_some(), "File still alive");
        assert_eq!(weak.inode(), file.inode());

        let cloned_weak = weak.clone();
        assert_eq!(cloned_weak.inode(), weak.inode());

        let cloned = file.clone();

        std::mem::drop(file);

        assert!(
            weak.upgrade().is_some(),
            "Clone still keeping the file alive"
        );

        std::mem::drop(cloned);

        assert!(weak.upgrade().is_none(), "Weak reference no longer valid");
        assert!(
            cloned_weak.upgrade().is_none(),
            "Weak reference no longer valid"
        );
    });

    dma_file_test!(weak_file_upgrade_off_thread, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(&path)
            .await
            .unwrap();

        let inode = file.inode();

        let weak: WeakDmaFile = file.downgrade();
        assert_eq!(weak.inode(), inode);

        crate::executor()
            .spawn_blocking(move || {
                let local_ex =
                    crate::executor::LocalExecutorBuilder::new(crate::executor::Placement::Unbound)
                        .record_io_latencies(true)
                        .make()
                        .unwrap();
                local_ex.run(async move {
                    let upgraded: DmaFile = weak.upgrade().unwrap().into();
                    assert_eq!(upgraded.inode(), inode);
                });
            })
            .await;
    });

    #[test]
    fn new_weak_file_strong_count() {
        assert_eq!(WeakDmaFile::new().strong_count(), 0);
    }

    dma_file_test!(weak_file_strong_count, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .tmpfile(true)
            .dma_open(&path)
            .await
            .unwrap();

        let weak = file.downgrade();
        assert_eq!(weak.strong_count(), 1);

        let cloned = file.clone();

        assert_eq!(weak.strong_count(), 2);

        std::mem::drop(file);

        assert_eq!(weak.strong_count(), 1);

        std::mem::drop(cloned);

        assert_eq!(weak.strong_count(), 0);
    });

    dma_file_test!(advisory_lock_exclusive_wait_for_exclusive, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .dma_open(path.join("testfile"))
            .await
            .unwrap();

        let same_path_different_file = OpenOptions::new()
            .read(true)
            .write(true)
            .dma_open(path.join("testfile"))
            .await
            .unwrap();

        let duped = file.dup().unwrap();

        let guard = file.try_lock_exclusive().await.unwrap();
        let waiting_on_exclusive_lock_attempted = std::rc::Rc::new(Semaphore::new(0));

        let notify_exclusive_lock_attempted = waiting_on_exclusive_lock_attempted.clone();
        let acquire_exclusive = crate::spawn_local(timeout(Duration::from_millis(500), async move {
            assert!(same_path_different_file.try_lock_exclusive().await.is_err(), "Shouldn't be able to acquire an exclusive lock on the same path while holding it through a different file");
            notify_exclusive_lock_attempted.signal(1);
            loop {
                if let Ok(locked) = same_path_different_file.try_lock_exclusive().await {
                    return Ok(locked)
                }
                sleep(Duration::from_millis(5)).await;
            }
        })).detach();

        duped.try_lock_shared().await.expect_err(
            "Shouldn't be able to acquire a lock on a dupe while holding it on the parent",
        );
        duped.try_lock_exclusive().await.expect_err(
            "Shouldn't be able to acquire a lock on a dupe while holding it on the parent",
        );

        let file = file.try_take_last_clone().expect_err(
            "Shouldn't be able to acquire a lock on a dupe while holding it on the parent",
        );
        let weak = file.downgrade();
        std::mem::drop(file);
        assert!(
            weak.upgrade().is_some(),
            "Guard should be keeping the file open"
        );

        waiting_on_exclusive_lock_attempted
            .acquire(1)
            .await
            .unwrap();
        guard.unlock().await.unwrap();

        assert!(
            weak.upgrade().is_none(),
            "Guard unlocking should have implicitly caused the file to close"
        );

        timeout(Duration::from_millis(15), async move {
            acquire_exclusive.await.expect("Task not cancelled")
        })
        .await
        .expect("Exclusive lock should have been automatically acquired");
    });

    dma_file_test!(advisory_lock_shared_wait_for_exclusive, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .dma_open(path.join("testfile"))
            .await
            .unwrap();

        let same_path_different_file = OpenOptions::new()
            .read(true)
            .write(true)
            .dma_open(path.join("testfile"))
            .await
            .unwrap();

        let duped = file.dup().unwrap();

        let guard = file.try_lock_exclusive().await.unwrap();
        let waiting_on_shared_lock = std::rc::Rc::new(Semaphore::new(0));

        let notify_waiting_on_shared_lock = waiting_on_shared_lock.clone();
        let acquire_exclusive = crate::spawn_local(timeout(Duration::from_millis(500), async move {
            assert!(same_path_different_file.try_lock_shared().await.is_err(), "Shouldn't be able to acquire a shared lock on the same path while holding an exclusive lock through a different file");
            notify_waiting_on_shared_lock.signal(1);
            loop {
                if let Ok(locked) = same_path_different_file.try_lock_shared().await {
                    return Ok(locked);
                }
                sleep(Duration::from_millis(5)).await;
            }
        })).detach();

        duped.try_lock_shared().await.expect_err(
            "Shouldn't be able to acquire a lock on a dupe while holding it on the parent",
        );
        duped.try_lock_exclusive().await.expect_err(
            "Shouldn't be able to acquire a lock on a dupe while holding it on the parent",
        );

        guard.unlock().await.unwrap();

        timeout(Duration::from_millis(15), async move {
            acquire_exclusive.await.expect("Task not cancelled")
        })
        .await
        .expect("Shared lock should have been automatically acquired");
    });

    dma_file_test!(advisory_lock_shared, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .dma_open(path.join("testfile"))
            .await
            .unwrap();

        let same_path_different_file = OpenOptions::new()
            .read(true)
            .write(true)
            .dma_open(path.join("testfile"))
            .await
            .unwrap();

        let duped = file.dup().unwrap();

        let guard = file.try_lock_shared().await.unwrap();

        duped.try_lock_shared().await.expect_err(
            "Shouldn't be able to acquire a lock on a dupe while holding it on the parent",
        );
        duped.try_lock_exclusive().await.expect_err(
            "Shouldn't be able to acquire a lock on a dupe while holding it on the parent",
        );

        let guard2 = same_path_different_file
            .try_lock_shared()
            .await
            .expect("Should be able to acquire a shared lock through a different open instance");

        guard.unlock().await.unwrap();

        duped.try_lock_exclusive().await.expect_err(
            "Shouldn't be able to acquire an exclusive lock while holding a shared lock",
        );

        guard2.unlock().await.unwrap();

        duped
            .try_lock_exclusive()
            .await
            .expect("Locked exclusively once the shared lock was released");
    });

    dma_file_test!(advisory_weak_lock, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .dma_open(path.join("testfile"))
            .await
            .unwrap();

        let exclusive = file.try_lock_exclusive().await.unwrap();
        let exclusive_clone = exclusive.clone();
        let weak_exclusive = exclusive.downgrade();

        assert!(weak_exclusive.upgrade().is_some());

        exclusive
            .unlock()
            .await
            .expect_err("Shouldn't be able to unlock while multiple lock references");

        assert!(weak_exclusive.upgrade().is_some());

        exclusive_clone.unlock().await.unwrap();

        assert!(weak_exclusive.upgrade().is_none());
    });

    dma_file_test!(advisory_take_last_unlock, path, _k, {
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .dma_open(path.join("testfile"))
            .await
            .unwrap();

        let exclusive = file.try_lock_exclusive().await.unwrap();
        // This drops the guard and unlocks.
        let file = file
            .try_take_last_clone_unlocking_guard(exclusive)
            .await
            .unwrap();
        let shared = file.try_lock_shared().await.unwrap();
        let file = file
            .try_take_last_clone()
            .expect_err("Can't take last clone if guard is holding it");
        let file = file
            .try_take_last_clone_unlocking_guard(shared)
            .await
            .unwrap();
        file.close().await.unwrap();
    });
}
