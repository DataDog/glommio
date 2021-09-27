// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::{
    io::{
        bulk_io::{CoalescedReads, IoVec, OrderedBulkIo, ReadManyArgs, ReadManyResult},
        glommio_file::GlommioFile,
        open_options::OpenOptions,
        read_result::ReadResult,
        ScheduledSource,
    },
    sys::{self, sysfs, DirectIo, DmaBuffer, PollableStatus},
};
use futures_lite::{Stream, StreamExt};
use nix::sys::statfs::*;
use std::{
    cell::Ref,
    io,
    os::unix::io::{AsRawFd, RawFd},
    path::Path,
    rc::Rc,
};

pub(super) type Result<T> = crate::Result<T, ()>;

pub(crate) fn align_up(v: u64, align: u64) -> u64 {
    (v + align - 1) & !(align - 1)
}

pub(crate) fn align_down(v: u64, align: u64) -> u64 {
    v & !(align - 1)
}

#[derive(Debug)]
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
    pollable: PollableStatus,
}

impl DmaFile {
    // FIXME: Don't assume 512, we can read this info from sysfs
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
        // Allow this to work on non direct I/O devices, but only
        // if this is in-memory.
        let file = GlommioFile::open_at(dir, path, flags, mode).await?;

        let buf = statfs(path).unwrap();
        let fstype = buf.filesystem_type();
        let mut pollable;

        if fstype == TMPFS_MAGIC {
            pollable = PollableStatus::NonPollable(DirectIo::Disabled);
        } else {
            pollable = PollableStatus::Pollable;
            sys::direct_io_ify(file.as_raw_fd(), flags)?;
        }

        // Docker overlay can show as dev_major 0.
        // Anything like that is obviously not something that supports the poll ring.
        if file.dev_major == 0
            || sysfs::BlockDevice::is_md(file.dev_major as _, file.dev_minor as _)
        {
            pollable = PollableStatus::NonPollable(DirectIo::Enabled);
        }

        Ok(DmaFile {
            file,
            o_direct_alignment: 4096,
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
        let mut f = enhanced_try!(res, opdesc, Some(path), None)?;
        // FIXME: Don't assume 512 or 4096, we can read this info from sysfs
        // currently, we just use the minimal {values which make sense}
        f.o_direct_alignment = if opts.write { 4096 } else { 512 };
        Ok(f)
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

    /// Write the buffer in `buf` to a specific position in the file.
    ///
    /// It is expected that the buffer and the position be properly aligned
    /// for Direct I/O. In most platforms that means 4096 bytes. There is no
    /// write_at_aligned, since a non aligned write would require a
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
    pub async fn write_at(&self, buf: DmaBuffer, pos: u64) -> Result<usize> {
        let source = self.file.reactor.upgrade().unwrap().write_dma(
            self.as_raw_fd(),
            buf,
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
    /// receive individual ReadResults corresponding to what you asked for.
    ///
    /// The first argument is a stream of [`IoVec`]. The last two
    /// arguments control how aggressive the IO coalescing should be:
    /// * `max_merged_buffer_size` controls how large a merged IO request can
    ///   be. A value of 0 disables merging completely.
    /// * `max_read_amp` is optional and defines the maximum read amplification
    ///   you are comfortable with. If two read requests are separated by a
    ///   distance less than this value, they will be merged. A value `None`
    ///   disables all read amplification limitation.
    ///
    /// It is not necessary to respect the `O_DIRECT` alignment of the file, and
    /// this API will internally convert the positions and sizes to match.
    pub fn read_many<V, S>(
        self: &Rc<DmaFile>,
        iovs: S,
        max_merged_buffer_size: usize,
        max_read_amp: Option<usize>,
    ) -> ReadManyResult<V, impl Stream<Item = (ScheduledSource, ReadManyArgs<V>)>>
    where
        V: IoVec + Unpin,
        S: Stream<Item = V> + Unpin,
    {
        let file = self.clone();
        let it = CoalescedReads::new(
            max_merged_buffer_size,
            max_read_amp,
            Some(self.o_direct_alignment),
            iovs,
        )
        .map(move |iov| {
            let reactor = file.file.reactor.upgrade().unwrap();
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
            inner: OrderedBulkIo::new(self.clone(), 128, it),
            current: Default::default(),
        }
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

    /// pre-allocates space in the filesystem to hold a file at least as big as
    /// the size argument.
    pub async fn pre_allocate(&self, size: u64) -> Result<()> {
        self.file.pre_allocate(size).await
    }

    /// Hint to the OS the size of increase of this file, to allow more
    /// efficient allocation of blocks.
    ///
    /// Allocating blocks at the filesystem level turns asynchronous writes into
    /// threaded synchronous writes, as we need to first find the blocks to
    /// host the file.
    ///
    /// If the extent is larger, that means many blocks are allocated at a time.
    /// For instance, if the extent size is 1MB, that means that only 1 out
    /// of 4 256kB writes will be turned synchronous. Combined with diligent
    /// use of `fallocate` we can greatly minimize context switches.
    ///
    /// It is important not to set the extent size too big. Writes can fail
    /// otherwise if the extent can't be allocated.
    pub async fn hint_extent_size(&self, size: usize) -> Result<i32> {
        self.file.hint_extent_size(size).await
    }

    /// Truncates a file to the specified size.
    ///
    /// **Warning:** synchronous operation, will block the reactor
    pub async fn truncate(&self, size: u64) -> Result<()> {
        self.file.truncate(size).await
    }

    /// rename this file.
    ///
    /// **Warning:** synchronous operation, will block the reactor
    pub async fn rename<P: AsRef<Path>>(&self, new_path: P) -> Result<()> {
        self.file.rename(new_path).await
    }

    /// remove this file.
    ///
    /// The file does not have to be closed to be removed. Removing removes
    /// the name from the filesystem but the file will still be accessible for
    /// as long as it is open.
    ///
    /// **Warning:** synchronous operation, will block the reactor
    pub async fn remove(&self) -> Result<()> {
        self.file.remove().await
    }

    /// Returns the size of a file, in bytes
    pub async fn file_size(&self) -> Result<u64> {
        self.file.file_size().await
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

    /// Convenience method that closes a DmaFile wrapped inside an Rc
    pub async fn close_rc(self: Rc<DmaFile>) -> Result<()> {
        match Rc::try_unwrap(self) {
            Err(file) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("{} references to file still held", Rc::strong_count(&file)),
            )
            .into()),
            Ok(file) => file.close().await,
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::{enclose, test_utils::*, ByteSliceMutExt, Latency, Local, Shares};
    use futures::join;
    use futures_lite::{stream, StreamExt};
    use rand::{seq::SliceRandom, thread_rng};
    use std::{cell::RefCell, path::PathBuf, time::Duration};

    #[cfg(test)]
    pub(crate) fn make_test_directories(test_name: &str) -> std::vec::Vec<TestDirectory> {
        let mut vec = Vec::new();

        // Glommio currently only supports NVMe-backed volumes formatted with XFS or
        // EXT4. We therefore let the user decide what directory glommio should
        // use to host the unit tests in. For more information regarding this
        // limitation, see the README
        match std::env::var("GLOMMIO_TEST_POLLIO_ROOTDIR") {
            Err(_) => {
                eprintln!(
                    "Glommio currently only supports NVMe-backed volumes formatted with XFS or \
                     EXT4. To run poll io-related tests, please set GLOMMIO_TEST_POLLIO_ROOTDIR \
                     to a NVMe-backed directory path in your environment.\nPoll io tests will not \
                     run."
                );
            }
            Ok(path) => {
                vec.push(make_poll_test_directory(path, test_name));
            }
        };

        vec.push(make_tmp_test_directory(test_name));
        vec
    }

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

    dma_file_test!(file_fallocate_alocatee, path, kind, {
        let new_file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");

        let res = new_file.pre_allocate(4096).await;
        if let TestDirectoryKind::TempFs = kind {
            res.expect_err("fallocate should error on tmpfs");
            return;
        }
        res.expect("fallocate failed");

        std::assert_eq!(
            new_file.file_size().await.unwrap(),
            4096,
            "file doesn't have expected size"
        );
        let metadata = std::fs::metadata(path.join("testfile")).unwrap();
        std::assert_eq!(metadata.len(), 4096);

        // should be noop
        new_file.pre_allocate(2048).await.expect("fallocate failed");

        std::assert_eq!(
            new_file.file_size().await.unwrap(),
            4096,
            "file doesn't have expected size"
        );
        let metadata = std::fs::metadata(path.join("testfile")).unwrap();
        std::assert_eq!(metadata.len(), 4096);

        new_file.close().await.expect("failed to close file");
    });

    dma_file_test!(file_fallocate_zero, path, _k, {
        let new_file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");
        new_file
            .pre_allocate(0)
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

        let stats = Local::io_stats();
        assert_eq!(stats.all_rings().files_opened(), 2);
        assert_eq!(stats.all_rings().files_closed(), 2);
        assert_eq!(stats.all_rings().file_reads(), (2, 4608));
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
            .pre_allocate(4096)
            .await
            .expect_err("pre allocating read-only files should fail");
        new_file.close().await.expect("failed to close file");
        assert_eq!(Local::io_stats().all_rings().file_writes(), (0, 0));
    });

    dma_file_test!(file_empty_read, path, _k, {
        std::fs::File::create(path.join("testfile")).expect("failed to create file");

        let new_file = DmaFile::open(path.join("testfile"))
            .await
            .expect("failed to open file");
        let buf = new_file.read_at(0, 512).await.expect("failed to read");
        std::assert_eq!(buf.len(), 0);
        new_file.close().await.expect("failed to close file");

        let stats = Local::io_stats();
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

        let stats = Local::io_stats();
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
                Local::local(async move {
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
        let q1 = Local::create_task_queue(Shares::default(), Latency::NotImportant, "q1");
        let q2 = Local::create_task_queue(
            Shares::default(),
            Latency::Matters(Duration::from_millis(1)),
            "q2",
        );
        let task1 =
            Local::local_into(read_write(path.join("q1")), q1).expect("failed to spawn task");
        let task2 =
            Local::local_into(read_write(path.join("q2")), q2).expect("failed to spawn task");

        join!(task1, task2);

        let stats = Local::io_stats();
        assert_eq!(stats.all_rings().files_opened(), 2);
        assert_eq!(stats.all_rings().files_closed(), 2);
        assert_eq!(stats.all_rings().file_reads().0, 4);
        assert_eq!(stats.all_rings().file_writes().0, 2);

        let stats = Local::task_queue_io_stats(q1).expect("failed to retrieve task queue io stats");
        assert_eq!(stats.all_rings().files_opened(), 1);
        assert_eq!(stats.all_rings().files_closed(), 1);
        assert_eq!(stats.all_rings().file_reads().0, 2);
        assert_eq!(stats.all_rings().file_writes().0, 1);

        let stats = Local::task_queue_io_stats(q2).expect("failed to retrieve task queue io stats");
        assert_eq!(stats.all_rings().files_opened(), 1);
        assert_eq!(stats.latency_ring.files_opened(), 1);
        assert_eq!(stats.all_rings().files_closed(), 1);
        assert_eq!(stats.latency_ring.files_closed(), 1);
        assert_eq!(stats.all_rings().file_reads().0, 2);
        assert_eq!(stats.all_rings().file_writes().0, 1);
    });

    dma_file_test!(file_many_reads, path, _k, {
        let new_file = Rc::new(write_dma_file(path.join("testfile"), 4096).await);

        let total_reads = Rc::new(RefCell::new(0));
        let last_read = Rc::new(RefCell::new(-1));

        let mut iovs: Vec<(u64, usize)> = (0..512).map(|x| (x * 8, 8)).collect();
        iovs.shuffle(&mut thread_rng());
        new_file
            .read_many(stream::iter(iovs.into_iter()), 4096, None)
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
        assert_eq!(
            Local::io_stats().all_rings().file_reads().0,
            4096 / new_file.o_direct_alignment
        );
        new_file.close_rc().await.expect("failed to close file");
    });

    dma_file_test!(file_many_reads_unaligned, path, _k, {
        let new_file = Rc::new(write_dma_file(path.join("testfile"), 4096).await);

        let total_reads = Rc::new(RefCell::new(0));
        let last_read = Rc::new(RefCell::new(-1));

        let mut iovs: Vec<(u64, usize)> = (0..511).map(|x| (x * 8 + 1, 7)).collect();
        iovs.shuffle(&mut thread_rng());
        new_file
            .read_many(stream::iter(iovs.into_iter()), 4096, None)
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
        assert_eq!(
            Local::io_stats().all_rings().file_reads().0,
            4096 / new_file.o_direct_alignment
        );
        new_file.close_rc().await.expect("failed to close file");
    });

    dma_file_test!(file_many_reads_no_coalescing, path, _k, {
        let new_file = Rc::new(write_dma_file(path.join("testfile"), 4096).await);

        let total_reads = Rc::new(RefCell::new(0));
        let last_read = Rc::new(RefCell::new(-1));

        new_file
            .read_many(stream::iter((0..511).map(|x| (x * 8 + 1, 7))), 0, Some(0))
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
        assert_eq!(
            Local::io_stats().all_rings().file_reads().0,
            4096 / new_file.o_direct_alignment
        );
        new_file.close_rc().await.expect("failed to close file");
    });
}
