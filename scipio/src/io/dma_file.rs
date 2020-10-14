// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::error::ErrorEnhancer;
use crate::io::read_result::ReadResult;
use crate::parking::Reactor;
use crate::sys;
use crate::sys::sysfs;
use crate::sys::{DmaBuffer, PollableStatus, SourceType};
use std::io;
use std::mem;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::path::{Path, PathBuf};
use std::rc::Rc;

pub(crate) fn align_up(v: u64, align: u64) -> u64 {
    (v + align - 1) & !(align - 1)
}

pub(crate) fn align_down(v: u64, align: u64) -> u64 {
    v & !(align - 1)
}

#[derive(Debug)]
/// A directory representation where asynchronous operations can be issued
pub struct Directory {
    file: ScipioFile,
}

impl AsRawFd for Directory {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

impl Directory {
    /// Try creating a clone of this Directory.
    ///
    /// The new object has a different file descriptor and has to be
    /// closed separately.
    pub fn try_clone(&self) -> io::Result<Directory> {
        let fd = enhanced_try!(
            sys::duplicate_file(self.file.as_raw_fd()),
            "Cloning directory",
            self.file
        )?;
        let file = unsafe { ScipioFile::from_raw_fd(fd as _) }.with_path(self.file.path.clone());
        Ok(Directory { file })
    }

    /// Synchronously open this directory.
    pub fn sync_open<P: AsRef<Path>>(path: P) -> io::Result<Directory> {
        let path = path.as_ref().to_owned();
        let flags = libc::O_CLOEXEC | libc::O_DIRECTORY;
        let fd = enhanced_try!(
            sys::sync_open(&path, flags, 0o755),
            "Synchronously opening directory",
            Some(&path),
            None
        )?;
        let file = unsafe { ScipioFile::from_raw_fd(fd as _) }.with_path(Some(path));
        Ok(Directory { file })
    }

    /// Asynchronously open the directory at path
    pub async fn open<P: AsRef<Path>>(path: P) -> io::Result<Directory> {
        let path = path.as_ref().to_owned();
        let flags = libc::O_DIRECTORY | libc::O_CLOEXEC;
        let source = Reactor::get().open_at(-1, &path, flags, 0o755);
        let fd = enhanced_try!(
            source.collect_rw().await,
            "Opening directory",
            Some(&path),
            None
        )?;
        let file = unsafe { ScipioFile::from_raw_fd(fd as _) }.with_path(Some(path));
        Ok(Directory { file })
    }

    /// Similar to create() in the standard library, but returns a DMA file
    pub fn sync_create<P: AsRef<Path>>(path: P) -> io::Result<Directory> {
        let path = path.as_ref().to_owned();
        enhanced_try!(
            match std::fs::create_dir(&path) {
                Ok(_) => Ok(()),
                Err(x) => {
                    match x.kind() {
                        std::io::ErrorKind::AlreadyExists => Ok(()),
                        _ => Err(x),
                    }
                }
            },
            "Synchronously creating directory",
            Some(&path),
            None
        )?;
        Self::sync_open(&path)
    }

    /// Returns an iterator to the contents of this directory
    pub fn sync_read_dir(&self) -> io::Result<std::fs::ReadDir> {
        let path = self.file.path_required("read directory")?;
        enhanced_try!(std::fs::read_dir(path), "Reading a directory", self.file)
    }

    /// Issues fdatasync into the underlying file.
    pub async fn sync(&self) -> io::Result<()> {
        let source = Reactor::get().fdatasync(self.as_raw_fd());
        source.collect_rw().await?;
        Ok(())
    }

    /// Closes this DMA file.
    pub async fn close(self) -> io::Result<()> {
        self.file.close().await
    }
}

#[derive(Debug)]
/// Constructs a file that can issue DMA operations.
/// All access uses Direct I/O, and all operations including
/// open and close are asynchronous.
pub struct DmaFile {
    file: ScipioFile,
    o_direct_alignment: u64,
    pollable: PollableStatus,
    inode: u64,
    dev_major: u32,
    dev_minor: u32,
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
    /// Returns true if the DmaFiles represent the same file on the underlying device.
    ///
    /// Files are considered to be the same if they live in the same file system and
    /// have the same Linux inode. Note that based on this rule a symlink is *not*
    /// considered to be the same file.
    ///
    /// Files will be considered to be the same if:
    /// * A file is opened multiple times (different file descriptors, but same file!)
    /// * they are hard links.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use scipio::LocalExecutor;
    /// use scipio::io::DmaFile;
    /// use std::os::unix::io::AsRawFd;
    ///
    /// let ex = LocalExecutor::make_default();
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
        self.inode == other.inode
            && self.dev_major == other.dev_major
            && self.dev_minor == other.dev_minor
    }

    async fn open_at(
        dir: RawFd,
        path: &Path,
        flags: libc::c_int,
        mode: libc::c_int,
    ) -> io::Result<DmaFile> {
        let mut pollable = PollableStatus::Pollable;
        let mut source = Reactor::get().open_at(dir, path, flags, mode);
        let mut res = source.collect_rw().await;

        res = match res {
            Err(os_err) => {
                // if we failed to open the file with a recoverable error,
                // open again without O_DIRECT
                if os_err.raw_os_error().unwrap() == libc::EINVAL {
                    pollable = PollableStatus::NonPollable;
                    source = Reactor::get().open_at(dir, path, flags & !libc::O_DIRECT, mode);
                    source.collect_rw().await
                } else {
                    Err(os_err)
                }
            }
            Ok(res) => Ok(res),
        };

        let file =
            unsafe { ScipioFile::from_raw_fd(res? as _) }.with_path(Some(path.to_path_buf()));
        let mut file = DmaFile {
            file,
            o_direct_alignment: 4096,
            pollable,
            inode: 0,
            dev_major: 0,
            dev_minor: 0,
        };

        let st = file.statx().await?;
        let major = st.stx_dev_major;
        let minor = st.stx_dev_minor;
        if sysfs::BlockDevice::is_md(major as _, minor as _) {
            file.pollable = PollableStatus::NonPollable;
        }
        file.inode = st.stx_ino;
        file.dev_major = st.stx_dev_major;
        file.dev_minor = st.stx_dev_minor;
        Ok(file)
    }

    /// Allocates a buffer that is suitable for using to write to this file.
    pub fn alloc_dma_buffer(size: usize) -> DmaBuffer {
        Reactor::get().alloc_dma_buffer(size)
    }

    /// Similar to create() in the standard library, but returns a DMA file
    pub async fn create<P: AsRef<Path>>(path: P) -> io::Result<DmaFile> {
        // FIXME: because we use the poll ring, we really only support xfs and ext4 for this.
        // We should check and maybe do something different in that case.
        let path = path.as_ref().to_owned();

        // try to open the file with O_DIRECT if the underlying media supports it
        let flags =
            libc::O_DIRECT | libc::O_CLOEXEC | libc::O_CREAT | libc::O_TRUNC | libc::O_WRONLY;
        let res = DmaFile::open_at(-1 as _, &path, flags, 0o644).await;

        let mut f = enhanced_try!(res, "Creating", Some(&path), None)?;
        f.o_direct_alignment = 4096;
        Ok(f)
    }

    /// Similar to open() in the standard library, but returns a DMA file
    pub async fn open<P: AsRef<Path>>(path: P) -> io::Result<DmaFile> {
        let path = path.as_ref();

        // try to open the file with O_DIRECT if the underlying media supports it
        let flags = libc::O_DIRECT | libc::O_CLOEXEC | libc::O_RDONLY;
        let res = DmaFile::open_at(-1 as _, path, flags, 0o644).await;

        let mut f = enhanced_try!(res, "Opening", Some(path), None)?;
        f.o_direct_alignment = 512;
        Ok(f)
    }

    /// Writes the buffer in buf to a specific position in the file.
    ///
    /// It is expected that the buffer and the position be properly aligned
    /// for Direct I/O. In most platforms that means 4096 bytes. There is no
    /// write_dma_aligned, since a non aligned write would require a
    /// read-modify-write.
    pub async fn write_dma(&self, buf: &DmaBuffer, pos: u64) -> io::Result<usize> {
        let source = Reactor::get().write_dma(self.as_raw_fd(), buf, pos, self.pollable);
        enhanced_try!(source.collect_rw().await, "Writing", self.file)
    }

    /// Reads from a specific position in the file and returns the buffer.
    ///
    /// The position must be aligned to for Direct I/O. In most platforms
    /// that means 512 bytes.
    pub async fn read_dma_aligned(&self, pos: u64, size: usize) -> io::Result<ReadResult> {
        let mut source = Reactor::get().read_dma(self.as_raw_fd(), pos, size, self.pollable);
        let read_size = enhanced_try!(source.collect_rw().await, "Reading", self.file)?;
        let stype = source.extract_source_type();
        let buffer = match stype {
            SourceType::DmaRead(_, buffer) => buffer
                .map(|mut buffer| {
                    buffer.trim_to_size(read_size);
                    buffer
                })
                .ok_or(bad_buffer!(self.file)),
            _ => Err(bad_buffer!(self.file)),
        }?;
        Ok(ReadResult::from_whole_buffer(buffer))
    }

    /// Reads into buffer in buf from a specific position in the file.
    ///
    /// It is not necessary to respect the O_DIRECT alignment of the file, and this
    /// API will internally convert the positions and sizes to match, at a cost.
    ///
    /// If you can guarantee proper alignment, prefer read_dma_aligned instead
    pub async fn read_dma(&self, pos: u64, size: usize) -> io::Result<ReadResult> {
        let eff_pos = self.align_down(pos);
        let b = (pos - eff_pos) as usize;

        let eff_size = self.align_up((size + b) as u64) as usize;
        let mut source =
            Reactor::get().read_dma(self.as_raw_fd(), eff_pos, eff_size, self.pollable);

        let read_size = enhanced_try!(source.collect_rw().await, "Reading", self.file)?;
        let stype = source.extract_source_type();
        let buffer = match stype {
            SourceType::DmaRead(_, buffer) => buffer
                .map(|mut buffer| {
                    buffer.trim_front(b);
                    buffer.trim_to_size(std::cmp::min(read_size, size));
                    buffer
                })
                .ok_or(bad_buffer!(self.file)),
            _ => Err(bad_buffer!(self.file)),
        }?;
        Ok(ReadResult::from_whole_buffer(buffer))
    }

    /// Issues fdatasync into the underlying file.
    pub async fn fdatasync(&self) -> io::Result<()> {
        let source = Reactor::get().fdatasync(self.as_raw_fd());
        enhanced_try!(source.collect_rw().await, "Syncing", self.file)?;
        Ok(())
    }

    /// pre-allocates space in the filesystem to hold a file at least as big as the size argument
    pub async fn pre_allocate(&self, size: u64) -> io::Result<()> {
        let flags = libc::FALLOC_FL_ZERO_RANGE;
        let source = Reactor::get().fallocate(self.as_raw_fd(), 0, size, flags);
        enhanced_try!(source.collect_rw().await, "Pre-allocate space", self.file)?;
        Ok(())
    }

    /// Allocating blocks at the filesystem level turns asynchronous writes into threaded
    /// synchronous writes, as we need to first find the blocks to host the file.
    ///
    /// If the extent is larger, that means many blocks are allocated at a time. For instance,
    /// if the extent size is 1MB, that means that only 1 out of 4 256kB writes will be turned
    /// synchronous. Combined with diligent use of fallocate we can greatly minimize context
    /// switches.
    ///
    /// It is important not to set the extent size too big. Writes can fail otherwise if the
    /// extent can't be allocated
    pub async fn hint_extent_size(&self, size: usize) -> nix::Result<i32> {
        sys::fs_hint_extentsize(self.as_raw_fd(), size)
    }

    /// Truncates a file to the specified size.
    ///
    /// Warning: synchronous operation, will block the reactor
    pub async fn truncate(&self, size: u64) -> io::Result<()> {
        enhanced_try!(
            sys::truncate_file(self.as_raw_fd(), size),
            "Truncating",
            self.file
        )
    }

    /// rename this file.
    ///
    /// Warning: synchronous operation, will block the reactor
    pub async fn rename<P: AsRef<Path>>(&mut self, new_path: P) -> io::Result<()> {
        let old_path = self.file.path_required("rename")?;
        enhanced_try!(
            crate::io::rename(old_path, &new_path).await,
            "Renaming",
            self.file
        )?;
        self.file.path = Some(new_path.as_ref().to_owned());
        Ok(())
    }

    /// remove this file
    ///
    /// The file does not have to be closed to be removed. Removing removes
    /// the name from the filesystem but the file will still be accessible for
    /// as long as it is open.
    ///
    /// Warning: synchronous operation, will block the reactor
    pub async fn remove(&self) -> io::Result<()> {
        let path = self.file.path_required("remove")?;
        enhanced_try!(sys::remove_file(path), "Removing", self.file)
    }

    // Retrieve file metadata, backed by the statx(2) syscall
    async fn statx(&self) -> io::Result<libc::statx> {
        let path = self.file.path_required("stat")?;

        let mut source = Reactor::get().statx(self.as_raw_fd(), path);
        enhanced_try!(
            source.collect_rw().await,
            "getting file metadata",
            self.file
        )?;
        let stype = source.extract_source_type();
        let stat_buf = match stype {
            SourceType::Statx(_, buf) => buf,
            _ => panic!("Source type is wrong for describe operation"),
        };
        Ok(stat_buf.into_inner())
    }

    /// Returns the size of a file, in bytes
    pub async fn file_size(&self) -> io::Result<u64> {
        let st = self.statx().await?;
        Ok(st.stx_size)
    }

    /// Closes this DMA file.
    pub async fn close(self) -> io::Result<()> {
        self.file.close().await
    }

    pub(crate) async fn close_rc(self: Rc<DmaFile>) -> io::Result<()> {
        match Rc::try_unwrap(self) {
            Err(file) => Err(io::Error::new(
                io::ErrorKind::Other,
                format!("{} references to file still held", Rc::strong_count(&file)),
            )),
            Ok(file) => file.close().await,
        }
    }
}

/// A wrapper over `std::fs::File` which carries a path (for better error
/// messages) and prints a warning if closed synchronously.
#[derive(Debug)]
struct ScipioFile {
    file: std::fs::File,
    // A file can appear in many paths, through renaming and linking.
    // If we do that, each path should have its own object. This is to
    // facilitate error displaying.
    path: Option<PathBuf>,
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
    async fn close(mut self) -> io::Result<()> {
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

    fn with_path(mut self, path: Option<PathBuf>) -> ScipioFile {
        self.path = path;
        self
    }

    fn path_required(&self, op: &'static str) -> io::Result<&Path> {
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
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use crate::Local;

    #[derive(Copy, Clone)]
    pub(crate) enum TestDirectoryKind {
        TempFs,
        StorageMedia,
    }

    pub(crate) struct TestDirectory {
        pub(crate) path: PathBuf,
        pub(crate) kind: TestDirectoryKind,
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[cfg(test)]
    pub(crate) fn make_test_directories(test_name: &str) -> std::vec::Vec<TestDirectory> {
        let mut vec = Vec::new();

        // Scipio currently only supports NVMe-backed volumes formatted with XFS or EXT4.
        // We therefore let the user decide what directory scipio should use to host the unit tests in.
        // For more information regarding this limitation, see the README
        match std::env::var("SCIPIO_TEST_POLLIO_ROOTDIR") {
            Err(_) => {
                eprintln!(
                    "Scipio currently only supports NVMe-backed volumes formatted with XFS \
                    or EXT4. To run poll io-related tests, please set SCIPIO_TEST_POLLIO_ROOTDIR to a \
                    NVMe-backed directory path in your environment.\nPoll io tests will not run."
                );
            }
            Ok(path) => {
                let mut dir = PathBuf::from(path);
                std::assert!(dir.exists());

                dir.push(test_name);
                let _ = std::fs::remove_dir_all(&dir);
                std::fs::create_dir_all(&dir).unwrap();
                vec.push(TestDirectory {
                    path: dir,
                    kind: TestDirectoryKind::StorageMedia,
                })
            }
        };

        let mut dir = std::env::temp_dir();
        dir.push(test_name);
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        vec.push(TestDirectory {
            path: dir,
            kind: TestDirectoryKind::TempFs,
        });
        return vec;
    }

    macro_rules! dma_file_test {
        ( $name:ident, $dir:ident, $kind:ident, $code:block) => {
            #[test]
            fn $name() {
                for dir in make_test_directories(stringify!($name)) {
                    let $dir = dir.path.clone();
                    let $kind = dir.kind;
                    test_executor!(async move { $code });
                }
            }
        };
    }

    dma_file_test!(fallback_drop_closes_the_file, path, _k, {
        let fd;
        {
            let file = DmaFile::create(path.join("testfile"))
                .await
                .expect("failed to create file");
            fd = file.as_raw_fd();
            std::fs::remove_file(path.join("testfile")).unwrap();
        }
        assert!(fd != -1);
        let ret = unsafe { libc::close(fd) };
        assert_eq!(ret, -1);
        let err = std::io::Error::last_os_error().raw_os_error().unwrap();
        assert_eq!(err, libc::EBADF);
    });

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
        let mut new_file = DmaFile::create(path.join("testfile"))
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
        let mut new_file = DmaFile::create(path.join("testfile"))
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

    dma_file_test!(file_simple_readwrite, path, _k, {
        let new_file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");

        let mut buf = DmaBuffer::new(4096).expect("failed to allocate dma buffer");
        buf.memset(42);
        new_file.write_dma(&buf, 0).await.expect("failed to write");
        new_file.close().await.expect("failed to close file");

        let new_file = DmaFile::open(path.join("testfile"))
            .await
            .expect("failed to create file");
        let read_buf = new_file.read_dma(0, 500).await.expect("failed to read");
        std::assert_eq!(read_buf.len(), 500);
        for i in 0..read_buf.len() {
            std::assert_eq!(read_buf.as_bytes()[i], buf.as_bytes()[i]);
        }

        let read_buf = new_file
            .read_dma_aligned(0, 4096)
            .await
            .expect("failed to read");
        std::assert_eq!(read_buf.len(), 4096);
        for i in 0..read_buf.len() {
            std::assert_eq!(read_buf.as_bytes()[i], buf.as_bytes()[i]);
        }

        new_file.close().await.expect("failed to close file");
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
            .write_dma(&buf, 0)
            .await
            .expect_err("writes to read-only files should fail");
        new_file
            .pre_allocate(4096)
            .await
            .expect_err("pre allocating read-only files should fail");
        new_file.close().await.expect("failed to close file");
    });

    dma_file_test!(file_empty_read, path, _k, {
        std::fs::File::create(path.join("testfile")).expect("failed to create file");

        let new_file = DmaFile::open(path.join("testfile"))
            .await
            .expect("failed to open file");
        let buf = new_file.read_dma(0, 512).await.expect("failed to read");
        std::assert_eq!(buf.len(), 0);
        new_file.close().await.expect("failed to close file");
    });

    // Futures not polled. Should be in the submission queue
    dma_file_test!(cancellation_doest_crash_futures_not_polled, path, _k, {
        let file = DmaFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");

        let size: usize = 4096;
        file.truncate(size as u64).await.unwrap();
        let mut buf = DmaFile::alloc_dma_buffer(size);
        let bytes = buf.as_bytes_mut();
        bytes[0] = 'x' as u8;
        let mut futs = vec![];
        for _ in 0..200 {
            let f = file.write_dma(&buf, 0);
            futs.push(f);
        }
        let mut all = join_all(futs);
        let _ = futures::poll!(&mut all);
        drop(all);
        file.close().await.unwrap();
    });

    // Futures polled. Should be a mixture of in the ring and in the in the submission queue
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

                    let mut buf = DmaFile::alloc_dma_buffer(size);
                    let bytes = buf.as_bytes_mut();
                    bytes[0] = 'x' as u8;
                    file.write_dma(&buf, 0).await.unwrap();
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
}
