// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//

use crate::io::glommio_file::GlommioFile;
use crate::io::read_result::ReadResult;
use std::io;
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::path::{Path, PathBuf};

/// An asynchronously accessed file backed by the OS page cache.
///
/// All access uses buffered I/O, and all operations including open and close are asynchronous (with
/// some exceptions noted).
///
/// See the module-level [documentation](index.html) for more details and examples.
#[derive(Debug)]
pub struct BufferedFile {
    file: GlommioFile,
}

impl AsRawFd for BufferedFile {
    fn as_raw_fd(&self) -> RawFd {
        self.file.as_raw_fd()
    }
}

impl FromRawFd for BufferedFile {
    unsafe fn from_raw_fd(fd: RawFd) -> Self {
        BufferedFile {
            file: GlommioFile::from_raw_fd(fd),
        }
    }
}

impl BufferedFile {
    /// Returns true if the BufferedFile represent the same file on the underlying device.
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
    /// use glommio::LocalExecutor;
    /// use glommio::io::BufferedFile;
    /// use std::os::unix::io::AsRawFd;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let mut wfile = BufferedFile::create("myfile.txt").await.unwrap();
    ///     let mut rfile = BufferedFile::open("myfile.txt").await.unwrap();
    ///     // Different objects (OS file descriptors), so they will be different...
    ///     assert_ne!(wfile.as_raw_fd(), rfile.as_raw_fd());
    ///     // However they represent the same object.
    ///     assert!(wfile.is_same(&rfile));
    ///     wfile.close().await;
    ///     rfile.close().await;
    /// });
    /// ```
    pub fn is_same(&self, other: &BufferedFile) -> bool {
        self.file.is_same(&other.file)
    }

    /// Similar to [`create`] in the standard library, but returns a `BufferedFile`
    ///
    /// [`create`]: https://doc.rust-lang.org/std/fs/struct.File.html#method.create
    pub async fn create<P: AsRef<Path>>(path: P) -> io::Result<BufferedFile> {
        let flags = libc::O_CLOEXEC | libc::O_CREAT | libc::O_TRUNC | libc::O_WRONLY;
        Ok(BufferedFile {
            file: enhanced_try!(
                GlommioFile::open_at(-1_i32, path.as_ref(), flags, 0o644).await,
                "Creating",
                Some(path.as_ref()),
                None
            )?,
        })
    }

    /// Similar to [`open`] in the standard library, but returns a `BufferedFile`
    ///
    /// [`open`]: https://doc.rust-lang.org/std/fs/struct.File.html#method.open
    pub async fn open<P: AsRef<Path>>(path: P) -> io::Result<BufferedFile> {
        let flags = libc::O_CLOEXEC | libc::O_RDONLY;
        Ok(BufferedFile {
            file: enhanced_try!(
                GlommioFile::open_at(-1_i32, path.as_ref(), flags, 0o644).await,
                "Reading",
                Some(path.as_ref()),
                None
            )?,
        })
    }

    /// Write the data in the buffer `buf` to this `BufferedFile` at the specified position
    ///
    /// This method acquires ownership of the buffer so the buffer can be kept alive
    /// while the kernel has it.
    ///
    /// Note that it is legal to return fewer bytes than the buffer size. That is the
    /// situation, for example, when the device runs out of space (See the man page for
    /// `write(2)` for details)
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::LocalExecutor;
    /// use glommio::io::BufferedFile;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = BufferedFile::create("test.txt").await.unwrap();
    ///
    ///     let mut buf = vec![0, 1, 2, 3];
    ///     file.write_at(buf, 0).await.unwrap();
    ///     file.close().await.unwrap();
    /// });
    /// ```
    pub async fn write_at(&self, buf: Vec<u8>, pos: u64) -> io::Result<usize> {
        let source =
            self.file
                .reactor
                .upgrade()
                .unwrap()
                .write_buffered(self.as_raw_fd(), buf, pos);
        enhanced_try!(source.collect_rw().await, "Writing", self.file)
    }

    /// Reads data at the specified position into a buffer allocated by this library.
    ///
    /// Note that this differs from [`DmaFile`]'s read APIs: that reflects the
    /// fact that buffered reads need no specific alignment and io_uring will not
    /// be able to use its own pre-allocated buffers for it anyway.
    ///
    /// [`DmaFile`]: struct.DmaFile.html
    /// Reads from a specific position in the file and returns the buffer.
    pub async fn read_at(&self, pos: u64, size: usize) -> io::Result<ReadResult> {
        let mut source =
            self.file
                .reactor
                .upgrade()
                .unwrap()
                .read_buffered(self.as_raw_fd(), pos, size);
        let read_size = enhanced_try!(source.collect_rw().await, "Reading", self.file)?;
        let mut buffer = source.extract_dma_buffer();
        buffer.trim_to_size(read_size);
        Ok(ReadResult::from_whole_buffer(buffer))
    }

    /// Issues `fdatasync` for the underlying file, instructing the OS to flush all writes to the
    /// device, providing durability even if the system crashes or is rebooted.
    pub async fn fdatasync(&self) -> io::Result<()> {
        self.file.fdatasync().await
    }

    /// pre-allocates space in the filesystem to hold a file at least as big as the size argument.
    pub async fn pre_allocate(&self, size: u64) -> io::Result<()> {
        self.file.pre_allocate(size).await
    }

    /// Truncates a file to the specified size.
    ///
    /// **Warning:** synchronous operation, will block the reactor
    pub async fn truncate(&self, size: u64) -> io::Result<()> {
        self.file.truncate(size).await
    }

    /// rename this file.
    ///
    /// **Warning:** synchronous operation, will block the reactor
    pub async fn rename<P: AsRef<Path>>(&mut self, new_path: P) -> io::Result<()> {
        self.file.rename(new_path).await
    }

    /// remove this file.
    ///
    /// The file does not have to be closed to be removed. Removing removes
    /// the name from the filesystem but the file will still be accessible for
    /// as long as it is open.
    ///
    /// **Warning:** synchronous operation, will block the reactor
    pub async fn remove(&self) -> io::Result<()> {
        self.file.remove().await
    }

    /// Returns the size of a file, in bytes.
    pub async fn file_size(&self) -> io::Result<u64> {
        self.file.file_size().await
    }

    /// Closes this file.
    pub async fn close(self) -> io::Result<()> {
        self.file.close().await
    }

    pub(crate) fn path(&self) -> &Path {
        self.file.path.as_ref().unwrap().as_path()
    }

    pub(crate) fn discard(self) -> (RawFd, Option<PathBuf>) {
        self.file.discard()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::io::dma_file::test::make_test_directories;

    macro_rules! buffered_file_test {
        ( $name:ident, $dir:ident, $kind:ident, $code:block) => {
            #[test]
            fn $name() {
                for dir in make_test_directories(&format!("buffered-{}", stringify!($name))) {
                    let $dir = dir.path.clone();
                    let $kind = dir.kind;
                    test_executor!(async move { $code });
                }
            }
        };
    }

    macro_rules! check_contents {
        ( $buf:expr, $start:expr ) => {
            for (idx, i) in $buf.iter().enumerate() {
                assert_eq!(*i, ($start + (idx as u64)) as u8);
            }
        };
    }

    buffered_file_test!(file_create_close, path, _k, {
        let new_file = BufferedFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");
        new_file.close().await.expect("failed to close file");
        std::assert!(path.join("testfile").exists());
    });

    buffered_file_test!(file_open, path, _k, {
        let new_file = BufferedFile::create(path.join("testfile"))
            .await
            .expect("failed to create file");
        new_file.close().await.expect("failed to close file");

        let file = BufferedFile::open(path.join("testfile"))
            .await
            .expect("failed to open file");
        file.close().await.expect("failed to close file");

        std::assert!(path.join("testfile").exists());
    });

    buffered_file_test!(file_open_nonexistent, path, _k, {
        BufferedFile::open(path.join("testfile"))
            .await
            .expect_err("opened nonexistent file");
        std::assert!(!path.join("testfile").exists());
    });

    buffered_file_test!(random_io, path, _k, {
        let writer = BufferedFile::create(path.join("testfile")).await.unwrap();

        let reader = BufferedFile::open(path.join("testfile")).await.unwrap();

        let wb = vec![0, 1, 2, 3, 4, 5];
        let r = writer.write_at(wb, 0).await.unwrap();
        assert_eq!(r, 6);

        let rb = reader.read_at(0, 6).await.unwrap();
        assert_eq!(rb.len(), 6);
        check_contents!(rb.as_bytes(), 0);

        // Can read again from the same position
        let rb = reader.read_at(0, 6).await.unwrap();
        assert_eq!(rb.len(), 6);
        check_contents!(rb.as_bytes(), 0);

        // Can read again from a random, unaligned position, and will hit
        // EOF.
        let rb = reader.read_at(3, 6).await.unwrap();
        assert_eq!(rb.len(), 3);
        check_contents!(rb.as_bytes()[0..3], 3);

        writer.close().await.unwrap();
        reader.close().await.unwrap();
    });

    buffered_file_test!(write_past_end, path, _k, {
        let writer = BufferedFile::create(path.join("testfile")).await.unwrap();

        let reader = BufferedFile::open(path.join("testfile")).await.unwrap();

        let rb = reader.read_at(0, 6).await.unwrap();
        assert_eq!(rb.len(), 0);

        let wb = vec![0, 1, 2, 3, 4, 5];
        let r = writer.write_at(wb, 10).await.unwrap();
        assert_eq!(r, 6);

        let rb = reader.read_at(0, 6).await.unwrap();
        assert_eq!(rb.len(), 6);
        for i in rb.as_bytes() {
            assert_eq!(*i, 0);
        }

        let rb = reader.read_at(10, 6).await.unwrap();
        assert_eq!(rb.len(), 6);
        check_contents!(rb.as_bytes(), 0);

        writer.close().await.unwrap();
        reader.close().await.unwrap();
    });
}
