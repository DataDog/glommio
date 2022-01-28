// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::{
    io::{dma_file::DmaFile, glommio_file::GlommioFile},
    sys,
    GlommioError,
};
use std::{
    cell::Ref,
    io,
    os::unix::io::{AsRawFd, FromRawFd, RawFd},
    path::Path,
};

type Result<T> = crate::Result<T, ()>;

#[derive(Debug)]
/// A directory representation where asynchronous operations can be issued
pub struct Directory {
    file: GlommioFile,
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
    pub fn try_clone(&self) -> Result<Directory> {
        let fd = sys::duplicate_file(self.file.as_raw_fd()).map_err(|source| {
            GlommioError::create_enhanced(
                source,
                "Cloning directory",
                self.file.path.borrow().as_ref(),
                Some(self.file.as_raw_fd()),
            )
        })?;
        let file =
            unsafe { GlommioFile::from_raw_fd(fd as _) }.with_path(self.file.path.borrow().clone());
        Ok(Directory { file })
    }

    /// Asynchronously open the directory at path
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Directory> {
        let path = path.as_ref().to_owned();
        let flags = libc::O_DIRECTORY | libc::O_CLOEXEC;
        let reactor = crate::executor().reactor();
        let source = reactor.open_at(-1, &path, flags, 0o755);
        let fd = source.collect_rw().await.map_err(|source| {
            GlommioError::create_enhanced(source, "Opening directory", Some(&path), None)
        })?;
        let file = unsafe { GlommioFile::from_raw_fd(fd as _) }.with_path(Some(path));
        Ok(Directory { file })
    }

    /// Opens a file under this directory, returns a DMA file
    ///
    /// NOTE: Path must not contain directories and just be a file name
    pub async fn open_file<P: AsRef<Path>>(&self, path: P) -> Result<DmaFile> {
        if contains_dir(path.as_ref()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Path cannot contain directories",
            )
            .into());
        };

        let path = self.file.path_required("open file")?.join(path.as_ref());
        DmaFile::open(path).await
    }

    /// Similar to create() in the standard library, but returns a DMA file
    pub async fn create<P: AsRef<Path>>(path: P) -> Result<Directory> {
        let path = path.as_ref().to_owned();
        let source = crate::executor().reactor().create_dir(&*path, 0o777).await;

        enhanced_try!(
            match source.collect_rw().await {
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
        Self::open(&path).await
    }

    /// Creates a file under this directory, returns a DMA file
    ///
    /// NOTE: Path must not contain directories and just be a file name
    pub async fn create_file<P: AsRef<Path>>(&self, path: P) -> Result<DmaFile> {
        if contains_dir(path.as_ref()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Path cannot contain directories",
            )
            .into());
        }

        let path = self.file.path_required("create file")?.join(path.as_ref());
        DmaFile::create(path).await
    }

    /// Returns an iterator to the contents of this directory
    pub fn sync_read_dir(&self) -> Result<std::fs::ReadDir> {
        let path = self.file.path_required("read directory")?;
        enhanced_try!(std::fs::read_dir(&*path), "Reading a directory", self.file)
            .map_err(Into::into)
    }

    /// Issues fdatasync into the underlying file.
    pub async fn sync(&self) -> Result<()> {
        let source = self
            .file
            .reactor
            .upgrade()
            .unwrap()
            .fdatasync(self.as_raw_fd());
        source.collect_rw().await?;
        Ok(())
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
}

fn contains_dir(path: &Path) -> bool {
    let mut iter = path.components();
    match iter.next() {
        Some(std::path::Component::Normal(_)) => iter.next().is_some(),
        _ => true,
    }
}
