// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::io::{
    dma_file::{DmaFile, Result},
    BufferedFile,
};
use std::{io, path::Path};

/// Options and flags which can be used to configure how a file is opened.
///
/// This builder exposes the ability to configure how a [`DmaFile`] is opened
/// and what operations are permitted on the open file.
/// The [`DmaFile::open`] and [`DmaFile::create`] methods are aliases for
/// commonly used options using this builder.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    read: bool,
    pub(super) write: bool,
    append: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
    tmpfile: bool,
    tmpfile_linkable: bool,
    // system-specific
    pub(super) custom_flags: libc::c_int,
    pub(super) mode: libc::mode_t,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenOptions {
    /// Creates a blank new set of options ready for configuration.
    ///
    /// All options are initially set to false.
    pub fn new() -> Self {
        Self {
            read: false,
            write: false,
            append: false,
            truncate: false,
            create: false,
            create_new: false,
            tmpfile: false,
            tmpfile_linkable: false,
            // system-specific
            custom_flags: 0,
            // previously, we defaulted to 0o644, but 0o666 is used by libstd
            mode: 0o666,
        }
    }

    /// Sets the option for read access.
    ///
    /// This option, when true, will indicate that the file should be read-able
    /// if opened.
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.read = read;
        self
    }

    /// Sets the option for write access.
    ///
    /// This option, when true, will indicate that the file should be write-able
    /// if opened.
    ///
    /// If the file already exists, any write calls on it will overwrite its
    /// contents, without truncating it.
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    /// Sets the option for appending a previous file.
    ///
    /// If a file is successfully opened with this option set it will append new
    /// data at the end of it.
    ///
    /// The file must be opened with write access for append to work.
    ///
    /// NOTE: Caution is required if using this with [`dma_open`] as
    /// [`write_at`] will cause the position to be reset to the end of file
    /// before each write as documented for `O_APPEND` in the
    /// [man page].
    ///
    /// [`dma_open`]: struct.OpenOptions.html#method.dma_open
    /// [`write_at`]: ../struct.DmaFile.html#method.write_at
    /// [man page]: https://man7.org/linux/man-pages/man2/open.2.html
    pub fn append(&mut self, append: bool) -> &mut Self {
        self.append = append;
        self
    }

    /// Sets the option for truncating a previous file.
    ///
    /// If a file is successfully opened with this option set it will truncate
    /// the file to 0 length if it already exists.
    ///
    /// The file must be opened with write access for truncate to work.
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }

    /// Sets the option to create a new file, or open it if it already exists.
    ///
    /// In order for the file to be created, [`OpenOptions::write`] access
    /// must be used.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    /// Sets the option to create a new file, failing if it already exists.
    ///
    /// No file is allowed to exist at the target location, also no (dangling)
    /// symlink. In this way, if the call succeeds, the file returned is
    /// guaranteed to be new.
    ///
    /// This option is useful because it is atomic. Otherwise, between checking
    /// whether a file exists and creating a new one, the file may have been
    /// created by another process (a TOCTOU race condition / attack).
    ///
    /// If `.create_new(true)` is set, `.create()` and `.truncate()` are
    /// ignored.
    ///
    /// The file must be opened with write or append access in order to create a
    /// new file.
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        self
    }

    /// Sets the option to create a new unnamed temporary file if the operating
    /// system supports it. The path provided to [`dma_open`](#method.dma_open) is the
    /// path of the directory to parent the file. This also requires [`write`](#method.write)
    /// to have been set. See [`tmpfile_linkable`](#method.tmpfile_linkable) if you want to
    /// be able to later create a name for this file using `linkat` (not yet implemented).
    pub fn tmpfile(&mut self, tmpfile: bool) -> &mut Self {
        self.tmpfile = tmpfile;
        self
    }

    /// When [`tmpfile`](#method.tmpfile) is set to true, this controls whether the temporary file
    /// may be put into the filesystem as a named file at a later date using `linkat`.
    /// For now, since `linkat` is not implemented within glommio, you'll need to link
    /// it by hand.
    pub fn tmpfile_linkable(&mut self, linkable: bool) -> &mut Self {
        self.tmpfile_linkable = linkable;
        self
    }

    /// Pass custom flags to the flags' argument of `open_at`.
    pub fn custom_flags(&mut self, flags: i32) -> &mut Self {
        self.custom_flags = flags;
        self
    }

    /// Sets the mode bits that a new file will be created with.
    pub fn mode(&mut self, mode: libc::mode_t) -> &mut Self {
        self.mode = mode;
        self
    }

    pub(super) fn get_access_mode(&self) -> Result<libc::c_int> {
        Ok(match (self.read, self.write, self.append) {
            (true, false, false) => libc::O_RDONLY,
            (false, true, false) => libc::O_WRONLY,
            (true, true, false) => libc::O_RDWR,
            (false, _, true) => libc::O_WRONLY | libc::O_APPEND,
            (true, _, true) => libc::O_RDWR | libc::O_APPEND,
            (false, false, false) => return Err(io::Error::from_raw_os_error(libc::EINVAL).into()),
        })
    }

    pub(super) fn get_creation_mode(&self) -> Result<libc::c_int> {
        match (self.write, self.append) {
            (true, false) => {}
            (false, false) => {
                if self.truncate || self.create || self.create_new || self.tmpfile {
                    return Err(io::Error::from_raw_os_error(libc::EINVAL).into());
                }
            }
            (_, true) => {
                if self.truncate && !(self.create_new || self.tmpfile) {
                    return Err(io::Error::from_raw_os_error(libc::EINVAL).into());
                }
            }
        }

        match (self.tmpfile, self.tmpfile_linkable) {
            (false, _) => (),
            (true, false) => return Ok(libc::O_EXCL | libc::O_TMPFILE),
            (true, true) => return Ok(libc::O_TMPFILE),
        }

        Ok(match (self.create, self.truncate, self.create_new) {
            (false, false, false) => 0,
            (true, false, false) => libc::O_CREAT,
            (false, true, false) => libc::O_TRUNC,
            (true, true, false) => libc::O_CREAT | libc::O_TRUNC,
            (_, _, true) => libc::O_CREAT | libc::O_EXCL,
        })
    }

    /// Similar to `OpenOptions::open()` in the standard library, but returns a
    /// DMA file
    pub async fn dma_open<P: AsRef<Path>>(&self, path: P) -> Result<DmaFile> {
        DmaFile::open_with_options(
            -1_i32,
            path.as_ref(),
            if self.create || self.create_new || self.tmpfile {
                "Creating"
            } else {
                "Opening"
            },
            self,
        )
        .await
    }

    /// Similar to `OpenOptions::open()` in the standard library, but returns a
    /// Buffered file
    pub async fn buffered_open<P: AsRef<Path>>(&self, path: P) -> Result<BufferedFile> {
        BufferedFile::open_with_options(
            -1_i32,
            path.as_ref(),
            if self.create || self.create_new || self.tmpfile {
                "Creating"
            } else {
                "Opening"
            },
            self,
        )
        .await
    }
}

#[cfg(test)]
mod test {
    /*
    use super::*;
    use crate::test_utils::*;
    use crate::{ByteSliceMutExt, Local};
    */

    // TODO: add tests
}
