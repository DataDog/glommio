// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::io::dma_file::{DmaFile, Result};
use std::io;
use std::path::Path;

/// Options and flags which can be used to configure how a file is opened.
///
/// This builder exposes the ability to configure how a [`DmaFile`] is opened
/// and what operations are permitted on the open file.
/// The [`DmaFile::open`] and [`DmaFile::create`] methods are aliases for commonly used options using this builder.
#[derive(Clone, Debug)]
pub struct DmaOpenOptions {
    read: bool,
    pub(super) write: bool,
    truncate: bool,
    create: bool,
    create_new: bool,
    // system-specific
    pub(super) custom_flags: libc::c_int,
    pub(super) mode: libc::mode_t,
}

impl Default for DmaOpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl DmaOpenOptions {
    /// Creates a blank new set of options ready for configuration.
    ///
    /// All options are initially set to false.
    pub fn new() -> Self {
        Self {
            read: false,
            write: false,
            truncate: false,
            create: false,
            create_new: false,
            // system-specific
            custom_flags: 0,
            // previously, we defaulted to 0o644, but 0o666 is used by libstd
            mode: 0o666,
        }
    }

    // the following methods are directly taken from std/sys/unix/fs.rs
    // NOTE(zserik): I omitted `append` from the following methods.
    //               I don't think it makes much sense for unbuffered files.

    /// Sets the option for read access.
    ///
    /// This option, when true, will indicate that the file should be read-able if opened.
    pub fn read(&mut self, read: bool) -> &mut Self {
        self.read = read;
        self
    }

    /// Sets the option for write access.
    ///
    /// This option, when true, will indicate that the file should be write-able if opened.
    ///
    /// If the file already exists, any write calls on it will overwrite its contents, without truncating it.
    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    /// Sets the option for truncating a previous file.
    ///
    /// If a file is successfully opened with this option set it will truncate the file to 0 length if it already exists.
    ///
    /// The file must be opened with write access for truncate to work.
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }

    /// Sets the option to create a new file, or open it if it already exists.
    ///
    /// In order for the file to be created, [`DmaOpenOptions::write`] access must be used.
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    /// Sets the option to create a new file, failing if it already exists.
    ///
    /// No file is allowed to exist at the target location, also no (dangling) symlink. In this way, if the call succeeds, the file returned is guaranteed to be new.
    ///
    /// This option is useful because it is atomic. Otherwise between checking whether a file exists and creating a new one, the file may have been created by another process (a TOCTOU race condition / attack).
    ///
    /// If `.create_new(true)` is set, `.create()` and `.truncate()` are ignored.
    ///
    /// The file must be opened with write or append access in order to create a new file.
    pub fn create_new(&mut self, create_new: bool) -> &mut Self {
        self.create_new = create_new;
        self
    }

    /// Pass custom flags to the flags argument of `open_at`.
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
        Ok(match (self.read, self.write) {
            (true, false) => libc::O_RDONLY,
            (false, true) => libc::O_WRONLY,
            (true, true) => libc::O_RDWR,
            (false, false) => return Err(io::Error::from_raw_os_error(libc::EINVAL).into()),
        })
    }

    pub(super) fn get_creation_mode(&self) -> Result<libc::c_int> {
        if !self.write && (self.truncate || self.create || self.create_new) {
            Err(io::Error::from_raw_os_error(libc::EINVAL).into())
        } else {
            Ok(match (self.create, self.truncate, self.create_new) {
                (false, false, false) => 0,
                (true, false, false) => libc::O_CREAT,
                (false, true, false) => libc::O_TRUNC,
                (true, true, false) => libc::O_CREAT | libc::O_TRUNC,
                (_, _, true) => libc::O_CREAT | libc::O_EXCL,
            })
        }
    }

    /// Similiar to `OpenOptions::open()` in the standard library, but returns a DMA file
    pub async fn open<P: AsRef<Path>>(&self, path: P) -> Result<DmaFile> {
        DmaFile::open_with_options(
            -1_i32,
            path.as_ref(),
            if self.create || self.create_new {
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
