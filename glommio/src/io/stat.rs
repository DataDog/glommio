// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//

use crate::sys::Statx;

/// Represents the status information about a file.
#[derive(Debug)]
pub struct Stat {
    /// The reported file size, in bytes. This will likely differ from
    /// allocated_file_size which returns how many bytes were allocated on
    /// the filesystem.
    pub file_size: u64,

    /// This returns how many bytes were allocated on the filesystem, ignoring
    /// sparse regions. The typical minimum granularity for filesystem
    /// allocation is 512 bytes although some filesystems may allocate more
    /// than this for the minimum.
    pub allocated_file_size: u64,

    /// The cluster size on the filesystem that this file descriptor is open on,
    /// in bytes.
    pub fs_cluster_size: u32,
}

impl From<Statx> for Stat {
    fn from(s: Statx) -> Self {
        Self {
            file_size: s.stx_size,
            allocated_file_size: s.stx_blocks * 512,
            fs_cluster_size: s.stx_blksize,
        }
    }
}
