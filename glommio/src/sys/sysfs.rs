// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use ahash::AHashMap;
use std::{
    cell::RefCell,
    fs::{canonicalize, read_dir, read_to_string},
    io,
    path::{Path, PathBuf},
    str::FromStr,
    vec::Vec,
};

thread_local!(static DEV_MAP: RefCell<AHashMap<(usize, usize), BlockDevice>> = RefCell::new(AHashMap::new()));

#[derive(Debug)]
enum StorageCache {
    WriteBack,
    WriteThrough,
}

impl FromStr for StorageCache {
    type Err = String;
    fn from_str(data: &str) -> Result<Self, Self::Err> {
        let contents = data.trim_matches('\n');
        match contents {
            "write back" => Ok(StorageCache::WriteBack),
            "write through" => Ok(StorageCache::WriteThrough),
            _ => Err(format!("Invalid value {}", data)),
        }
    }
}

#[derive(Debug)]
pub(crate) struct BlockDevice {
    memory_device: bool,
    rotational: bool,
    minimum_io_size: usize,
    optimal_io_size: usize,
    logical_block_size: usize,
    physical_block_size: usize,
    cache: StorageCache,
    subcomponents: Vec<PathBuf>,
}

macro_rules! block_property {
    ( $map:expr, $property:tt, $major:expr, $minor:expr ) => {
        DEV_MAP.with(|x| {
            let key = ($major, $minor);
            let mut map = x.borrow_mut();
            let bdev = map.entry(key).or_insert_with(|| BlockDevice::new(key));

            bdev.$property.clone()
        });
    };
}

fn read_int<P: AsRef<Path>>(path: P) -> isize {
    let path = path.as_ref();
    let data =
        read_to_string(path).unwrap_or_else(|err| panic!("reading {} ({})", path.display(), err));
    let contents = data.trim_matches('\n');
    contents.parse::<isize>().unwrap()
}

#[allow(dead_code)]
impl BlockDevice {
    fn in_memory() -> BlockDevice {
        BlockDevice {
            memory_device: true,
            rotational: false,
            minimum_io_size: 512,
            optimal_io_size: 128 << 10,
            logical_block_size: 512,
            physical_block_size: 512,
            cache: StorageCache::WriteBack,
            subcomponents: Vec::new(),
        }
    }

    fn new(dev_id: (usize, usize)) -> BlockDevice {
        // /sys/dev/block/major:minor is a symlink to the device in /sys/devices/
        // However if this a partition, we actually want to look at the main device.
        // So minor is always zero.
        let dir_path = format!("/sys/dev/block/{}:{}", dev_id.0, 0);
        let dir = match canonicalize(Path::new(dir_path.as_str())) {
            Ok(path) => path,
            Err(x) => match x.kind() {
                io::ErrorKind::NotFound => return BlockDevice::in_memory(),
                _ => panic!("Unexpected error: {:?}", x),
            },
        };
        let queue = dir.join("queue");

        let rotational = read_int(&queue.join("rotational")) != 0;
        let minimum_io_size = read_int(&queue.join("minimum_io_size")) as _;
        let optimal_io_size = read_int(&queue.join("optimal_io_size")) as _;
        let logical_block_size = read_int(&queue.join("logical_block_size")) as _;
        let physical_block_size = read_int(&queue.join("physical_block_size")) as _;

        let cache_data = read_to_string(&queue.join("write_cache")).unwrap();
        let cache = cache_data.parse::<StorageCache>().unwrap();
        let subcomponents = read_dir(dir.join("slaves"))
            .unwrap()
            .map(|x| x.unwrap().path())
            .collect();

        BlockDevice {
            memory_device: false,
            rotational,
            minimum_io_size,
            optimal_io_size,
            logical_block_size,
            physical_block_size,
            cache,
            subcomponents,
        }
    }

    pub(crate) fn memory_device(major: usize, minor: usize) -> bool {
        block_property!(DEV_MAP, memory_device, major, minor)
    }

    pub(crate) fn is_rotational(major: usize, minor: usize) -> bool {
        block_property!(DEV_MAP, rotational, major, minor)
    }

    pub(crate) fn minimum_io_size(major: usize, minor: usize) -> usize {
        block_property!(DEV_MAP, minimum_io_size, major, minor)
    }

    pub(crate) fn optimal_io_size(major: usize, minor: usize) -> usize {
        block_property!(DEV_MAP, optimal_io_size, major, minor)
    }

    pub(crate) fn logical_block_size(major: usize, minor: usize) -> usize {
        block_property!(DEV_MAP, logical_block_size, major, minor)
    }

    pub(crate) fn physical_block_size(major: usize, minor: usize) -> usize {
        block_property!(DEV_MAP, physical_block_size, major, minor)
    }

    pub(crate) fn is_md(major: usize, minor: usize) -> bool {
        !block_property!(DEV_MAP, subcomponents, major, minor).is_empty()
    }
}
