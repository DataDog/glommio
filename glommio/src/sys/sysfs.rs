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
    marker::PhantomData,
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
#[allow(dead_code)]
pub(crate) struct BlockDevice {
    memory_device: bool,
    rotational: bool,
    minimum_io_size: usize,
    optimal_io_size: usize,
    logical_block_size: usize,
    physical_block_size: usize,
    max_sectors_size: usize,
    max_segment_size: usize,
    cache: StorageCache,
    iopoll: Option<bool>,
    subcomponents: Vec<PathBuf>,
}

macro_rules! block_property {
    ( $map:expr, $property:tt, $major:expr, $minor:expr ) => {
        DEV_MAP.with(|x| {
            let key = ($major, $minor);
            let mut map = x.borrow_mut();
            let bdev = map.entry(key).or_insert_with(|| BlockDevice::new(key));

            bdev.$property.clone()
        })
    };
}

macro_rules! set_block_property {
    ( $map:expr, $property:tt, $major:expr, $minor:expr, $value:expr ) => {
        DEV_MAP.with(|x| {
            let key = ($major, $minor);
            let mut map = x.borrow_mut();
            let bdev = map.entry(key).or_insert_with(|| BlockDevice::new(key));

            bdev.$property = $value;
        })
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
            max_sectors_size: 128 << 10,
            max_segment_size: (u32::MAX - 1) as usize,
            cache: StorageCache::WriteBack,
            iopoll: Some(false),
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
        let max_sectors_kb = read_int(&queue.join("max_sectors_kb")) as usize;
        let max_segment_size = read_int(&queue.join("max_segment_size")) as usize;

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
            max_sectors_size: max_sectors_kb << 10,
            max_segment_size,
            cache,
            iopoll: None,
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

    pub(crate) fn max_sectors_size(major: usize, minor: usize) -> usize {
        block_property!(DEV_MAP, max_sectors_size, major, minor)
    }

    pub(crate) fn max_segment_size(major: usize, minor: usize) -> usize {
        block_property!(DEV_MAP, max_segment_size, major, minor)
    }

    pub(crate) fn is_md(major: usize, minor: usize) -> bool {
        !block_property!(DEV_MAP, subcomponents, major, minor).is_empty()
    }

    pub(crate) fn iopoll(major: usize, minor: usize) -> Option<bool> {
        block_property!(DEV_MAP, iopoll, major, minor)
    }

    pub(crate) fn set_iopoll_support(major: usize, minor: usize, supported: bool) {
        set_block_property!(DEV_MAP, iopoll, major, minor, Some(supported))
    }
}

/// Implements an [`Iterator`] over lists of unsigned values commonly
/// encountered in `sysfs`.  A list with the format "0,1,4-6" would iterate over
/// values 0,1,4,5,6.  The resulting [`Iterator::Item`] is an
/// Option<Result<usize>>, where Some(Err(...)) is returned if unexpected
/// formatting is encountered in the list.
///
/// Each range within the list is allowed to be of the format `9-38:4/10` with
/// syntax `range:used_size/group_size`.
/// Example: 0-1023:2/256 ==> 0,1,256,257,512,513,768,769
///
/// The input string has the following characteristics:
/// * ASCII string terminated with a `\0` or `\n`
/// * Ranges are separated by commas and whitespaces (see definition of
///   whitespaces in `ListIterator::skip_delim`)
/// * Repeat range delimiters are allowed (e.g. `,,1,,4-6,, \n\n  ,,\0` is ok)
/// * If the range has a `used_size` then a `group_size` is required
///
/// For additional details see:
/// https://www.kernel.org/doc/html/latest/admin-guide/cputopology.html
/// https://github.com/torvalds/linux/blob/d93a0d43e3d0ba9e19387be4dae4a8d5b175a8d7/lib/bitmap.c#L614-L629
/// https://github.com/torvalds/linux/blob/d93a0d43e3d0ba9e19387be4dae4a8d5b175a8d7/lib/bitmap.c#L488-L494
pub(crate) struct ListIterator {
    /// the input list
    list_str: String,
    /// the next index to parse
    idx: usize,
    /// an iterator over a range that is part of the list; e.g. if the list is
    /// 1-3,5,7-9 then each of the inclusive ranges 1-3 and 5-5 and 7-9 is a
    /// `range_iter` stored here
    range_iter: Option<RangeIter<Checked>>,
}

impl ListIterator {
    pub(super) fn from_path(path: &Path) -> io::Result<Self> {
        let s = std::fs::read_to_string(&path)?;
        Self::from_str(s)
    }

    fn from_str(s: impl Into<String>) -> io::Result<Self> {
        let list_str = s.into();
        if !list_str.is_ascii() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "input is not ascii",
            ));
        }
        Ok(Self {
            list_str,
            idx: 0,
            range_iter: None,
        })
    }

    fn parse_next_num(&mut self) -> io::Result<usize> {
        let beg = self.idx;

        self.idx += self
            .list_str
            .get(beg..)
            .expect("invalid range: this is a bug")
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(self.list_str.len() - beg);

        self.list_str
            .get(beg..self.idx)
            .expect("invalid range: this is a bug")
            .parse::<usize>()
            .map_err(|_| self.err_invalid())
    }

    fn skip_delim(&mut self) {
        fn is_space(c: char) -> bool {
            // White spaces are defined in:
            // https://github.com/torvalds/linux/blob/d93a0d43e3d0ba9e19387be4dae4a8d5b175a8d7/include/linux/ctype.h#L17-L33
            // https://github.com/torvalds/linux/blob/d93a0d43e3d0ba9e19387be4dae4a8d5b175a8d7/lib/ctype.c#L12-L36
            let c = c as u8;
            (9..=13).contains(&c) || c == 32 || c == 160
        }

        // leave a list terminating '\n' for syntax verification
        let idx_max = if self.idx < self.list_str.len() {
            self.list_str.len() - 1
        } else {
            return;
        };

        self.idx += self
            .list_str
            .get(self.idx..idx_max)
            .expect("invalid range: this is a bug")
            .find(|c| c != ',' && !is_space(c))
            .unwrap_or(idx_max - self.idx);
    }

    fn skip_char(&mut self, chr: char) -> bool {
        match self.peek_char() {
            Some(c) if c == chr => {
                self.idx += 1;
                true
            }
            _ => false,
        }
    }

    fn peek_char(&self) -> Option<char> {
        self.list_str.get(self.idx..)?.chars().next()
    }

    fn set_range_iter(&mut self) -> io::Result<RangeIterStatus> {
        self.skip_delim();

        if let Some("\0") | Some("\n") = self.list_str.get(self.idx..) {
            return Ok(RangeIterStatus::Done);
        }

        let rng_iter = RangeIter::new(self.parse_next_num()?);
        if !self.skip_char('-') {
            self.range_iter = Some(rng_iter.check().map_err(|_| self.err_invalid())?);
            return Ok(RangeIterStatus::HasNext);
        }
        let rng_iter = rng_iter.set_end(self.parse_next_num()?);

        if !self.skip_char(':') {
            self.range_iter = Some(rng_iter.check().map_err(|_| self.err_invalid())?);
            return Ok(RangeIterStatus::HasNext);
        }

        let rng_iter = rng_iter.set_used_sized(self.parse_next_num()?);
        if !self.skip_char('/') {
            return Err(self.err_invalid());
        }
        let rng_iter = rng_iter.set_group_size(self.parse_next_num()?);

        self.range_iter = Some(rng_iter.check().map_err(|_| self.err_invalid())?);
        Ok(RangeIterStatus::HasNext)
    }

    fn next(&mut self) -> Option<io::Result<usize>> {
        loop {
            if let Some(ref mut iter) = self.range_iter {
                if let Some(v) = iter.next() {
                    return Some(Ok(v));
                }
            }
            match self.set_range_iter() {
                Ok(RangeIterStatus::HasNext) => {}
                Ok(RangeIterStatus::Done) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }

    fn err_invalid(&self) -> io::Error {
        let msg = format!("Unknown list format: {}", self.list_str);
        io::Error::new(io::ErrorKind::InvalidData, msg)
    }

    // Returns the first error if any elements were errors, otherwise returns the
    // collection of unwrapped elements
    #[cfg(test)]
    fn collect_ok<C>(self) -> io::Result<C>
    where
        C: std::iter::FromIterator<usize>,
    {
        let mut err = None;
        let c = self
            .map(|r| match r {
                Ok(v) => Some(v),
                Err(e) => {
                    err = Some(Err(e));
                    None
                }
            })
            .take_while(Option::is_some)
            .map(Option::unwrap)
            .collect::<C>();

        err.unwrap_or(Ok(c))
    }
}

impl Iterator for ListIterator {
    type Item = io::Result<usize>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
}

#[derive(Debug)]
enum RangeIterStatus {
    HasNext,
    Done,
}

/// Helper used internally by ListIterator
#[derive(Debug)]
struct RangeIter<T> {
    last_item: Option<usize>,
    beg: usize,
    end: usize,
    used_size: usize,
    group_size: usize,
    _marker: PhantomData<T>,
}

#[derive(Debug)]
struct Checked;
#[derive(Debug)]
struct Unchecked;

impl RangeIter<Unchecked> {
    fn new(begin: usize) -> Self {
        Self {
            last_item: None,
            beg: begin,
            end: begin,
            used_size: 1,
            group_size: 1,
            _marker: PhantomData::<Unchecked>,
        }
    }

    fn set_end(mut self, end: usize) -> Self {
        self.end = end;
        self
    }

    fn set_group_size(mut self, group_size: usize) -> Self {
        self.group_size = group_size;
        self
    }

    fn set_used_sized(mut self, used_size: usize) -> Self {
        self.used_size = used_size;
        self
    }

    fn check(self) -> Result<RangeIter<Checked>, ()> {
        if self.end < self.beg || self.group_size < self.used_size || self.group_size == 0 {
            return Err(());
        }
        Ok(RangeIter::<Checked> {
            last_item: self.last_item,
            beg: self.beg,
            end: self.end,
            used_size: self.used_size,
            group_size: self.group_size,
            _marker: PhantomData::<Checked>,
        })
    }
}

impl RangeIter<Checked> {
    fn next(&mut self) -> Option<usize> {
        if self.used_size == 0 {
            return None;
        }

        let last = match self.last_item {
            Some(l) => l,
            None => return self.ret(self.beg),
        };

        // at this point: 0 < self.used_size <= self.group_size
        if (last - self.beg) % self.group_size < self.used_size - 1 {
            self.ret(last.checked_add(1)?)
        } else {
            self.ret(
                last.checked_add(1)?
                    .checked_add(self.group_size - self.used_size)?,
            )
        }
    }

    fn ret(&mut self, v: usize) -> Option<usize> {
        self.last_item = Some(v);
        (v <= self.end).then(|| v)
    }
}

impl Iterator for RangeIter<Checked> {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        self.next()
    }
}

#[cfg(test)]
pub(super) mod test_helpers {
    use super::{io, Path};

    pub(super) struct HexBitIterator {
        hex_str: String,
        s_beg: usize,
        s_end: usize,
        bits_on_deck: Option<u64>,
        n_bits_on_deck: usize,
        bit_counter: usize,
    }

    impl HexBitIterator {
        pub(super) fn from_path(path: &Path) -> io::Result<Self> {
            let s = std::fs::read_to_string(&path)?;
            Self::from_str(s)
        }

        pub(super) fn from_str(s: impl Into<String>) -> io::Result<Self> {
            let hex_str = s.into();
            if !hex_str.is_ascii() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "input is not ascii",
                ));
            }
            let len = hex_str.len();
            Ok(Self {
                hex_str,
                s_beg: len,
                s_end: len,
                bits_on_deck: None,
                n_bits_on_deck: 0,
                bit_counter: 0,
            })
        }

        fn next(&mut self) -> Option<usize> {
            loop {
                match self.bits_on_deck {
                    Some(0) | None => {
                        self.set_on_deck()?;
                    }
                    Some(ref mut m) => {
                        let is_set = (*m & 1) != 0;
                        // `self.n_bits_on_deck` == 0 implies `self.bits_on_deck` == 0, which
                        // would take the branch above, so this cannot underflow
                        self.n_bits_on_deck -= 1;
                        self.bit_counter += 1;
                        *m >>= 1;
                        if is_set {
                            return Some(self.bit_counter - 1);
                        }
                    }
                }
            }
        }

        fn set_on_deck(&mut self) -> Option<u64> {
            // Count unused bits (these are all equal to 0)
            self.bit_counter += self.n_bits_on_deck;

            self.s_end = 1 + self
                .hex_str
                .get(..self.s_beg)?
                .rfind(|c: char| c.is_ascii_hexdigit())?;
            self.s_beg = self
                .hex_str
                .get(..self.s_end)?
                .rfind(|c: char| !c.is_ascii_hexdigit())
                .map_or(0, |ii| ii + 1);

            // Each hexadecimal char represents 4 bits and we only have space for 64 bits
            if self.s_end - self.s_beg > 16 {
                self.s_beg = self.s_end - 16;
            }

            let sub_str = self.hex_str.get(self.s_beg..self.s_end)?;
            // `sub_str` should only contain hexadecimal values at this point
            self.bits_on_deck = Some(u64::from_str_radix(sub_str, 16).expect("not a hex value"));
            // `sub_str` could represent less than 64 bits
            self.n_bits_on_deck = 4 * sub_str.len();
            self.bits_on_deck
        }
    }

    impl Iterator for HexBitIterator {
        type Item = usize;
        fn next(&mut self) -> Option<Self::Item> {
            self.next()
        }
    }
}

#[cfg(test)]
mod test {
    use super::{test_helpers::*, *};

    #[test]
    fn hex_bit_iterator() -> io::Result<()> {
        assert_eq!(
            HexBitIterator::from_str("")?.collect::<Vec<_>>(),
            Vec::<usize>::new()
        );
        assert_eq!(
            HexBitIterator::from_str("\n")?.collect::<Vec<_>>(),
            Vec::<usize>::new()
        );
        assert_eq!(
            HexBitIterator::from_str("00\n")?.collect::<Vec<_>>(),
            Vec::<usize>::new()
        );
        assert_eq!(
            HexBitIterator::from_str("0,0\n")?.collect::<Vec<_>>(),
            Vec::<usize>::new()
        );
        assert_eq!(
            HexBitIterator::from_str("03")?.collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            HexBitIterator::from_str("0c")?.collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(
            HexBitIterator::from_str("c0")?.collect::<Vec<_>>(),
            vec![6, 7]
        );
        assert_eq!(
            HexBitIterator::from_str("03,00\n")?.collect::<Vec<_>>(),
            vec![8, 9]
        );
        assert_eq!(
            HexBitIterator::from_str("03,fF")?.collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        );
        assert_eq!(
            HexBitIterator::from_str("03,Ff")?.collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        );

        {
            let n = 20;
            let mut s = "f".repeat(n);
            s.push_str(",\n,,\n,\n");
            assert_eq!(
                HexBitIterator::from_str(s)?.collect::<Vec<_>>(),
                (0..n * 4).collect::<Vec<_>>()
            );
        }

        {
            let n = 20;
            let s = "f,\n,,\n,\n".repeat(n);
            assert_eq!(
                HexBitIterator::from_str(s)?.collect::<Vec<_>>(),
                (0..n * 4).collect::<Vec<_>>()
            );
        }

        {
            let n = 20;
            let mut s = "f,\n,,\n,\n".repeat(n);
            let ss = "0".repeat(n);
            s.push_str(&ss);
            assert_eq!(
                HexBitIterator::from_str(s)?.collect::<Vec<_>>(),
                (n * 4..2 * (n * 4)).collect::<Vec<_>>()
            );
        }
        {
            let n = 20;
            let mut s = "0,\n,,\n,\n".repeat(n);
            let ss = "f,\n,,\n,\n".repeat(n);
            s.push_str(&ss);
            assert_eq!(
                HexBitIterator::from_str(s)?.collect::<Vec<_>>(),
                (0..n * 4).collect::<Vec<_>>()
            );
        }
        Ok(())
    }

    #[test]
    fn range_iter() -> Result<(), ()> {
        let it = RangeIter::new(5)
            .set_end(8)
            .set_group_size(5)
            .set_used_sized(0)
            .check()?
            .collect::<Vec<_>>();
        assert_eq!(it, Vec::<usize>::new());

        let it = RangeIter::new(5)
            .set_end(25)
            .set_group_size(50)
            .set_used_sized(50)
            .check()?
            .collect::<Vec<_>>();
        assert_eq!(it, (5..=25).collect::<Vec<_>>());

        let it = RangeIter::new(5)
            .set_end(8)
            .set_group_size(2)
            .set_used_sized(1)
            .check()?
            .collect::<Vec<_>>();
        assert_eq!(it, vec![5, 7]);

        let it = RangeIter::new(12)
            .set_end(32)
            .set_group_size(10)
            .set_used_sized(2)
            .check()?
            .collect::<Vec<_>>();
        assert_eq!(it, vec![12, 13, 22, 23, 32]);

        let it = RangeIter::new(10)
            .set_end(20)
            .set_group_size(5)
            .set_used_sized(2)
            .check()?
            .collect::<Vec<_>>();
        assert_eq!(it, vec![10, 11, 15, 16, 20]);

        let it = RangeIter::new(usize::MAX - 10)
            .set_end(usize::MAX)
            .set_group_size(5)
            .set_used_sized(1)
            .check()?
            .max()
            .unwrap();
        assert_eq!(it, usize::MAX);

        let it = RangeIter::new(usize::MAX - 9)
            .set_end(usize::MAX)
            .set_group_size(5)
            .set_used_sized(1)
            .check()?
            .max()
            .unwrap();
        assert_eq!(it, usize::MAX - 4);

        Ok(())
    }

    #[test]
    fn list_iterator() -> io::Result<()> {
        assert_eq!(
            ListIterator::from_str("\0")?
                .collect_ok::<Vec<_>>()
                .unwrap(),
            Vec::<usize>::new()
        );
        assert_eq!(
            ListIterator::from_str("\n")?
                .collect_ok::<Vec<_>>()
                .unwrap(),
            Vec::<usize>::new()
        );
        assert_eq!(
            ListIterator::from_str("0,8\n")?
                .collect_ok::<Vec<_>>()
                .unwrap(),
            vec![0, 8]
        );
        assert_eq!(
            ListIterator::from_str("0-3\n")?
                .collect_ok::<Vec<_>>()
                .unwrap(),
            vec![0, 1, 2, 3]
        );
        assert_eq!(
            ListIterator::from_str("0-3,8,12-16\0")?
                .collect_ok::<Vec<_>>()
                .unwrap(),
            vec![0, 1, 2, 3, 8, 12, 13, 14, 15, 16]
        );
        assert_eq!(
            ListIterator::from_str("9-38:4/10\0")?
                .collect_ok::<Vec<_>>()
                .unwrap(),
            vec![9, 10, 11, 12, 19, 20, 21, 22, 29, 30, 31, 32]
        );
        assert_eq!(
            ListIterator::from_str("0-1023:2/256\0")?
                .collect_ok::<Vec<_>>()
                .unwrap(),
            vec![0, 1, 256, 257, 512, 513, 768, 769]
        );
        assert_eq!(
            ListIterator::from_str("0-3,8,12-32:2/10\n")?
                .collect_ok::<Vec<_>>()
                .unwrap(),
            vec![0, 1, 2, 3, 8, 12, 13, 22, 23, 32]
        );
        assert!(ListIterator::from_str("-")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("3-")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("-3")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("0,-3")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("0--3")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("0-3,8,12\0\n")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("0-3,8,12-16,")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("0-3,,8,12-16")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("0-3\n8,12-16")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("0-3,5-80:9/8\0")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("5-80:9\0")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("5-80:9/\0")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("5-80:0/0\0")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("+0-3\n")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("!0-3\n")?.any(|e| e.is_err()));
        assert!(ListIterator::from_str("all\n")?.any(|e| e.is_err()));

        assert!(ListIterator::from_str("0-3,8,12\n\n")?.all(|e| e.is_ok()));
        assert!(ListIterator::from_str("0-3,8,12\n\0")?.all(|e| e.is_ok()));
        assert!(ListIterator::from_str("0-3,8,12-16,\n")?.all(|e| e.is_ok()));
        assert!(ListIterator::from_str("0-3,,8,12-16\n")?.all(|e| e.is_ok()));
        assert!(ListIterator::from_str("0-3\n8,12-16\0")?.all(|e| e.is_ok()));
        assert!(ListIterator::from_str("0-3,,\n\n,,8,12-16\n")?.all(|e| e.is_ok()));
        assert!(ListIterator::from_str("0-3,5-80:3/8,99,101-112\0")?.all(|e| e.is_ok()));
        assert!(ListIterator::from_str("5-80:0/1\0")?.all(|e| e.is_ok()));
        assert!(ListIterator::from_str(",,1,,4-6,,       ,,\0")?.any(|e| e.is_ok()));

        assert!(
            ListIterator::from_str("collect_ok_err\0")?
                .collect_ok::<Vec<_>>()
                .is_err()
        );

        Ok(())
    }

    #[test]
    fn list_and_mask_iterator_consistent_this_machine_sysfs() {
        let sysfs_path = Path::new("/sys/devices/system");
        let cpus_online = ListIterator::from_path(&sysfs_path.join("cpu/online")).unwrap();
        for cpu in cpus_online {
            let cpu = cpu.unwrap();
            let cpu_topology = sysfs_path.join(format!("cpu/cpu{}/topology", cpu));
            let paths = ["core_cpus", "core_siblings", "die_cpus", "package_cpus"];
            for path in &paths {
                let f_mask = cpu_topology.join(path);
                let f_list = cpu_topology.join(format!("{}_list", path));
                assert_eq!(
                    HexBitIterator::from_path(&f_mask)
                        .unwrap()
                        .collect::<Vec<_>>(),
                    ListIterator::from_path(&f_list)
                        .unwrap()
                        .map(Result::unwrap)
                        .collect::<Vec<_>>(),
                );
            }
        }
    }
}
