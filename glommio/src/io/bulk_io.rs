use crate::io::{
    dma_file::{align_down, align_up},
    DmaFile,
    ReadResult,
    ScheduledSource,
};
use core::task::{Context, Poll};
use futures_lite::{ready, Stream, StreamExt};
use std::{
    cmp::{max, min},
    collections::VecDeque,
    os::unix::io::AsRawFd,
    pin::Pin,
    rc::Rc,
};

/// An interface to an IO vector.
pub trait IoVec {
    /// The read position (the offset) in the file
    fn pos(&self) -> u64;
    /// The number of bytes to read at [`Self::pos`]
    fn size(&self) -> usize;
}

impl IoVec for (u64, usize) {
    fn pos(&self) -> u64 {
        self.0
    }

    fn size(&self) -> usize {
        self.1
    }
}

pub(crate) struct MergedIOVecs<V: IoVec>(pub(crate) VecDeque<V>, pub(crate) (u64, usize));

impl<V: IoVec> MergedIOVecs<V> {
    fn combine(&mut self, mut other: Self) -> Option<Self> {
        if self.1 == other.1 {
            self.0.append(&mut other.0);
            None
        } else {
            Some(std::mem::replace(self, other))
        }
    }
}

impl<V: IoVec> Iterator for MergedIOVecs<V> {
    type Item = (V, (u64, usize));

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(io) = self.0.pop_front() {
            Some((io, self.1))
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.0.len(), Some(self.0.len()))
    }
}

struct IOVecMerger<V: IoVec> {
    max_merged_buffer_size: usize,
    max_read_amp: Option<usize>,

    current: Option<(u64, usize)>,
    merged: VecDeque<V>,
}

impl<V: IoVec> IOVecMerger<V> {
    pub(super) fn merge(&mut self, io: V) -> Option<MergedIOVecs<V>> {
        let (pos, size) = (io.pos(), io.size());
        match self.current {
            None => {
                self.current = Some((pos, size));
                self.merged.push_back(io);
                None
            }
            Some(cur) => {
                if let Some(gap) = self.max_read_amp {
                    // if the read gap is > to the max configured, don't merge
                    if u64::saturating_sub(cur.pos(), pos) > gap as u64
                        || u64::saturating_sub(pos, cur.pos() + cur.size() as u64) > gap as u64
                    {
                        return Some(MergedIOVecs(
                            std::mem::replace(&mut self.merged, VecDeque::from(vec![io])),
                            self.current.replace((pos, size)).unwrap(),
                        ));
                    }
                }
                let merged: (u64, u64) = (
                    min(cur.pos(), pos),
                    max(cur.pos() + cur.size() as u64, pos + size as u64),
                );
                if merged.1 - merged.0 > self.max_merged_buffer_size as u64 {
                    // if the merged buffer is too large, don't merge
                    return Some(MergedIOVecs(
                        std::mem::replace(&mut self.merged, VecDeque::from(vec![io])),
                        self.current.replace((pos, size)).unwrap(),
                    ));
                }
                self.merged.push_back(io);
                self.current = Some((merged.0, (merged.1 - merged.0) as usize));
                None
            }
        }
    }

    pub(super) fn flush(self) -> Option<MergedIOVecs<V>> {
        self.current.map(|x| MergedIOVecs(self.merged, x))
    }
}

pub(crate) struct CoalescedReads<V: IoVec, S: Iterator<Item = V>> {
    iter: S,
    merger: Option<IOVecMerger<V>>,
    alignment: Option<u64>,

    last: Option<MergedIOVecs<V>>,
}

impl<V: IoVec, S: Iterator<Item = V>> CoalescedReads<V, S> {
    pub(crate) fn new(
        iter: S,
        max_merged_buffer_size: usize,
        max_read_amp: Option<usize>,
        alignment: Option<u64>,
    ) -> CoalescedReads<V, S> {
        CoalescedReads {
            iter,
            merger: Some(IOVecMerger {
                max_merged_buffer_size,
                max_read_amp,
                current: None,
                merged: Default::default(),
            }),
            alignment,
            last: None,
        }
    }
}

impl<V: IoVec, S: Iterator<Item = V>> Iterator for CoalescedReads<V, S> {
    // CoalescedReads returns the original (offset, size) and the (offset, size) it
    // was merged in
    type Item = MergedIOVecs<V>;

    fn next(&mut self) -> Option<Self::Item> {
        let align = |mut merged_iovec: MergedIOVecs<V>, alignment: u64| {
            let (pos, size) = (merged_iovec.1.0, merged_iovec.1.1);
            let eff_pos = align_down(pos, alignment);
            let b = (pos - eff_pos) as usize;
            merged_iovec.1 = (eff_pos, align_up((size + b) as u64, alignment) as usize);
            merged_iovec
        };

        let next_inner = |this: &mut Self| {
            for io in &mut this.iter {
                if let Some(merged) = this.merger.as_mut().unwrap().merge(io) {
                    return Some(merged);
                }
            }

            if let Some(merger) = this.merger.take() {
                merger.flush()
            } else {
                None
            }
        };

        while let Some(mut inner) = next_inner(self) {
            if let Some(alignment) = self.alignment {
                inner = align(inner, alignment);
            }

            if let Some(last) = &mut self.last {
                if let Some(last) = last.combine(inner) {
                    return Some(last);
                }
            } else {
                self.last = Some(inner);
            }
        }
        self.last.take()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

#[derive(Debug)]
pub(crate) struct OrderedBulkIo<U> {
    file: Rc<DmaFile>,
    iovs: VecDeque<(ScheduledSource, U)>,
}

impl<U: Unpin> OrderedBulkIo<U> {
    pub(crate) fn new<S: Iterator<Item = (ScheduledSource, U)>>(
        file: Rc<DmaFile>,
        iovs: S,
    ) -> OrderedBulkIo<U> {
        OrderedBulkIo {
            file,
            iovs: iovs.collect(),
        }
    }
}

impl<U: Unpin> Stream for OrderedBulkIo<U> {
    type Item = (ScheduledSource, U);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.iovs.front_mut() {
            None => Poll::Ready(None),
            Some((source, _)) => {
                let res = if source.result().is_some() {
                    Poll::Ready(Some(self.iovs.pop_front().unwrap()))
                } else {
                    Poll::Pending
                };

                if let Some((source, _)) = self.iovs.front_mut() {
                    source.add_waiter_many(cx.waker().clone());
                }
                res
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.iovs.len(), Some(self.iovs.len()))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ReadManyArgs<V: IoVec + Unpin> {
    pub(crate) user_reads: VecDeque<V>,
    pub(crate) system_read: (u64, usize),
}

/// A stream of ReadResult produced asynchronously.
///
/// See [`DmaFile::read_many`] for more information
#[derive(Debug)]
pub struct ReadManyResult<V: IoVec + Unpin> {
    pub(crate) inner: OrderedBulkIo<ReadManyArgs<V>>,
    pub(crate) current: Option<(ScheduledSource, ReadManyArgs<V>)>,
}

impl<V: IoVec + Unpin> Stream for ReadManyResult<V> {
    type Item = super::Result<(V, ReadResult)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some((source, args)) = &mut self.current {
            if let Some(io) = args.user_reads.pop_front() {
                let (pos, size) = (io.pos(), io.size());
                return Poll::Ready(Some(Ok((
                    io,
                    ReadResult::from_sliced_buffer(
                        source.clone(),
                        (pos - args.system_read.pos()) as usize,
                        size,
                    ),
                ))));
            }
        }

        match ready!(self.inner.poll_next(cx)) {
            None => Poll::Ready(None),
            Some((source, args)) => {
                enhanced_try!(source.result().unwrap(), "Reading", self.inner.file)?;
                self.current = Some((source, args));
                self.poll_next(cx)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_iovec_merging() {
        let reads: Vec<(u64, usize)> =
            vec![(0, 64), (0, 256), (32, 4064), (4095, 10), (8192, 4096)];
        let merged: Vec<((u64, usize), (u64, usize))> =
            CoalescedReads::new(reads.iter().copied(), 4096, None, None)
                .flatten()
                .collect();

        assert_eq!(
            merged,
            [
                ((0, 64), (0, 4096)),
                ((0, 256), (0, 4096)),
                ((32, 4064), (0, 4096)),
                ((4095, 10), (4095, 10)),
                ((8192, 4096), (8192, 4096))
            ]
        );
    }

    #[test]
    fn large_input_passthrough() {
        let reads: Vec<(u64, usize)> =
            vec![(0, 64), (0, 256), (32, 4064), (4095, 10), (8192, 4096)];
        let merged: Vec<((u64, usize), (u64, usize))> =
            CoalescedReads::new(reads.iter().copied(), 500, None, None)
                .flatten()
                .collect();

        assert_eq!(
            merged,
            [
                ((0, 64), (0, 256)),
                ((0, 256), (0, 256)),
                ((32, 4064), (32, 4064)),
                ((4095, 10), (4095, 10)),
                ((8192, 4096), (8192, 4096))
            ]
        );

        let merged: Vec<((u64, usize), (u64, usize))> =
            CoalescedReads::new(reads.iter().copied(), 0, None, None)
                .flatten()
                .collect();

        assert_eq!(
            merged,
            [
                ((0, 64), (0, 64)),
                ((0, 256), (0, 256)),
                ((32, 4064), (32, 4064)),
                ((4095, 10), (4095, 10)),
                ((8192, 4096), (8192, 4096))
            ]
        );
    }

    #[test]
    fn nonmonotonic_iovec_merging() {
        let reads: Vec<(u64, usize)> = vec![(64, 256), (32, 4064), (4095, 10), (8192, 4096)];
        let merged: Vec<((u64, usize), (u64, usize))> =
            CoalescedReads::new(reads.iter().copied(), 4096, None, None)
                .flatten()
                .collect();

        assert_eq!(
            merged,
            [
                ((64, 256), (32, 4073)),
                ((32, 4064), (32, 4073)),
                ((4095, 10), (32, 4073)),
                ((8192, 4096), (8192, 4096))
            ]
        );
    }

    #[test]
    fn read_amplification_limit() {
        let reads: Vec<(u64, usize)> = vec![
            (128, 128),
            (128, 1),
            (160, 96),
            (96, 160),
            (64, 192),
            (0, 256),
        ];
        let merged: Vec<((u64, usize), (u64, usize))> =
            CoalescedReads::new(reads.iter().copied(), 4096, Some(0), None)
                .flatten()
                .collect();

        assert_eq!(
            merged,
            [
                ((128, 128), (128, 128)),
                ((128, 1), (128, 128)),
                ((160, 96), (128, 128)),
                ((96, 160), (96, 160)),
                ((64, 192), (64, 192)),
                ((0, 256), (0, 256)),
            ]
        );

        let merged: Vec<((u64, usize), (u64, usize))> =
            CoalescedReads::new(reads.iter().copied(), 4096, Some(32), None)
                .flatten()
                .collect();

        assert_eq!(
            merged,
            [
                ((128, 128), (64, 192)),
                ((128, 1), (64, 192)),
                ((160, 96), (64, 192)),
                ((96, 160), (64, 192)),
                ((64, 192), (64, 192)),
                ((0, 256), (0, 256)),
            ]
        );
    }

    #[test]
    fn read_no_coalescing() {
        let reads: Vec<(u64, usize)> = vec![
            (128, 128),
            (128, 1),
            (160, 96),
            (96, 160),
            (64, 192),
            (0, 256),
        ];
        let merged: Vec<((u64, usize), (u64, usize))> =
            CoalescedReads::new(reads.iter().copied(), 0, Some(0), None)
                .flatten()
                .collect();

        assert_eq!(
            merged,
            [
                ((128, 128), (128, 128)),
                ((128, 1), (128, 1)),
                ((160, 96), (160, 96)),
                ((96, 160), (96, 160)),
                ((64, 192), (64, 192)),
                ((0, 256), (0, 256)),
            ]
        );
    }
}
