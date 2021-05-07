use crate::{
    io::{DmaFile, ReadResult},
    sys::Source,
};
use core::task::{Context, Poll};
use futures_lite::{ready, Stream, StreamExt};
use itertools::{Itertools, MultiPeek};
use std::{
    cmp::{max, min},
    collections::VecDeque,
    os::unix::io::AsRawFd,
    pin::Pin,
    rc::Rc,
};

#[derive(Debug)]
pub(crate) struct OrderedBulkIo<U> {
    file: Rc<DmaFile>,
    iovs: VecDeque<(Option<Source>, U)>,
}

impl<U: Copy + Unpin> OrderedBulkIo<U> {
    pub(crate) fn new<S: Iterator<Item = (Option<Source>, U)>>(
        file: Rc<DmaFile>,
        iovs: S,
    ) -> OrderedBulkIo<U> {
        OrderedBulkIo {
            file,
            iovs: iovs.collect(),
        }
    }
}

impl<U: Copy + Unpin> Stream for OrderedBulkIo<U> {
    type Item = (Option<Source>, U);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.iovs.front() {
            None => Poll::Ready(None),
            Some((Some(source), _)) => {
                let res = if source.has_result() {
                    Poll::Ready(Some(self.iovs.pop_front().unwrap()))
                } else {
                    Poll::Pending
                };
                if let Some((Some(source), _)) = self.iovs.front() {
                    source.add_waiter(cx.waker().clone());
                }
                res
            }
            Some((None, _)) => Poll::Ready(Some(self.iovs.pop_front().unwrap())),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.iovs.len(), Some(self.iovs.len()))
    }
}

/// An interface to an IO vector.
pub trait IoVec: Copy + Unpin {
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

#[derive(Debug, Copy, Clone)]
pub(crate) struct ReadManyArgs<V: IoVec> {
    pub(crate) user_read: V,
    pub(crate) system_read: (u64, usize),
}

/// A stream of ReadResult produced asynchronously.
///
/// See [`DmaFile::read_many`] for more information
#[derive(Debug)]
pub struct ReadManyResult<V: IoVec> {
    pub(crate) inner: OrderedBulkIo<ReadManyArgs<V>>,
    pub(crate) current_result: ReadResult,
}

impl<V: IoVec> Stream for ReadManyResult<V> {
    type Item = super::Result<(V, ReadResult)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.inner.poll_next(cx)) {
            None => Poll::Ready(None),
            Some((source, args)) => {
                if let Some(mut source) = source {
                    enhanced_try!(source.take_result().unwrap(), "Reading", self.inner.file)?;
                    self.current_result =
                        ReadResult::from_whole_buffer(source.extract_dma_buffer());
                }
                Poll::Ready(Some(Ok((
                    args.user_read,
                    ReadResult::slice(
                        &self.current_result,
                        (args.user_read.pos() - args.system_read.pos()) as usize,
                        args.user_read.size(),
                    )
                    .unwrap_or_default(),
                ))))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

pub(crate) struct CoalescedReads<V: IoVec, S: Iterator<Item = V>> {
    iter: MultiPeek<S>,
    next_merged: Option<(usize, (u64, usize))>,
    max_merged_buffer_size: usize,
    max_read_amp: Option<usize>,
}

impl<V: IoVec, S: Iterator<Item = V>> CoalescedReads<V, S> {
    pub(crate) fn new(
        iter: S,
        max_merged_buffer_size: usize,
        max_read_amp: Option<usize>,
    ) -> CoalescedReads<V, S> {
        CoalescedReads {
            iter: iter.multipeek(),
            next_merged: None,
            max_merged_buffer_size,
            max_read_amp,
        }
    }
}

impl<V: IoVec, S: Iterator<Item = V>> Iterator for CoalescedReads<V, S> {
    // CoalescedReads returns the original (offset, size) and the (offset, size) it
    // was merged in
    type Item = (V, (u64, usize));

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.next_merged {
            if next.0 == 1 {
                self.next_merged = None;
            } else {
                self.next_merged = Some((next.0 - 1, next.1));
            }
            return Some((self.iter.next().unwrap(), next.1));
        }

        let mut current: Option<(u64, usize)> = None;
        let mut taken = 0;
        while let Some(x) = self.iter.peek() {
            match current {
                None => {
                    current = Some((x.pos(), x.size()));
                    taken = 1;
                    continue;
                }
                Some(cur) => {
                    if let Some(gap) = self.max_read_amp {
                        // if the read gap is > to the max configured, don't merge
                        if u64::saturating_sub(cur.pos(), x.pos()) > gap as u64 {
                            break;
                        }
                        if u64::saturating_sub(x.pos(), cur.pos() + cur.size() as u64) > gap as u64
                        {
                            break;
                        }
                    }
                    let merged: (u64, u64) = (
                        min(cur.pos(), x.pos()),
                        max(cur.pos() + cur.size() as u64, x.pos() + x.size() as u64),
                    );
                    if merged.1 - merged.0 > self.max_merged_buffer_size as u64 {
                        // if the merged buffer is too large, don't merge
                        break;
                    }
                    taken += 1;
                    current = Some((merged.0, (merged.1 - merged.0) as usize));
                }
            }
        }

        if taken > 0 {
            self.next_merged = Some((taken, current.unwrap()));
            self.next()
        } else {
            None
        }
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
            CoalescedReads::new(reads.iter().copied(), 4096, None).collect();

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
            CoalescedReads::new(reads.iter().copied(), 500, None).collect();

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
            CoalescedReads::new(reads.iter().copied(), 0, None).collect();

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
            CoalescedReads::new(reads.iter().copied(), 4096, None).collect();

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
            CoalescedReads::new(reads.iter().copied(), 4096, Some(0)).collect();

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
            CoalescedReads::new(reads.iter().copied(), 4096, Some(32)).collect();

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
}
