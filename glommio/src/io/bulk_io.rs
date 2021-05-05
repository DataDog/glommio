use crate::{
    io::{DmaFile, ReadResult},
    sys::Source,
};
use core::task::{Context, Poll};
use futures_lite::Stream;
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

#[derive(Debug, Copy, Clone)]
pub(crate) struct ReadManyArgs {
    pub(crate) user_read: (u64, usize),
    pub(crate) system_read: (u64, usize),
}

#[derive(Debug)]
pub struct ReadManyResult {
    pub(crate) inner: OrderedBulkIo<ReadManyArgs>,
    pub(crate) current_result: ReadResult,
}

impl Stream for ReadManyResult {
    type Item = super::Result<(u64, ReadResult)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(next) => next,
            Poll::Pending => return Poll::Pending,
        };

        match next {
            None => Poll::Ready(None),
            Some((source, args)) => {
                if let Some(mut source) = source {
                    let read_size =
                        enhanced_try!(source.take_result().unwrap(), "Reading", self.inner.file)?;
                    let mut buffer = source.extract_dma_buffer();
                    buffer.trim_to_size(std::cmp::min(read_size, args.system_read.1));
                    self.current_result = ReadResult::from_whole_buffer(buffer);
                }
                Poll::Ready(Some(Ok((
                    args.user_read.0,
                    ReadResult::slice(
                        &self.current_result,
                        (args.user_read.0 - args.system_read.0) as usize,
                        args.user_read.1,
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

pub(crate) struct CoalescedReads<S: Iterator<Item = (u64, usize)>> {
    iter: MultiPeek<S>,
    next_merged: Option<(usize, (u64, usize))>,
    max_merged_buffer_size: usize,
    max_read_amp: Option<usize>,
}

impl<S: Iterator<Item = (u64, usize)>> CoalescedReads<S> {
    pub(crate) fn new(
        iter: S,
        max_merged_buffer_size: usize,
        max_read_amp: Option<usize>,
    ) -> CoalescedReads<S> {
        CoalescedReads {
            iter: iter.multipeek(),
            next_merged: None,
            max_merged_buffer_size,
            max_read_amp,
        }
    }
}

impl<S: Iterator<Item = (u64, usize)>> Iterator for CoalescedReads<S> {
    // CoalescedReads returns the original (offset, size) and the (offset, size) it
    // was merged in
    type Item = ((u64, usize), (u64, usize));

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
                    current = Some((x.0, x.1));
                    taken = 1;
                    continue;
                }
                Some(cur) => {
                    if let Some(gap) = self.max_read_amp {
                        // if the read gap is > to the max configured, don't merge
                        if u64::saturating_sub(cur.0, x.0) > gap as u64 {
                            break;
                        }
                        if u64::saturating_sub(x.0, cur.0 + cur.1 as u64) > gap as u64 {
                            break;
                        }
                    }
                    let merged: (u64, u64) =
                        (min(cur.0, x.0), max(cur.0 + cur.1 as u64, x.0 + x.1 as u64));
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
