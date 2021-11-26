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

pub(crate) struct MergedIOVecs<V: IoVec> {
    pub(crate) coalesced_user_iovecs: VecDeque<V>,
    pub(crate) pos: u64,
    pub(crate) size: usize,
}

impl<V: IoVec> IoVec for MergedIOVecs<V> {
    fn pos(&self) -> u64 {
        self.pos
    }

    fn size(&self) -> usize {
        self.size
    }
}

impl<V: IoVec> MergedIOVecs<V> {
    fn deduplicate(&mut self, mut other: Self) -> Option<Self> {
        if self.pos() == other.pos() && self.size() == other.size() {
            self.coalesced_user_iovecs
                .append(&mut other.coalesced_user_iovecs);
            None
        } else {
            Some(std::mem::replace(self, other))
        }
    }
}

impl<V: IoVec> Iterator for MergedIOVecs<V> {
    type Item = (V, (u64, usize));

    fn next(&mut self) -> Option<Self::Item> {
        self.coalesced_user_iovecs
            .pop_front()
            .map(|io| (io, (self.pos(), self.size())))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.coalesced_user_iovecs.len(),
            Some(self.coalesced_user_iovecs.len()),
        )
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
                        let (pos, size) = self.current.replace((pos, size)).unwrap();
                        self.merged.push_front(io);

                        return Some(MergedIOVecs {
                            coalesced_user_iovecs: self.merged.drain(1..).collect(),
                            pos,
                            size,
                        });
                    }
                }
                let merged: (u64, u64) = (
                    min(cur.pos(), pos),
                    max(cur.pos() + cur.size() as u64, pos + size as u64),
                );
                if merged.1 - merged.0 > self.max_merged_buffer_size as u64 {
                    // if the merged buffer is too large, don't merge
                    let (pos, size) = self.current.replace((pos, size)).unwrap();
                    self.merged.push_front(io);

                    return Some(MergedIOVecs {
                        coalesced_user_iovecs: self.merged.drain(1..).collect(),
                        pos,
                        size,
                    });
                }
                self.merged.push_back(io);
                self.current = Some((merged.0, (merged.1 - merged.0) as usize));
                None
            }
        }
    }

    pub(super) fn flush(&mut self) -> Option<MergedIOVecs<V>> {
        self.current.map(|x| MergedIOVecs {
            coalesced_user_iovecs: self.merged.drain(..).collect(),
            pos: x.pos(),
            size: x.size(),
        })
    }
}

pub(crate) struct CoalescedReads<V: IoVec + Unpin, S: Stream<Item = V> + Unpin> {
    iter: S,
    merger: Option<IOVecMerger<V>>,
    alignment: Option<u64>,

    last: Option<MergedIOVecs<V>>,
}

impl<V: IoVec + Unpin, S: Stream<Item = V> + Unpin> CoalescedReads<V, S> {
    pub(crate) fn new(
        max_merged_buffer_size: usize,
        max_read_amp: Option<usize>,
        alignment: Option<u64>,
        iter: S,
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

impl<V: IoVec + Unpin, S: Stream<Item = V> + Unpin> Stream for CoalescedReads<V, S> {
    // CoalescedReads returns the original (offset, size) and the (offset, size) it
    // was merged in
    type Item = MergedIOVecs<V>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let align = |mut merged_iovec: MergedIOVecs<V>, alignment: u64| {
            let (pos, size) = (merged_iovec.pos(), merged_iovec.size());
            let eff_pos = align_down(pos, alignment);
            let b = (pos - eff_pos) as usize;
            merged_iovec.pos = eff_pos;
            merged_iovec.size = align_up((size + b) as u64, alignment) as usize;
            merged_iovec
        };

        let next_inner = |this: &mut Self, cx: &mut Context<'_>| {
            // pull IO requests from the underlying stream and attempt to merge them with
            // the previous ones, if any.
            // To avoid adding undo latency, we flush whatever is in the merger if the
            // underlying stream returns Poll::Pending.
            loop {
                match this.iter.poll_next(cx) {
                    Poll::Ready(Some(io)) => {
                        if let Some(merged) = this.merger.as_mut().unwrap().merge(io) {
                            return Poll::Ready(Some(merged));
                        }
                    }
                    Poll::Pending => {
                        return if let Some(merged) = this.merger.as_mut().unwrap().flush() {
                            Poll::Ready(Some(merged))
                        } else {
                            Poll::Pending
                        };
                    }
                    _ => break,
                }
            }

            // the underlying stream is closed so pull out of the merger whatever is there,
            // if anything
            Poll::Ready(if let Some(mut merger) = this.merger.take() {
                merger.flush()
            } else {
                None
            })
        };

        let this = self.get_mut();
        while let Some(mut inner) = ready!(next_inner(this, cx)) {
            if let Some(alignment) = this.alignment {
                inner = align(inner, alignment);
            }

            // two subsequent merged IO requests may ask for the exact same data (because we
            // align then up and down after merging) so there is one more opportunity to
            // deduplicate here
            if let Some(last) = &mut this.last {
                if let Some(last) = last.deduplicate(inner) {
                    return Poll::Ready(Some(last));
                }
            } else {
                this.last = Some(inner);
            }
        }
        Poll::Ready(this.last.take())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self.iter.size_hint() {
            (low, Some(hi)) => (low.min(1), Some(hi)),
            (low, None) => (low.min(1), None),
        }
    }
}

#[derive(Debug)]
pub(crate) struct OrderedBulkIo<U: Unpin, S: Stream<Item = (ScheduledSource, U)> + Unpin> {
    file: Rc<DmaFile>,
    iovs: S,

    inflight: VecDeque<(ScheduledSource, U)>,
    cap: usize,
    terminated: bool,
}

impl<U: Unpin, S: Stream<Item = (ScheduledSource, U)> + Unpin> OrderedBulkIo<U, S> {
    pub(crate) fn new(file: Rc<DmaFile>, concurrency: usize, iovs: S) -> OrderedBulkIo<U, S> {
        assert!(concurrency > 0);
        OrderedBulkIo {
            file,
            iovs,
            inflight: VecDeque::with_capacity(concurrency),
            cap: concurrency,
            terminated: false,
        }
    }

    pub(crate) fn set_concurrency(&mut self, concurrency: usize) {
        assert!(concurrency > 0);
        assert!(
            !self.terminated && self.inflight.is_empty(),
            "should be called before the first call to poll()"
        );
        self.inflight.reserve(concurrency);
        self.cap = concurrency
    }
}

impl<U: Unpin, S: Stream<Item = (ScheduledSource, U)> + Unpin> Stream for OrderedBulkIo<U, S> {
    type Item = (ScheduledSource, U);

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Poll the underlying stream and insert the resulting source, if any, in the
        // local buffer
        let poll_inner = |this: &mut Self, cx: &mut Context<'_>| match this.iovs.poll_next(cx) {
            Poll::Ready(Some(res)) => this.inflight.push_front(res),
            Poll::Ready(None) => this.terminated = true,
            _ => {}
        };

        // poll the local buffer for a fulfilled source, if any, and replace it with a
        // new one from the underlying stream
        let poll_buffer = |this: &mut Self, cx: &mut Context<'_>| {
            if this.inflight.back_mut().unwrap().0.result().is_some() {
                // we have a source with a result in the buffer so we take it out and replace it
                // with a new from the stream, if any, to keep the buffer full
                let ret = this.inflight.pop_back().unwrap();
                if !this.terminated {
                    poll_inner(this, cx);
                }
                Poll::Ready(Some(ret))
            } else {
                // we have a source in the buffer but it's not ready yet to we register the
                // buffer and return
                this.inflight
                    .back_mut()
                    .unwrap()
                    .0
                    .add_waiter_many(cx.waker().clone());
                Poll::Pending
            }
        };

        if this.inflight.len() == this.cap || (this.terminated && !this.inflight.is_empty()) {
            // The internal buffer is full so we consume them instead of creating new ones
            poll_buffer(this, cx)
        } else {
            // fill the internal buffer as much as possible
            while this.inflight.len() < this.cap && !this.terminated {
                poll_inner(this, cx);
            }

            // if there is anything we can return immediately, do so
            if !this.terminated || !this.inflight.is_empty() {
                poll_buffer(this, cx)
            } else {
                Poll::Ready(None)
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self.iovs.size_hint() {
            (low, Some(hi)) => (low + self.inflight.len(), Some(hi + self.inflight.len())),
            (low, None) => (low + self.inflight.len(), None),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadManyArgs<V: IoVec + Unpin> {
    pub(crate) user_reads: VecDeque<V>,
    pub(crate) system_read: (u64, usize),
}

/// A stream of ReadResult produced asynchronously.
///
/// See [`DmaFile::read_many`] for more information
#[derive(Debug)]
pub struct ReadManyResult<
    V: IoVec + Unpin,
    S: Stream<Item = (ScheduledSource, ReadManyArgs<V>)> + Unpin,
> {
    pub(crate) inner: OrderedBulkIo<ReadManyArgs<V>, S>,
    pub(crate) current: Option<(ScheduledSource, ReadManyArgs<V>)>,
}

impl<V: IoVec + Unpin, S: Stream<Item = (ScheduledSource, ReadManyArgs<V>)> + Unpin>
    ReadManyResult<V, S>
{
    /// Set the amount of IO concurrency of this stream, i.e., the number of IO
    /// requests this stream will submit at any one time
    ///
    /// Higher concurrency levels may improve performance and will extend the
    /// lifetime of IO requests, meaning they have a greater chance of being
    /// reused via IO request deduplication.
    /// However, higher values will increase memory usage and possibly starve
    /// by-standing IO-emitting tasks.
    ///
    /// This function should be called before the stream is first polled and
    /// will panic otherwise.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.inner.set_concurrency(concurrency);
        self
    }
}

impl<V: IoVec + Unpin, S: Stream<Item = (ScheduledSource, ReadManyArgs<V>)> + Unpin> Stream
    for ReadManyResult<V, S>
{
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
    use futures_lite::{stream, StreamExt};

    async fn collect_iovecs<V: IoVec, S: Stream<Item = MergedIOVecs<V>> + Unpin>(
        stream: S,
    ) -> Vec<(V, (u64, usize))> {
        stream
            .collect::<Vec<S::Item>>()
            .await
            .into_iter()
            .flatten()
            .collect()
    }

    #[test]
    fn monotonic_iovec_merging() {
        test_executor!(async move {
            let reads: Vec<(u64, usize)> =
                vec![(0, 64), (0, 256), (32, 4064), (4095, 10), (8192, 4096)];
            let merged = collect_iovecs(CoalescedReads::new(
                4096,
                None,
                None,
                stream::iter(reads.iter().copied()),
            ))
            .await;

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
        });
    }

    #[test]
    fn large_input_passthrough() {
        test_executor!(async move {
            let reads: Vec<(u64, usize)> =
                vec![(0, 64), (0, 256), (32, 4064), (4095, 10), (8192, 4096)];
            let merged = collect_iovecs(CoalescedReads::new(
                500,
                None,
                None,
                stream::iter(reads.iter().copied()),
            ))
            .await;

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

            let merged = collect_iovecs(CoalescedReads::new(
                0,
                None,
                None,
                stream::iter(reads.iter().copied()),
            ))
            .await;

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
        });
    }

    #[test]
    fn nonmonotonic_iovec_merging() {
        test_executor!(async move {
            let reads: Vec<(u64, usize)> = vec![(64, 256), (32, 4064), (4095, 10), (8192, 4096)];
            let merged = collect_iovecs(CoalescedReads::new(
                4096,
                None,
                None,
                stream::iter(reads.iter().copied()),
            ))
            .await;

            assert_eq!(
                merged,
                [
                    ((64, 256), (32, 4073)),
                    ((32, 4064), (32, 4073)),
                    ((4095, 10), (32, 4073)),
                    ((8192, 4096), (8192, 4096))
                ]
            );
        });
    }

    #[test]
    fn read_amplification_limit() {
        test_executor!(async move {
            let reads: Vec<(u64, usize)> = vec![
                (128, 128),
                (128, 1),
                (160, 96),
                (96, 160),
                (64, 192),
                (0, 256),
            ];
            let merged = collect_iovecs(CoalescedReads::new(
                4096,
                Some(0),
                None,
                stream::iter(reads.iter().copied()),
            ))
            .await;

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

            let merged = collect_iovecs(CoalescedReads::new(
                4096,
                Some(32),
                None,
                stream::iter(reads.iter().copied()),
            ))
            .await;

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
        });
    }

    #[test]
    fn read_no_coalescing() {
        test_executor!(async move {
            let reads: Vec<(u64, usize)> = vec![
                (128, 128),
                (128, 1),
                (160, 96),
                (96, 160),
                (64, 192),
                (0, 256),
            ];
            let merged = collect_iovecs(CoalescedReads::new(
                0,
                Some(0),
                None,
                stream::iter(reads.iter().copied()),
            ))
            .await;

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
        });
    }
}
