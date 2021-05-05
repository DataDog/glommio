use crate::{
    io::{DmaFile, ReadResult},
    sys::Source,
};
use core::task::{Context, Poll};
use futures_lite::Stream;
use std::{collections::VecDeque, os::unix::io::AsRawFd, pin::Pin, rc::Rc};

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
