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
