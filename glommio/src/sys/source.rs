// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.
//
use crate::{
    iou::sqe::{SockAddr, SockAddrStorage},
    sys::{
        DmaBuffer,
        IoBuffer,
        OsResult,
        PollableStatus,
        ReactorQueue,
        SourceId,
        TimeSpec64,
        Wakers,
    },
    GlommioError,
    IoRequirements,
    ReactorErrorKind,
    RingIoStats,
    TaskQueueHandle,
};
use futures_lite::{future, io};
use std::{
    cell::{Ref, RefCell, RefMut},
    convert::TryFrom,
    ffi::CString,
    fmt,
    mem::MaybeUninit,
    os::unix::io::RawFd,
    path::PathBuf,
    pin::Pin,
    rc::Rc,
    task::{Poll, Waker},
    time::Duration,
};

#[derive(Debug)]
pub(crate) enum SourceType {
    Write(PollableStatus, IoBuffer),
    Read(PollableStatus, Option<IoBuffer>),
    PollAdd,
    SockSend(DmaBuffer),
    SockRecv(Option<DmaBuffer>),
    SockRecvMsg(
        Option<DmaBuffer>,
        libc::iovec,
        libc::msghdr,
        MaybeUninit<nix::sys::socket::sockaddr_storage>,
    ),
    SockSendMsg(
        DmaBuffer,
        libc::iovec,
        libc::msghdr,
        nix::sys::socket::SockAddr,
    ),
    Open(CString),
    FdataSync,
    Fallocate,
    Truncate,
    Close,
    LinkRings,
    ForeignNotifier(u64, bool),
    Statx(CString, Box<RefCell<libc::statx>>),
    Timeout(TimeSpec64, u32),
    Connect(SockAddr),
    Accept(SockAddrStorage),
    Rename(PathBuf, PathBuf),
    CreateDir(PathBuf),
    Remove(PathBuf),
    BlockingFn,
    Invalid,
    #[cfg(feature = "bench")]
    Noop,
}

impl TryFrom<SourceType> for libc::statx {
    type Error = GlommioError<()>;

    fn try_from(value: SourceType) -> Result<Self, Self::Error> {
        match value {
            SourceType::Statx(_, buf) => Ok(buf.into_inner()),
            src => Err(GlommioError::ReactorError(
                ReactorErrorKind::IncorrectSourceType(format!("{:?}", src)),
            )),
        }
    }
}

#[derive(PartialEq, Clone, Copy)]
pub(crate) enum EnqueuedStatus {
    Enqueued,
    Canceled,
    Dispatched,
}

#[derive(Clone)]
pub struct EnqueuedSource {
    pub(crate) id: SourceId,
    pub(crate) queue: ReactorQueue,
    pub(crate) status: EnqueuedStatus,
}

pub(crate) type StatsCollectionFn = fn(&io::Result<usize>, &mut RingIoStats, waiters: u64) -> ();
pub(crate) type LatencyCollectionFn =
    fn(std::time::Duration, std::time::Duration, std::time::Duration, &mut RingIoStats) -> ();

#[derive(Copy, Clone)]
pub(crate) struct StatsCollection {
    /// fulfilled runs when the source exits the reactor
    pub(crate) fulfilled: Option<StatsCollectionFn>,

    /// reused runs when a fulfilled source is reused by another consumer
    pub(crate) reused: Option<StatsCollectionFn>,

    /// Called when a result is first consumed. It collects the time
    /// it took for user code to consume the result of a fulfilled source.
    /// This is called once, even if the source is reused multiple time.
    pub(crate) latency: Option<LatencyCollectionFn>,
}

/// A registered source of I/O events.
pub(crate) struct InnerSource {
    /// Raw file descriptor on Unix platforms.
    pub(crate) raw: RawFd,

    /// Tasks interested in events on this source.
    pub(crate) wakers: Wakers,

    pub(crate) source_type: SourceType,

    pub(crate) io_requirements: IoRequirements,

    pub(crate) timeout: Option<TimeSpec64>,

    pub(crate) enqueued: Option<EnqueuedSource>,

    pub(crate) stats_collection: Option<StatsCollection>,

    pub(crate) task_queue: Option<TaskQueueHandle>,
}

impl InnerSource {
    pub(crate) fn update_source_type(&mut self, source_type: SourceType) -> SourceType {
        std::mem::replace(&mut self.source_type, source_type)
    }
}

impl fmt::Debug for InnerSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InnerSource")
            .field("raw", &self.raw)
            .field("wakers", &self.wakers)
            .field("source_type", &self.source_type)
            .field("io_requirements", &self.io_requirements)
            .finish()
    }
}

#[derive(Debug)]
pub struct Source {
    pub(crate) inner: Pin<Rc<RefCell<InnerSource>>>,
}

impl Source {
    /// Registers an I/O source in the reactor.
    pub(crate) fn new(
        ioreq: IoRequirements,
        raw: RawFd,
        source_type: SourceType,
        stats_collection: Option<StatsCollection>,
        task_queue: Option<TaskQueueHandle>,
    ) -> Source {
        Source {
            inner: Rc::pin(RefCell::new(InnerSource {
                raw,
                wakers: Wakers::new(),
                source_type,
                io_requirements: ioreq,
                enqueued: None,
                timeout: None,
                stats_collection,
                task_queue,
            })),
        }
    }

    pub(crate) fn set_timeout(&self, d: Duration) -> Option<Duration> {
        let mut inner = self.inner.borrow_mut();
        let t = &mut inner.timeout;
        let old = *t;
        match TimeSpec64::try_from(d) {
            Ok(dur) | Err(dur) => *t = Some(dur),
        }
        old.map(Duration::from)
    }

    pub(super) fn timeout_ref(&self) -> Ref<'_, Option<TimeSpec64>> {
        Ref::map(self.inner.borrow(), |x| &x.timeout)
    }

    pub(super) fn source_type(&self) -> Ref<'_, SourceType> {
        Ref::map(self.inner.borrow(), |x| &x.source_type)
    }

    pub(crate) fn source_type_mut(&self) -> RefMut<'_, SourceType> {
        RefMut::map(self.inner.borrow_mut(), |x| &mut x.source_type)
    }

    pub(crate) fn extract_source_type(self) -> SourceType {
        self.inner
            .borrow_mut()
            .update_source_type(SourceType::Invalid)
    }

    pub(crate) fn extract_buffer(self) -> IoBuffer {
        let stype = self.extract_source_type();
        match stype {
            SourceType::Read(_, Some(buffer)) => buffer,
            SourceType::Write(_, buffer) => buffer,
            x => panic!("Could not extract buffer. Source: {:?}", x),
        }
    }

    pub(crate) fn buffer(&self) -> Ref<'_, IoBuffer> {
        Ref::map(self.source_type(), |stype| match &*stype {
            SourceType::Read(_, Some(buffer)) => buffer,
            SourceType::Write(_, buffer) => buffer,
            x => panic!("Could not extract buffer. Source: {:?}", x),
        })
    }

    pub(crate) fn result(&self) -> Option<io::Result<usize>> {
        let mut inner = self.inner.borrow_mut();
        let ret = inner
            .wakers
            .result
            .as_ref()
            .map(|x| OsResult::from(x).into());
        if ret.is_none() {
            return ret;
        }

        // if there is a scheduler latency collection function present, invoke it once
        if let Some(Some(stat_fn)) = inner.stats_collection.as_ref().map(|x| x.latency) {
            if let Some(lat) = inner.wakers.timestamps() {
                drop(inner);
                let pre_lat = lat.submitted_at - lat.queued_at;
                let io_lat = lat.fulfilled_at - lat.submitted_at;
                let post_lat = lat.fulfilled_at.elapsed();

                let reactor = &crate::executor().reactor().sys;
                (stat_fn)(
                    pre_lat,
                    io_lat,
                    post_lat,
                    reactor.ring_for_source(self).io_stats_mut(),
                );
                (stat_fn)(
                    pre_lat,
                    io_lat,
                    post_lat,
                    reactor
                        .ring_for_source(self)
                        .io_stats_for_task_queue_mut(crate::executor().current_task_queue()),
                );
            }
        }

        ret
    }

    // adds a single waiter to the list, replacing any waiter that may already
    // exist. Should be used for single streams that map a future 1:1 to their I/O
    // source
    pub(crate) fn add_waiter_single(&self, waker: &Waker) {
        let mut inner = self.inner.borrow_mut();
        let waiters = &mut inner.wakers.waiters;
        match waiters.first_mut() {
            Some(w) => {
                if !w.will_wake(waker) {
                    *w = waker.clone();
                }
            }
            None => waiters.push(waker.clone()),
        }
        debug_assert_eq!(inner.wakers.waiters.len(), 1)
    }

    // adds a waiter to the list. Useful for streams that have many futures waiting
    // on a single I/O source
    pub(crate) fn add_waiter_many(&self, waker: Waker) {
        self.inner.borrow_mut().wakers.waiters.push(waker)
    }

    pub(super) fn is_installed(&self) -> Option<bool> {
        match &self.inner.borrow().source_type {
            SourceType::ForeignNotifier(_, installed) => Some(*installed),
            _ => None,
        }
    }

    pub(super) fn raw(&self) -> RawFd {
        self.inner.borrow().raw
    }

    pub(crate) fn stats_collection(&self) -> Option<StatsCollection> {
        self.inner.borrow().stats_collection
    }

    pub(crate) async fn collect_rw(&self) -> io::Result<usize> {
        future::poll_fn(|cx| {
            if let Some(result) = self.result() {
                return Poll::Ready(result);
            }

            self.add_waiter_many(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        let mut inner = self.inner.borrow_mut();
        let enqueued = inner.enqueued.as_mut();
        if let Some(EnqueuedSource { id, queue, status }) = enqueued {
            match status {
                EnqueuedStatus::Enqueued => {
                    // We never submitted the request, so it's safe to consume
                    // source here -- kernel didn't see our buffers. By consuming
                    // the source we are signaling to the submit method that we are
                    // no longer interested in submitting this. This should be cheaper
                    // than removing elements from the vector all the time.

                    *status = EnqueuedStatus::Canceled;
                }
                EnqueuedStatus::Dispatched => {
                    // We are cancelling this request, but it is already submitted.
                    // This means that the kernel might be using the buffers right
                    // now, so we delay `consume_source` until we consume the
                    // corresponding event from the completion queue.

                    queue.borrow_mut().cancel_request(*id);
                    *status = EnqueuedStatus::Canceled; // not necessary, but useful for correctness
                }
                EnqueuedStatus::Canceled => unreachable!(),
            }
        }
    }
}
