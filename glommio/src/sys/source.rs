// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2021 Datadog, Inc.
//
use crate::{
    iou::sqe::{SockAddr, SockAddrStorage},
    sys::{DmaBuffer, IoBuffer, PollableStatus, ReactorQueue, SourceId, TimeSpec64, Wakers},
    GlommioError,
    IoRequirements,
    Latency,
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
    rc::Rc,
    task::{Poll, Waker},
    time::Duration,
};

#[derive(Debug)]
pub(crate) enum SourceType {
    Write(PollableStatus, IoBuffer),
    Read(PollableStatus, Option<IoBuffer>),
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
    Close,
    LinkRings,
    Statx(CString, Box<RefCell<libc::statx>>),
    Timeout(TimeSpec64),
    Connect(SockAddr),
    Accept(SockAddrStorage),
    Invalid,
    #[cfg(feature = "bench")]
    Noop,
}

impl TryFrom<SourceType> for libc::statx {
    type Error = GlommioError<()>;

    fn try_from(value: SourceType) -> Result<Self, Self::Error> {
        match value {
            SourceType::Statx(_, buf) => Ok(buf.into_inner()),
            _ => Err(GlommioError::ReactorError(
                ReactorErrorKind::IncorrectSourceType,
            )),
        }
    }
}

pub struct EnqueuedSource {
    pub(crate) id: SourceId,
    pub(crate) queue: ReactorQueue,
}

pub(crate) type StatsCollectionFn = fn(&io::Result<usize>, &mut RingIoStats) -> ();

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

    pub(crate) stats_collection: Option<StatsCollectionFn>,

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
    pub(crate) inner: Rc<RefCell<InnerSource>>,
}

impl Source {
    /// Registers an I/O source in the reactor.
    pub(crate) fn new(
        ioreq: IoRequirements,
        raw: RawFd,
        source_type: SourceType,
        stats_collection_fn: Option<StatsCollectionFn>,
        task_queue: Option<TaskQueueHandle>,
    ) -> Source {
        Source {
            inner: Rc::new(RefCell::new(InnerSource {
                raw,
                wakers: Wakers::new(),
                source_type,
                io_requirements: ioreq,
                enqueued: None,
                timeout: None,
                stats_collection: stats_collection_fn,
                task_queue,
            })),
        }
    }

    pub(crate) fn set_timeout(&self, d: Duration) -> Option<Duration> {
        let mut inner = self.inner.borrow_mut();
        let t = &mut inner.timeout;
        let old = *t;
        *t = Some(TimeSpec64::from(d));
        old.map(Duration::from)
    }

    pub(super) fn timeout_ref(&self) -> Ref<'_, Option<TimeSpec64>> {
        Ref::map(self.inner.borrow(), |x| &x.timeout)
    }

    pub(crate) fn latency_req(&self) -> Latency {
        self.inner.borrow().io_requirements.latency_req
    }

    pub(super) fn source_type(&self) -> Ref<'_, SourceType> {
        Ref::map(self.inner.borrow(), |x| &x.source_type)
    }

    pub(crate) fn source_type_mut(&self) -> RefMut<'_, SourceType> {
        RefMut::map(self.inner.borrow_mut(), |x| &mut x.source_type)
    }

    pub(crate) fn extract_source_type(&self) -> SourceType {
        self.inner
            .borrow_mut()
            .update_source_type(SourceType::Invalid)
    }

    pub(crate) fn extract_buffer(&self) -> IoBuffer {
        let stype = self.extract_source_type();
        match stype {
            SourceType::Read(_, Some(buffer)) => buffer,
            SourceType::Write(_, buffer) => buffer,
            x => panic!("Could not extract buffer. Source: {:?}", x),
        }
    }

    pub(crate) fn take_result(&self) -> Option<io::Result<usize>> {
        self.inner
            .borrow_mut()
            .wakers
            .result
            .take()
            .map(|x| x.map(|x| x as usize))
    }

    pub(crate) fn has_result(&self) -> bool {
        self.inner.borrow().wakers.result.is_some()
    }

    pub(crate) fn add_waiter(&self, waker: Waker) {
        self.inner.borrow_mut().wakers.waiter.replace(waker);
    }

    pub(super) fn raw(&self) -> RawFd {
        self.inner.borrow().raw
    }

    pub(crate) async fn collect_rw(&self) -> io::Result<usize> {
        future::poll_fn(|cx| {
            if let Some(result) = self.take_result() {
                return Poll::Ready(result);
            }

            self.add_waiter(cx.waker().clone());
            Poll::Pending
        })
        .await
    }
}

impl Drop for Source {
    fn drop(&mut self) {
        let enqueued = self.inner.borrow_mut().enqueued.take();
        if let Some(EnqueuedSource { id, queue }) = enqueued {
            queue.borrow_mut().cancel_request(id);
        }
    }
}
