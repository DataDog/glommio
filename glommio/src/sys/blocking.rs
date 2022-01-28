use crate::{
    executor::bind_to_cpu_set,
    sys::{InnerSource, SleepNotifier},
    PoolPlacement,
};
use ahash::AHashMap;
use alloc::rc::Rc;
use core::fmt::{Debug, Formatter};
use flume::{Receiver, Sender};
use std::{
    cell::{Cell, RefCell},
    convert::{TryFrom, TryInto},
    ffi::CString,
    future::Future,
    io,
    os::unix::{ffi::OsStrExt, prelude::*},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    thread::JoinHandle,
};

// So hard to copy/clone io::Error, plus need to send between threads. Best to
// do all i64.
macro_rules! raw_syscall {
    ($fn:ident $args:tt) => {{
        let res = unsafe { libc::$fn $args };
        BlockingThreadResult::Syscall(if res == -1 {
            -std::io::Error::last_os_error().raw_os_error().unwrap() as i64
        } else {
            res as i64
        })
    }};
}

fn to_result(res: i64) -> io::Result<usize> {
    if res < 0 {
        Err(std::io::Error::from_raw_os_error(-res as i32))
    } else {
        Ok(res as usize)
    }
}

fn cstr(path: &Path) -> io::Result<CString> {
    Ok(CString::new(path.as_os_str().as_bytes())?)
}

macro_rules! c_str {
    ( $path:expr ) => {
        match cstr($path) {
            Ok(x) => x,
            Err(_) => {
                return BlockingThreadResult::Syscall(-libc::EFAULT as i64);
            }
        }
    };
}

pub(super) enum BlockingThreadOp {
    Rename(PathBuf, PathBuf),
    Remove(PathBuf),
    CreateDir(PathBuf, libc::c_int),
    Truncate(RawFd, i64),
    Fn(Box<dyn FnOnce() + Send + 'static>),
}

impl Debug for BlockingThreadOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            BlockingThreadOp::Rename(from, to) => write!(f, "rename `{:?}` -> `{:?}`", from, to),
            BlockingThreadOp::Remove(path) => write!(f, "remove `{:?}`", path),
            BlockingThreadOp::CreateDir(path, flags) => {
                write!(f, "create dir `{:?}` (`{:b}`)", path, flags)
            }
            BlockingThreadOp::Truncate(fd, to) => write!(f, "truncate `{}` -> `{}`", fd, to),
            BlockingThreadOp::Fn(_) => write!(f, "user function"),
        }
    }
}

impl BlockingThreadOp {
    fn execute(self) -> BlockingThreadResult {
        match self {
            BlockingThreadOp::CreateDir(path, mode) => {
                let p = c_str!(&path);
                raw_syscall!(mkdir(p.as_ptr(), mode as u32))
            }
            BlockingThreadOp::Rename(old, new) => {
                let o = c_str!(&old);
                let n = c_str!(&new);
                raw_syscall!(rename(o.as_ptr(), n.as_ptr()))
            }
            BlockingThreadOp::Remove(path) => {
                let p = c_str!(&path);
                raw_syscall!(unlink(p.as_ptr()))
            }
            BlockingThreadOp::Truncate(fd, sz) => {
                raw_syscall!(ftruncate(fd, sz))
            }
            BlockingThreadOp::Fn(f) => {
                f();
                BlockingThreadResult::Fn
            }
        }
    }
}

#[derive(Debug)]
pub(super) enum BlockingThreadResult {
    Syscall(i64),
    Fn,
}

impl TryFrom<BlockingThreadResult> for std::io::Result<usize> {
    type Error = ();

    fn try_from(value: BlockingThreadResult) -> Result<Self, Self::Error> {
        match value {
            BlockingThreadResult::Syscall(x) => Ok(to_result(x)),
            BlockingThreadResult::Fn => Ok(Ok(0)),
        }
    }
}

#[derive(Debug)]
pub(super) struct BlockingThreadReq {
    op: BlockingThreadOp,
    latency_sensitive: bool,
    id: u64,
}

pub(super) struct BlockingThreadResp {
    id: u64,
    res: BlockingThreadResult,
}

#[derive(Debug)]
struct BlockingThread(JoinHandle<()>);

impl BlockingThread {
    pub(super) fn new(
        reactor_sleep_notifier: Arc<SleepNotifier>,
        rx: Arc<Receiver<BlockingThreadReq>>,
        tx: Arc<Sender<BlockingThreadResp>>,
        bindings: Option<impl IntoIterator<Item = usize> + Send + 'static>,
    ) -> Self {
        Self(std::thread::spawn(move || {
            if let Some(bindings) = bindings {
                bind_to_cpu_set(bindings).expect("failed to bind blocking thread");
            }
            while let Ok(el) = rx.recv() {
                let res = el.op.execute();
                let id = el.id;
                let resp = BlockingThreadResp { id, res };

                if tx.send(resp).is_err() {
                    panic!("failed to send response");
                }
                reactor_sleep_notifier.notify(el.latency_sensitive);
            }
        }))
    }
}

#[derive(Debug)]
pub(crate) struct BlockingThreadPool {
    tx: Rc<Sender<BlockingThreadReq>>,
    rx: Receiver<BlockingThreadResp>,
    sources: RefCell<AHashMap<u64, Pin<Rc<RefCell<InnerSource>>>>>,
    requests: Cell<u64>,
    _threads: Vec<BlockingThread>,
}

impl BlockingThreadPool {
    pub(crate) fn new(
        placement: PoolPlacement,
        sleep_notifier: Arc<SleepNotifier>,
    ) -> crate::Result<Self, ()> {
        let (in_tx, in_rx) = flume::bounded(4 << 10);
        let (out_tx, out_rx) = flume::bounded(4 << 10);
        let in_rx = Arc::new(in_rx);
        let out_tx = Arc::new(out_tx);

        let thread_count = placement.executor_count();
        let mut placements = placement.generate_cpu_set()?;
        let mut threads = Vec::with_capacity(thread_count);
        for _ in 0..thread_count {
            let bindings = placements.next().cpu_binding();
            threads.push(BlockingThread::new(
                sleep_notifier.clone(),
                in_rx.clone(),
                out_tx.clone(),
                bindings,
            ));
        }

        Ok(Self {
            _threads: threads,
            tx: Rc::new(in_tx),
            rx: out_rx,
            sources: RefCell::new(Default::default()),
            requests: Cell::new(0),
        })
    }

    pub(super) fn push(
        &self,
        op: BlockingThreadOp,
        source: Pin<Rc<RefCell<InnerSource>>>,
    ) -> impl Future<Output = ()> {
        let id = self.requests.get();
        self.requests.set(id.overflowing_add(1).0);
        let req = BlockingThreadReq {
            op,
            id,
            latency_sensitive: matches!(
                source.borrow().io_requirements.latency_req,
                crate::Latency::Matters(_)
            ),
        };
        let mut waiters = self.sources.borrow_mut();
        assert!(waiters.insert(id, source).is_none());
        let tx = self.tx.clone();

        async move {
            tx.send_async(req)
                .await
                .expect("failed to enqueue blocking operation");
        }
    }

    pub(super) fn flush(&self) -> usize {
        let mut woke = 0;
        let mut waiters = self.sources.borrow_mut();
        for x in self.rx.try_iter() {
            let id = x.id;
            let res = x.res;

            let src = waiters.remove(&id).unwrap();
            let mut inner_source = src.borrow_mut();
            inner_source.wakers.result.replace(
                res.try_into()
                    .expect("not a valid blocking operation's result"),
            );
            inner_source.wakers.wake_waiters();

            woke += 1;
        }
        woke
    }
}
