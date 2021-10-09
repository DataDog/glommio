use crate::sys::{create_eventfd, read_eventfd, write_eventfd, SleepNotifier};
use crossbeam::queue::ArrayQueue;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    convert::TryFrom,
    ffi::CString,
    io,
    os::unix::{ffi::OsStrExt, prelude::*},
    path::{Path, PathBuf},
    sync::Arc,
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

#[derive(Clone, Debug)]
pub(super) enum BlockingThreadOp {
    Rename(PathBuf, PathBuf),
    Remove(PathBuf),
    CreateDir(PathBuf, libc::c_int),
    Truncate(RawFd, i64),
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
        }
    }
}

#[derive(Clone, Debug)]
pub(super) enum BlockingThreadResult {
    Syscall(i64),
}

impl TryFrom<BlockingThreadResult> for std::io::Result<usize> {
    type Error = ();

    fn try_from(value: BlockingThreadResult) -> Result<Self, Self::Error> {
        match value {
            BlockingThreadResult::Syscall(x) => Ok(to_result(x)),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct BlockingThreadReq {
    op: BlockingThreadOp,
    id: u64,
}

#[derive(Clone, Debug)]
pub(super) struct BlockingThreadResp {
    id: u64,
    res: BlockingThreadResult,
}

pub(super) type BlockingThreadHandler = Box<dyn Fn(BlockingThreadResult)>;

pub(super) struct BlockingThread {
    eventfd: RawFd,
    queue: Arc<ArrayQueue<BlockingThreadReq>>,
    responses: Arc<ArrayQueue<BlockingThreadResp>>,
    waiters: RefCell<BTreeMap<u64, BlockingThreadHandler>>,
    requests: Cell<u64>,
}

impl BlockingThread {
    pub(super) fn push(&self, op: BlockingThreadOp, action: Box<dyn Fn(BlockingThreadResult)>) {
        let id = self.requests.get();
        self.requests.set(id + 1);

        let req = BlockingThreadReq { op, id };

        if self.queue.push(req).is_err() {
            panic!("syscall queue full!");
        }

        let mut waiters = self.waiters.borrow_mut();
        waiters.insert(id, action);

        write_eventfd(self.eventfd);
    }

    pub(super) fn flush(&self) -> usize {
        let mut woke = 0;
        let mut waiters = self.waiters.borrow_mut();
        while let Some(x) = self.responses.pop() {
            let id = x.id;
            let res = x.res;
            let func = waiters.remove(&id).unwrap();
            func(res);
            woke += 1;
        }
        woke
    }

    pub(super) fn new(reactor_sleep_notifier: Arc<SleepNotifier>) -> Self {
        let eventfd = create_eventfd().unwrap();
        let queue = Arc::new(ArrayQueue::<BlockingThreadReq>::new(1024));
        let responses = Arc::new(ArrayQueue::<BlockingThreadResp>::new(1024));
        let waiters = RefCell::new(BTreeMap::new());
        let requests = Cell::new(0);

        let tq = queue.clone();
        let rsp = responses.clone();

        std::thread::spawn(move || {
            loop {
                read_eventfd(eventfd);
                while let Some(el) = tq.pop() {
                    let res = el.op.execute();
                    let id = el.id;
                    let resp = BlockingThreadResp { id, res };

                    if rsp.push(resp).is_err() {
                        panic!("Could not add response to syscall response queue");
                    }
                }
                reactor_sleep_notifier.notify_if_sleeping();
            }
        });
        BlockingThread {
            eventfd,
            queue,
            responses,
            waiters,
            requests,
        }
    }
}
