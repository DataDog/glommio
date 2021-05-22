use crate::{io::glommio_file::Identity, sys::Source, IoRequirements};
use intrusive_collections::{intrusive_adapter, Bound, KeyAdapter, RBTree, RBTreeLink};
use std::{
    cell::{Cell, RefCell},
    ops::{Deref, Range},
    rc::{Rc, Weak},
};

#[derive(Debug)]
pub(crate) struct IoScheduler {
    /// I/O Requirements of the task currently executing.
    current_requirements: Cell<IoRequirements>,

    file_schedulers: RefCell<RBTree<FileSchedulerAdapter>>,
}

impl IoScheduler {
    pub(crate) fn new() -> IoScheduler {
        IoScheduler {
            current_requirements: Cell::new(Default::default()),
            file_schedulers: RefCell::new(Default::default()),
        }
    }

    pub(crate) fn requirements(&self) -> IoRequirements {
        self.current_requirements.get()
    }

    pub(crate) fn inform_requirements(&self, req: IoRequirements) {
        self.current_requirements.set(req);
    }

    pub(super) fn get_file_scheduler(self: &Rc<Self>, identity: Identity) -> FileScheduler {
        let mut borrow = self.file_schedulers.borrow_mut();
        let file = if let Some(file) = borrow.find(&identity).clone_pointer() {
            file
        } else {
            let file_sched = Rc::new(FileSchedulerInner {
                link: RBTreeLink::new(),
                identity,
                sources: RefCell::new(Default::default()),
            });
            borrow.insert(file_sched.clone());
            file_sched
        };

        FileScheduler {
            inner: file,
            io_scheduler: Rc::downgrade(self),
        }
    }

    fn remove_file(&self, file: &FileScheduler) {
        unsafe {
            self.file_schedulers
                .borrow_mut()
                .cursor_mut_from_ptr(file.inner.as_ref())
                .remove()
        };
    }
}

#[derive(Debug)]
struct FileSchedulerInner {
    link: RBTreeLink,
    identity: Identity,
    sources: RefCell<RBTree<ScheduledSourceAdapter>>,
}

intrusive_adapter!(FileSchedulerAdapter = Rc<FileSchedulerInner>: FileSchedulerInner { link: RBTreeLink });
impl<'a> KeyAdapter<'a> for FileSchedulerAdapter {
    type Key = Identity;
    fn get_key(&self, s: &'a FileSchedulerInner) -> Self::Key {
        s.identity
    }
}

impl FileSchedulerInner {
    fn remove_source(&self, source: &ScheduledSource) {
        unsafe {
            self.sources
                .borrow_mut()
                .cursor_mut_from_ptr(source.inner.as_ref())
                .remove()
        };
    }
}

#[derive(Debug)]
pub(crate) struct FileScheduler {
    inner: Rc<FileSchedulerInner>,
    io_scheduler: Weak<IoScheduler>,
}

impl Drop for FileScheduler {
    fn drop(&mut self) {
        if Rc::strong_count(&self.inner) <= 2 {
            if let Some(io_sched) = self.io_scheduler.upgrade() {
                // the scheduler owns one Rc to this FileScheduler
                // so if the count is two or less, then we can remove this file scheduler
                io_sched.remove_file(self)
            }
        }
    }
}

impl FileScheduler {
    pub(crate) fn consume_scheduled(&self, data_range: Range<u64>) -> Option<ScheduledSource> {
        let sources = self.inner.sources.borrow();
        let mut candidates = sources.range(
            Bound::Included(&data_range.start),
            Bound::Excluded(&data_range.end),
        );

        if let Some(sched_source) = candidates.find(|&x| {
            x.data_range.contains(&data_range.start) && x.data_range.contains(&(data_range.end - 1))
        }) {
            unsafe {
                let offset_start = data_range.start - sched_source.data_range.start;
                Some(ScheduledSource {
                    inner: sources
                        .cursor_from_ptr(sched_source)
                        .clone_pointer()
                        .unwrap(),
                    file: Rc::downgrade(&self.inner),
                    offseted_range: offset_start..offset_start + data_range.end,
                })
            }
        } else {
            None
        }
    }

    pub(crate) fn schedule(&self, source: Source, data_range: Range<u64>) -> ScheduledSource {
        let scheduled = Rc::new(ScheduledSourceInner {
            source,
            link: Default::default(),
            data_range: data_range.clone(),
        });
        self.inner.sources.borrow_mut().insert(scheduled.clone());
        ScheduledSource {
            inner: scheduled,
            file: Rc::downgrade(&self.inner),
            offseted_range: 0..data_range.end - data_range.start,
        }
    }
}

#[derive(Debug)]
struct ScheduledSourceInner {
    source: Source,
    link: RBTreeLink,
    data_range: Range<u64>,
}

intrusive_adapter!(ScheduledSourceAdapter = Rc<ScheduledSourceInner>: ScheduledSourceInner { link: RBTreeLink });
impl<'a> KeyAdapter<'a> for ScheduledSourceAdapter {
    type Key = u64;
    fn get_key(&self, s: &'a ScheduledSourceInner) -> Self::Key {
        s.data_range.start
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ScheduledSource {
    inner: Rc<ScheduledSourceInner>,
    file: Weak<FileSchedulerInner>,
    offseted_range: Range<u64>,
}

impl ScheduledSource {
    pub(crate) fn new_raw(source: Source, data_range: Range<u64>) -> ScheduledSource {
        ScheduledSource {
            inner: Rc::new(ScheduledSourceInner {
                source,
                link: Default::default(),
                data_range: data_range.clone(),
            }),
            file: Default::default(),
            offseted_range: 0..data_range.end - data_range.start,
        }
    }

    pub(crate) unsafe fn as_bytes(&self) -> &[u8] {
        std::slice::from_raw_parts(
            self.inner
                .source
                .buffer()
                .as_ptr()
                .add(self.offseted_range.start as usize),
            (self.offseted_range.end - self.offseted_range.start) as usize,
        )
    }
}

impl Deref for ScheduledSource {
    type Target = Source;

    fn deref(&self) -> &Self::Target {
        &self.inner.source
    }
}

impl Drop for ScheduledSource {
    fn drop(&mut self) {
        if Rc::strong_count(&self.inner) <= 2 {
            if let Some(file) = self.file.upgrade() {
                // the file scheduler owns one Rc to this ScheduledSource
                // so if the count is two or less, then we can remove this source
                file.remove_source(self)
            }
        }
    }
}
