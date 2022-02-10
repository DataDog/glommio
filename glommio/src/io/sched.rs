use crate::{
    io::glommio_file::Identity,
    sys::{Reactor, Source},
    IoRequirements,
};
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
    pub(crate) fn consume_scheduled(
        &self,
        data_range: Range<u64>,
        sys: Option<&Reactor>,
    ) -> Option<ScheduledSource> {
        let sources = self.inner.sources.borrow();
        let mut candidates = sources.range(
            Bound::Included(&data_range.start),
            Bound::Excluded(&data_range.end),
        );

        if let Some(sched_source) = candidates.find(|&x| {
            x.data_range.contains(&data_range.start) && x.data_range.contains(&(data_range.end - 1))
        }) {
            if let (Some(sys), Some(result)) = (sys, sched_source.source.result()) {
                if let Some(reused) = sched_source
                    .source
                    .stats_collection()
                    .and_then(|x| x.reused)
                {
                    let mut ring = sys.ring_for_source(&sched_source.source);
                    reused(&result, ring.io_stats_mut(), 1);
                    reused(
                        &result,
                        ring.io_stats_for_task_queue_mut(crate::executor().current_task_queue()),
                        1,
                    );
                }
            }

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
pub struct ScheduledSource {
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

#[cfg(test)]
#[macro_use]
pub(crate) mod test {
    use super::*;
    use crate::{
        io::{DmaFile, OpenOptions, ReadResult},
        sys::SourceType,
        test_utils::make_test_directories,
    };
    use futures::join;
    use std::rc::Rc;

    macro_rules! dma_file_test {
        ( $name:ident, $dir:ident, $kind:ident, $code:block) => {
            #[test]
            fn $name() {
                for dir in make_test_directories(&format!("dma-{}", stringify!($name))) {
                    let $dir = dir.path.clone();
                    let $kind = dir.kind;
                    test_executor!(async move { $code });
                }
            }
        };
    }

    #[test]
    fn file_sched_lifetime() {
        let sched = Rc::new(IoScheduler::new());

        let file_sched1 = sched.get_file_scheduler((0, 1));
        let file_sched2 = sched.get_file_scheduler((0, 1));
        assert!(Rc::ptr_eq(&file_sched1.inner, &file_sched2.inner));

        assert_eq!(sched.file_schedulers.borrow().iter().count(), 1);
        drop(file_sched2);
        assert_eq!(sched.file_schedulers.borrow().iter().count(), 1);
        drop(file_sched1);
        assert_eq!(sched.file_schedulers.borrow().iter().count(), 0);
    }

    #[test]
    fn file_sched_drop_orphan() {
        let sched = Rc::new(IoScheduler::new());

        let file_sched1 = sched.get_file_scheduler((0, 0));
        drop(sched);
        drop(file_sched1);
    }

    #[test]
    fn file_sched_conflicts() {
        let sched = Rc::new(IoScheduler::new());

        let file_sched1 = sched.get_file_scheduler((0, 1));
        let file_sched2 = sched.get_file_scheduler((0, 2));
        assert!(!Rc::ptr_eq(&file_sched1.inner, &file_sched2.inner));

        assert_eq!(sched.file_schedulers.borrow().iter().count(), 2);
        drop(file_sched2);
        assert_eq!(sched.file_schedulers.borrow().iter().count(), 1);
        drop(file_sched1);
        assert_eq!(sched.file_schedulers.borrow().iter().count(), 0);
    }

    #[test]
    fn source_sched_lifetime() {
        let sched = Rc::new(IoScheduler::new());

        let file = sched.get_file_scheduler((0, 1));

        assert!(file.consume_scheduled(0..512, None).is_none());
        let sched_source1 = file.schedule(
            Source::new(Default::default(), 0, SourceType::Invalid, None, None),
            0..512,
        );
        let sched_source2 = file.consume_scheduled(0..512, None).unwrap();

        assert!(Rc::ptr_eq(&sched_source1.inner, &sched_source2.inner));

        assert_eq!(file.inner.sources.borrow().iter().count(), 1);
        drop(sched_source1);
        assert_eq!(file.inner.sources.borrow().iter().count(), 1);
        drop(sched_source2);
        assert_eq!(file.inner.sources.borrow().iter().count(), 0);
    }

    #[test]
    fn source_sched_drop_orphan() {
        {
            let sched = Rc::new(IoScheduler::new());
            let file = sched.get_file_scheduler((0, 1));
            let _ = file.schedule(
                Source::new(Default::default(), 0, SourceType::Invalid, None, None),
                0..512,
            );
            // test dropping a ScheduledSource with a ScheduledFile but no io scheduler
            drop(sched);
        }

        {
            let sched = Rc::new(IoScheduler::new());
            let file = sched.get_file_scheduler((0, 1));
            let _ = file.schedule(
                Source::new(Default::default(), 0, SourceType::Invalid, None, None),
                0..512,
            );
            // test dropping a ScheduledSource with no host ScheduledFile
            drop(file);
        }

        {
            let sched = Rc::new(IoScheduler::new());
            let file = sched.get_file_scheduler((0, 1));
            let _ = file.schedule(
                Source::new(Default::default(), 0, SourceType::Invalid, None, None),
                0..512,
            );
            // test dropping a ScheduledSource with no host ScheduledFile and io scheduler
            drop(sched);
            drop(file);
        }
    }

    async fn read_some(file: Rc<DmaFile>, r: Range<usize>) -> ReadResult {
        let read_buf = file
            .read_at(r.start as u64, r.end - r.start)
            .await
            .expect("failed to read");
        std::assert_eq!(read_buf.len(), r.end - r.start);
        for i in 0..read_buf.len() {
            std::assert_eq!(read_buf[i], (r.start + i) as u8);
        }
        read_buf
    }

    dma_file_test!(file_simple_dedup, path, _k, {
        let mut new_file = Rc::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .dma_open(path.join("testfile"))
                .await
                .expect("failed to create file"),
        );

        let mut buf = new_file.alloc_dma_buffer(512 << 10);
        for x in 0..512 << 10 {
            buf.as_bytes_mut()[x] = x as u8;
        }
        let res = new_file.write_at(buf, 0).await.expect("failed to write");
        assert_eq!(res, 512 << 10);
        assert_eq!(crate::executor().io_stats().all_rings().file_reads().0, 0);

        new_file = Rc::new(
            OpenOptions::new()
                .read(true)
                .dma_open(path.join("testfile"))
                .await
                .expect("failed to open file"),
        );
        new_file.attach_scheduler();

        let read_buf1 = read_some(new_file.clone(), 0..4096).await;
        // we expect one IO to have been performed at this point
        // all buffers are dead so this last read should trigger an IO request
        let io_stats = crate::executor().io_stats().all_rings();
        assert_eq!(io_stats.file_reads().0, 1);
        assert_eq!(io_stats.file_deduped_reads().0, 0);

        let read_buf2 = read_some(new_file.clone(), 0..4096).await;
        // should feed from the first buffer
        // all buffers are dead so this last read should trigger an IO request
        let io_stats = crate::executor().io_stats().all_rings();
        assert_eq!(io_stats.file_reads().0, 0);
        assert_eq!(io_stats.file_deduped_reads().0, 1);

        drop(read_buf1);
        let read_buf3 = read_some(new_file.clone(), 0..4096).await;
        // initial buffer lifetime should have been extended
        let io_stats = crate::executor().io_stats().all_rings();
        assert_eq!(io_stats.file_reads().0, 0);
        assert_eq!(io_stats.file_deduped_reads().0, 1);

        drop(read_buf2);
        drop(read_buf3);
        let _ = read_some(new_file.clone(), 0..4096).await;
        // all buffers are dead so this last read should trigger an IO request
        let io_stats = crate::executor().io_stats().all_rings();
        assert_eq!(io_stats.file_reads().0, 1);
        assert_eq!(io_stats.file_deduped_reads().0, 0);

        new_file.close_rc().await.expect("failed to close file");
    });

    dma_file_test!(file_simple_dedup_concurrent, path, _k, {
        let mut new_file = Rc::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .dma_open(path.join("testfile"))
                .await
                .expect("failed to create file"),
        );

        let mut buf = new_file.alloc_dma_buffer(512 << 10);
        for x in 0..512 << 10 {
            buf.as_bytes_mut()[x] = x as u8;
        }
        let res = new_file.write_at(buf, 0).await.expect("failed to write");
        assert_eq!(res, 512 << 10);
        assert_eq!(crate::executor().io_stats().all_rings().file_reads().0, 0);

        new_file = Rc::new(
            OpenOptions::new()
                .read(true)
                .dma_open(path.join("testfile"))
                .await
                .expect("failed to open file"),
        );
        new_file.attach_scheduler();

        let read_buf1 = read_some(new_file.clone(), 0..4096);
        let read_buf2 = read_some(new_file.clone(), 0..4096);
        assert_eq!(crate::executor().io_stats().all_rings().file_reads().0, 0);

        join!(read_buf1, read_buf2);

        // should feed from the first buffer
        let io_stats = crate::executor().io_stats().all_rings();
        assert_eq!(io_stats.file_reads().0, 1);
        assert_eq!(io_stats.file_deduped_reads().0, 1);
        new_file.close_rc().await.expect("failed to close file");
    });

    dma_file_test!(file_offset_dedup, path, _k, {
        let mut new_file = Rc::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .dma_open(path.join("testfile"))
                .await
                .expect("failed to create file"),
        );

        let mut buf = new_file.alloc_dma_buffer(512 << 10);
        for x in 0..512 << 10 {
            buf.as_bytes_mut()[x] = x as u8;
        }
        let res = new_file.write_at(buf, 0).await.expect("failed to write");
        assert_eq!(res, 512 << 10);
        assert_eq!(crate::executor().io_stats().all_rings().file_reads().0, 0);

        new_file = Rc::new(
            OpenOptions::new()
                .read(true)
                .dma_open(path.join("testfile"))
                .await
                .expect("failed to open file"),
        );
        new_file.attach_scheduler();

        let _first = read_some(new_file.clone(), 0..16384).await;
        // we expect one IO to have been performed at this point
        let io_stats = crate::executor().io_stats().all_rings();
        assert_eq!(io_stats.file_reads().0, 1);
        assert_eq!(io_stats.file_deduped_reads().0, 0);

        let _second = read_some(new_file.clone(), 67..578).await;
        // should feed from the first buffer
        let io_stats = crate::executor().io_stats().all_rings();
        assert_eq!(io_stats.file_reads().0, 0);
        assert_eq!(io_stats.file_deduped_reads().0, 1);
        new_file.close_rc().await.expect("failed to close file");
    });

    dma_file_test!(file_opt_out_dedup, path, _k, {
        let mut new_file = Rc::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .dma_open(path.join("testfile"))
                .await
                .expect("failed to create file"),
        );

        let mut buf = new_file.alloc_dma_buffer(512 << 10);
        for x in 0..512 << 10 {
            buf.as_bytes_mut()[x] = x as u8;
        }
        let res = new_file.write_at(buf, 0).await.expect("failed to write");
        assert_eq!(res, 512 << 10);
        assert_eq!(crate::executor().io_stats().all_rings().file_reads().0, 0);

        new_file = Rc::new(
            OpenOptions::new()
                .read(true)
                .dma_open(path.join("testfile"))
                .await
                .expect("failed to open file"),
        );

        let _first = read_some(new_file.clone(), 0..4096).await;
        assert_eq!(crate::executor().io_stats().all_rings().file_reads().0, 1);

        let _second = read_some(new_file.clone(), 0..4096).await;
        let io_stats = crate::executor().io_stats().all_rings();
        assert_eq!(io_stats.file_reads().0, 1);
        assert_eq!(io_stats.file_deduped_reads().0, 0);
        new_file.close_rc().await.expect("failed to close file");
    });

    dma_file_test!(file_hard_link_dedup, path, _k, {
        let mut new_file = Rc::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .dma_open(path.join("testfile"))
                .await
                .expect("failed to create file"),
        );

        std::fs::hard_link(path.join("testfile"), path.join("link"))
            .expect("failed to create hard link");

        let linked_file = Rc::new(
            OpenOptions::new()
                .read(true)
                .dma_open(path.join("link"))
                .await
                .expect("failed to open file"),
        );
        linked_file.attach_scheduler();

        let mut buf = new_file.alloc_dma_buffer(512 << 10);
        for x in 0..512 << 10 {
            buf.as_bytes_mut()[x] = x as u8;
        }
        let res = new_file.write_at(buf, 0).await.expect("failed to write");
        assert_eq!(res, 512 << 10);
        assert_eq!(crate::executor().io_stats().all_rings().file_reads().0, 0);

        new_file = Rc::new(
            OpenOptions::new()
                .read(true)
                .dma_open(path.join("testfile"))
                .await
                .expect("failed to open file"),
        );
        new_file.attach_scheduler();

        let _first = read_some(new_file.clone(), 0..4096).await;
        // we expect one IO to have been performed at this point
        assert_eq!(crate::executor().io_stats().all_rings().file_reads().0, 1);

        let _second = read_some(linked_file.clone(), 0..4096).await;
        // should feed from the first buffer
        let io_stats = crate::executor().io_stats().all_rings();
        assert_eq!(io_stats.file_reads().0, 0);
        assert_eq!(io_stats.file_deduped_reads().0, 1);
        new_file.close_rc().await.expect("failed to close file");
        linked_file.close_rc().await.expect("failed to close file");
    });

    dma_file_test!(file_soft_link_dedup, path, _k, {
        let mut new_file = Rc::new(
            OpenOptions::new()
                .create(true)
                .write(true)
                .dma_open(path.join("testfile"))
                .await
                .expect("failed to create file"),
        );

        std::os::unix::fs::symlink(path.join("testfile"), path.join("link"))
            .expect("failed to create soft link");

        let linked_file = Rc::new(
            OpenOptions::new()
                .read(true)
                .dma_open(path.join("link"))
                .await
                .expect("failed to open file"),
        );
        linked_file.attach_scheduler();

        let mut buf = new_file.alloc_dma_buffer(512 << 10);
        for x in 0..512 << 10 {
            buf.as_bytes_mut()[x] = x as u8;
        }
        let res = new_file.write_at(buf, 0).await.expect("failed to write");
        assert_eq!(res, 512 << 10);
        assert_eq!(crate::executor().io_stats().all_rings().file_reads().0, 0);

        new_file = Rc::new(
            OpenOptions::new()
                .read(true)
                .dma_open(path.join("testfile"))
                .await
                .expect("failed to open file"),
        );
        new_file.attach_scheduler();

        let _first = read_some(new_file.clone(), 0..4096).await;
        // we expect one IO to have been performed at this point
        assert_eq!(crate::executor().io_stats().all_rings().file_reads().0, 1);

        let _second = read_some(linked_file.clone(), 0..4096).await;
        // should feed from the first buffer
        let io_stats = crate::executor().io_stats().all_rings();
        assert_eq!(io_stats.file_reads().0, 0);
        assert_eq!(io_stats.file_deduped_reads().0, 1);
        new_file.close_rc().await.expect("failed to close file");
        linked_file.close_rc().await.expect("failed to close file");
    });
}
