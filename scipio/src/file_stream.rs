// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::dma_file::align_down;
use crate::sys::DmaBuffer;
use crate::task;
use crate::{DmaFile, Local};
use core::task::Waker;
use futures::future::join_all;
use futures::io::AsyncRead;
use futures::task::{Context, Poll};
use futures_lite::Future;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::vec::Vec;

macro_rules! current_error {
    ( $state:expr ) => {
        $state.error.take().map(|err| Err(err.into()))
    };
}

#[derive(Debug, Clone)]
/// Builds a StreamReader, allowing linear access to a Direct I/O [`DmaFile`]
///
/// [`DmaFile`]: struct.DmaFile.html
pub struct StreamReaderBuilder {
    start: u64,
    end: u64,
    buffer_size: usize,
    read_ahead: usize,
    file: Rc<DmaFile>,
}

impl StreamReaderBuilder {
    /// Creates a new StreamReaderBuilder, given a [`DmaFile`]
    ///
    /// Various properties can be set by using its `with` methods.
    ///
    /// A [`StreamReader`] can later be constructed from it by
    /// calling [`build`]
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use scipio::{DmaFile, StreamReaderBuilder, LocalExecutor};
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async {
    ///     let file = DmaFile::open("myfile.txt").await.unwrap();
    ///     let _reader = StreamReaderBuilder::new(file).build();
    /// });
    /// ```
    /// [`DmaFile`]: struct.DmaFile.html
    /// [`StreamReader`]: struct.StreamReader.html
    /// [`build`]: #method.build
    #[must_use = "The builder must be built to be useful"]
    pub fn new(file: DmaFile) -> StreamReaderBuilder {
        StreamReaderBuilder {
            start: 0,
            end: u64::MAX,
            buffer_size: 128 << 10,
            read_ahead: 1,
            file: Rc::new(file),
        }
    }

    /// Define a starting position.
    ///
    /// Reads from the [`StreamReader`] will start from this position
    ///
    /// [`StreamReader`]: struct.StreamReader.html
    pub fn with_start_pos(mut self, start: u64) -> Self {
        self.start = start;
        self
    }

    /// Define an end position.
    ///
    /// Reads from the [`StreamReader`] will end at this position even if the file
    /// is larger.
    ///
    /// [`StreamReader`]: struct.StreamReader.html
    pub fn with_end_pos(mut self, end: u64) -> Self {
        self.end = end;
        self
    }

    /// Define the number of read-ahead buffers that will be used by the [`StreamReader`]
    ///
    /// Higher read-ahead numbers mean more parallelism but also more memory usage.
    ///
    /// [`StreamReader`]: struct.StreamReader.html
    pub fn with_read_ahead(mut self, read_ahead: usize) -> Self {
        self.read_ahead = read_ahead;
        self
    }

    /// Define the buffer size that will be used by the [`StreamReader`]
    ///
    /// [`StreamReader`]: struct.StreamReader.html
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        let buffer_size = std::cmp::max(buffer_size, 1);
        self.buffer_size = self.file.align_up(buffer_size as _) as usize;
        self
    }

    /// Builds a [`StreamReader`] with the properties defined by this [`StreamReaderBuilder`]
    ///
    /// [`StreamReader`]: struct.StreamReader.html
    /// [`StreamReaderBuilder`]: struct.StreamReaderBuilder.html
    pub fn build(self) -> StreamReader {
        StreamReader::new(self)
    }
}

#[derive(Debug)]
struct StreamReaderState {
    buffer_read_ahead_current_pos: u64,
    max_pos: u64,
    buffer_size: u64,
    read_ahead: usize,
    wakermap: HashMap<u64, Vec<Waker>>,
    pending: HashMap<u64, task::JoinHandle<(), ()>>,
    error: Option<io::Error>,
    buffermap: HashMap<u64, Rc<DmaBuffer>>,
}

impl StreamReaderState {
    fn discard_buffer(&mut self, id: u64) {
        self.buffermap.remove(&id);
        if let Some(handle) = self.pending.remove(&id) {
            handle.cancel();
        }
    }

    fn replenish_read_ahead(&mut self, state: Rc<RefCell<Self>>, file: Rc<DmaFile>) {
        while self.buffermap.len() + self.pending.len() < self.read_ahead {
            self.fill_buffer(state.clone(), file.clone());
        }
    }

    fn fill_buffer(&mut self, read_state: Rc<RefCell<Self>>, file: Rc<DmaFile>) {
        let pos = self.buffer_read_ahead_current_pos;
        let buffer_id = self.buffer_id(pos);

        if pos > self.max_pos {
            return;
        }
        let len = self.buffer_size;

        if self.pending.get(&buffer_id).is_some() || self.buffermap.get(&buffer_id).is_some() {
            return;
        }

        let pending = Local::local(async move {
            if read_state.borrow().error.is_some() {
                return;
            }
            let buffer = file.read_dma_aligned(pos, len as _).await;

            let mut state = read_state.borrow_mut();
            match buffer {
                Ok(buf) => {
                    state.buffermap.insert(buffer_id, Rc::new(buf));
                }
                Err(x) => {
                    state.error = Some(x);
                }
            }
            state.pending.remove(&buffer_id);
            let wakers = state.wakermap.remove(&buffer_id);
            drop(state);
            for w in wakers.into_iter().flatten() {
                w.wake();
            }
        })
        .detach();

        self.pending.insert(buffer_id, pending);
        self.buffer_read_ahead_current_pos += len;
    }

    fn offset_of(&self, pos: u64) -> usize {
        (pos - align_down(pos, self.buffer_size)) as usize
    }

    fn buffer_id(&self, pos: u64) -> u64 {
        pos / self.buffer_size
    }

    fn copy_data(&mut self, pos: u64, result: &mut [u8]) -> usize {
        let buffer_id = self.buffer_id(pos);
        let in_buffer_offset = self.offset_of(pos);
        if pos >= self.max_pos {
            return 0;
        }
        let max_len = std::cmp::min(result.len(), (self.max_pos - pos) as usize);
        let len: usize;

        match self.buffermap.get(&buffer_id) {
            None => {
                panic!("Buffer not found. But we should only call this function after we verified that all buffers exist");
            }
            Some(buffer) => {
                len = buffer.copy_to_slice(in_buffer_offset, &mut result[..max_len]);
            }
        }
        len
    }

    fn add_waker(&mut self, buffer_id: u64, waker: Waker) {
        match self.wakermap.get_mut(&buffer_id) {
            Some(v) => {
                v.push(waker);
            }
            None => {
                self.wakermap.insert(buffer_id, vec![waker]);
            }
        }
    }

    fn cancel_all_in_flight(&mut self) -> Vec<task::JoinHandle<(), ()>> {
        let mut handles = Vec::new();
        for (_k, v) in self.pending.drain() {
            v.cancel();
            handles.push(v);
        }
        handles
    }
}

impl Drop for StreamReaderState {
    fn drop(&mut self) {
        for (_k, v) in self.pending.drain() {
            v.cancel();
        }
    }
}

#[derive(Debug)]
/// Provides linear access to a [`DmaFile`]. The [`DmaFile`] is a convenient way to
/// manage a file through Direct I/O, but its interface is conductive to random access,
/// as a position must always be specified.
///
/// In situations where the file must be scanned linearly - either because a large chunk
/// is to be read or because we are scanning the whole file, it may be more convenient to use
/// a linear scan API.
///
/// The [`StreamReader`] implements [`AsyncRead`], which can offer the user with a convenient
/// way of issuing reads. However note that this mandates a copy between the Dma Buffer used
/// to read the file contents and the user-specified buffer.
///
/// To avoid that copy the [`StreamReader`] provides the [`get_buffer_aligned`] method which
/// exposes the buffer as a byte slice. Different situations will call for different APIs to
/// be used.
///
/// [`DmaFile`]: struct.DmaFile.html
/// [`DmaBuffer`]: struct.DmaBuffer.html
/// [`StreamReader`]: struct.StreamReader.html
/// [`get_buffer_aligned`]: struct.StreamReader.html#method.get_buffer_aligned
/// [`AsyncRead`]: https://docs.rs/futures/0.3.5/futures/io/trait.AsyncRead.html
pub struct StreamReader {
    start: u64,
    end: u64,
    current_pos: u64,
    buffer_size: u64,
    file: Rc<DmaFile>,
    state: Rc<RefCell<StreamReaderState>>,
}

macro_rules! collect_error {
    ( $state:expr, $res:expr ) => {{
        let mut state = $state.borrow_mut();
        if let Err(x) = $res {
            state.error = Some(x.into());
            true
        } else {
            false
        }
    }};
}

macro_rules! close_rc_file {
    ( $state:expr, $file:expr ) => {
        let inner = Rc::get_mut(&mut $file);
        match inner {
            None => {
                let mut state = $state.borrow_mut();
                state.error = Some(io::Error::new(
                    io::ErrorKind::Other,
                    format!("{} references to file still held", Rc::strong_count(&$file)),
                ));
            }
            Some(f) => {
                let res = f.close().await;
                collect_error!($state, res);
            }
        }
    };
}

impl StreamReader {
    /// Closes this [`StreamReader`].
    ///
    /// It is illegal to close the [`StreamReader`] more than once.
    ///
    /// # Examples
    /// ```no_run
    /// use scipio::{DmaFile, StreamReaderBuilder, LocalExecutor};
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async {
    ///     let file = DmaFile::open("myfile.txt").await.unwrap();
    ///     let mut reader = StreamReaderBuilder::new(file).build();
    ///     reader.close().await.unwrap();
    /// });
    /// ```
    pub async fn close(&mut self) -> io::Result<()> {
        let mut state = self.state.borrow_mut();
        let handles = state.cancel_all_in_flight();
        drop(state);
        join_all(handles).await;
        close_rc_file!(self.state, self.file);

        let mut state = self.state.borrow_mut();
        match state.error.take() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn new(builder: StreamReaderBuilder) -> StreamReader {
        let state = StreamReaderState {
            buffer_read_ahead_current_pos: align_down(builder.start, builder.buffer_size as u64),
            read_ahead: builder.read_ahead,
            max_pos: builder.end,
            buffer_size: builder.buffer_size as u64,
            wakermap: HashMap::new(),
            buffermap: HashMap::new(),
            pending: HashMap::new(),
            error: None,
        };

        let state = Rc::new(RefCell::new(state));

        {
            let state_rc = state.clone();
            let mut state = state.borrow_mut();
            state.replenish_read_ahead(state_rc, builder.file.clone());
        }

        StreamReader {
            file: builder.file,
            start: builder.start,
            end: builder.end,
            current_pos: builder.start,
            buffer_size: builder.buffer_size as _,
            state,
        }
    }

    /// Skip reading bytes from this [`StreamReader`].
    ///
    /// The file cursor is advanced by the provided bytes. As this is a linear
    /// access API, once those bytes are skipped they are gone. If you need to
    /// read those bytes, just not now, it is better to read them and save them
    /// in some temporary buffer.
    ///
    /// # Examples
    /// ```no_run
    /// use scipio::{DmaFile, StreamReaderBuilder, LocalExecutor};
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async {
    ///     let file = DmaFile::open("myfile.txt").await.unwrap();
    ///     let mut reader = StreamReaderBuilder::new(file).build();
    ///     assert_eq!(reader.current_pos(), 0);
    ///     reader.skip(8);
    ///     assert_eq!(reader.current_pos(), 8);
    ///     reader.close().await.unwrap();
    /// });
    /// ```
    ///
    /// [`StreamReader`]: struct.StreamReader.html
    pub fn skip(&mut self, bytes: u64) {
        let mut state = self.state.borrow_mut();
        let buffer_id = state.buffer_id(self.current_pos);
        self.current_pos = std::cmp::min(self.current_pos + bytes, self.end);
        let new_buffer_id = state.buffer_id(self.current_pos);
        let candidate_read_ahead_pos = align_down(self.current_pos, self.buffer_size);
        state.buffer_read_ahead_current_pos = std::cmp::max(
            state.buffer_read_ahead_current_pos,
            candidate_read_ahead_pos,
        );

        // remember the end range is exclusive so if we didn't cross a buffer
        // we don't discard anything
        for id in buffer_id..new_buffer_id {
            state.discard_buffer(id);
        }
        state.replenish_read_ahead(self.state.clone(), self.file.clone());
    }

    /// Acquires the current position of this [`StreamReader`].
    ///
    /// # Examples
    /// ```no_run
    /// use scipio::{DmaFile, StreamReaderBuilder, LocalExecutor};
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async {
    ///     let file = DmaFile::open("myfile.txt").await.unwrap();
    ///     let mut reader = StreamReaderBuilder::new(file).build();
    ///     assert_eq!(reader.current_pos(), 0);
    ///     reader.skip(8);
    ///     assert_eq!(reader.current_pos(), 8);
    ///     reader.close().await.unwrap();
    /// });
    /// ```
    ///
    /// [`StreamReader`]: struct.StreamReader.html
    pub fn current_pos(&self) -> u64 {
        self.current_pos
    }

    /// Allows access to the buffer that holds the current position with no extra copy
    ///
    /// In order to use this API, one must guarantee that reading the specified length may cross
    /// into a different buffer.  Users of this API are expected to be aware of their buffer size
    /// (selectable in the [`StreamReaderBuilder`]) and act accordingly.
    ///
    /// The buffer is also not released until the returned [`ReadResult`] goes out of scope. So
    /// if you plan to keep this alive for a long time this is probably the wrong API.
    ///
    /// Let's say you want to open a file and check if its header is sane: this is a good API for
    /// that.
    ///
    /// But if after such header there is an index that you want to keep in memory, then you
    /// are probably better off with one of the methods from [`AsyncReadExt`].
    ///
    /// # Examples
    /// ```no_run
    /// use scipio::{DmaFile, StreamReaderBuilder, LocalExecutor};
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async {
    ///     let file = DmaFile::open("myfile.txt").await.unwrap();
    ///     let mut reader = StreamReaderBuilder::new(file).build();
    ///     assert_eq!(reader.current_pos(), 0);
    ///     let result = reader.get_buffer_aligned(512).await.unwrap();
    ///     assert_eq!(result.len(), 512);
    ///     println!("First 512 bytes: {:?}", result.as_bytes());
    ///     reader.close().await.unwrap();
    /// });
    /// ```
    ///
    /// [`StreamReader`]: struct.StreamReader.html
    /// [`StreamReaderBuilder`]: struct.StreamReaderBuilder.html
    /// [`AsyncReadExt`]: https://docs.rs/futures/0.3.5/futures/io/trait.AsyncReadExt.html
    /// [`ReadResult`]: struct.ReadResult.html
    pub async fn get_buffer_aligned(&mut self, len: u64) -> io::Result<ReadResult> {
        let x = PrepareBuffer::new(
            self.state.clone(),
            self.file.clone(),
            self.current_pos,
            len,
            false,
        )
        .await?;
        self.skip(len);
        Ok(x)
    }
}

impl AsyncRead for StreamReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state.borrow_mut();
        if let Some(err) = current_error!(state) {
            return Poll::Ready(err);
        }

        let mut pos = self.current_pos;

        let start = state.buffer_id(pos);
        let end = state.buffer_id(pos + buf.len() as u64) + 1;
        for id in start..end {
            match state.buffermap.get(&id) {
                Some(buffer) => {
                    if (buffer.len() as u64) < self.buffer_size {
                        break;
                    }
                }
                None => {
                    state.fill_buffer(self.state.clone(), self.file.clone());
                    state.add_waker(id, cx.waker().clone());
                    return Poll::Pending;
                }
            }
        }

        let mut current_offset = 0;
        while current_offset < buf.len() {
            let bytes_copied = state.copy_data(pos, &mut buf[current_offset..]);
            current_offset += bytes_copied;
            pos += bytes_copied as u64;
            if bytes_copied == 0 {
                break;
            }
        }
        drop(state);
        self.skip(current_offset as u64);
        Poll::Ready(Ok(current_offset))
    }
}

#[derive(Debug)]
/// ReadResult encapsulates a buffer, returned by [`get_buffer_aligned`]
///
/// [`get_buffer_aligned`]: struct.StreamReader.html#method.get_buffer_aligned
pub struct ReadResult {
    buffer: Rc<DmaBuffer>,
    offset: usize,
    end: usize,
}

#[allow(clippy::len_without_is_empty)]
impl ReadResult {
    /// The length of this buffer
    pub fn len(&self) -> usize {
        self.end - self.offset
    }

    /// Allows accessing the contents of this buffer as a byte slice
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer.as_bytes()[self.offset..self.end]
    }
}

// If we had an I/O thread (or if -- one can hope -- uring could do mremap()), we could remap
// those buffers into a contiguous memory area and provide unaligned access. If we manage to
// accumulate all mremap from all threads in a single thread (instead of one syscall thread per
// executor), then the mmap sem would not ever need to be help and we can do that relatively
// efficiently.
//
// None of that is for now.
#[derive(Debug)]
pub struct PrepareBuffer {
    state: Rc<RefCell<StreamReaderState>>,
    file: Rc<DmaFile>,
    pos: u64,
    len: u64,
    merge_buffers: bool,
}

impl PrepareBuffer {
    fn new(
        state: Rc<RefCell<StreamReaderState>>,
        file: Rc<DmaFile>,
        pos: u64,
        len: u64,
        merge_buffers: bool,
    ) -> Self {
        PrepareBuffer {
            state,
            file,
            pos,
            len,
            merge_buffers,
        }
    }
}

impl Future for PrepareBuffer {
    type Output = io::Result<ReadResult>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.borrow_mut();

        let start_id = state.buffer_id(self.pos);
        let end_id = state.buffer_id(self.pos + self.len);
        if start_id != end_id {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::WouldBlock, format!("Reading {} bytes from position {} would cross a buffer boundary (Buffer size {})", self.len, self.pos, state.buffer_size))));
        }

        if let Some(err) = current_error!(state) {
            return Poll::Ready(err);
        }

        let buffer_id = state.buffer_id(self.pos);
        let offset = state.offset_of(self.pos);

        match state.buffermap.get(&buffer_id) {
            None => {
                state.fill_buffer(self.state.clone(), self.file.clone());
                state.add_waker(buffer_id, cx.waker().clone());
                Poll::Pending
            }
            Some(buffer) => {
                let len = std::cmp::min(self.len as usize, buffer.len() - offset);
                Poll::Ready(Ok(ReadResult {
                    buffer: buffer.clone(),
                    offset,
                    end: offset + len,
                }))
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::dma_file::align_up;
    use crate::dma_file::test::make_test_directories;
    use futures::AsyncReadExt;
    use std::io::ErrorKind;

    macro_rules! file_stream_read_test {
        ( $name:ident, $dir:ident, $kind:ident, $file:ident, $file_size:ident: $size:tt, $code:block) => {
            #[test]
            fn $name() {
                for dir in make_test_directories(stringify!($name)) {
                    let $dir = dir.path.clone();
                    let $kind = dir.kind;
                    test_executor!(async move {
                        let filename = $dir.join("testfile");
                        let mut new_file = DmaFile::create(&filename)
                            .await
                            .expect("failed to create file");
                        if $size > 0 {
                            let bufsz = align_up($size, 4096);
                            let mut buf = DmaBuffer::new(bufsz as _).unwrap();
                            for (v, x) in buf.as_mut_bytes().iter_mut().enumerate() {
                                *x = v as u8;
                            }
                            new_file.write_dma(&buf, 0).await.unwrap();
                            if bufsz != $size {
                                new_file.truncate($size).await.unwrap();
                            }
                        }
                        new_file.close().await.unwrap();
                        let $file = DmaFile::open(&filename).await.unwrap();
                        let $file_size = $size;
                        $code
                    });
                }
            }
        };
    }

    macro_rules! check_contents {
        ( $buf:expr, $start:expr ) => {
            for (idx, i) in $buf.iter().enumerate() {
                assert_eq!(*i, ($start + (idx as u64)) as u8);
            }
        };
    }

    file_stream_read_test!(read_exact_empty_file, path, _k, file, _file_size: 0, {
        let mut reader = StreamReaderBuilder::new(file)
            .build();

        let mut buf = [0u8; 128];
        match reader.read_exact(&mut buf).await {
            Err(x) => match x.kind() {
                ErrorKind::UnexpectedEof => {},
                _ => panic!("unexpected error"),
            }
            _ => panic!("unexpected success"),
        }
    });

    file_stream_read_test!(read_exact_zero_buffer, path, _k, file, _file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(0)
            .build();

        let mut buf = [0u8; 1024];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_exact_single_buffer, path, _k, file, _file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(64 << 10)
            .build();

        let mut buf = [0u8; 1024];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_exact_crosses_buffer, path, _k, file, _file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(1 << 10)
            .build();

        let mut buf = [0u8; 8000];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_exact_skip, path, _k, file, _file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(1 << 10)
            .build();

        let mut buf = [0u8; 128];
        reader.skip(1000);
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 1000);
    });

    file_stream_read_test!(read_exact_late_start, path, _k, file, file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_start_pos(file_size - 100)
            .with_buffer_size(1 << 10)
            .build();

        let mut buf = [0u8; 128];
        match reader.read_exact(&mut buf).await {
            Err(x) => match x.kind() {
                ErrorKind::UnexpectedEof => {},
                _ => panic!("unexpected error"),
            }
            _ => panic!("unexpected success"),
        }
        assert_eq!(reader.current_pos(), 128 << 10);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_exact_early_eof, path, _k, file, _file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_end_pos(100)
            .with_buffer_size(1 << 10)
            .build();

        let mut buf = [0u8; 128];
        match reader.read_exact(&mut buf).await {
            Err(x) => match x.kind() {
                ErrorKind::UnexpectedEof => {},
                _ => panic!("unexpected error"),
            }
            _ => panic!("unexpected success"),
        }
        assert_eq!(reader.current_pos(), 100);
        reader.close().await.unwrap();
    });

    // note the size is 128k + 2bytes
    file_stream_read_test!(read_exact_unaligned_file_size, path, _k, file, file_size: 131074, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(1 << 10)
            .build();

        let mut buf = Vec::with_capacity(file_size);
        buf.resize(file_size, 0);
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_to_end_no_read_ahead, path, _k, file, file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(4096)
            .with_read_ahead(0)
            .build();

        let mut buf = Vec::new();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x, file_size);
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_to_end_of_empty_file, path, _k, file, _file_size: 0, {
        let mut reader = StreamReaderBuilder::new(file)
            .build();

        let mut buf = Vec::new();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_to_end_single_buffer, path, _k, file, file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(file_size)
            .build();

        let mut buf = Vec::new();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x, file_size);
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_to_end_buffer_crossing, path, _k, file, file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(1024)
            .build();

        let mut buf = Vec::new();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x as u64, file_size);
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_to_end_buffer_crossing_read_ahead, path, _k, file, file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(4096)
            .with_read_ahead(64)
            .build();

        let mut buf = Vec::new();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x as u64, file_size);
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_to_end_after_skip, path, _k, file, file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(4096)
            .with_read_ahead(2)
            .build();

        let mut buf = Vec::new();
        reader.skip(file_size - 128);
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x as u64, 128);
        check_contents!(buf, file_size - 128);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_simple, path, _k, file, file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .build();

        let mut buf = [0u8; 4096];
        for i in 0..100 {
            let x = reader.read(&mut buf).await.unwrap();
            if i < file_size / 4096 {
                assert_eq!(x, 4096);
                check_contents!(buf, {i * 4096});
            } else {
                assert_eq!(x, 0);
            }
        }
        reader.close().await.unwrap();
    });

    // note the size is 128k + 2 bytes
    file_stream_read_test!(read_get_buffer_aligned, path, _k, file, _file_size: 131074, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(1024)
            .build();

        let buffer = reader.get_buffer_aligned(24).await.unwrap();
        assert_eq!(buffer.len(), 24);
        check_contents!(buffer.as_bytes(), 0);

        reader.skip(980);

        let buffer = reader.get_buffer_aligned(8).await.unwrap();
        assert_eq!(buffer.len(), 8);
        check_contents!(buffer.as_bytes(), 1004);

        match reader.get_buffer_aligned(20).await {
            Err(_) => {},
            Ok(_) => panic!("Expected an error"),
        }
        assert_eq!(reader.current_pos(), 1012);
        reader.skip((128 << 10) - 1012);
        let eof_short_buffer = reader.get_buffer_aligned(4).await.unwrap();
        assert_eq!(eof_short_buffer.len(), 2);
        check_contents!(eof_short_buffer.as_bytes(), 131072);

        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_mixed_api, path, _k, file, _file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(1024)
            .build();

        let buffer = reader.get_buffer_aligned(24).await.unwrap();
        assert_eq!(buffer.len(), 24);
        check_contents!(buffer.as_bytes(), 0);

        let mut buf = [0u8; 2000];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 24);
        assert_eq!(reader.current_pos(), 2024);

        reader.close().await.unwrap();
    });

    macro_rules! expect_specific_error {
        ( $op:expr, $err:expr ) => {
            match $op {
                Ok(_) => panic!("should have failed"),
                Err(x) => {
                    let kind = x.kind();
                    match kind {
                        io::ErrorKind::Other => {
                            assert_eq!($err, format!("{:?}", x.into_inner().unwrap()));
                        }
                        _ => panic!("Wrong error"),
                    }
                }
            }
        };
    }

    file_stream_read_test!(read_close_twice, path, _k, file, _file_size: 131072, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_buffer_size(1024)
            .build();

        reader.close().await.unwrap();
        expect_specific_error!(reader.close().await, "\"Bad file descriptor (os error 9)\"");
    });

    file_stream_read_test!(read_wronly_file, path, _k, _file, _file_size: 131072, {
        let newfile = path.join("wronly_file");
        let rfile = DmaFile::create(&newfile).await.unwrap();
        let mut reader = StreamReaderBuilder::new(rfile)
            .with_buffer_size(1024)
            .build();

        let mut buf = [0u8; 2000];
        expect_specific_error!(reader.read_exact(&mut buf).await, "\"Bad file descriptor (os error 9)\"");
        reader.close().await.unwrap();
    });
}
