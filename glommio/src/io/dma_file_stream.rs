// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::{
    io::{dma_file::align_down, read_result::ReadResult, DmaFile},
    sys::DmaBuffer,
    task,
    ByteSliceExt,
    ByteSliceMutExt,
    GlommioError,
    Local,
    ResourceType,
};
use ahash::AHashMap;
use core::task::Waker;
use futures_lite::{
    future::poll_fn,
    io::{AsyncRead, AsyncWrite},
    stream::{self, StreamExt},
};
use std::{
    cell::RefCell,
    cmp::Reverse,
    io,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    vec::Vec,
};

type Result<T> = crate::Result<T, ()>;

macro_rules! current_error {
    ( $state:expr ) => {
        $state.error.take().map(|err| Err(err.into()))
    };
}

#[derive(Debug, Clone)]
/// Builds a DmaStreamReader, allowing linear access to a Direct I/O [`DmaFile`]
///
/// [`DmaFile`]: struct.DmaFile.html
pub struct DmaStreamReaderBuilder {
    start: u64,
    end: u64,
    buffer_size: usize,
    read_ahead: usize,
    pub(super) file: Rc<DmaFile>,
}

impl DmaStreamReaderBuilder {
    /// Creates a new DmaStreamReaderBuilder, given a shared pointer to a
    /// [`DmaFile`]
    ///
    /// This method is useful in situations where a shared pointer was already
    /// present. For example if you need to scan a header in the file and
    /// also make it available for random reads.
    ///
    /// Other than that, it is the same as [`new`]
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{
    ///     io::{DmaFile, DmaStreamReaderBuilder},
    ///     Local,
    ///     LocalExecutor,
    /// };
    /// use std::rc::Rc;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = Rc::new(DmaFile::open("myfile.txt").await.unwrap());
    ///     let _reader = DmaStreamReaderBuilder::from_rc(file.clone()).build();
    ///
    ///     // issue random I/O now, even though a stream is open.
    ///     Local::local(async move {
    ///         file.read_at(0, 8).await.unwrap();
    ///     })
    ///     .await;
    /// });
    /// ```
    /// [`DmaFile`]: struct.DmaFile.html
    /// [`DmaStreamReader`]: struct.DmaStreamReader.html
    /// [`build`]: #method.build
    /// [`new`]: #method.new
    #[must_use = "The builder must be built to be useful"]
    pub fn from_rc(file: Rc<DmaFile>) -> DmaStreamReaderBuilder {
        DmaStreamReaderBuilder {
            start: 0,
            end: u64::MAX,
            buffer_size: 128 << 10,
            read_ahead: 4,
            file,
        }
    }

    /// Creates a new DmaStreamReaderBuilder, given a [`DmaFile`]
    ///
    /// Various properties can be set by using its `with` methods.
    ///
    /// A [`DmaStreamReader`] can later be constructed from it by
    /// calling [`build`]
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{
    ///     io::{DmaFile, DmaStreamReaderBuilder},
    ///     LocalExecutor,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::open("myfile.txt").await.unwrap();
    ///     let _reader = DmaStreamReaderBuilder::new(file).build();
    /// });
    /// ```
    /// [`DmaFile`]: struct.DmaFile.html
    /// [`DmaStreamReader`]: struct.DmaStreamReader.html
    /// [`build`]: #method.build
    #[must_use = "The builder must be built to be useful"]
    pub fn new(file: DmaFile) -> DmaStreamReaderBuilder {
        Self::from_rc(Rc::new(file))
    }

    /// Define a starting position.
    ///
    /// Reads from the [`DmaStreamReader`] will start from this position
    ///
    /// [`DmaStreamReader`]: struct.DmaStreamReader.html
    pub fn with_start_pos(mut self, start: u64) -> Self {
        self.start = start;
        self
    }

    /// Define an end position.
    ///
    /// Reads from the [`DmaStreamReader`] will end at this position even if the
    /// file is larger.
    ///
    /// [`DmaStreamReader`]: struct.DmaStreamReader.html
    pub fn with_end_pos(mut self, end: u64) -> Self {
        self.end = end;
        self
    }

    /// Define the number of read-ahead buffers that will be used by the
    /// [`DmaStreamReader`]
    ///
    /// Higher read-ahead numbers mean more parallelism but also more memory
    /// usage.
    ///
    /// [`DmaStreamReader`]: struct.DmaStreamReader.html
    pub fn with_read_ahead(mut self, read_ahead: usize) -> Self {
        self.read_ahead = read_ahead;
        self
    }

    /// Define the buffer size that will be used by the [`DmaStreamReader`]
    ///
    /// [`DmaStreamReader`]: struct.DmaStreamReader.html
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        let buffer_size = std::cmp::max(buffer_size, 1);
        self.buffer_size = self.file.align_up(buffer_size as _) as usize;
        self
    }

    /// Builds a [`DmaStreamReader`] with the properties defined by this
    /// [`DmaStreamReaderBuilder`]
    ///
    /// [`DmaStreamReader`]: struct.DmaStreamReader.html
    /// [`DmaStreamReaderBuilder`]: struct.DmaStreamReaderBuilder.html
    pub fn build(self) -> DmaStreamReader {
        DmaStreamReader::new(self)
    }
}

#[derive(Debug)]
struct DmaStreamReaderState {
    buffer_read_ahead_current_pos: u64,
    max_pos: u64,
    buffer_size: u64,
    buffer_size_mask: u64,
    buffer_size_shift: u64,
    read_ahead: usize,
    wakermap: AHashMap<u64, Waker>,
    pending: AHashMap<u64, task::JoinHandle<()>>,
    error: Option<io::Error>,
    buffermap: AHashMap<u64, ReadResult>,
    cached_buffer: Option<(u64, ReadResult)>,
}

impl DmaStreamReaderState {
    fn discard_buffer(&mut self, id: u64) {
        self.buffermap.remove(&id);
        if let Some(handle) = self.pending.remove(&id) {
            handle.cancel();
        }
    }

    fn replenish_read_ahead(&mut self, state: Rc<RefCell<Self>>, file: Rc<DmaFile>) {
        for _ in self.buffermap.len() + self.pending.len()..self.read_ahead {
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
            let buffer = file.read_at_aligned(pos, len as _).await;

            let mut state = read_state.borrow_mut();
            match buffer {
                Ok(buf) => {
                    state.buffermap.insert(buffer_id, buf);
                }
                Err(x) => {
                    state.error = Some(x.into());
                }
            }
            state.pending.remove(&buffer_id);
            let wakers = state.wakermap.remove(&buffer_id);
            drop(state);
            for w in wakers.into_iter() {
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
        (pos & self.buffer_size_mask) >> self.buffer_size_shift
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
                panic!(
                    "Buffer not found. But we should only call this function after we verified \
                     that all buffers exist"
                );
            }
            Some(buffer) => {
                len = buffer.read_at(in_buffer_offset, &mut result[..max_len]);
            }
        }
        len
    }

    fn add_waker(&mut self, buffer_id: u64, waker: Waker) {
        match self.wakermap.get_mut(&buffer_id) {
            Some(_) => {
                panic!("More than one waker in a linear access API. Something is wrong!");
            }
            None => {
                self.wakermap.insert(buffer_id, waker);
            }
        }
    }

    fn cancel_all_in_flight(&mut self) -> Vec<task::JoinHandle<()>> {
        let mut handles = Vec::new();
        for (_k, v) in self.pending.drain() {
            v.cancel();
            handles.push(v);
        }
        handles
    }

    fn update_cached_buffer(&mut self, buffer_id: &u64) -> Option<&ReadResult> {
        if let Some(buffer) = self.buffermap.get(buffer_id) {
            self.cached_buffer = Some((*buffer_id, buffer.clone()));
            Some(buffer)
        } else {
            None
        }
    }

    fn get_cached_buffer(&mut self, buffer_id: &u64) -> Option<&ReadResult> {
        match &self.cached_buffer {
            None => return self.update_cached_buffer(buffer_id),
            Some((id, _)) => {
                if id != buffer_id {
                    self.update_cached_buffer(buffer_id);
                }
            }
        }

        if let Some((id, buffer)) = &self.cached_buffer {
            if id == buffer_id {
                return Some(buffer);
            }
        }
        None
    }
}

impl Drop for DmaStreamReaderState {
    fn drop(&mut self) {
        for (_k, v) in self.pending.drain() {
            v.cancel();
        }
    }
}

#[derive(Debug)]
/// Provides linear access to a [`DmaFile`]. The [`DmaFile`] is a convenient way
/// to manage a file through Direct I/O, but its interface is conductive to
/// random access, as a position must always be specified.
///
/// In situations where the file must be scanned linearly - either because a
/// large chunk is to be read or because we are scanning the whole file, it may
/// be more convenient to use a linear scan API.
///
/// The [`DmaStreamReader`] implements [`AsyncRead`], which can offer the user
/// with a convenient way of issuing reads. However note that this mandates a
/// copy between the Dma Buffer used to read the file contents and the
/// user-specified buffer.
///
/// To avoid that copy the [`DmaStreamReader`] provides the
/// [`get_buffer_aligned`] method which exposes the buffer as a byte slice.
/// Different situations will call for different APIs to be used.
///
/// [`DmaFile`]: struct.DmaFile.html
/// [`DmaBuffer`]: struct.DmaBuffer.html
/// [`DmaStreamReader`]: struct.DmaStreamReader.html
/// [`get_buffer_aligned`]:
/// struct.DmaStreamReader.html#method.get_buffer_aligned
/// [`AsyncRead`]: https://docs.rs/futures/0.3.5/futures/io/trait.AsyncRead.html
pub struct DmaStreamReader {
    start: u64,
    end: u64,
    current_pos: u64,
    buffer_size: u64,
    file: Rc<DmaFile>,
    state: Rc<RefCell<DmaStreamReaderState>>,
}

macro_rules! collect_error {
    ( $state:expr, $res:expr ) => {{
        if let Err(x) = $res {
            $state.borrow_mut().error = Some(x.into());
            true
        } else {
            false
        }
    }};
}

impl DmaStreamReader {
    /// Closes this [`DmaStreamReader`].
    ///
    /// It is illegal to close the [`DmaStreamReader`] more than once.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{
    ///     io::{DmaFile, DmaStreamReaderBuilder},
    ///     LocalExecutor,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::open("myfile.txt").await.unwrap();
    ///     let mut reader = DmaStreamReaderBuilder::new(file).build();
    ///     reader.close().await.unwrap();
    /// });
    /// ```
    pub async fn close(self) -> Result<()> {
        let handles = {
            let mut state = self.state.borrow_mut();
            state.cancel_all_in_flight()
        };

        let to_cancel = handles.len();
        let cancelled = stream::iter(handles).then(|f| f).count().await;
        assert_eq!(to_cancel, cancelled);
        let res = self.file.close_rc().await;
        collect_error!(self.state, res);

        let mut state = self.state.borrow_mut();
        match state.error.take() {
            Some(err) => Err(err.into()),
            None => Ok(()),
        }
    }

    fn new(builder: DmaStreamReaderBuilder) -> DmaStreamReader {
        let state = DmaStreamReaderState {
            buffer_read_ahead_current_pos: align_down(builder.start, builder.buffer_size as u64),
            read_ahead: builder.read_ahead,
            max_pos: builder.end,
            buffer_size: builder.buffer_size as u64,
            buffer_size_mask: !(builder.buffer_size as u64 - 1),
            buffer_size_shift: (builder.buffer_size as f64).log2() as u64,
            wakermap: AHashMap::with_capacity(builder.read_ahead),
            buffermap: AHashMap::with_capacity(builder.read_ahead),
            pending: AHashMap::with_capacity(builder.read_ahead),
            error: None,
            cached_buffer: None,
        };

        let state = Rc::new(RefCell::new(state));

        {
            let state_rc = state.clone();
            let mut state = state.borrow_mut();
            state.replenish_read_ahead(state_rc, builder.file.clone());
        }

        DmaStreamReader {
            file: builder.file,
            start: builder.start,
            end: builder.end,
            current_pos: builder.start,
            buffer_size: builder.buffer_size as _,
            state,
        }
    }

    /// Skip reading bytes from this [`DmaStreamReader`].
    ///
    /// The file cursor is advanced by the provided bytes. As this is a linear
    /// access API, once those bytes are skipped they are gone. If you need to
    /// read those bytes, just not now, it is better to read them and save them
    /// in some temporary buffer.
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{
    ///     io::{DmaFile, DmaStreamReaderBuilder},
    ///     LocalExecutor,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::open("myfile.txt").await.unwrap();
    ///     let mut reader = DmaStreamReaderBuilder::new(file).build();
    ///     assert_eq!(reader.current_pos(), 0);
    ///     reader.skip(8);
    ///     assert_eq!(reader.current_pos(), 8);
    ///     reader.close().await.unwrap();
    /// });
    /// ```
    ///
    /// [`DmaStreamReader`]: struct.DmaStreamReader.html
    pub fn skip(&mut self, bytes: u64) {
        let mut state = self.state.borrow_mut();
        let buffer_id = state.buffer_id(self.current_pos);
        self.current_pos = std::cmp::min(self.current_pos + bytes, self.end);
        let new_buffer_id = state.buffer_id(self.current_pos);
        if buffer_id != new_buffer_id {
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
    }

    /// Acquires the current position of this [`DmaStreamReader`].
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{
    ///     io::{DmaFile, DmaStreamReaderBuilder},
    ///     LocalExecutor,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::open("myfile.txt").await.unwrap();
    ///     let mut reader = DmaStreamReaderBuilder::new(file).build();
    ///     assert_eq!(reader.current_pos(), 0);
    ///     reader.skip(8);
    ///     assert_eq!(reader.current_pos(), 8);
    ///     reader.close().await.unwrap();
    /// });
    /// ```
    ///
    /// [`DmaStreamReader`]: struct.DmaStreamReader.html
    pub fn current_pos(&self) -> u64 {
        self.current_pos
    }

    /// Allows access to the buffer that holds the current position with no
    /// extra copy
    //_ In order to use this API, one must guarantee that reading the specified
    /// length may not cross into a different buffer.  Users of this API are
    /// expected to be aware of their buffer size (selectable in the
    /// [`DmaStreamReaderBuilder`]) and act accordingly.
    ///
    /// The buffer is also not released until the returned [`ReadResult`] goes
    /// out of scope. So if you plan to keep this alive for a long time this
    /// is probably the wrong API.
    ///
    /// If EOF is hit while reading with this method, the number of bytes in the
    /// returned buffer will be less than number requested.
    ///
    /// Let's say you want to open a file and check if its header is sane: this
    /// is a good API for that.
    ///
    /// But if after such header there is an index that you want to keep in
    /// memory, then you are probably better off with one of the methods
    /// from [`AsyncReadExt`].
    ///
    /// # Examples
    /// ```no_run
    /// use glommio::{
    ///     io::{DmaFile, DmaStreamReaderBuilder},
    ///     LocalExecutor,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::open("myfile.txt").await.unwrap();
    ///     let mut reader = DmaStreamReaderBuilder::new(file).build();
    ///     assert_eq!(reader.current_pos(), 0);
    ///     let result = reader.get_buffer_aligned(512).await.unwrap();
    ///     assert_eq!(result.len(), 512);
    ///     println!("First 512 bytes: {:?}", &*result);
    ///     reader.close().await.unwrap();
    /// });
    /// ```
    ///
    /// [`DmaStreamReader`]: struct.DmaStreamReader.html
    /// [`DmaStreamReaderBuilder`]: struct.DmaStreamReaderBuilder.html
    /// [`AsyncReadExt`]: https://docs.rs/futures-lite/1.11.2/futures_lite/io/trait.AsyncReadExt.html
    /// [`ReadResult`]: struct.ReadResult.html
    pub async fn get_buffer_aligned(&mut self, len: u64) -> Result<ReadResult> {
        if len == 0 {
            return Ok(ReadResult::empty_buffer());
        }

        let (start_id, end_id, buffer_size) = {
            let state = self.state.borrow();
            let start_id = state.buffer_id(self.current_pos);
            let end_id = state.buffer_id(self.current_pos + len - 1);
            (start_id, end_id, state.buffer_size)
        };

        if start_id != end_id {
            return Err(GlommioError::<()>::WouldBlock(ResourceType::File(format!(
                "Reading {} bytes from position {} would cross a buffer boundary (Buffer size {})",
                len, self.current_pos, buffer_size
            ))));
        }

        let x = poll_fn(|cx| self.get_buffer(cx, len, start_id)).await?;
        self.skip(len);
        Ok(x)
    }

    fn get_buffer(
        &mut self,
        cx: &mut Context<'_>,
        len: u64,
        buffer_id: u64,
    ) -> Poll<Result<ReadResult>> {
        let mut state = self.state.borrow_mut();
        if let Some(err) = current_error!(state) {
            return Poll::Ready(err);
        }

        match state.get_cached_buffer(&buffer_id).cloned() {
            None => {
                state.fill_buffer(self.state.clone(), self.file.clone());
                state.add_waker(buffer_id, cx.waker().clone());
                Poll::Pending
            }
            Some(buffer) => {
                let offset = state.offset_of(self.current_pos);
                if buffer.len() <= offset {
                    Poll::Ready(Ok(ReadResult::empty_buffer()))
                } else {
                    let len = std::cmp::min(len as usize, buffer.len() - offset);
                    Poll::Ready(ReadResult::slice(&buffer, offset, len).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "buffer offset out of range")
                            .into()
                    }))
                }
            }
        }
    }
}

impl AsyncRead for DmaStreamReader {
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
        if pos > state.max_pos {
            return Poll::Ready(Ok(0));
        }

        let start = state.buffer_id(pos);
        let end = state.buffer_id(pos + buf.len() as u64);

        // special-casing the single buffer scenario helps small reads, as it allows
        // us to do a single buffer lookup instead of N * 2;
        if start == end {
            match state.get_cached_buffer(&start).cloned() {
                Some(buffer) => {
                    let max_len = std::cmp::min(buf.len(), (state.max_pos - pos) as usize);
                    let offset = state.offset_of(pos);
                    let bytes_copied = buffer.read_at(offset, &mut buf[..max_len]);
                    drop(state);
                    self.skip(bytes_copied as u64);
                    Poll::Ready(Ok(bytes_copied))
                }
                None => {
                    state.fill_buffer(self.state.clone(), self.file.clone());
                    state.add_waker(start, cx.waker().clone());
                    Poll::Pending
                }
            }
        } else {
            for id in start..=end {
                match state.get_cached_buffer(&id) {
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
}

#[derive(Debug)]
/// Builds a DmaStreamWriter, allowing linear access to a Direct I/O [`DmaFile`]
///
/// [`DmaFile`]: struct.DmaFile.html
pub struct DmaStreamWriterBuilder {
    buffer_size: usize,
    write_behind: usize,
    flush_on_close: bool,
    file: Rc<DmaFile>,
}

impl DmaStreamWriterBuilder {
    /// Creates a new DmaStreamWriterBuilder, given a [`DmaFile`]
    ///
    /// Various properties can be set by using its `with` methods.
    ///
    /// A [`DmaStreamWriter`] can later be constructed from it by
    /// calling [`build`]
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{
    ///     io::{DmaFile, DmaStreamWriterBuilder},
    ///     LocalExecutor,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::create("myfile.txt").await.unwrap();
    ///     let _reader = DmaStreamWriterBuilder::new(file).build();
    /// });
    /// ```
    /// [`DmaFile`]: struct.DmaFile.html
    /// [`DmaStreamWriter`]: struct.DmaStreamWriter.html
    /// [`build`]: #method.build
    #[must_use = "The builder must be built to be useful"]
    pub fn new(file: DmaFile) -> DmaStreamWriterBuilder {
        DmaStreamWriterBuilder {
            buffer_size: 128 << 10,
            write_behind: 4,
            flush_on_close: true,
            file: Rc::new(file),
        }
    }

    /// Define the number of write-behind buffers that will be used by the
    /// [`DmaStreamWriter`]
    ///
    /// Higher write-behind numbers mean more parallelism but also more memory
    /// usage. As long as there is still write-behind buffers available
    /// writing to this [`DmaStreamWriter`] will not block.
    ///
    /// [`DmaStreamWriter`]: struct.DmaStreamWriter.html
    pub fn with_write_behind(mut self, write_behind: usize) -> Self {
        self.write_behind = std::cmp::max(write_behind, 1);
        self
    }

    /// Does not issue a sync operation when closing the file. This is dangerous
    /// and in most cases may lead to data loss.
    pub fn with_sync_on_close_disabled(mut self, flush_disabled: bool) -> Self {
        self.flush_on_close = !flush_disabled;
        self
    }

    /// Define the buffer size that will be used by the [`DmaStreamWriter`]
    ///
    /// [`DmaStreamWriter`]: struct.DmaStreamWriter.html
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        let buffer_size = std::cmp::max(buffer_size, 1);
        self.buffer_size = self.file.align_up(buffer_size as _) as usize;
        self
    }

    /// Builds a [`DmaStreamWriter`] with the properties defined by this
    /// [`DmaStreamWriterBuilder`]
    ///
    /// [`DmaStreamWriter`]: struct.DmaStreamWriter.html
    /// [`DmaStreamWriterBuilder`]: struct.DmaStreamWriterBuilder.html
    pub fn build(self) -> DmaStreamWriter {
        DmaStreamWriter::new(self)
    }
}

#[derive(Debug)]
enum FileStatus {
    Open,    // Open, can be closed.
    Closing, // We are closing it.
    Closed,  // It was closed, but the poll
}

#[derive(Debug)]
struct DmaStreamWriterState {
    buffer_size: usize,
    waker: Option<Waker>,
    file_status: FileStatus,
    error: Option<io::Error>,
    pending: AHashMap<u64, task::JoinHandle<()>>,
    current_buffer: Option<DmaBuffer>,
    file_pos: u64,
    flush_id: u64,
    // this is so we track the last flushed pos. Buffers may return
    // out of order, so we can't report them as flushed_pos yet. Store for
    // later.
    out_of_order_write_returns: Vec<u64>,
    flushed_pos: u64,
    buffer_pos: usize,
    write_behind: usize,
    flush_on_close: bool,
}

macro_rules! already_closed {
    () => {
        // We do it like this so that the write and read close errors are exactly the
        // same. The reader uses enhanced errors, so it has a message in
        // the inner attribute of io::Error
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::Other,
            format!("{}", io::Error::from_raw_os_error(libc::EBADF)),
        )))
    };
}

macro_rules! ensure_not_closed {
    ( $file:expr ) => {
        match $file.take() {
            Some(file) => file,
            None => {
                return already_closed!();
            }
        }
    };
}

impl DmaStreamWriterState {
    fn add_waker(&mut self, waker: Waker) {
        // Linear file stream, not supposed to have parallel writers!!
        assert!(self.waker.is_none());
        self.waker = Some(waker);
    }

    fn initiate_close(
        &mut self,
        waker: Waker,
        state: Rc<RefCell<Self>>,
        file: Rc<DmaFile>,
        do_close: bool,
    ) {
        let final_pos = self.file_pos();
        let must_truncate = final_pos != self.file_pos;

        if self.buffer_pos > 0 {
            let buffer = self.current_buffer.take();
            self.flush_one_buffer(buffer.unwrap(), state.clone(), file.clone());
            if must_truncate {
                // flush will have adjusted that, we will fix.
                self.file_pos = final_pos;
            }
        }
        let mut drainers = std::mem::take(&mut self.pending);
        let flush_on_close = self.flush_on_close;
        self.file_status = FileStatus::Closing;
        Local::local(async move {
            defer! {
                waker.wake();
            }

            for (_, v) in drainers.drain() {
                v.await;
            }
            if state.borrow().error.is_some() {
                return;
            }

            if flush_on_close {
                let res = file.fdatasync().await;
                if collect_error!(state, res) {
                    return;
                }
            }

            if must_truncate {
                let res = file.truncate(final_pos).await;
                if collect_error!(state, res) {
                    return;
                }
            }

            if do_close {
                let res = file.close_rc().await;
                collect_error!(state, res);
            }

            let mut state = state.borrow_mut();
            state.flushed_pos = final_pos;
        })
        .detach();
    }

    fn file_pos(&self) -> u64 {
        self.file_pos + self.buffer_pos as u64
    }

    fn flushed_pos(&self) -> u64 {
        self.flushed_pos
    }

    fn adjust_flushed_pos(&mut self, just_written: u64) {
        self.out_of_order_write_returns.push(just_written);
        self.out_of_order_write_returns.sort_by_key(|x| Reverse(*x));
        while let Some(x) = self.out_of_order_write_returns.last() {
            if *x == self.flushed_pos {
                self.flushed_pos += self.buffer_size as u64;
                self.out_of_order_write_returns.pop();
            } else {
                return;
            }
        }
    }

    fn current_pending(&mut self) -> Vec<task::JoinHandle<()>> {
        let mut handles = Vec::new();
        for (_k, v) in self.pending.drain() {
            handles.push(v);
        }
        handles
    }

    fn flush_one_buffer(&mut self, buffer: DmaBuffer, state: Rc<RefCell<Self>>, file: Rc<DmaFile>) {
        let file_pos = self.file_pos;
        self.file_pos += self.buffer_size as u64;
        self.buffer_pos = 0;
        let flush_id = self.flush_id;
        self.flush_id += 1;
        self.pending.insert(
            flush_id,
            Local::local(async move {
                let res = file.write_at(buffer, file_pos).await;
                collect_error!(state, res);

                let mut state = state.borrow_mut();
                state.adjust_flushed_pos(file_pos);
                state.pending.remove(&flush_id); // can be None during close
                if let Some(waker) = state.waker.take() {
                    drop(state);
                    waker.wake();
                }
            })
            .detach(),
        );
    }
}

impl Drop for DmaStreamWriterState {
    fn drop(&mut self) {
        for (_, v) in self.pending.drain() {
            v.cancel();
        }
    }
}

#[derive(Debug)]
/// Provides linear access to a [`DmaFile`]. The [`DmaFile`] is a convenient way
/// to manage a file through Direct I/O, but its interface is conductive to
/// random access, as a position must always be specified.
///
/// Very rarely does one need to issue random writes to a file. Therefore, the
/// [`DmaStreamWriter`] is likely your go-to API when it comes to writing files.
///
/// The [`DmaStreamWriter`] implements [`AsyncWrite`]. Because it is backed by a
/// Direct I/O file, the flush method has no effect. Closing the file issues a
/// sync so that the data can be flushed from the internal NVMe caches.
///
/// [`DmaFile`]: struct.DmaFile.html
/// [`DmaStreamReader`]: struct.DmaStreamReader.html
/// [`get_buffer_aligned`]:
/// struct.DmaStreamReader.html#method.get_buffer_aligned
/// [`AsyncWrite`]: https://docs.rs/futures/0.3.5/futures/io/trait.AsyncWrite.html
pub struct DmaStreamWriter {
    file: Option<Rc<DmaFile>>,
    state: Rc<RefCell<DmaStreamWriterState>>,
}

impl DmaStreamWriter {
    fn new(builder: DmaStreamWriterBuilder) -> DmaStreamWriter {
        let state = DmaStreamWriterState {
            buffer_size: builder.buffer_size,
            write_behind: builder.write_behind,
            flush_on_close: builder.flush_on_close,
            current_buffer: None,
            waker: None,
            pending: AHashMap::new(),
            error: None,
            file_status: FileStatus::Open,
            file_pos: 0,
            buffer_pos: 0,
            flushed_pos: 0,
            flush_id: 0,
            out_of_order_write_returns: Vec::new(),
        };

        let state = Rc::new(RefCell::new(state));
        DmaStreamWriter {
            file: Some(builder.file),
            state,
        }
    }

    /// Acquires the current position of this [`DmaStreamWriter`].
    ///
    /// # Examples
    /// ```no_run
    /// use futures::io::AsyncWriteExt;
    /// use glommio::{
    ///     io::{DmaFile, DmaStreamWriterBuilder},
    ///     LocalExecutor,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::create("myfile.txt").await.unwrap();
    ///     let mut writer = DmaStreamWriterBuilder::new(file).build();
    ///     assert_eq!(writer.current_pos(), 0);
    ///     writer.write_all(&[0, 1, 2, 3, 4]).await.unwrap();
    ///     assert_eq!(writer.current_pos(), 5);
    ///     writer.close().await.unwrap();
    /// });
    /// ```
    ///
    /// [`DmaStreamWriter`]: struct.DmaStreamWriter.html
    pub fn current_pos(&self) -> u64 {
        self.state.borrow().file_pos()
    }

    /// Acquires the current position of this [`DmaStreamWriter`] that is
    /// flushed to the underlying media.
    ///
    /// Warning: the position reported by this API is not restart or crash safe.
    /// You need to call [`sync`] for that. Although the DmaStreamWriter
    /// uses Direct I/O, modern storage devices have their own caches and
    /// may still lose data that sits on those caches upon a restart until
    /// [`sync`] is called (Note that [`close`] implies a sync).
    ///
    /// However within the same session, new readers trying to read from any
    /// position before what we return in this method will be guaranteed to
    /// read the data we just wrote.
    ///
    /// # Examples
    /// ```no_run
    /// use futures::io::AsyncWriteExt;
    /// use glommio::{
    ///     io::{DmaFile, DmaStreamWriterBuilder},
    ///     LocalExecutor,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::create("myfile.txt").await.unwrap();
    ///     let mut writer = DmaStreamWriterBuilder::new(file).build();
    ///     assert_eq!(writer.current_pos(), 0);
    ///     writer.write_all(&[0, 1, 2, 3, 4]).await.unwrap();
    ///     assert_eq!(writer.current_pos(), 5);
    ///     // The write above is not enough to cause a flush
    ///     assert_eq!(writer.current_flushed_pos(), 0);
    ///     writer.close().await.unwrap();
    ///     // Close implies a forced-flush and a sync.
    ///     assert_eq!(writer.current_flushed_pos(), 5);
    /// });
    /// ```
    ///
    /// [`DmaStreamWriter`]: struct.DmaStreamWriter.html
    /// [`sync`]: struct.DmaStreamWriter.html#method.sync
    /// [`close`]: https://docs.rs/futures/0.3.5/futures/io/trait.AsyncWriteExt.html#method.close
    pub fn current_flushed_pos(&self) -> u64 {
        self.state.borrow().flushed_pos()
    }

    /// Waits for all currently in-flight buffers to return and be safely stored
    /// in the underlying storage
    ///
    /// Note that the current buffer being written to is not flushed, as it may
    /// not be properly aligned. Buffers that are currently in-flight will
    /// be waited on, and a sync operation will be issued by the operating
    /// system.
    ///
    /// Returns the flushed position of the file at the time the sync started.
    ///
    /// # Examples
    /// ```no_run
    /// use futures::io::AsyncWriteExt;
    /// use glommio::{
    ///     io::{DmaFile, DmaStreamWriterBuilder},
    ///     LocalExecutor,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = DmaFile::create("myfile.txt").await.unwrap();
    ///     let mut writer = DmaStreamWriterBuilder::new(file)
    ///         .with_buffer_size(4096)
    ///         .with_write_behind(2)
    ///         .build();
    ///     let buffer = [0u8; 5000];
    ///     writer.write_all(&buffer).await.unwrap();
    ///     // with 5000 bytes written into a 4096-byte buffer a flush
    ///     // has certainly started. But if very likely didn't finish right
    ///     // away. It will not be reflected on current_flushed_pos(), but a
    ///     // sync() will wait on it.
    ///     assert_eq!(writer.current_flushed_pos(), 0);
    ///     assert_eq!(writer.sync().await.unwrap(), 4096);
    ///     writer.close().await.unwrap();
    /// });
    /// ```
    pub async fn sync(&self) -> Result<u64> {
        let (mut pending, file_pos_at_sync_time) = {
            let mut state = self.state.borrow_mut();
            (state.current_pending(), state.file_pos)
        };

        for v in pending.drain(..) {
            v.await;
        }

        let file = self.file.clone().unwrap();
        file.fdatasync().await?;
        Ok(file_pos_at_sync_time)
    }

    // internal function that does everything that close does (flushes buffers, etc,
    // but leaves the file open. Useful for the immutable file abstraction.
    pub(super) fn poll_seal(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<DmaStreamReaderBuilder>> {
        let mut state = self.state.borrow_mut();
        match state.file_status {
            FileStatus::Open => {
                let file = ensure_not_closed!(self.file.clone());
                state.initiate_close(cx.waker().clone(), self.state.clone(), file, false);
                Poll::Pending
            }
            FileStatus::Closing => {
                state.file_status = FileStatus::Closed;
                let file = ensure_not_closed!(self.file);

                let wb = state.write_behind;
                let bufsz = state.buffer_size;
                let builder = DmaStreamReaderBuilder::from_rc(file)
                    .with_read_ahead(wb)
                    .with_buffer_size(bufsz);

                match current_error!(state) {
                    Some(err) => Poll::Ready(err),
                    None => Poll::Ready(Ok(builder)),
                }
            }
            FileStatus::Closed => {
                already_closed!()
            }
        }
    }
}

impl AsyncWrite for DmaStreamWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state.borrow_mut();
        if let Some(err) = current_error!(state) {
            return Poll::Ready(err);
        }

        let mut written = 0;
        while written < buf.len() {
            match state.current_buffer.take() {
                None => {
                    if state.pending.len() < state.write_behind {
                        state.current_buffer = Some(
                            self.file
                                .as_ref()
                                .unwrap()
                                .alloc_dma_buffer(state.buffer_size),
                        );
                    } else {
                        break;
                    }
                }
                Some(mut buffer) => {
                    let writesz = buffer.write_at(state.buffer_pos, &buf[written..]);
                    written += writesz;
                    state.buffer_pos += writesz;
                    if state.buffer_pos == state.buffer_size {
                        state.flush_one_buffer(
                            buffer,
                            self.state.clone(),
                            self.file.clone().unwrap(),
                        );
                    } else {
                        state.current_buffer = Some(buffer);
                    }
                }
            }
        }

        if written == 0 {
            state.add_waker(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Ok(written))
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let file = self.file.take();
        let mut state = self.state.borrow_mut();
        match state.file_status {
            FileStatus::Open => {
                state.initiate_close(cx.waker().clone(), self.state.clone(), file.unwrap(), true);
                Poll::Pending
            }
            FileStatus::Closing => {
                state.file_status = FileStatus::Closed;
                match current_error!(state) {
                    Some(err) => Poll::Ready(err),
                    None => Poll::Ready(Ok(())),
                }
            }
            FileStatus::Closed => {
                already_closed!()
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        io::dma_file::{align_up, test::make_test_directories},
        timer::Timer,
    };
    use futures::{AsyncReadExt, AsyncWriteExt};
    use std::{io::ErrorKind, time::Duration};

    macro_rules! file_stream_read_test {
        ( $name:ident, $dir:ident, $kind:ident, $file:ident, $file_size:ident: $size:tt, $code:block) => {
            #[test]
            fn $name() {
                for dir in make_test_directories(stringify!($name)) {
                    let $dir = dir.path.clone();
                    let $kind = dir.kind;
                    test_executor!(async move {
                        let filename = $dir.join("testfile");
                        let new_file = DmaFile::create(&filename)
                            .await
                            .expect("failed to create file");
                        if $size > 0 {
                            let bufsz = align_up($size, 4096);
                            let mut buf = DmaBuffer::new(bufsz as _).unwrap();
                            for (v, x) in buf.as_bytes_mut().iter_mut().enumerate() {
                                *x = v as u8;
                            }
                            new_file.write_at(buf, 0).await.unwrap();
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

    macro_rules! file_stream_write_test {
        ( $name:ident, $dir:ident, $kind:ident, $filename:ident, $file:ident, $code:block) => {
            #[test]
            fn $name() {
                for dir in make_test_directories(stringify!($name)) {
                    let $dir = dir.path.clone();
                    let $kind = dir.kind;
                    test_executor!(async move {
                        let $filename = $dir.join("testfile");
                        let $file = DmaFile::create(&$filename).await.unwrap();
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
        let mut reader = DmaStreamReaderBuilder::new(file)
            .build();

        let mut buf = [0u8; 128];
        match reader.read_exact(&mut buf).await {
            Err(x) => match x.kind() {
                ErrorKind::UnexpectedEof => {},
                _ => panic!("unexpected error"),
            }
            _ => panic!("unexpected success"),
        }
        reader.close().await.unwrap();
    });

    // other tests look like they may be doing this, but the buffer_size can be
    // rounded up So this one writes a bit more data
    file_stream_write_test!(write_more_than_write_behind, path, _k, filename, file, {
        let mut writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(128 << 10)
            .with_write_behind(2)
            .build();

        for i in 0..1_000_000 {
            assert_eq!(writer.current_pos(), i);
            writer.write_all(&[i as u8]).await.unwrap();
        }

        writer.close().await.unwrap();

        let rfile = DmaFile::open(&filename).await.unwrap();
        assert_eq!(rfile.file_size().await.unwrap(), 1_000_000);
    });

    file_stream_read_test!(read_exact_zero_buffer, path, _k, file, _file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
            .with_buffer_size(0)
            .build();

        let mut buf = [0u8; 1024];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_exact_single_buffer, path, _k, file, _file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
            .with_buffer_size(64 << 10)
            .build();

        let mut buf = [0u8; 1024];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_exact_crosses_buffer, path, _k, file, _file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
            .with_buffer_size(1 << 10)
            .build();

        let mut buf = [0u8; 8000];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_exact_skip, path, _k, file, _file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
            .with_buffer_size(1 << 10)
            .build();

        let mut buf = [0u8; 128];
        reader.skip(1000);
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 1000);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_exact_late_start, path, _k, file, file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
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
        let mut reader = DmaStreamReaderBuilder::new(file)
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
        let mut reader = DmaStreamReaderBuilder::new(file)
            .with_buffer_size(1 << 10)
            .build();

        let mut buf = vec![0; file_size];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_to_end_no_read_ahead, path, _k, file, file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
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
        let mut reader = DmaStreamReaderBuilder::new(file)
            .build();

        let mut buf = Vec::new();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_to_end_single_buffer, path, _k, file, file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
            .with_buffer_size(file_size)
            .build();

        let mut buf = Vec::new();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x, file_size);
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_to_end_buffer_crossing, path, _k, file, file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
            .with_buffer_size(1024)
            .build();

        let mut buf = Vec::new();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x as u64, file_size);
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_to_end_buffer_crossing_read_ahead, path, _k, file, file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
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
        let mut reader = DmaStreamReaderBuilder::new(file)
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
        let mut reader = DmaStreamReaderBuilder::new(file)
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
        let mut reader = DmaStreamReaderBuilder::new(file)
            .with_buffer_size(1024)
            .build();

        let buffer = reader.get_buffer_aligned(24).await.unwrap();
        assert_eq!(buffer.len(), 24);
        check_contents!(*buffer, 0);

        reader.skip(980);

        let buffer = reader.get_buffer_aligned(8).await.unwrap();
        assert_eq!(buffer.len(), 8);
        check_contents!(*buffer, 1004);

        match reader.get_buffer_aligned(20).await {
            Err(_) => {},
            Ok(_) => panic!("Expected an error"),
        }
        assert_eq!(reader.current_pos(), 1012);
        reader.skip((128 << 10) - 1012);
        let eof_short_buffer = reader.get_buffer_aligned(4).await.unwrap();
        assert_eq!(eof_short_buffer.len(), 2);
        check_contents!(*eof_short_buffer, 131072);

        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_get_buffer_aligned_zero_buffer, path, _k, file, _file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
            .with_buffer_size(131072)
            .build();

        let buffer = reader.get_buffer_aligned(0).await.unwrap();
        assert_eq!(buffer.len(), 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_get_buffer_aligned_entire_buffer_at_once, path, _k, file, _file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
            .with_buffer_size(131072)
            .build();

        let buffer = reader.get_buffer_aligned(131072).await.unwrap();
        assert_eq!(buffer.len(), 131072);
        check_contents!(*buffer, 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_get_buffer_aligned_past_the_end, path, _k, file, _file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
            .with_buffer_size(131072)
            .build();

        reader.skip(131073);
        let buffer = reader.get_buffer_aligned(32).await.unwrap();
        assert_eq!(buffer.len(), 0);
        reader.close().await.unwrap();
    });

    file_stream_read_test!(read_mixed_api, path, _k, file, _file_size: 131072, {
        let mut reader = DmaStreamReaderBuilder::new(file)
            .with_buffer_size(1024)
            .build();

        let buffer = reader.get_buffer_aligned(24).await.unwrap();
        assert_eq!(buffer.len(), 24);
        check_contents!(*buffer, 0);

        let mut buf = [0u8; 2000];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 24);
        assert_eq!(reader.current_pos(), 2024);

        reader.close().await.unwrap();
    });

    #[track_caller]
    fn expect_specific_error<T>(op: std::result::Result<T, io::Error>, expected_err: &'static str) {
        match op {
            Ok(_) => panic!("should have failed"),
            Err(err) => {
                let kind = err.kind();
                match kind {
                    io::ErrorKind::Other => {
                        let x = format!("{}", err.into_inner().unwrap());
                        assert!(x.starts_with(expected_err));
                    }
                    _ => panic!("Wrong error"),
                }
            }
        }
    }

    file_stream_read_test!(read_wronly_file, path, _k, _file, _file_size: 131072, {
        let newfile = path.join("wronly_file");
        let rfile = DmaFile::create(&newfile).await.unwrap();
        let mut reader = DmaStreamReaderBuilder::new(rfile)
            .with_buffer_size(1024)
            .build();

        let mut buf = [0u8; 2000];
        expect_specific_error(reader.read_exact(&mut buf).await, "Bad file descriptor (os error 9)");
        reader.close().await.unwrap();
    });

    file_stream_write_test!(write_all, path, _k, filename, file, {
        let mut writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(128 << 10)
            .build();

        writer.write_all(&[0, 1, 2, 3, 4]).await.unwrap();

        assert_eq!(writer.current_pos(), 5);
        writer.close().await.unwrap();
        assert_eq!(writer.current_pos(), 5);

        let rfile = DmaFile::open(&filename).await.unwrap();
        assert_eq!(rfile.file_size().await.unwrap(), 5);

        let mut reader = DmaStreamReaderBuilder::new(rfile)
            .with_buffer_size(1024)
            .build();

        let mut buf = Vec::new();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x, 5);
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_write_test!(write_multibuffer, path, _k, filename, file, {
        let mut writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(1024)
            .with_write_behind(2)
            .build();

        for i in 0..4096 {
            assert_eq!(writer.current_pos(), i);
            writer.write_all(&[i as u8]).await.unwrap();
        }

        writer.close().await.unwrap();

        let rfile = DmaFile::open(&filename).await.unwrap();
        assert_eq!(rfile.file_size().await.unwrap(), 4096);

        let mut reader = DmaStreamReaderBuilder::new(rfile)
            .with_buffer_size(1024)
            .build();

        let mut buf = [0u8; 4096];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_write_test!(write_close_twice, path, _k, filename, file, {
        let mut writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(128 << 10)
            .build();

        writer.close().await.unwrap();
        expect_specific_error(writer.close().await, "Bad file descriptor (os error 9)");
    });

    file_stream_write_test!(write_no_write_behind, path, _k, filename, file, {
        let mut writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(1024)
            .with_write_behind(0)
            .build();

        for i in 0..4096 {
            assert_eq!(writer.current_pos(), i);
            writer.write_all(&[i as u8]).await.unwrap();
        }

        writer.close().await.unwrap();

        let rfile = DmaFile::open(&filename).await.unwrap();
        assert_eq!(rfile.file_size().await.unwrap(), 4096);

        let mut reader = DmaStreamReaderBuilder::new(rfile)
            .with_buffer_size(1024)
            .build();

        let mut buf = [0u8; 4096];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    file_stream_write_test!(write_zero_buffer, path, _k, filename, file, {
        let mut writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(0)
            .build();

        for i in 0..4096 {
            assert_eq!(writer.current_pos(), i);
            writer.write_all(&[i as u8]).await.unwrap();
        }

        writer.close().await.unwrap();

        let rfile = DmaFile::open(&filename).await.unwrap();
        assert_eq!(rfile.file_size().await.unwrap(), 4096);

        let mut reader = DmaStreamReaderBuilder::new(rfile)
            .with_buffer_size(1024)
            .build();

        let mut buf = [0u8; 4096];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    // Unfortunately we don't record the file type so we won't know if it is
    // writeable or not until we actually try. In this test we'll try on close,
    // when we force a flush
    file_stream_write_test!(write_with_readable_file, path, _k, filename, _file, {
        let rfile = DmaFile::open(&filename).await.unwrap();

        let mut writer = DmaStreamWriterBuilder::new(rfile)
            .with_buffer_size(4096)
            .build();

        for i in 0..1024 {
            assert_eq!(writer.current_pos(), i);
            writer.write_all(&[i as u8]).await.unwrap();
        }

        expect_specific_error(writer.close().await, "Bad file descriptor (os error 9)");
    });

    file_stream_write_test!(flushed_position_small_buffer, path, _k, filename, file, {
        let mut writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(4096)
            .build();

        assert_eq!(writer.current_pos(), 0);
        writer.write_all(&[0, 1, 2, 3, 4]).await.unwrap();
        assert_eq!(writer.current_pos(), 5);
        // The write above is not enough to cause a flush
        assert_eq!(writer.current_flushed_pos(), 0);
        writer.close().await.unwrap();
        // Close implies a forced-flush and a sync.
        assert_eq!(writer.current_flushed_pos(), 5);
    });

    file_stream_write_test!(flushed_position_big_buffer, path, _k, filename, file, {
        let mut writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(4096)
            .with_write_behind(2)
            .build();

        assert_eq!(writer.current_pos(), 0);
        let buffer = [0u8; 5000];
        writer.write_all(&buffer).await.unwrap();

        assert_eq!(writer.current_flushed_pos(), 0);
        Timer::new(Duration::from_secs(1)).await;
        assert_eq!(writer.current_flushed_pos(), 4096);

        writer.close().await.unwrap();
        assert_eq!(writer.current_flushed_pos(), 5000);
    });

    file_stream_write_test!(sync_and_close, path, _k, filename, file, {
        let mut writer = DmaStreamWriterBuilder::new(file)
            .with_buffer_size(4096)
            .with_write_behind(2)
            .build();

        assert_eq!(writer.current_pos(), 0);
        let buffer = [0u8; 5000];
        writer.write_all(&buffer).await.unwrap();
        assert_eq!(writer.sync().await.unwrap(), 4096);
        // write more
        writer.write_all(&buffer).await.unwrap();
        writer.close().await.unwrap();

        assert_eq!(writer.current_flushed_pos(), 10000);
    });
}
