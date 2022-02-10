// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.

use crate::{
    io::{BufferedFile, ScheduledSource},
    reactor::Reactor,
    sys::{IoBuffer, Source},
};
use futures_lite::{
    io::{AsyncBufRead, AsyncRead, AsyncSeek, AsyncWrite, SeekFrom},
    ready,
};
use pin_project_lite::pin_project;
use std::{
    convert::TryInto,
    io,
    os::unix::io::AsRawFd,
    pin::Pin,
    rc::{Rc, Weak},
    task::{Context, Poll, Waker},
};

type Result<T> = crate::Result<T, ()>;

pin_project! {
    #[derive(Debug)]
    /// Provides linear read access to a [`BufferedFile`].
    ///
    /// The [`StreamReader`] implements [`AsyncRead`] and [`AsyncBufRead`], which can offer the user with a convenient
    /// way of issuing reads. However, note that this mandates a copy between the OS page cache and
    /// an intermediary buffer before it reaches the user-specified buffer
    ///
    /// [`BufferedFile`]: struct.BufferedFile.html
    /// [`StreamReader`]: struct.StreamReader.html
    /// [`AsyncRead`]: https://docs.rs/futures/0.3.6/futures/io/trait.AsyncRead.html
    /// [`AsyncBufRead`]: https://docs.rs/futures/0.3.6/futures/io/trait.AsyncBufRead.html
    pub struct StreamReader {
        file: BufferedFile,
        file_pos: u64,
        max_pos: u64,
        io_source: Option<ScheduledSource>,
        seek_source: Option<Source>,
        buffer: Buffer,
        reactor: Weak<Reactor>,
    }
}

pin_project! {
    #[derive(Debug)]
    /// Standard input accessor. This implements [`AsyncRead`] and [`AsyncBufRead`]
    ///
    /// [`AsyncRead`]: https://docs.rs/futures/0.3.6/futures/io/trait.AsyncRead.html
    /// [`AsyncBufRead`]: https://docs.rs/futures/0.3.6/futures/io/trait.AsyncBufRead.html
    pub struct Stdin {
        source: Option<ScheduledSource>,
        buffer: Buffer,
        reactor: Weak<Reactor>,
    }
}

/// Allows asynchronous read access to the standard input
///
/// # Examples
///
/// ```no_run
/// use futures_lite::AsyncBufReadExt;
/// use glommio::{io::stdin, LocalExecutor};
///
/// let ex = LocalExecutor::default();
/// ex.run(async {
///     let mut sin = stdin();
///     loop {
///         let mut buf = String::new();
///         sin.read_line(&mut buf).await.unwrap();
///         println!("you just typed {}", buf);
///     }
/// });
/// ```
pub fn stdin() -> Stdin {
    Stdin {
        source: None,
        buffer: Buffer::new(128),
        reactor: Rc::downgrade(&crate::executor().reactor()),
    }
}

#[derive(Debug)]
enum FileStatus {
    Open,    // Open, can be closed.
    Closing, // We are closing it.
    Closed,  // It was closed, but the poll
}

#[derive(Debug)]
/// Provides linear write access to a [`BufferedFile`].
///
/// The [`StreamWriter`] implements [`AsyncWrite`]
///
/// [`BufferedFile`]: struct.BufferedFile.html
/// [`StreamWriter`]: struct.StreamWriter.html
/// [`AsyncWrite`]: https://docs.rs/futures/0.3.6/futures/io/trait.AsyncWrite.html
pub struct StreamWriter {
    file: Option<BufferedFile>,
    file_pos: u64,
    sync_on_close: bool,
    source: Option<Source>,
    buffer: Buffer,
    file_status: FileStatus,
    reactor: Weak<Reactor>,
}

#[derive(Debug)]
/// Builds a [`StreamReader`], allowing linear read access to a [`BufferedFile`]
///
/// [`BufferedFile`]: struct.BufferedFile.html
/// [`StreamReader`]: struct.StreamReader.html
pub struct StreamReaderBuilder {
    start: u64,
    end: u64,
    buffer_size: usize,
    file: BufferedFile,
}

#[derive(Debug)]
/// Builds a [`StreamWriter`], allowing linear write access to a
/// [`BufferedFile`]
///
/// [`BufferedFile`]: struct.BufferedFile.html
/// [`StreamWriter`]: struct.StreamWriter.html
pub struct StreamWriterBuilder {
    buffer_size: usize,
    sync_on_close: bool,
    file: BufferedFile,
}

#[derive(Debug)]
struct Buffer {
    max_buffer_size: usize,
    buffer_pos: usize,
    data: Vec<u8>,
}

// We can use the same implementation for reads and writes but need to
// be careful: For writes, buffer_pos is how much we have written, so
// we are always interested in returning 0 -> buffer_pos.
//
// For reads, buffer_pos means how much we have consumed, so we are
// interested in returning buffer_pos -> max_size.
//
// To avoid confusion we'll use consumed_bytes() for writes and
// unconsumed_bytes(). The as_bytes() variant, closer to what one would
// expect from the standard library give raw access to the entire buffer and
// are mostly used for filling the buffer
impl Buffer {
    fn new(max_buffer_size: usize) -> Buffer {
        Buffer {
            buffer_pos: 0,
            data: Vec::with_capacity(max_buffer_size),
            max_buffer_size,
        }
    }

    fn replace_buffer(&mut self, buf: Vec<u8>) {
        self.buffer_pos = 0;
        self.data = buf;
    }

    fn replenish_buffer(&mut self, buf: &IoBuffer, len: usize) {
        use crate::ByteSliceExt;
        self.buffer_pos = 0;
        self.data.resize(len, 0u8);
        buf[..len].read_at(0, &mut self.data);
    }

    fn remaining_unconsumed_bytes(&self) -> usize {
        self.data.len() - self.buffer_pos
    }

    fn consume(&mut self, amt: usize) {
        self.buffer_pos += amt;
    }

    // copies as many bytes as possible from buf to ourselves, return how many bytes
    // we copied.
    fn copy_from_buffer(&mut self, buf: &[u8]) -> usize {
        let max_size = self.max_buffer_size;
        let copy_size = std::cmp::min(max_size - self.data.len(), buf.len());
        self.data.extend_from_slice(&buf[0..copy_size]);
        self.buffer_pos += copy_size;
        copy_size
    }

    fn consumed_bytes(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.data)
    }

    fn unconsumed_bytes(&self) -> &[u8] {
        &self.data[self.buffer_pos..]
    }
}

impl StreamReader {
    /// Asynchronously closes the underlying file.
    pub async fn close(self) -> Result<()> {
        self.file.close().await
    }

    fn new(builder: StreamReaderBuilder) -> StreamReader {
        StreamReader {
            file: builder.file,
            file_pos: builder.start,
            max_pos: builder.end,
            io_source: None,
            seek_source: None,
            buffer: Buffer::new(builder.buffer_size),
            reactor: Rc::downgrade(&crate::executor().reactor()),
        }
    }
}

impl StreamReaderBuilder {
    /// Creates a new StreamReaderBuilder, given a [`BufferedFile`]
    ///
    /// Various properties can be set by using its `with` methods.
    ///
    /// A [`StreamReader`] can later be constructed from it by
    /// calling [`build`]
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{
    ///     io::{BufferedFile, StreamReaderBuilder},
    ///     LocalExecutor,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = BufferedFile::open("myfile.txt").await.unwrap();
    ///     let _reader = StreamReaderBuilder::new(file).build();
    /// });
    /// ```
    /// [`BufferedFile`]: struct.BufferedFile.html
    /// [`StreamReader`]: struct.StreamReader.html
    /// [`build`]: #method.build
    #[must_use = "The builder must be built to be useful"]
    pub fn new(file: BufferedFile) -> StreamReaderBuilder {
        StreamReaderBuilder {
            start: 0,
            end: u64::MAX,
            buffer_size: 4 << 10,
            file,
        }
    }

    /// Define a starting position.
    ///
    /// Reads from the [`StreamReader`] will start from this position
    ///
    /// [`StreamReader`]: struct.StreamReader.html
    #[must_use = "The builder must be built to be useful"]
    pub fn with_start_pos(mut self, start: u64) -> Self {
        self.start = start;
        self
    }

    /// Define an end position.
    ///
    /// Reads from the [`StreamReader`] will end at this position even if the
    /// file is larger.
    ///
    /// [`StreamReader`]: struct.StreamReader.html
    #[must_use = "The builder must be built to be useful"]
    pub fn with_end_pos(mut self, end: u64) -> Self {
        self.end = end;
        self
    }

    /// Define the buffer size that will be used by the [`StreamReader`]
    ///
    /// [`StreamReader`]: struct.StreamReader.html
    #[must_use = "The builder must be built to be useful"]
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = std::cmp::max(buffer_size, 1);
        self
    }

    /// Builds a [`StreamReader`] with the properties defined by this
    /// [`StreamReaderBuilder`]
    ///
    /// [`StreamReader`]: struct.StreamReader.html
    /// [`StreamReaderBuilder`]: struct.StreamReaderBuilder.html
    pub fn build(self) -> StreamReader {
        StreamReader::new(self)
    }
}

impl StreamWriterBuilder {
    /// Creates a new StreamWriterBuilder, given a [`BufferedFile`]
    ///
    /// Various properties can be set by using its `with` methods.
    ///
    /// A [`StreamWriter`] can later be constructed from it by
    /// calling [`build`]
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use glommio::{
    ///     io::{BufferedFile, StreamWriterBuilder},
    ///     LocalExecutor,
    /// };
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async {
    ///     let file = BufferedFile::create("myfile.txt").await.unwrap();
    ///     let _reader = StreamWriterBuilder::new(file).build();
    /// });
    /// ```
    /// [`BufferedFile`]: struct.BufferedFile.html
    /// [`StreamWriter`]: struct.StreamWriter.html
    /// [`build`]: #method.build
    #[must_use = "The builder must be built to be useful"]
    pub fn new(file: BufferedFile) -> StreamWriterBuilder {
        StreamWriterBuilder {
            buffer_size: 128 << 10,
            sync_on_close: true,
            file,
        }
    }

    /// Chooses whether to issue a sync operation when closing the file
    /// (default enabled). Disabling this is dangerous and in most cases may
    /// lead to data loss upon power failure.
    #[must_use = "The builder must be built to be useful"]
    pub fn with_sync_on_close_disabled(mut self, flush_disabled: bool) -> Self {
        self.sync_on_close = !flush_disabled;
        self
    }

    /// Define the buffer size that will be used by the [`StreamWriter`]
    ///
    /// [`StreamWriter`]: struct.StreamWriter.html
    #[must_use = "The builder must be built to be useful"]
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = std::cmp::max(buffer_size, 1);
        self
    }

    /// Builds a [`StreamWriter`] with the properties defined by this
    /// [`StreamWriterBuilder`]
    ///
    /// [`StreamWriter`]: struct.StreamWriter.html
    /// [`StreamWriterBuilder`]: struct.StreamWriterBuilder.html
    pub fn build(self) -> StreamWriter {
        StreamWriter::new(self)
    }
}

impl StreamWriter {
    fn new(builder: StreamWriterBuilder) -> StreamWriter {
        StreamWriter {
            file: Some(builder.file),
            file_status: FileStatus::Open,
            sync_on_close: builder.sync_on_close,
            file_pos: 0,
            source: None,
            buffer: Buffer::new(builder.buffer_size),
            reactor: Rc::downgrade(&crate::executor().reactor()),
        }
    }

    fn consume_flush_result(&mut self, source: Source) -> io::Result<()> {
        let res = source.result().unwrap();
        if res.is_ok() {
            if let IoBuffer::Buffered(mut buffer) = source.extract_buffer() {
                self.file_pos += buffer.len() as u64;
                buffer.truncate(0);
                self.buffer.replace_buffer(buffer);
            } else {
                unreachable!("expected vec buffer");
            }
        }
        res.map(|_x| ())
    }

    fn flush_write_buffer(&mut self, waker: &Waker) -> bool {
        assert!(self.source.is_none());
        let bytes = self.buffer.consumed_bytes();
        if !bytes.is_empty() {
            let source = self.reactor.upgrade().unwrap().write_buffered(
                self.file.as_ref().unwrap().as_raw_fd(),
                bytes,
                self.file_pos,
            );
            source.add_waiter_single(waker);
            self.source = Some(source);
            true
        } else {
            false
        }
    }

    fn poll_sync(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.sync_on_close {
            Poll::Ready(Ok(()))
        } else {
            match self.source.take() {
                None => {
                    let source = self
                        .reactor
                        .upgrade()
                        .unwrap()
                        .fdatasync(self.file.as_ref().unwrap().as_raw_fd());
                    source.add_waiter_single(cx.waker());
                    self.source = Some(source);
                    Poll::Pending
                }
                Some(source) => {
                    let _ = source.result().unwrap();
                    Poll::Ready(Ok(()))
                }
            }
        }
    }

    fn poll_inner_close(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.source.take() {
            None => {
                let source = self
                    .reactor
                    .upgrade()
                    .unwrap()
                    .close(self.file.as_ref().unwrap().as_raw_fd());
                source.add_waiter_single(cx.waker());
                self.source = Some(source);
                Poll::Pending
            }
            Some(source) => {
                let _ = source.result().unwrap();
                self.file.take().unwrap().discard();
                Poll::Ready(Ok(()))
            }
        }
    }

    fn do_poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.source.take() {
            None => match self.flush_write_buffer(cx.waker()) {
                true => Poll::Pending,
                false => Poll::Ready(Ok(())),
            },
            Some(source) => Poll::Ready(self.consume_flush_result(source)),
        }
    }

    fn do_poll_close(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.file.is_some() {
            match self.file_status {
                FileStatus::Open => {
                    let res = ready!(self.do_poll_flush(cx));
                    if res.is_err() {
                        return Poll::Ready(res);
                    }
                    self.file_status = FileStatus::Closing;
                    continue;
                }
                FileStatus::Closing => {
                    let res = ready!(self.poll_sync(cx));
                    if res.is_err() {
                        return Poll::Ready(res);
                    }
                    self.file_status = FileStatus::Closed;
                    continue;
                }
                FileStatus::Closed => {
                    return self.poll_inner_close(cx);
                }
            }
        }
        Poll::Ready(Ok(()))
    }
}

macro_rules! do_seek {
    ( $self:expr, $source:expr, $fileobj:expr, $cx:expr, $pos:expr ) => {
        match $pos {
            SeekFrom::Start(pos) => {
                $self.file_pos = pos;
                Poll::Ready(Ok(pos))
            }
            SeekFrom::Current(pos) => {
                $self.file_pos = ($self.file_pos as i64 + pos) as u64;
                Poll::Ready(Ok($self.file_pos))
            }
            SeekFrom::End(pos) => match $source.take() {
                None => {
                    let source = $self
                        .reactor
                        .upgrade()
                        .unwrap()
                        .statx($fileobj.as_raw_fd(), &$fileobj.path().unwrap());
                    source.add_waiter_single($cx.waker());
                    $source = Some(source);
                    Poll::Pending
                }
                Some(source) => {
                    let stype = source.extract_source_type();
                    let stat_buf: libc::statx = stype.try_into().unwrap();
                    let end = stat_buf.stx_size as i64;
                    $self.file_pos = (end + pos) as u64;
                    Poll::Ready(Ok($self.file_pos))
                }
            },
        }
    };
}

impl AsyncSeek for StreamReader {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        do_seek!(self, self.seek_source, &self.file, cx, pos)
    }
}

impl AsyncSeek for StreamWriter {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        do_seek!(self, self.source, self.file.as_ref().unwrap(), cx, pos)
    }
}

impl AsyncRead for StreamReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        // This is by far the most annoying thing about this interface.
        // read_exact works well if we use the user-provided buffer directly,
        // but read_to_end resets the buffer between calls.
        let buffer = ready!(self.as_mut().poll_fill_buf(cx))?;
        let bytes_read = std::cmp::min(buffer.len(), buf.len());
        buf[0..bytes_read].copy_from_slice(&buffer[0..bytes_read]);
        self.consume(bytes_read);
        Poll::Ready(Ok(bytes_read))
    }
}

impl AsyncBufRead for StreamReader {
    fn poll_fill_buf<'a>(
        mut self: Pin<&'a mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<&'a [u8]>> {
        match self.io_source.take() {
            Some(source) => {
                let res = source.result().unwrap();
                match res {
                    Err(x) => Poll::Ready(Err(x)),
                    Ok(sz) => {
                        let old_pos = self.file_pos;
                        let new_pos = std::cmp::min(old_pos + sz as u64, self.max_pos);
                        let added_size = new_pos - old_pos;
                        self.file_pos += added_size;
                        self.buffer
                            .replenish_buffer(&source.buffer(), added_size as usize);
                        let this = self.project();
                        Poll::Ready(Ok(this.buffer.unconsumed_bytes()))
                    }
                }
            }
            None => {
                if self.buffer.remaining_unconsumed_bytes() > 0 {
                    let this = self.project();
                    Poll::Ready(Ok(this.buffer.unconsumed_bytes()))
                } else {
                    let file_pos = self.file_pos;
                    let fd = self.file.as_raw_fd();
                    let source = self.reactor.upgrade().unwrap().read_buffered(
                        fd,
                        file_pos,
                        self.buffer.max_buffer_size,
                        self.file.file.scheduler.borrow().as_ref(),
                    );
                    source.add_waiter_single(cx.waker());
                    self.io_source = Some(source);
                    Poll::Pending
                }
            }
        }
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.buffer.consume(amt);
    }
}

impl AsyncWrite for StreamWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(source) = self.source.take() {
            if let Err(x) = self.consume_flush_result(source) {
                return Poll::Ready(Err(x));
            }
        }

        if !self.buffer.data.is_empty() {
            let x = self.flush_write_buffer(cx.waker());
            assert!(x);
            Poll::Pending
        } else {
            Poll::Ready(Ok(self.buffer.copy_from_buffer(buf)))
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.do_poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.do_poll_close(cx)
    }
}

impl AsyncRead for Stdin {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let buffer = ready!(self.as_mut().poll_fill_buf(cx))?;
        let bytes_read = std::cmp::min(buffer.len(), buf.len());
        buf[0..bytes_read].copy_from_slice(&buffer[0..bytes_read]);
        self.consume(bytes_read);
        Poll::Ready(Ok(bytes_read))
    }
}

impl AsyncBufRead for Stdin {
    fn poll_fill_buf<'a>(
        mut self: Pin<&'a mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<&'a [u8]>> {
        match self.source.take() {
            Some(source) => {
                let res = source.result().unwrap();
                match res {
                    Err(x) => Poll::Ready(Err(x)),
                    Ok(sz) => {
                        self.buffer.replenish_buffer(&source.buffer(), sz);
                        let this = self.project();
                        Poll::Ready(Ok(this.buffer.unconsumed_bytes()))
                    }
                }
            }
            None => {
                if self.buffer.remaining_unconsumed_bytes() > 0 {
                    let this = self.project();
                    Poll::Ready(Ok(this.buffer.unconsumed_bytes()))
                } else {
                    let source = self.reactor.upgrade().unwrap().read_buffered(
                        libc::STDIN_FILENO,
                        0,
                        self.buffer.max_buffer_size,
                        None,
                    );
                    source.add_waiter_single(cx.waker());
                    self.source = Some(source);
                    Poll::Pending
                }
            }
        }
    }

    fn consume(mut self: Pin<&mut Self>, amt: usize) {
        self.buffer.consume(amt);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_utils::make_test_directories;
    use futures_lite::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, StreamExt};
    use std::io::ErrorKind;

    macro_rules! read_test {
        ( $name:ident, $dir:ident, $kind:ident, $file:ident, $file_size:ident: $size:tt, $code:block) => {
            #[test]
            fn $name() {
                for dir in make_test_directories(stringify!($name)) {
                    let $dir = dir.path.clone();
                    let $kind = dir.kind;
                    test_executor!(async move {
                        let filename = $dir.join("testfile");
                        let new_file = BufferedFile::create(&filename)
                            .await
                            .expect("failed to create file");
                        if $size > 0 {
                            let mut buf = Vec::new();
                            #[allow(clippy::reversed_empty_ranges)]
                            for v in 0..$size {
                                buf.push(v as u8);
                            }
                            new_file.write_at(buf, 0).await.unwrap();
                        }
                        new_file.close().await.unwrap();
                        let $file = BufferedFile::open(&filename).await.unwrap();
                        let $file_size = $size;
                        $code
                    });
                }
            }
        };
    }

    macro_rules! write_test {
        ( $name:ident, $dir:ident, $kind:ident, $file:ident, $code:block) => {
            #[test]
            fn $name() {
                for dir in make_test_directories(stringify!($name)) {
                    let $dir = dir.path.clone();
                    let $kind = dir.kind;
                    test_executor!(async move {
                        let filename = $dir.join("testfile");
                        let $file = BufferedFile::create(&filename).await.unwrap();
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

    read_test!(read_exact_empty_file, path, _k, file, _file_size: 0, {
        let mut reader = StreamReaderBuilder::new(file).build();


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

    read_test!(read_exact_part_of_file, path, _k, file, _file_size: 4096, {
        let mut reader = StreamReaderBuilder::new(file).build();

        let mut buf = [0u8; 128];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);

        reader.close().await.unwrap();
    });

    read_test!(seek_start_and_read_exact, path, _k, file, _file_size: 4096, {
        let mut reader = StreamReaderBuilder::new(file).build();

        let mut buf = [0u8; 128];
        reader.seek(SeekFrom::Start(10)).await.unwrap();
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 10);

        reader.close().await.unwrap();
    });

    read_test!(seek_end_and_read_exact, path, _k, file, _file_size: 4096, {
        let mut reader = StreamReaderBuilder::new(file).build();

        let mut buf = [0u8; 96];
        reader.seek(SeekFrom::End(-96)).await.unwrap();
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 4000);

        reader.close().await.unwrap();
    });

    read_test!(read_to_end_from_start, path, _k, file, file_size: 4096, {
        let mut reader = StreamReaderBuilder::new(file).build();

        let mut buf = Vec::new();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x, file_size);
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    read_test!(read_slice, path, _k, file, _file_size: 4096, {
        let mut reader = StreamReaderBuilder::new(file)
            .with_start_pos(2)
            .with_end_pos(12)
            .build();

        let mut buf = Vec::new();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x, 10);
        check_contents!(buf, 2);
        reader.close().await.unwrap();
    });

    read_test!(read_to_end_after_seek, path, _k, file, _file_size: 4096, {
        let mut reader = StreamReaderBuilder::new(file).build();

        let mut buf = Vec::new();
        reader.seek(SeekFrom::Start(4000)).await.unwrap();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x, 96);
        check_contents!(buf, 4000);
        reader.close().await.unwrap();
    });

    read_test!(read_until_empty_file, path, _k, file, _file_size: 0, {
        let mut reader = StreamReaderBuilder::new(file).build();

        let mut buf = Vec::new();
        let x = reader.read_until(0xA, &mut buf).await.unwrap();
        assert_eq!(x, 0); // remember the extra 0
        reader.close().await.unwrap();
    });

    read_test!(read_until, path, _k, file, _file_size: 8192, {
        let mut reader = StreamReaderBuilder::new(file).build();

        let mut buf = Vec::new();
        let x = reader.read_until(0xA, &mut buf).await.unwrap();
        assert_eq!(x, 0xB); // remember the extra 0
        // test that the file pos is updated
        for _ in 0..16 {
            let x = reader.read_until(0xA, &mut buf).await.unwrap();
            assert_eq!(x, 256); // the extra 0
        }
        reader.close().await.unwrap();
    });

    read_test!(read_until_eof, path, _k, file, file_size: 32, {
        let mut reader = StreamReaderBuilder::new(file).build();

        let mut buf = Vec::new();
        let x = reader.read_until(255, &mut buf).await.unwrap();
        assert_eq!(x, file_size);
        reader.close().await.unwrap();
    });

    read_test!(read_line, path, _k, file, _file_size: 8192, {
        let mut reader = StreamReaderBuilder::new(file).build();

        let mut buf = String::new();
        let x = reader.read_line(&mut buf).await.unwrap();
        assert_eq!(x, 0xB); // remember the extra 0
        reader.close().await.unwrap();
    });

    write_test!(lines, path, _k, file, {
        let mut writer = StreamWriterBuilder::new(file).build();

        writer.write_all(b"123\n123\n123\n").await.unwrap();
        writer.close().await.unwrap();

        let filename = path.join("testfile");
        let file = BufferedFile::open(&filename).await.unwrap();
        let reader = StreamReaderBuilder::new(file).build();

        let mut lines = reader.lines();

        let mut found = 0;
        while let Some(line) = lines.next().await {
            assert_eq!(line.unwrap(), "123");
            found += 1;
        }
        assert_eq!(found, 3)
    });

    read_test!(split, path, _k, file, file_size: 4096, {
        let reader = StreamReaderBuilder::new(file).build();

        let split : Vec<Vec<u8>> = reader.split(255).try_collect().await.unwrap();
        assert_eq!(split.len(), file_size / 256);
        for line in split.iter() {
            assert_eq!(line.len(), 255);
        }
    });

    read_test!(mix_and_match_apis, path, _k, file, _file_size: 4096, {
        let mut reader = StreamReaderBuilder::new(file).build();

        let mut buf = Vec::new();
        let x = reader.read_until(0xA, &mut buf).await.unwrap();
        assert_eq!(x, 0xB);
        let mut buf = [0u8; 10];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0xB);
        reader.close().await.unwrap();
    });

    write_test!(write_simple, path, _k, file, {
        let mut writer = StreamWriterBuilder::new(file).build();

        writer.write_all(&[0, 1, 2, 3, 4]).await.unwrap();
        writer.close().await.unwrap();
        let filename = path.join("testfile");
        let file = BufferedFile::open(&filename).await.unwrap();
        assert_eq!(file.file_size().await.unwrap(), 5);
        file.close().await.unwrap();
    });

    write_test!(seek_and_write, path, _k, file, {
        let mut writer = StreamWriterBuilder::new(file).build();

        writer.seek(SeekFrom::Start(10)).await.unwrap();
        writer.write_all(&[0, 1, 2, 3, 4]).await.unwrap();
        writer.close().await.unwrap();
        let filename = path.join("testfile");
        let file = BufferedFile::open(&filename).await.unwrap();
        assert_eq!(file.file_size().await.unwrap(), 15);

        let mut reader = StreamReaderBuilder::new(file).build();
        let mut buf = [0u8; 15];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(&buf[10..], 0);
        for i in buf[..10].iter() {
            assert_eq!(*i, 0);
        }
        reader.close().await.unwrap();
    });

    write_test!(nop_seek_write, path, _k, file, {
        let mut writer = StreamWriterBuilder::new(file).build();

        writer.write_all(&[0, 1, 2, 3, 4]).await.unwrap();
        writer.seek(SeekFrom::End(0)).await.unwrap();
        writer.write_all(&[5, 6, 7, 8, 9]).await.unwrap();
        writer.close().await.unwrap();

        let filename = path.join("testfile");
        let file = BufferedFile::open(&filename).await.unwrap();
        assert_eq!(file.file_size().await.unwrap(), 10);

        let mut reader = StreamReaderBuilder::new(file).build();
        let mut buf = [0u8; 10];
        reader.read_exact(&mut buf).await.unwrap();
        check_contents!(buf, 0);
        reader.close().await.unwrap();
    });

    write_test!(write_and_flush, path, _k, file, {
        let mut writer = StreamWriterBuilder::new(file).build();

        writer.write_all(&[0, 1, 2, 3, 4]).await.unwrap();
        // tests that flush is enough to send the data to the underlying file
        writer.flush().await.unwrap();
        let filename = path.join("testfile");
        let file = BufferedFile::open(&filename).await.unwrap();
        let mut reader = StreamReaderBuilder::new(file).build();

        let mut buf = Vec::new();
        let x = reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(x, 5);
        check_contents!(buf, 0);

        reader.close().await.unwrap();
        writer.close().await.unwrap();
    });
}
