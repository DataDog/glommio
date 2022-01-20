// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::io::{
    bulk_io::{BulkIo, MergedBufferLimit, ReadAmplificationLimit, ReadManyArgs},
    open_options::OpenOptions,
    DmaStreamReaderBuilder,
    DmaStreamWriter,
    DmaStreamWriterBuilder,
    IoVec,
    ReadManyResult,
    ReadResult,
};
use futures_lite::{future::poll_fn, io::AsyncWrite, Stream};
use std::{
    cell::Ref,
    io,
    path::Path,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
};

type Result<T> = crate::Result<T, ()>;

#[derive(Debug)]
/// Builds a new [`ImmutableFile`], allowing linear and random access to a
/// Direct I/O [`DmaFile`].
///
/// Working with an [`ImmutableFile`] happens in two steps:
///
/// * First, all the writes are done, through the [`ImmutableFilePreSealSink`],
///   which has no read methods.
/// * Then, after the sink is [`seal`]ed, it is no longer possible to write to
///   it
///
/// [`DmaFile`]: struct.DmaFile.html
/// [`ImmutableFilePreSealSink`]: ImmutableFilePreSealSink
/// [`seal`]: ImmutableFilePreSealSink::seal
pub struct ImmutableFileBuilder<P>
where
    P: AsRef<Path>,
{
    concurrency: usize,
    buffer_size: usize,
    flush_disabled: bool,
    pre_allocate: Option<u64>,
    hint_extent_size: Option<usize>,
    path: P,
}

#[derive(Debug)]
/// Sink portion of the [`ImmutableFile`]
///
/// To build it, use [`build_sink`] on the [`ImmutableFileBuilder`]
///
/// [`build_sink`]: ImmutableFileBuilder::build_sink
/// [`ImmutableFileBuilder`]: ImmutableFileBuilder
pub struct ImmutableFilePreSealSink {
    writer: DmaStreamWriter,
}

#[derive(Debug, Clone)]
/// A Direct I/O enabled file abstraction that can not be written to.
///
/// Glommio cannot guarantee that the file is not modified outside the process.
/// But so long as the file is only used within Glommio, using this API
/// guarantees that the file will never change.
///
/// This allows us to employ optimizations like caching and request coalescing.
/// The behavior of this API upon external modifications is undefined.
///
/// It can be read sequentially by building a stream with [`stream_reader`], or
/// randomly through the [`read_at`] (single read) or [`read_many`] (multiple
/// reads) APIs.
///
/// To build an `ImmutableFile`, use [`ImmutableFileBuilder`].
///
/// [`ImmutableFileBuilder`]: ImmutableFileBuilder
/// [`stream_reader`]: ImmutableFile::stream_reader
/// [`read_at`]: ImmutableFile::read_at
/// [`read_many`]: ImmutableFile::read_many
pub struct ImmutableFile {
    stream_builder: DmaStreamReaderBuilder,
    size: u64,
}

impl<P> ImmutableFileBuilder<P>
where
    P: AsRef<Path>,
{
    /// Creates a new [`ImmutableFileBuilder`].
    ///
    /// If this is a new file, use [`build_sink`] to build the
    /// [`ImmutableFilePreSealSink`] stage that will allow writes until
    /// [`seal`]. In that case, the filename must not exist. However, no
    /// error happens until [`build_sink`] is attempted.
    ///
    /// If this is an existing file that will be exclusively read, use
    /// [`build_existing`] to create an [`ImmutableFile`] directly, skipping
    /// the [`ImmutableFilePreSealSink`] stage
    ///
    /// [`ImmutableFile`]: ImmutableFile
    /// [`ImmutableFilePreSealSink`]: ImmutableFilePreSealSink
    /// [`seal`]: ImmutableFilePreSealSink::seal
    /// [`build_sink`]: ImmutableFileBuilder::build_sink
    /// [`build_existing`]: ImmutableFileBuilder::build_existing
    #[must_use = "The builder must be built to be useful"]
    pub fn new(fname: P) -> Self {
        Self {
            path: fname,
            buffer_size: 128 << 10,
            concurrency: 10,
            flush_disabled: false,
            pre_allocate: None,
            hint_extent_size: None,
        }
    }

    /// Define the number of buffers that will be used by the
    /// [`ImmutableFile`] during sequential access.
    ///
    /// This number is upheld for both the sink and read phases of the
    /// [`ImmutableFile`]. Higher numbers mean more concurrency but also
    /// more memory usage.
    ///
    /// [`ImmutableFile`]: ImmutableFile
    #[must_use = "The builder must be built to be useful"]
    pub fn with_sequential_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Does not issue a sync operation when sealing the file. This is dangerous
    /// and in most cases may lead to data loss.
    ///
    /// Please note that even for `O_DIRECT` files, data may still be present in
    /// your device's internal cache until a sync happens.
    ///
    /// This option is ignored if you are creating an [`ImmutableFile`] directly
    /// and skipping the [`ImmutableFilePreSealSink`] step
    ///
    /// [`ImmutableFile`]: ImmutableFile
    /// [`ImmutableFilePreSealSink`]: ImmutableFilePreSealSink
    #[must_use = "The builder must be built to be useful"]
    pub fn with_sync_on_close_disabled(mut self, flush_disabled: bool) -> Self {
        self.flush_disabled = flush_disabled;
        self
    }

    /// Define the buffer size that will be used by the sequential operations on
    /// this [`ImmutableFile`]
    ///
    /// [`ImmutableFile`]: ImmutableFile
    #[must_use = "The builder must be built to be useful"]
    pub fn with_buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    /// pre-allocates space in the filesystem to hold a file at least as big as
    /// the size argument.
    #[must_use = "The builder must be built to be useful"]
    pub fn with_pre_allocation(mut self, size: Option<u64>) -> Self {
        self.pre_allocate = size;
        self
    }

    /// Hint to the OS the size of increase of this file, to allow more
    /// efficient allocation of blocks.
    ///
    /// Allocating blocks at the filesystem level turns asynchronous writes into
    /// threaded synchronous writes, as we need to first find the blocks to
    /// host the file.
    ///
    /// If the extent is larger, that means many blocks are allocated at a time.
    /// For instance, if the extent size is 1 MiB, that means that only 1 out
    /// of 4 256 KiB writes will be turned synchronous. Combined with diligent
    /// use of `fallocate` we can greatly minimize context switches.
    ///
    /// It is important not to set the extent size too big. Writes can fail
    /// otherwise if the extent can't be allocated.
    #[must_use = "The builder must be built to be useful"]
    pub fn with_hint_extent_size(mut self, size: Option<usize>) -> Self {
        self.hint_extent_size = size;
        self
    }

    /// Builds an [`ImmutableFilePreSealSink`] with the properties defined by
    /// this builder.
    ///
    /// The resulting sink can be written to until [`seal`] is called.
    ///
    /// [`ImmutableFile`]: ImmutableFilePreSealSink
    /// [`new`]: ImmutableFileBuilder::new
    /// [`seal`]: ImmutableFilePreSealSink::seal
    pub async fn build_sink(self) -> Result<ImmutableFilePreSealSink> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .dma_open(self.path)
            .await?;

        // these two syscall are hints and are allowed to fail.
        if let Some(size) = self.pre_allocate {
            let _ = file.pre_allocate(size).await;
        }
        if let Some(size) = self.hint_extent_size {
            let _ = file.hint_extent_size(size).await;
        }

        let writer = DmaStreamWriterBuilder::new(file)
            .with_sync_on_close_disabled(self.flush_disabled)
            .with_buffer_size(self.buffer_size)
            .with_write_behind(self.concurrency)
            .build();

        Ok(ImmutableFilePreSealSink { writer })
    }

    /// Builds an [`ImmutableFile`] with the properties defined by this
    /// builder.
    ///
    /// The resulting file cannot be written to. Glommio may optimize access
    /// patterns by assuming the file is read only. Writing to this file
    /// from an external process leads to undefined behavior.
    ///
    /// [`ImmutableFile`]: ImmutableFilePreSealSink
    /// [`new`]: ImmutableFileBuilder::new
    pub async fn build_existing(self) -> Result<ImmutableFile> {
        let file = Rc::new(
            OpenOptions::new()
                .read(true)
                .write(false)
                .dma_open(self.path)
                .await?,
        );
        file.attach_scheduler();
        let size = file.file_size().await?;
        let stream_builder = DmaStreamReaderBuilder::from_rc(file)
            .with_buffer_size(self.buffer_size)
            .with_read_ahead(self.concurrency);

        Ok(ImmutableFile {
            stream_builder,
            size,
        })
    }
}

impl ImmutableFilePreSealSink {
    /// Seals the file for further writes.
    ///
    /// Once this is called, it is no longer possible to write to the resulting
    /// [`ImmutableFile`]
    pub async fn seal(mut self) -> Result<ImmutableFile> {
        let stream_builder = poll_fn(|cx| self.writer.poll_seal(cx)).await?;
        stream_builder.file.attach_scheduler();

        let size = stream_builder.file.file_size().await?;
        Ok(ImmutableFile {
            stream_builder,
            size,
        })
    }

    /// Waits for all currently in-flight buffers to be written to the
    /// underlying storage.
    ///
    /// This does not include the current buffer if it is not full. If all data
    /// must be flushed, use [`flush`].
    ///
    /// Returns the flushed position of the file.
    ///
    /// [`flush`]: https://docs.rs/futures/0.3.15/futures/io/trait.AsyncWriteExt.html#method.flush
    pub async fn flush_aligned(&self) -> Result<u64> {
        self.writer.flush_aligned().await
    }

    /// Waits for all currently in-flight buffers to be written to the
    /// underlying storage, and ensures they are safely persisted.
    ///
    /// This does not include the current buffer if it is not full. If all data
    /// must be synced, use [`Self::sync`].
    ///
    /// Returns the flushed position of the file at the time the sync started.
    pub async fn sync_aligned(&self) -> Result<u64> {
        self.writer.sync_aligned().await
    }

    /// Waits for all buffers to be written to the underlying storage, and
    /// ensures they are safely persisted.
    ///
    /// This includes the current buffer even if it is not full, by padding it.
    /// The padding will get over-written by future writes, or truncated upon
    /// [`Self::seal`] or [`close`].
    ///
    /// Returns the flushed position of the file at the time the sync started.
    ///
    /// [`close`]: https://docs.rs/futures/0.3.15/futures/io/trait.AsyncWriteExt.html#method.close
    pub async fn sync(&self) -> Result<u64> {
        self.writer.sync().await
    }

    /// Acquires the current position of this [`ImmutableFilePreSealSink`].
    pub fn current_pos(&self) -> u64 {
        self.writer.current_pos()
    }

    /// Acquires the current position of this [`ImmutableFilePreSealSink`] that
    /// is flushed to the underlying media.
    ///
    /// Warning: the position reported by this API is not restart or crash safe.
    /// You need to call [`ImmutableFilePreSealSink::sync`] for that. Although
    /// the ImmutableFilePreSealSink uses Direct I/O, modern storage devices
    /// have their own caches and may still lose data that sits on those
    /// caches upon a restart until [`ImmutableFilePreSealSink::sync`] is called
    /// (Note that [`ImmutableFilePreSealSink::seal`] implies a sync).
    ///
    /// However, within the same session, new readers trying to read from any
    /// position before what we return in this method will be guaranteed to
    /// read the data we just wrote.
    pub fn current_flushed_pos(&self) -> u64 {
        self.writer.current_flushed_pos()
    }
}

impl AsyncWrite for ImmutableFilePreSealSink {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.writer).poll_close(cx)
    }
}

impl ImmutableFile {
    /// Returns an `Option` containing the path associated with this open
    /// directory, or `None` if there isn't one.
    pub fn path(&self) -> Option<Ref<'_, Path>> {
        self.stream_builder.file.path()
    }

    /// Returns the size of a file, in bytes
    pub fn file_size(&self) -> u64 {
        self.size
    }

    /// Returns true if the ['ImmutableFile'] represent the same file on the
    /// underlying device.
    ///
    /// Files are considered to be the same if they live in the same file system
    /// and have the same Linux inode. Note that based on this rule a
    /// symlink is *not* considered to be the same file.
    ///
    /// Files will be considered to be the same if:
    /// * A file is opened multiple times (different file descriptors, but same
    ///   file!)
    /// * they are hard links.
    pub fn is_same(&self, other: &ImmutableFile) -> bool {
        self.stream_builder.file.is_same(&other.stream_builder.file)
    }

    /// Reads into buffer in buf from a specific position in the file.
    ///
    /// It is not necessary to respect the `O_DIRECT` alignment of the file, and
    /// this API will internally convert the positions and sizes to match,
    /// at a cost.
    pub async fn read_at(&self, pos: u64, size: usize) -> Result<ReadResult> {
        self.stream_builder.file.read_at(pos, size).await
    }

    /// Submit many reads and process the results in a stream-like fashion via a
    /// [`ReadManyResult`].
    ///
    /// This API will optimistically coalesce and deduplicate IO requests such
    /// that two overlapping or adjacent reads will result in a single IO
    /// request. This is transparent for the consumer, you will still
    /// receive individual [`ReadResult`]s corresponding to what you asked for.
    ///
    /// The first argument is a stream of [`IoVec`]. The last two
    /// arguments control how aggressive the IO coalescing should be:
    /// * `buffer_limit` controls how large a merged IO request can get;
    /// * `read_amp_limit` controls how much read amplification is acceptable.
    ///
    /// It is not necessary to respect the `O_DIRECT` alignment of the file, and
    /// this API will internally align the reads appropriately.
    pub fn read_many<V, S>(
        &self,
        iovs: S,
        buffer_limit: MergedBufferLimit,
        read_amp_limit: ReadAmplificationLimit,
    ) -> ReadManyResult<V, impl BulkIo<ReadManyArgs<V>>>
    where
        V: IoVec + Unpin,
        S: Stream<Item = V> + Unpin,
    {
        self.stream_builder
            .file
            .read_many(iovs, buffer_limit, read_amp_limit)
    }

    /// A variant of [`ImmutableFile::read_many`] that yields [`ReadResult`]s in
    /// the order of IO completion.
    pub fn read_many_unordered<V, S>(
        &self,
        iovs: S,
        buffer_limit: MergedBufferLimit,
        read_amp_limit: ReadAmplificationLimit,
    ) -> ReadManyResult<V, impl BulkIo<ReadManyArgs<V>>>
    where
        V: IoVec + Unpin,
        S: Stream<Item = V> + Unpin,
    {
        self.stream_builder
            .file
            .read_many_unordered(iovs, buffer_limit, read_amp_limit)
    }

    /// Rename this file.
    ///
    /// Note: this syscall might be issued in a background thread depending on
    /// the system's capabilities.
    pub async fn rename<P: AsRef<Path>>(&self, new_path: P) -> Result<()> {
        self.stream_builder.file.rename(new_path).await
    }

    /// Remove this file.
    ///
    /// The file does not have to be closed to be removed. Removing removes
    /// the name from the filesystem but the file will still be accessible for
    /// as long as it is open.
    ///
    /// Note: this syscall might be issued in a background thread depending on
    /// the system's capabilities.
    pub async fn remove(&self) -> Result<()> {
        self.stream_builder.file.remove().await
    }

    /// Closes this [`ImmutableFile`]
    pub async fn close(self) -> Result<()> {
        self.stream_builder.file.close_rc().await?;
        Ok(())
    }

    /// Creates a [`DmaStreamReaderBuilder`] from this `ImmutableFile`.
    ///
    /// The resulting builder can be augmented with any option available to the
    /// [`DmaStreamReaderBuilder`] and used to read this file sequentially.
    pub fn stream_reader(&self) -> DmaStreamReaderBuilder {
        self.stream_builder.clone()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{enclose, io::DmaFile, test_utils::make_test_directories};
    use futures::{AsyncReadExt, AsyncWriteExt};
    use futures_lite::stream::{self, StreamExt};

    macro_rules! immutable_file_test {
        ( $name:ident, $dir:ident, $code:block) => {
            #[test]
            fn $name() {
                for dir in make_test_directories(&format!("immutable-dma-{}", stringify!($name))) {
                    let $dir = dir.path.clone();
                    test_executor!(async move { $code });
                }
            }
        };

        ( panic: $name:ident, $dir:ident, $code:block) => {
            #[test]
            #[should_panic]
            fn $name() {
                for dir in make_test_directories(&format!("immutable-dma-{}", stringify!($name))) {
                    let $dir = dir.path.clone();
                    test_executor!(async move { $code });
                }
            }
        };
    }

    immutable_file_test!(panic: fail_on_already_existent, path, {
        let fname = path.join("testfile");
        DmaFile::create(&fname).await.unwrap();

        ImmutableFileBuilder::new(fname).build_sink().await.unwrap();
    });

    immutable_file_test!(panic: fail_reader_on_non_existent, path, {
        let fname = path.join("testfile");
        DmaFile::create(&fname).await.unwrap();

        ImmutableFileBuilder::new(fname)
            .build_existing()
            .await
            .unwrap_err();
    });

    immutable_file_test!(seal_and_stream, path, {
        let fname = path.join("testfile");
        let mut immutable = ImmutableFileBuilder::new(fname).build_sink().await.unwrap();
        let written = immutable.write(&[0, 1, 2, 3, 4, 5]).await.unwrap();
        assert_eq!(written, 6);
        let stream = immutable.seal().await.unwrap();
        let mut reader = stream.stream_reader().build();

        let mut buf = [0u8; 128];
        let x = reader.read(&mut buf).await.unwrap();
        assert_eq!(x, 6);

        reader.close().await.unwrap();
        stream.close().await.unwrap();
    });

    immutable_file_test!(stream_pos, path, {
        let fname = path.join("testfile");
        let mut immutable = ImmutableFileBuilder::new(fname).build_sink().await.unwrap();
        assert_eq!(immutable.current_pos(), 0);
        assert_eq!(immutable.current_flushed_pos(), 0);

        let written = immutable.write(&[0, 1, 2, 3, 4, 5]).await.unwrap();
        assert_eq!(written, 6);
        assert_eq!(immutable.current_pos(), 6);
        assert_eq!(immutable.current_flushed_pos(), 0);

        let written = immutable.write(&[6, 7, 8, 9]).await.unwrap();
        assert_eq!(written, 4);

        let stream = immutable.seal().await.unwrap();
        let mut reader = stream.stream_reader().build();

        let mut buf = [0u8; 128];
        let x = reader.read(&mut buf).await.unwrap();
        assert_eq!(x, 10);

        reader.close().await.unwrap();
        stream.close().await.unwrap();
    });

    immutable_file_test!(seal_and_random, path, {
        let fname = path.join("testfile");
        let mut immutable = ImmutableFileBuilder::new(fname).build_sink().await.unwrap();
        let written = immutable.write(&[0, 1, 2, 3, 4, 5]).await.unwrap();
        assert_eq!(written, 6);
        let stream = immutable.seal().await.unwrap();

        let task1 = crate::spawn_local(enclose! { (stream) async move {
            let buf = stream.read_at(0, 6).await.unwrap();
            assert_eq!(&*buf, &[0, 1, 2, 3, 4, 5]);
        }});

        let task2 = crate::spawn_local(enclose! { (stream) async move {
            let buf = stream.read_at(0, 6).await.unwrap();
            assert_eq!(&*buf, &[0, 1, 2, 3, 4, 5]);
        }});

        assert_eq!(
            2,
            stream::iter(vec![task1, task2]).then(|x| x).count().await
        );

        stream.close().await.unwrap();
    });

    immutable_file_test!(seal_ready_many, path, {
        let fname = path.join("testfile");
        let mut immutable = ImmutableFileBuilder::new(fname).build_sink().await.unwrap();
        let written = immutable.write(&[0, 1, 2, 3, 4, 5]).await.unwrap();
        assert_eq!(written, 6);
        let stream = immutable.seal().await.unwrap();

        {
            let iovs = vec![(0, 1), (3, 1)];
            let mut bufs = stream.read_many(
                stream::iter(iovs.into_iter()),
                MergedBufferLimit::NoMerging,
                ReadAmplificationLimit::NoAmplification,
            );
            let next_buffer = bufs.next().await.unwrap();
            assert_eq!(next_buffer.unwrap().1.len(), 1);
            let next_buffer = bufs.next().await.unwrap();
            assert_eq!(next_buffer.unwrap().1.len(), 1);
        } // ReadManyResult hols a reference to the file so we scope it

        stream.close().await.unwrap();
    });
}
