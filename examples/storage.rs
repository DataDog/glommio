use clap::{App, Arg};
use futures_lite::{
    stream::{self, StreamExt},
    AsyncReadExt,
    AsyncWriteExt,
};
use glommio::{
    enclose,
    io::{
        BufferedFile,
        DmaFile,
        DmaStreamReader,
        DmaStreamReaderBuilder,
        DmaStreamWriterBuilder,
        StreamReaderBuilder,
        StreamWriterBuilder,
    },
    Local,
    LocalExecutorBuilder,
};
use pretty_bytes::converter;
use std::{
    cell::Cell,
    fs,
    path::PathBuf,
    rc::Rc,
    time::{Duration, Instant},
};

struct BenchDirectory {
    path: PathBuf,
}

impl Drop for BenchDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

impl BenchDirectory {
    fn new(path: PathBuf) -> Self {
        fs::create_dir_all(&path).unwrap();
        BenchDirectory { path }
    }
}

async fn stream_write<T: AsyncWriteExt + std::marker::Unpin, S: Into<String>>(
    mut stream: T,
    name: S,
    file_size: u64,
) {
    let contents = vec![1; 512 << 10];
    let start = Instant::now();
    for _ in 0..(file_size / (contents.len() as u64)) {
        stream.write_all(&contents).await.unwrap();
    }
    let name = name.into();

    let endw = Instant::now();
    let time = start.elapsed();
    let bytes = converter::convert(file_size as _);
    let rate = converter::convert((file_size as f64 / time.as_secs_f64()) as _);
    println!("{}: Wrote {} in {:#?}, {}/s", &name, bytes, time, rate);
    stream.close().await.unwrap();
    let rate = converter::convert((file_size as f64 / start.elapsed().as_secs_f64()) as _);
    let time = endw.elapsed();
    println!(
        "{}: Closed in {:#?}, Amortized total {}/s",
        &name, time, rate
    );
}

async fn stream_scan<T: AsyncReadExt + std::marker::Unpin, S: Into<String>>(
    mut stream: T,
    name: S,
) -> T {
    let mut buf = vec![0; 4 << 10];
    let expected = vec![1; 4 << 10];
    let mut bytes_read = 0;
    let mut ops = 0;

    let start = Instant::now();
    loop {
        let res = stream.read(&mut buf).await.unwrap();
        bytes_read += res;
        ops += 1;
        if res == 0 {
            break;
        }
        assert_eq!(expected, buf);
    }
    let time = start.elapsed();
    let name = name.into();

    let bytes = converter::convert(bytes_read as _);
    let rate = converter::convert((bytes_read as f64 / time.as_secs_f64()) as _);
    println!(
        "{}: Scanned {} in {:#?}, {}/s, {} IOPS",
        &name,
        bytes,
        time,
        rate,
        (ops as f64 / time.as_secs_f64()) as usize
    );
    stream
}

async fn stream_scan_alt_api<S: Into<String>>(
    mut stream: DmaStreamReader,
    name: S,
    buffer_size: usize,
) {
    let mut expected = Vec::with_capacity(buffer_size);
    expected.resize(buffer_size, 1u8);

    let mut bytes_read = 0;
    let mut ops = 0;

    let start = Instant::now();
    loop {
        let buffer = stream.get_buffer_aligned(buffer_size as _).await.unwrap();
        bytes_read += buffer.len();
        ops += 1;
        if buffer.len() < buffer_size {
            break;
        }
        assert_eq!(*expected, *buffer);
    }
    let time = start.elapsed();
    let name = name.into();

    let bytes = converter::convert(bytes_read as _);
    let rate = converter::convert((bytes_read as f64 / time.as_secs_f64()) as _);
    println!(
        "{}: Scanned {} in {:#?}, {}/s, {} IOPS",
        &name,
        bytes,
        time,
        rate,
        (ops as f64 / time.as_secs_f64()) as usize
    );
    stream.close().await.unwrap();
}

enum Reader {
    Direct(Rc<DmaFile>),
    Buffered(BufferedFile),
}

impl Reader {
    async fn read(&self, pos: u64, io_size: u64, _expected: &[u8]) {
        match &self {
            Reader::Direct(file) => {
                file.read_at_aligned(pos, io_size as _).await.unwrap();
            }
            Reader::Buffered(file) => {
                file.read_at(pos, io_size as _).await.unwrap();
            }
        }
    }

    async fn read_many<S: Iterator<Item = (u64, usize)>>(
        &self,
        iovs: S,
        _expected: &[u8],
        max_buffer_size: usize,
    ) {
        match &self {
            Reader::Direct(file) => {
                file.read_many(iovs, max_buffer_size, None)
                    .for_each(|_| {})
                    .await;
            }
            Reader::Buffered(_) => {
                panic!("bulk io is not available for buffered files")
            }
        }
    }

    async fn close(self) {
        match self {
            Reader::Direct(file) => {
                file.close_rc().await.unwrap();
            }
            Reader::Buffered(file) => {
                file.close().await.unwrap();
            }
        }
    }
}

async fn random_read<S: Into<String>>(
    file: Reader,
    name: S,
    random: u64,
    parallelism: usize,
    io_size: u64,
) {
    let end = (random / io_size) - 1;
    let name = name.into();
    let mut expected = Vec::with_capacity(io_size as _);
    expected.resize(io_size as _, 1);

    let file = Rc::new(file);
    let iops = Rc::new(Cell::new(0));

    let time = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..parallelism {
        tasks.push(
            Local::local(enclose! { (file, iops, expected) async move {
                while time.elapsed() < Duration::from_secs(20) {
                    let pos = fastrand::u64(0..end);
                    file.read(pos * io_size, io_size as _, &expected).await;
                    iops.set(iops.get() + 1);
                }
            }})
            .detach(),
        );
    }

    let finished = stream::iter(tasks).then(|f| f).count().await;

    match Rc::try_unwrap(file) {
        Err(_) => unreachable!(),
        Ok(file) => file.close().await,
    };

    assert_eq!(finished, parallelism as _);
    let bytes = converter::convert(random as _);
    let dur = time.elapsed();
    println!(
        "{}: Random Read (uniform) size span of {}, for {:#?}, {} IOPS",
        &name,
        bytes,
        dur,
        (iops.get() as f64 / dur.as_secs_f64()) as usize
    );
}

async fn random_many_read<S: Into<String>>(
    file: Reader,
    name: S,
    random: u64,
    parallelism: usize,
    io_size: u64,
    max_buffer_size: usize,
) {
    let end = (random / io_size) - 1;
    let name = name.into();
    let mut expected = Vec::with_capacity(io_size as _);
    expected.resize(io_size as _, 1);

    let file = Rc::new(file);
    let iops = Rc::new(Cell::new(0));

    let time = Instant::now();
    let mut tasks = Vec::new();
    for _ in 0..parallelism {
        tasks.push(
            Local::local(enclose! { (file, iops, expected) async move {
                while time.elapsed() < Duration::from_secs(20) {
                    file.read_many((0..parallelism).map(|_| {
                        let pos = fastrand::u64(0..end);
                        ((pos * io_size) as u64, io_size as usize)
                    }), &expected, max_buffer_size).await;
                    iops.set(iops.get() + parallelism);
                }
            }})
            .detach(),
        );
    }

    let finished = stream::iter(tasks).then(|f| f).count().await;

    match Rc::try_unwrap(file) {
        Err(_) => unreachable!(),
        Ok(file) => file.close().await,
    };

    assert_eq!(finished, parallelism as _);
    let bytes = converter::convert(random as _);
    let max_merged = converter::convert(max_buffer_size as _);
    let dur = time.elapsed();
    println!(
        "{}: Random Bulk Read (uniform) size span of {}, for {:#?} (max merged size of {}), {} \
         IOPS",
        &name,
        bytes,
        dur,
        max_merged,
        (iops.get() as f64 / dur.as_secs_f64()) as usize
    );
}

fn main() {
    let matches = App::new("storage example")
        .version("0.1.0")
        .author("Glauber Costa <glauber@datadoghq.com>")
        .about("demonstrate glommio's storage APIs")
        .arg(
            Arg::with_name("storage_dir")
                .long("dir")
                .takes_value(true)
                .required(true)
                .help("The directory where to write and read file for this test"),
        )
        .arg(
            Arg::with_name("file_size")
                .long("size-gb")
                .takes_value(true)
                .required(false)
                .help("size of the file in GB (default: 2 * memory_size)"),
        )
        .get_matches();

    let path = matches.value_of("storage_dir").unwrap();
    let mut dir = PathBuf::from(path);
    assert!(dir.exists());
    dir.push("benchfiles");
    assert!(!dir.exists());
    let dir = BenchDirectory::new(dir);

    let total_memory = sys_info::mem_info().unwrap().total << 10;

    let file_size = matches
        .value_of("file_size")
        .and_then(|s| Some(s.parse::<u64>().unwrap() << 30))
        .unwrap_or(total_memory * 2);

    let random = total_memory / 10;

    let local_ex = LocalExecutorBuilder::new()
        .pin_to_cpu(0)
        .spin_before_park(Duration::from_millis(10))
        .spawn(move || async move {
            let mut dio_filename = dir.path.clone();
            dio_filename.push("benchfile-dio-1");

            let mut buf_filename = dir.path.clone();
            buf_filename.push("benchfile-buf-1");

            let file = BufferedFile::create(&buf_filename).await.unwrap();
            let stream = StreamWriterBuilder::new(file).build();
            stream_write(stream, "Buffered I/O", file_size).await;

            let file = DmaFile::create(&dio_filename).await.unwrap();
            let stream = DmaStreamWriterBuilder::new(file)
                .with_write_behind(1)
                .with_buffer_size(512 << 10)
                .build();
            stream_write(stream, "Direct I/O", file_size).await;

            let file = DmaFile::create(&dio_filename).await.unwrap();
            let stream = DmaStreamWriterBuilder::new(file)
                .with_write_behind(10)
                .with_buffer_size(512 << 10)
                .build();
            stream_write(stream, "Direct I/O, write-behind", file_size).await;

            let file = BufferedFile::open(&buf_filename).await.unwrap();
            let stream = StreamReaderBuilder::new(file).build();
            let stream = stream_scan(stream, "Buffered I/O").await;
            stream.close().await.unwrap();

            let file = DmaFile::open(&dio_filename).await.unwrap();
            let stream = DmaStreamReaderBuilder::new(file)
                .with_read_ahead(1)
                .with_buffer_size(4 << 10)
                .build();
            let stream = stream_scan(stream, "Direct I/O").await;
            stream.close().await.unwrap();

            let file = DmaFile::open(&dio_filename).await.unwrap();
            let stream = DmaStreamReaderBuilder::new(file)
                .with_read_ahead(50)
                .with_buffer_size(4 << 10)
                .build();
            let stream = stream_scan(stream, "Direct I/O, read ahead").await;
            stream.close().await.unwrap();

            let file = DmaFile::open(&dio_filename).await.unwrap();
            let stream = DmaStreamReaderBuilder::new(file)
                .with_read_ahead(50)
                .with_buffer_size(4 << 10)
                .build();
            stream_scan_alt_api(stream, "Direct I/O, glommio API", 4 << 10).await;

            let file = DmaFile::open(&dio_filename).await.unwrap();
            let stream = DmaStreamReaderBuilder::new(file)
                .with_read_ahead(10)
                .with_buffer_size(512 << 10)
                .build();
            stream_scan_alt_api(stream, "Direct I/O, glommio API, large buffer", 512 << 10).await;

            let file = BufferedFile::open(&buf_filename).await.unwrap();
            random_read(Reader::Buffered(file), "Buffered I/O", random, 50, 4096).await;

            let file = Rc::new(DmaFile::open(&dio_filename).await.unwrap());
            random_read(Reader::Direct(file), "Direct I/O", random, 50, 4096).await;

            let file = Rc::new(DmaFile::open(&dio_filename).await.unwrap());
            random_many_read(Reader::Direct(file), "Direct I/O", random, 50, 4096, 0).await;

            let file = Rc::new(DmaFile::open(&dio_filename).await.unwrap());
            random_many_read(Reader::Direct(file), "Direct I/O", random, 50, 4096, 131072).await;

            let file = BufferedFile::open(&buf_filename).await.unwrap();
            random_read(Reader::Buffered(file), "Buffered I/O", file_size, 50, 4096).await;

            let file = Rc::new(DmaFile::open(&dio_filename).await.unwrap());
            random_read(Reader::Direct(file), "Direct I/O", file_size, 50, 4096).await;

            let file = Rc::new(DmaFile::open(&dio_filename).await.unwrap());
            random_many_read(Reader::Direct(file), "Direct I/O", file_size, 50, 4096, 0).await;

            let file = Rc::new(DmaFile::open(&dio_filename).await.unwrap());
            random_many_read(
                Reader::Direct(file),
                "Direct I/O",
                file_size,
                50,
                4096,
                131072,
            )
            .await;
        })
        .unwrap();

    local_ex.join().unwrap();
}
