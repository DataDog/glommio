use std::cell::RefCell;
use std::io::Result;
use std::net::SocketAddr;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use futures::io::IoSliceMut;
use futures::task::{Context, Poll};
use futures::{AsyncBufReadExt, AsyncWriteExt};
use futures_lite::{AsyncBufRead, AsyncRead, AsyncWrite};

use glommio::channels::local_channel;
use glommio::channels::local_channel::{LocalReceiver, LocalSender};
use glommio::net::{TcpListener, TcpStream};
use glommio::task::JoinHandle;
use glommio::timer::sleep;
use glommio::{enclose, Local, LocalExecutor, Task};

type Request = Rc<Vec<u8>>;
type Response = Rc<Vec<u8>>;

pub fn server(port: u16) -> SocketAddr {
    let listener = Rc::new(TcpListener::bind(("0.0.0.0", port)).unwrap());

    Local::local(enclose!((listener) async move {
        while let Ok(stream) = listener.accept().await {
            let stream = Rc::new(RefCell::new(stream));

            Local::local(enclose!((stream) async move {
                pipeline(stream).await
            })).detach();
        }
    }))
    .detach();

    listener.local_addr().unwrap()
}

async fn pipeline(stream: Rc<RefCell<TcpStream>>) {
    let (sender, receiver) = local_channel::new_unbounded();

    Local::local(enclose!((stream) async move {
        read_requests(stream.clone(), sender).await;
    }))
    .detach();

    write_responses(stream, receiver).await;
}

async fn read_requests(
    stream: Rc<RefCell<TcpStream>>,
    responses: LocalSender<JoinHandle<Response>>,
) {
    let mut reader = Reader(AsyncIO(stream));
    loop {
        let request = reader.read_line().await.unwrap();
        if let Err(_) = responses.send(handle_request(request)).await {
            break;
        }
    }
}

fn handle_request(request: Request) -> JoinHandle<Response> {
    Task::<Response>::local(async {
        let delay = Duration::from_millis(fastrand::u64(..100));
        sleep(delay).await;
        println!(
            "{:?}: slept {:?}",
            String::from_utf8(request.to_vec()).unwrap(),
            delay
        );
        request
    })
    .detach()
}

async fn write_responses(
    stream: Rc<RefCell<TcpStream>>,
    receiver: LocalReceiver<JoinHandle<Response>>,
) {
    let mut writer = Writer(AsyncIO(stream));
    while let Some(response) = receiver.recv().await {
        if let Some(response) = &response.await {
            writer.write_line(response.deref()).await.unwrap();
        }
    }
}

struct AsyncIO<T>(Rc<RefCell<T>>);

impl<T: AsyncRead + Unpin> AsyncRead for AsyncIO<T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize>> {
        Pin::new(self.0.borrow_mut().deref_mut()).poll_read(cx, buf)
    }

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<Result<usize>> {
        Pin::new(self.0.borrow_mut().deref_mut()).poll_read_vectored(cx, bufs)
    }
}

impl<T: AsyncBufRead + Unpin> AsyncBufRead for AsyncIO<T> {
    fn poll_fill_buf(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<&[u8]>> {
        Pin::new(unsafe { &mut *self.0.as_ptr() }).poll_fill_buf(cx)
    }

    fn consume(self: Pin<&mut Self>, amt: usize) {
        Pin::new(self.0.borrow_mut().deref_mut()).consume(amt);
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for AsyncIO<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        Pin::new(self.0.borrow_mut().deref_mut()).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(self.0.borrow_mut().deref_mut()).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(self.0.borrow_mut().deref_mut()).poll_close(cx)
    }
}

struct Reader<T>(AsyncIO<T>);

impl<T: AsyncBufRead + AsyncWrite + Unpin> Reader<T> {
    async fn read_line(&mut self) -> Result<Rc<Vec<u8>>> {
        let mut line = Box::new(Vec::new());
        self.0.read_until(b'\n', line.as_mut()).await?;
        line.truncate(line.len() - 1);
        Ok(Rc::from(line))
    }
}

struct Writer<T>(AsyncIO<T>);

impl<T: AsyncWrite + Unpin> Writer<T> {
    async fn write_line(&mut self, line: &[u8]) -> Result<()> {
        self.0.write(line).await?;
        self.0.write(b"\n").await?;
        Ok(())
    }
}

fn main() {
    LocalExecutor::default().run(async {
        let port = server(0).port();
        let stream = Rc::new(RefCell::new(
            TcpStream::connect(("127.0.0.1", port)).await.unwrap(),
        ));

        let n_requests: usize = 10;
        Local::local(enclose!((stream) async move {
            let mut writer = Writer(AsyncIO(stream));
            for i in 0..n_requests {
                writer.write_line(format!("line {}", i).as_bytes()).await.unwrap();
            }
        }))
        .detach();

        let mut reader = Reader(AsyncIO(stream));
        for _ in 0..n_requests {
            println!(
                "{:?}",
                String::from_utf8(reader.read_line().await.unwrap().to_vec()).unwrap()
            )
        }
    })
}
