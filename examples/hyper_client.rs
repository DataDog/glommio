// Provide --http1 or --http2 arg in run command
// cargo run --example hyper_client -- --http1
mod hyper_compat {
    use futures_lite::{AsyncRead, AsyncWrite, Future};
    use std::{
        io::Write,
        pin::Pin,
        slice,
        task::{Context, Poll},
        vec,
    };

    use glommio::net::TcpStream;

    use http_body_util::BodyExt;
    use hyper::body::{Body as HttpBody, Bytes, Frame};
    use hyper::Error;
    use hyper::Request;
    use std::io;
    use std::marker::PhantomData;

    #[derive(Clone)]
    struct HyperExecutor;

    impl<F> hyper::rt::Executor<F> for HyperExecutor
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        fn execute(&self, fut: F) {
            glommio::spawn_local(fut).detach();
        }
    }

    struct HyperStream(pub TcpStream);

    impl hyper::rt::Write for HyperStream {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.0).poll_write(cx, buf)
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
            Pin::new(&mut self.0).poll_close(cx)
        }
    }

    impl hyper::rt::Read for HyperStream {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            mut buf: hyper::rt::ReadBufCursor<'_>,
        ) -> Poll<std::io::Result<()>> {
            unsafe {
                let read_slice = {
                    let buffer = buf.as_mut();
                    buffer.as_mut_ptr().write_bytes(0, buffer.len());
                    slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut u8, buffer.len())
                };
                Pin::new(&mut self.0).poll_read(cx, read_slice).map(|n| {
                    if let Ok(n) = n {
                        buf.advance(n);
                    }
                    Ok(())
                })
            }
        }
    }

    struct GlommioSleep(glommio::timer::Timer);

    impl Future for GlommioSleep {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            match Pin::new(&mut self.0).poll(cx) {
                Poll::Ready(_) => Poll::Ready(()),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl hyper::rt::Sleep for GlommioSleep {}
    unsafe impl Send for GlommioSleep {}
    unsafe impl Sync for GlommioSleep {}

    #[derive(Clone, Copy, Debug)]
    pub struct GlommioTimer;

    impl hyper::rt::Timer for GlommioTimer {
        fn sleep(&self, duration: std::time::Duration) -> Pin<Box<dyn hyper::rt::Sleep>> {
            Box::pin(GlommioSleep(glommio::timer::Timer::new(duration)))
        }

        fn sleep_until(&self, deadline: std::time::Instant) -> Pin<Box<dyn hyper::rt::Sleep>> {
            Box::pin(GlommioSleep(glommio::timer::Timer::new(
                deadline - std::time::Instant::now(),
            )))
        }
    }

    struct Body {
        // Our Body type is !Send and !Sync:
        _marker: PhantomData<*const ()>,
        data: Option<Bytes>,
    }

    impl From<&[u8]> for Body {
        fn from(data: &[u8]) -> Self {
            Body {
                _marker: PhantomData,
                data: Some(Bytes::copy_from_slice(data)),
            }
        }
    }

    impl HttpBody for Body {
        type Data = Bytes;
        type Error = Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            Poll::Ready(self.get_mut().data.take().map(|d| Ok(Frame::data(d))))
        }
    }

    pub async fn http1_client(
        executor_id: usize,
        url: hyper::Uri,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let host = url.host().expect("uri has no host");
        let port = url.port_u16().unwrap_or(80);
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(addr).await?;

        let io = HyperStream(stream);

        let (mut sender, conn) = hyper::client::conn::http1::handshake(io).await?;

        glommio::spawn_local(async move {
            if let Err(err) = conn.await {
                println!("{executor_id}: Connection failed: {:?}", err);
            }
        })
        .detach();

        let body = serde_json::json!({"test": {}});
        let body_bytes = serde_json::to_vec(&body).unwrap();

        let authority = url.authority().unwrap().clone();
        for request_id in 0..4 {
            let request = Request::builder()
                .uri(url.clone())
                .header(hyper::header::HOST, authority.as_str())
                .body(Body::from(body_bytes.as_slice()))?;

            let mut response = sender.send_request(request).await.unwrap();

            // Print the response body
            let mut res_buff = vec![];
            while let Some(next) = response.frame().await {
                let frame = next.unwrap();
                if let Some(chunk) = frame.data_ref() {
                    res_buff.write_all(chunk).unwrap();
                }
            }

            println!(
                "{executor_id}: request_id = {request_id} | response_status = {} | response_body = {}",
                response.status(),
                String::from_utf8_lossy(&res_buff)
            );
        }

        Ok(())
    }

    pub async fn http2_client(
        executor_id: usize,
        url: hyper::Uri,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let host = url.host().expect("uri has no host");
        let port = url.port_u16().unwrap_or(80);
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(addr).await?;

        let io = HyperStream(stream);

        let (mut sender, conn) = hyper::client::conn::http2::handshake(HyperExecutor, io).await?;

        glommio::spawn_local(async move {
            if let Err(err) = conn.await {
                println!("{executor_id}: Connection failed: {:?}", err);
            }
        })
        .detach();

        let body = serde_json::json!({"test": {}});
        let body_bytes = serde_json::to_vec(&body).unwrap();

        let authority = url.authority().unwrap().clone();
        for request_id in 0..4 {
            let request = Request::builder()
                .uri(url.clone())
                .header(hyper::header::HOST, authority.as_str())
                .body(Body::from(body_bytes.as_slice()))?;

            let mut response = sender.send_request(request).await.unwrap();

            // Print the response body
            let mut res_buff = vec![];
            while let Some(next) = response.frame().await {
                let frame = next.unwrap();
                if let Some(chunk) = frame.data_ref() {
                    res_buff.write_all(chunk).unwrap();
                }
            }

            println!(
                "{executor_id}: request_id = {request_id} | response_status = {} | response_body = {}",
                response.status(),
                String::from_utf8_lossy(&res_buff)
            );
        }

        Ok(())
    }
}

use glommio::{CpuSet, LocalExecutorPoolBuilder, PoolPlacement};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 2 {
        println!("Provide args --http1 or --http2");
        return;
    }

    match args[1].as_str() {
        "--http1" => {
            LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(
                num_cpus::get(),
                CpuSet::online().ok(),
            ))
            .on_all_shards(|| async move {
                let executor_id = glommio::executor().id();
                println!("Starting executor {executor_id}");
                hyper_compat::http1_client(
                    executor_id,
                    "http://0.0.0.0:8000/hello".parse::<hyper::Uri>().unwrap(),
                )
                .await
                .unwrap()
            })
            .unwrap()
            .join_all();
        }
        "--http2" => {
            LocalExecutorPoolBuilder::new(PoolPlacement::MaxSpread(
                num_cpus::get(),
                CpuSet::online().ok(),
            ))
            .on_all_shards(|| async move {
                let executor_id = glommio::executor().id();
                println!("Starting executor {executor_id}");
                hyper_compat::http2_client(
                    executor_id,
                    "http://0.0.0.0:8000/hello".parse::<hyper::Uri>().unwrap(),
                )
                .await
                .unwrap()
            })
            .unwrap()
            .join_all();
        }
        _ => println!("Provide args --http1 or --http2"),
    }
}
