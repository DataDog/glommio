use futures_lite::io::{AsyncReadExt, AsyncWriteExt};
use glommio::{
    net::{VsockListener, VsockStream},
    prelude::*,
};
use std::io::Result;

fn main() -> Result<()> {
    let executor = LocalExecutor::default();
    executor.run(async move {
        let listener = VsockListener::bind_with_cid_port(u32::MAX, 1337).unwrap();
        let mut stream = listener.accept().await.unwrap().buffered();

        let mut buf = [0u8; 1];
        while stream.read(&mut buf).await.unwrap() != 0 {
            stream.write(&buf).await.unwrap();
        }

        println!("done!");
    });
    Ok(())
}
