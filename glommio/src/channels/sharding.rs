use std::{
    fmt::{self, Debug, Formatter},
    pin::Pin,
    rc::Rc,
};

use futures_lite::{Future, Stream, StreamExt};

use crate::{
    channels::channel_mesh::{FullMesh, Senders},
    task::JoinHandle,
    GlommioError,
    ResourceType,
    Result,
};

/// Alias for return type of `Handler`
pub type HandlerResult = Pin<Box<dyn Future<Output = ()>>>;

/// Trait for handling sharded messages
pub trait Handler<T>: Clone {
    /// Handle a message either received from an external stream or forwarded
    /// from another peer.
    /// * `msg` - The message to handle.
    /// * `src_shard` - ID of the shard where the msg is from.
    /// * `cur_shard` - ID of the local shard.
    fn handle(&self, msg: T, src_shard: usize, cur_shard: usize) -> HandlerResult;
}

/// The public interface for sharding
pub struct Sharded<T: Send, H> {
    shard: Rc<Shard<T, H>>,
    consumers: Vec<JoinHandle<()>>,
    forward_tasks: Vec<JoinHandle<()>>,
    closed: bool,
}

impl<T: Send, H> Debug for Sharded<T, H> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Sharded")
    }
}

/// Alias for sharding function
pub type ShardFn<T> = fn(&T, usize) -> usize;

impl<T: Send + 'static, H: Handler<T> + 'static> Sharded<T, H> {
    /// Join a full mesh for sharding
    pub async fn new(mesh: FullMesh<T>, shard_fn: ShardFn<T>, handler: H) -> Result<Self, ()> {
        let nr_shards = mesh.nr_peers();

        let (senders, mut receivers) = mesh.join().await?;

        let shard = Rc::new(Shard {
            nr_shards,
            shard_id: senders.peer_id(),
            shard_fn,
            senders,
            handler: handler.clone(),
        });

        let mut forward_tasks = Vec::with_capacity(nr_shards);
        for (src_shard, stream) in receivers.streams() {
            let handler = handler.clone();
            let cur_shard = shard.shard_id;
            let consumer = crate::spawn_local(async move {
                while let Some(msg) = stream.recv().await {
                    handler.handle(msg, src_shard, cur_shard).await;
                }
            });
            forward_tasks.push(consumer.detach());
        }

        Ok(Self {
            shard,
            consumers: Vec::new(),
            forward_tasks,
            closed: false,
        })
    }

    /// Returns the total number of shards
    pub fn nr_shards(&self) -> usize {
        self.shard.nr_shards
    }

    /// Returns the shard_id associated with ourselves
    pub fn shard_id(&self) -> usize {
        self.shard.shard_id
    }

    /// Consume messages from a stream. It will return a
    /// [`GlommioError::Closed`] if this [`Sharded`] is closed. Otherwise,
    /// the function will return immediately after spawning a background
    /// task draining messages from the stream.
    ///
    /// [`GlommioError::Closed`]: crate::GlommioError::Closed
    pub fn handle<S: Stream<Item = T> + Unpin + 'static>(&mut self, messages: S) -> Result<(), S> {
        if self.closed {
            Err(GlommioError::Closed(ResourceType::Channel(messages)))
        } else {
            let shard = self.shard.clone();
            let consumer = crate::spawn_local(async move { shard.handle(messages).await }).detach();
            self.consumers.push(consumer);
            Ok(())
        }
    }

    /// Sends an individual message to a given shard.
    ///
    /// This function returns [`GlommioError::Closed`] if this [`Sharded`] is
    /// closed, or [`InvalidInput`] if the destination id is invalid.
    ///
    /// This function ignores the sharding function.
    ///
    /// [`GlommioError::Closed`]: crate::GlommioError::Closed
    /// [`InvalidInput`]: std::io::ErrorKind::InvalidInput
    pub async fn send_to(&mut self, dst_shard: usize, message: T) -> Result<(), T> {
        self.shard.send_to(dst_shard, message).await
    }

    /// Sends an individual message to the correct shard.
    ///
    /// The correct shard is calculated using the sharding function in this
    /// `Sharded` object.
    ///
    /// This function returns [`GlommioError::Closed`] if this [`Sharded`] is
    /// closed.
    ///
    /// [`GlommioError::Closed`]: crate::GlommioError::Closed
    pub async fn send(&self, message: T) -> Result<(), T> {
        self.shard.send(message).await
    }

    /// Close this [`Sharded`] and wait for all existing background tasks to
    /// finish. No more consuming task will be spawned, but incoming
    /// messages from the streams consumed by existing background tasks
    /// will not be rejected. So it would be important to truncate the streams
    /// from upstream before calling this method to prevent it from hanging.
    pub async fn close(&mut self) {
        while let Some(consumer) = self.consumers.pop() {
            consumer.await;
        }

        self.shard.close();

        while let Some(task) = self.forward_tasks.pop() {
            task.await;
        }
    }
}

struct Shard<T: Send, H> {
    nr_shards: usize,
    shard_id: usize,
    shard_fn: ShardFn<T>,
    senders: Senders<T>,
    handler: H,
}

impl<T: Send + 'static, H: Handler<T> + 'static> Shard<T, H> {
    async fn handle<S: Stream<Item = T> + Unpin>(&self, mut messages: S) {
        while let Some(msg) = messages.next().await {
            self.send(msg).await.unwrap();
        }
    }

    async fn send_to(&self, dst_shard: usize, msg: T) -> Result<(), T> {
        if dst_shard == self.shard_id {
            self.handler.handle(msg, self.shard_id, self.shard_id).await;
        } else {
            self.senders.send_to(dst_shard, msg).await?;
        }
        Ok(())
    }

    async fn send(&self, msg: T) -> Result<(), T> {
        let dst_shard = (self.shard_fn)(&msg, self.nr_shards);
        self.send_to(dst_shard, msg).await
    }

    fn close(&self) {
        self.senders.close();
    }
}

#[cfg(test)]
mod tests {
    use futures_lite::{future::ready, stream::repeat_with, FutureExt, StreamExt};

    use crate::{
        channels::{
            channel_mesh::MeshBuilder,
            sharding::{Handler, HandlerResult, Sharded},
        },
        enclose,
        prelude::*,
    };

    #[test]
    fn test_send() {
        type Msg = usize;

        let nr_shards = 10;

        fn shard_fn(msg: &Msg, nr_shards: usize) -> usize {
            *msg % nr_shards
        }

        #[derive(Clone)]
        struct RequestHandler {
            _nr_shards: usize,
        }

        impl Handler<Msg> for RequestHandler {
            fn handle(&self, msg: Msg, _src_shard: usize, cur_shard: usize) -> HandlerResult {
                assert_eq!(msg, cur_shard);
                ready(()).boxed_local()
            }
        }

        let mesh = MeshBuilder::full(nr_shards, 1024);

        let shards = (0..nr_shards).map(|_| {
            LocalExecutorBuilder::default().spawn(enclose!((mesh) move || async move {
                let handler = RequestHandler { _nr_shards: nr_shards };
                let mut sharded = Sharded::new(mesh, shard_fn, handler).await.unwrap();
                for i in 0..nr_shards {
                    sharded.send(i).await.unwrap();
                }
                sharded.close().await;
            }))
        });

        for s in shards.collect::<Vec<_>>() {
            s.unwrap().join().unwrap();
        }
    }

    #[test]
    fn test_send_to() {
        type Msg = usize;

        let nr_shards = 10;

        fn shard_fn(_msg: &Msg, _nr_shards: usize) -> usize {
            panic!("Should not be called")
        }

        #[derive(Clone)]
        struct RequestHandler {
            _nr_shards: usize,
        }

        impl Handler<Msg> for RequestHandler {
            fn handle(&self, msg: Msg, _src_shard: usize, cur_shard: usize) -> HandlerResult {
                assert_eq!(msg, cur_shard);
                ready(()).boxed_local()
            }
        }

        let mesh = MeshBuilder::full(nr_shards, 1024);

        let shards = (0..nr_shards).map(|_| {
            LocalExecutorBuilder::default().spawn(enclose!((mesh) move || async move {
                let handler = RequestHandler { _nr_shards: nr_shards };
                let mut sharded = Sharded::new(mesh, shard_fn, handler).await.unwrap();
                for i in 0..nr_shards {
                    sharded.send_to(i, i).await.unwrap();
                }
                sharded.send_to(nr_shards + 1 , nr_shards + 1).await.unwrap_err();
                sharded.close().await;
            }))
        });

        for s in shards.collect::<Vec<_>>() {
            s.unwrap().join().unwrap();
        }
    }

    #[test]
    fn test() {
        type Msg = i32;

        let nr_shards = 10;

        fn shard_fn(msg: &Msg, nr_shards: usize) -> usize {
            *msg as usize % nr_shards
        }

        #[derive(Clone)]
        struct RequestHandler {
            nr_shards: usize,
        }

        impl Handler<i32> for RequestHandler {
            fn handle(&self, msg: Msg, _src_shard: usize, cur_shard: usize) -> HandlerResult {
                assert_eq!(shard_fn(&msg, self.nr_shards), cur_shard);
                ready(()).boxed_local()
            }
        }

        let mesh = MeshBuilder::full(nr_shards, 1024);

        let shards = (0..nr_shards).map(|_| {
            LocalExecutorBuilder::default().spawn(enclose!((mesh) move || async move {
                let handler = RequestHandler { nr_shards };
                let mut sharded = Sharded::new(mesh, shard_fn, handler).await.unwrap();
                let messages = repeat_with(|| fastrand::i32(0..100)).take(1000);
                sharded.handle(messages).unwrap();
                sharded.close().await;
            }))
        });

        for s in shards.collect::<Vec<_>>() {
            s.unwrap().join().unwrap();
        }
    }
}
