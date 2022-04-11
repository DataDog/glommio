// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
//
use crate::{
    channels::spsc_queue::{make, BufferHalf, Consumer, Producer},
    enclose,
    reactor::Reactor,
    sys::{self, SleepNotifier},
    GlommioError,
    ResourceType,
};
use futures_lite::{future, stream::Stream};
use std::{
    fmt,
    future::Future,
    pin::Pin,
    rc::{Rc, Weak},
    sync::Arc,
    task::{Context, Poll},
};

type Result<T, V> = crate::Result<T, V>;

/// The `SharedReceiver` is the receiving end of the Shared Channel.
/// It implements [`Send`] so it can be passed to any thread. However,
/// it doesn't implement any method: before it is used it must be changed
/// into a [`ConnectedReceiver`], which then makes sure it will be used by
/// at most one thread.
///
/// It is technically possible to share this among multiple threads inside
/// a lock, although such design is discouraged and beats the purpose of a
/// spsc channel.
///
/// [`ConnectedReceiver`]: struct.ConnectedReceiver.html
/// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
pub struct SharedReceiver<T: Send + Sized> {
    state: Option<Rc<ReceiverState<T>>>,
}

/// The `SharedSender` is the sending end of the Shared Channel.
/// It implements [`Send`] so it can be passed to any thread. However,
/// it doesn't implement any method: before it is used it must be changed
/// into a [`ConnectedSender`], which then makes sure it will be used by
/// at most one thread.
///
/// It is technically possible to share this among multiple threads inside
/// a lock, although such design is discouraged and beats the purpose of a
/// spsc channel.
///
/// [`ConnectedSender`]: struct.ConnectedSender.html
/// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
pub struct SharedSender<T: Send + Sized> {
    state: Option<Rc<SenderState<T>>>,
}

impl<T: Send + Sized> fmt::Debug for SharedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.state {
            Some(s) => write!(f, "Unbound SharedSender {:?}", s.buffer),
            None => write!(f, "Bound SharedSender"),
        }
    }
}

impl<T: Send + Sized> fmt::Debug for SharedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.state {
            Some(s) => write!(f, "Unbound SharedReceiver: {:?}", s.buffer),
            None => write!(f, "Bound SharedReceiver"),
        }
    }
}

unsafe impl<T: Send + Sized> Send for SharedReceiver<T> {}
unsafe impl<T: Send + Sized> Send for SharedSender<T> {}

/// The `ConnectedReceiver` is the receiving end of the Shared Channel.
pub struct ConnectedReceiver<T: Send + Sized> {
    id: u64,
    state: Rc<ReceiverState<T>>,
    reactor: Weak<Reactor>,
    notifier: Arc<SleepNotifier>,
}

/// The `ConnectedSender` is the sending end of the Shared Channel.
pub struct ConnectedSender<T: Send + Sized> {
    id: u64,
    state: Rc<SenderState<T>>,
    reactor: Weak<Reactor>,
    notifier: Arc<SleepNotifier>,
}

impl<T: Send + Sized> fmt::Debug for ConnectedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connected Receiver {}: {:?}", self.id, self.state.buffer)
    }
}

impl<T: Send + Sized> fmt::Debug for ConnectedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connected Sender {} : {:?}", self.id, self.state.buffer)
    }
}

#[derive(Debug)]
struct SenderState<V: Send + Sized> {
    buffer: Producer<V>,
}

#[derive(Debug)]
struct ReceiverState<V: Send + Sized> {
    buffer: Consumer<V>,
}

struct Connector<T: BufferHalf + Clone> {
    buffer: T,
    reactor: Weak<Reactor>,
}

impl<T: BufferHalf + Clone> Connector<T> {
    fn new(buffer: T, reactor: Weak<Reactor>) -> Self {
        Self { buffer, reactor }
    }
}

impl<T: BufferHalf + Clone> Future for Connector<T> {
    type Output = Arc<SleepNotifier>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let reactor = self.reactor.upgrade().unwrap();

        match self.buffer.peer_id() {
            0 => {
                reactor.add_shared_channel_connection_waker(cx.waker().clone());
                Poll::Pending
            }
            // usize::MAX (the disconnected) always has a placeholder notifier that never
            // returns its fd. So if the other side disconnected it will unblock us here
            id => Poll::Ready(sys::get_sleep_notifier_for(id).unwrap()),
        }
    }
}

/// Creates a new `shared_channel` returning its sender and receiver
/// endpoints.
///
/// All shared channels must be bounded.
pub fn new_bounded<T: Send + Sized>(size: usize) -> (SharedSender<T>, SharedReceiver<T>) {
    let (producer, consumer) = make(size);
    (
        SharedSender {
            state: Some(Rc::new(SenderState { buffer: producer })),
        },
        SharedReceiver {
            state: Some(Rc::new(ReceiverState { buffer: consumer })),
        },
    )
}

impl<T: 'static + Send + Sized> SharedSender<T> {
    /// Connects this sender, returning a [`ConnectedSender`] that can be used
    /// to send data into this channel
    ///
    /// [`ConnectedSender`]: struct.ConnectedSender.html
    pub async fn connect(mut self) -> ConnectedSender<T> {
        let state = self.state.take().unwrap();
        let reactor = crate::executor().reactor();
        state.buffer.connect(reactor.id());
        let id = reactor.register_shared_channel(Box::new(enclose! {(state) move || {
            if state.buffer.consumer_disconnected() {
                state.buffer.capacity()
            } else {
                state.buffer.free_space()
            }
        }}));

        let reactor = Rc::downgrade(&reactor);
        let peer = Connector::new(state.buffer.clone(), reactor.clone());
        let notifier = peer.await;
        ConnectedSender {
            id,
            state,
            reactor,
            notifier,
        }
    }
}

impl<T: Send + Sized> ConnectedSender<T> {
    /// Sends data into this channel.
    ///
    /// It returns a [`GlommioError::Closed`] if the receiver is destroyed.
    /// It returns a [`GlommioError::WouldBlock`] if this is a bounded channel
    /// that has no more capacity
    ///
    /// # Examples
    /// ```
    /// use futures_lite::StreamExt;
    /// use glommio::{channels::shared_channel, prelude::*};
    ///
    /// let (sender, receiver) = shared_channel::new_bounded(1);
    /// let producer = LocalExecutorBuilder::default()
    ///     .name("producer")
    ///     .spawn(move || async move {
    ///         let sender = sender.connect().await;
    ///         sender.try_send(0);
    ///     })
    ///     .unwrap();
    /// let receiver = LocalExecutorBuilder::default()
    ///     .name("receiver")
    ///     .spawn(move || async move {
    ///         let mut receiver = receiver.connect().await;
    ///         receiver.next().await.unwrap();
    ///     })
    ///     .unwrap();
    /// producer.join().unwrap();
    /// receiver.join().unwrap();
    /// ```
    ///
    /// [`BrokenPipe`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.BrokenPipe
    /// [`WouldBlock`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.WouldBlock
    /// [`Other`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.Other
    /// [`GlommioError`]: ../../struct.GlommioError.html
    pub fn try_send(&self, item: T) -> Result<(), T> {
        // This is a shared channel so state can change under our noses.
        // We test if the buffer is disconnected before sending to avoid
        // sending a value that will not be received (otherwise we would only
        // receive WouldBlock when the buffer capacity fills).
        //
        // However after we try_push(), we can still fail because the buffer
        // disconnected between now and then. That's okay as all we're trying to
        // do here is prevent unnecessary sends.
        //
        // Note that we check `producer_disconnected` because:
        // (1) senders can be referenced by multiple tasks simultaneously
        // (2) senders can be closed at any time using `Self::close` (which does not
        // automatically cause the receiver / consumer to drop)
        // (3) `Self::try_send` is used by async `Self::send`, which will not
        // unblock correctly if the channel was closed in another task
        if self.state.buffer.consumer_disconnected()
            || self.state.buffer.buffer.producer_disconnected()
        {
            return Err(GlommioError::Closed(ResourceType::Channel(item)));
        }
        match self.state.buffer.try_push(item) {
            None => {
                self.notifier.notify(false);
                Ok(())
            }
            Some(item) => {
                let res = if self.state.buffer.consumer_disconnected()
                    || self.state.buffer.buffer.producer_disconnected()
                {
                    GlommioError::Closed(ResourceType::Channel(item))
                } else {
                    GlommioError::WouldBlock(ResourceType::Channel(item))
                };
                Err(res)
            }
        }
    }

    /// Sends data into this channel when it is ready to receive it
    ///
    /// # Examples
    /// ```
    /// use glommio::{channels::shared_channel, prelude::*};
    ///
    /// let (sender, receiver) = shared_channel::new_bounded(1);
    /// let producer = LocalExecutorBuilder::default()
    ///     .name("producer")
    ///     .spawn(move || async move {
    ///         let sender = sender.connect().await;
    ///         sender.send(0).await;
    ///     })
    ///     .unwrap();
    /// let receiver = LocalExecutorBuilder::default()
    ///     .name("receiver")
    ///     .spawn(move || async move {
    ///         let mut receiver = receiver.connect().await;
    ///         receiver.recv().await.unwrap();
    ///     })
    ///     .unwrap();
    /// producer.join().unwrap();
    /// receiver.join().unwrap();
    /// ```
    #[track_caller]
    pub async fn send(&self, item: T) -> Result<(), T> {
        let waiter = future::poll_fn(|cx| self.wait_for_room(cx));
        waiter.await;
        let res = self.try_send(item);
        if let Err(GlommioError::WouldBlock(_)) = &res {
            panic!("operation would block")
        }
        res
    }

    fn wait_for_room(&self, cx: &mut Context<'_>) -> Poll<()> {
        match self.state.buffer.free_space() > 0 || self.state.buffer.producer_disconnected() {
            true => Poll::Ready(()),
            false => {
                self.reactor
                    .upgrade()
                    .unwrap()
                    .add_shared_channel_waker(self.id, cx.waker().clone());
                Poll::Pending
            }
        }
    }

    /// Close the sender
    pub fn close(&self) {
        if !self.state.buffer.disconnect() {
            if let Some(r) = self.reactor.upgrade() {
                self.notifier.notify(false);
                // wake other tasks `awaiting` the same sender letting them know sender is
                // closed; we don't `unregister_shared_channel` here because
                // another task could still be `await`ing this sender, in which
                // case we need to be able to `wake` it
                r.process_shared_channels_by_id(self.id);
            }
        }
    }
}

impl<T: 'static + Send + Sized> SharedReceiver<T> {
    /// Connects this receiver, returning a [`ConnectedReceiver`] that can be
    /// used to send data into this channel
    ///
    /// [`ConnectedReceiver`]: struct.ConnectedReceiver.html
    pub async fn connect(mut self) -> ConnectedReceiver<T> {
        let reactor = crate::executor().reactor();
        let state = self.state.take().unwrap();
        state.buffer.connect(reactor.id());
        let id = reactor.register_shared_channel(Box::new(enclose! { (state) move || {
            if state.buffer.producer_disconnected() {
                state.buffer.capacity()
            } else {
                state.buffer.size()
            }
        }}));

        let reactor = Rc::downgrade(&reactor);
        let peer = Connector::new(state.buffer.clone(), reactor.clone());
        let notifier = peer.await;
        ConnectedReceiver {
            id,
            state,
            reactor,
            notifier,
        }
    }
}

impl<T: Send + Sized> ConnectedReceiver<T> {
    /// Receives data from this channel
    ///
    /// If the sender is no longer available it returns [`None`]. Otherwise,
    /// blocks until an item is available and returns it wrapped in [`Some`].
    ///
    /// Notice that this is also available as a Stream. Whether to consume from
    /// a stream or `recv` is up to the application. The biggest difference
    /// is that [`StreamExt`]'s [`next`] method takes a mutable reference to
    /// self. If the LocalReceiver is, say, behind an [`Rc`] it may be more
    /// ergonomic to `recv`.
    ///
    /// # Examples
    /// ```
    /// use glommio::{channels::shared_channel, prelude::*};
    ///
    /// let (sender, receiver) = shared_channel::new_bounded(1);
    /// let producer = LocalExecutorBuilder::default()
    ///     .name("producer")
    ///     .spawn(move || async move {
    ///         let sender = sender.connect().await;
    ///         sender.try_send(0u32);
    ///     })
    ///     .unwrap();
    /// let receiver = LocalExecutorBuilder::default()
    ///     .name("receiver")
    ///     .spawn(move || async move {
    ///         let mut receiver = receiver.connect().await;
    ///         let x = receiver.recv().await.unwrap();
    ///         assert_eq!(x, 0);
    ///     })
    ///     .unwrap();
    /// producer.join().unwrap();
    /// receiver.join().unwrap();
    /// ```
    ///
    /// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
    /// [`Some`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.Some
    /// [`StreamExt`]: https://docs.rs/futures-lite/1.11.2/futures_lite/stream/index.html
    /// [`next`]: https://docs.rs/futures-lite/1.11.2/futures_lite/stream/trait.StreamExt.html#method.next
    /// [`Rc`]: https://doc.rust-lang.org/std/rc/struct.Rc.html
    pub async fn recv(&self) -> Option<T> {
        let waiter = future::poll_fn(|cx| self.recv_one(cx));
        waiter.await
    }

    fn recv_one(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.do_recv_one(cx, false)
    }

    fn do_recv_one(&self, cx: &mut Context<'_>, disconnected: bool) -> Poll<Option<T>> {
        match self.state.buffer.try_pop() {
            None => {
                if disconnected {
                    Poll::Ready(None)
                } else if self.state.buffer.producer_disconnected() {
                    // Double check in case the producer sent the last message and
                    // disconnected right after a `None` is returned from `try_pop`
                    self.do_recv_one(cx, true)
                } else {
                    self.reactor
                        .upgrade()
                        .unwrap()
                        .add_shared_channel_waker(self.id, cx.waker().clone());
                    Poll::Pending
                }
            }
            res => {
                self.notifier.notify(false);
                Poll::Ready(res)
            }
        }
    }
}

impl<T: Send + Sized> Stream for ConnectedReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.recv_one(cx)
    }
}

impl<T: Send + Sized> Drop for SharedSender<T> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            // Never connected, we must connect ourselves.
            if !state.buffer.disconnect() {
                let id = state.buffer.peer_id();
                if let Some(notifier) = sys::get_sleep_notifier_for(id) {
                    notifier.notify(false);
                }
            }
        }
    }
}

impl<T: Send + Sized> Drop for SharedReceiver<T> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            // Never connected, we must connect ourselves.
            if !state.buffer.disconnect() {
                let id = state.buffer.peer_id();
                if let Some(notifier) = sys::get_sleep_notifier_for(id) {
                    notifier.notify(false);
                }
            }
        }
    }
}

impl<T: Send + Sized> Drop for ConnectedReceiver<T> {
    fn drop(&mut self) {
        if !self.state.buffer.disconnect() {
            if let Some(r) = self.reactor.upgrade() {
                self.notifier.notify(false);
                r.unregister_shared_channel(self.id);
            }
        }
    }
}

impl<T: Send + Sized> Drop for ConnectedSender<T> {
    fn drop(&mut self) {
        if !self.state.buffer.disconnect() {
            if let Some(r) = self.reactor.upgrade() {
                self.notifier.notify(false);
                r.unregister_shared_channel(self.id)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        timer::{sleep, Timer},
        LocalExecutorBuilder,
        Placement,
    };
    use futures_lite::{FutureExt, StreamExt};
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    #[test]
    fn producer_consumer() {
        let (sender, receiver) = new_bounded(10);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                Timer::new(Duration::from_millis(10)).await;
                sender.try_send(100).unwrap();
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let x = receiver.recv().await;
                assert_eq!(x.unwrap(), 100);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn producer_stream_consumer() {
        let (sender, receiver) = new_bounded(1);

        let ex1 = LocalExecutorBuilder::new(Placement::Fixed(0))
            .spin_before_park(Duration::from_millis(1000000))
            .spawn(move || async move {
                let sender = sender.connect().await;
                for _ in 0..10 {
                    sender.send(1).await.unwrap();
                    Timer::new(Duration::from_millis(1)).await;
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::new(Placement::Fixed(1))
            .spin_before_park(Duration::from_millis(1000000))
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let sum = receiver.fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 10);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn consumer_sleeps_before_producer_produces() {
        let (sender, receiver) = new_bounded(1);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                Timer::new(Duration::from_millis(100)).await;
                let sender = sender.connect().await;
                sender.send(1).await.unwrap();
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let recv = receiver.recv().await.unwrap();
                assert_eq!(recv, 1);
                let sum = receiver.fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 0);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn producer_sleeps_before_consumer_consumes() {
        let (sender, receiver) = new_bounded(1);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                // This will go right away because the channel fits 1 element
                sender.try_send(1).unwrap();
                // This will sleep. The consumer should unblock us
                sender.send(1).await.unwrap();
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                Timer::new(Duration::from_millis(100)).await;
                let receiver = receiver.connect().await;
                let sum = receiver.fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 2);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn producer_never_connects() {
        let (sender, receiver) = new_bounded(1);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                drop(sender);
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver: ConnectedReceiver<usize> = receiver.connect().await;
                assert!(receiver.recv().await.is_none());
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn destroy_with_pending_wakers() {
        let (sender, receiver) = new_bounded::<u8>(1);

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect();
                let sender = sender.connect();
                let (receiver, sender) = futures::future::join(receiver, sender).await;

                future::poll_fn(move |cx| {
                    let mut f1 = receiver.recv().boxed_local();
                    assert_eq!(f1.poll(cx), Poll::Pending);
                    assert!(sender.try_send(1).is_ok());
                    let r = receiver.recv_one(cx);
                    assert_eq!(r, Poll::Ready(Some(1)));
                    r
                })
                .await;
                sleep(Duration::from_secs(1)).await;
            })
            .unwrap();

        ex2.join().unwrap();
    }

    #[test]
    fn consumer_never_connects() {
        let (sender, receiver) = new_bounded(1);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                drop(receiver);
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                Timer::new(Duration::from_millis(100)).await;
                let sender: ConnectedSender<usize> = sender.connect().await;
                match sender.send(0).await {
                    Ok(_) => panic!("Should not have sent"),
                    Err(GlommioError::Closed(ResourceType::Channel(_))) => {
                        // all good
                    }
                    Err(other_err) => {
                        panic!("incorrect error type: '{}' for channel send", other_err)
                    }
                }
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn pass_function() {
        let (sender, receiver) = new_bounded(10);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                Timer::new(Duration::from_millis(10)).await;
                if sender.send(|| 32).await.is_err() {
                    panic!("send failed");
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let x = receiver.recv().await.unwrap();
                assert_eq!(32, x());
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn send_to_full_channel() {
        let (sender, receiver) = new_bounded(1);

        let status = Arc::new(AtomicUsize::new(0));
        let s1 = status.clone();

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                sender.send(0).await.unwrap();
                let x = sender.try_send(1);
                assert!(x.is_err());
                s1.store(1, Ordering::Relaxed);
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;

                while status.load(Ordering::Relaxed) == 0 {}
                let x = receiver.recv().await.unwrap();
                assert_eq!(0, x);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn non_copy_shared() {
        let (sender, receiver) = new_bounded(1);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                let string1 = "Some string data here..".to_string();
                sender.send(string1).await.unwrap();
                let string2 = "different data..".to_string();
                sender.send(string2).await.unwrap();
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let x = receiver.recv().await.unwrap();
                assert_eq!(x, "Some string data here..".to_string());
                let y = receiver.recv().await.unwrap();
                assert_eq!(y, "different data..".to_string());
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[test]
    fn copy_shared() {
        let (sender, receiver) = new_bounded(2);

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                sender.send(100usize).await.unwrap();
                sender.send(200usize).await.unwrap();
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let x = receiver.recv().await.unwrap();
                let y = receiver.recv().await.unwrap();
                assert_eq!(x, 100usize);
                assert_eq!(y, 200usize);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }

    #[derive(Debug)]
    struct WithDrop(Arc<AtomicUsize>, usize);

    impl Drop for WithDrop {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn shared_drop_gets_called() {
        let (sender, receiver) = new_bounded(1000);

        let original = Arc::new(AtomicUsize::new(0));
        let send_count = original.clone();
        let drop_count = original.clone();

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                for x in 0..1000 {
                    let val = WithDrop(send_count.clone(), x);
                    drop_count.fetch_add(1, Ordering::Relaxed);
                    let _ = sender.send(val).await;
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let y = receiver.recv().await.unwrap();
                drop(y);
                Timer::new(Duration::from_secs(1)).await;
                let y = receiver.recv().await.unwrap();
                drop(y);
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();

        // make sure that our total is always 0, to ensure we have dropped all entries,
        // despite differing conditions.
        assert_eq!(original.load(Ordering::Relaxed), 0usize);
    }

    #[test]
    fn shared_drop_gets_called_reversed() {
        let (sender, receiver) = new_bounded(100);

        let original = Arc::new(AtomicUsize::new(0));
        let send_count = original.clone();
        let drop_count = original.clone();

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                for x in 0..110 {
                    let val = WithDrop(send_count.clone(), x);
                    drop_count.fetch_add(1, Ordering::Relaxed);
                    let _ = sender.send(val).await;
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let y = receiver.recv().await.unwrap();
                drop(y);
                let y = receiver.recv().await.unwrap();
                drop(y);
            })
            .unwrap();

        ex2.join().unwrap();
        ex1.join().unwrap();

        // make sure that our total is always 0, to ensure we have dropped all entries,
        // despite differing conditions.
        assert_eq!(original.load(Ordering::Relaxed), 0usize);
    }

    #[test]
    fn shared_drop_cascade_drop_executor() {
        let (sender, receiver) = new_bounded(100);

        let original = Arc::new(AtomicUsize::new(0));
        let send_count = original.clone();
        let drop_count = original.clone();

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                for x in 0..50 {
                    let val = WithDrop(send_count.clone(), x);
                    drop_count.fetch_add(1, Ordering::Relaxed);
                    let _ = sender.send(val).await;
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let _resp = receiver.recv().await.unwrap();
            })
            .unwrap();
        ex2.join().unwrap();
        ex1.join().unwrap();

        // make sure that our total is always 0, to ensure we have dropped all entries,
        // despite differing conditions.
        assert_eq!(original.load(Ordering::Relaxed), 0usize);
    }

    #[test]
    fn shared_drop_cascade_drop_executor_reverse() {
        let (sender, receiver) = new_bounded(100);

        let original = Arc::new(AtomicUsize::new(0));
        let send_count = original.clone();
        let drop_count = original.clone();

        let ex1 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let sender = sender.connect().await;
                for x in 0..50 {
                    let val = WithDrop(send_count.clone(), x);
                    drop_count.fetch_add(1, Ordering::SeqCst);
                    let _ = sender.send(val).await;
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                for x in 0..50 {
                    let resp = receiver.recv().await.unwrap();
                    assert_eq!(x, resp.1);
                }
            })
            .unwrap();

        drop(ex1);
        ex2.join().unwrap();

        // make sure that our total is always 0, to ensure we have dropped all entries,
        // despite differing conditions.
        assert_eq!(original.load(Ordering::Relaxed), 0usize);
    }

    #[test]
    fn close_sender_while_blocked_on_send() {
        use std::sync::{Condvar, Mutex};

        let (sender, receiver) = new_bounded(10);
        let cv_mtx_1 = Arc::new((Condvar::new(), Mutex::new(0)));
        let cv_mtx_2 = Arc::clone(&cv_mtx_1);
        let cv_mtx_3 = Arc::clone(&cv_mtx_1);

        let ex1 = LocalExecutorBuilder::default()
            .spawn({
                move || async move {
                    let s1 = Rc::new(sender.connect().await);
                    let s2 = Rc::clone(&s1);
                    let t1 = crate::executor().spawn_local(async move {
                        let mut ii = 0;
                        while s1.try_send(ii).is_ok() {
                            ii += 1;
                        }
                        s1.close();
                        *cv_mtx_1.1.lock().unwrap() = 1;
                        cv_mtx_1.0.notify_all();
                    });
                    let t2 = crate::executor().spawn_local(async move {
                        let mut lck = cv_mtx_2
                            .0
                            .wait_while(cv_mtx_2.1.lock().unwrap(), |l| *l < 1)
                            .unwrap();
                        assert!(s2.send(-1).await.is_err());
                        *lck = 2;
                        cv_mtx_2.0.notify_all();
                    });
                    t1.await;
                    t2.await;
                }
            })
            .unwrap();

        let ex2 = LocalExecutorBuilder::default()
            .spawn(move || async move {
                let receiver = receiver.connect().await;
                let _lck = cv_mtx_3
                    .0
                    .wait_while(cv_mtx_3.1.lock().unwrap(), |l| *l < 2)
                    .unwrap();
                while let Some(v) = receiver.recv().await {
                    assert!(0 <= v);
                }
            })
            .unwrap();

        ex1.join().unwrap();
        ex2.join().unwrap();
    }
}
