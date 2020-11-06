// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::channels::ChannelCapacity;
use futures_lite::future;
use futures_lite::stream::Stream;
use futures_lite::FutureExt;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[derive(Debug)]
/// This is the error that is returned from channel operations if something goes wrong.
///
/// It encapsulates an [`ErrorKind`], which will be different for each kind of error and
/// also makes the item sent through the channel available through the public attribute `item`.
///
/// You can convert between this and [`io::Error`] (losing information on `item`) so the
/// ? constructs around [`io::Error`] should all work. Another possible way of doing this
/// is returning an [`io::Error`] and encapsulating `item` on its inner field. However that
/// adds [`Send`] and [`Sync`] requirements plus an allocation to the inner field (or to a
/// helper struct around the inner field) that we'd like to avoid.
///
/// [`ErrorKind`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html
/// [`io::Error`]: https://doc.rust-lang.org/std/io/struct.Error.html
/// [`Send`]: https://doc.rust-lang.org/std/marker/trait.Send.html
/// [`Sync`]: https://doc.rust-lang.org/std/marker/trait.Sync.html
pub struct ChannelError<T> {
    kind: io::ErrorKind,
    /// The `ChannelError` encapsulates the item we originally tried to send into the channel
    /// in case you need to do something with it upon failure.
    pub item: T,
}

impl<T> ChannelError<T> {
    fn new(kind: io::ErrorKind, item: T) -> ChannelError<T> {
        ChannelError { kind, item }
    }
}

impl<T: std::fmt::Debug> From<ChannelError<T>> for io::Error {
    fn from(error: ChannelError<T>) -> Self {
        io::Error::new(error.kind, format!("item: {:?}", error.item))
    }
}

#[derive(Debug)]
/// Send endpoint to the `local_channel`
pub struct LocalSender<T> {
    channel: LocalChannel<T>,
}

#[derive(Debug)]
/// Receive endpoint to the `local_channel`
///
/// The `LocalReceiver` provides an interface compatible with [`StreamExt`] and will
/// keep yielding elements until the sender is destroyed.
///
/// # Examples
///
/// ```
/// use glommio::{LocalExecutor, Local};
/// use glommio::channels::local_channel;
/// use futures_lite::StreamExt;
///
/// let ex = LocalExecutor::make_default();
/// ex.run(async move {
///     let (sender, mut receiver) = local_channel::new_unbounded();
///
///     let h = Local::local(async move {
///         let sum = receiver.fold(0, |acc, x| acc + x).await;
///         assert_eq!(sum, 45);
///     }).detach();
///
///     for i in 0..10 {
///         sender.try_send(i);
///     }
///     drop(sender);
///     h.await;
/// });
/// ```
///
/// [`StreamExt`]: https://docs.rs/futures/0.3.6/futures/stream/trait.StreamExt.html
pub struct LocalReceiver<T> {
    channel: LocalChannel<T>,
}

#[derive(Debug)]
struct State<T> {
    capacity: ChannelCapacity,
    channel: VecDeque<T>,
    recv_waiters: Option<VecDeque<Waker>>,
    send_waiters: Option<VecDeque<Waker>>,
}

#[derive(Debug, Clone)]
struct LocalChannel<T> {
    state: Rc<RefCell<State<T>>>,
}

impl<T> LocalChannel<T> {
    #[allow(clippy::new_ret_no_self)]
    fn new(capacity: ChannelCapacity) -> (LocalSender<T>, LocalReceiver<T>) {
        let channel = match capacity {
            ChannelCapacity::Unbounded => VecDeque::new(),
            ChannelCapacity::Bounded(x) => VecDeque::with_capacity(x),
        };

        let state = Rc::new(RefCell::new(State {
            capacity,
            channel,
            send_waiters: Some(VecDeque::new()),
            recv_waiters: Some(VecDeque::new()),
        }));

        (
            LocalSender {
                channel: LocalChannel {
                    state: state.clone(),
                },
            },
            LocalReceiver {
                channel: LocalChannel { state },
            },
        )
    }

    fn push(&self, item: T) -> Result<(), ChannelError<T>> {
        let mut state = self.state.borrow_mut();
        if state.recv_waiters.is_none() {
            return Err(ChannelError::new(io::ErrorKind::BrokenPipe, item));
        }

        if let ChannelCapacity::Bounded(x) = state.capacity {
            if state.channel.len() >= x {
                return Err(ChannelError::new(io::ErrorKind::WouldBlock, item));
            }
        }

        state.channel.push_back(item);
        if let Some(w) = state.recv_waiters.as_mut().and_then(|x| x.pop_front()) {
            drop(state);
            w.wake();
        }
        Ok(())
    }

    fn is_full(&self) -> bool {
        let state = self.state.borrow();
        match state.capacity {
            ChannelCapacity::Unbounded => false,
            ChannelCapacity::Bounded(x) => state.channel.len() >= x,
        }
    }
}

/// Creates a new `local_channel` with unbounded capacity
///
/// # Examples
/// ```
/// use glommio::{LocalExecutor, Local};
/// use glommio::channels::local_channel;
/// use futures_lite::StreamExt;
///
/// let ex = LocalExecutor::make_default();
/// ex.run(async move {
///     let (sender, mut receiver) = local_channel::new_unbounded();
///     let h = Local::local(async move {
///         assert_eq!(receiver.next().await.unwrap(), 0);
///     }).detach();
///     sender.try_send(0);
///     drop(sender);
///     h.await;
/// });
/// ```
pub fn new_unbounded<T>() -> (LocalSender<T>, LocalReceiver<T>) {
    LocalChannel::new(ChannelCapacity::Unbounded)
}

/// Creates a new `local_channel` with capacity limited to the `size` argument
///
/// # Examples
/// ```
/// use glommio::{LocalExecutor, Local};
/// use glommio::channels::local_channel;
/// use futures_lite::StreamExt;
///
/// let ex = LocalExecutor::make_default();
/// ex.run(async move {
///     let (sender, mut receiver) = local_channel::new_bounded(1);
///     assert_eq!(sender.is_full(), false);
///     sender.try_send(0);
///     assert_eq!(sender.is_full(), true);
///     receiver.next().await.unwrap();
///     assert_eq!(sender.is_full(), false);
/// });
/// ```
pub fn new_bounded<T>(size: usize) -> (LocalSender<T>, LocalReceiver<T>) {
    LocalChannel::new(ChannelCapacity::Bounded(size))
}

#[allow(clippy::len_without_is_empty)]
impl<T> LocalSender<T> {
    /// Sends data into this channel.
    ///
    /// It returns a [`ChannelError`] encapsulating a [`BrokenPipe`] if the receiver is destroyed.
    /// It returns a [`ChannelError`] encapsulating a [`WouldBlock`] if this is a bounded channel that has no more capacity
    ///
    /// # Examples
    /// ```
    /// use glommio::{LocalExecutor, Local};
    /// use glommio::channels::local_channel;
    /// use futures_lite::StreamExt;
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async move {
    ///     let (sender, mut receiver) = local_channel::new_bounded(1);
    ///     sender.try_send(0);
    ///     sender.try_send(0).unwrap_err(); // no more capacity
    ///     receiver.next().await.unwrap(); // now we have capacity again
    ///     drop(receiver); // but because the receiver is destroyed send will err
    ///     sender.try_send(0).unwrap_err();
    /// });
    /// ```
    ///
    /// [`BrokenPipe`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.BrokenPipe
    /// [`WouldBlock`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.WouldBlock
    /// [`Other`]: https://doc.rust-lang.org/std/io/enum.ErrorKind.html#variant.Other
    /// [`io::Error`]: https://doc.rust-lang.org/std/io/struct.Error.html
    pub fn try_send(&self, item: T) -> Result<(), ChannelError<T>> {
        self.channel.push(item)
    }

    /// Sends data into this channel when it is ready to receive it
    ///
    /// For an unbounded channel this is just a more expensive version of [`send`]. Prefer
    /// to use [`send`] instead.
    ///
    /// For a bounded channel this will push to the channel when the channel is ready to
    /// receive data.
    ///
    /// # Examples
    /// ```
    /// use glommio::{LocalExecutor, Local};
    /// use glommio::channels::local_channel;
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async move {
    ///     let (sender, receiver) = local_channel::new_bounded(1);
    ///     sender.send(0).await.unwrap();
    ///     drop(receiver);
    /// });
    /// ```
    ///
    /// [`send`]: struct.LocalSender.html#method.send
    pub async fn send(&self, item: T) -> Result<(), ChannelError<T>> {
        if !self.is_full() {
            self.try_send(item)
        } else {
            let waiter = future::poll_fn(|cx| self.wait_for_room(cx));
            waiter.await;
            self.try_send(item)
        }
    }

    /// Checks if there is room to send data in this channel
    ///
    /// # Examples
    /// ```
    /// use glommio::{LocalExecutor, Local};
    /// use glommio::channels::local_channel;
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async move {
    ///     let (sender, receiver) = local_channel::new_bounded(1);
    ///     assert_eq!(sender.is_full(), false);
    ///     sender.try_send(0);
    ///     assert_eq!(sender.is_full(), true);
    ///     drop(receiver);
    /// });
    /// ```
    pub fn is_full(&self) -> bool {
        self.channel.is_full()
    }

    /// Returns the number of items still queued in this channel
    ///
    /// # Examples
    /// ```
    /// use glommio::{LocalExecutor, Local};
    /// use glommio::channels::local_channel;
    /// use futures_lite::StreamExt;
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async move {
    ///     let (sender, mut receiver) = local_channel::new_unbounded();
    ///     sender.try_send(0);
    ///     sender.try_send(0);
    ///     assert_eq!(sender.len(), 2);
    ///     receiver.next().await.unwrap();
    ///     assert_eq!(sender.len(), 1);
    ///     drop(receiver);
    /// });
    /// ```
    pub fn len(&self) -> usize {
        let state = self.channel.state.borrow();
        state.channel.len()
    }

    fn wait_for_room(&self, cx: &mut Context<'_>) -> Poll<()> {
        if !self.channel.is_full() {
            Poll::Ready(())
        } else {
            let mut state = self.channel.state.borrow_mut();
            state
                .send_waiters
                .as_mut()
                .unwrap()
                .push_back(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl<T> Drop for LocalSender<T> {
    fn drop(&mut self) {
        let mut state = self.channel.state.borrow_mut();
        // Will not wake up senders, but we are dropping the sender so nobody
        // wants to wake up anyway.
        state.send_waiters.take();

        if state.recv_waiters.is_none() {
            return;
        }
        let waiters = state.recv_waiters.replace(VecDeque::new()).unwrap();
        drop(state);

        for w in waiters {
            w.wake();
        }
    }
}

impl<T> Drop for LocalReceiver<T> {
    fn drop(&mut self) {
        let mut state = self.channel.state.borrow_mut();
        state.recv_waiters.take();

        if state.send_waiters.is_none() {
            return;
        }

        let waiters = state.send_waiters.replace(VecDeque::new()).unwrap();
        drop(state);

        for w in waiters {
            w.wake();
        }
    }
}

impl<T> Stream for LocalReceiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut waiter = future::poll_fn(|cx| self.recv_one(cx));
        waiter.poll(cx)
    }
}

impl<T> LocalReceiver<T> {
    /// Receives data from this channel
    ///
    /// If the sender is no longer available it returns [`None`]. Otherwise block until
    /// an item is available and returns it wrapped in [`Some`]
    ///
    /// Notice that this is also available as a Stream. Whether to consume from a stream
    /// or `recv` is up to the application. The biggest difference is that [`StreamExt`]'s
    /// [`next`] method takes a mutable reference to self. If the LocalReceiver is, say,
    /// behind an [`Rc`] it may be more ergonomic to recv.
    ///
    /// # Examples
    /// ```
    /// use glommio::{LocalExecutor, Local};
    /// use glommio::channels::local_channel;
    ///
    /// let ex = LocalExecutor::make_default();
    /// ex.run(async move {
    ///     let (sender, receiver) = local_channel::new_bounded(1);
    ///     sender.send(0).await.unwrap();
    ///     let x = receiver.recv().await.unwrap();
    ///     assert_eq!(x, 0);
    /// });
    /// ```
    ///
    /// [`None`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.None
    /// [`Some`]: https://doc.rust-lang.org/std/option/enum.Option.html#variant.Some
    /// [`StreamExt`]: https://docs.rs/futures-lite/1.11.2/futures_lite/stream/index.html
    /// [`Rc`]: https://doc.rust-lang.org/std/rc/struct.Rc.html
    pub async fn recv(&self) -> Option<T> {
        let waiter = future::poll_fn(|cx| self.recv_one(cx));
        waiter.await
    }

    fn recv_one(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let mut state = self.channel.state.borrow_mut();
        match state.channel.pop_front() {
            Some(item) => {
                if let Some(w) = state.send_waiters.as_mut().and_then(|x| x.pop_front()) {
                    drop(state);
                    w.wake();
                }
                Poll::Ready(Some(item))
            }
            None => match state.send_waiters.is_some() {
                true => {
                    state
                        .recv_waiters
                        .as_mut()
                        .unwrap()
                        .push_back(cx.waker().clone());
                    Poll::Pending
                }
                false => Poll::Ready(None),
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{enclose, Local};
    use futures_lite::stream::{self, StreamExt};

    #[test]
    fn producer_consumer() {
        test_executor!(async move {
            let (sender, receiver) = new_unbounded();

            let handle = Local::local(async move {
                let sum = receiver.fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 10);
            })
            .detach();

            for _ in 0..10 {
                sender.try_send(1).unwrap();
            }
            drop(sender);
            handle.await;
        });
    }

    #[test]
    fn producer_parallel_consumer() {
        use futures::future;
        use futures::stream::StreamExt;

        test_executor!(async move {
            let (sender, receiver) = new_unbounded();

            let handle = Local::local(async move {
                let mut sum = 0;
                receiver
                    .for_each_concurrent(1000, |x| {
                        sum += x;
                        future::ready(())
                    })
                    .await;
                assert_eq!(sum, 10);
            })
            .detach();

            for _ in 0..10 {
                sender.try_send(1).unwrap();
            }
            drop(sender);

            handle.await;
        });
    }

    #[test]
    fn producer_receiver_serialized() {
        test_executor!(async move {
            let (sender, receiver) = new_unbounded();

            for _ in 0..10 {
                sender.try_send(1).unwrap();
            }
            drop(sender);

            let handle = Local::local(async move {
                let sum = receiver.fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 10);
            })
            .detach();

            handle.await;
        });
    }

    #[test]
    fn producer_early_drop_receiver() {
        test_executor!(async move {
            let (sender, receiver) = new_unbounded();

            let handle = Local::local(async move {
                let sum = receiver.take(3).fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 3);
            })
            .detach();

            loop {
                match sender.try_send(1) {
                    Ok(_) => Local::later().await,
                    Err(x) => {
                        assert_eq!(x.item, 1);
                        assert_eq!(x.kind, io::ErrorKind::BrokenPipe);
                        break;
                    }
                }
            }
            handle.await;
        });
    }

    #[test]
    fn producer_bounded_early_error() {
        test_executor!(async move {
            let (sender, receiver) = new_bounded(1);
            sender.try_send(0).unwrap();
            sender.try_send(0).unwrap_err();
            drop(receiver);
        });
    }

    #[test]
    fn producer_bounded_has_capacity() {
        test_executor!(async move {
            let (sender, receiver) = new_bounded(1);
            assert_eq!(sender.is_full(), false);
            sender.try_send(0).unwrap();
            assert_eq!(sender.is_full(), true);
            drop(receiver);
        });
    }

    #[test]
    fn producer_bounded_ping_pong() {
        test_executor!(async move {
            let (sender, receiver) = new_bounded(1);

            let handle = Local::local(async move {
                let sum = receiver.fold(0, |acc, x| acc + x).await;
                assert_eq!(sum, 10);
            })
            .detach();

            for _ in 0..10 {
                sender.send(1).await.unwrap();
            }
            drop(sender);
            handle.await;
        });
    }

    #[test]
    fn producer_bounded_previously_blocked_still_errors_out() {
        test_executor!(async move {
            let (sender, mut receiver) = new_bounded(1);

            let s = Local::local(async move {
                sender.try_send(0).unwrap();
                sender.send(0).await.unwrap_err();
            })
            .detach();

            let r = Local::local(async move {
                receiver.next().await.unwrap();
                drop(receiver);
            })
            .detach();
            r.await;
            s.await;
        });
    }

    #[test]
    fn non_stream_receiver() {
        test_executor!(async move {
            let (sender, receiver) = new_bounded(1);

            let s = Local::local(async move {
                sender.try_send(0).unwrap();
                sender.send(0).await.unwrap_err();
            })
            .detach();

            let r = Local::local(async move {
                receiver.recv().await.unwrap();
                drop(receiver);
            })
            .detach();
            r.await;
            s.await;
        });
    }

    #[test]
    fn multiple_task_receivers() {
        test_executor!(async move {
            let (sender, receiver) = new_bounded(1);
            let receiver = Rc::new(receiver);

            let mut ret = Vec::new();
            for _ in 0..10 {
                ret.push(Local::local(enclose! {(receiver) async move {
                    receiver.recv().await.unwrap();
                }}));
            }

            for _ in 0..10 {
                sender.send(0).await.unwrap();
            }

            let recvd = stream::iter(ret).then(|f| f).count().await;
            assert_eq!(recvd, 10);
        });
    }
}
