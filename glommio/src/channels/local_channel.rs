// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::channels::{ChannelCapacity, ChannelError};
use futures_lite::{future, stream::Stream};
use std::task::{Context, Poll, Waker};
use std::{cell::RefCell, io, pin::Pin, rc::Rc};

use intrusive_collections::intrusive_adapter;
use intrusive_collections::{LinkedList, LinkedListLink};

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
/// let ex = LocalExecutor::default();
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
struct WaiterNode {
    waker: Waker,
    link: LinkedListLink,
}

#[derive(Debug)]
struct ChannelNode<T> {
    value: T,
    link: LinkedListLink,
}

intrusive_adapter!(WaiterAdapter = Rc<WaiterNode> : WaiterNode {link : LinkedListLink});
intrusive_adapter!(ChannelAdapter<T> = Rc<ChannelNode<T>> : ChannelNode<T> {link : LinkedListLink});

#[derive(Debug)]
struct State<T> {
    capacity: ChannelCapacity,
    channel: LinkedList<ChannelAdapter<T>>,
    channel_len: usize,
    recv_waiters: Option<LinkedList<WaiterAdapter>>,
    send_waiters: Option<LinkedList<WaiterAdapter>>,
}

impl<T> State<T> {
    fn push(&mut self, item: T) -> Result<Option<Waker>, ChannelError<T>> {
        if self.recv_waiters.is_none() {
            Err(ChannelError::new(io::ErrorKind::BrokenPipe, item))
        } else if self.is_full() {
            Err(ChannelError::new(io::ErrorKind::WouldBlock, item))
        } else {
            self.channel.push_back(Rc::new(ChannelNode {
                value: item,
                link: LinkedListLink::new(),
            }));
            self.channel_len += 1;

            Ok(self.recv_waiters.as_mut().and_then(|x| {
                x.pop_front().map(|n| {
                    Rc::try_unwrap(n)
                        .expect("Wake is shared between several containers")
                        .waker
                })
            }))
        }
    }

    fn is_full(&self) -> bool {
        match self.capacity {
            ChannelCapacity::Unbounded => false,
            ChannelCapacity::Bounded(x) => self.channel_len >= x,
        }
    }

    fn wait_for_room(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if !self.is_full() {
            Poll::Ready(())
        } else {
            self.send_waiters
                .as_mut()
                .unwrap()
                .push_back(Rc::new(WaiterNode {
                    waker: cx.waker().clone(),
                    link: LinkedListLink::new(),
                }));
            Poll::Pending
        }
    }

    fn recv_one(&mut self, cx: &mut Context<'_>) -> Poll<Option<(T, Option<Waker>)>> {
        match self.channel.pop_front() {
            Some(item) => {
                self.channel_len -= 1;

                Poll::Ready(Some((
                    Rc::try_unwrap(item).ok().unwrap().value,
                    self.send_waiters.as_mut().and_then(|x| {
                        x.pop_front().map(|node| {
                            Rc::try_unwrap(node)
                                .expect("Waker is stored in several containers")
                                .waker
                        })
                    }),
                )))
            }
            None => match self.send_waiters.is_some() {
                true => {
                    self.recv_waiters
                        .as_mut()
                        .unwrap()
                        .push_back(Rc::new(WaiterNode {
                            waker: cx.waker().clone(),
                            link: LinkedListLink::new(),
                        }));
                    Poll::Pending
                }
                false => Poll::Ready(None),
            },
        }
    }
}

#[derive(Debug)]
struct LocalChannel<T> {
    state: Rc<RefCell<State<T>>>,
}

impl<T> Clone for LocalChannel<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> LocalChannel<T> {
    #[allow(clippy::new_ret_no_self)]
    fn new(capacity: ChannelCapacity) -> (LocalSender<T>, LocalReceiver<T>) {
        let channel = LinkedList::new(ChannelAdapter::new());

        let channel = LocalChannel {
            state: Rc::new(RefCell::new(State {
                capacity,
                channel,
                channel_len: 0,
                send_waiters: Some(LinkedList::new(WaiterAdapter::NEW)),
                recv_waiters: Some(LinkedList::new(WaiterAdapter::NEW)),
            })),
        };

        (
            LocalSender {
                channel: channel.clone(),
            },
            LocalReceiver { channel },
        )
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
/// let ex = LocalExecutor::default();
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
/// let ex = LocalExecutor::default();
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
    /// let ex = LocalExecutor::default();
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
        if let Some(w) = self.channel.state.borrow_mut().push(item)? {
            w.wake();
        }
        Ok(())
    }

    /// Sends data into this channel when it is ready to receive it
    ///
    /// For an unbounded channel this is just a more expensive version of [`try_send`]. Prefer
    /// to use [`try_send`] instead.
    ///
    /// For a bounded channel this will push to the channel when the channel is ready to
    /// receive data.
    ///
    /// # Examples
    /// ```
    /// use glommio::{LocalExecutor, Local};
    /// use glommio::channels::local_channel;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let (sender, receiver) = local_channel::new_bounded(1);
    ///     sender.send(0).await.unwrap();
    ///     drop(receiver);
    /// });
    /// ```
    ///
    /// [`try_send`]: struct.LocalSender.html#method.try_send
    pub async fn send(&self, item: T) -> Result<(), ChannelError<T>> {
        future::poll_fn(|cx| self.wait_for_room(cx)).await;
        self.try_send(item)
    }

    /// Checks if there is room to send data in this channel
    ///
    /// # Examples
    /// ```
    /// use glommio::{LocalExecutor, Local};
    /// use glommio::channels::local_channel;
    ///
    /// let ex = LocalExecutor::default();
    /// ex.run(async move {
    ///     let (sender, receiver) = local_channel::new_bounded(1);
    ///     assert_eq!(sender.is_full(), false);
    ///     sender.try_send(0);
    ///     assert_eq!(sender.is_full(), true);
    ///     drop(receiver);
    /// });
    /// ```
    #[inline]
    pub fn is_full(&self) -> bool {
        self.channel.state.borrow().is_full()
    }

    /// Returns the number of items still queued in this channel
    ///
    /// # Examples
    /// ```
    /// use glommio::{LocalExecutor, Local};
    /// use glommio::channels::local_channel;
    /// use futures_lite::StreamExt;
    ///
    /// let ex = LocalExecutor::default();
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
    #[inline]
    pub fn len(&self) -> usize {
        self.channel.state.borrow().channel_len
    }

    fn wait_for_room(&self, cx: &mut Context<'_>) -> Poll<()> {
        // NOTE: it is important that the borrow is dropped
        // if Poll::Pending is returned
        self.channel.state.borrow_mut().wait_for_room(cx)
    }
}

fn wake_up_all(ws: &mut Option<LinkedList<WaiterAdapter>>) {
    if let Some(ref mut waiters) = ws {
        // we assume here that wakes don't try to acquire a borrow on
        // the channel.state
        for w in core::mem::take(waiters) {
            Rc::try_unwrap(w)
                .expect("Waker is stored in several containers")
                .waker
                .wake();
        }
    }
}

impl<T> Drop for LocalSender<T> {
    fn drop(&mut self) {
        let mut state = self.channel.state.borrow_mut();
        // Will not wake up senders, but we are dropping the sender so nobody
        // wants to wake up anyway.
        state.send_waiters.take();
        wake_up_all(&mut state.recv_waiters);
    }
}

impl<T> Drop for LocalReceiver<T> {
    fn drop(&mut self) {
        let mut state = self.channel.state.borrow_mut();
        state.recv_waiters.take();
        wake_up_all(&mut state.send_waiters);
    }
}

impl<T> Stream for LocalReceiver<T> {
    type Item = T;

    #[inline]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.recv_one(cx)
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
    /// let ex = LocalExecutor::default();
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
    /// [`next`]: https://docs.rs/futures-lite/1.11.2/futures_lite/stream/trait.StreamExt.html#method.next
    /// [`Rc`]: https://doc.rust-lang.org/std/rc/struct.Rc.html
    pub async fn recv(&self) -> Option<T> {
        future::poll_fn(|cx| self.recv_one(cx)).await
    }

    fn recv_one(&self, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.channel.state.borrow_mut().recv_one(cx).map(|opt| {
            opt.map(|(ret, mw)| {
                if let Some(w) = mw {
                    w.wake();
                }
                ret
            })
        })
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
