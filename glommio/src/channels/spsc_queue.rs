use std::{
    cell::UnsafeCell,
    fmt,
    marker::PhantomData,
    mem::MaybeUninit,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Debug)]
#[repr(align(128))]
struct ProducerCacheline {
    /// Index position of current tail
    tail: AtomicUsize,
    limit: AtomicUsize,
    /// Id == 0 : never connected
    /// Id == usize::MAX: disconnected
    consumer_id: AtomicUsize,
}

#[derive(Debug)]
#[repr(align(128))]
struct ConsumerCacheline {
    /// Index position of the current head
    head: AtomicUsize,
    /// Id == 0 : never connected
    /// Id == usize::MAX: disconnected
    producer_id: AtomicUsize,
}

#[derive(Debug)]
struct Slot<T> {
    value: UnsafeCell<MaybeUninit<T>>,
    has_value: AtomicBool,
}

unsafe impl<T: Send> Sync for Slot<T> {}

/// The internal memory buffer used by the queue.
///
/// `Buffer` holds a pointer to allocated memory which represents the bounded
/// ring buffer, as well as a head and tail `AtomicUsize` which the producer and
/// consumer use to track location in the ring.
#[repr(C)]
pub(crate) struct Buffer<T> {
    buffer_storage: Box<[Slot<T>]>,
    capacity: usize,
    mask: usize,
    lookahead: usize,

    pcache: ProducerCacheline,
    ccache: ConsumerCacheline,

    _marker: PhantomData<T>,
}

impl<T> fmt::Debug for Buffer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let head = self.ccache.head.load(Ordering::Relaxed);
        let tail = self.pcache.tail.load(Ordering::Relaxed);
        let limit = self.pcache.limit.load(Ordering::Relaxed);
        let id_to_str = |id| match id {
            0 => "not connected".into(),
            usize::MAX => "disconnected".into(),
            x => format!("{x}"),
        };

        let consumer_id = id_to_str(self.pcache.consumer_id.load(Ordering::Relaxed));
        let producer_id = id_to_str(self.ccache.producer_id.load(Ordering::Relaxed));

        f.debug_struct("SPSC Buffer")
            .field("capacity:", &self.capacity)
            .field("consumer_head:", &head)
            .field("producer_tail:", &tail)
            .field("lookahead_limit:", &limit)
            .field("consumer_id:", &consumer_id)
            .field("producer_id:", &producer_id)
            .finish()
    }
}
unsafe impl<T: Sync> Sync for Buffer<T> {}

/// A handle to the queue which allows consuming values from the buffer.
///
/// # SPSC Guarantee
///
/// This type does NOT implement `Clone`. The queue is designed as a
/// Single-Producer-Single-Consumer (SPSC) queue, and allowing multiple
/// consumers would violate memory safety guarantees.
///
/// Cloning the consumer would allow multiple threads to concurrently call
/// `try_pop()`, which uses `Ordering::Relaxed` and assumes exclusive access.
/// This can lead to:
/// - Multiple consumers reading the same value from the queue
/// - Double-free when both consumers drop the same value
/// - Heap corruption and undefined behavior
///
/// See issue #700 for details.
pub struct Consumer<T> {
    pub(crate) buffer: Arc<Buffer<T>>,
}

impl<T> Consumer<T> {
    /// Internal cloning method for use within glommio.
    ///
    /// # Safety
    ///
    /// This is intentionally NOT exposed via the Clone trait to prevent
    /// external code from creating multiple consumers, which would violate
    /// SPSC guarantees and cause memory corruption.
    ///
    /// Internal use within glommio (e.g., shared_channel) must ensure that
    /// only one consumer is actually active at any time, even if multiple
    /// Consumer handles exist temporarily during handoff.
    pub(crate) fn clone_internal(&self) -> Self {
        Consumer {
            buffer: self.buffer.clone(),
        }
    }
}

/// A handle to the queue which allows adding values onto the buffer.
///
/// # SPSC Guarantee
///
/// This type does NOT implement `Clone`. The queue is designed as a
/// Single-Producer-Single-Consumer (SPSC) queue, and allowing multiple
/// producers would violate memory safety guarantees.
///
/// Cloning the producer would allow multiple threads to concurrently call
/// `try_push()`, which uses `Ordering::Relaxed` and assumes exclusive access.
/// This can lead to:
/// - Multiple producers writing to the same slot
/// - Values being overwritten without being dropped (memory leak)
/// - Lost updates and data corruption
///
/// See issue #700 for details.
pub struct Producer<T> {
    pub(crate) buffer: Arc<Buffer<T>>,
}

impl<T> Producer<T> {
    /// Internal cloning method for use within glommio.
    ///
    /// # Safety
    ///
    /// This is intentionally NOT exposed via the Clone trait to prevent
    /// external code from creating multiple producers, which would violate
    /// SPSC guarantees and cause memory corruption.
    ///
    /// Internal use within glommio (e.g., shared_channel) must ensure that
    /// only one producer is actually active at any time, even if multiple
    /// Producer handles exist temporarily during handoff.
    pub(crate) fn clone_internal(&self) -> Self {
        Producer {
            buffer: self.buffer.clone(),
        }
    }
}

impl<T> fmt::Debug for Consumer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Consumer {:?}", self.buffer)
    }
}

impl<T> fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Producer {:?}", self.buffer)
    }
}

impl<T> Buffer<T> {
    /// Attempt to pop a value off the buffer.
    ///
    /// If the buffer is empty, this method will not block. Instead, it will
    /// return `None` signifying the buffer was empty. The caller may then
    /// decide what to do next (e.g. spin-wait, sleep, process something
    /// else, etc.)
    fn try_pop(&self) -> Option<T> {
        let head = self.ccache.head.load(Ordering::Relaxed);
        let slot = &self.buffer_storage[head & self.mask];
        if !slot.has_value.load(Ordering::Acquire) {
            return None;
        }
        let v = Some(unsafe { slot.value.get().read().assume_init() });
        slot.has_value.store(false, Ordering::Release);
        self.ccache
            .head
            .store(head.wrapping_add(1), Ordering::Relaxed);
        v
    }

    /// Attempt to push a value onto the buffer.
    ///
    /// If the buffer is full, this method will not block.  Instead, it will
    /// return `Some(v)`, where `v` was the value attempting to be pushed
    /// onto the buffer.  If the value was successfully pushed onto the
    /// buffer, `None` will be returned signifying success.
    fn try_push(&self, v: T) -> Option<T> {
        if self.consumer_disconnected() {
            return Some(v);
        }

        let tail = self.pcache.tail.load(Ordering::Relaxed);
        let limit = self.pcache.limit.load(Ordering::Relaxed);

        if tail == limit {
            let idx = tail.wrapping_add(self.lookahead);
            let slot = &self.buffer_storage[idx & self.mask];
            if !slot.has_value.load(Ordering::Acquire) {
                self.pcache.limit.store(idx, Ordering::Relaxed);
            } else {
                let slot: &Slot<T> = &self.buffer_storage[tail & self.mask];
                if slot.has_value.load(Ordering::Acquire) {
                    return Some(v);
                }
                self.pcache
                    .limit
                    .store(tail.wrapping_add(1), Ordering::Relaxed);
            }
        }

        let slot = &self.buffer_storage[tail & self.mask];
        unsafe {
            slot.value.get().write(MaybeUninit::new(v));
        }
        slot.has_value.store(true, Ordering::Release);
        self.pcache
            .tail
            .store(tail.wrapping_add(1), Ordering::Relaxed);
        None
    }

    /// Disconnects the consumer, and returns whether it was already
    /// disconnected
    pub(crate) fn disconnect_consumer(&self) -> bool {
        self.pcache.consumer_id.swap(usize::MAX, Ordering::Release) == usize::MAX
    }

    /// Disconnects the producer, and returns whether it was already
    /// disconnected
    pub(crate) fn disconnect_producer(&self) -> bool {
        self.ccache.producer_id.swap(usize::MAX, Ordering::Release) == usize::MAX
    }

    /// Returns whether the producer is disconnected.
    pub(crate) fn producer_disconnected(&self) -> bool {
        self.ccache.producer_id.load(Ordering::Acquire) == usize::MAX
    }

    /// Returns whether the consumer is disconnected.
    pub(crate) fn consumer_disconnected(&self) -> bool {
        self.pcache.consumer_id.load(Ordering::Acquire) == usize::MAX
    }

    /// Returns the current size of the queue
    ///
    /// This value represents the current size of the queue.  This value can be
    /// from 0-`capacity` inclusive.
    pub(crate) fn size(&self) -> usize {
        std::cmp::min(
            self.capacity,
            self.pcache
                .tail
                .load(Ordering::Acquire)
                .saturating_sub(self.ccache.head.load(Ordering::Acquire)),
        )
    }
}

/// Handles deallocation of heap memory when the buffer is dropped
impl<T> Drop for Buffer<T> {
    fn drop(&mut self) {
        // Pop the rest of the values off the queue. By moving them into this scope,
        // we implicitly call their destructor
        while self.try_pop().is_some() {}
    }
}

/// Creates a new `spsc_queue` returning its producer and consumer
/// endpoints.
pub fn make<T: Send>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    inner_make(capacity, 0)
}

const MAX_LOOKAHEAD: usize = 1 << 12;

fn inner_make<T>(capacity: usize, initial_value: usize) -> (Producer<T>, Consumer<T>) {
    let capacity = capacity.next_power_of_two();
    let buffer_storage = allocate_buffer::<T>(capacity);
    let buf = Arc::new(Buffer {
        buffer_storage,
        capacity,
        mask: capacity - 1,
        lookahead: (capacity / 4).clamp(1, MAX_LOOKAHEAD),
        pcache: ProducerCacheline {
            tail: AtomicUsize::new(initial_value),
            limit: AtomicUsize::new(initial_value),
            consumer_id: AtomicUsize::new(0),
        },
        ccache: ConsumerCacheline {
            head: AtomicUsize::new(initial_value),
            producer_id: AtomicUsize::new(0),
        },
        _marker: PhantomData,
    });
    (
        Producer {
            buffer: buf.clone(),
        },
        Consumer { buffer: buf },
    )
}

fn allocate_buffer<T>(capacity: usize) -> Box<[Slot<T>]> {
    std::iter::repeat_with(|| Slot {
        value: UnsafeCell::new(MaybeUninit::uninit()),
        has_value: AtomicBool::new(false),
    })
    .take(capacity)
    .collect::<Vec<_>>()
    .into_boxed_slice()
}

pub(crate) trait BufferHalf {
    type Item;

    fn buffer(&self) -> &Buffer<Self::Item>;
    fn connect(&self, id: usize);
    fn peer_id(&self) -> usize;

    /// Returns the total capacity of this queue
    ///
    /// This value represents the total capacity of the queue when it is full.
    /// It does not represent the current usage.  For that, call `size()`.
    fn capacity(&self) -> usize {
        self.buffer().capacity
    }

    /// Returns the current size of the queue
    ///
    /// This value represents the current size of the queue.  This value can be
    /// from 0-`capacity` inclusive.
    fn size(&self) -> usize {
        self.buffer().size()
    }
}

impl<T> BufferHalf for Producer<T> {
    type Item = T;
    fn buffer(&self) -> &Buffer<T> {
        &self.buffer
    }

    fn connect(&self, id: usize) {
        assert_ne!(id, 0);
        assert_ne!(id, usize::MAX);
        self.buffer.ccache.producer_id.store(id, Ordering::Release);
    }

    fn peer_id(&self) -> usize {
        self.buffer.pcache.consumer_id.load(Ordering::Acquire)
    }
}

impl<T> Producer<T> {
    /// Attempt to push a value onto the buffer.
    ///
    /// This method does not block.  If the queue is not full, the value will be
    /// added to the queue and the method will return `None`, signifying
    /// success.  If the queue is full, this method will return `Some(v)``,
    /// where `v` is your original value.
    pub fn try_push(&self, v: T) -> Option<T> {
        (*self.buffer).try_push(v)
    }

    /// Disconnects the producer, signaling to the consumer that no new values
    /// are going to be produced.
    ///
    /// Returns the buffer status before the disconnect
    pub fn disconnect(&self) -> bool {
        (*self.buffer).disconnect_producer()
    }

    /// Whether the associated consumer is disconnected.
    pub fn consumer_disconnected(&self) -> bool {
        (*self.buffer).consumer_disconnected()
    }

    /// Whether the associated producer is disconnected.
    pub(crate) fn producer_disconnected(&self) -> bool {
        (*self.buffer).producer_disconnected()
    }

    /// Returns the available space in the queue
    ///
    /// This value represents the number of items that can be pushed onto the
    /// queue before it becomes full.
    pub fn free_space(&self) -> usize {
        self.capacity() - self.size()
    }
}

impl<T> BufferHalf for Consumer<T> {
    type Item = T;
    fn buffer(&self) -> &Buffer<T> {
        &self.buffer
    }

    fn connect(&self, id: usize) {
        assert_ne!(id, usize::MAX);
        assert_ne!(id, 0);
        self.buffer.pcache.consumer_id.store(id, Ordering::Release);
    }

    fn peer_id(&self) -> usize {
        self.buffer.ccache.producer_id.load(Ordering::Acquire)
    }
}

impl<T> Consumer<T> {
    /// Disconnects the consumer, signaling to the producer that no new values
    /// are going to be consumed. After this is done, any attempt on the
    /// producer to try_push should fail
    ///
    /// Returns the buffer status before the disconnect
    pub fn disconnect(&self) -> bool {
        (*self.buffer).disconnect_consumer()
    }

    /// Whether the associated producer is disconnected.
    pub fn producer_disconnected(&self) -> bool {
        (*self.buffer).producer_disconnected()
    }

    /// Attempt to pop a value off the queue.
    ///
    /// This method does not block.  If the queue is empty, the method will
    /// return `None`.  If there is a value available, the method will
    /// return `Some(v)`, where `v` is the value being popped off the queue.
    pub fn try_pop(&self) -> Option<T> {
        (*self.buffer).try_pop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_try_push() {
        let (p, _) = super::make(10);

        assert_eq!(p.capacity(), 16);

        for i in 0..16 {
            p.try_push(i);
            assert_eq!(p.size(), i + 1);
        }

        match p.try_push(16) {
            Some(v) => {
                assert_eq!(v, 16);
            }
            None => unreachable!("Queue should not have accepted another write!"),
        }
    }

    #[test]
    fn test_try_poll() {
        let (p, c) = super::make(10);

        if c.try_pop().is_some() {
            unreachable!("Queue was empty but a value was read!")
        }

        p.try_push(123);

        match c.try_pop() {
            Some(v) => assert_eq!(v, 123),
            None => unreachable!("Queue was not empty but poll() returned nothing!"),
        }

        if c.try_pop().is_some() {
            unreachable!("Queue was empty but a value was read!")
        }
    }

    #[test]
    fn test_threaded() {
        let (p, c) = super::make(500);

        thread::spawn(move || {
            for i in 0..100000 {
                loop {
                    if p.try_push(i).is_none() {
                        break;
                    }
                }
            }
        });

        for i in 0..100000 {
            loop {
                if let Some(t) = c.try_pop() {
                    assert!(t == i);
                    break;
                }
            }
        }
    }

    #[test]
    fn test_wrap() {
        let (p, c) = super::inner_make(10, usize::MAX - 1);

        for i in 0..10 {
            assert!(p.try_push(i).is_none());
        }

        for i in 0..10 {
            assert_eq!(c.try_pop(), Some(i));
        }
    }

    fn assert_send_sync<T: Send + Sync>() {}
    #[test]
    fn producer_consumer_are_send() {
        assert_send_sync::<Producer<u32>>();
        assert_send_sync::<Consumer<u32>>();
    }
}
