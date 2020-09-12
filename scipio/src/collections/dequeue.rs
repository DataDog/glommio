// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use crate::Semaphore;
use std::cell::RefCell;
use std::collections::VecDeque;
use std::io::Result;

/// The Deque is an async deque similar to the standard library's VecDeque.
///
/// The main difference is that when one pops from this queue (either
/// front or back), the queue will not return an option if it is empty,
/// but rather asynchronously wait for new elements to be produced.
#[derive(Debug)]
pub struct Deque<T> {
    deque: RefCell<VecDeque<T>>,
    sem: Semaphore,
}

impl<T> Deque<T> {
    /// Creates a new async Deque
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::collections::Deque;
    ///
    /// let _ : Deque<usize> = Deque::new();
    ///
    /// ```
    pub fn new() -> Self {
        Deque {
            deque: RefCell::new(VecDeque::new()),
            sem: Semaphore::new(0),
        }
    }

    /// Closes the current queue. All waiters will return Err(), and
    /// no new waiters are accepted.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::collections::Deque;
    ///
    /// let ad : Deque<usize> = Deque::new();
    /// ad.close();
    /// ```
    pub fn close(&self) {
        self.sem.close();
    }

    /// Creates a new queue with the specified capacity
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::collections::Deque;
    ///
    /// let _ : Deque<usize> = Deque::with_capacity(10);
    ///
    /// ```
    pub fn with_capacity(capacity: usize) -> Self {
        Deque {
            deque: RefCell::new(VecDeque::with_capacity(capacity)),
            sem: Semaphore::new(0),
        }
    }

    /// returns the amount of elements currently in the queue
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::collections::Deque;
    ///
    /// let ad : Deque<usize> = Deque::with_capacity(10);
    /// ad.push_back(1);
    /// assert_eq!(ad.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.deque.borrow().len()
    }

    /// pushes an element to the front of the queue
    /// # Examples
    ///
    /// ```
    /// use scipio::collections::Deque;
    ///
    /// let ad : Deque<usize> = Deque::with_capacity(10);
    /// ad.push_front(1);
    /// assert_eq!(ad.len(), 1);
    /// ```
    pub fn push_front(&self, el: T) {
        self.deque.borrow_mut().push_front(el);
        self.sem.signal(1);
    }

    /// pushes an element to the back of the queue
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::collections::Deque;
    ///
    /// let ad : Deque<usize> = Deque::with_capacity(10);
    /// ad.push_back(1);
    /// assert_eq!(ad.len(), 1);
    /// ```
    pub fn push_back(&self, el: T) {
        self.deque.borrow_mut().push_front(el);
        self.sem.signal(1);
    }

    /// Pops the back of the queue.
    ///
    /// If the queue is empty, this blocks until an element can be pop'd.
    ///
    /// This method returns Ok(T) unless the collection is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::collections::Deque;
    /// use scipio::LocalExecutor;
    ///
    /// let ad : Deque<usize> = Deque::with_capacity(10);
    ///
    /// let ex = LocalExecutor::new(None).unwrap();
    /// ex.run(async move {
    ///     ad.push_front(1);
    ///     ad.push_front(2);
    ///     let res = ad.pop_back().await.unwrap();
    ///     assert_eq!(res, 1);
    /// });
    /// ```
    pub async fn pop_back(&self) -> Result<T> {
        self.sem.acquire(1).await?;
        Ok(self.deque.borrow_mut().pop_back().unwrap())
    }

    /// Pops the front of the queue.
    ///
    /// If the queue is empty, this blocks until an element can be pop'd.
    ///
    /// This method returns Ok(T) unless the collection is closed.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::collections::Deque;
    /// use scipio::LocalExecutor;
    ///
    /// let ad : Deque<usize> = Deque::with_capacity(10);
    ///
    /// let ex = LocalExecutor::new(None).unwrap();
    /// ex.run(async move {
    ///     ad.push_front(1);
    ///     ad.push_front(2);
    ///     let res = ad.pop_front().await.unwrap();
    ///     assert_eq!(res, 2);
    /// });
    /// ```
    pub async fn pop_front(&self) -> Result<T> {
        self.sem.acquire(1).await?;
        Ok(self.deque.borrow_mut().pop_front().unwrap())
    }
}
