// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use std::cell::UnsafeCell;
use std::rc::{Rc, Weak};

/// A shared pointer with contents that are safe for Scipio users to access.
///
/// It is similar to [`Rc`]<[`RefCell`]<T>>, except that it does not perform runtime checks on the
/// inner cell, and the reference counted pointer does not implement [`Deref`].
///
/// Because Scipio does not use helper threads a particular shared pointer is always ever used by a
/// single CPU, which provides natural serialization. There are no other users as long as it
/// doesn't cross a suspension point.
///
/// Because [`Deref`] is not implemented the internal contents can only be accessed with the
/// [`do_with`]. It is illegal to have any suspension points in code that is passed to [`do_with`]
///
/// [`Rc`]: https://doc.rust-lang.org/beta/std/rc/struct.Rc.html
/// [`RefCell`]: https://doc.rust-lang.org/std/cell/struct.RefCell.html
/// [`Deref`]: https://doc.rust-lang.org/std/ops/trait.Deref.html
/// [`do_with`]: struct.Shared.html#method.do_with
#[derive(Debug, Clone)]
pub struct Shared<T> {
    inner: Rc<UnsafeCell<T>>,
}

/// A weak pointer with contents that are safe for Scipio users to access.
///
/// It is similar to [`Weak`]<[`RefCell`]<T>>, except that it does not perform runtime checks on the
/// inner cell, and the reference counted pointer does not implement [`Deref`]. It upgrades to a
/// [`Shared`] and must be upgraded before using.
///
/// [`Weak`]: https://doc.rust-lang.org/beta/std/rc/struct.Weak.html
/// [`RefCell`]: https://doc.rust-lang.org/std/cell/struct.RefCell.html
/// [`Shared`]: struct.Shared.html
/// [`Deref`]: https://doc.rust-lang.org/std/ops/trait.Deref.html
#[derive(Debug, Clone)]
pub struct WeakShared<T> {
    inner: Weak<UnsafeCell<T>>,
}

impl<T> Default for WeakShared<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> WeakShared<T> {
    /// Constructs a new `WeakShared<T>`, without allocating any memory. Calling upgrade on the return value always gives [`None`].
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::WeakShared;
    /// let empty: WeakShared<i64> = WeakShared::new();
    /// assert!(empty.upgrade().is_none());
    /// ```
    ///
    /// [`None`]: https://doc.rust-lang.org/beta/std/option/enum.Option.html#variant.None
    pub fn new() -> WeakShared<T> {
        WeakShared { inner: Weak::new() }
    }

    /// Attempts to upgrade the WeakShared pointer to a [`Shared`], delaying dropping of the inner value if successful.
    ///
    /// Returns [`None`] if the inner value has since been dropped or if this was created through
    /// [`WeakShared::new`]
    ///
    /// # Examples
    /// ```
    /// use scipio::{Shared, WeakShared};
    ///
    /// let five = Shared::new(5);
    ///
    /// let weak_five = Shared::downgrade(&five);
    ///
    /// let strong_five: Option<Shared<_>> = weak_five.upgrade();
    /// assert!(strong_five.is_some());
    ///
    /// // Destroy all strong pointers.
    /// drop(strong_five);
    /// drop(five);
    ///
    /// assert!(weak_five.upgrade().is_none());
    /// ```
    /// [`Shared`]: struct.Shared.html
    /// [`None`]: https://doc.rust-lang.org/beta/std/option/enum.Option.html#variant.None
    /// [`WeakShared::new`]: struct.WeakShared.html#method.new
    pub fn upgrade(&self) -> Option<Shared<T>> {
        self.inner.upgrade().map(|inner| Shared { inner })
    }

    /// Returns `true` if the two WeakShared point to the same allocation (similar to [`ptr::eq`])
    ///
    /// [`ptr::eq`]: https://doc.rust-lang.org/beta/std/ptr/fn.eq.html
    pub fn ptr_eq(&self, other: &WeakShared<T>) -> bool {
        self.inner.ptr_eq(&other.inner)
    }

    /// Get the number of strong pointers to this allocation
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{Shared, WeakShared};
    ///
    /// let x = Shared::new(0);
    /// assert_eq!(Shared::strong_count(&x), 1);
    /// let y = Shared::downgrade(&x);
    /// assert_eq!(y.strong_count(), 1);
    /// ```
    pub fn strong_count(&self) -> usize {
        self.inner.strong_count()
    }

    /// Get the number of weak pointers to this allocation
    ///
    /// If no strong pointers remain, this will return zero.
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{Shared, WeakShared};
    ///
    /// let x = Shared::new(0);
    /// assert_eq!(Shared::weak_count(&x), 0);
    /// let y = Shared::downgrade(&x);
    /// assert_eq!(y.weak_count(), 1);
    /// ```
    pub fn weak_count(&self) -> usize {
        self.inner.weak_count()
    }
}

impl<T> Shared<T> {
    /// Creates a new `Shared` object
    ///
    /// This shared object implements [`Clone`] and is reference counted so it is safe to pass
    /// to multiple tasks in the same [`LocalExecutor`]
    ///
    ///
    /// # Examples
    /// ```
    /// use scipio::Shared;
    ///
    /// let x = Shared::new(0);
    /// ```
    /// [`Clone`]: https://doc.rust-lang.org/std/clone/trait.Clone.html
    /// [`LocalExecutor`]: struct.LocalExecutor.html
    pub fn new(t: T) -> Self {
        Shared {
            inner: Rc::new(UnsafeCell::new(t)),
        }
    }

    /// Returns `true` if the two `Shared` point to the same allocation (similar to [`ptr::eq`])
    ///
    /// [`ptr::eq`]: https://doc.rust-lang.org/beta/std/ptr/fn.eq.html
    pub fn ptr_eq(this: &Shared<T>, other: &Shared<T>) -> bool {
        Rc::ptr_eq(&this.inner, &other.inner)
    }

    /// Executes the closure `func`, with a mutable reference to this `Shared`'s inner value as a
    /// parameter
    ///
    /// # Examples
    /// ```
    /// use scipio::Shared;
    ///
    /// let x = Shared::new(0);
    /// x.do_with(|x| assert_eq!(*x, 0) );
    /// x.do_with(|x| *x = 2 );
    /// x.do_with(|x| assert_eq!(*x, 2) );
    /// ```
    pub fn do_with<F, R>(&self, func: F) -> R
    where
        F: FnOnce(&mut T) -> R,
        R: Sized,
    {
        let k = unsafe { &mut *self.inner.get() };
        func(k)
    }

    /// Convenience method that replaces the current value of this `Shared` by the argument
    /// `new_value`.
    ///
    /// # Examples
    /// ```
    /// use scipio::Shared;
    ///
    /// let x = Shared::new(0);
    /// // This is an equivalent call than the swap below ...
    /// let y = x.do_with(|x| std::mem::replace(x, 2) );
    /// assert_eq!(y, 0);
    ///
    /// // ... but swap is way more convenient for cases where all we want is to update the value.
    /// let y = x.swap(4);
    /// assert_eq!(y, 2);
    /// x.do_with(|x| assert_eq!(*x, 4) );
    /// ```
    pub fn swap(&self, new_value: T) -> T {
        unsafe { std::mem::replace(&mut *self.inner.get(), new_value) }
    }

    /// Convenience method that returns the current value of this `Shared` if `T` implements [`Copy`]
    ///
    /// # Examples
    /// ```
    /// use scipio::Shared;
    ///
    /// let x = Shared::new(0);
    /// // This is an equivalent call than the inner below ...
    /// let y = x.do_with(|x| *x );
    /// assert_eq!(y, 0);
    ///
    /// // ... but inner is more convenient for cases where `T` implements [`Copy`]
    /// assert_eq!(x.inner(), 0);
    /// ```
    ///
    /// [`Copy`]: https://doc.rust-lang.org/std/marker/trait.Copy.html
    pub fn inner(&self) -> T
    where
        T: Copy,
    {
        unsafe { *self.inner.get() }
    }

    /// Get the number of strong pointers to this allocation
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::Shared;
    ///
    /// let x = Shared::new(0);
    /// assert_eq!(Shared::strong_count(&x), 1);
    /// let y = x.clone();
    /// assert_eq!(Shared::strong_count(&x), 2);
    /// ```
    pub fn strong_count(this: &Shared<T>) -> usize {
        Rc::strong_count(&this.inner)
    }

    /// Get the number of weak pointers to this allocation
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{Shared, WeakShared};
    ///
    /// let x = Shared::new(0);
    /// assert_eq!(Shared::weak_count(&x), 0);
    /// let y = Shared::downgrade(&x);
    /// assert_eq!(Shared::weak_count(&x), 1);
    /// ```
    pub fn weak_count(this: &Shared<T>) -> usize {
        Rc::weak_count(&this.inner)
    }

    /// Creates a new [`WeakShared`] pointer to this allocation
    ///
    /// # Examples
    ///
    /// ```
    /// use scipio::{Shared, WeakShared};
    ///
    /// let x = Shared::new(0);
    /// assert_eq!(Shared::weak_count(&x), 0);
    /// let y = Shared::downgrade(&x);
    /// assert_eq!(Shared::weak_count(&x), 1);
    /// ```
    /// [`WeakShared`]: struct.WeakShared.html
    pub fn downgrade(this: &Shared<T>) -> WeakShared<T> {
        WeakShared {
            inner: Rc::downgrade(&this.inner),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn strong_count_ok() {
        let x = Shared::new(0);
        let y = x.clone();
        assert_eq!(Shared::strong_count(&x), 2);
        assert_eq!(Shared::weak_count(&x), 0);
        drop(y);
        assert_eq!(Shared::strong_count(&x), 1);
    }

    #[test]
    fn weak_count_ok() {
        let x = Shared::new(0);
        let y = x.clone();
        let weak = Shared::downgrade(&x);
        assert_eq!(Shared::strong_count(&x), 2);
        assert_eq!(Shared::weak_count(&x), 1);
        drop(x);
        assert_eq!(weak.weak_count(), 1);
        drop(y);
        assert_eq!(weak.weak_count(), 0);

        assert_eq!(weak.strong_count(), 0);
    }

    #[test]
    fn upgrade_while_alive_ok() {
        let x = Shared::new(0);
        let weak = Shared::downgrade(&x);
        assert_eq!(Shared::weak_count(&x), 1);

        let strong = weak.upgrade().unwrap();
        assert_eq!(0, strong.swap(42));
        assert_eq!(weak.strong_count(), 2);
        assert_eq!(Shared::weak_count(&x), 1);
        drop(weak);
        assert_eq!(Shared::weak_count(&x), 0);
    }

    #[should_panic]
    #[test]
    fn upgrade_after_drop_panics() {
        let x = Shared::new(0);
        let weak = Shared::downgrade(&x);
        assert_eq!(Shared::weak_count(&x), 1);
        drop(x);

        weak.upgrade().unwrap();
    }

    #[should_panic]
    #[test]
    fn upgrade_from_no_strong_panics() {
        let weak: WeakShared<usize> = WeakShared::new();
        assert_eq!(weak.weak_count(), 1);
        weak.upgrade().unwrap();
    }
}
