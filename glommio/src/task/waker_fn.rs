// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use core::task::{RawWaker, RawWakerVTable, Waker};

/// Creates a waker that does nothing.
///
/// This [`Waker`] is useful for polling a `Future` to check whether it is
/// `Ready`, without doing any additional work.
pub(crate) fn dummy_waker() -> Waker {
    fn raw_waker() -> RawWaker {
        // the pointer is never dereferenced, so null is ok
        RawWaker::new(std::ptr::null::<()>(), vtable())
    }

    fn vtable() -> &'static RawWakerVTable {
        &RawWakerVTable::new(|_| raw_waker(), |_| {}, |_| {}, |_| {})
    }

    unsafe { Waker::from_raw(raw_waker()) }
}
