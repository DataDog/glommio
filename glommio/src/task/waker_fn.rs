// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use core::{
    mem::{self, ManuallyDrop},
    task::{RawWaker, RawWakerVTable, Waker},
};
use std::rc::Rc;

/// Creates a waker from a wake function.
///
/// The function gets called every time the waker is woken.
pub fn waker_fn<F: Fn() + Send + Sync + 'static>(f: F) -> Waker {
    let raw = Rc::into_raw(Rc::new(f)) as *const ();
    let vtable = &Helper::<F>::VTABLE;
    unsafe { Waker::from_raw(RawWaker::new(raw, vtable)) }
}

struct Helper<F>(F);

impl<F: Fn() + Send + Sync + 'static> Helper<F> {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_waker,
        Self::wake,
        Self::wake_by_ref,
        Self::drop_waker,
    );

    #[allow(clippy::redundant_clone)]
    // Clippy sees this rc.clone() call as redundant. However what we are doing here
    // is making sure that the waker is alive until a later explicit call to
    // drop_waker We need to leave this function with the reference count
    // bumped.
    unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
        let rc = ManuallyDrop::new(Rc::from_raw(ptr as *const F));
        mem::forget(rc.clone());
        RawWaker::new(ptr, &Self::VTABLE)
    }

    unsafe fn wake(ptr: *const ()) {
        let rc = Rc::from_raw(ptr as *const F);
        (rc)();
    }

    unsafe fn wake_by_ref(ptr: *const ()) {
        let rc = ManuallyDrop::new(Rc::from_raw(ptr as *const F));
        (rc)();
    }

    unsafe fn drop_waker(ptr: *const ()) {
        drop(Rc::from_raw(ptr as *const F));
    }
}
