use alloc::rc::Rc;
use core::mem::{self, ManuallyDrop};
use core::task::{RawWaker, RawWakerVTable, Waker};

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

    unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
        let arc = ManuallyDrop::new(Rc::from_raw(ptr as *const F));
        mem::forget(arc.clone());
        RawWaker::new(ptr, &Self::VTABLE)
    }

    unsafe fn wake(ptr: *const ()) {
        let arc = Rc::from_raw(ptr as *const F);
        (arc)();
    }

    unsafe fn wake_by_ref(ptr: *const ()) {
        let arc = ManuallyDrop::new(Rc::from_raw(ptr as *const F));
        (arc)();
    }

    unsafe fn drop_waker(ptr: *const ()) {
        drop(Rc::from_raw(ptr as *const F));
    }
}
