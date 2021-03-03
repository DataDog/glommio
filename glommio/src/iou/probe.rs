use crate::uring_sys;
use std::io;
use std::ptr::NonNull;

/// A probe of the operations supported by this kernel version's io-uring interface.
#[derive(Debug)]
pub struct Probe {
    probe: NonNull<uring_sys::io_uring_probe>,
}

impl Probe {
    pub fn new() -> io::Result<Probe> {
        unsafe {
            let probe = uring_sys::io_uring_get_probe();
            NonNull::new(probe)
                .ok_or_else(io::Error::last_os_error)
                .map(|probe| Probe { probe })
        }
    }

    pub(crate) fn for_ring(ring: *mut uring_sys::io_uring) -> io::Result<Probe> {
        unsafe {
            let probe = uring_sys::io_uring_get_probe_ring(ring);
            NonNull::new(probe)
                .ok_or_else(io::Error::last_os_error)
                .map(|probe| Probe { probe })
        }
    }

    pub fn supports(&self, op: uring_sys::IoRingOp) -> bool {
        unsafe { uring_sys::io_uring_opcode_supported(self.probe.as_ptr(), op as _) != 0 }
    }
}

impl Drop for Probe {
    fn drop(&mut self) {
        unsafe { libc::free(self.probe.as_ptr() as *mut _) }
    }
}
