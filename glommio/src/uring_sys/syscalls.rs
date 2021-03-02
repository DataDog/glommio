use super::io_uring_params;

// syscall constants
#[allow(non_upper_case_globals)]
const __NR_io_uring_setup: libc::c_long = 425;
#[allow(non_upper_case_globals)]
const __NR_io_uring_enter: libc::c_long = 426;
#[allow(non_upper_case_globals)]
const __NR_io_uring_register: libc::c_long = 427;

pub unsafe fn io_uring_register(
    fd: libc::c_int,
    opcode: libc::c_uint,
    arg: *const libc::c_void,
    nr_args: libc::c_uint,
) -> libc::c_int {
    libc::syscall(__NR_io_uring_register, fd, opcode, arg, nr_args) as libc::c_int
}

pub unsafe fn io_uring_setup(entries: libc::c_uint, p: *mut io_uring_params) -> libc::c_int {
    libc::syscall(__NR_io_uring_setup, entries, p) as libc::c_int
}

pub unsafe fn io_uring_enter(
    fd: libc::c_int,
    to_submit: libc::c_uint,
    min_complete: libc::c_uint,
    flags: libc::c_uint,
    sig: *const libc::sigset_t,
) -> libc::c_int {
    libc::syscall(
        __NR_io_uring_enter,
        fd,
        to_submit,
        min_complete,
        flags,
        sig,
        core::mem::size_of::<libc::sigset_t>(),
    ) as libc::c_int
}
