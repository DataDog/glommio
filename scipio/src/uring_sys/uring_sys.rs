pub(crate) const LIBURING_UDATA_TIMEOUT: libc::__u64 = libc::__u64::max_value();

// sqe opcode constants
#[repr(C)]
#[non_exhaustive]
#[allow(nonstandard_style)]
#[derive(Debug)]
pub(crate) enum IoRingOp {
    IORING_OP_NOP,
    IORING_OP_READV,
    IORING_OP_WRITEV,
    IORING_OP_FSYNC,
    IORING_OP_READ_FIXED,
    IORING_OP_WRITE_FIXED,
    IORING_OP_POLL_ADD,
    IORING_OP_POLL_REMOVE,
    IORING_OP_SYNC_FILE_RANGE,
    IORING_OP_SENDMSG,
    IORING_OP_RECVMSG,
    IORING_OP_TIMEOUT,
    IORING_OP_TIMEOUT_REMOVE,
    IORING_OP_ACCEPT,
    IORING_OP_ASYNC_CANCEL,
    IORING_OP_LINK_TIMEOUT,
    IORING_OP_CONNECT,
    IORING_OP_FALLOCATE,
    IORING_OP_OPENAT,
    IORING_OP_CLOSE,
    IORING_OP_FILES_UPDATE,
    IORING_OP_STATX,
    IORING_OP_READ,
    IORING_OP_WRITE,
    IORING_OP_FADVISE,
    IORING_OP_MADVISE,
    IORING_OP_SEND,
    IORING_OP_RECV,
    IORING_OP_OPENAT2,
    IORING_OP_EPOLL_CTL,
    IORING_OP_SPLICE,
    IORING_OP_PROVIDE_BUFFERS,
    IORING_OP_REMOVE_BUFFERS,
}

// sqe.flags
pub(crate) const IOSQE_FIXED_FILE: libc::__u8 = 1 << 0; /* use fixed fileset */
pub(crate) const IOSQE_IO_DRAIN: libc::__u8 = 1 << 1; /* issue after inflight IO */
pub(crate) const IOSQE_IO_LINK: libc::__u8 = 1 << 2; /* links next sqe */
pub(crate) const IOSQE_IO_HARDLINK: libc::__u8 = 1 << 3; /* like LINK, but stronger */
pub(crate) const IOSQE_ASYNC: libc::__u8 = 1 << 4; /* always go async */
pub(crate) const IOSQE_BUFFER_SELECT: libc::__u8 = 1 << 5; /* select buf from sqe->buf_group */

// sqe.cmd_flags.fsync_flags
pub(crate) const IORING_FSYNC_DATASYNC: libc::__u32 = 1 << 0;

// sqe.cmd_flags.timeout_flags
pub(crate) const IORING_TIMEOUT_ABS: libc::__u32 = 1 << 0;

// sqe.cmd_flags.splice_flags
pub(crate) const SPLICE_F_FD_IN_FXIED: libc::__u32 = 1 << 31;

// io_uring_setup flags
pub(crate) const IORING_SETUP_IOPOLL: libc::c_uint = 1 << 0; /* io_context is polled */
pub(crate) const IORING_SETUP_SQPOLL: libc::c_uint = 1 << 1; /* SQ poll thread */
pub(crate) const IORING_SETUP_SQ_AFF: libc::c_uint = 1 << 2; /* sq_thread_cpu is valid */
pub(crate) const IORING_SETUP_CQSIZE: libc::c_uint = 1 << 3; /* app defines CQ size */
pub(crate) const IORING_SETUP_CLAMP: libc::c_uint = 1 << 4; /* clamp SQ/CQ ring sizes */
pub(crate) const IORING_SETUP_ATTACH_WQ: libc::c_uint = 1 << 5; /* attach to existing wq */

// cqe.flags
pub(crate) const IORING_CQE_BUFFER_SHIFT: libc::c_uint = 1 << 0;

// Magic offsets for the application to mmap the data it needs
pub(crate) const IORING_OFF_SQ_RING: libc::__u64 = 0;
pub(crate) const IORING_OFF_CQ_RING: libc::__u64 = 0x8000000;
pub(crate) const IORING_OFF_SQES: libc::__u64 = 0x10000000;

// sq_ring.kflags
pub(crate) const IORING_SQ_NEED_WAKEUP: libc::c_uint = 1 << 0;

// io_uring_enter flags
pub(crate) const IORING_ENTER_GETEVENTS: libc::c_uint = 1 << 0;
pub(crate) const IORING_ENTER_SQ_WAKEUP: libc::c_uint = 1 << 1;

// io_uring_params.features flags
pub(crate) const IORING_FEAT_SINGLE_MMAP: libc::__u32 = 1 << 0;
pub(crate) const IORING_FEAT_NODROP: libc::__u32 = 1 << 1;
pub(crate) const IORING_FEAT_SUBMIT_STABLE: libc::__u32 = 1 << 2;
pub(crate) const IORING_FEAT_RW_CUR_POS: libc::__u32 = 1 << 3;
pub(crate) const IORING_FEAT_CUR_PERSONALITY: libc::__u32 = 1 << 4;
pub(crate) const IORING_FEAT_FAST_POLL: libc::__u32 = 1 << 5;

// io_uring_register opcodes and arguments
pub(crate) const IORING_REGISTER_BUFFERS: libc::c_uint = 0;
pub(crate) const IORING_UNREGISTER_BUFFERS: libc::c_uint = 1;
pub(crate) const IORING_REGISTER_FILES: libc::c_uint = 2;
pub(crate) const IORING_UNREGISTER_FILES: libc::c_uint = 3;
pub(crate) const IORING_REGISTER_EVENTFD: libc::c_uint = 4;
pub(crate) const IORING_UNREGISTER_EVENTFD: libc::c_uint = 5;
pub(crate) const IORING_REGISTER_FILES_UPDATE: libc::c_uint = 6;
pub(crate) const IORING_REGISTER_EVENTFD_ASYNC: libc::c_uint = 7;
pub(crate) const IORING_REGISTER_PROBE: libc::c_uint = 8;
pub(crate) const IORING_REGISTER_PERSONALITY: libc::c_uint = 9;
pub(crate) const IORING_UNREGISTER_PERSONALITY: libc::c_uint = 10;

#[repr(C)]
pub(crate) struct io_uring {
    pub(crate) sq: io_uring_sq,
    pub(crate) cq: io_uring_cq,
    pub(crate) flags: libc::c_uint,
    pub(crate) ring_fd: libc::c_int,
}

#[repr(C)]
pub(crate) struct io_uring_sq {
    pub(crate) khead: *mut libc::c_uint,
    pub(crate) ktail: *mut libc::c_uint,
    pub(crate) kring_mask: *mut libc::c_uint,
    pub(crate) kring_entries: *mut libc::c_uint,
    pub(crate) kflags: *mut libc::c_uint,
    pub(crate) kdropped: *mut libc::c_uint,
    pub(crate) array: *mut libc::c_uint,
    pub(crate) sqes: *mut io_uring_sqe,

    pub(crate) sqe_head: libc::c_uint,
    pub(crate) sqe_tail: libc::c_uint,

    pub(crate) ring_sz: libc::size_t,
    pub(crate) ring_ptr: *mut libc::c_void,
}

#[repr(C)]
pub(crate) struct io_uring_sqe {
    pub(crate) opcode: libc::__u8,  /* type of operation for this sqe */
    pub(crate) flags: libc::__u8,   /* IOSQE_ flags */
    pub(crate) ioprio: libc::__u16, /* ioprio for the request */
    pub(crate) fd: libc::__s32,     /* file descriptor to do IO on */
    pub(crate) off_addr2: off_addr2,
    pub(crate) addr: libc::__u64, /* pointer to buffer or iovecs */
    pub(crate) len: libc::__u32,  /* buffer size or number of iovecs */
    pub(crate) cmd_flags: cmd_flags,
    pub(crate) user_data: libc::__u64, /* data to be passed back at completion time */
    pub(crate) buf_index: buf_index_padding, /* index into fixed buffers, if used */
}

#[repr(C)]
pub(crate) union off_addr2 {
    pub(crate) off: libc::__u64,
    pub(crate) addr2: libc::__u64,
}

#[repr(C)]
pub(crate) union cmd_flags {
    pub(crate) rw_flags: __kernel_rwf_t,
    pub(crate) fsync_flags: libc::__u32,
    pub(crate) poll_events: libc::__u16,
    pub(crate) sync_range_flags: libc::__u32,
    pub(crate) msg_flags: libc::__u32,
    pub(crate) timeout_flags: libc::__u32,
    pub(crate) accept_flags: libc::__u32,
    pub(crate) cancel_flags: libc::__u32,
    pub(crate) open_flags: libc::__u32,
    pub(crate) statx_flags: libc::__u32,
    pub(crate) fadvise_advice: libc::__u32,
    pub(crate) splice_flags: libc::__u32,
}

#[allow(non_camel_case_types)]
type __kernel_rwf_t = libc::c_int;

#[repr(C)]
pub(crate) union buf_index_padding {
    pub(crate) buf_index: buf_index,
    pub(crate) __pad2: [libc::__u64; 3],
}

#[repr(C)]
#[derive(Copy, Clone)]
pub(crate) struct buf_index {
    pub(crate) index_or_group: libc::__u16,
    pub(crate) personality: libc::__u16,
    pub(crate) splice_fd_in: libc::__s32,
}

#[repr(C)]
pub(crate) struct io_uring_cq {
    pub(crate) khead: *mut libc::c_uint,
    pub(crate) ktail: *mut libc::c_uint,
    pub(crate) kring_mask: *mut libc::c_uint,
    pub(crate) kring_entries: *mut libc::c_uint,
    pub(crate) koverflow: *mut libc::c_uint,
    pub(crate) cqes: *mut io_uring_cqe,

    pub(crate) ring_sz: libc::size_t,
    pub(crate) ring_ptr: *mut libc::c_void,
}

#[repr(C)]
pub(crate) struct io_uring_cqe {
    pub(crate) user_data: libc::__u64, /* sqe->data submission passed back */
    pub(crate) res: libc::__s32,       /* result code for this event */
    pub(crate) flags: libc::__u32,
}

#[repr(C)]
pub(crate) struct io_uring_params {
    pub(crate) sq_entries: libc::__u32,
    pub(crate) cq_entries: libc::__u32,
    pub(crate) flags: libc::__u32,
    pub(crate) sq_thread_cpu: libc::__u32,
    pub(crate) sq_thread_idle: libc::__u32,
    pub(crate) features: libc::__u32,
    pub(crate) resv: [libc::__u32; 4],
    pub(crate) sq_off: io_sqring_offsets,
    pub(crate) cq_off: io_cqring_offsets,
}

#[repr(C)]
pub(crate) struct io_sqring_offsets {
    pub(crate) head: libc::__u32,
    pub(crate) tail: libc::__u32,
    pub(crate) ring_mask: libc::__u32,
    pub(crate) ring_entries: libc::__u32,
    pub(crate) flags: libc::__u32,
    pub(crate) dropped: libc::__u32,
    pub(crate) array: libc::__u32,
    pub(crate) resv1: libc::__u32,
    pub(crate) resv2: libc::__u64,
}

#[repr(C)]
pub(crate) struct io_cqring_offsets {
    pub(crate) head: libc::__u32,
    pub(crate) tail: libc::__u32,
    pub(crate) ring_mask: libc::__u32,
    pub(crate) ring_entries: libc::__u32,
    pub(crate) overflow: libc::__u32,
    pub(crate) cqes: libc::__u32,
    pub(crate) resv: [libc::__u64; 2],
}

#[repr(C)]
pub(crate) struct io_uring_probe {
    last_op: libc::__u8,
    ops_len: libc::__u8,
    resv: libc::__u16,
    resv2: [libc::__u32; 3],
    ops: [io_uring_probe_op; 0],
}

#[repr(C)]
pub(crate) struct io_uring_probe_op {
    op: libc::__u8,
    resv: libc::__u8,
    flags: libc::__u16,
    resv2: libc::__u32,
}

#[repr(C)]
pub(crate) struct __kernel_timespec {
    pub(crate) tv_sec: i64,
    pub(crate) tv_nsec: libc::c_longlong,
}

#[link(name = "uring")]
extern "C" {
    pub(crate) fn io_uring_queue_init(
        entries: libc::c_uint,
        ring: *mut io_uring,
        flags: libc::c_uint,
    ) -> libc::c_int;

    pub(crate) fn io_uring_queue_init_params(
        entries: libc::c_uint,
        ring: *mut io_uring,
        params: *mut io_uring_params,
    ) -> libc::c_int;

    pub(crate) fn io_uring_queue_mmap(
        fd: libc::c_int,
        params: *mut io_uring_params,
        ring: *mut io_uring,
    ) -> libc::c_int;

    pub(crate) fn io_uring_get_probe_ring(ring: *mut io_uring) -> *mut io_uring_probe;

    pub(crate) fn io_uring_get_probe() -> *mut io_uring_probe;

    pub(crate) fn io_uring_free_probe(probe: *mut io_uring_probe);

    pub(crate) fn io_uring_dontfork(ring: *mut io_uring) -> libc::c_int;

    pub(crate) fn io_uring_queue_exit(ring: *mut io_uring);

    pub(crate) fn io_uring_peek_batch_cqe(
        ring: *mut io_uring,
        cqes: *mut *mut io_uring_cqe,
        count: libc::c_uint,
    ) -> libc::c_uint;

    pub(crate) fn io_uring_wait_cqes(
        ring: *mut io_uring,
        cqe_ptr: *mut *mut io_uring_cqe,
        wait_nr: libc::c_uint,
        ts: *const __kernel_timespec,
        sigmask: *const libc::sigset_t,
    ) -> libc::c_int;

    pub(crate) fn io_uring_wait_cqe_timeout(
        ring: *mut io_uring,
        cqe_ptr: *mut *mut io_uring_cqe,
        ts: *mut __kernel_timespec,
    ) -> libc::c_int;

    pub(crate) fn io_uring_submit(ring: *mut io_uring) -> libc::c_int;

    pub(crate) fn io_uring_submit_and_wait(
        ring: *mut io_uring,
        wait_nr: libc::c_uint,
    ) -> libc::c_int;

    pub(crate) fn io_uring_get_sqe(ring: *mut io_uring) -> *mut io_uring_sqe;

    pub(crate) fn io_uring_register_buffers(
        ring: *mut io_uring,
        iovecs: *const libc::iovec,
        nr_iovecs: libc::c_uint,
    ) -> libc::c_int;

    pub(crate) fn io_uring_unregister_buffers(ring: *mut io_uring) -> libc::c_int;

    pub(crate) fn io_uring_register_files(
        ring: *mut io_uring,
        files: *const libc::c_int,
        nr_files: libc::c_uint,
    ) -> libc::c_int;

    pub(crate) fn io_uring_unregister_files(ring: *mut io_uring) -> libc::c_int;

    pub(crate) fn io_uring_register_files_update(
        ring: *mut io_uring,
        off: libc::c_uint,
        files: *const libc::c_int,
        nr_files: libc::c_uint,
    ) -> libc::c_int;

    pub(crate) fn io_uring_register_eventfd(ring: *mut io_uring, fd: libc::c_int) -> libc::c_int;

    pub(crate) fn io_uring_unregister_eventfd(ring: *mut io_uring) -> libc::c_int;

    pub(crate) fn io_uring_register_probe(
        ring: *mut io_uring,
        p: *mut io_uring_probe,
        nr: libc::c_uint,
    ) -> libc::c_int;

    pub(crate) fn io_uring_register_personality(ring: *mut io_uring) -> libc::c_int;

    pub(crate) fn io_uring_unregister_personality(ring: *mut io_uring, id: libc::c_int);
}

#[link(name = "rusturing")]
extern "C" {
    #[link_name = "rust_io_uring_opcode_supported"]
    pub(crate) fn io_uring_opcode_supported(p: *mut io_uring_probe, op: libc::c_int)
        -> libc::c_int;

    #[link_name = "rust_io_uring_cq_advance"]
    pub(crate) fn io_uring_cq_advance(ring: *mut io_uring, nr: libc::c_uint);

    #[link_name = "rust_io_uring_cqe_seen"]
    pub(crate) fn io_uring_cqe_seen(ring: *mut io_uring, cqe: *mut io_uring_cqe);

    #[link_name = "rust_io_uring_sqe_set_data"]
    pub(crate) fn io_uring_sqe_set_data(sqe: *mut io_uring_sqe, data: *mut libc::c_void);

    #[link_name = "rust_io_uring_cqe_get_data"]
    pub(crate) fn io_uring_cqe_get_data(cqe: *mut io_uring_cqe) -> *mut libc::c_void;

    #[link_name = "rust_io_uring_sqe_set_flags"]
    pub(crate) fn io_uring_sqe_set_flags(sqe: *mut io_uring_sqe, flags: libc::c_uint);

    #[link_name = "rust_io_uring_prep_rw"]
    pub(crate) fn io_uring_prep_rw(
        op: libc::c_int,
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        addr: *const libc::c_void,
        len: libc::c_uint,
        offset: libc::__u64,
    );

    #[link_name = "rust_io_uring_prep_splice"]
    pub(crate) fn io_uring_prep_splice(
        sqe: *mut io_uring_sqe,
        fd_in: libc::c_int,
        off_in: libc::loff_t,
        fd_out: libc::c_int,
        off_out: libc::loff_t,
        nbytes: libc::c_uint,
        splice_flags: libc::c_uint,
    );

    #[link_name = "rust_io_uring_prep_readv"]
    pub(crate) fn io_uring_prep_readv(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        iovecs: *const libc::iovec,
        nr_vecs: libc::c_uint,
        offset: libc::off_t,
    );

    #[link_name = "rust_io_uring_prep_read_fixed"]
    pub(crate) fn io_uring_prep_read_fixed(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        buf: *mut libc::c_void,
        nbytes: libc::c_uint,
        offset: libc::off_t,
        buf_index: libc::c_int,
    );

    #[link_name = "rust_io_uring_prep_writev"]
    pub(crate) fn io_uring_prep_writev(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        iovecs: *const libc::iovec,
        nr_vecs: libc::c_uint,
        offset: libc::off_t,
    );

    #[link_name = "rust_io_uring_prep_write_fixed"]
    pub(crate) fn io_uring_prep_write_fixed(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        buf: *const libc::c_void,
        nbytes: libc::c_uint,
        offset: libc::off_t,
        buf_index: libc::c_int,
    );

    #[link_name = "rust_io_uring_prep_recvmsg"]
    pub(crate) fn io_uring_prep_recvmsg(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        msg: *mut libc::msghdr,
        flags: libc::c_uint,
    );

    #[link_name = "rust_io_uring_prep_sendmsg"]
    pub(crate) fn io_uring_prep_sendmsg(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        msg: *const libc::msghdr,
        flags: libc::c_uint,
    );

    #[link_name = "rust_io_uring_prep_poll_add"]
    pub(crate) fn io_uring_prep_poll_add(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        poll_mask: libc::c_short,
    );

    #[link_name = "rust_io_uring_prep_poll_remove"]
    pub(crate) fn io_uring_prep_poll_remove(sqe: *mut io_uring_sqe, user_data: *mut libc::c_void);

    #[link_name = "rust_io_uring_prep_fsync"]
    pub(crate) fn io_uring_prep_fsync(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        fsync_flags: libc::c_uint,
    );

    #[link_name = "rust_io_uring_prep_nop"]
    pub(crate) fn io_uring_prep_nop(sqe: *mut io_uring_sqe);

    #[link_name = "rust_io_uring_prep_timeout"]
    pub(crate) fn io_uring_prep_timeout(
        sqe: *mut io_uring_sqe,
        ts: *mut __kernel_timespec,
        count: libc::c_uint,
        flags: libc::c_uint,
    );

    #[link_name = "rust_io_uring_prep_timeout_remove"]
    pub(crate) fn io_uring_prep_timeout_remove(
        sqe: *mut io_uring_sqe,
        user_data: libc::__u64,
        flags: libc::c_uint,
    );

    #[link_name = "rust_io_uring_prep_accept"]
    pub(crate) fn io_uring_prep_accept(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        addr: *mut libc::sockaddr,
        addrlen: *mut libc::socklen_t,
        flags: libc::c_int,
    );

    #[link_name = "rust_io_uring_prep_cancel"]
    pub(crate) fn io_uring_prep_cancel(
        sqe: *mut io_uring_sqe,
        user_data: *mut libc::c_void,
        flags: libc::c_int,
    );

    #[link_name = "rust_io_uring_prep_link_timeout"]
    pub(crate) fn io_uring_prep_link_timeout(
        sqe: *mut io_uring_sqe,
        ts: *mut __kernel_timespec,
        flags: libc::c_uint,
    );

    #[link_name = "rust_io_uring_prep_connect"]
    pub(crate) fn io_uring_prep_connect(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        addr: *mut libc::sockaddr,
        addrlen: libc::socklen_t,
    );

    #[link_name = "rust_io_uring_prep_files_update"]
    pub(crate) fn io_uring_prep_files_update(
        sqe: *mut io_uring_sqe,
        fds: *mut libc::c_int,
        nr_fds: libc::c_uint,
        offset: libc::c_int,
    );

    #[link_name = "rust_io_uring_prep_fallocate"]
    pub(crate) fn io_uring_prep_fallocate(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        mode: libc::c_int,
        offset: libc::off_t,
        len: libc::off_t,
    );

    #[link_name = "rust_io_uring_prep_openat"]
    pub(crate) fn io_uring_prep_openat(
        sqe: *mut io_uring_sqe,
        dfd: libc::c_int,
        path: *const libc::c_char,
        flags: libc::c_int,
        mode: libc::mode_t,
    );

    #[link_name = "rust_io_uring_prep_close"]
    pub(crate) fn io_uring_prep_close(sqe: *mut io_uring_sqe, fd: libc::c_int);

    #[link_name = "rust_io_uring_prep_read"]
    pub(crate) fn io_uring_prep_read(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        buf: *mut libc::c_void,
        nbytes: libc::c_uint,
        offset: libc::off_t,
    );

    #[link_name = "rust_io_uring_prep_write"]
    pub(crate) fn io_uring_prep_write(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        buf: *const libc::c_void,
        nbytes: libc::c_uint,
        offset: libc::off_t,
    );

    #[link_name = "rust_io_uring_prep_statx"]
    pub(crate) fn io_uring_prep_statx(
        sqe: *mut io_uring_sqe,
        dfd: libc::c_int,
        path: *const libc::c_char,
        flags: libc::c_int,
        mask: libc::c_uint,
        statx: *mut libc::statx,
    );

    #[link_name = "rust_io_uring_prep_fadvise: libc::c_int"]
    pub(crate) fn io_uring_prep_fadvise(
        sqe: *mut io_uring_sqe,
        fd: libc::c_int,
        offset: libc::off_t,
        len: libc::off_t,
        advice: libc::c_int,
    );

    #[link_name = "rust_io_uring_prep_madvise"]
    pub(crate) fn io_uring_prep_madvise(
        sqe: *mut io_uring_sqe,
        addr: *mut libc::c_void,
        length: libc::off_t,
        advice: libc::c_int,
    );

    #[link_name = "rust_io_uring_prep_send"]
    pub(crate) fn io_uring_prep_send(
        sqe: *mut io_uring_sqe,
        sockfd: libc::c_int,
        buf: *const libc::c_void,
        len: libc::size_t,
        flags: libc::c_int,
    );

    #[link_name = "rust_io_uring_prep_recv"]
    pub(crate) fn io_uring_prep_recv(
        sqe: *mut io_uring_sqe,
        sockfd: libc::c_int,
        buf: *mut libc::c_void,
        len: libc::size_t,
        flags: libc::c_int,
    );

    #[link_name = "rust_io_uring_prep_openat2"]
    pub(crate) fn io_uring_prep_openat2(
        sqe: *mut io_uring_sqe,
        dfd: libc::c_int,
        path: *const libc::c_char,
        how: *mut libc::c_void,
    );

    #[link_name = "rust_io_uring_prep_epoll_ctl"]
    pub(crate) fn io_uring_prep_epoll_ctl(
        sqe: *mut io_uring_sqe,
        epfd: libc::c_int,
        fd: libc::c_int,
        op: libc::c_int,
        ev: *mut libc::epoll_event,
    );

    #[link_name = "rust_io_uring_prep_provide_buffers"]
    pub(crate) fn io_uring_prep_provide_buffers(
        sqe: *mut io_uring_sqe,
        addr: *mut libc::c_void,
        len: libc::c_int,
        nr: libc::c_int,
        bgid: libc::c_int,
        bid: libc::c_int,
    );

    #[link_name = "rust_io_uring_prep_remove_buffers"]
    pub(crate) fn io_uring_prep_remove_buffers(
        sqe: *mut io_uring_sqe,
        nr: libc::c_int,
        bgid: libc::c_int,
    );

    #[link_name = "rust_io_uring_sq_ready"]
    pub(crate) fn io_uring_sq_ready(ring: *mut io_uring) -> libc::c_uint;

    #[link_name = "rust_io_uring_sq_space_left"]
    pub(crate) fn io_uring_sq_space_left(ring: *mut io_uring) -> libc::c_uint;

    #[link_name = "rust_io_uring_cq_ready"]
    pub(crate) fn io_uring_cq_ready(ring: *mut io_uring) -> libc::c_uint;

    #[link_name = "rust_io_uring_wait_cqe_nr"]
    pub(crate) fn io_uring_wait_cqe_nr(
        ring: *mut io_uring,
        cqe_ptr: *mut *mut io_uring_cqe,
        wait_nr: libc::c_uint,
    ) -> libc::c_int;

    #[link_name = "rust_io_uring_peek_cqe"]
    pub(crate) fn io_uring_peek_cqe(
        ring: *mut io_uring,
        cqe_ptr: *mut *mut io_uring_cqe,
    ) -> libc::c_int;

    #[link_name = "rust_io_uring_wait_cqe"]
    pub(crate) fn io_uring_wait_cqe(
        ring: *mut io_uring,
        cqe_ptr: *mut *mut io_uring_cqe,
    ) -> libc::c_int;
}
