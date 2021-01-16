//! Provides a [`NopSubmitter`] which can be used to submit nop operations to the ring. This is mainly useful for benchmarking Glommio.
use std::io;
use std::rc::{Rc, Weak};

use crate::parking::Reactor;
use crate::Local;

/// Submit no-op operations to io_uring.
///
/// This is mainly useful for benchmarking Glommio.
#[derive(Debug)]
pub struct NopSubmitter {
    reactor: Weak<Reactor>,
}

impl NopSubmitter {
    /// Construct a new [`NopSubmitter`].
    pub fn new() -> Self {
        let reactor = Local::get_reactor();
        let reactor = Rc::downgrade(&reactor);
        Self { reactor }
    }

    /// Submit a no-op io_uring operation, and wait for completion.
    pub async fn run_nop(&self) -> io::Result<()> {
        let reactor = self.reactor.upgrade().unwrap();
        let source = reactor.nop();
        source.collect_rw().await?;
        Ok(())
    }
}
