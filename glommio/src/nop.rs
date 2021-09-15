//! Provides a [`NopSubmitter`] which can be used to submit nop operations to
//! the ring. This is mainly useful for benchmarking Glommio.
use std::{
    io,
    rc::{Rc, Weak},
};

use crate::reactor::Reactor;

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
        Self::default()
    }

    /// Submit a no-op io_uring operation, and wait for completion.
    pub async fn run_nop(&self) -> io::Result<()> {
        let reactor = self.reactor.upgrade().unwrap();
        let source = reactor.nop();
        source.collect_rw().await?;
        Ok(())
    }
}

impl Default for NopSubmitter {
    fn default() -> Self {
        let reactor = crate::executor().reactor();
        let reactor = Rc::downgrade(&reactor);
        Self { reactor }
    }
}
