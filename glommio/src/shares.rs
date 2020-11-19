// Unless explicitly stated otherwise all files in this repository are licensed under the
// MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2020 Datadog, Inc.
//
use core::fmt::Debug;
use std::rc::Rc;
use std::time::Duration;

/// The SharesManager allows the user to implement dynamic shares for a [`TaskQueue`]
///
/// In terms of behavior, a [`TaskQueue`] with static shares is the same as a `SharesManager`
/// managed queue that always return the same value. However this is a bit more expensive
/// to compute because it needs to be reevaluated constantly.
///
/// The difference is akin to a constant versus variable in your favorite programming language.
///
/// [`TaskQueue`]: struct.LocalExecutor.html#method.create_task_queue
pub trait SharesManager {
    /// The amount of shares that this [`TaskQueue`] should receive in the next adjustment period
    ///
    /// [`TaskQueue`]: struct.LocalExecutor.html#method.create_task_queue
    fn shares(&self) -> usize;

    /// How often to recompute the amount of shares for this [`TaskQueue`]
    ///
    /// [`TaskQueue`]: struct.LocalExecutor.html#method.create_task_queue
    fn adjustment_period(&self) -> Duration {
        Duration::from_millis(250)
    }
}

impl Debug for dyn SharesManager {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "Shares Manager: adjusting every {:#?}. Now: {}",
            self.adjustment_period(),
            self.shares()
        )
    }
}

#[derive(Debug, Clone)]
/// Represents how many shares a [`TaskQueue`] should receive.
///
/// Glommio's scheduler doesn't work with priorities, but rather shares. That means
/// that if there is only one active task queue in the system, it will always
/// receive 100 % of the resources.
///
/// As soon as two or more task queues are active, resources will be split between them
/// proportionally to their shares: a queue with more shares will receive more resources.
///
/// Be careful when trying to reason about percentages of utilization as they will depend on the
/// active task queues: The percentage of resources assigned to a task queue should be close to
/// `shares(i) / sum(shares(i) for i in t)`.
///
/// For example, if all task queues have 1000 shares (the default), when two of them are active
/// they will have each 50% of the resources. As soon as a third one activates, each now has
/// 33%.
///
/// This can be far off if there are other heavy processes competing for resources with
/// your application in a way that glommio can't see. For best results you should consider
/// dedicating CPUs and storage devices to your application.
///
/// Shares are enforced by the system to be between 1 and 1000. So if all [`TaskQueue`]s want
/// maximum resources they should all get similar fractions. It is not possible for a [`TaskQueue`]
/// to say it wants to use more than the others: it is only possible for the other task queues to
/// say they are okay with using less (by reducing their shares)
///
/// [`TaskQueue`]: struct.LocalExecutor.html#method.create_task_queue
pub enum Shares {
    /// Static shares never change over the course of a lifetime of the application, therefore they
    /// never have to be recomputed
    Static(usize),
    /// Dynamic shares can change and are periodically recomputed.
    Dynamic(Rc<dyn SharesManager>),
}

impl Default for Shares {
    fn default() -> Self {
        Shares::Static(1000)
    }
}

impl Shares {
    pub(crate) fn reciprocal_shares(&self) -> u64 {
        let shares = match self {
            Shares::Static(shares) => *shares,
            Shares::Dynamic(bm) => bm.shares(),
        };
        let shares = std::cmp::max(shares, 1);
        let shares = std::cmp::min(shares, 1000);
        (1u64 << 22) / (shares as u64)
    }
}
