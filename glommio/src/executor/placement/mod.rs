// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.
//
//! This module reads the machine's hardware topology with respect to the
//! following components: `NumaNode`, `Package` (a.k.a. socket), `Core`, and
//! `Cpu`.  It further provides iterators over the CPUs that are currently
//! online in the system.
//!
//! The implementation represents the topology as tree, which can be used to
//! select CPUs that have a low or high degree of separation from other
//! previously selected CPUs.
//!
//! For `MaxSpread` (see `Node::priority_spread`), selection begins at the
//! `SystemRoot` and proceeds by selecting which of its child `Node`s have the
//! lowest "saturation".  Saturation is defined as the ratio of previously
//! selected `Cpu`s under the current `Node` divided by the total number of
//! `Cpu`s under this `Node` in the machine topology.
//!
//! For `MaxPack`, `Node`s are selected based on the following decisions (see
//! `Node::priority_pack`:
//! 1) if a `Node` is partially saturated (i.e. currently being filled by the
//! packing iterator such    that some CPUs have been selected and others have
//! not), then it takes priority 2) if the `Node` is not partially saturated,
//! then it is either equally saturated with the other    `Node`s at its `Level`
//! or one `Node` has a greater saturation than the other:    a) if `Node`s are
//! equally saturated, use the `Node` with the greater number of total slots
//!    (i.e.  CPUs that are online)
//!    b) if `Node`s are not equally saturated, use the node with lower
//! saturation
//!
//! `Node:::select_cpu` is used to recursively proceed through levels in the
//! tree until a `Cpu` is selected.  The `Nodes` which were traversed in
//! reaching the `Cpu` are reflected in the `Path` returned by
//! `Node::select_cpu`.
//!
//! Note that the numa distance or amount of memory available on each `Node` is
//! not currently considered in selecting CPUs.
//!
//! Some helpful references:
//! https://www.kernel.org/doc/html/latest/admin-guide/cputopology.html
//! https://www.kernel.org/doc/html/latest/vm/numa.html
//! https://www.kernel.org/doc/html/latest/core-api/cpu_hotplug.html

mod linux_sysfs;
mod pq_tree;

use pq_tree::{
    marker::{Pack, Priority, Spread},
    Level,
    Node,
};

use super::Placement;

use std::{collections::HashSet, convert::TryInto, io};

/// A description of the CPUs location in the machine topology.
#[derive(Clone, Debug)]
pub struct CpuLocation {
    /// Holds the CPU id.  This is the most granular field and will distinguish
    /// among [`hyper-threads`].
    ///
    /// [`hyper-threads`]: https://en.wikipedia.org/wiki/Hyper-threading
    pub cpu: usize,
    /// Holds the core id on which the `cpu` is located.
    pub core: usize,
    /// Holds the package or socket id on which the `cpu` is located.
    pub package: usize,
    /// Holds the NUMA node on which the `cpu` is located.
    pub numa_node: usize,
}

/// A set of CPUs associated with a [`LocalExecutor`] when created via a
/// [`LocalExecutorPoolBuilder`].
#[derive(Clone, Debug)]
pub struct CpuSet(pub(super) Option<Vec<CpuLocation>>);

pub enum CpuSetGenerator {
    Unbound,
    MaxSpread(MaxSpreader),
    MaxPack(MaxPacker),
}

impl CpuSetGenerator {
    pub fn new(placement: Placement) -> io::Result<Self> {
        let this = match placement {
            Placement::Unbound => Self::Unbound,
            Placement::MaxSpread => Self::MaxSpread(MaxSpreader::new()?),
            Placement::MaxPack => Self::MaxPack(MaxPacker::new()?),
        };
        Ok(this)
    }

    /// A method that generates a [`CpuSet`] according to the provided
    /// [`Placement`] policy. Sequential calls may generate different sets
    /// depending on the [`Placement`].
    pub fn next(&mut self) -> CpuSet {
        match self {
            Self::Unbound => CpuSet(None),
            Self::MaxSpread(it) => CpuSet(it.next().map(|l| vec![l])),
            Self::MaxPack(it) => CpuSet(it.next().map(|l| vec![l])),
        }
    }
}

/// An [`Iterator`] over [`CpuLocation`]s in the machine topology which have a
/// high degree of separation from previous [`CpuLocation`]s returned by
/// `MaxSpreader`.  The order in which items are returned by `MaxSpreader` is
/// non-deterministic.  Iterating `MaxSpreader` will never return `Option::None`
/// and will cycle through [`CpuLocation`], where each cycle itself is
/// non-deterministic (i.e.  the order may differ from prior cycles).
type MaxSpreader = TopologyIter<Spread>;

/// An [`Iterator`] over [`CpuLocation`]s in the machine topology which have a
/// low degree of separation from previous [`CpuLocation`]s returned by
/// `MaxPacker`.  Iterating `MaxPacker` will never return `Option::None` and
/// will cycle through [`CpuLocation`] non-deterministically.
type MaxPacker = TopologyIter<Pack>;

pub struct TopologyIter<T> {
    tree: Node<T>,
}

impl<T: Priority> Iterator for TopologyIter<T> {
    type Item = CpuLocation;
    fn next(&mut self) -> Option<Self::Item> {
        Some(self.tree.select_cpu().try_into().unwrap())
    }
}

impl<T> TopologyIter<T>
where
    T: Priority,
{
    pub fn new() -> io::Result<Self> {
        let topology = linux_sysfs::get_machine_topology_unsorted()?;
        Self::from_topology(topology)
    }

    /// Construct a `TopologyIter` from a `Vec<CpuLocation>`.  The tree of
    /// priority queues is constructed sequentially (using the change in
    /// `Node` ID to indicate that a `Node` is ready to be pushed onto its
    /// parent), so IDs at a particular level should be unique (e.g. a
    /// `Core` with ID 0 should not exist on `Package`s with IDs 0 and 1).
    pub fn from_topology(mut topology: Vec<CpuLocation>) -> io::Result<Self> {
        // use the number of unique numa IDs and package IDs to determine whether numa
        // nodes reside outside / above or inside / below packages in the
        // machine topology
        let nr_numa_node = Self::count_unique_by(&topology, |c| c.numa_node);
        let nr_package = Self::count_unique_by(&topology, |c| c.package);

        // the topology must be sorted such that all children of a `Node` are added to a
        // parent consecutively
        let f_level: fn(&CpuLocation, usize) -> Level = if nr_package < nr_numa_node {
            topology.sort_by_key(|l| (l.package, l.numa_node, l.core, l.cpu));
            |cpu_loc, depth| -> Level {
                match depth {
                    0 => Level::SystemRoot,
                    1 => Level::Package(cpu_loc.package),
                    2 => Level::NumaNode(cpu_loc.numa_node),
                    3 => Level::Core(cpu_loc.core),
                    4 => Level::Cpu(cpu_loc.cpu),
                    _ => panic!("unexpected tree level: {}", depth),
                }
            }
        } else {
            topology.sort_by_key(|l| (l.numa_node, l.package, l.core, l.cpu));
            |cpu_loc, depth| -> Level {
                match depth {
                    0 => Level::SystemRoot,
                    1 => Level::NumaNode(cpu_loc.numa_node),
                    2 => Level::Package(cpu_loc.package),
                    3 => Level::Core(cpu_loc.core),
                    4 => Level::Cpu(cpu_loc.cpu),
                    _ => panic!("unexpected tree level: {}", depth),
                }
            }
        };

        let mut iter = topology.into_iter();
        match iter.next() {
            mut next @ Some(_) => {
                let mut node_root = Node::<T>::new(Level::SystemRoot);
                Self::build_sub_tree(&mut node_root, &mut None, &mut next, &mut iter, &f_level, 1);
                Ok(Self { tree: node_root })
            }
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "no cpu in machine topology",
            )),
        }
    }

    fn build_sub_tree<I, F>(
        parent: &mut Node<T>,
        prev: &mut Option<CpuLocation>,
        next: &mut Option<CpuLocation>,
        iter: &mut I,
        f_level: &F,
        depth: usize,
    ) where
        I: Iterator<Item = CpuLocation>,
        F: Fn(&CpuLocation, usize) -> Level,
    {
        loop {
            match (&prev, &next) {
                (None, Some(loc_n)) => {
                    let mut node = Node::<T>::new(f_level(loc_n, depth));
                    match node.level() {
                        Level::Cpu(_) => {
                            std::mem::swap(prev, next);
                            *next = iter.next();
                        }
                        _ => Self::build_sub_tree(&mut node, prev, next, iter, f_level, depth + 1),
                    }
                    parent.push_child(node);
                }
                (Some(loc_p), Some(loc_n))
                    if f_level(loc_p, depth - 1) == f_level(loc_n, depth - 1) =>
                {
                    *prev = None
                }
                _ => return,
            }
        }
    }

    fn count_unique_by<L, ID>(it: impl IntoIterator<Item = L>, mut f: impl FnMut(L) -> ID) -> usize
    where
        ID: Eq + std::hash::Hash,
    {
        it.into_iter()
            .fold(HashSet::new(), |mut set, item| {
                set.insert(f(item));
                set
            })
            .len()
    }
}

#[cfg(test)]
pub use linux_sysfs::get_machine_topology_unsorted;

#[cfg(test)]
mod test {
    use super::{linux_sysfs::test_helpers::cpu_loc, *};

    #[test]
    fn max_spreader_this_machine() {
        let n = 4096;
        let mut max_spreader = MaxSpreader::new().unwrap();

        assert_eq!(0, max_spreader.tree.nr_slots_selected());
        for _ in 0..n {
            let _cpu_location: CpuLocation = max_spreader.next().unwrap();
        }
        assert_eq!(n, max_spreader.tree.nr_slots_selected());
    }

    #[test]
    fn max_packer_this_machine() {
        let n = 4096;
        let mut max_packer = MaxSpreader::new().unwrap();

        for _ in 0..n {
            let _cpu_location: CpuLocation = max_packer.next().unwrap();
        }
    }

    #[test]
    #[should_panic(expected = "no cpu in machine topology")]
    fn max_spreader_with_topology_empty() {
        let topology = Vec::new();
        MaxSpreader::from_topology(topology).unwrap();
    }

    #[test]
    #[should_panic(expected = "no cpu in machine topology")]
    fn max_packer_with_topology_empty() {
        let topology = Vec::new();
        MaxPacker::from_topology(topology).unwrap();
    }

    #[test]
    fn max_spreader_with_topology_0() {
        let mut counts = [0; 4];
        let topology = vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
        ];

        let mut max_spreader = MaxSpreader::from_topology(topology).unwrap();
        assert_eq!(4, max_spreader.tree.nr_slots());
        assert_eq!(0, max_spreader.tree.nr_slots_selected());

        for _ in 0..4 {
            let cpu_location = max_spreader.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(counts, [1, 1, 1, 1]);
        assert_eq!(4, max_spreader.tree.nr_slots_selected());

        for _ in 0..4 {
            let cpu_location = max_spreader.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(counts, [2, 2, 2, 2]);
        assert_eq!(8, max_spreader.tree.nr_slots_selected());

        for _ in 0..4 * 8 {
            let cpu_location = max_spreader.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(counts, [10, 10, 10, 10]);
        assert_eq!(40, max_spreader.tree.nr_slots_selected());

        assert_eq!(4, max_spreader.tree.nr_slots());
    }

    #[test]
    fn max_spreader_with_topology_1() {
        let mut counts = [0; 4];
        let topology = vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 1, 3),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 1),
        ];

        let mut max_spreader = MaxSpreader::from_topology(topology).unwrap();
        assert_eq!(4, max_spreader.tree.nr_slots());

        for _ in 0..2 {
            let cpu_location = max_spreader.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(1, counts[..3].iter().sum());
        assert_eq!(1, counts[3]);

        for _ in 2..4 {
            let cpu_location = max_spreader.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(counts, [1, 1, 1, 1]);
        assert_eq!(4, max_spreader.tree.nr_slots_selected());

        for _ in 4..6 {
            let cpu_location = max_spreader.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(4, counts[..3].iter().sum());
        assert_eq!(2, counts[3]);

        for _ in 6..8 {
            let cpu_location = max_spreader.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(counts, [2, 2, 2, 2]);
        assert_eq!(8, max_spreader.tree.nr_slots_selected());

        assert_eq!(4, max_spreader.tree.nr_slots());
    }

    #[test]
    fn max_spreader_with_topology_2() {
        let mut numa = 0;
        let mut pkg = 0;
        let mut core = 0;
        let mut cpu = 0;

        // level[0] is number of numa nodes
        // ...
        // level[3] is number of cpus
        let levels = vec![2, 3, 5, 7];
        let nr_cpu = levels.iter().product();

        let mut counts = vec![0; nr_cpu];
        let mut topology = Vec::new();

        #[allow(clippy::explicit_counter_loop)]
        for _ in 0..levels[0] {
            for _ in 0..levels[1] {
                for _ in 0..levels[2] {
                    for _ in 0..levels[3] {
                        topology.push(cpu_loc(numa, pkg, core, cpu));
                        cpu += 1;
                    }
                    core += 1;
                }
                pkg += 1;
            }
            numa += 1;
        }

        let mut max_spreader = MaxSpreader::from_topology(topology).unwrap();

        let mut selected_prev = 0;
        let mut selected = 1;
        for nn in levels.into_iter() {
            selected *= nn;
            for _ in 0..selected - selected_prev {
                let cpu_location = max_spreader.next().unwrap();
                counts[cpu_location.cpu] += 1;
            }
            counts
                .chunks(nr_cpu / selected)
                .for_each(|c| assert_eq!(1, c.iter().sum()));
            assert_eq!(selected, max_spreader.tree.nr_slots_selected());
            selected_prev = selected;
        }
    }

    #[test]
    fn max_packer_with_topology_0() {
        let mut counts = [0; 7];
        let topology = vec![
            cpu_loc(0, 0, 1, 6),
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 1, 2, 5),
            cpu_loc(1, 2, 3, 4),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
        ];

        super::linux_sysfs::test_helpers::check_topolgy(topology.clone());

        let mut max_packer = MaxPacker::from_topology(topology).unwrap();
        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(1, counts[..4].iter().sum());
        assert_eq!(0, counts[5..].iter().sum());

        for _ in 1..4 {
            let cpu_location = max_packer.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(4, counts[..4].iter().sum());
        assert_eq!(0, counts[5..].iter().sum());

        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(1, counts[6]);
        assert_eq!(5, counts.iter().sum());

        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(1, counts[5]);
        assert_eq!(6, counts.iter().sum());

        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(1, counts[4]);
        assert_eq!(7, counts.iter().sum());

        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(5, counts[..4].iter().sum());
        assert_eq!(8, counts.iter().sum());

        for _ in 8..10 * counts.len() {
            let cpu_location = max_packer.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(counts, [10, 10, 10, 10, 10, 10, 10]);
    }

    #[test]
    fn max_packer_numa_in_pkg() {
        let mut counts = [0; 7];

        // numa node nodes 0 and 1 are both in package 0
        let topology = vec![
            cpu_loc(0, 0, 1, 4),
            cpu_loc(0, 0, 0, 5),
            cpu_loc(0, 0, 0, 6),
            cpu_loc(1, 0, 2, 3),
            cpu_loc(2, 1, 4, 1),
            cpu_loc(2, 1, 4, 2),
            cpu_loc(3, 1, 3, 0),
        ];

        super::linux_sysfs::test_helpers::check_topolgy(topology.clone());

        let mut max_packer = MaxPacker::from_topology(topology).unwrap();
        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(1, counts[4..].iter().sum());
        assert_eq!(0, counts[..4].iter().sum());

        for _ in 1..3 {
            let cpu_location = max_packer.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(3, counts[4..].iter().sum());
        assert_eq!(0, counts[..4].iter().sum());

        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(4, counts[3..].iter().sum());
        assert_eq!(0, counts[..3].iter().sum());

        for _ in 4..6 {
            let cpu_location = max_packer.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(6, counts[1..].iter().sum());
        assert_eq!(0, counts[..1].iter().sum());

        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(1, *counts.iter().max().unwrap());

        for _ in 7..10 * counts.len() {
            let cpu_location = max_packer.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(counts, [10, 10, 10, 10, 10, 10, 10]);
    }
}
