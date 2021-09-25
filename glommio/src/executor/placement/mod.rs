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
//! The implementation represents the topology as a tree, which can be used to
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
//!    packing iterator such that some CPUs have been selected and others have
//!    not), then it takes priority
//! 2) if the `Node` is not partially saturated, then it is either equally
//!    saturated with the other `Node`s at its `Level` or one `Node` has a
//!    greater saturation than the other:
//!     a) if `Node`s are equally saturated, use the `Node` with the greater
//!     number of total slots (i.e. greater number of CPUs that are online)
//!     b) if `Node`s are not equally saturated, use the node with lower
//!     saturation
//!
//! `Node:::select_cpu` is used to recursively proceed through levels in the
//! tree until a `Cpu` is selected.  The `Nodes` which were traversed in
//! reaching the `Cpu` are reflected in the `Path` returned by
//! `Node::select_cpu`.
//!
//! Note that the NUMA distance or amount of memory available on each `Node` is
//! not currently considered in selecting CPUs.
//!
//! Some helpful references:
//! <https://www.kernel.org/doc/html/latest/admin-guide/cputopology.html>
//! <https://www.kernel.org/doc/html/latest/vm/numa.html>
//! <https://www.kernel.org/doc/html/latest/core-api/cpu_hotplug.html>

mod pq_tree;

use crate::{error::BuilderErrorKind, sys::hardware_topology, CpuLocation, GlommioError};

use pq_tree::{
    marker::{Pack, Priority, Spread},
    Level,
    Node,
};
use std::{
    collections::hash_map::RandomState,
    collections::hash_set::{Difference, Intersection, IntoIter, Iter, SymmetricDifference, Union},
    collections::HashSet,
    convert::TryInto,
    iter::FromIterator,
};

type Result<T> = crate::Result<T, ()>;

#[cfg(doc)]
use super::{LocalExecutor, LocalExecutorPoolBuilder};

/// Specifies a policy by which [`LocalExecutorPoolBuilder`] selects CPUs.
///
/// `Placement` is use to bind [`LocalExecutor`]s to a set of CPUs via
/// preconfigured policies designed to address a variety of use cases.  The
/// default is `Unbound`.
///
/// ## Example
///
/// Some `Placement`s allow manually filtering available CPUs via a [`CpuSet`],
/// such as `MaxSpread`.  The following would place shards on four CPUs
/// (a.k.a.  hyper-threads) that are on NUMA node 0 and have an even numbered
/// package ID according to their [`CpuLocation`]. The selection aims to achieve
/// a high degree of separation between the CPUs in terms of machine topology.
/// Each [`LocalExecutor`] would be bound to a single CPU.
///
/// Note that if four CPUs are not available, the call to
/// [`LocalExecutorPoolBuilder::on_all_shards`] would return an `Err` when using
/// `MaxSpread`.
///
/// ```no_run
/// use glommio::{CpuSet, LocalExecutorPoolBuilder, Placement};
///
/// let cpus = CpuSet::online()
///     .expect("Err: please file an issue with glommio")
///     .filter(|l| l.numa_node == 0)
///     .filter(|l| l.package % 2 == 0);
///
/// let handles = LocalExecutorPoolBuilder::new(4)
///     .placement(Placement::MaxSpread(Some(cpus)))
///     .on_all_shards(|| async move {
///         // ... important stuff ...
///     })
///     .unwrap();
///
/// handles.join_all();
/// ```
#[derive(Clone, Debug, PartialEq)]
pub enum Placement {
    /// For the `Unbound` variant, the [`LocalExecutor`]s created by a
    /// [`LocalExecutorPoolBuilder`] are not bound to any CPU.  This is the
    /// default placement.
    ///
    /// [`LocalExecutor`]: super::LocalExecutor
    /// [`LocalExecutorPoolBuilder`]: super::LocalExecutorPoolBuilder
    Unbound,
    /// The `Fenced` variant binds each [`LocalExecutor`] to the set of
    /// CPUs specified by [`CpuSet`].  With an unfiltered CPU
    /// set returned by [`CpuSet::online`], this is similar to using `Unbound`
    /// with the distinction that bringing additional CPUs online will not
    /// allow the executors to run on the newly available CPUs.  The
    /// `Fenced` variant allows the number of shards specified in
    /// [`LocalExecutorPoolBuilder::new`] to be greater than the number of CPUs
    /// as long as at least one CPU is included in `CpuSet`.
    ///
    /// #### Errors
    ///
    /// If the provided [`CpuSet`] contains no CPUs, a call to
    /// [`LocalExecutorPoolBuilder::on_all_shards`] will return `Result::
    /// Err`.
    Fenced(CpuSet),
    /// Each [`LocalExecutor`] is pinned to a particular [`CpuLocation`] such
    /// that the set of all CPUs selected has a high degree of sepration.
    /// The selection proceeds from all CPUs that are online in a
    /// non-deterministic manner.  The `Option<CpuSet>` parameter may be used to
    /// restrict the [`CpuSet`] from which CPUs are selected; specifying `None`
    /// is equivalent to using `Some(CpuSet::online()?)`.
    ///
    /// #### Errors
    ///
    /// If the number of shards specified in [`LocalExecutorPoolBuilder::new`]
    /// is greater than the number of CPUs available, then a call to
    /// [`LocalExecutorPoolBuilder::on_all_shards`] will return `Result::
    /// Err`.
    MaxSpread(Option<CpuSet>),
    /// Each [`LocalExecutor`] is pinned to a particular [`CpuLocation`] such
    /// that the set of all CPUs selected has a low degree of sepration.
    /// The selection proceeds from all CPUs that are online in a
    /// non-deterministic manner.  The `Option<CpuSet>` parameter may be used to
    /// restrict the [`CpuSet`] from which CPUs are selected; specifying `None`
    /// is equivalent to using `Some(CpuSet::online()?)`.
    ///
    /// #### Errors
    ///
    /// If the number of shards specified in [`LocalExecutorPoolBuilder::new`]
    /// is greater than the number of CPUs available, then a call to
    /// [`LocalExecutorPoolBuilder::on_all_shards`] will return `Result::
    /// Err`.
    MaxPack(Option<CpuSet>),
    /// One [`LocalExecutor`] is bound to each of the [`CpuSet`]s specified by
    /// `Custom`. The number of `CpuSet`s in the `Vec` should match the
    /// number of shards requested from the pool builder.
    ///
    /// #### Errors
    ///
    /// [`LocalExecutorPoolBuilder::on_all_shards`] will return `Result::Err` if
    /// either of the follow is true:
    /// * The length of the vector does not match the number of shards requested
    ///   in [`LocalExecutorPoolBuilder::new`].
    /// * Any of the [`CpuSet`]s is empty.
    Custom(Vec<CpuSet>),
}

/// The default is `Placement::Unbound`
impl Default for Placement {
    fn default() -> Self {
        Self::Unbound
    }
}

/// Used to specify a set of permitted CPUs on which
/// executors created by a
/// [`LocalExecutorPoolBuilder`](super::LocalExecutorPoolBuilder) are run.
///
/// Please see the documentation for [`Placement`] variants to
/// understand how `CpuSet` restrictions apply to each variant.  CPUs are
/// identified via their [`CpuLocation`].
#[derive(Clone, Debug, PartialEq)]
pub struct CpuSet(HashSet<CpuLocation>);

impl FromIterator<CpuLocation> for CpuSet
{
    fn from_iter<I: IntoIterator<Item = CpuLocation>>(cpus: I) -> Self {
        Self(HashSet::<CpuLocation>::from_iter(cpus.into_iter()))
    }
}

impl<'a> IntoIterator for &'a CpuSet {
    type Item = &'a CpuLocation;
    type IntoIter = Iter<'a, CpuLocation>;

    #[inline]
    fn into_iter(self) -> Iter<'a, CpuLocation> {
        self.0.iter()
    }
}

impl IntoIterator for CpuSet {
    type Item = CpuLocation;
    type IntoIter = IntoIter<CpuLocation>;

    #[inline]
    fn into_iter(self) -> IntoIter<CpuLocation> {
        self.0.into_iter()
    }
}

impl CpuSet {
    /// Creates a `CpuSet` representing all CPUs that are online.
    /// The function will return an `Err` if the hardware topology could not
    /// be obtained from this machine.
    pub fn online() -> Result<Self> {
        let topo = hardware_topology::get_machine_topology_unsorted()?;
        Ok(Self::from_iter(topo))
    }

    /// This method can be used to restrict the CPUs held by `CpuSet`.  The
    /// resulting `CpuSet` will only include [`CpuLocation`]s for which the
    /// provided closure returns `true`. Note that each call to `filter`
    /// will use as input the set of CPUs previously selected (i.e. the set is
    /// *not* reset on each call).
    ///
    /// ```
    /// use glommio::CpuSet;
    ///
    /// // get CPUs on NUMA node 0
    /// let cpus = CpuSet::online()
    ///     .expect("Err: please file an issue with glommio")
    ///     .filter(|l| l.numa_node == 0);
    ///
    /// println!("The filtered CPUs are: {:#?}", cpus);
    /// ```
    pub fn filter<F>(mut self, f: F) -> Self
    where
        F: FnMut(&CpuLocation) -> bool,
    {
        self.0 = self.0.into_iter().filter(f).collect();
        self
    }

    /// Returns a reference to the [`CpuLocation`]s currently included in the
    /// `CpuSet`.
    pub fn as_vec(&self) -> Vec<&CpuLocation> {
        self.0.iter().collect()
    }

    /// Checks whether the `CpuSet` is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// An iterator visiting all [`CpuLocation`]s in this CpuSet in arbitrary order.
    pub fn iter(&self) -> Iter<'_, CpuLocation> {
        self.0.iter()
    }

    /// Returns the number of CPUs included in the `CpuSet`.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Consumes the `CpuSet` and returns the [`CpuLocation`]s.
    fn take(mut self) -> Vec<CpuLocation> {
        self.0.drain().collect()
    }

    // Delegate Set implementation
    /// Returns `true` if the `CpuSet` contains a `CpuLocation`.
    pub fn contains(&self, value: &CpuLocation) -> bool {
        self.0.contains(value)
    }

    /// Visits the [`CpuLocation`]s representing the difference, i.e., the values that are in self but not in
    /// other.
    pub fn difference<'a>(&'a self, other: &'a Self) -> Difference<'a, CpuLocation, RandomState> {
        self.0.difference(&other.0)
    }

    /// Visits the [`CpuLocation`]s representing the intersection, i.e., the values that are both in self and
    /// other.
    pub fn intersection<'a>(&'a self, other: &'a Self) -> Intersection<'a, CpuLocation, RandomState> {
        self.0.intersection(&other.0)
    }

    /// Returns true if self has no [`CpuLocation`]s in common with other. This is equivalent to checking
    /// for an empty intersection.
    pub fn is_disjoint(&self, other: &Self) -> bool {
        self.0.is_disjoint(&other.0)
    }

    /// Returns true if this `CpuSet` is a subset of another, i.e., other contains at least all the
    /// values in self.
    pub fn is_subset(&self, other: &Self) -> bool {
        self.0.is_subset(&other.0)
    }

    /// Returns true if this `CpuSet` is a superset of another, i.e., self contains at least all the
    /// values in other.
    pub fn is_superset(&self, other: &Self) -> bool {
        self.0.is_superset(&other.0)
    }

    /// Visits the [`CpuLocation`]s representing the symmetric difference, i.e., the values that are in self
    /// or in other but not in both.
    pub fn symmetric_difference<'a>(&'a self, other: &'a Self) -> SymmetricDifference<'a, CpuLocation, RandomState> {
        self.0.symmetric_difference(&other.0)
    }

    /// Visits the [`CpuLocation`]s representing the union, i.e., all the values in self or other, without
    /// duplicates.
    pub fn union<'a>(&'a self, other: &'a Self) -> Union<'a, CpuLocation, RandomState> {
        self.0.union(&other.0)
    }
}

/// Iterates over a set of CPUs associated with a [`LocalExecutor`] when created
/// via a [`LocalExecutorPoolBuilder`].
#[derive(Clone, Debug)]
pub(crate) enum CpuIter {
    Unbound,
    Single(CpuLocation),
    Multi(Vec<CpuLocation>),
}

impl CpuIter {
    fn from_vec(v: Vec<CpuLocation>) -> Self {
        Self::Multi(v)
    }

    fn from_option(cpu_loc: Option<CpuLocation>) -> Self {
        match cpu_loc {
            None => Self::Unbound,
            Some(cpu) => Self::Single(cpu),
        }
    }

    pub fn cpu_binding(self) -> Option<impl IntoIterator<Item = usize>> {
        match self {
            Self::Unbound => None,
            Self::Single(_) | Self::Multi(_) => Some(self.map(|l| l.cpu)),
        }
    }
}

impl Iterator for CpuIter {
    type Item = CpuLocation;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Unbound => None,
            Self::Single(_) => match std::mem::replace(self, Self::Unbound) {
                Self::Single(cpu) => Some(cpu),
                _ => unreachable!("expected CpuIter::Single"),
            },
            Self::Multi(v) => v.pop(),
        }
    }
}

/// Generates CPU sets as iterators.  The sets generated depend on the specified
/// [`Placement`].
pub(crate) enum CpuSetGenerator {
    Unbound,
    Fenced(CpuSet),
    MaxSpread(MaxSpreader),
    MaxPack(MaxPacker),
    Custom(Vec<CpuSet>),
}

impl CpuSetGenerator {
    pub fn new(placement: Placement, nr_shards: usize) -> Result<Self> {
        let this = match placement {
            Placement::Unbound => Self::Unbound,
            Placement::Fenced(cpus) => {
                Self::check_nr_cpus(1, &cpus)?;
                Self::Fenced(cpus)
            }
            Placement::MaxSpread(cpus) => {
                let cpus = match cpus {
                    Some(cpus) => cpus,
                    None => CpuSet::online()?,
                };
                Self::check_nr_cpus(nr_shards, &cpus)?;
                Self::MaxSpread(MaxSpreader::from_cpu_set(cpus))
            }
            Placement::MaxPack(cpus) => {
                let cpus = match cpus {
                    Some(cpus) => cpus,
                    None => CpuSet::online()?,
                };
                Self::check_nr_cpus(nr_shards, &cpus)?;
                Self::MaxPack(MaxPacker::from_cpu_set(cpus))
            }
            Placement::Custom(cpu_sets) => {
                if cpu_sets.len() != nr_shards {
                    return Err(GlommioError::BuilderError(BuilderErrorKind::NrShards {
                        cpu_sets: cpu_sets.len(),
                        shards: nr_shards,
                    }));
                }
                for cpu_set in &cpu_sets {
                    Self::check_nr_cpus(1, cpu_set)?;
                }
                Self::Custom(cpu_sets)
            }
        };
        Ok(this)
    }

    fn check_nr_cpus(required: usize, cpu_set: &CpuSet) -> Result<()> {
        let available = cpu_set.len();
        if required <= available {
            Ok(())
        } else {
            Err(GlommioError::BuilderError(
                BuilderErrorKind::InsufficientCpus {
                    required,
                    available,
                },
            ))
        }
    }

    /// A method that generates a [`CpuIter`] according to the provided
    /// [`Placement`] policy. Sequential calls may generate different sets
    /// depending on the [`Placement`].
    pub fn next(&mut self) -> CpuIter {
        match self {
            Self::Unbound => CpuIter::Unbound,
            Self::Fenced(cpus) => CpuIter::from_vec(cpus.clone().into_iter().collect()),
            Self::MaxSpread(it) => CpuIter::from_option(it.next()),
            Self::MaxPack(it) => CpuIter::from_option(it.next()),
            Self::Custom(cpu_sets) => {
                CpuIter::Multi(cpu_sets.pop().expect("insufficient cpu sets").take())
            }
        }
    }
}

/// An [`Iterator`] over [`CpuLocation`]s in the machine topology which have a
/// high degree of separation from previous [`CpuLocation`]s returned by
/// `MaxSpreader`.  The order in which items are returned by `MaxSpreader` is
/// non-deterministic.  Unless the [`MaxSpreader::new`] is called with an empty
/// `Vec` as arguments, iterating `MaxSpreader` will never return `Option::None`
/// and will cycle through [`CpuLocation`], where each cycle itself is
/// non-deterministic (i.e.  the order may differ from prior cycles).
type MaxSpreader = TopologyIter<Spread>;

/// An [`Iterator`] over [`CpuLocation`]s in the machine topology which have a
/// low degree of separation from previous [`CpuLocation`]s returned by
/// `MaxPacker`.  Unless the [`MaxPacker::new`] is called with an empty
/// `Vec` as arguments, iterating `MaxPacker` will never return `Option::None`
/// and will cycle through [`CpuLocation`] non-deterministically.
type MaxPacker = TopologyIter<Pack>;

pub(crate) struct TopologyIter<T> {
    tree: Node<T>,
}

impl<T: Priority> Iterator for TopologyIter<T> {
    type Item = CpuLocation;
    fn next(&mut self) -> Option<Self::Item> {
        self.tree.select_cpu().try_into().ok()
    }
}

impl<T> TopologyIter<T>
where
    T: Priority,
{
    fn from_cpu_set(cpus: CpuSet) -> Self {
        Self::from_topology(cpus.into_iter().collect())
    }

    /// Construct a `TopologyIter` from a `Vec<CpuLocation>`.  The tree of
    /// priority queues is constructed sequentially (using the change in
    /// `Node` ID to indicate that a `Node` is ready to be pushed onto its
    /// parent), so IDs at a particular level should be unique (e.g. a
    /// `Core` with ID 0 should not exist on `Package`s with IDs 0 and 1).
    pub fn from_topology(mut topology: Vec<CpuLocation>) -> Self {
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
                    _ => unreachable!("unexpected tree level: {}", depth),
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
                    _ => unreachable!("unexpected tree level: {}", depth),
                }
            }
        };

        let mut node_root = Node::<T>::new(Level::SystemRoot);
        let mut iter = topology.into_iter();
        if let mut next @ Some(_) = iter.next() {
            Self::build_sub_tree(&mut node_root, &mut None, &mut next, &mut iter, &f_level, 1);
        }
        Self { tree: node_root }
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
mod test {
    use super::*;
    use crate::sys::hardware_topology::test_helpers::cpu_loc;

    #[test]
    fn cpu_set() {
        let set = CpuSet::online().unwrap();
        let len = set.len();
        assert!(len > 0);
        let set = set.filter(|_| true);
        assert_eq!(len, set.len());
        let set = set.filter(|_| false);
        assert_eq!(0, set.len());
        assert!(set.is_empty());
        let v: Vec<_> = set.into_iter().collect();
        assert_eq!(0, v.len());
    }

    #[test]
    fn placement_unbound_clone() {
        assert!(matches!(Placement::Unbound.clone(), Placement::Unbound));
    }

    #[test]
    fn placement_fenced_clone() {
        let set = CpuSet::online().unwrap();
        let placement = Placement::Fenced(set);
        let placement_clone = placement.clone();
        assert_eq!(placement_clone, placement);
    }

    #[test]
    fn placement_max_spread_clone() {
        let set = CpuSet::online().unwrap();
        let some_placement = Placement::MaxSpread(Some(set));
        let some_placement_clone = some_placement.clone();
        assert_eq!(some_placement_clone, some_placement);
        let none_placement = Placement::MaxSpread(None);
        let none_placement_clone = none_placement.clone();
        assert_eq!(none_placement_clone, none_placement);
    }

    #[test]
    fn placement_max_pack_clone() {
        let set = CpuSet::online().unwrap();
        let some_placement = Placement::MaxPack(Some(set));
        let some_placement_clone = some_placement.clone();
        assert_eq!(some_placement_clone, some_placement);
        let none_placement = Placement::MaxPack(None);
        let none_placement_clone = none_placement.clone();
        assert_eq!(none_placement_clone, none_placement);
    }

    #[test]
    fn placement_custom_clone() {
        let set1 = CpuSet::online().unwrap();
        let set2 = CpuSet::online().unwrap();
        assert!(!set1.is_empty());
        assert!(!set2.is_empty());
        let vec_set = vec![set1, set2];
        let placement = Placement::Custom(vec_set);
        assert_eq!(placement.clone(), placement);
    }

    #[test]
    fn max_spreader_this_machine() {
        let n = 4096;
        let mut max_spreader = MaxSpreader::from_cpu_set(CpuSet::online().unwrap());

        assert_eq!(0, max_spreader.tree.nr_slots_selected());
        for _ in 0..n {
            let _cpu_location: CpuLocation = max_spreader.next().unwrap();
        }
        assert_eq!(n, max_spreader.tree.nr_slots_selected());
    }

    #[test]
    fn max_packer_this_machine() {
        let n = 4096;
        let mut max_packer = MaxSpreader::from_cpu_set(CpuSet::online().unwrap());

        for _ in 0..n {
            let _cpu_location: CpuLocation = max_packer.next().unwrap();
        }
    }

    #[test]
    fn max_spreader_with_topology_empty() {
        let topology = Vec::new();
        assert!(MaxSpreader::from_topology(topology).next().is_none());
    }

    #[test]
    fn max_packer_with_topology_empty() {
        let topology = Vec::new();
        assert!(MaxPacker::from_topology(topology).next().is_none());
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

        let mut max_spreader = MaxSpreader::from_topology(topology);
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

        let mut max_spreader = MaxSpreader::from_topology(topology);
        assert_eq!(4, max_spreader.tree.nr_slots());

        for _ in 0..2 {
            let cpu_location = max_spreader.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(1, counts[..3].iter().sum::<i32>());
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
        assert_eq!(4, counts[..3].iter().sum::<i32>());
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

        let mut max_spreader = MaxSpreader::from_topology(topology);

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
                .for_each(|c| assert_eq!(1, c.iter().sum::<i32>()));
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

        super::hardware_topology::test_helpers::check_topolgy(topology.clone());

        let mut max_packer = MaxPacker::from_topology(topology);
        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(1, counts[..4].iter().sum::<i32>());
        assert_eq!(0, counts[5..].iter().sum::<i32>());

        for _ in 1..4 {
            let cpu_location = max_packer.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(4, counts[..4].iter().sum::<i32>());
        assert_eq!(0, counts[5..].iter().sum::<i32>());

        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(1, counts[6]);
        assert_eq!(5, counts.iter().sum::<i32>());

        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(1, counts[5]);
        assert_eq!(6, counts.iter().sum::<i32>());

        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(1, counts[4]);
        assert_eq!(7, counts.iter().sum::<i32>());

        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(5, counts[..4].iter().sum::<i32>());
        assert_eq!(8, counts.iter().sum::<i32>());

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

        super::hardware_topology::test_helpers::check_topolgy(topology.clone());

        let mut max_packer = MaxPacker::from_topology(topology);
        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(1, counts[4..].iter().sum::<i32>());
        assert_eq!(0, counts[..4].iter().sum::<i32>());

        for _ in 1..3 {
            let cpu_location = max_packer.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(3, counts[4..].iter().sum::<i32>());
        assert_eq!(0, counts[..4].iter().sum::<i32>());

        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(4, counts[3..].iter().sum::<i32>());
        assert_eq!(0, counts[..3].iter().sum::<i32>());

        for _ in 4..6 {
            let cpu_location = max_packer.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(6, counts[1..].iter().sum::<i32>());
        assert_eq!(0, counts[..1].iter().sum::<i32>());

        let cpu_location = max_packer.next().unwrap();
        counts[cpu_location.cpu] += 1;
        assert_eq!(1, *counts.iter().max().unwrap());

        for _ in 7..10 * counts.len() {
            let cpu_location = max_packer.next().unwrap();
            counts[cpu_location.cpu] += 1;
        }
        assert_eq!(counts, [10, 10, 10, 10, 10, 10, 10]);
    }

    #[test]
    fn custom_placement() {
        let set1 = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
        ]);
        let set2 = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(1, 1, 1, 4),
            cpu_loc(0, 0, 0, 1),
        ]);
        let set3 = CpuSet::from_iter(vec![]);

        {
            let p = Placement::Custom(vec![set1.clone(), set2.clone()]);
            let mut gen = CpuSetGenerator::new(p, 2).unwrap();
            let mut bindings = vec![];
            for _ in 0..2 {
                let v = gen
                    .next()
                    .cpu_binding()
                    .unwrap()
                    .into_iter()
                    .collect::<Vec<_>>();
                bindings.push(v);
            }
            assert_eq!(bindings, vec![vec![1, 4, 0, 5], vec![1, 3, 0, 2]]);
        }
        {
            let p = Placement::Custom(vec![set1.clone(), set2.clone()]);
            assert!(CpuSetGenerator::new(p, 1).is_err());
        }
        {
            let p = Placement::Custom(vec![set1.clone(), set2.clone()]);
            assert!(CpuSetGenerator::new(p, 3).is_err());
        }
        {
            let p = Placement::Custom(vec![set1, set2, set3]);
            assert!(CpuSetGenerator::new(p, 3).is_err());
        }
    }

    // Set API
    #[test]
    fn cpuset_disjoint() {
        let xs = CpuSet::from_iter(vec![]);
        let ys = CpuSet::from_iter(vec![]);
        assert!(xs.is_disjoint(&ys));
        assert!(ys.is_disjoint(&xs));

        let xs = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
        ]);
        let ys = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
        ]);
        assert!(xs.is_disjoint(&ys));
        assert!(ys.is_disjoint(&xs));

        let xs = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
        ]);
        let ys = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
            cpu_loc(1, 1, 1, 4),
            cpu_loc(1, 1, 1, 3),
        ]);
        assert!(xs.is_disjoint(&ys));
        assert!(ys.is_disjoint(&xs));
    }

    #[test]
    fn cpuset_subset_and_superset() {
        let xs = CpuSet::from_iter(vec![]);
        let ys = CpuSet::from_iter(vec![]);
        assert!(xs.is_subset(&ys));
        assert!(xs.is_superset(&ys));
        assert!(ys.is_subset(&xs));
        assert!(ys.is_superset(&xs));

        let xs = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
        ]);
        let ys = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
        ]);
        assert!(!xs.is_subset(&ys));
        assert!(!xs.is_superset(&ys));
        assert!(!ys.is_subset(&xs));
        assert!(!ys.is_superset(&xs));

        let xs = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
        ]);
        let ys = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(1, 1, 1, 4),
            cpu_loc(0, 0, 0, 1),
        ]);
        assert!(!xs.is_subset(&ys));
        assert!(!xs.is_superset(&ys));
        assert!(!ys.is_subset(&xs));
        assert!(!ys.is_superset(&xs));

        let xs = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
        ]);
        let ys = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(1, 1, 1, 5),
        ]);
        assert!(xs.is_subset(&ys));
        assert!(!xs.is_superset(&ys));
        assert!(!ys.is_subset(&xs));
        assert!(ys.is_superset(&xs));

        let xs = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
            cpu_loc(0, 0, 0, 2),
        ]);
        let ys = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
        ]);
        assert!(!xs.is_subset(&ys));
        assert!(xs.is_superset(&ys));
        assert!(ys.is_subset(&xs));
        assert!(!ys.is_superset(&xs));
    }

    #[test]
    fn cpuset_iterate() {
        let a = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
        ]);
        let mut observed: u32 = 0;
        for _ in &a {
            observed += 1
        }
        assert_eq!(observed, 4);
    }

    #[test]
    fn cpuset_intersection() {
        let a = CpuSet::from_iter(vec![]);
        let b = CpuSet::from_iter(vec![]);
        assert!(a.intersection(&b).next().is_none());

        let a = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
        ]);
        let b = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(1, 1, 1, 4),
            cpu_loc(0, 0, 0, 1),
        ]);

        let mut i = 0;
        let expected = [
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 1),
        ];
        for x in a.intersection(&b) {
            assert!(expected.contains(x));
            i += 1
        }
        assert_eq!(i, expected.len());

        // make a bigger than b
        let a = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
            cpu_loc(0, 0, 0, 4),
        ]);
        let b = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(1, 1, 1, 4),
            cpu_loc(0, 0, 0, 1),
        ]);

        i = 0;
        for x in a.intersection(&b) {
            assert!(expected.contains(x));
            i += 1
        }
        assert_eq!(i, expected.len());

        i = 0;
        for x in b.intersection(&a) {
            assert!(expected.contains(x));
            i += 1
        }
        assert_eq!(i, expected.len());
    }

    #[test]
    fn cpuset_difference() {
        let a = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
            cpu_loc(0, 0, 0, 4),
        ]);
        let b = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(1, 1, 1, 4),
            cpu_loc(0, 0, 0, 1),
        ]);

        let mut i = 0;
        let expected = [
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 4),
        ];
        for x in a.difference(&b) {
            assert!(expected.contains(x));
            i += 1
        }
        assert_eq!(i, expected.len());
    }

    #[test]
    fn cpuset_symmetric_difference() {
        let a = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
            cpu_loc(0, 0, 0, 4),
        ]);
        let b = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(1, 1, 1, 4),
            cpu_loc(0, 0, 0, 1),
            cpu_loc(1, 1, 1, 3),
        ]);

        let mut i = 0;
        let expected = [
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 4),
            cpu_loc(1, 1, 1, 5),
            cpu_loc(1, 1, 1, 4),
            cpu_loc(1, 1, 1, 3),
        ];
        for x in a.symmetric_difference(&b) {
            assert!(expected.contains(x));
            i += 1
        }
        assert_eq!(i, expected.len());
    }

    #[test]
    fn cpuset_union() {
        let a = CpuSet::from_iter(vec![]);
        let b = CpuSet::from_iter(vec![]);
        assert!(a.union(&b).next().is_none());
        assert!(b.union(&a).next().is_none());
        let a = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
            cpu_loc(0, 0, 0, 4),
        ]);
        let b = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(1, 1, 1, 4),
            cpu_loc(0, 0, 0, 1),
            cpu_loc(1, 1, 1, 3),
        ]);

        let mut i = 0;
        let expected = [
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
            cpu_loc(0, 0, 0, 4),
            cpu_loc(1, 1, 1, 5),
            cpu_loc(1, 1, 1, 4),
            cpu_loc(1, 1, 1, 3),
        ];
        for x in a.union(&b) {
            assert!(expected.contains(x));
            i += 1
        }
        assert_eq!(i, expected.len());

        // make a bigger than b
        let a = CpuSet::from_iter(vec![
            cpu_loc(0, 0, 0, 2),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 0, 0, 3),
            cpu_loc(0, 0, 0, 1),
            cpu_loc(0, 0, 0, 4),
            cpu_loc(1, 1, 1, 3),
        ]);
        let b = CpuSet::from_iter(vec![
            cpu_loc(1, 1, 1, 5),
            cpu_loc(0, 0, 0, 0),
            cpu_loc(1, 1, 1, 4),
            cpu_loc(0, 0, 0, 1),
            cpu_loc(1, 1, 1, 3),
        ]);


        i = 0;
        for x in a.union(&b) {
            assert!(expected.contains(x));
            i += 1
        }
        assert_eq!(i, expected.len());

        i = 0;
        for x in b.union(&a) {
            assert!(expected.contains(x));
            i += 1
        }
        assert_eq!(i, expected.len());
    }
}
