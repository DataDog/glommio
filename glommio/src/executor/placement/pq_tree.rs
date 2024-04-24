// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.
//

use std::{cmp::Ordering, collections::BinaryHeap, marker::PhantomData};

use super::CpuLocation;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Level {
    SystemRoot,
    Cpu(usize),
    Core(usize),
    Package(usize),
    NumaNode(usize),
}

/// `Node`s are connected to build a tree.  The `children` of each `Node` have a
/// `Priority` which specifies a means by which a child is selected in choosing
/// the next CPU to be returned by the iterator.
#[derive(Debug)]
pub struct Node<T: ?Sized> {
    /// Hardware level containing an id e.g. `Level::Package(2)`
    level: Level,
    /// Total number of CPUs in this node
    nr_slots: usize,
    /// Number of CPUs in this node that have been selected
    nr_slots_selected: usize,
    /// Child nodes
    children: BinaryHeap<Self>,
    /// A marker to specialize the priority queue order e.g. `marker::Spread`
    _marker: PhantomData<T>,
}

impl<T> Node<T>
where
    T: marker::Priority,
{
    pub fn new(level: Level) -> Self {
        Self {
            level,
            nr_slots: 1,
            nr_slots_selected: 0,
            children: BinaryHeap::new(),
            _marker: PhantomData,
        }
    }

    pub fn push_child(&mut self, node: Self) {
        if self.children.is_empty() {
            self.nr_slots = 0;
        }
        self.nr_slots += node.nr_slots;
        self.children.push(node)
    }

    pub fn level(&self) -> &Level {
        &self.level
    }

    fn slot_saturation(&self) -> (usize, usize) {
        (self.nr_slots_selected, self.nr_slots)
    }

    fn cmp_saturation(lhs: (usize, usize), rhs: (usize, usize)) -> Ordering {
        (lhs.0 * rhs.1).cmp(&(rhs.0 * lhs.1))
    }

    fn priority_spread(&self, other: &Self) -> Ordering {
        let ord = Self::cmp_saturation(self.slot_saturation(), other.slot_saturation());
        // use as min heap: smaller values have higher priority
        ord.reverse()
    }

    fn priority_pack(&self, other: &Self) -> Ordering {
        // if a node is partially saturated (i.e. currently being filled by the packing
        // iterator), then it takes priority
        if self.nr_slots_selected % self.nr_slots != 0 {
            Ordering::Greater
        } else if other.nr_slots_selected % other.nr_slots != 0 {
            Ordering::Less
        } else {
            // if the node is not partially saturated, then it is either equally saturated
            // or one node has a greater saturation than the other (note that
            // integer division here is exact because we checked for remainders
            // above)
            match (self.nr_slots_selected / self.nr_slots)
                .cmp(&(other.nr_slots_selected / other.nr_slots))
            {
                // if nodes are equally saturated, use the node with more slots (i.e. CPUs online)
                Ordering::Equal => self.nr_slots.cmp(&other.nr_slots),
                // if nodes are not equally saturated, use the node with lower saturation
                ord => ord.reverse(),
            }
        }
    }

    pub fn select_cpu(&mut self) -> Path {
        let mut path = Path::default();
        self.select_cpu_recur(&mut path);
        path
    }

    fn select_cpu_recur(&mut self, path: &mut Path) {
        self.nr_slots_selected += 1;
        path.push(self.level.clone());
        if let Some(mut c) = self.children.peek_mut() {
            c.select_cpu_recur(path);
        }
    }

    #[cfg(test)]
    pub fn nr_slots(&self) -> usize {
        self.nr_slots
    }

    #[cfg(test)]
    pub fn nr_slots_selected(&self) -> usize {
        self.nr_slots_selected
    }
}

pub mod marker {
    use super::Node;
    use std::cmp::Ordering;

    #[derive(Debug)]
    pub struct Spread;
    #[derive(Debug)]
    pub struct Pack;

    pub trait Priority {
        fn priority(lhs: &Node<Self>, rhs: &Node<Self>) -> Ordering;
    }

    impl Priority for Spread {
        fn priority(lhs: &Node<Self>, rhs: &Node<Self>) -> Ordering {
            lhs.priority_spread(rhs)
        }
    }

    impl Priority for Pack {
        fn priority(lhs: &Node<Self>, rhs: &Node<Self>) -> Ordering {
            lhs.priority_pack(rhs)
        }
    }

    impl<T> Ord for Node<T>
    where
        T: Priority,
    {
        fn cmp(&self, other: &Self) -> Ordering {
            T::priority(self, other)
        }
    }

    impl<T> PartialOrd for Node<T>
    where
        T: Priority,
    {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl<T> PartialEq<Self> for Node<T>
    where
        T: Priority,
    {
        fn eq(&self, other: &Self) -> bool {
            match self.cmp(other) {
                Ordering::Equal => true,
                Ordering::Less | Ordering::Greater => false,
            }
        }
    }

    impl<T> Eq for Node<T> where T: Priority {}
}

#[derive(Debug, Default)]
pub struct Path(Vec<Level>);

impl Path {
    fn push(&mut self, l: Level) {
        self.0.push(l)
    }

    fn into_iter(self) -> impl Iterator<Item = Level> {
        self.0.into_iter()
    }

    #[cfg(test)]
    fn last(&self) -> Option<&Level> {
        self.0.last()
    }
}

impl std::convert::TryFrom<Path> for CpuLocation {
    type Error = &'static str;
    fn try_from(path: Path) -> Result<Self, Self::Error> {
        let mut cpu = None;
        let mut core = None;
        let mut pkg = None;
        let mut numa = None;

        for p in path.into_iter() {
            match p {
                Level::SystemRoot => {}
                Level::Cpu(id) => match cpu {
                    None => cpu = Some(id),
                    Some(_) => return Err("duplicate cpu in path"),
                },
                Level::Core(id) => match core {
                    None => core = Some(id),
                    Some(_) => return Err("duplicate core in path"),
                },
                Level::Package(id) => match pkg {
                    None => pkg = Some(id),
                    Some(_) => return Err("duplicate pkg in path"),
                },
                Level::NumaNode(id) => match numa {
                    None => numa = Some(id),
                    Some(_) => return Err("duplicate numa in path"),
                },
            }
        }

        match (cpu, core, pkg, numa) {
            (Some(cpu), Some(core), Some(package), Some(numa_node)) => Ok(CpuLocation {
                cpu,
                core,
                package,
                numa_node,
            }),
            _ => Err("Failed to construct Path from CpuLocation"),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    type NodeSpread = Node<marker::Spread>;
    type NodePack = Node<marker::Pack>;

    #[test]
    fn construct_pq_tree_spread() {
        use std::collections::HashSet;

        let nr_numa = 2; // # per system / root
        let nr_pkg = 3; // # per numa
        let nr_core = 5; // # per pkg
        let nr_cpu = 7; // # per cpu

        let total_cpus = nr_numa * nr_pkg * nr_core * nr_cpu;

        let mut node_root = NodeSpread::new(Level::SystemRoot);
        for numa in 0..nr_numa {
            let mut node_numa = NodeSpread::new(Level::NumaNode(numa));
            for package in 0..nr_pkg {
                let package = numa * nr_pkg + package;
                let mut node_package = NodeSpread::new(Level::Package(package));
                for core in 0..nr_core {
                    let core = package * nr_core + core;
                    let mut node_core = NodeSpread::new(Level::Core(core));
                    for cpu in 0..nr_cpu {
                        let cpu = core * nr_cpu + cpu;
                        node_core.push_child(NodeSpread::new(Level::Cpu(cpu)));
                    }
                    node_package.push_child(node_core);
                }
                node_numa.push_child(node_package);
            }
            node_root.push_child(node_numa);
        }

        assert_eq!(node_root.nr_slots(), total_cpus);
        assert_eq!(node_root.nr_slots_selected(), 0);

        let mut set = (0..total_cpus).collect::<HashSet<_>>();

        for ii in 0..total_cpus {
            if let Some(Level::Cpu(cpu_id)) = node_root.select_cpu().last() {
                set.take(cpu_id)
                    .unwrap_or_else(|| panic!("missing cpu {ii}"));
            }
        }

        assert_eq!(node_root.nr_slots_selected(), total_cpus);
        assert_eq!(0, set.len());
    }

    #[test]
    fn construct_pq_tree_pack() {
        use std::collections::HashSet;

        let nr_numa = 2; // # in system / root
        let nr_pkg = 3; // # per numa
        let nr_core = 5; // # per pkg
        let nr_cpu = 7; // # per cpu

        let total_cpus = nr_numa * nr_pkg * nr_core * nr_cpu;

        let mut node_root = NodePack::new(Level::SystemRoot);
        for numa in 0..nr_numa {
            let mut node_numa = NodePack::new(Level::NumaNode(numa));
            for package in 0..nr_pkg {
                let package = numa * nr_pkg + package;
                let mut node_package = NodePack::new(Level::Package(package));
                for core in 0..nr_core {
                    let core = package * nr_core + core;
                    let mut node_core = NodePack::new(Level::Core(core));
                    for cpu in 0..nr_cpu {
                        let cpu = core * nr_cpu + cpu;
                        node_core.push_child(NodePack::new(Level::Cpu(cpu)));
                    }
                    node_package.push_child(node_core);
                }
                node_numa.push_child(node_package);
            }
            node_root.push_child(node_numa);
        }

        assert_eq!(node_root.nr_slots(), total_cpus);
        assert_eq!(node_root.nr_slots_selected(), 0);

        let mut set = (0..total_cpus).collect::<HashSet<_>>();

        for ii in 0..total_cpus {
            if let Some(Level::Cpu(cpu_id)) = node_root.select_cpu().last() {
                set.take(cpu_id)
                    .unwrap_or_else(|| panic!("missing cpu {ii}"));
            }
        }

        assert_eq!(node_root.nr_slots_selected(), total_cpus);
        assert_eq!(0, set.len());
    }
}
