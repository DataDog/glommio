// Unless explicitly stated otherwise all files in this repository are licensed
// under the MIT/Apache-2.0 License, at your convenience
//
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2020 Datadog, Inc.
//

use std::{
    collections::{HashMap, HashSet},
    io::{self, ErrorKind},
    path::Path,
};

use super::sysfs::ListIterator;

/// A description of the CPU's location in the machine topology.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
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

fn build_cpu_location(
    sysfs_path: &Path,
    cpu: usize,
    numa_node: usize,
    cpu_to_core: &mut HashMap<usize, usize>,
) -> io::Result<CpuLocation> {
    let cpu_path = sysfs_path.join(format!("cpu/cpu{}/topology", cpu));

    let package_id = ListIterator::from_path(&cpu_path.join("physical_package_id"))?
        .next()
        .ok_or_else(|| io::Error::new(ErrorKind::Other, "failed to parse physical_package_id"))??;

    Ok(CpuLocation {
        cpu,
        core: get_core_id(cpu, &cpu_path, cpu_to_core)?,
        package: package_id,
        numa_node,
    })
}

/// Request the machine topology.  Only CPUs that are currently `online`
/// according to `/sys/devices/system/cpu/online` are provided;  `sysfs` is
/// always at `/sys` per: https://www.kernel.org/doc/html/latest/admin-guide/sysfs-rules.html
pub fn get_machine_topology_unsorted() -> io::Result<Vec<CpuLocation>> {
    let sysfs_path = Path::new("/sys/devices/system");
    let mut cpus_online = HashSet::new();
    for cpu in ListIterator::from_path(&sysfs_path.join("cpu/online"))? {
        cpus_online.insert(cpu?);
    }
    let mut cpu_locations = Vec::new();
    let mut cpu_to_core = HashMap::new();

    let nodes_online = match ListIterator::from_path(&sysfs_path.join("node/online")) {
        Ok(x) => x,
        Err(x) => match x.kind() {
            io::ErrorKind::NotFound => {
                for cpu in cpus_online.drain() {
                    let cpu_location = build_cpu_location(sysfs_path, cpu, 0, &mut cpu_to_core)?;
                    cpu_locations.push(cpu_location);
                }
                return Ok(cpu_locations);
            }
            _ => {
                return Err(x);
            }
        },
    };

    for node in nodes_online {
        let node = node?;
        let node_path = sysfs_path.join(format!("node/node{}", node));
        let node_cpus = ListIterator::from_path(&node_path.join("cpulist"))?;
        for cpu in node_cpus {
            let cpu = cpu?;
            // only map CPUs that are online
            if !cpus_online.contains(&cpu) {
                continue;
            }

            let cpu_location = build_cpu_location(sysfs_path, cpu, node, &mut cpu_to_core)?;
            cpu_locations.push(cpu_location);
        }
    }

    Ok(cpu_locations)
}

fn get_core_id(
    cpu: usize,
    cpu_path: &Path,
    cpu_to_core: &mut HashMap<usize, usize>,
) -> io::Result<usize> {
    // `hwloc` suggests that some hardware assigns unique `core_id`s to each CPU
    // even though they are hyper-threads, so we ensure we have the same
    // `core_id` for all CPUs in `core_cpus_list` (`thread_siblings` is
    // deprecated in favor of `core_cpus`) see: https://github.com/open-mpi/hwloc/blob/3c8ed197d9a017ca5399007861981b60032e7ca6/hwloc/topology-linux.c#L4267
    let cpu_siblings = ListIterator::from_path(&cpu_path.join("core_cpus_list"))?;
    match cpu_to_core.get(&cpu) {
        Some(core) => Ok(*core),
        None => {
            let core = ListIterator::from_path(&cpu_path.join("core_id"))?
                .next()
                .transpose()?
                .ok_or_else(|| io::Error::new(ErrorKind::Other, "failed to parse core_id"))?;
            for sibling in cpu_siblings {
                cpu_to_core.insert(sibling?, core);
            }
            Ok(core)
        }
    }
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::{CpuLocation, HashMap};

    pub fn check_topolgy(mut topology: Vec<CpuLocation>) {
        // Check that we don't have a system where any hardware component has an id that
        // is not unique system-wide (e.g. both numa node 0 and 1 have a core
        // with id 0); this precondition is assumed throughout
        topology.sort_by_key(|l| (l.numa_node, l.package, l.core, l.cpu));

        let cpus = topology.into_iter();
        let mut cpu_to_core = HashMap::new();
        let mut core_to_pkg = HashMap::new();
        let mut core_to_numa = HashMap::new();
        let mut pkg_to_numa = Some(HashMap::new());
        let mut numa_to_pkg = Some(HashMap::new());

        for cpu in cpus {
            cpu_to_core
                .entry(cpu.cpu)
                .and_modify(|e| {
                    assert_eq!(
                        *e, cpu.core,
                        "cpu {} in cores {} and {}",
                        cpu.cpu, cpu.core, *e
                    )
                })
                .or_insert(cpu.core);

            core_to_pkg
                .entry(cpu.core)
                .and_modify(|e| {
                    assert_eq!(
                        *e, cpu.package,
                        "core {} in packages {} and {}",
                        cpu.core, cpu.package, *e
                    )
                })
                .or_insert(cpu.package);

            core_to_numa
                .entry(cpu.core)
                .and_modify(|e| {
                    assert_eq!(
                        *e, cpu.numa_node,
                        "core {} in numa_nodes {} and {}",
                        cpu.core, cpu.numa_node, *e
                    )
                })
                .or_insert(cpu.numa_node);

            let mut either = false;
            if let Some(ref mut map) = pkg_to_numa {
                if matches!(map.insert(cpu.package, cpu.numa_node), Some(n) if n != cpu.numa_node) {
                    pkg_to_numa = None;
                } else {
                    either = true;
                }
            }
            if let Some(ref mut map) = numa_to_pkg {
                if matches!(map.insert(cpu.numa_node, cpu.package), Some(p) if p != cpu.package) {
                    numa_to_pkg = None;
                } else {
                    either = true;
                }
            }

            assert!(
                either,
                "unsupported topology hierarchy: numa node {} and package {}",
                cpu.numa_node, cpu.package
            );
        }
    }

    pub fn cpu_loc(numa_node: usize, package: usize, core: usize, cpu: usize) -> CpuLocation {
        CpuLocation {
            cpu,
            core,
            package,
            numa_node,
        }
    }
}

#[cfg(test)]
mod test {
    use super::{test_helpers::*, *};

    #[test]
    fn machine_topology() {
        get_machine_topology_unsorted().unwrap();
    }

    #[test]
    fn topology_this_machine_unique_ids() {
        let topology = get_machine_topology_unsorted().unwrap();
        check_topolgy(topology)
    }

    #[test]
    #[should_panic(expected = "unsupported topology hierarchy")]
    fn check_topology_check() {
        // panic because topology level are unclear:
        // numa node 0 is associated with package 0 and 1
        // package 1 is associated with numa node 0 and 2
        let topology = vec![
            cpu_loc(0, 0, 0, 0),
            cpu_loc(0, 1, 1, 1),
            cpu_loc(2, 1, 2, 2),
        ];

        check_topolgy(topology);
    }
}
