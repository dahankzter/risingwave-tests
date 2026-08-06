//! CPU pinning: keep the load generator off the cores the cluster is measured on.
//!
//! Two cores are reserved for the bench process, the rest go to the container. The part that
//! actually buys the isolation is `parallelism`: RisingWave sizes its streaming thread pools from
//! the core count it detects, so a container restricted to 62 cores would still spawn workers for
//! all 64 and thrash the two the bench is using. `streaming_parallelism` must match the cpuset.
//!
//! Pinning is opt-in (`--pin`). Every number recorded in the README so far was measured unpinned,
//! so switching the default would silently change what those numbers mean.
//!
//! Platform split: on Linux the container gets `--cpuset-cpus` and this process gets
//! `sched_setaffinity`. macOS has no equivalent — its affinity API exposes only scheduler *hints*
//! (affinity tags), which cannot restrict a process to a core set — so `apply_to_self` is a no-op
//! there and `why` records it.

/// The layout to apply: cpuset strings for the container and this process, the matching
/// `streaming_parallelism`, and a human-readable account of how it was chosen (rendered in the
/// details tab — a layout nobody can see is a layout nobody can check).
#[derive(Debug, Clone, PartialEq)]
pub struct Layout {
    pub cluster: Option<String>,
    pub bench: Option<String>,
    pub parallelism: Option<u32>,
    pub why: String,
}

/// Cores kept for the bench process itself.
const BENCH_CORES: usize = 2;

/// Below this, partitioning leaves too little for either side to be meaningful.
const MIN_TOTAL: usize = 8;

/// Plan a layout for a machine with `total` usable cores. Pure — the detection lives in
/// [`usable_cores`] so this is testable without a machine of each size.
pub fn plan(total: usize) -> Layout {
    if total < MIN_TOTAL {
        return Layout {
            cluster: None,
            bench: None,
            parallelism: None,
            why: format!("too few cores to partition ({total}); pinning nothing"),
        };
    }
    let cluster_cores = total - BENCH_CORES;
    Layout {
        cluster: Some(format!("0-{}", cluster_cores - 1)),
        bench: Some(format!("{}-{}", cluster_cores, total - 1)),
        parallelism: Some(cluster_cores as u32),
        why: format!(
            "{total} cores: cluster 0-{}, bench {}-{}, streaming_parallelism {} (matched to the \
             cluster cpuset so RisingWave does not size its pools for cores it cannot use)",
            cluster_cores - 1,
            cluster_cores,
            total - 1,
            cluster_cores
        ),
    }
}

/// Cores this process may actually use. Prefers the cgroup v2 CPU quota over the physical count:
/// inside a constrained container `available_parallelism` reports the host's cores, so a layout
/// built from it would pin to cores the cgroup forbids.
pub fn usable_cores() -> usize {
    #[cfg(target_os = "linux")]
    if let Some(n) = cgroup_quota_cores() {
        return n;
    }
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}

/// `cpu.max` is `"$MAX $PERIOD"`, where `$MAX` is `"max"` when unlimited. Quota/period rounded
/// down is the whole-core allowance; a fractional allowance floors to at least 1.
#[cfg(target_os = "linux")]
fn cgroup_quota_cores() -> Option<usize> {
    let raw = std::fs::read_to_string("/sys/fs/cgroup/cpu.max").ok()?;
    let mut parts = raw.split_whitespace();
    let max = parts.next()?;
    let period: f64 = parts.next()?.parse().ok()?;
    if max == "max" || period <= 0.0 {
        return None;
    }
    let quota: f64 = max.parse().ok()?;
    Some(((quota / period).floor() as usize).max(1))
}

/// Restrict this process to `layout.bench`. Linux only; a no-op elsewhere so the crate builds and
/// runs everywhere (the caller reports `layout.why`, which says as much).
#[cfg(target_os = "linux")]
pub fn apply_to_self(layout: &Layout) -> anyhow::Result<()> {
    let Some(spec) = layout.bench.as_deref() else {
        return Ok(());
    };
    let cores = parse_range(spec)?;
    // `sched_setaffinity(0, ...)` via libc-free syscall: build the mask by hand rather than take a
    // dependency for one call. Mask is a bitset of u64 words, LSB = core 0.
    const WORDS: usize = 16; // 1024 cores, comfortably beyond anything this bench runs on
    let mut mask = [0u64; WORDS];
    for c in cores {
        let (w, b) = (c / 64, c % 64);
        if w >= WORDS {
            anyhow::bail!("core {c} beyond the supported affinity mask width");
        }
        mask[w] |= 1u64 << b;
    }
    // SAFETY: `mask` is a valid, initialized buffer of exactly the length passed; pid 0 means
    // "this thread group". The kernel only reads from the pointer.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_sched_setaffinity,
            0,
            std::mem::size_of_val(&mask),
            mask.as_ptr(),
        )
    };
    if rc != 0 {
        anyhow::bail!("sched_setaffinity failed: {}", std::io::Error::last_os_error());
    }
    Ok(())
}

/// Non-Linux: nothing to do. macOS exposes affinity *tags* (scheduler hints), not a cpuset, so
/// there is no honest way to restrict this process — the container's cpuset still applies.
#[cfg(not(target_os = "linux"))]
pub fn apply_to_self(_layout: &Layout) -> anyhow::Result<()> {
    Ok(())
}

/// `"6-7"` / `"3"` / `"0-2,7"` -> the core numbers. Rejects anything else rather than silently
/// pinning to a subset of what the operator asked for.
pub fn parse_range(spec: &str) -> anyhow::Result<Vec<usize>> {
    let mut out = Vec::new();
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            anyhow::bail!("empty range component in cpuset {spec:?}");
        }
        match part.split_once('-') {
            Some((a, b)) => {
                let (a, b): (usize, usize) = (a.trim().parse()?, b.trim().parse()?);
                if b < a {
                    anyhow::bail!("descending range {part:?} in cpuset {spec:?}");
                }
                out.extend(a..=b);
            }
            None => out.push(part.parse()?),
        }
    }
    if out.is_empty() {
        anyhow::bail!("cpuset {spec:?} selects no cores");
    }
    Ok(out)
}

/// What `apply_to_self` can do on this platform, for the details tab.
pub fn platform_note() -> &'static str {
    if cfg!(target_os = "linux") {
        "container cpuset + process affinity"
    } else {
        "container cpuset only (no process-affinity API on this platform)"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn too_few_cores_pins_nothing() {
        let l = plan(4);
        assert!(l.cluster.is_none() && l.bench.is_none());
        assert!(l.why.contains("too few"), "why: {}", l.why);
    }

    #[test]
    fn a_large_box_reserves_two_cores_for_the_bench() {
        let l = plan(64);
        assert_eq!(l.bench.as_deref(), Some("62-63"));
        assert_eq!(l.cluster.as_deref(), Some("0-61"));
        // Parallelism must match the cpuset, or the cluster spawns 64 workers on 62 cores.
        assert_eq!(l.parallelism, Some(62));
    }

    #[test]
    fn the_boundary_case_still_leaves_a_usable_cluster() {
        let l = plan(8);
        assert_eq!(l.bench.as_deref(), Some("6-7"));
        assert_eq!(l.cluster.as_deref(), Some("0-5"));
        assert_eq!(l.parallelism, Some(6));
    }

    #[test]
    fn ranges_parse_and_bad_ones_are_rejected() {
        assert_eq!(parse_range("6-7").unwrap(), vec![6, 7]);
        assert_eq!(parse_range("3").unwrap(), vec![3]);
        assert_eq!(parse_range("0-2,7").unwrap(), vec![0, 1, 2, 7]);
        assert!(parse_range("7-6").is_err());
        assert!(parse_range("").is_err());
        assert!(parse_range("a-b").is_err());
    }

    #[test]
    fn applying_a_layout_that_pins_nothing_is_a_no_op() {
        let l = plan(4);
        assert!(apply_to_self(&l).is_ok());
    }
}
