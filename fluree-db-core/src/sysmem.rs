//! cgroup-aware system memory limit detection.
//!
//! `sysinfo::total_memory()` reports the *host* RAM, which is wrong inside a
//! container or any cgroup-constrained process: a 4 GB-limited container on a
//! 256 GB host would otherwise size budgets/caches to 256 GB and OOM. Callers
//! should clamp their host-total reading with [`effective_memory_limit_bytes`]
//! so budgets track the real limit.

/// The cgroup memory limit applied to this process, in bytes, if one is set.
///
/// Returns `None` when there is no limit (unlimited / unconstrained cgroup),
/// on non-Linux platforms, or if the limit cannot be read.
#[cfg(target_os = "linux")]
pub fn cgroup_memory_limit_bytes() -> Option<u64> {
    // cgroup v2 (unified): `/sys/fs/cgroup/memory.max` — a decimal or "max".
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let t = s.trim();
        if t == "max" {
            return None;
        }
        if let Ok(v) = t.parse::<u64>() {
            return sane_limit(v);
        }
    }
    // cgroup v1: `/sys/fs/cgroup/memory/memory.limit_in_bytes` — a near-u64::MAX
    // sentinel means unlimited.
    if let Ok(s) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Ok(v) = s.trim().parse::<u64>() {
            return sane_limit(v);
        }
    }
    None
}

/// Non-Linux platforms have no cgroup limit.
#[cfg(not(target_os = "linux"))]
pub fn cgroup_memory_limit_bytes() -> Option<u64> {
    None
}

/// Treat the kernel's "unlimited" sentinels (0, or anything in the exabyte
/// range) as no limit.
#[cfg(target_os = "linux")]
fn sane_limit(v: u64) -> Option<u64> {
    if v == 0 || v >= (1u64 << 60) {
        None
    } else {
        Some(v)
    }
}

/// Clamp a host total-memory reading (e.g. from `sysinfo`) to the cgroup limit
/// when one is present. Returns the host value unchanged when unconstrained.
pub fn effective_memory_limit_bytes(host_total_bytes: u64) -> u64 {
    match cgroup_memory_limit_bytes() {
        Some(limit) => limit.min(host_total_bytes),
        None => host_total_bytes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn effective_never_exceeds_host() {
        for host in [0u64, 1 << 20, 16u64 << 30, 256u64 << 30] {
            assert!(effective_memory_limit_bytes(host) <= host);
        }
    }

    #[test]
    fn detection_does_not_panic() {
        let _ = cgroup_memory_limit_bytes();
    }
}
