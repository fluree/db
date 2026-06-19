//! Best-effort process memory readout for phase-level import logging.
//!
//! Bulk import trades RAM for local scratch disk; these helpers let each phase
//! log its current resident set and projected footprint so a real run is
//! observable (and so a tmpfs `/tmp` — where the spill never leaves RAM — is
//! visible as flat-but-high RSS rather than a silent OOM).

/// Current resident set size of this process in bytes, best-effort.
///
/// Linux reads `/proc/self/statm` (resident pages × page size) for the *live*
/// RSS. Other unix platforms fall back to `getrusage` peak RSS (a high-water
/// mark, not the current value). Non-unix (Windows) and any failure return 0.
pub fn current_rss_bytes() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/self/statm") {
            // Fields are in pages: size resident shared text lib data dt.
            if let Some(resident) = s.split_whitespace().nth(1) {
                if let Ok(pages) = resident.parse::<u64>() {
                    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
                    if page > 0 {
                        return pages.saturating_mul(page as u64);
                    }
                }
            }
        }
        0
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } == 0 {
            let maxrss = usage.ru_maxrss.max(0) as u64;
            // macOS reports bytes; other BSDs report kibibytes.
            return if cfg!(target_os = "macos") {
                maxrss
            } else {
                maxrss.saturating_mul(1024)
            };
        }
        0
    }
    #[cfg(not(unix))]
    {
        0
    }
}

/// Current RSS in whole mebibytes (for compact `tracing` fields).
pub fn current_rss_mib() -> u64 {
    current_rss_bytes() / (1024 * 1024)
}

/// Bytes → whole mebibytes.
pub fn mib(bytes: u64) -> u64 {
    bytes / (1024 * 1024)
}
