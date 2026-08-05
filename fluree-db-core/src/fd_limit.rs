//! Process open-file-limit (`RLIMIT_NOFILE`) helpers.
//!
//! Large bulk imports fan out over many temporary files (spool chunks, sorted
//! run files, class-membership partitions). On platforms with a low default
//! soft limit — macOS ships 256 — an unbounded fan-out dies with
//! `EMFILE`/"Too many open files". Two complementary mechanisms live here:
//!
//! 1. [`raise_nofile_soft_to_hard`] — a best-effort, unprivileged raise of the
//!    soft limit to the hard limit, called once at process startup (CLI,
//!    server) and again defensively at the start of an import.
//! 2. [`FdBudget`] — the observed post-raise limit minus a reserve, handed to
//!    the index build so every fan-in/fan-out stage plans within it (mirroring
//!    how `run_budget_bytes` plans memory). This is what keeps imports correct
//!    in environments where the hard limit itself is low and fixed (containers,
//!    AWS Lambda's 1024).
//!
//! Memory-mapped regions do not count against `RLIMIT_NOFILE` once their
//! `File` is dropped; only real open handles need budgeting.

/// Soft and hard `RLIMIT_NOFILE` values as reported by `getrlimit`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NofileLimits {
    /// Current (soft) limit — the enforced ceiling on open descriptors.
    pub soft: u64,
    /// Maximum (hard) limit the soft limit can be raised to without privileges.
    pub hard: u64,
}

/// Result of a [`raise_nofile_soft_to_hard`] attempt.
#[derive(Clone, Copy, Debug)]
pub enum RaiseOutcome {
    /// Soft limit was raised.
    Raised { from: u64, to: u64 },
    /// Soft limit already at the effective maximum; nothing to do.
    AlreadyAtMax { soft: u64 },
    /// `setrlimit` (or `getrlimit`) failed; `soft` is the still-current limit.
    Failed { errno: i32, soft: u64 },
    /// Not a unix platform; limits are not observable here.
    Unsupported,
}

/// Read the current `RLIMIT_NOFILE`. `None` on non-unix platforms or if
/// `getrlimit` fails.
#[cfg(unix)]
// rlim_t is platform-dependent (u64 on 64-bit macOS/glibc, narrower
// elsewhere), so the widening casts are not universally redundant.
#[allow(clippy::unnecessary_cast)]
pub fn nofile_limits() -> Option<NofileLimits> {
    let mut rl = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: getrlimit only writes into the rlimit struct we pass.
    let rc = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) };
    if rc != 0 {
        return None;
    }
    Some(NofileLimits {
        soft: rl.rlim_cur as u64,
        hard: rl.rlim_max as u64,
    })
}

/// Read the current `RLIMIT_NOFILE`. `None` on non-unix platforms.
#[cfg(not(unix))]
pub fn nofile_limits() -> Option<NofileLimits> {
    None
}

/// macOS refuses `setrlimit` soft values above `kern.maxfilesperproc` even
/// when the hard limit reports `RLIM_INFINITY`, so the raise target must be
/// clamped to it. Falls back to `OPEN_MAX` (10240) if the sysctl fails.
#[cfg(target_os = "macos")]
fn macos_max_files_per_proc() -> u64 {
    const OPEN_MAX: u64 = 10_240;
    let mut val: libc::c_int = 0;
    let mut len = std::mem::size_of::<libc::c_int>();
    // SAFETY: sysctlbyname writes at most `len` bytes into `val`.
    let rc = unsafe {
        libc::sysctlbyname(
            c"kern.maxfilesperproc".as_ptr(),
            std::ptr::from_mut(&mut val).cast::<libc::c_void>(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if rc == 0 && val > 0 {
        val as u64
    } else {
        OPEN_MAX
    }
}

/// Best-effort raise of the `RLIMIT_NOFILE` soft limit to the hard limit
/// (clamped to `kern.maxfilesperproc` on macOS). Never errors — callers log
/// the outcome via [`log_raise_outcome`] once tracing is initialized.
#[cfg(unix)]
pub fn raise_nofile_soft_to_hard() -> RaiseOutcome {
    let Some(NofileLimits { soft, hard }) = nofile_limits() else {
        return RaiseOutcome::Failed {
            errno: std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            soft: 0,
        };
    };

    #[cfg(target_os = "macos")]
    let target = hard.min(macos_max_files_per_proc());
    #[cfg(not(target_os = "macos"))]
    let target = hard;

    if soft >= target {
        return RaiseOutcome::AlreadyAtMax { soft };
    }

    let rl = libc::rlimit {
        rlim_cur: target as libc::rlim_t,
        rlim_max: hard as libc::rlim_t,
    };
    // SAFETY: setrlimit reads the rlimit struct we pass; raising the soft
    // limit up to the hard limit needs no privileges.
    let rc = unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &rl) };
    if rc == 0 {
        RaiseOutcome::Raised {
            from: soft,
            to: target,
        }
    } else {
        RaiseOutcome::Failed {
            errno: std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
            soft,
        }
    }
}

/// Best-effort raise; no-op on non-unix platforms.
#[cfg(not(unix))]
pub fn raise_nofile_soft_to_hard() -> RaiseOutcome {
    RaiseOutcome::Unsupported
}

/// Emit the outcome of a raise attempt on the `tracing` subscriber. Split
/// from [`raise_nofile_soft_to_hard`] because entry points raise *before*
/// initializing tracing and log after.
pub fn log_raise_outcome(outcome: &RaiseOutcome) {
    match *outcome {
        RaiseOutcome::Raised { from, to } => {
            tracing::debug!(from, to, "raised open-file (RLIMIT_NOFILE) soft limit");
        }
        RaiseOutcome::AlreadyAtMax { soft } => {
            tracing::debug!(soft, "open-file soft limit already at effective maximum");
        }
        RaiseOutcome::Failed { errno, soft } => {
            tracing::warn!(
                errno,
                soft,
                "failed to raise open-file (RLIMIT_NOFILE) soft limit; \
                 large imports fall back to file-descriptor-conserving strategies"
            );
        }
        RaiseOutcome::Unsupported => {}
    }
}

/// True when an I/O error is file-descriptor exhaustion: `EMFILE` (per-process
/// limit) or `ENFILE` (system-wide table full).
#[cfg(unix)]
pub fn is_fd_exhaustion(e: &std::io::Error) -> bool {
    matches!(e.raw_os_error(), Some(code) if code == libc::EMFILE || code == libc::ENFILE)
}

/// True when an I/O error is file-descriptor exhaustion. Always false on
/// non-unix platforms (no stable errno mapping).
#[cfg(not(unix))]
pub fn is_fd_exhaustion(_e: &std::io::Error) -> bool {
    false
}

/// The file-descriptor budget an FD-heavy operation may plan against: the
/// observed soft limit minus a reserve for descriptors owned by everything
/// else in the process. The reserve's largest audited consumer is the
/// dictionary upload that runs concurrently with the index build (~15–19
/// simultaneous handles across its four `try_join` arms), plus stdio, the
/// tokio poller/wakeup pipes, and tracing/OTEL sockets — hence the floor of
/// 32. The reserve is storage-backend-blind: an object-store backend's
/// connection pool also draws from it, so callers on such backends at very
/// low limits should prefer `FLUREE_FD_BUDGET` to shrink the plan further.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FdBudget {
    /// Observed (or overridden) soft limit.
    pub soft: u64,
    /// Descriptors held back from planning.
    pub reserve: u64,
}

impl FdBudget {
    /// Budget from the live `getrlimit` soft limit, overridable with the
    /// `FLUREE_FD_BUDGET` environment variable (ops/test escape hatch: the
    /// override replaces the *soft* value the reserve is derived from — it
    /// changes how the build *plans*, never what the kernel enforces, and
    /// preflight limit checks deliberately read the real limit instead).
    pub fn detect() -> Self {
        if let Some(soft) = std::env::var("FLUREE_FD_BUDGET")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|&n| n > 0)
        {
            return Self::from_soft(soft);
        }
        match nofile_limits() {
            Some(limits) => Self::from_soft(limits.soft),
            None => Self::unlimited(),
        }
    }

    /// Budget for a given soft limit; reserve is `min(max(soft/4, 32), 64)`
    /// (see the struct docs for what the 32-descriptor floor covers).
    pub fn from_soft(soft: u64) -> Self {
        Self {
            soft,
            reserve: (soft / 4).clamp(32, 64),
        }
    }

    /// No budgeting: every stage plans as if descriptors were free. This is
    /// the pre-budget legacy behavior, used by tests and callers that manage
    /// limits themselves. (The `u64::MAX` sentinel coincides with Linux's
    /// `RLIM_INFINITY`; that collision is benign — a genuinely uncapped
    /// process and "no budgeting" want identical plans.)
    pub fn unlimited() -> Self {
        Self {
            soft: u64::MAX,
            reserve: 0,
        }
    }

    /// Descriptors available for planning: `max(soft - reserve, 32)`, so even
    /// absurdly low limits yield a plan that can make progress (the kernel
    /// will still enforce the real limit; 32 merely keeps the plan sane).
    pub fn available(&self) -> usize {
        if self.soft == u64::MAX {
            return usize::MAX;
        }
        usize::try_from(self.soft.saturating_sub(self.reserve).max(32)).unwrap_or(usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_reserve_formula() {
        // A quarter of the limit, floored at 32 (stdio + tokio + the
        // concurrent dict upload), capped at 64.
        assert_eq!(FdBudget::from_soft(96).reserve, 32);
        assert_eq!(FdBudget::from_soft(256).reserve, 64);
        assert_eq!(FdBudget::from_soft(1024).reserve, 64);
        assert_eq!(FdBudget::from_soft(10_240).reserve, 64);
    }

    #[test]
    fn budget_available_floors_and_saturates() {
        assert_eq!(FdBudget::from_soft(96).available(), 64);
        assert_eq!(FdBudget::from_soft(256).available(), 192);
        assert_eq!(FdBudget::from_soft(1024).available(), 960);
        // Degenerate limits floor at 32 rather than planning zero fan-in.
        assert_eq!(FdBudget::from_soft(8).available(), 32);
        assert_eq!(FdBudget::unlimited().available(), usize::MAX);
    }

    #[cfg(unix)]
    #[test]
    fn limits_observable_and_raise_is_safe() {
        let before = nofile_limits().expect("getrlimit should succeed on unix");
        assert!(before.soft > 0);
        // Raising is monotonic and never errors out of the process.
        let outcome = raise_nofile_soft_to_hard();
        match outcome {
            RaiseOutcome::Raised { from, to } => assert!(to > from),
            RaiseOutcome::AlreadyAtMax { .. } | RaiseOutcome::Failed { .. } => {}
            RaiseOutcome::Unsupported => panic!("unix build must not report Unsupported"),
        }
        let after = nofile_limits().expect("getrlimit should succeed on unix");
        assert!(after.soft >= before.soft);
    }

    #[cfg(unix)]
    #[test]
    fn fd_exhaustion_detection() {
        assert!(is_fd_exhaustion(&std::io::Error::from_raw_os_error(
            libc::EMFILE
        )));
        assert!(is_fd_exhaustion(&std::io::Error::from_raw_os_error(
            libc::ENFILE
        )));
        assert!(!is_fd_exhaustion(&std::io::Error::from_raw_os_error(
            libc::ENOENT
        )));
        assert!(!is_fd_exhaustion(&std::io::Error::other("no raw os error")));
    }
}
