//! Scratch directories named for the process that made them, and whether that
//! process is still running: a crash leaves its directories behind.

use std::io;
use std::path::Path;

/// Whether process `pid` is running on this host. Assumed so where this
/// cannot be told.
pub fn running(pid: u32) -> bool {
    if pid == std::process::id() {
        return true;
    }
    #[cfg(unix)]
    {
        let Ok(pid) = libc::pid_t::try_from(pid) else {
            return false;
        };
        // SAFETY: signal 0 only checks that the process exists.
        let alive = unsafe { libc::kill(pid, 0) } == 0;
        alive || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// Remove the entries of `dir` whose process, which `pid_of` reads from an
/// entry's name, is no longer running. An entry `pid_of` names no process
/// for stays; a missing `dir` has none. Returns how many it removed.
pub fn clear_ended(dir: &Path, pid_of: impl Fn(&str) -> Option<u32>) -> io::Result<usize> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let mut removed = 0;
    for entry in entries {
        let entry = entry?;
        let pid = entry.file_name().to_str().and_then(&pid_of);
        if pid.is_some_and(|pid| !running(pid)) {
            std::fs::remove_dir_all(entry.path())?;
            removed += 1;
        }
    }
    Ok(removed)
}
