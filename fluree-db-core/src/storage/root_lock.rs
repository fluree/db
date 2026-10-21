//! The lock that keeps a file storage root to one open instance.
//!
//! Two instances on one root are not coordinated: each serves its own cached
//! state, runs its own indexer and sweeps files the other may still need. The
//! lock is advisory and belongs to an open handle, so the operating system
//! releases it however the holder exits; the file left on disk means nothing.
//! A child forked without exec shares that handle, so dropping the lock
//! unlocks it rather than waiting for every copy to close, and the child
//! gives its copy up as it starts, for a parent that exits without dropping.

use fs2::FileExt;
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{LazyLock, Mutex, MutexGuard, PoisonError};

/// The lock file's name under a storage root.
pub const ROOT_LOCK_FILE: &str = ".fluree-lock";

/// Set to a truthy value (`1`, `true`, `yes`, `on`) to open a root whose
/// filesystem cannot lock it (some network and FUSE mounts), without
/// protection from a second process. Unset, such a root fails to open.
pub const ALLOW_UNLOCKED_ENV_VAR: &str = "FLUREE_STORAGE_ALLOW_UNLOCKED";

/// An exclusive lock on a file storage root, held until dropped.
#[derive(Debug)]
pub struct RootLock {
    handle: Option<File>,
    /// This root's entry in [`HELD_HERE`], while this lock holds it.
    held_here: Option<PathBuf>,
}

/// The roots this process holds. A refusal tells this process from another
/// by it, not by the lock file, which Windows does not let a second handle
/// read while it is locked.
static HELD_HERE: LazyLock<Mutex<HashSet<PathBuf>>> = LazyLock::new(Default::default);

fn held_here() -> MutexGuard<'static, HashSet<PathBuf>> {
    HELD_HERE.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Debug, thiserror::Error)]
pub enum RootLockError {
    /// Another open instance holds the root. `holder` is its process id:
    /// this process's when it holds the root, else read from the lock file,
    /// which Windows does not allow.
    #[error("storage root is held by another open instance")]
    Held { holder: Option<u32> },
    /// The root's filesystem cannot lock, and [`ALLOW_UNLOCKED_ENV_VAR`] did
    /// not allow opening it unlocked.
    #[error("the filesystem cannot lock the storage root: {0}")]
    Unsupported(io::Error),
    #[error("cannot lock the storage root: {0}")]
    Io(#[from] io::Error),
}

impl RootLock {
    /// Take the lock on `root`, creating the directory if needed.
    ///
    /// Fails at once when another instance holds it, in this process or any
    /// other, and on any error opening or locking the lock file.
    pub fn acquire(root: &Path) -> Result<Self, RootLockError> {
        std::fs::create_dir_all(root)?;
        let key = std::fs::canonicalize(root)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(ROOT_LOCK_FILE))?;
        match file.try_lock_exclusive() {
            Ok(()) => {}
            Err(error) if is_contended(&error) => {
                let holder = if held_here().contains(&key) {
                    Some(std::process::id())
                } else {
                    read_holder(&mut file)
                };
                return Err(RootLockError::Held { holder });
            }
            Err(error) if is_unsupported(&error) => {
                if !unlocked_allowed() {
                    return Err(RootLockError::Unsupported(error));
                }
                tracing::warn!(
                    root = %root.display(),
                    %error,
                    "{ALLOW_UNLOCKED_ENV_VAR} is set: opening a storage root its \
                     filesystem cannot lock, without protection from a second process"
                );
                return Ok(Self {
                    handle: None,
                    held_here: None,
                });
            }
            Err(error) => return Err(error.into()),
        }
        // The id only names the holder in a refusal; nothing decides on it.
        let _ = file
            .set_len(0)
            .and_then(|()| writeln!(file, "{}", std::process::id()));
        held_here().insert(key.clone());
        #[cfg(unix)]
        fork::track(&file);
        Ok(Self {
            handle: Some(file),
            held_here: Some(key),
        })
    }

    /// Whether this lock keeps other opens out. False for a root opened
    /// unprotected under [`ALLOW_UNLOCKED_ENV_VAR`].
    pub fn is_locked(&self) -> bool {
        self.handle.is_some()
    }
}

impl Drop for RootLock {
    fn drop(&mut self) {
        // Before the handle closes and frees the root, so no other lock can
        // have taken it and entered it in the set by then.
        if let Some(key) = self.held_here.take() {
            held_here().remove(&key);
        }
        if let Some(file) = &self.handle {
            let _ = FileExt::unlock(file);
            // While the descriptor is still open, so its number is not reused.
            #[cfg(unix)]
            fork::untrack(file);
        }
    }
}

/// A child forked without exec shares each held lock's open file with its
/// parent, and a lock the parent never unlocks lasts until every copy
/// closes, so the root would stay locked after the parent exited. The child
/// gives up its copies as it starts.
#[cfg(unix)]
mod fork {
    use std::fs::File;
    use std::os::fd::AsRawFd;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Once;

    /// The descriptors of the locks this process holds, readable without a
    /// lock from a child forked mid-update. A child forked while more than
    /// 64 roots are held keeps the rest locked until it exits.
    static HELD: [AtomicI32; 64] = [const { AtomicI32::new(-1) }; 64];
    static AT_FORK: Once = Once::new();

    pub(super) fn track(file: &File) {
        AT_FORK.call_once(|| {
            // SAFETY: the handler makes only async-signal-safe calls.
            unsafe { libc::pthread_atfork(None, None, Some(release_in_child)) };
        });
        let fd = file.as_raw_fd();
        for slot in &HELD {
            if slot
                .compare_exchange(-1, fd, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    pub(super) fn untrack(file: &File) {
        let fd = file.as_raw_fd();
        for slot in &HELD {
            if slot
                .compare_exchange(fd, -1, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Points each held descriptor at /dev/null. Unlocking would free the
    /// parent's lock as well, and closing would free the number for reuse
    /// under a `File` the child still owns.
    extern "C" fn release_in_child() {
        if HELD.iter().all(|slot| slot.load(Ordering::Acquire) < 0) {
            return;
        }
        // SAFETY: open, dup2, fcntl and close are async-signal-safe.
        unsafe {
            let null = libc::open(c"/dev/null".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC);
            if null < 0 {
                return;
            }
            for slot in &HELD {
                let fd = slot.load(Ordering::Acquire);
                // dup2 clears close-on-exec, which the lock file had.
                if fd >= 0 && libc::dup2(null, fd) == fd {
                    libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
                }
            }
            libc::close(null);
        }
    }
}

fn is_contended(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::WouldBlock
        || error.raw_os_error() == fs2::lock_contended_error().raw_os_error()
}

/// The filesystem has no working lock, as opposed to failing to take one.
fn is_unsupported(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::Unsupported {
        return true;
    }
    #[cfg(unix)]
    let codes = [libc::ENOLCK, libc::ENOTSUP, libc::EOPNOTSUPP, libc::ENOSYS];
    // ERROR_INVALID_FUNCTION, ERROR_NOT_SUPPORTED
    #[cfg(windows)]
    let codes = [1, 50];
    #[cfg(not(any(unix, windows)))]
    let codes: [i32; 0] = [];
    error
        .raw_os_error()
        .is_some_and(|code| codes.contains(&code))
}

fn unlocked_allowed() -> bool {
    std::env::var(ALLOW_UNLOCKED_ENV_VAR).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

/// The holder's process id. Windows refuses reads of a locked range, so
/// there it is unknown.
fn read_holder(file: &mut File) -> Option<u32> {
    let mut text = String::new();
    file.read_to_string(&mut text).ok()?;
    text.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_held_root_refuses_a_second_lock_and_names_the_holder() {
        let dir = tempfile::tempdir().unwrap();
        let _held = RootLock::acquire(dir.path()).unwrap();

        let refused = RootLock::acquire(dir.path()).unwrap_err();

        let RootLockError::Held { holder } = refused else {
            panic!("expected Held, got {refused:?}");
        };
        assert_eq!(holder, Some(std::process::id()));
    }

    /// Windows does not let a second handle read a locked file; an emptied
    /// lock file stands in for that here.
    #[test]
    fn a_refusal_in_this_process_names_it_without_reading_the_lock_file() {
        let dir = tempfile::tempdir().unwrap();
        let _held = RootLock::acquire(dir.path()).unwrap();
        std::fs::write(dir.path().join(ROOT_LOCK_FILE), "").unwrap();

        let refused = RootLock::acquire(dir.path()).unwrap_err();

        assert!(
            matches!(refused, RootLockError::Held { holder: Some(pid) } if pid == std::process::id()),
            "{refused:?}"
        );
    }

    #[test]
    fn drop_frees_the_root() {
        let dir = tempfile::tempdir().unwrap();
        drop(RootLock::acquire(dir.path()).unwrap());

        RootLock::acquire(dir.path()).expect("dropped lock frees the root");
    }

    #[test]
    fn acquire_creates_a_missing_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("nested").join("root");

        let _held = RootLock::acquire(&root).unwrap();

        assert!(root.join(ROOT_LOCK_FILE).is_file());
    }

    /// An unwritable lock file must not read as "cannot lock": that would
    /// open the root unprotected while its holder still runs.
    #[cfg(unix)]
    #[test]
    fn an_unopenable_lock_file_fails_closed() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let _held = RootLock::acquire(dir.path()).unwrap();
        let lock_file = dir.path().join(ROOT_LOCK_FILE);
        std::fs::set_permissions(&lock_file, std::fs::Permissions::from_mode(0o444)).unwrap();

        let second = RootLock::acquire(dir.path());

        assert!(second.is_err(), "{second:?}");
    }

    /// A forked child holds a copy like this one until it exits.
    #[test]
    fn a_copy_of_the_handle_does_not_keep_a_dropped_lock() {
        let dir = tempfile::tempdir().unwrap();
        let held = RootLock::acquire(dir.path()).unwrap();
        let copy = held.handle.as_ref().unwrap().try_clone().unwrap();

        drop(held);

        RootLock::acquire(dir.path()).expect("the copy kept the root locked");
        drop(copy);
    }

    /// The parent closes its handle without unlocking, as exiting does, once
    /// the child has started.
    #[cfg(unix)]
    #[test]
    fn a_forked_child_does_not_keep_the_root_locked_after_its_parent_exits() {
        let dir = tempfile::tempdir().unwrap();
        let mut held = RootLock::acquire(dir.path()).unwrap();
        let (mut started, mut release) = ([0; 2], [0; 2]);
        // SAFETY: the child makes only async-signal-safe calls.
        let child = unsafe {
            assert_eq!(libc::pipe(started.as_mut_ptr()), 0);
            assert_eq!(libc::pipe(release.as_mut_ptr()), 0);
            let child = libc::fork();
            if child == 0 {
                libc::close(release[1]);
                libc::write(started[1], [1u8].as_ptr().cast(), 1);
                let mut byte = 0u8;
                libc::read(release[0], (&raw mut byte).cast(), 1);
                libc::_exit(0);
            }
            assert!(child > 0, "fork failed");
            libc::close(release[0]);
            let mut byte = 0u8;
            libc::read(started[0], (&raw mut byte).cast(), 1);
            child
        };
        let file = held.handle.take().unwrap();
        fork::untrack(&file);
        drop(held);
        drop(file);

        let reacquired = RootLock::acquire(dir.path());

        // SAFETY: closing the write end lets the child exit; it is reaped here.
        unsafe {
            libc::close(release[1]);
            libc::close(started[0]);
            libc::close(started[1]);
            libc::waitpid(child, std::ptr::null_mut(), 0);
        }
        reacquired.expect("the forked child kept the root locked");
    }

    #[test]
    fn only_a_lockless_filesystem_counts_as_unsupported() {
        assert!(is_unsupported(&io::Error::from(io::ErrorKind::Unsupported)));
        #[cfg(unix)]
        assert!(is_unsupported(&io::Error::from_raw_os_error(libc::ENOLCK)));
        assert!(!is_unsupported(&io::Error::from(
            io::ErrorKind::PermissionDenied
        )));
        assert!(!is_unsupported(&fs2::lock_contended_error()));
    }
}
