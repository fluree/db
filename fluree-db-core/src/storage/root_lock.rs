//! The lock that keeps a file storage root to one open instance.
//!
//! Two instances on one root are not coordinated: each serves its own cached
//! state, runs its own indexer and sweeps files the other may still need. The
//! lock is advisory and belongs to an open handle, so the operating system
//! releases it however the holder exits; the file left on disk means nothing.

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
    _handle: Option<File>,
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
                    _handle: None,
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
        Ok(Self {
            _handle: Some(file),
            held_here: Some(key),
        })
    }
}

impl Drop for RootLock {
    fn drop(&mut self) {
        // Before the handle closes and frees the root, so no other lock can
        // have taken it and entered it in the set by then.
        if let Some(key) = self.held_here.take() {
            held_here().remove(&key);
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
