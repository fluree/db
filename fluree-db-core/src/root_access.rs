//! Shared ordinary-mode lease and the persistent experimental journal fence.
//! Compiled in every Unix native build, including builds without journal support.
use fs2::FileExt;
use parking_lot::Mutex;
use std::{fs::File, io, path::Path, sync::Arc};

pub(crate) const JOURNAL_DIR: &str = ".fluree-wal";

#[derive(Debug, Default)]
pub(crate) struct RootAccess(Mutex<Option<Arc<File>>>);

impl RootAccess {
    pub(crate) fn ensure(&self, root: &Path) -> io::Result<()> {
        let mut lease = self.0.lock();
        if lease.is_some() {
            return Ok(());
        }
        // Keep construction pure. First access may create an empty root, just as
        // an ordinary write previously created its missing parent directories.
        let root = if root.as_os_str().is_empty() {
            Path::new(".")
        } else {
            root
        };
        // Lock the nearest existing directory before creating a missing root.
        // Otherwise an owner could initialize that empty ancestor between our
        // marker check and mkdir. Reject managed ancestors BEFORE any side effect.
        let mut existing = root;
        let canonical_parent = loop {
            match std::fs::canonicalize(existing) {
                Ok(path) => break path,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    existing = existing
                        .parent()
                        .filter(|p| !p.as_os_str().is_empty())
                        .unwrap_or(Path::new("."));
                }
                Err(e) => return Err(e),
            }
        };
        let parent_lease = File::open(&canonical_parent)?;
        FileExt::try_lock_shared(&parent_lease)?;
        reject_journal_ancestor(&canonical_parent)?;
        std::fs::create_dir_all(root)?;
        let canonical = std::fs::canonicalize(root)?;
        let directory = File::open(&canonical)?;
        FileExt::try_lock_shared(&directory).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "storage root is exclusively owned: {}: {e}",
                    canonical.display()
                ),
            )
        })?;
        reject_journal_ancestor(&canonical)?;
        *lease = Some(Arc::new(directory));
        Ok(())
    }
}

pub(crate) fn reject_journal_ancestor(root: &Path) -> io::Result<()> {
    for ancestor in root.ancestors() {
        match std::fs::symlink_metadata(ancestor.join(JOURNAL_DIR)) {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "journal-managed root requires exclusive recovery access: {}",
                        ancestor.display()
                    ),
                ))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
