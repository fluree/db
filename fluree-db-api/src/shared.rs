//! One instance per file storage root per process.
//!
//! [`SharedFluree::open`] gives every caller in a process that opens a root
//! the same instance, so their writes serialize on its ledger locks and each
//! sees the others' commits at once. The instance holds the root's lock
//! ([`FlureeBuilder::with_storage_lock`]) and shuts down when its last handle
//! is closed or dropped; the handles alone decide when.

use crate::{ApiError, Fluree, FlureeBuilder, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard, PoisonError, Weak};
use tokio::sync::watch;

/// The settings every opener of one root must agree on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct SharedOptions {
    /// Run background indexing in this process. On by default.
    pub indexing: bool,
}

impl Default for SharedOptions {
    fn default() -> Self {
        Self { indexing: true }
    }
}

/// A handle on the instance this process keeps for a file storage root.
///
/// Handles dereference to [`Fluree`]. Clones, and handles from other
/// [`SharedFluree::open`] calls on the same root, share the instance. Once
/// the last handle is closed or dropped, the instance shuts down and then
/// releases the root, so another process can open it.
///
/// The handles own the instance's lifecycle: [`Fluree::disconnect`] on it
/// does nothing. A `Fluree` cloned from a handle keeps the root locked until
/// that clone is dropped too, so a clone outliving the handles can never
/// write to a root another owner holds.
#[derive(Clone)]
pub struct SharedFluree {
    root: Arc<SharedRoot>,
}

struct SharedRoot {
    id: u64,
    key: PathBuf,
    options: SharedOptions,
    runtime: tokio::runtime::Handle,
    /// Taken when shutdown begins, which only happens once no handle is
    /// left to dereference it.
    live: Option<Live>,
}

struct Live {
    fluree: Fluree,
    /// Dropped once shutdown completes, which wakes openers that found the
    /// root closing.
    shutdown_done: watch::Sender<()>,
}

enum Slot {
    /// An opener is building the instance; dropped sender = settled.
    Opening(watch::Receiver<()>),
    Open {
        id: u64,
        root: Weak<SharedRoot>,
        shutdown_done: watch::Receiver<()>,
    },
}

enum Step {
    Join(Arc<SharedRoot>),
    Start(watch::Sender<()>),
    Wait(watch::Receiver<()>),
}

static ROOTS: LazyLock<Mutex<HashMap<PathBuf, Slot>>> = LazyLock::new(Default::default);
static NEXT_ID: AtomicU64 = AtomicU64::new(0);

fn roots() -> MutexGuard<'static, HashMap<PathBuf, Slot>> {
    ROOTS.lock().unwrap_or_else(PoisonError::into_inner)
}

impl SharedFluree {
    /// Open the file storage root at `path`, creating it if needed, or join
    /// the instance this process already has open there.
    ///
    /// Joining refuses `options` that differ from the open instance's. A root
    /// whose last handle is still shutting down is waited for, then opened
    /// afresh. A root that another process holds fails with
    /// [`ApiError::StorageInUse`]. The build runs as a task of its own, so a
    /// cancelled open leaves the next one to wait for it.
    pub async fn open(path: impl AsRef<Path>, options: SharedOptions) -> Result<Self> {
        let key = canonical_root(path.as_ref())?;
        loop {
            match Self::next_step(&key) {
                Step::Join(root) => return Self::join(root, options),
                Step::Start(opened) => {
                    return tokio::spawn(Self::start(key, options, opened))
                        .await
                        .map_err(|e| ApiError::internal(format!("opening storage failed: {e}")))?
                }
                Step::Wait(mut settled) => while settled.changed().await.is_ok() {},
            }
        }
    }

    fn next_step(key: &Path) -> Step {
        let mut roots = roots();
        let pending = match roots.get(key) {
            Some(Slot::Open {
                root,
                shutdown_done,
                ..
            }) => match root.upgrade() {
                Some(root) => return Step::Join(root),
                None => shutdown_done.clone(),
            },
            Some(Slot::Opening(opened)) => opened.clone(),
            None => {
                let (opened, settled) = watch::channel(());
                roots.insert(key.to_path_buf(), Slot::Opening(settled));
                return Step::Start(opened);
            }
        };
        // A slot whose sender is gone was abandoned (its build or its
        // shutdown panicked): clear it and look again.
        if pending.has_changed().is_err() {
            roots.remove(key);
            drop(roots);
            return Self::next_step(key);
        }
        Step::Wait(pending)
    }

    fn join(root: Arc<SharedRoot>, options: SharedOptions) -> Result<Self> {
        if root.options != options {
            return Err(ApiError::config(format!(
                "storage '{}' is already open in this process with background indexing {}; \
                 open it with the same settings",
                root.key.display(),
                if root.options.indexing { "on" } else { "off" },
            )));
        }
        Ok(Self { root })
    }

    async fn start(
        key: PathBuf,
        options: SharedOptions,
        opened: watch::Sender<()>,
    ) -> Result<Self> {
        let built = build(&key, options).await;
        let mut roots = roots();
        let outcome = match built {
            Ok(fluree) => {
                let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
                let (shutdown_done, shutdown_watch) = watch::channel(());
                let root = Arc::new(SharedRoot {
                    id,
                    key: key.clone(),
                    options,
                    runtime: tokio::runtime::Handle::current(),
                    live: Some(Live {
                        fluree: fluree.owned_by_shared_handles(),
                        shutdown_done,
                    }),
                });
                roots.insert(
                    key,
                    Slot::Open {
                        id,
                        root: Arc::downgrade(&root),
                        shutdown_done: shutdown_watch,
                    },
                );
                Ok(Self { root })
            }
            Err(error) => {
                roots.remove(&key);
                Err(error)
            }
        };
        drop(roots);
        drop(opened);
        outcome
    }

    /// Close this handle. When it is the instance's last, the instance shuts
    /// down and releases the root before this returns. The shutdown runs as
    /// a task of its own, so cancelling this call does not cut it short.
    pub async fn close(self) {
        let Some(mut root) = Arc::into_inner(self.root) else {
            return;
        };
        let shutdown = root.begin_shutdown();
        drop(root);
        if let Some(shutdown) = shutdown {
            let _ = shutdown.await;
        }
    }

    /// The canonical path of the storage root.
    pub fn root(&self) -> &Path {
        &self.root.key
    }
}

impl std::ops::Deref for SharedFluree {
    type Target = Fluree;

    fn deref(&self) -> &Fluree {
        &self
            .root
            .live
            .as_ref()
            .expect("a handle exists, so its instance is live")
            .fluree
    }
}

impl SharedRoot {
    /// Shut the instance down in a task of its own, once.
    fn begin_shutdown(&mut self) -> Option<tokio::task::JoinHandle<()>> {
        let Live {
            fluree,
            shutdown_done,
        } = self.live.take()?;
        let (key, id) = (self.key.clone(), self.id);
        Some(self.runtime.spawn(async move {
            fluree.shut_down().await;
            // The last reference unless a caller kept a clone, so dropping
            // it releases the root.
            drop(fluree);
            let mut roots = roots();
            if matches!(roots.get(&key), Some(Slot::Open { id: open, .. }) if *open == id) {
                roots.remove(&key);
            }
            drop(roots);
            drop(shutdown_done);
        }))
    }
}

impl Drop for SharedRoot {
    fn drop(&mut self) {
        self.begin_shutdown();
    }
}

async fn build(key: &Path, options: SharedOptions) -> Result<Fluree> {
    let path = key.to_str().ok_or_else(|| {
        ApiError::config(format!(
            "storage path '{}' is not valid UTF-8",
            key.display()
        ))
    })?;
    let mut builder = FlureeBuilder::file(path).with_storage_lock();
    if !options.indexing {
        builder = builder.without_indexing();
    }
    builder.build_async().await
}

/// `path` resolved through symlinks and `.`/`..`, so every spelling of one
/// root finds the same instance. Creates the directory, which must exist to
/// be resolved.
fn canonical_root(path: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(path)
        .and_then(|()| std::fs::canonicalize(path))
        .map_err(|e| ApiError::config(format!("cannot open storage '{}': {e}", path.display())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const LEDGER: &str = "shared/test:main";

    fn without_indexing() -> SharedOptions {
        SharedOptions { indexing: false }
    }

    async fn open(path: &Path) -> SharedFluree {
        SharedFluree::open(path, without_indexing())
            .await
            .unwrap_or_else(|e| panic!("open: {e}"))
    }

    async fn insert(fluree: &Fluree, n: usize) {
        fluree
            .graph(LEDGER)
            .transact()
            .insert(&json!({
                "@context": { "ex": "http://example.org/" },
                "@id": format!("ex:item-{n}"),
                "ex:n": n,
            }))
            .commit()
            .await
            .expect("insert");
    }

    async fn count(fluree: &Fluree) -> usize {
        let rows = fluree
            .graph(LEDGER)
            .query()
            .jsonld(&json!({
                "@context": { "ex": "http://example.org/" },
                "select": ["?s"],
                "where": { "@id": "?s", "ex:n": "?n" }
            }))
            .execute_formatted()
            .await
            .expect("query");
        rows.as_array().map_or(0, Vec::len)
    }

    fn lock_separately(path: &Path) -> Result<Fluree> {
        FlureeBuilder::file(path.to_str().unwrap())
            .without_indexing()
            .with_storage_lock()
            .build()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn opens_of_one_root_share_an_instance() {
        let dir = tempfile::tempdir().unwrap();
        let a = open(dir.path()).await;
        a.create_ledger(LEDGER).await.expect("create");
        insert(&a, 0).await;

        let b = open(&dir.path().join(".")).await;
        assert_eq!(count(&b).await, 1);
        insert(&a, 1).await;
        assert_eq!(count(&b).await, 2, "b sees a's commit without a refresh");
        insert(&b, 2).await;
        assert_eq!(count(&a).await, 3);

        a.close().await;
        b.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn joining_with_other_settings_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let held = open(dir.path()).await;

        let refused = SharedFluree::open(dir.path(), SharedOptions::default())
            .await
            .err()
            .expect("settings differ");

        assert!(
            refused.to_string().contains("background indexing off"),
            "{refused}"
        );
        held.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_root_stays_held_until_the_last_handle_closes() {
        let dir = tempfile::tempdir().unwrap();
        let a = open(dir.path()).await;
        let b = open(dir.path()).await;
        assert!(matches!(
            lock_separately(dir.path()),
            Err(ApiError::StorageInUse { .. })
        ));

        a.close().await;
        assert!(
            matches!(
                lock_separately(dir.path()),
                Err(ApiError::StorageInUse { .. })
            ),
            "b still holds the root"
        );

        b.close().await;
        let next_owner = lock_separately(dir.path()).expect("the last close releases the root");
        next_owner.disconnect().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reopening_while_the_last_drop_shuts_down_waits_for_it() {
        let dir = tempfile::tempdir().unwrap();
        let first = open(dir.path()).await;
        first.create_ledger(LEDGER).await.expect("create");
        insert(&first, 0).await;
        drop(first);

        let reopened = open(dir.path()).await;
        assert_eq!(count(&reopened).await, 1);
        reopened.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cancelled_close_still_finishes_shutting_down() {
        let dir = tempfile::tempdir().unwrap();
        let handle = open(dir.path()).await;
        let key = handle.root().to_path_buf();

        let _ = tokio::time::timeout(std::time::Duration::ZERO, handle.close()).await;

        let settled = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while roots().contains_key(&key) {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(settled.is_ok(), "the shutdown stopped with its caller");
        let next_owner = lock_separately(dir.path()).expect("the shutdown released the root");
        next_owner.disconnect().await;
    }

    /// One blocking thread, held busy, queues the first open's WAL replay,
    /// so that open is cancelled while its build holds the root.
    #[test]
    fn reopening_after_a_cancelled_open_waits_for_its_build() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        runtime.block_on(async {
            let (release, wait) = std::sync::mpsc::channel::<()>();
            let busy = tokio::task::spawn_blocking(move || {
                let _ = wait.recv();
            });
            let cancelled = tokio::time::timeout(std::time::Duration::ZERO, open(dir.path())).await;
            assert!(cancelled.is_err(), "the replay was meant to be queued");

            let mut reopen = std::pin::pin!(SharedFluree::open(dir.path(), without_indexing()));
            let first_poll = tokio::time::timeout(std::time::Duration::ZERO, reopen.as_mut()).await;
            release.send(()).unwrap();
            busy.await.unwrap();
            let reopened = match first_poll {
                Ok(result) => result,
                Err(_) => reopen.await,
            }
            .unwrap_or_else(|e| panic!("reopen after a cancelled open: {e}"));
            reopened.close().await;
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn disconnect_through_a_handle_leaves_the_instance_to_its_handles() {
        let dir = tempfile::tempdir().unwrap();
        let a = open(dir.path()).await;
        let b = open(dir.path()).await;
        a.create_ledger(LEDGER).await.expect("create");
        insert(&a, 0).await;

        a.disconnect().await;

        let cached = b.ledger_manager().expect("caching").cached_aliases().await;
        assert_eq!(cached.len(), 1, "b's cache survives a's disconnect");
        assert!(matches!(
            lock_separately(dir.path()),
            Err(ApiError::StorageInUse { .. })
        ));
        a.close().await;
        b.close().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_clone_of_the_instance_keeps_the_root_until_it_drops() {
        let dir = tempfile::tempdir().unwrap();
        let handle = open(dir.path()).await;
        let clone = Fluree::clone(&handle);

        handle.close().await;
        assert!(
            matches!(
                lock_separately(dir.path()),
                Err(ApiError::StorageInUse { .. })
            ),
            "the clone still holds the root"
        );

        drop(clone);
        let next_owner = lock_separately(dir.path()).expect("the last clone released the root");
        next_owner.disconnect().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn concurrent_open_write_close_loses_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let seed = open(dir.path()).await;
        seed.create_ledger(LEDGER).await.expect("create");
        seed.close().await;

        let workers: Vec<_> = (0..8)
            .map(|worker| {
                let path = dir.path().to_path_buf();
                tokio::spawn(async move {
                    for round in 0..5 {
                        let fluree = open(&path).await;
                        insert(&fluree, worker * 100 + round).await;
                        fluree.close().await;
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.await.expect("worker");
        }

        let fluree = open(dir.path()).await;
        assert_eq!(count(&fluree).await, 40);
        fluree.close().await;
    }
}
