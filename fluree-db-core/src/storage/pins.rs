//! Protection for commit and transaction blobs a writer has stored but not
//! yet published, against a collector deleting them meanwhile.
//!
//! Content addressing shares such blobs: a retry, or another branch
//! committing the same transaction body, stores the same id, and storing
//! bytes already present writes nothing. A collector that found a blob
//! unreferenced could otherwise delete it just before a writer that stored
//! it again publishes a commit naming it. So a writer pins each id before
//! storing it and releases the pin once a published commit names it, or
//! abandons it on failure, which queues the blob for a garbage record. A
//! collector deletes only under a claim, granted only while nothing pins the
//! id; a pin asked for during a claim waits for the claim to end, and its
//! writer then stores the bytes anew.

use std::collections::HashMap;
use std::hash::{BuildHasher, BuildHasherDefault, Hash};
use std::sync::Arc;
use std::time::Duration;

use futures::channel::oneshot;
use futures::future::{FutureExt, Shared};
use parking_lot::Mutex;
use rustc_hash::FxHasher;

use crate::address_path::{StorageNamespace, StorageRoot};
use crate::clock::Instant;
use crate::ContentId;

const SHARDS: usize = 64;

/// Abandoned blobs kept for the collector; past this, reconciliation finds
/// the rest.
const MAX_ABANDONED: usize = 100_000;

enum Slot {
    Shared(usize),
    /// Resolves when the exclusive hold ends.
    Exclusive(Shared<oneshot::Receiver<()>>),
}

/// Shared holds that wait out an exclusive one, and exclusive holds granted
/// only while no hold is taken, per key.
struct Gate<K> {
    shards: Box<[Mutex<HashMap<K, Slot>>]>,
}

impl<K: Hash + Eq + Clone> Default for Gate<K> {
    fn default() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::default()).collect(),
        }
    }
}

impl<K: Hash + Eq + Clone> Gate<K> {
    fn shard(&self, key: &K) -> &Mutex<HashMap<K, Slot>> {
        let hash = BuildHasherDefault::<FxHasher>::default().hash_one(key);
        &self.shards[hash as usize % SHARDS]
    }

    /// How long it waited out an exclusive hold, if it did.
    async fn share(&self, key: &K) -> Option<Duration> {
        let mut waiting_since = None;
        loop {
            let exclusive = {
                let mut shard = self.shard(key).lock();
                match shard.get_mut(key) {
                    Some(Slot::Shared(n)) => {
                        *n += 1;
                        None
                    }
                    Some(Slot::Exclusive(ended)) => Some(ended.clone()),
                    None => {
                        shard.insert(key.clone(), Slot::Shared(1));
                        None
                    }
                }
            };
            match exclusive {
                Some(ended) => {
                    waiting_since.get_or_insert_with(Instant::now);
                    let _ = ended.await;
                }
                None => return waiting_since.map(|since| since.elapsed()),
            }
        }
    }

    fn unshare(&self, key: &K) {
        let mut shard = self.shard(key).lock();
        if let Some(Slot::Shared(n)) = shard.get_mut(key) {
            *n -= 1;
            if *n == 0 {
                shard.remove(key);
            }
        }
    }

    /// The sender that ends the exclusive hold when dropped, once
    /// [`Self::release`] has removed it.
    fn try_exclusive(&self, key: &K) -> Option<oneshot::Sender<()>> {
        let mut shard = self.shard(key).lock();
        if shard.contains_key(key) {
            return None;
        }
        let (done, ended) = oneshot::channel();
        shard.insert(key.clone(), Slot::Exclusive(ended.shared()));
        Some(done)
    }

    fn release(&self, key: &K) {
        self.shard(key).lock().remove(key);
    }
}

/// The pins of one storage's writers. See the module docs.
#[derive(Default)]
pub struct BlobPins {
    blobs: Gate<ContentId>,
    roots: Gate<StorageRoot>,
    abandoned: Mutex<Vec<Abandoned>>,
    waits: Mutex<PinWaits>,
}

/// Pins that waited for a collector's claim to end, and their waits in all.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PinWaits {
    pub count: u64,
    pub total: Duration,
}

/// A blob whose writer stored it and then failed before publishing a
/// commit naming it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Abandoned {
    pub namespace: StorageNamespace,
    pub id: ContentId,
}

impl std::fmt::Debug for BlobPins {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobPins").finish_non_exhaustive()
    }
}

impl BlobPins {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin `id`, stored under `namespace`, once no claim holds it.
    pub async fn pin(self: &Arc<Self>, namespace: &StorageNamespace, id: &ContentId) -> BlobPin {
        if let Some(waited) = self.blobs.share(id).await {
            let mut waits = self.waits.lock();
            waits.count += 1;
            waits.total += waited;
        }
        BlobPin {
            pins: Arc::clone(self),
            namespace: namespace.clone(),
            id: id.clone(),
        }
    }

    /// A claim for deleting `id`, unless a writer pins it or another claim
    /// holds it.
    pub fn try_claim(self: &Arc<Self>, id: &ContentId) -> Option<BlobClaim> {
        let done = self.blobs.try_exclusive(id)?;
        Some(BlobClaim {
            pins: Arc::clone(self),
            id: id.clone(),
            _done: done,
        })
    }

    /// Keep collection out of the ledger at `root` for a transfer that
    /// stores blobs over several calls before publishing a head naming
    /// them, once a collection pass there has ended.
    pub async fn hold_root(self: &Arc<Self>, root: &StorageRoot) -> RootHold {
        self.roots.share(root).await;
        RootHold {
            pins: Arc::clone(self),
            root: root.clone(),
        }
    }

    /// A collection pass over the ledger at `root`, unless a transfer holds
    /// it or another pass is under way.
    pub fn try_collect_root(self: &Arc<Self>, root: &StorageRoot) -> Option<RootCollection> {
        let done = self.roots.try_exclusive(root)?;
        Some(RootCollection {
            pins: Arc::clone(self),
            root: root.clone(),
            _done: done,
        })
    }

    /// The blobs abandoned since the last call.
    pub fn take_abandoned(&self) -> Vec<Abandoned> {
        std::mem::take(&mut *self.abandoned.lock())
    }

    pub fn waits(&self) -> PinWaits {
        *self.waits.lock()
    }
}

/// A writer's protection of one blob. Dropping it releases the protection.
pub struct BlobPin {
    pins: Arc<BlobPins>,
    namespace: StorageNamespace,
    id: ContentId,
}

impl BlobPin {
    pub fn id(&self) -> &ContentId {
        &self.id
    }

    /// Release the pin, queueing the blob for a garbage record: its writer
    /// failed, so no published commit it made names the blob.
    pub fn abandon(self) {
        let mut abandoned = self.pins.abandoned.lock();
        if abandoned.len() < MAX_ABANDONED {
            abandoned.push(Abandoned {
                namespace: self.namespace.clone(),
                id: self.id.clone(),
            });
        }
    }
}

impl Drop for BlobPin {
    fn drop(&mut self) {
        self.pins.blobs.unshare(&self.id);
    }
}

impl std::fmt::Debug for BlobPin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobPin").field("id", &self.id).finish()
    }
}

/// A collector's exclusive hold on a blob it may delete. Pins asked for
/// meanwhile wait until it is dropped.
pub struct BlobClaim {
    pins: Arc<BlobPins>,
    id: ContentId,
    _done: oneshot::Sender<()>,
}

impl Drop for BlobClaim {
    fn drop(&mut self) {
        self.pins.blobs.release(&self.id);
    }
}

/// A transfer's hold on a ledger against collection. Dropping it lets
/// collection back in.
pub struct RootHold {
    pins: Arc<BlobPins>,
    root: StorageRoot,
}

impl Drop for RootHold {
    fn drop(&mut self) {
        self.pins.roots.unshare(&self.root);
    }
}

impl std::fmt::Debug for RootHold {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RootHold")
            .field("root", &self.root.as_str())
            .finish()
    }
}

/// A collection pass's exclusive hold on a ledger. Transfers asking for a
/// hold meanwhile wait until it is dropped.
pub struct RootCollection {
    pins: Arc<BlobPins>,
    root: StorageRoot,
    _done: oneshot::Sender<()>,
}

impl Drop for RootCollection {
    fn drop(&mut self) {
        self.pins.roots.release(&self.root);
    }
}

/// Where a content store's writes are pinned: its storage's registry and
/// its own namespace.
#[derive(Clone, Debug)]
pub struct BlobPinner {
    pins: Arc<BlobPins>,
    namespace: StorageNamespace,
}

impl BlobPinner {
    pub fn new(pins: Arc<BlobPins>, namespace: StorageNamespace) -> Self {
        Self { pins, namespace }
    }

    pub async fn pin(&self, id: &ContentId) -> BlobPin {
        self.pins.pin(&self.namespace, id).await
    }
}

/// The pins a writer holds until a commit naming their blobs is published.
/// Dropped unpublished, each is abandoned.
#[derive(Debug, Default)]
pub struct Unpublished(Vec<BlobPin>);

impl Unpublished {
    pub fn new() -> Self {
        Self::default()
    }

    /// Pin `id` through `pinner`, when the store has one.
    pub async fn pin(&mut self, pinner: Option<&BlobPinner>, id: &ContentId) {
        if let Some(pinner) = pinner {
            self.0.push(pinner.pin(id).await);
        }
    }

    pub fn push(&mut self, pin: BlobPin) {
        self.0.push(pin);
    }

    /// A published commit names every pinned blob: release them.
    pub fn published(mut self) {
        self.0.clear();
    }
}

impl Drop for Unpublished {
    fn drop(&mut self) {
        for pin in self.0.drain(..) {
            pin.abandon();
        }
    }
}

#[cfg(all(test, feature = "native"))]
mod tests {
    use super::*;
    use crate::address_path::StorageRoot;
    use crate::ContentKind;
    use std::time::Duration;

    fn namespace() -> StorageNamespace {
        StorageNamespace::new(StorageRoot::parse("ledger").unwrap(), "main")
    }

    fn id(seed: &[u8]) -> ContentId {
        ContentId::new(ContentKind::Txn, seed)
    }

    #[tokio::test]
    async fn a_blob_stays_protected_until_its_last_pin_goes() {
        let pins = Arc::new(BlobPins::new());
        let first = pins.pin(&namespace(), &id(b"body")).await;
        let second = pins.pin(&namespace(), &id(b"body")).await;
        assert_eq!(pins.waits(), PinWaits::default());
        drop(first);
        assert!(pins.try_claim(&id(b"body")).is_none());
        drop(second);
        assert!(pins.try_claim(&id(b"body")).is_some());
    }

    #[tokio::test]
    async fn a_pin_waits_out_a_claim() {
        let pins = Arc::new(BlobPins::new());
        let claim = pins.try_claim(&id(b"body")).expect("unpinned");
        assert!(pins.try_claim(&id(b"body")).is_none());
        let waiting = tokio::spawn({
            let pins = Arc::clone(&pins);
            async move { pins.pin(&namespace(), &id(b"body")).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished());
        drop(claim);
        let pin = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("woken")
            .unwrap();
        assert!(pins.try_claim(&id(b"body")).is_none());
        let waits = pins.waits();
        assert_eq!(waits.count, 1);
        assert!(waits.total >= Duration::from_millis(20), "{waits:?}");
        drop(pin);
        assert!(pins.try_claim(&id(b"other")).is_some());
    }

    #[tokio::test]
    async fn a_held_ledger_is_not_collected_and_a_hold_waits_out_a_pass() {
        let pins = Arc::new(BlobPins::new());
        let root = StorageRoot::parse("ledger").unwrap();
        let hold = pins.hold_root(&root).await;
        assert!(pins.try_collect_root(&root).is_none());
        drop(hold);
        let pass = pins.try_collect_root(&root).expect("unheld");
        let waiting = tokio::spawn({
            let pins = Arc::clone(&pins);
            let root = root.clone();
            async move { pins.hold_root(&root).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiting.is_finished());
        drop(pass);
        let hold = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("woken")
            .unwrap();
        assert!(pins.try_collect_root(&root).is_none());
        drop(hold);
    }

    #[tokio::test]
    async fn only_abandoned_pins_are_queued() {
        let pins = Arc::new(BlobPins::new());
        let mut published = Unpublished::new();
        let pinner = BlobPinner::new(Arc::clone(&pins), namespace());
        published.pin(Some(&pinner), &id(b"kept")).await;
        published.published();
        let mut failed = Unpublished::new();
        failed.pin(Some(&pinner), &id(b"failed")).await;
        drop(failed);
        assert_eq!(
            pins.take_abandoned(),
            vec![Abandoned {
                namespace: namespace(),
                id: id(b"failed"),
            }]
        );
        assert!(pins.try_claim(&id(b"failed")).is_some());
        assert!(pins.take_abandoned().is_empty());
    }
}
