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
use std::hash::{BuildHasher, BuildHasherDefault};
use std::sync::Arc;

use futures::channel::oneshot;
use futures::future::{FutureExt, Shared};
use parking_lot::Mutex;
use rustc_hash::FxHasher;

use crate::address_path::StorageNamespace;
use crate::ContentId;

const SHARDS: usize = 64;

/// Abandoned blobs kept for the collector; past this, reconciliation finds
/// the rest.
const MAX_ABANDONED: usize = 100_000;

enum Slot {
    Pinned(usize),
    /// Resolves when the claim ends.
    Claimed(Shared<oneshot::Receiver<()>>),
}

type Shard = Mutex<HashMap<ContentId, Slot>>;

/// The pins of one storage's writers. See the module docs.
pub struct BlobPins {
    shards: Box<[Shard]>,
    abandoned: Mutex<Vec<Abandoned>>,
}

/// A blob whose writer stored it and then failed before publishing a
/// commit naming it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Abandoned {
    pub namespace: StorageNamespace,
    pub id: ContentId,
}

impl Default for BlobPins {
    fn default() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Shard::default()).collect(),
            abandoned: Mutex::default(),
        }
    }
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

    fn shard(&self, id: &ContentId) -> &Shard {
        let hash = BuildHasherDefault::<FxHasher>::default().hash_one(id);
        &self.shards[hash as usize % SHARDS]
    }

    /// Pin `id`, stored under `namespace`, once no claim holds it.
    pub async fn pin(self: &Arc<Self>, namespace: &StorageNamespace, id: &ContentId) -> BlobPin {
        loop {
            let claimed = {
                let mut shard = self.shard(id).lock();
                match shard.get_mut(id) {
                    Some(Slot::Pinned(n)) => {
                        *n += 1;
                        None
                    }
                    Some(Slot::Claimed(done)) => Some(done.clone()),
                    None => {
                        shard.insert(id.clone(), Slot::Pinned(1));
                        None
                    }
                }
            };
            match claimed {
                Some(done) => {
                    let _ = done.await;
                }
                None => {
                    return BlobPin {
                        pins: Arc::clone(self),
                        namespace: namespace.clone(),
                        id: id.clone(),
                    }
                }
            }
        }
    }

    /// A claim for deleting `id`, unless a writer pins it or another claim
    /// holds it.
    pub fn try_claim(self: &Arc<Self>, id: &ContentId) -> Option<BlobClaim> {
        let mut shard = self.shard(id).lock();
        if shard.contains_key(id) {
            return None;
        }
        let (done, ended) = oneshot::channel();
        shard.insert(id.clone(), Slot::Claimed(ended.shared()));
        Some(BlobClaim {
            pins: Arc::clone(self),
            id: id.clone(),
            _done: done,
        })
    }

    /// The blobs abandoned since the last call.
    pub fn take_abandoned(&self) -> Vec<Abandoned> {
        std::mem::take(&mut *self.abandoned.lock())
    }

    fn release(&self, id: &ContentId) {
        let mut shard = self.shard(id).lock();
        if let Some(Slot::Pinned(n)) = shard.get_mut(id) {
            *n -= 1;
            if *n == 0 {
                shard.remove(id);
            }
        }
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
        self.pins.release(&self.id);
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
        self.pins.shard(&self.id).lock().remove(&self.id);
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
        drop(pin);
        assert!(pins.try_claim(&id(b"other")).is_some());
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
