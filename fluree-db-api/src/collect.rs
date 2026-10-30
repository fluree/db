//! What a cluster member's writers check before they store: the addresses
//! the cluster's collection has condemned.
//!
//! The leader condemns unreferenced blobs as numbered intents, which each
//! member loads here in order. A write runs in a scope ([`Condemned::scoped`])
//! that records how many intents were loaded when it began ([`seen`]) and
//! what it stores. The command that publishes it presents `seen`, and is
//! refused once an intent it began before has begun deleting, so a write
//! that stored a condemned address unchecked never names it after the
//! collector's final look. A store to a condemned address is spared first
//! ([`Spare`]); one whose deletion has begun waits for the intent to close.
//! Loading an intent spares what scopes under way already stored of it, so
//! their writes need not be refused.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Notify;

use fluree_db_core::ContentId;

use crate::{ApiError, Fluree, Result};

/// Where intent objects live, outside every ledger's folders.
const INTENT_DIR: &str = "@retention/collect/intent";

/// What an intent condemns. Value-free: addresses and why.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntentObject {
    /// Drawn for each intent: no two intents share a name, so a delete of
    /// a closed intent's object, however late or replayed, removes no
    /// other's.
    pub nonce: String,
    pub entries: Vec<IntentEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IntentEntry {
    pub address: String,
    /// Why it is collectable.
    pub reason: String,
    /// The ledger whose branches decide whether it is still unreferenced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ledger: Option<String>,
    /// The garbage record that named it, dropped once it is decided.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<String>,
}

/// A cluster's collection, proposed on its leader; see the module docs.
#[async_trait]
pub trait Collector: Send + Sync + std::fmt::Debug {
    /// Condemn what the intent object `intent` names: its number.
    async fn condemn(&self, intent: &str) -> Result<u64>;

    /// Begin intent `number`'s deletion: what writers spared; `None` once
    /// the intent is closed.
    async fn delete(&self, number: u64) -> Result<Option<Vec<String>>>;

    /// Close intent `number`: its deletes are done.
    async fn complete(&self, number: u64) -> Result<()>;

    /// The open intents: number, object hash, and whether deleting.
    async fn open(&self) -> Vec<(u64, String, bool)>;

    /// What ledger `ledger`'s queued requests, and the requests its
    /// idempotency records keep, name: their envelopes, and what a queued
    /// envelope names (a raw body, pushed commits).
    async fn queued(&self, fluree: &Fluree, ledger: &str) -> Result<HashSet<ContentId>>;

    /// Until when this member, as leader, may delete: no other leader can
    /// be elected, and complete an intent, before then. `None` when it is
    /// not the leader.
    async fn lease(&self) -> Option<std::time::Instant>;
}

impl Fluree {
    /// Collect this cluster member's garbage through `collector`, when it
    /// leads.
    pub fn set_collector(&self, collector: Arc<dyn Collector>) {
        if let Some(runtime) = self.retention.erasure.get() {
            let _ = runtime.collector.set(collector);
        }
    }

    /// The cluster's collection, on a cluster member.
    pub(crate) fn collector(&self) -> Option<&Arc<dyn Collector>> {
        self.retention.erasure.get()?.collector.get()
    }
}

impl Fluree {
    /// Where the intent object `hash` names lives, under storage `method`.
    pub fn intent_address(&self, method: &str, hash: &str) -> String {
        format!("fluree:{method}://{INTENT_DIR}/{hash}.json")
    }

    /// Store an intent condemning `entries`, immutable and named by its
    /// hash, which it returns.
    pub async fn put_intent(&self, entries: Vec<IntentEntry>) -> Result<String> {
        let storage = self
            .admin_storage()
            .ok_or_else(|| ApiError::internal("collection needs managed storage"))?;
        let intent = IntentObject {
            nonce: hex::encode(rand::random::<[u8; 16]>()),
            entries,
        };
        let bytes = serde_json::to_vec(&intent)?;
        let hash = hex::encode(Sha256::digest(&bytes));
        let address = self.intent_address(storage.storage_method(), &hash);
        storage.write_bytes(&address, &bytes).await?;
        Ok(hash)
    }

    /// The intent object `hash` names, checked against it.
    pub async fn intent(&self, hash: &str) -> Result<IntentObject> {
        let storage = self
            .admin_storage()
            .ok_or_else(|| ApiError::internal("collection needs managed storage"))?;
        let bytes = storage
            .read_bytes(&self.intent_address(storage.storage_method(), hash))
            .await?;
        if hex::encode(Sha256::digest(&bytes)) != hash {
            return Err(ApiError::internal(format!(
                "intent object {hash} does not match its name"
            )));
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
}

/// What a [`Spare`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Spared {
    /// The address stays.
    Kept,
    /// The intent's deletion has begun; its close is when to write.
    Refused,
    /// The intent is closed.
    Closed,
}

/// Keeps an address out of an intent's deletion, through the cluster.
#[async_trait]
pub trait Spare: Send + Sync {
    async fn spare(&self, number: u64, address: &str) -> std::result::Result<Spared, String>;
}

tokio::task_local! {
    static SCOPE: Arc<Scope>;
}

/// The intents loaded when the write under way here began, if it runs in a
/// scope: what the command publishing it presents.
pub fn seen() -> Option<u64> {
    SCOPE
        .try_with(|scope| {
            scope
                .seen
                .load(Ordering::SeqCst)
                .min(scope.ceiling.load(Ordering::SeqCst))
        })
        .ok()
}

/// Present no more than `epoch` for the write under way here, whatever
/// loads after: it publishes something another task stored, after this
/// member had loaded `epoch` intents, which no load finds in the scope.
pub fn lower_seen(epoch: u64) {
    let _ = SCOPE.try_with(|scope| scope.ceiling.fetch_min(epoch, Ordering::SeqCst));
}

/// One write under way.
struct Scope {
    seen: AtomicU64,
    /// What [`lower_seen`] bounds `seen` to.
    ceiling: AtomicU64,
    stored: Mutex<Vec<String>>,
}

/// This member's condemned addresses and the writes under way.
#[derive(Default)]
pub struct Condemned {
    /// Set on a cluster member: its writes are checked.
    active: AtomicBool,
    /// Whether any loaded intent is open.
    any: AtomicBool,
    inner: Mutex<Inner>,
    closed: Notify,
    spare: OnceLock<Arc<dyn Spare>>,
    /// Held to load: by the member's loader, and by a writer catching up.
    loading: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct Inner {
    /// Intents numbered below this are loaded or were closed unloaded.
    loaded: u64,
    /// Each condemned address and its intent, but those spared here.
    addresses: HashMap<String, u64>,
    /// Open intents' addresses.
    intents: BTreeMap<u64, Vec<String>>,
    scopes: Vec<Weak<Scope>>,
}

impl std::fmt::Debug for Condemned {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        f.debug_struct("Condemned")
            .field("active", &self.active.load(Ordering::Relaxed))
            .field("loaded", &inner.loaded)
            .field("open", &inner.intents.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Condemned {
    /// Check this member's writes from now on, sparing through `spare`.
    pub fn activate(&self, spare: Arc<dyn Spare>) {
        let _ = self.spare.set(spare);
        self.active.store(true, Ordering::SeqCst);
    }

    /// Intents numbered below this are loaded.
    pub fn loaded(&self) -> u64 {
        self.inner.lock().loaded
    }

    /// Held to load intents, one loader at a time.
    pub async fn loading(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.loading.lock().await
    }

    /// [`Self::loaded`] where this member's writes are checked.
    pub fn intents_loaded(&self) -> Option<u64> {
        self.active.load(Ordering::SeqCst).then(|| self.loaded())
    }

    /// Run `write` in a scope: what it stores is checked, and [`seen`]
    /// answers for it. A member whose writes are not checked runs it as is.
    pub async fn scoped<F: Future>(&self, write: F) -> F::Output {
        if !self.active.load(Ordering::SeqCst) {
            return write.await;
        }
        let scope = {
            let mut inner = self.inner.lock();
            let scope = Arc::new(Scope {
                seen: AtomicU64::new(inner.loaded),
                ceiling: AtomicU64::new(u64::MAX),
                stored: Mutex::default(),
            });
            inner.scopes.retain(|s| s.strong_count() > 0);
            inner.scopes.push(Arc::downgrade(&scope));
            scope
        };
        SCOPE.scope(scope, write).await
    }

    /// Before `address` is stored: record it in the scope under way, and
    /// spare it if an intent condemns it.
    pub(crate) async fn before_store(&self, address: &str) -> Result<()> {
        if !self.active.load(Ordering::SeqCst) {
            return Ok(());
        }
        let scoped = SCOPE
            .try_with(|scope| scope.stored.lock().push(address.to_string()))
            .is_ok();
        // Recorded before `any` is read, so an intent loaded meanwhile
        // finds the address in the scope instead.
        if !self.any.load(Ordering::SeqCst) {
            return Ok(());
        }
        let Some(number) = self.inner.lock().addresses.get(address).copied() else {
            return Ok(());
        };
        if !scoped {
            // Its publisher must present the intents loaded before it stored.
            tracing::debug!(
                address,
                "a write outside any scope stores a condemned address"
            );
        }
        match self.spare(number, address).await? {
            Spared::Kept | Spared::Closed => {}
            Spared::Refused => self.wait_closed(number).await,
        }
        Ok(())
    }

    async fn spare(&self, number: u64, address: &str) -> Result<Spared> {
        let spare = self
            .spare
            .get()
            .ok_or_else(|| ApiError::internal("no way to spare a condemned address"))?;
        let spared = spare
            .spare(number, address)
            .await
            .map_err(|e| ApiError::internal(format!("sparing {address}: {e}")))?;
        if spared == Spared::Kept {
            let mut inner = self.inner.lock();
            if inner.addresses.get(address) == Some(&number) {
                inner.addresses.remove(address);
            }
        }
        Ok(spared)
    }

    async fn wait_closed(&self, number: u64) {
        loop {
            let closed = self.closed.notified();
            tokio::pin!(closed);
            closed.as_mut().enable();
            if !self.inner.lock().intents.contains_key(&number) {
                return;
            }
            closed.await;
        }
    }

    /// Load intent `number`, condemning `addresses`. Intents load in order.
    /// What scopes under way since before it already stored of it is
    /// spared, and each scope that stored none, or had all of it kept,
    /// counts as having seen it. It counts as loaded once those spares are
    /// done.
    pub async fn load(&self, number: u64, addresses: Vec<String>) -> Result<()> {
        let mut stored_here = Vec::new();
        {
            let mut inner = self.inner.lock();
            if number < inner.loaded {
                return Ok(());
            }
            if number != inner.loaded {
                return Err(ApiError::internal(format!(
                    "intent {number} loaded out of order; {} expected",
                    inner.loaded
                )));
            }
            for address in &addresses {
                inner.addresses.insert(address.clone(), number);
            }
            inner.intents.insert(number, addresses);
            self.any.store(true, Ordering::SeqCst);
            let scopes: Vec<Arc<Scope>> = inner.scopes.iter().filter_map(Weak::upgrade).collect();
            for scope in scopes {
                if scope.seen.load(Ordering::SeqCst) != number {
                    continue;
                }
                let hits: Vec<String> = scope
                    .stored
                    .lock()
                    .iter()
                    .filter(|a| inner.addresses.get(*a) == Some(&number))
                    .cloned()
                    .collect();
                if hits.is_empty() {
                    scope.seen.store(number + 1, Ordering::SeqCst);
                } else {
                    stored_here.push((scope, hits));
                }
            }
        }
        for (scope, hits) in stored_here {
            let mut kept = true;
            for address in &hits {
                match self.spare(number, address).await {
                    Ok(Spared::Kept) => {}
                    // Deleted, or being deleted, after the scope stored it.
                    Ok(Spared::Refused | Spared::Closed) => kept = false,
                    Err(error) => {
                        tracing::warn!(%error, "a write under way stays fenced by intent {number}");
                        kept = false;
                    }
                }
            }
            if kept {
                let _ = scope.seen.compare_exchange(
                    number,
                    number + 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                );
            }
        }
        self.inner.lock().loaded = number + 1;
        Ok(())
    }

    /// Intent `number` closed before it was loaded here. Scopes under way
    /// since before it stay fenced by it: what they stored was not checked.
    pub fn skip(&self, number: u64) {
        let mut inner = self.inner.lock();
        if inner.loaded == number {
            inner.loaded = number + 1;
        }
    }

    /// Intent `number` closed: its addresses may be written again.
    pub fn close(&self, number: u64) {
        {
            let mut inner = self.inner.lock();
            if let Some(addresses) = inner.intents.remove(&number) {
                for address in addresses {
                    if inner.addresses.get(&address) == Some(&number) {
                        inner.addresses.remove(&address);
                    }
                }
            }
            if inner.intents.is_empty() {
                self.any.store(false, Ordering::SeqCst);
            }
        }
        self.closed.notify_waiters();
    }

    /// The open intents loaded here.
    pub fn open(&self) -> Vec<u64> {
        self.inner.lock().intents.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[derive(Default)]
    struct Spares {
        refuse: AtomicBool,
        spared: Mutex<Vec<(u64, String)>>,
    }

    #[async_trait]
    impl Spare for Spares {
        async fn spare(&self, number: u64, address: &str) -> std::result::Result<Spared, String> {
            if self.refuse.load(Ordering::SeqCst) {
                return Ok(Spared::Refused);
            }
            self.spared.lock().push((number, address.to_string()));
            Ok(Spared::Kept)
        }
    }

    fn active() -> (Arc<Condemned>, Arc<Spares>) {
        let condemned = Arc::new(Condemned::default());
        let spares = Arc::new(Spares::default());
        condemned.activate(spares.clone());
        (condemned, spares)
    }

    /// A scope sees the intents loaded when it began; one that stored
    /// nothing an intent then condemns counts as having seen it.
    #[tokio::test]
    async fn a_scope_that_stored_nothing_condemned_sees_the_intent() {
        let (condemned, spares) = active();
        assert_eq!(seen(), None);
        let seen_after = condemned
            .scoped(async {
                assert_eq!(seen(), Some(0));
                condemned.before_store("a").await.unwrap();
                condemned.load(0, vec!["b".into()]).await.unwrap();
                seen()
            })
            .await;
        assert_eq!(seen_after, Some(1));
        assert!(spares.spared.lock().is_empty());
    }

    /// A scope that stored what an intent then condemns has it spared and
    /// sees the intent; refused, because deletion has begun, it stays
    /// fenced.
    #[tokio::test]
    async fn what_a_scope_stored_is_spared_when_condemned() {
        let (condemned, spares) = active();
        let seen_after = condemned
            .scoped(async {
                condemned.before_store("a").await.unwrap();
                condemned.load(0, vec!["a".into()]).await.unwrap();
                seen()
            })
            .await;
        assert_eq!(seen_after, Some(1));
        assert_eq!(*spares.spared.lock(), [(0, "a".to_string())]);

        spares.refuse.store(true, Ordering::SeqCst);
        let seen_after = condemned
            .scoped(async {
                condemned.before_store("b").await.unwrap();
                condemned.load(1, vec!["b".into()]).await.unwrap();
                seen()
            })
            .await;
        assert_eq!(seen_after, Some(1), "fenced by intent 1");
    }

    /// A scope publishing what another task stored presents no more than
    /// the intents loaded when that task began: a load cannot find the
    /// other task's store in the scope to spare it.
    #[tokio::test]
    async fn a_scope_lowered_for_another_tasks_store_stays_lowered() {
        let (condemned, spares) = active();
        let seen_after = condemned
            .scoped(async {
                lower_seen(0);
                condemned.load(0, vec!["raw".into()]).await.unwrap();
                seen()
            })
            .await;
        assert_eq!(seen_after, Some(0));
        assert!(spares.spared.lock().is_empty());
    }

    /// A store to a condemned address spares it once; refused, it waits
    /// for the intent to close.
    #[tokio::test]
    async fn a_store_to_a_condemned_address_spares_it_or_waits_for_the_close() {
        let (condemned, spares) = active();
        condemned
            .load(0, vec!["a".into(), "b".into()])
            .await
            .unwrap();
        condemned
            .scoped(async {
                condemned.before_store("a").await.unwrap();
                condemned.before_store("a").await.unwrap();
            })
            .await;
        assert_eq!(*spares.spared.lock(), [(0, "a".to_string())]);

        spares.refuse.store(true, Ordering::SeqCst);
        let store = {
            let condemned = condemned.clone();
            tokio::spawn(async move {
                condemned
                    .scoped(async { condemned.before_store("b").await })
                    .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!store.is_finished(), "the deletion may be running");
        condemned.close(0);
        store.await.unwrap().unwrap();
        assert!(condemned.open().is_empty());
    }

    /// A member whose writes are not checked runs them as is.
    #[tokio::test]
    async fn an_inactive_member_checks_nothing() {
        let condemned = Condemned::default();
        condemned.load(0, vec!["a".into()]).await.unwrap();
        let seen_inside = condemned
            .scoped(async {
                condemned.before_store("a").await.unwrap();
                seen()
            })
            .await;
        assert_eq!(seen_inside, None);
    }
}
