//! Typed cache-miss signaling for targets without a sync→async CAS bridge.
//!
//! On native targets, the sync read path bridges a cache miss to an async CAS
//! fetch via [`run_sync_on_runtime`](super::binary_index_store::run_sync_on_runtime).
//! On `wasm32-unknown-unknown` no such bridge can exist (single thread, no
//! `block_on`), so the sync accessors instead consult the content store's
//! resident-bytes tier ([`ContentStore::resolve_cached_bytes`]) and, on a miss,
//! surface a [`NeedFetch`] error naming the wanted [`ContentId`]. An async
//! caller above the operator boundary catches it, fetches the object into the
//! resident tier, and re-runs — the same "typed error through sync frames,
//! catch at the async boundary" channel the fuel tracker already uses (see
//! `BinaryCursor::next_batch`'s fuel charge and its downcast contract).
//!
//! Channel choice: `NeedFetch` rides inside [`io::Error`] as a custom payload
//! (`io::Error::other`) rather than as a new variant on the store's error
//! types. Every sync accessor already returns `io::Result`, so no signature
//! changes, and native builds construct `NeedFetch` on **no** path — the type
//! is only reachable from `cfg(target_arch = "wasm32")` read paths and from
//! tests. Catchers use [`NeedFetch::from_io_error`] at an `io::Error`
//! boundary, or [`NeedFetch::find_in_chain`] to search a wrapped error's
//! `source()` chain from higher layers.

use std::io;
use std::sync::Arc;

use fluree_db_core::{ContentId, ContentStore};

/// Which read-path family wanted the bytes. Diagnostic only — retry logic
/// needs just the [`ContentId`] (fetch the object, re-run); the kind labels
/// telemetry and error messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FetchKind {
    /// Whole index-leaf blob (scan opens, point-lookup probes, promotions).
    IndexLeaf,
    /// Per-leaf history sidecar (time-travel replay).
    HistorySidecar,
    /// Dictionary-tree leaf (reverse lookups: IRI → id, string → id).
    DictLeaf,
    /// Forward pack (id → IRI, id → string materialization).
    ForwardPack,
    /// Vector arena shard (vector search).
    VectorShard,
}

impl std::fmt::Display for FetchKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            FetchKind::IndexLeaf => "index-leaf",
            FetchKind::HistorySidecar => "history-sidecar",
            FetchKind::DictLeaf => "dict-leaf",
            FetchKind::ForwardPack => "forward-pack",
            FetchKind::VectorShard => "vector-shard",
        };
        f.write_str(s)
    }
}

/// A sync read needed CAS-backed bytes that are not resident.
///
/// Carries everything the async boundary needs to make progress: fetch
/// `cid` into the store's resident tier, then re-run the failed unit of
/// work (the cursor leaf/leaflet, or the whole query on a v1 peer).
#[derive(Debug, Clone)]
pub struct NeedFetch {
    /// The content object to fetch.
    pub cid: ContentId,
    /// Which read-path family wanted it.
    pub kind: FetchKind,
}

impl NeedFetch {
    pub fn new(cid: ContentId, kind: FetchKind) -> Self {
        Self { cid, kind }
    }

    /// Wrap into the `io::Error` channel the sync accessors return.
    pub fn into_io_error(self) -> io::Error {
        io::Error::other(self)
    }

    /// Recover a `NeedFetch` from an `io::Error`, if it carries one.
    pub fn from_io_error(err: &io::Error) -> Option<&NeedFetch> {
        err.get_ref()?.downcast_ref::<NeedFetch>()
    }

    /// Recover a `NeedFetch` from anywhere in an error's `source()` chain.
    ///
    /// Higher layers wrap the accessor `io::Error` (e.g. query errors); as
    /// long as each wrapper preserves `source()`, the retry loop can find
    /// the miss without knowing the intermediate error types.
    pub fn find_in_chain<'a>(err: &'a (dyn std::error::Error + 'static)) -> Option<&'a NeedFetch> {
        let mut cur: Option<&(dyn std::error::Error + 'static)> = Some(err);
        while let Some(e) = cur {
            if let Some(nf) = e.downcast_ref::<NeedFetch>() {
                return Some(nf);
            }
            cur = if let Some(io_err) = e.downcast_ref::<io::Error>() {
                // `io::Error::source()` skips its custom payload (it returns
                // the payload's source), so descend into the payload itself.
                io_err
                    .get_ref()
                    .map(|inner| inner as &(dyn std::error::Error + 'static))
                    .or_else(|| e.source())
            } else {
                e.source()
            };
        }
        None
    }
}

impl std::fmt::Display for NeedFetch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "content not resident: {} {} (fetch and retry)",
            self.kind, self.cid
        )
    }
}

impl std::error::Error for NeedFetch {}

/// Serve `cid` from the store's resident-bytes tier, or report the miss.
///
/// This is the whole wasm read tier: an O(1) lookup returning a shared,
/// zero-copy `Arc<[u8]>` on hit, and a [`NeedFetch`] on miss. It performs
/// no I/O and never blocks, so it is safe from any sync frame on any
/// target. Native production code does not call it (the bridge fetches
/// instead); native tests drive it directly to pin the miss/retry contract.
pub fn resident_or_need_fetch(
    cs: &dyn ContentStore,
    cid: &ContentId,
    kind: FetchKind,
) -> io::Result<Arc<[u8]>> {
    match cs.resolve_cached_bytes(cid) {
        Some(bytes) => Ok(bytes),
        None => Err(NeedFetch::new(cid.clone(), kind).into_io_error()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::read::binary_index_store::run_sync_on_runtime;
    use async_trait::async_trait;
    use fluree_db_core::content_kind::ContentKind;
    use fluree_db_core::MemoryContentStore;
    use parking_lot::RwLock;
    use std::collections::HashMap;

    /// Content store whose async side has the bytes but whose resident tier
    /// starts empty: the wasm-shaped scenario where the object exists in CAS
    /// but has not been fetched into sync-readable memory yet.
    #[derive(Debug)]
    struct MissInjectingStore {
        inner: MemoryContentStore,
        resident: RwLock<HashMap<ContentId, Arc<[u8]>>>,
    }

    impl MissInjectingStore {
        fn new() -> Self {
            Self {
                inner: MemoryContentStore::new(),
                resident: RwLock::new(HashMap::new()),
            }
        }

        /// The async fetch step a retry loop performs: pull from CAS, pin
        /// into the resident tier.
        async fn fetch_into_resident(&self, cid: &ContentId) -> fluree_db_core::Result<()> {
            let bytes = self.inner.get(cid).await?;
            self.resident
                .write()
                .insert(cid.clone(), Arc::from(bytes.into_boxed_slice()));
            Ok(())
        }
    }

    #[async_trait]
    impl ContentStore for MissInjectingStore {
        async fn has(&self, id: &ContentId) -> fluree_db_core::Result<bool> {
            self.inner.has(id).await
        }

        async fn get(&self, id: &ContentId) -> fluree_db_core::Result<Vec<u8>> {
            self.inner.get(id).await
        }

        async fn put(&self, kind: ContentKind, bytes: &[u8]) -> fluree_db_core::Result<ContentId> {
            self.inner.put(kind, bytes).await
        }

        async fn put_with_id(&self, id: &ContentId, bytes: &[u8]) -> fluree_db_core::Result<()> {
            self.inner.put_with_id(id, bytes).await
        }

        async fn release(&self, id: &ContentId) -> fluree_db_core::Result<()> {
            self.inner.release(id).await
        }

        fn resolve_cached_bytes(&self, id: &ContentId) -> Option<Arc<[u8]>> {
            self.resident.read().get(id).cloned()
        }
    }

    /// The full miss/fetch/retry round trip, with a positive assertion that
    /// the miss actually fired (an absent error is a failure, not a pass).
    #[test]
    fn miss_fetch_retry_round_trip() {
        let store = Arc::new(MissInjectingStore::new());
        let payload = b"leaf bytes".to_vec();
        let cid = run_sync_on_runtime({
            let inner = store.inner.clone();
            let payload = payload.clone();
            async move {
                inner
                    .put(ContentKind::IndexLeaf, &payload)
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))
            }
        })
        .expect("seed CAS");

        // Miss MUST fire and MUST be recoverable as a typed NeedFetch.
        let err = resident_or_need_fetch(store.as_ref(), &cid, FetchKind::IndexLeaf)
            .expect_err("resident tier is empty; the miss must surface");
        let nf = NeedFetch::from_io_error(&err).expect("io::Error must carry a typed NeedFetch");
        assert_eq!(nf.cid, cid, "the miss must name the wanted CID");
        assert_eq!(nf.kind, FetchKind::IndexLeaf);
        assert!(
            err.to_string().contains(&cid.to_string()),
            "message names the CID for logs: {err}"
        );

        // The retry step: async fetch into the resident tier, then re-run.
        run_sync_on_runtime({
            let store = Arc::clone(&store);
            let cid = cid.clone();
            async move {
                store
                    .fetch_into_resident(&cid)
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))
            }
        })
        .expect("fetch into resident tier");

        let bytes = resident_or_need_fetch(store.as_ref(), &cid, FetchKind::IndexLeaf)
            .expect("resident after fetch");
        assert_eq!(&bytes[..], &payload[..]);

        // Zero-copy contract: a second hit returns the same allocation.
        let again = resident_or_need_fetch(store.as_ref(), &cid, FetchKind::IndexLeaf).unwrap();
        assert!(
            Arc::ptr_eq(&bytes, &again),
            "hits must clone the Arc, not copy the bytes"
        );
    }

    /// Wrapper error preserving `source()`, as higher layers do.
    #[derive(Debug)]
    struct Wrapped(io::Error);

    impl std::fmt::Display for Wrapped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "query failed: {}", self.0)
        }
    }

    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(&self.0)
        }
    }

    #[test]
    fn find_in_chain_walks_wrapped_sources() {
        let store = MissInjectingStore::new();
        let cid = run_sync_on_runtime({
            let inner = store.inner.clone();
            async move {
                inner
                    .put(ContentKind::IndexLeaf, b"x")
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))
            }
        })
        .unwrap();

        let io_err = resident_or_need_fetch(&store, &cid, FetchKind::DictLeaf).unwrap_err();
        let outer = Wrapped(io_err);
        let nf = NeedFetch::find_in_chain(&outer).expect("found through the source chain");
        assert_eq!(nf.cid, cid);
        assert_eq!(nf.kind, FetchKind::DictLeaf);

        let unrelated = Wrapped(io::Error::other("plain failure"));
        assert!(
            NeedFetch::find_in_chain(&unrelated).is_none(),
            "unrelated errors must not read as misses"
        );
    }

    /// A default-implementation store (no resident tier) always misses —
    /// pins that the trait default is None, not a panic or a fetch.
    #[test]
    fn default_resolve_cached_bytes_is_a_miss() {
        let store = MemoryContentStore::new();
        let cid = run_sync_on_runtime({
            let store = store.clone();
            async move {
                store
                    .put(ContentKind::IndexLeaf, b"y")
                    .await
                    .map_err(|e| io::Error::other(e.to_string()))
            }
        })
        .unwrap();
        let err = resident_or_need_fetch(&store, &cid, FetchKind::ForwardPack).unwrap_err();
        assert!(NeedFetch::from_io_error(&err).is_some());
    }
}
