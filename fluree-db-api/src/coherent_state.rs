//! Phase 2 — the coherent ledger-state bundle.
//!
//! [`CoherentLedgerState`] couples a [`LedgerState`] (whose four copy-on-write
//! `Arc` fields and embedded `snapshot.range_provider` form one bundle) with the
//! *typed* binary index it reads. Today `LedgerHandle` keeps the concrete store
//! in a second, out-of-band `RwLock<Option<Arc<BinaryIndexStore>>>` kept in sync
//! with `state.binary_store` (the type-erased copy) by hand; bundling the typed
//! store here lets a single lock hold everything coherently.
//!
//! Phase 2 lands in two steps:
//! - **P2a** (this commit): `LedgerHandle` holds one `RwLock<CoherentLedgerState>`
//!   instead of two locks. Concurrency semantics are unchanged (still a `RwLock`);
//!   the win is structural — the store can no longer drift from the snapshot.
//! - **P2b**: that single `RwLock` becomes an `ArcSwap`, so readers never block
//!   and torn states are impossible by construction.

use std::sync::Arc;

use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_ledger::LedgerState;

/// The typed binary index attached to a coherent ledger state: the concrete
/// store the snapshot's `range_provider` reads.
///
/// Caching the concrete `Arc<BinaryIndexStore>` here (rather than re-`downcast`ing
/// `core.binary_store` — the type-erased copy — on every read) is what lets the
/// reader path drop the out-of-band `RwLock<Option<Arc<BinaryIndexStore>>>`.
#[derive(Clone)]
pub(crate) struct AttachedIndex {
    pub(crate) store: Arc<BinaryIndexStore>,
}

/// An immutable, internally-coherent ledger-state bundle.
///
/// Coherence invariant (the reason this type exists): `index.is_some()` iff
/// `core.snapshot.range_provider.is_some()`, and when an index is attached the
/// provider reads `index.store` and holds the *same* `dict_novelty` Arc as
/// `core.dict_novelty` (the load-bearing identity contract documented on
/// `LedgerState`). Every writer publishes a whole new bundle, so a reader always
/// observes one coherent `(core, index)` pair.
#[derive(Clone)]
pub(crate) struct CoherentLedgerState {
    pub(crate) core: LedgerState,
    pub(crate) index: Option<AttachedIndex>,
}

impl CoherentLedgerState {
    /// Bundle a core with an explicit, already-loaded concrete store (the
    /// construct/load path, which has the store in hand). The caller is
    /// responsible for having attached the matching `range_provider` to
    /// `core.snapshot` (see `ledger_manager::attach_range_provider`).
    pub(crate) fn new(core: LedgerState, store: Option<Arc<BinaryIndexStore>>) -> Self {
        Self {
            core,
            index: store.map(|store| AttachedIndex { store }),
        }
    }

    /// Bundle a core whose `snapshot.range_provider` is already coherent,
    /// deriving the typed index cache from `core.binary_store`.
    ///
    /// This is the write-side publish path (commit / push / reload): the store
    /// rides inside `core.binary_store`, so deriving `index` here is what
    /// replaces the old `sync_binary_store_from_state` round-trip — the store and
    /// snapshot are swapped together, atomically, and cannot drift.
    pub(crate) fn from_core(core: LedgerState) -> Self {
        let store = core
            .binary_store
            .as_ref()
            .and_then(|te| Arc::clone(&te.0).downcast::<BinaryIndexStore>().ok());
        Self::new(core, store)
    }

    /// The concrete binary store, if an index is attached.
    pub(crate) fn store(&self) -> Option<&Arc<BinaryIndexStore>> {
        self.index.as_ref().map(|i| &i.store)
    }
}
