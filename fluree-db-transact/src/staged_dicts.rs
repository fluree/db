//! Dictionary coverage for reads over uncommitted state.
//!
//! A [`BinaryRangeProvider`] resolves overlay flakes through the persisted
//! dictionaries plus the ledger's [`DictNovelty`]. Both are committed-state
//! artefacts: the subjects and strings a transaction is *introducing* are in
//! neither, so any binary-lane read over a [`StagedLedger`] — SHACL
//! validation, `f:postState` policy conditions — fails to translate exactly
//! the flakes the transaction is about, logs a WARN per probe, and falls back
//! to raw-flake merging of the whole graph novelty.
//!
//! [`attach_staged_dicts`] gives a staged view a provider whose dictionaries
//! are the base ones extended by the staged flakes. The extension is
//! view-local: commit rebuilds the provider from the ledger's canonical
//! dictionaries (see `commit_txn`), so the ids minted here never reach a
//! committed state, and the staged view's own
//! [`content_version`](fluree_db_core::OverlayProvider::content_version)
//! keeps every cross-query translation cache from serving its products for
//! the committed state.
//!
//! [`detach_binary_provider`] / [`attach_binary_provider`] bracket in-place
//! dictionary mutation on a [`LedgerState`] whose snapshot already carries a
//! provider: the provider pins `Arc` clones of the dictionaries, so mutating
//! with it attached both deep-clones them (`Arc::make_mut`) and leaves the
//! provider reading the pre-mutation copies.

use crate::error::{Result, TransactError};
use fluree_db_binary_index::dict_novelty_safe::populate_dict_novelty_safe;
use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::DictNovelty;
use fluree_db_ledger::{LedgerState, StagedLedger};
use fluree_db_query::BinaryRangeProvider;
use std::sync::Arc;

fn provider_of(state: &LedgerState) -> Option<&BinaryRangeProvider> {
    state
        .snapshot
        .range_provider
        .as_ref()
        .and_then(|rp| rp.as_any().downcast_ref::<BinaryRangeProvider>())
}

/// Attach a range provider whose dictionaries cover the staged flakes.
///
/// No-op when the view has no staged flakes, no binary provider (genesis /
/// overlay-only state — the range path there never translates), an
/// uninitialized dictionary, or when it has already been attached. The base
/// dictionaries are cloned and extended, persisted-first, so an id is minted
/// only for entries in neither the persisted dictionary nor the committed
/// novelty layer. Cost is one dictionary clone per transaction that actually
/// reads its own staged state, against the per-probe whole-novelty
/// re-translation it replaces.
pub fn attach_staged_dicts(view: &mut StagedLedger) -> Result<()> {
    if view.dicts_cover_staged() || !view.has_staged() {
        return Ok(());
    }
    let provider = {
        let base = view.base();
        let Some(brp) = provider_of(base) else {
            return Ok(());
        };
        if !brp.dict_novelty().is_initialized() {
            return Ok(());
        }
        let store = Arc::clone(brp.store());
        let mut dict_novelty: DictNovelty = (**brp.dict_novelty()).clone();
        populate_dict_novelty_safe(&mut dict_novelty, Some(&store), view.staged_flakes().iter())
            .map_err(|e| {
                TransactError::FlakeGeneration(format!("staged dict novelty layer: {e}"))
            })?;
        let mut runtime_small_dicts = (**brp.runtime_small_dicts()).clone();
        runtime_small_dicts.populate_from_flakes(view.staged_flakes());
        Arc::new(BinaryRangeProvider::new(
            store,
            Arc::new(dict_novelty),
            Arc::new(runtime_small_dicts),
            Some(base.snapshot.shared_namespaces()),
        ))
    };
    Arc::make_mut(&mut view.base_mut().snapshot).range_provider = Some(provider);
    view.set_dicts_cover_staged();
    Ok(())
}

/// Detach the snapshot's binary range provider, returning its store so
/// [`attach_binary_provider`] can rebuild it once the dictionaries have been
/// mutated in place. `None` when no binary provider was attached — callers
/// must then leave the state provider-less, preserving the genesis /
/// no-index shape.
pub fn detach_binary_provider(state: &mut LedgerState) -> Option<Arc<BinaryIndexStore>> {
    let store = provider_of(state).map(|brp| Arc::clone(brp.store()))?;
    Arc::make_mut(&mut state.snapshot).range_provider = None;
    Some(store)
}

/// Rebuild the binary range provider over `store` and the state's current
/// dictionaries, so reads through the snapshot resolve every subject and
/// string the state's `dict_novelty` now knows.
pub fn attach_binary_provider(state: &mut LedgerState, store: Arc<BinaryIndexStore>) {
    let provider = Arc::new(BinaryRangeProvider::new(
        store,
        Arc::clone(&state.dict_novelty),
        Arc::clone(&state.runtime_small_dicts),
        Some(state.snapshot.shared_namespaces()),
    ));
    Arc::make_mut(&mut state.snapshot).range_provider = Some(provider);
}
