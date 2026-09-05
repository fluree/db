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
use fluree_db_core::{DictNovelty, Flake, RuntimeSmallDicts};
use fluree_db_ledger::{LedgerState, StagedLedger};
use fluree_db_query::BinaryRangeProvider;
use std::collections::HashMap;
use std::sync::Arc;

fn provider_of(state: &LedgerState) -> Option<&BinaryRangeProvider> {
    state
        .snapshot
        .range_provider
        .as_ref()
        .and_then(|rp| rp.as_any().downcast_ref::<BinaryRangeProvider>())
}

/// A base state's dictionaries extended, view-locally, by a set of staged
/// flakes (see [`staged_dicts`]).
pub struct StagedDicts {
    pub store: Arc<BinaryIndexStore>,
    pub dict_novelty: Arc<DictNovelty>,
    pub runtime_small_dicts: Arc<RuntimeSmallDicts>,
}

impl StagedDicts {
    /// A range provider over these dictionaries.
    pub fn provider(
        &self,
        namespace_codes_fallback: Arc<HashMap<u16, String>>,
    ) -> Arc<BinaryRangeProvider> {
        Arc::new(BinaryRangeProvider::new(
            Arc::clone(&self.store),
            Arc::clone(&self.dict_novelty),
            Arc::clone(&self.runtime_small_dicts),
            Some(namespace_codes_fallback),
        ))
    }
}

/// Extend `base`'s dictionaries with the subjects and strings `staged`
/// introduces.
///
/// `None` when there is nothing to cover: no staged flakes, no binary
/// provider on the base (genesis / overlay-only state — the range path there
/// never translates), or an uninitialized dictionary. The base dictionaries
/// are cloned and extended, persisted-first, so an id is minted only for
/// entries in neither the persisted dictionary nor the committed novelty
/// layer. Cost is one dictionary clone per call, against the per-probe
/// whole-novelty re-translation it replaces.
pub fn staged_dicts(base: &LedgerState, staged: &[Flake]) -> Result<Option<StagedDicts>> {
    if staged.is_empty() {
        return Ok(None);
    }
    let Some(brp) = provider_of(base) else {
        return Ok(None);
    };
    if !brp.dict_novelty().is_initialized() {
        return Ok(None);
    }
    let store = Arc::clone(brp.store());
    let mut dict_novelty: DictNovelty = (**brp.dict_novelty()).clone();
    populate_dict_novelty_safe(&mut dict_novelty, Some(&store), staged.iter())
        .map_err(|e| TransactError::FlakeGeneration(format!("staged dict novelty layer: {e}")))?;
    let mut runtime_small_dicts = (**brp.runtime_small_dicts()).clone();
    runtime_small_dicts.populate_from_flakes(staged);
    Ok(Some(StagedDicts {
        store,
        dict_novelty: Arc::new(dict_novelty),
        runtime_small_dicts: Arc::new(runtime_small_dicts),
    }))
}

/// Attach a range provider whose dictionaries cover the staged flakes.
///
/// No-op when [`staged_dicts`] has nothing to cover or when it has already
/// been attached. Runs only before a read of the staged view (SHACL
/// validation, post-state policy conditions), so a transaction that never
/// reads its own staged state pays nothing.
pub fn attach_staged_dicts(view: &mut StagedLedger) -> Result<()> {
    if view.dicts_cover_staged() {
        return Ok(());
    }
    let Some(dicts) = staged_dicts(view.base(), view.staged_flakes())? else {
        return Ok(());
    };
    let provider = dicts.provider(view.base().snapshot.shared_namespaces());
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
