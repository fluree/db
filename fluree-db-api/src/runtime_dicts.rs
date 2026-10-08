use std::sync::Arc;

use fluree_db_binary_index::BinaryIndexStore;
use fluree_db_core::RuntimeSmallDicts;
use fluree_db_ledger::LedgerState;
use fluree_db_novelty::Novelty;

pub(crate) fn build_runtime_small_dicts(
    store: &Arc<BinaryIndexStore>,
    novelty: Option<&Arc<Novelty>>,
) -> Arc<RuntimeSmallDicts> {
    let mut runtime_small_dicts = store.runtime_small_dicts();
    if let Some(novelty) = novelty {
        runtime_small_dicts
            .populate_from_flakes_iter(novelty.iter_flakes(fluree_db_core::IndexType::Post));
    }
    Arc::new(runtime_small_dicts)
}

pub(crate) fn reseed_runtime_small_dicts(state: &mut LedgerState, store: &Arc<BinaryIndexStore>) {
    state.runtime_small_dicts = build_runtime_small_dicts(store, Some(&state.novelty));
}

/// Reseed against a newly published store from the dictionaries the state
/// already has, rather than from a walk over its novelty: the previous
/// dictionaries hold every predicate and datatype the novelty uses (a
/// superset is harmless; ids are runtime-only), so this costs the number of
/// predicates and datatypes, not the number of flakes.
pub(crate) fn reseed_runtime_small_dicts_from_previous(
    state: &mut LedgerState,
    store: &Arc<BinaryIndexStore>,
) {
    let previous = Arc::clone(&state.runtime_small_dicts);
    let mut reseeded = store.runtime_small_dicts();
    for id in 0..previous.predicate_count() {
        if let Some(sid) =
            previous.predicate_sid(fluree_db_core::ids::RuntimePredicateId::from_u32(id))
        {
            reseeded.assign_or_lookup_predicate(sid);
        }
    }
    for id in 0..previous.datatype_count() {
        if let Some(sid) =
            previous.datatype_sid(fluree_db_core::ids::RuntimeDatatypeId::from_u16(id))
        {
            reseeded.assign_or_lookup_datatype(sid);
        }
    }
    state.runtime_small_dicts = Arc::new(reseeded);
}
