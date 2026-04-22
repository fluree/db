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
        runtime_small_dicts.populate_from_flakes_iter(
            novelty
                .iter_index(fluree_db_core::IndexType::Post)
                .map(|id| novelty.get_flake(id)),
        );
    }
    Arc::new(runtime_small_dicts)
}

pub(crate) fn reseed_runtime_small_dicts(state: &mut LedgerState, store: &Arc<BinaryIndexStore>) {
    state.runtime_small_dicts = build_runtime_small_dicts(store, Some(&state.novelty));
}
